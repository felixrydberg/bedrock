//! The actor workload's history checker (#124 / S-16). Each actor is a counter: `Incr` commits it plus one, `Get` reads it
//! from the actor's slots and `Peek` answers from the instance's memory. Every answer names the instance that gave it
//! by its claim epoch, which a later claim of the actor has higher. Only answered calls constrain: a call that failed or
//! timed out may or may not have run.
//!
//! - **No lost acknowledged commit.** Acknowledged increments are distinct, and one acknowledged before another started
//!   has the lower value.
//! - **One serving instance per actor.** Once an instance answered, an older one never answers a call that started after
//!   that.
//! - **No stale read below `min_seq`.** A `Get` returns at least every increment acknowledged before it started.
//! - **A deleted actor stays deleted.** No call that started once its deletion was acknowledged gets an answer; the
//!   caller makes no call to a deleted actor until then, so one that starts at the same instant came after it.
//! - **Alarms fire.** Every acknowledged alarm fires at least once.
//!
//! Plain data, so it compiles in every build and `tests/crash` can reuse it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Incr,
    Get,
    Peek,
}

/// An answer: the instance that gave it and the counter it reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Answer {
    pub instance: u64,
    pub value: u64,
}

/// One call, timed by the caller's clock from the run's start.
#[derive(Debug, Clone)]
pub struct Call {
    pub actor: String,
    pub op: Op,
    pub start: Duration,
    pub end: Duration,
    /// `None` when it failed or timed out.
    pub answer: Option<Answer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Two acknowledged increments share a value, or a later one is not above an earlier one.
    LostCommit {
        actor: String,
        earlier: u64,
        later: u64,
    },
    /// `older` answered a call that started after `newer` had answered one.
    TwoServing {
        actor: String,
        older: u64,
        newer: u64,
    },
    /// A `Get` returned `read`, below `acknowledged`.
    StaleRead {
        actor: String,
        read: u64,
        acknowledged: u64,
    },
    /// `instance` answered a call that started after the actor's deletion was acknowledged.
    DeletedServed { actor: String, instance: u64 },
    /// An acknowledged alarm at `at_ms` never fired.
    AlarmLost { actor: String, at_ms: u64 },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::LostCommit {
                actor,
                earlier,
                later,
            } => write!(
                f,
                "no lost acknowledged commit: {actor} acknowledged {later} after {earlier}"
            ),
            Violation::TwoServing {
                actor,
                older,
                newer,
            } => write!(
                f,
                "one serving instance: {actor}'s instance {older} answered after {newer} did"
            ),
            Violation::StaleRead {
                actor,
                read,
                acknowledged,
            } => write!(
                f,
                "no stale read: {actor} read {read} after {acknowledged} was acknowledged"
            ),
            Violation::DeletedServed { actor, instance } => {
                write!(
                    f,
                    "deleted stays deleted: {actor}'s instance {instance} answered"
                )
            }
            Violation::AlarmLost { actor, at_ms } => {
                write!(f, "alarms fire: {actor}'s alarm at {at_ms} never fired")
            }
        }
    }
}

/// What the workload saw.
#[derive(Debug, Default, Clone)]
pub struct History {
    pub calls: Vec<Call>,
    /// When each deleted actor's deletion was acknowledged.
    pub deleted: BTreeMap<String, Duration>,
    /// Acknowledged alarms, by actor and time.
    pub alarms: BTreeSet<(String, u64)>,
    /// Alarms that fired.
    pub fired: BTreeSet<(String, u64)>,
}

impl History {
    /// The first violation, checks in the order of the module docs.
    // ponytail: quadratic per actor; the workload has a few hundred calls per actor. Sort by end and keep prefix maxima if
    // runs grow.
    pub fn check(&self) -> Result<(), Violation> {
        let mut by_actor: BTreeMap<&str, Vec<(&Call, Answer)>> = BTreeMap::new();
        for call in &self.calls {
            if let Some(answer) = call.answer {
                by_actor
                    .entry(&call.actor)
                    .or_default()
                    .push((call, answer));
            }
        }
        for (actor, answered) in &by_actor {
            let incrs: Vec<_> = answered.iter().filter(|(c, _)| c.op == Op::Incr).collect();
            for (i, (a, x)) in incrs.iter().enumerate() {
                for (b, y) in &incrs[i + 1..] {
                    let (earlier, later) = if a.start <= b.start { (x, y) } else { (y, x) };
                    let ordered = a.end < b.start || b.end < a.start;
                    if x.value == y.value || (ordered && earlier.value >= later.value) {
                        return Err(Violation::LostCommit {
                            actor: actor.to_string(),
                            earlier: earlier.value,
                            later: later.value,
                        });
                    }
                }
            }
        }
        for (actor, answered) in &by_actor {
            for (call, answer) in answered {
                for (other, newer) in answered {
                    if newer.instance > answer.instance && other.end < call.start {
                        return Err(Violation::TwoServing {
                            actor: actor.to_string(),
                            older: answer.instance,
                            newer: newer.instance,
                        });
                    }
                }
            }
        }
        for (actor, answered) in &by_actor {
            for (get, read) in answered.iter().filter(|(c, _)| c.op == Op::Get) {
                let acknowledged = answered
                    .iter()
                    .filter(|(c, _)| c.op == Op::Incr && c.end < get.start)
                    .map(|(_, a)| a.value)
                    .max();
                if let Some(acknowledged) = acknowledged.filter(|&a| read.value < a) {
                    return Err(Violation::StaleRead {
                        actor: actor.to_string(),
                        read: read.value,
                        acknowledged,
                    });
                }
            }
        }
        for (actor, deleted) in &self.deleted {
            let served = by_actor
                .get(actor.as_str())
                .into_iter()
                .flatten()
                .find(|(c, _)| c.start >= *deleted);
            if let Some((_, answer)) = served {
                return Err(Violation::DeletedServed {
                    actor: actor.clone(),
                    instance: answer.instance,
                });
            }
        }
        if let Some((actor, at_ms)) = self.alarms.difference(&self.fired).next() {
            return Err(Violation::AlarmLost {
                actor: actor.clone(),
                at_ms: *at_ms,
            });
        }
        Ok(())
    }
}
