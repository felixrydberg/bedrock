//! The oracle for #25 / B-23: what the writer acknowledged, and whether what nodes hold could have come from it.
//!
//! Clients retry every write with its idempotency key until it is acknowledged, so a finished run knows the operation at
//! every sequence. Copies sampled during the run are checked once the history can explain their revision. The core check
//! is that a copy at revision R holds exactly the stream's state at R, because data and progress commit atomically. Each
//! mismatch is named for the invariant it breaks, using the names in the issue.
//!
//! A soak would outgrow memory keeping everything, so a stream forgets what lies more than [`KEPT`] sequences behind what it
//! knows, apart from each key's newest value. A copy or an answer older than that fails the run rather than go unchecked.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Far more than any copy lags or any retry takes: the API's retry records expire long before this many writes.
pub const KEPT: u64 = 100_000;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
/// `(dataset, namespace)`: one writer sequence.
pub type Stream = (String, String);

/// A broken invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub invariant: &'static str,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.invariant, self.detail)
    }
}

fn violation<T>(invariant: &'static str, detail: String) -> Result<T, Violation> {
    Err(Violation { invariant, detail })
}

fn text(bytes: &[u8]) -> String {
    bytes.escape_ascii().to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Operation {
    /// The client's identity for this operation, its idempotency key.
    id: String,
    key: Key,
    /// `None` deletes.
    value: Option<Value>,
}

#[derive(Default)]
pub struct History {
    streams: BTreeMap<Stream, BTreeMap<u64, Operation>>,
    sequences: BTreeMap<(Stream, String), u64>,
    /// Each key's values by sequence, `None` when deleted, so a lookup does not depend on how long the run has been.
    versions: BTreeMap<Stream, BTreeMap<Key, BTreeMap<u64, Option<Value>>>>,
    /// The sequence up to which every operation of a stream is known.
    known: BTreeMap<Stream, u64>,
    /// The sequence up to which a stream has been forgotten.
    forgotten: BTreeMap<Stream, u64>,
}

impl History {
    /// Records that the writer acknowledged operation `id` at `sequence`. The same answer may arrive more than once.
    pub fn acknowledged(
        &mut self,
        stream: &Stream,
        sequence: u64,
        id: &str,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), Violation> {
        // A forgotten sequence's operation is gone too, so an answer there cannot be told from a reused sequence.
        self.kept(stream, sequence.saturating_sub(1))?;
        let earlier = self.sequences.entry((stream.clone(), id.to_owned()));
        let known = *earlier.or_insert(sequence);
        if known != sequence {
            return violation(
                "RetryProtection",
                format!("{stream:?} applied {id} at both {known} and {sequence}"),
            );
        }
        let operation = Operation {
            id: id.to_owned(),
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
        };
        let operations = self.streams.entry(stream.clone()).or_default();
        match operations.get(&sequence) {
            Some(other) if *other != operation => violation(
                "NoLostAck",
                format!(
                    "{stream:?} acknowledged {} and {id} at the same sequence {sequence}",
                    other.id
                ),
            ),
            _ => {
                operations.insert(sequence, operation);
                let known = self.known.entry(stream.clone()).or_default();
                while operations.contains_key(&(*known + 1)) {
                    *known += 1;
                }
                let versions = self.versions.entry(stream.clone()).or_default();
                let key = versions.entry(key.to_vec()).or_default();
                key.insert(sequence, value.map(<[u8]>::to_vec));
                self.forget(stream);
                Ok(())
            }
        }
    }

    /// Forgets what lies more than [`KEPT`] behind what `stream` knows. An unacknowledged sequence stops `known`, so nothing
    /// a client may still be retrying is forgotten.
    fn forget(&mut self, stream: &Stream) {
        let known = self.known_through(stream);
        let forgotten = self.forgotten.entry(stream.clone()).or_default();
        while *forgotten + KEPT < known {
            *forgotten += 1;
            let sequence = *forgotten;
            let operations = self.streams.get_mut(stream).expect("it was just written");
            let operation = operations.remove(&sequence).expect("it is below known");
            self.sequences.remove(&(stream.clone(), operation.id));
            let versions = self.versions.get_mut(stream).expect("it was just written");
            let versions = versions.get_mut(&operation.key).expect("it was written");
            *versions = versions.split_off(&sequence);
        }
    }

    /// Fails on a revision the history no longer holds: not a bug found, but a run it can no longer check.
    fn kept(&self, stream: &Stream, revision: u64) -> Result<(), Violation> {
        let forgotten = self.forgotten.get(stream).copied().unwrap_or(0);
        if revision < forgotten {
            return violation(
                "HistoryKept",
                format!("{stream:?} reached back to {revision}, below {forgotten}"),
            );
        }
        Ok(())
    }

    /// The highest acknowledged sequence of `stream`.
    pub fn head(&self, stream: &Stream) -> u64 {
        self.streams
            .get(stream)
            .and_then(|operations| operations.keys().next_back().copied())
            .unwrap_or(0)
    }

    /// The sequence up to which every operation of `stream` is known: a copy at or below it can be checked.
    pub fn known_through(&self, stream: &Stream) -> u64 {
        self.known.get(stream).copied().unwrap_or(0)
    }

    pub fn streams(&self) -> impl Iterator<Item = &Stream> {
        self.streams.keys()
    }

    /// Every sequence up to each head was acknowledged. Call once no write is outstanding.
    pub fn gap_free(&self) -> Result<(), Violation> {
        for stream in self.streams.keys() {
            let head = self.head(stream);
            let known = self.known_through(stream);
            if known < head {
                let missing = known + 1;
                return violation(
                    "GapFreeApply",
                    format!("{stream:?} has no operation at {missing} below head {head}"),
                );
            }
        }
        Ok(())
    }

    /// The writer's durable head can only be at or above everything it acknowledged.
    pub fn check_head(&self, stream: &Stream, durable: u64) -> Result<(), Violation> {
        let head = self.head(stream);
        if durable < head {
            return violation(
                "NoLostAck",
                format!("{stream:?} acknowledged {head} but holds only {durable}"),
            );
        }
        Ok(())
    }

    /// The values of the keys of `stream` that `selected` accepts, after `revision`.
    pub fn state_at(
        &self,
        stream: &Stream,
        revision: u64,
        selected: impl Fn(&[u8]) -> bool,
    ) -> BTreeMap<Key, Value> {
        let keys = self.versions.get(stream).into_iter().flatten();
        keys.filter(|(key, _)| selected(key))
            .filter_map(|(key, versions)| {
                let (_, value) = versions.range(..=revision).next_back()?;
                Some((key.clone(), value.clone()?))
            })
            .collect()
    }

    fn versions(&self, stream: &Stream, key: &[u8]) -> Option<&BTreeMap<u64, Option<Value>>> {
        self.versions.get(stream)?.get(key)
    }

    /// `held` is a copy of the keys of `stream` that `selected` accepts, at applied `revision`.
    pub fn check_copy(
        &self,
        stream: &Stream,
        revision: u64,
        selected: impl Fn(&[u8]) -> bool,
        held: &BTreeMap<Key, Value>,
    ) -> Result<(), Violation> {
        self.kept(stream, revision)?;
        let head = self.head(stream);
        if revision > head {
            return violation(
                "NoLostAck",
                format!("{stream:?} is applied at {revision}, past the writer's head {head}"),
            );
        }
        let expected = self.state_at(stream, revision, selected);
        let keys: BTreeSet<&Key> = expected.keys().chain(held.keys()).collect();
        for key in keys {
            self.check_key(stream, revision, key, expected.get(key), held.get(key))?;
        }
        Ok(())
    }

    /// A read at or above a minimum revision: `observed` came from a copy applied at `revision`, and that revision proves
    /// both values and absence.
    pub fn check_read(
        &self,
        stream: &Stream,
        min_revision: u64,
        revision: u64,
        key: &[u8],
        observed: Option<&[u8]>,
    ) -> Result<(), Violation> {
        if revision < min_revision {
            return violation(
                "MinRevisionReads",
                format!("{stream:?} served at {revision}, below the minimum {min_revision}"),
            );
        }
        self.kept(stream, revision)?;
        let versions = self.versions(stream, key);
        let latest = versions.and_then(|versions| versions.range(..=revision).next_back());
        let expected = latest.and_then(|(_, value)| value.as_deref());
        if observed != expected {
            return violation(
                "MinRevisionReads",
                format!(
                    "{stream:?} key {} at {revision} read {:?}, expected {:?}",
                    text(key),
                    observed.map(text),
                    expected.map(text)
                ),
            );
        }
        Ok(())
    }

    fn check_key(
        &self,
        stream: &Stream,
        revision: u64,
        key: &[u8],
        expected: Option<&Value>,
        held: Option<&Value>,
    ) -> Result<(), Violation> {
        let at = || format!("{stream:?} key {} at revision {revision}", text(key));
        let from_later = |value: &Value| {
            self.versions(stream, key).is_some_and(|versions| {
                versions
                    .range(revision + 1..)
                    .any(|(_, later)| later.as_ref() == Some(value))
            })
        };
        match (expected, held) {
            (Some(expected), Some(held)) if expected == held => Ok(()),
            (None, None) => Ok(()),
            (Some(expected), None) => violation(
                "GapFreeApply",
                format!("{} is missing {}", at(), text(expected)),
            ),
            // Newer data than its progress covers: the two did not commit together.
            (_, Some(held)) if from_later(held) => violation(
                "GapFreeApply",
                format!("{} holds {} from a later revision", at(), text(held)),
            ),
            (None, Some(held)) => violation(
                "NoResurrection",
                format!("{} holds {} but should be absent", at(), text(held)),
            ),
            (Some(expected), Some(held)) => violation(
                "NoStaleOverwrite",
                format!("{} holds {}, not {}", at(), text(held), text(expected)),
            ),
        }
    }
}

/// `(start, end)`, `end` 0 meaning 2⁶⁴.
pub type Range = (u64, u64);

pub fn within((start, end): Range, hash: u64) -> bool {
    hash >= start && (end == 0 || hash < end)
}

/// A range owner's state, mirroring the placement directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerState {
    Populating,
    Ready,
    Leaving,
}

/// One dataset's placement directory at one moment: each range with its owners.
pub type Placement = Vec<(Range, Vec<(String, OwnerState)>)>;

/// Ranges must tile the whole hash space without gaps or overlaps.
pub fn check_coverage(placement: &Placement) -> Result<(), Violation> {
    let mut next = Some(0);
    for ((start, end), _) in placement {
        if next != Some(*start) {
            return violation(
                "DirectoryCoverage",
                format!("a range starts at {start:x}, expected {next:x?}"),
            );
        }
        next = (*end != 0).then_some(*end);
    }
    if !placement.is_empty() && next.is_some() {
        return violation(
            "DirectoryCoverage",
            format!("ranges stop at {next:x?} before the end of the hash space"),
        );
    }
    Ok(())
}

/// The owners of the range containing `hash`.
/// The owners of the range holding `hash`, found by binary search: `placement` is sorted by start, as the harnesses build
/// it from the directory. A scan made every directory write cost the square of the range count (#66 / B-61).
fn owners_at(placement: &Placement, hash: u64) -> impl Iterator<Item = &(String, OwnerState)> {
    let after = placement.partition_point(|(range, _)| range.0 <= hash);
    placement[..after]
        .last()
        .filter(|(range, _)| within(*range, hash))
        .into_iter()
        .flat_map(|(_, owners)| owners)
}

/// A range that had a healthy ready owner keeps a serving one: a `Ready` owner, or a `Leaving` one still holding the
/// data until its replacements are ready. `healthy` lists the members still able to hold it, because losing the only
/// copy to a crash is not a handoff.
pub fn check_handoff(
    before: &Placement,
    after: &Placement,
    healthy: &BTreeSet<String>,
) -> Result<(), Violation> {
    for (range, owners) in before {
        let ready = |(owner, state): &(String, OwnerState)| {
            *state == OwnerState::Ready && healthy.contains(owner)
        };
        if !owners.iter().any(ready) {
            continue;
        }
        let serving = owners_at(after, range.0).any(|(_, state)| *state != OwnerState::Populating);
        if !serving {
            return violation(
                "NoDropBeforeReady",
                format!("range {range:x?} went from {owners:?} to {after:?}"),
            );
        }
    }
    Ok(())
}

/// `node` stores data only in ranges it owns or is leaving.
pub fn check_held(
    placement: &Placement,
    node: &str,
    hashes: impl IntoIterator<Item = u64>,
) -> Result<(), Violation> {
    for hash in hashes {
        if !owners_at(placement, hash).any(|(owner, _)| owner == node) {
            return violation(
                "OwnedOrLeaving",
                format!("{node} holds hash {hash:x} outside its ranges"),
            );
        }
    }
    Ok(())
}

/// A member already unhealthy at `before` must not gain ranges by `after`.
pub fn check_routing(
    before: &Placement,
    after: &Placement,
    unhealthy: &BTreeSet<String>,
) -> Result<(), Violation> {
    for (range, owners) in after {
        for (owner, _) in owners {
            let held = owners_at(before, range.0).any(|(earlier, _)| earlier == owner);
            if unhealthy.contains(owner) && !held {
                return violation(
                    "NoUnhealthyRouting",
                    format!("unhealthy {owner} gained range {range:x?}"),
                );
            }
        }
    }
    Ok(())
}
