//! Per-peer reachability from real traffic (B-13). There is no health protocol: [`crate::transport::Peers::call`]
//! feeds every outbound call's outcome into a per-peer circuit breaker, and placement and source selection
//! [`Health::route`] around peers whose breaker is open, on top of membership (B-12).
//!
//! - **closed → open** after [`CONSECUTIVE_FAILURES`] failures in a row, more than half of at least
//!   [`WINDOW_MIN_CALLS`] calls failing within [`WINDOW`], or a dropped persistent stream ([`Health::trip`]).
//! - **open → half-open** once the backoff has passed: the next real call to the peer is the probe.
//! - **half-open → closed** on success, otherwise **open** again with double the backoff, up to [`MAX_BACKOFF`].
//!
//! The view is local to this node and never shared; removing a node from the cell is B-12's decision.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tokio::time::Instant;

use crate::transport::admin::Admin;

pub const CONSECUTIVE_FAILURES: u32 = 5;
pub const WINDOW: Duration = Duration::from_secs(10);
pub const WINDOW_MIN_CALLS: u32 = 20;
pub const BASE_BACKOFF: Duration = Duration::from_secs(1);
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct TransitionLabels {
    to: State,
}

#[derive(Debug)]
enum Breaker {
    Closed {
        consecutive: u32,
        // ponytail: tumbling window, so a burst split across a boundary can escape the rate check (the consecutive
        // check still catches a dead peer); use a sliding window if that matters.
        window_start: Instant,
        calls: u32,
        failures: u32,
    },
    Open {
        until: Instant,
        backoff: Duration,
    },
    /// A probe is in flight until `probe_until`; after that it was abandoned and the next call probes again.
    HalfOpen {
        probe: Arc<()>,
        probe_until: Instant,
        backoff: Duration,
    },
}

impl Breaker {
    fn closed(now: Instant) -> Self {
        Breaker::Closed {
            consecutive: 0,
            window_start: now,
            calls: 0,
            failures: 0,
        }
    }

    fn state(&self) -> State {
        match self {
            Breaker::Closed { .. } => State::Closed,
            Breaker::Open { .. } => State::Open,
            Breaker::HalfOpen { .. } => State::HalfOpen,
        }
    }

    /// Whether routing may send a call: closed, or due a probe.
    fn usable(&self, now: Instant) -> bool {
        match *self {
            Breaker::Closed { .. } => true,
            Breaker::Open { until, .. } => now >= until,
            Breaker::HalfOpen { probe_until, .. } => now >= probe_until,
        }
    }

    fn reopen(backoff: Duration, now: Instant) -> Self {
        let backoff = (backoff * 2).min(MAX_BACKOFF);
        Breaker::Open {
            until: now + backoff,
            backoff,
        }
    }
}

/// This node's breakers, keyed by peer address (`host:port`).
pub struct Health {
    // ponytail: entries are never evicted, like `Peers::channels`; drop departed peers once membership exists (B-12).
    breakers: Mutex<BTreeMap<String, Breaker>>,
    transitions: Family<TransitionLabels, Counter>,
    fail_open: Counter,
}

impl Health {
    pub fn new(admin: &Admin) -> Self {
        let transitions = Family::default();
        let fail_open = Counter::default();
        admin.register(
            "health_breaker_transitions",
            "Per-peer circuit breaker transitions by new state",
            transitions.clone(),
        );
        admin.register(
            "health_fail_open",
            "Routing decisions where every candidate's breaker was open, so all were tried",
            fail_open.clone(),
        );
        Health {
            breakers: Mutex::default(),
            transitions,
            fail_open,
        }
    }

    /// `candidates` in order, without peers whose breaker is open. When that leaves none, all of them: a breaker bug
    /// must not take down a whole bucket (fail open).
    pub fn route<'a, T: AsRef<str>>(&self, candidates: &'a [T]) -> Vec<&'a T> {
        let now = now();
        let breakers = self.breakers.lock().unwrap();
        let usable: Vec<&T> = candidates
            .iter()
            .filter(|p| breakers.get(p.as_ref()).is_none_or(|b| b.usable(now)))
            .collect();
        if usable.is_empty() && !candidates.is_empty() {
            self.fail_open.inc();
            return candidates.iter().collect();
        }
        usable
    }

    /// Opens `peer`'s breaker now, e.g. when a persistent stream to it ends. A half-open breaker backs off longer;
    /// an open one is left alone.
    pub fn trip(&self, peer: &str) {
        let now = now();
        self.update(peer, |b| match *b {
            Breaker::Closed { .. } => Some(Breaker::Open {
                until: now + BASE_BACKOFF,
                backoff: BASE_BACKOFF,
            }),
            Breaker::HalfOpen { backoff, .. } => Some(Breaker::reopen(backoff, now)),
            Breaker::Open { .. } => None,
        });
    }

    pub fn state(&self, peer: &str) -> State {
        self.breakers
            .lock()
            .unwrap()
            .get(peer)
            .map_or(State::Closed, Breaker::state)
    }

    /// A call to `peer` that gives up after `timeout` is starting. Returns its identity if it owns the probe.
    pub(crate) fn begin(&self, peer: &str, timeout: Duration) -> Option<Arc<()>> {
        let now = now();
        let mut probe = None;
        self.update(peer, |b| match *b {
            Breaker::Open {
                until: probe_until,
                backoff,
            }
            | Breaker::HalfOpen {
                probe_until,
                backoff,
                ..
            } if now >= probe_until => {
                let identity = Arc::new(());
                probe = Some(identity.clone());
                Some(Breaker::HalfOpen {
                    probe: identity,
                    probe_until: now + timeout,
                    backoff,
                })
            }
            _ => None,
        });
        probe
    }

    /// The outcome of a call to `peer`. Outcomes while open come from calls started before it opened, or from fail-open
    /// routing, and are ignored. Only the call owning the active probe may resolve a half-open breaker.
    pub(crate) fn record(&self, peer: &str, probe: Option<Arc<()>>, ok: bool) {
        let now = now();
        self.update(peer, |b| match b {
            Breaker::Closed {
                consecutive,
                window_start,
                calls,
                failures,
            } => {
                if now.duration_since(*window_start) >= WINDOW {
                    *window_start = now;
                    *calls = 0;
                    *failures = 0;
                }
                *calls += 1;
                if ok {
                    *consecutive = 0;
                } else {
                    *consecutive += 1;
                    *failures += 1;
                }
                let tripped = *consecutive >= CONSECUTIVE_FAILURES
                    || (*calls >= WINDOW_MIN_CALLS && *failures * 2 > *calls);
                tripped.then_some(Breaker::Open {
                    until: now + BASE_BACKOFF,
                    backoff: BASE_BACKOFF,
                })
            }
            Breaker::HalfOpen {
                probe: active,
                backoff,
                ..
            } if probe.as_ref().is_some_and(|p| Arc::ptr_eq(p, active)) => Some(if ok {
                Breaker::closed(now)
            } else {
                Breaker::reopen(*backoff, now)
            }),
            Breaker::Open { .. } | Breaker::HalfOpen { .. } => None,
        });
    }

    /// Applies `f` to `peer`'s breaker; `Some` replaces it and counts the transition if the state changed.
    fn update(&self, peer: &str, f: impl FnOnce(&mut Breaker) -> Option<Breaker>) {
        let mut breakers = self.breakers.lock().unwrap();
        let breaker = breakers
            .entry(peer.to_owned())
            .or_insert_with(|| Breaker::closed(now()));
        let Some(next) = f(breaker) else { return };
        if next.state() != breaker.state() {
            self.transitions
                .get_or_create(&TransitionLabels { to: next.state() })
                .inc();
        }
        *breaker = next;
    }
}

// madsim's `Instant` is std's, and madsim simulates its `now`; the lints guard normal builds.
#[cfg_attr(madsim, allow(clippy::disallowed_methods))]
fn now() -> Instant {
    Instant::now()
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;

    const PROBE_TIMEOUT: Duration = Duration::from_millis(100);

    fn fail(health: &Health, peer: &str, times: u32) {
        for _ in 0..times {
            let probe = health.begin(peer, PROBE_TIMEOUT);
            health.record(peer, probe, false);
        }
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn failing_peer_is_skipped_within_n_calls() {
        let health = Health::new(&Admin::new());
        fail(&health, "a", CONSECUTIVE_FAILURES - 1);
        assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
        fail(&health, "a", 1);
        assert_eq!(health.state("a"), State::Open);
        assert_eq!(health.route(&["a", "b"]), [&"b"]);
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn high_error_rate_trips_without_consecutive_failures() {
        let health = Health::new(&Admin::new());
        // Two failures for every success: never CONSECUTIVE_FAILURES in a row, but well over half.
        for i in 0..WINDOW_MIN_CALLS {
            assert_eq!(
                health.state("a"),
                State::Closed,
                "tripped early at call {i}"
            );
            let probe = health.begin("a", PROBE_TIMEOUT);
            health.record("a", probe, i % 3 == 0);
        }
        assert_eq!(health.state("a"), State::Open);
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn recovered_peer_is_used_after_one_probe() {
        let health = Health::new(&Admin::new());
        health.trip("a");
        sleep(BASE_BACKOFF).await;
        assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);

        let probe = health.begin("a", PROBE_TIMEOUT);
        assert_eq!(health.state("a"), State::HalfOpen);
        // Only one probe at a time.
        assert_eq!(health.route(&["a", "b"]), [&"b"]);

        health.record("a", probe, true);
        assert_eq!(health.state("a"), State::Closed);
        assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn failing_peer_is_probed_at_decreasing_rate() {
        let health = Health::new(&Admin::new());
        health.trip("a");
        let tick = Duration::from_millis(1);
        for secs in [1, 2, 4, 8, 16, 32, 60, 60] {
            sleep(Duration::from_secs(secs) - tick).await;
            assert_eq!(health.route(&["a", "b"]), [&"b"], "probed before {secs}s");
            sleep(tick).await;
            assert_eq!(
                health.route(&["a", "b"]),
                [&"a", &"b"],
                "not probed at {secs}s"
            );
            fail(&health, "a", 1);
            assert_eq!(health.state("a"), State::Open);
        }
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn abandoned_probe_is_retried() {
        let health = Health::new(&Admin::new());
        health.trip("a");
        sleep(BASE_BACKOFF).await;
        let _ = health.begin("a", PROBE_TIMEOUT);
        assert_eq!(health.route(&["a", "b"]), [&"b"]);
        sleep(PROBE_TIMEOUT).await;
        assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn only_the_active_probe_can_resolve_half_open() {
        for ok in [true, false] {
            for interrupted in [true, false] {
                let health = Health::new(&Admin::new());
                let closed_call = health.begin("a", PROBE_TIMEOUT);
                health.trip("a");
                let open_call = health.begin("a", PROBE_TIMEOUT);
                sleep(BASE_BACKOFF).await;
                let old_probe = health.begin("a", PROBE_TIMEOUT);
                assert!(old_probe.is_some());
                let concurrent_call = health.begin("a", PROBE_TIMEOUT);

                for call in [closed_call, open_call, concurrent_call] {
                    assert!(call.is_none());
                    health.record("a", call, ok);
                    assert_eq!(health.state("a"), State::HalfOpen);
                }

                if interrupted {
                    health.trip("a");
                    sleep(BASE_BACKOFF * 2).await;
                } else {
                    sleep(PROBE_TIMEOUT).await;
                }
                let active_probe = health.begin("a", PROBE_TIMEOUT);
                assert!(active_probe.is_some());
                health.record("a", old_probe, ok);
                assert_eq!(health.state("a"), State::HalfOpen);
                assert_eq!(health.route(&["a", "b"]), [&"b"]);

                health.record("a", active_probe, ok);
                assert_eq!(
                    health.state("a"),
                    if ok { State::Closed } else { State::Open }
                );
            }
        }
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn every_candidate_open_fails_open() {
        let admin = Admin::new();
        let health = Health::new(&admin);
        health.trip("a");
        health.trip("b");
        assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
        assert!(health.route::<&str>(&[]).is_empty());

        let metrics = admin.render();
        assert!(
            metrics.contains("bedrock_health_fail_open_total 1"),
            "{metrics}"
        );
        assert!(
            metrics.contains(r#"bedrock_health_breaker_transitions_total{to="Open"} 2"#),
            "{metrics}"
        );
    }
}
