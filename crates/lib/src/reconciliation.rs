//! Generic desired-vs-actual reconciliation with repair scheduling (#21 / B-20).
//!
//! A [`Reconcile`] user supplies two sources keyed by the same unit (a range and owner, an artifact), and plans one
//! idempotent repair for each key where they disagree. Each [`Reconciler::pass`] re-reads both sources, diffs them in
//! key order and runs the due repairs, highest priority first, within a token-bucket rate limit. A failed repair backs
//! off exponentially with seeded jitter and is planned again on later passes; nothing about a repair is persisted, so
//! one interrupted by a restart shows up in the next diff and runs again. Users carry no product logic into this module.
//!
//! Repairs should start or advance a step and return, leaving progress to show in `actual()`: L2 handoff (#22 / B-21)
//! plans "push interest and bootstrap" for a `populating` owner with no interest, "mark ready" once its bootstrap is
//! acknowledged through the head, and "revoke" for a `leaving` owner once every new owner is `ready`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use tokio::time::{Instant, sleep, timeout};

use crate::monotonic_now;
use crate::transport::admin::Admin;

#[async_trait::async_trait]
pub trait Reconcile: Send + Sync + 'static {
    type Key: Ord + Clone + Debug + Send + Sync;
    type Desired: Send + Sync;
    type Actual: Send + Sync;
    type Repair: Send + Sync;
    type Error: Display + Send;

    async fn desired(&self) -> Result<BTreeMap<Self::Key, Self::Desired>, Self::Error>;
    async fn actual(&self) -> Result<BTreeMap<Self::Key, Self::Actual>, Self::Error>;

    /// The repair for `key` and its priority (lower runs first), or `None` when it needs none. Called for every key in
    /// either source, so "only desired" and "only actual" are the `None` sides.
    fn plan(
        &self,
        key: &Self::Key,
        desired: Option<&Self::Desired>,
        actual: Option<&Self::Actual>,
    ) -> Option<(u8, Self::Repair)>;

    /// Must be idempotent: it may run again after a failure, a restart, or before `actual()` reflects it.
    async fn repair(&self, key: &Self::Key, repair: &Self::Repair) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Repairs allowed back to back.
    pub burst: u32,
    /// One more repair is allowed every `refill`.
    pub refill: Duration,
    /// Maximum time spent waiting for one repair.
    pub repair_timeout: Duration,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// Seeds backoff jitter, so a simulation seed replays the same schedule (B-23a).
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Labels {
    reconciler: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct RepairLabels {
    reconciler: &'static str,
    outcome: &'static str,
}

/// Registered once per process and shared by every [`Reconciler`], labelled by reconciler name only.
#[derive(Clone)]
pub struct Metrics {
    pending: Family<Labels, Gauge>,
    failing: Family<Labels, Gauge>,
    repairs: Family<RepairLabels, Counter>,
    source_errors: Family<Labels, Counter>,
}

impl Metrics {
    pub fn new(admin: &Admin) -> Self {
        let metrics = Metrics {
            pending: Family::default(),
            failing: Family::default(),
            repairs: Family::default(),
            source_errors: Family::default(),
        };
        admin.register(
            "reconcile_pending",
            "Keys whose desired and actual state differ after the last pass",
            metrics.pending.clone(),
        );
        admin.register(
            "reconcile_failing",
            "Pending keys whose last repair failed and are backing off",
            metrics.failing.clone(),
        );
        admin.register(
            "reconcile_repairs",
            "Repairs run, by outcome",
            metrics.repairs.clone(),
        );
        admin.register(
            "reconcile_source_errors",
            "Passes abandoned because the desired or actual source failed",
            metrics.source_errors.clone(),
        );
        metrics
    }
}

/// What one pass found and did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Pass {
    /// Keys needing a repair at the start of the pass.
    pub pending: usize,
    pub repaired: usize,
    pub failed: usize,
}

struct Retry {
    attempts: u32,
    not_before: Instant,
}

pub struct Reconciler<R: Reconcile> {
    name: &'static str,
    source: Arc<R>,
    config: Config,
    metrics: Metrics,
    tokens: u32,
    refilled: Instant,
    retries: BTreeMap<R::Key, Retry>,
    rng: u64,
}

impl<R: Reconcile> Reconciler<R> {
    pub fn new(name: &'static str, source: Arc<R>, config: Config, metrics: &Metrics) -> Self {
        Reconciler {
            name,
            source,
            tokens: config.burst,
            refilled: monotonic_now(),
            rng: config.seed,
            config,
            metrics: metrics.clone(),
            retries: BTreeMap::new(),
        }
    }

    /// Diffs the sources and runs the due repairs the rate limit allows; the rest wait for a later pass.
    pub async fn pass(&mut self) -> Result<Pass, R::Error> {
        let labels = Labels {
            reconciler: self.name,
        };
        let source = &self.source;
        let sources =
            async { Ok::<_, R::Error>((source.desired().await?, source.actual().await?)) };
        let (desired, actual) = match sources.await {
            Ok(sources) => sources,
            Err(error) => {
                self.metrics.source_errors.get_or_create(&labels).inc();
                return Err(error);
            }
        };

        let keys: BTreeSet<&R::Key> = desired.keys().chain(actual.keys()).collect();
        let mut tasks: Vec<_> = keys
            .into_iter()
            .filter_map(|key| {
                let (priority, repair) =
                    self.source.plan(key, desired.get(key), actual.get(key))?;
                Some((priority, key.clone(), repair))
            })
            .collect();
        // Stable: equal priorities stay in key order.
        tasks.sort_by_key(|(priority, ..)| *priority);
        // Keys that converged, or no longer exist, forget their failures.
        let planned: BTreeSet<&R::Key> = tasks.iter().map(|(_, key, _)| key).collect();
        self.retries.retain(|key, _| planned.contains(key));

        let mut pass = Pass {
            pending: tasks.len(),
            ..Pass::default()
        };
        for (_, key, repair) in &tasks {
            let now = monotonic_now();
            if self.retries.get(key).is_some_and(|r| now < r.not_before) {
                continue;
            }
            if !self.take_token(now) {
                break;
            }
            let failed =
                match timeout(self.config.repair_timeout, self.source.repair(key, repair)).await {
                    Ok(Ok(())) => false,
                    Ok(Err(error)) => {
                        tracing::warn!(reconciler = self.name, ?key, %error, "repair failed");
                        true
                    }
                    Err(_) => {
                        tracing::warn!(reconciler = self.name, ?key, "repair timed out");
                        true
                    }
                };
            let ok = if failed {
                let attempts = self.retries.get(key).map_or(1, |r| r.attempts + 1);
                let not_before = monotonic_now() + self.backoff(attempts);
                self.retries.insert(
                    key.clone(),
                    Retry {
                        attempts,
                        not_before,
                    },
                );
                pass.failed += 1;
                false
            } else {
                self.retries.remove(key);
                pass.repaired += 1;
                true
            };
            self.metrics
                .repairs
                .get_or_create(&RepairLabels {
                    reconciler: self.name,
                    outcome: if ok { "ok" } else { "failed" },
                })
                .inc();
        }
        self.metrics
            .pending
            .get_or_create(&labels)
            .set(pass.pending as i64);
        self.metrics
            .failing
            .get_or_create(&labels)
            .set(self.retries.len() as i64);
        Ok(pass)
    }

    /// Runs a pass every `interval` until the task is dropped.
    // ponytail: polls; wake on a source change when a user needs lower latency than `interval`.
    pub async fn run(mut self, interval: Duration) {
        loop {
            if let Err(error) = self.pass().await {
                tracing::warn!(reconciler = self.name, %error, "reconciliation sources failed");
            }
            sleep(interval).await;
        }
    }

    fn take_token(&mut self, now: Instant) -> bool {
        let refill = self.config.refill.max(Duration::from_nanos(1));
        let earned = (now.duration_since(self.refilled).as_nanos() / refill.as_nanos())
            .min(u128::from(self.config.burst)) as u32;
        if self.tokens + earned >= self.config.burst {
            self.tokens = self.config.burst;
            self.refilled = now;
        } else {
            self.tokens += earned;
            self.refilled += refill * earned;
        }
        let Some(left) = self.tokens.checked_sub(1) else {
            return false;
        };
        self.tokens = left;
        true
    }

    /// `base · 2^(attempts−1)`, capped at the maximum, scaled into `[½, 1)` by SplitMix64 over the seed.
    fn backoff(&mut self, attempts: u32) -> Duration {
        let backoff = self
            .config
            .base_backoff
            .saturating_mul(1 << (attempts - 1).min(31))
            .min(self.config.max_backoff);
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        backoff.mul_f64(0.5 + (z >> 11) as f64 / (1u64 << 54) as f64)
    }
}

#[cfg(test)]
#[path = "../tests/unit/reconciliation.rs"]
mod tests;
