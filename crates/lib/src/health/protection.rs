//! Read protection (#24 / B-22b): stops a cold-cache surge from overwhelming shared read capacity.
//!
//! A cell fails and its traffic lands on cold caches, or a range move leaves a new CDN owner warming artifacts. Every
//! miss then heads for the same few sources: S3, namespace opens and recent committed state.
//!
//! - [`Coalescer`] makes identical concurrent requests entering one coalescer share one upstream operation. Each node
//!   has its own, so a key fetched through three nodes costs three fetches: there is no global deduplication.
//! - [`Limiter`] bounds each source on each node, so separate coalescers stay bounded too. A request over its cell's
//!   bound fails at once with a retryable [`StorageError::Overloaded`], and the caller backs off.
//!
//! [`crate::health::fallback::SourceChain`] coalesces reads and [`crate::health::fallback::Limited`] puts a limiter in
//! front of one tier. Customer-KV owners bound namespace opens with [`NAMESPACE_OPENS`] (`replication::writer`).
//! Customer request-rate policy is separate (B-56).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::future::Future;
use std::sync::{Arc, Mutex};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::admin::Admin;
use crate::storage::{ErrorContext, StorageError, StorageResult};

/// Bounds for one source on one node.
///
/// A cell runs at most `per_cell` operations and queues at most `queue` more; anything beyond that is rejected. So a
/// surging cell holds at most `per_cell` of `total`, and because the shared `total` queue is first come, first served,
/// each busy cell gets an equal turn however much it sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub total: usize,
    pub per_cell: usize,
    pub queue: usize,
}

// Starting values, not measurements; B-53 tunes them. `per_cell` is a quarter of `total`, so one surging cell leaves
// three quarters for the others, and `queue` is four times `per_cell`, so an admitted request waits about four
// operations at worst.

/// Whole-object and range reads from S3, e.g. CDN artifacts.
pub const S3_FETCHES: Limits = Limits {
    total: 64,
    per_cell: 16,
    queue: 64,
};

/// Namespace handle opens. Far fewer than fetches: each one reads a manifest, writes a fence and starts engine tasks.
pub const NAMESPACE_OPENS: Limits = Limits {
    total: 8,
    per_cell: 2,
    queue: 8,
};

/// Requests to a namespace's home writer for state newer than its S3 view. That writer also takes the writes.
pub const RECENT_STATE: Limits = Limits {
    total: 32,
    per_cell: 8,
    queue: 32,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct SourceLabels {
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct CellLabels {
    source: String,
    cell: String,
}

/// Registered once per process. `source` is a dataset or limiter name and `cell` a deployed cell, so both stay small;
/// namespaces and keys never appear here.
#[derive(Clone)]
pub struct Metrics {
    coalesced: Family<SourceLabels, Counter>,
    rejected: Family<CellLabels, Counter>,
    queued: Family<SourceLabels, Gauge>,
    running: Family<SourceLabels, Gauge>,
}

impl Metrics {
    pub fn new(admin: &Admin) -> Self {
        let metrics = Metrics {
            coalesced: Family::default(),
            rejected: Family::default(),
            queued: Family::default(),
            running: Family::default(),
        };
        admin.register(
            "read_coalesced",
            "Requests answered by another caller's upstream operation",
            metrics.coalesced.clone(),
        );
        admin.register(
            "read_limit_rejected",
            "Requests rejected as overloaded, by source and cell",
            metrics.rejected.clone(),
        );
        admin.register(
            "read_limit_queued",
            "Admitted requests waiting for capacity",
            metrics.queued.clone(),
        );
        admin.register(
            "read_limit_running",
            "Operations running against a source",
            metrics.running.clone(),
        );
        metrics
    }
}

/// Shares one upstream operation between identical concurrent requests.
///
/// The first caller for a key leads: it runs the operation on its own task. The rest wait for its answer. If the leader
/// is cancelled, say its client disconnected, one waiter takes over, so nobody is stranded and nothing runs detached.
// ponytail: one lock per coalescer, taken on every read; shard by key if it shows up in a profile.
// ponytail: waiters are not capped; each is a caller already holding a request, and costs the source nothing.
pub struct Coalescer<Key: Ord, Value> {
    flights: Mutex<BTreeMap<Key, watch::Receiver<Option<Value>>>>,
    coalesced: Counter,
}

/// Ends a flight when its leader finishes or is cancelled.
struct Leading<'a, Key: Ord, Value> {
    coalescer: &'a Coalescer<Key, Value>,
    key: Key,
}

impl<Key: Ord, Value> Drop for Leading<'_, Key, Value> {
    fn drop(&mut self) {
        self.coalescer.flights.lock().unwrap().remove(&self.key);
    }
}

impl<Key: Ord + Clone, Value: Clone> Coalescer<Key, Value> {
    /// `source` labels the coalesced counter.
    pub fn new(source: &str, metrics: &Metrics) -> Self {
        let labels = SourceLabels {
            source: source.to_owned(),
        };
        Coalescer {
            flights: Mutex::default(),
            coalesced: metrics.coalesced.get_or_create(&labels).clone(),
        }
    }

    /// Runs `fetch`, or waits for the flight already running for `key`. A waiter keeps a finished flight's answer only
    /// if `accept` says it applies to this caller; otherwise it tries again, leading if nobody else is.
    pub async fn run<F: Future<Output = Value>>(
        &self,
        key: Key,
        accept: impl Fn(&Value) -> bool,
        fetch: impl FnOnce() -> F,
    ) -> Value {
        let sender = loop {
            let mut flight = match self.flights.lock().unwrap().entry(key.clone()) {
                Entry::Occupied(flight) => flight.get().clone(),
                Entry::Vacant(free) => {
                    let (sender, receiver) = watch::channel(None);
                    free.insert(receiver);
                    break sender;
                }
            };
            // An error means the leader was cancelled.
            let landed = flight
                .wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|value| (*value).clone());
            if let Some(value) = landed.filter(&accept) {
                self.coalesced.inc();
                return value;
            }
        };
        // Dropped before `sender` on cancellation, so waiters that wake find the flight gone.
        let leading = Leading {
            coalescer: self,
            key,
        };
        let value = fetch().await;
        drop(leading);
        sender.send_replace(Some(value.clone()));
        value
    }
}

/// Decrements its gauge when dropped, including when a waiting caller is cancelled.
struct Counted(Gauge);

impl Counted {
    fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Counted(gauge)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// One cell's share of a source. `admitted` covers running and queued requests, `running` only those past the queue.
struct Lane {
    admitted: Arc<Semaphore>,
    running: Arc<Semaphore>,
}

/// Holds a place in a [`Limiter`] until dropped.
pub struct Permit {
    _running: Counted,
    _places: [OwnedSemaphorePermit; 3],
}

/// Bounded concurrency, queues and per-cell fairness for one source on one node.
pub struct Limiter {
    source: String,
    limits: Limits,
    total: Arc<Semaphore>,
    // ponytail: lanes are never removed; there is one per deployed cell, plus any background lanes callers name.
    lanes: Mutex<BTreeMap<String, Lane>>,
    metrics: Metrics,
    queued: Gauge,
    running: Gauge,
}

impl Limiter {
    pub fn new(source: &str, limits: Limits, metrics: &Metrics) -> Self {
        let labels = SourceLabels {
            source: source.to_owned(),
        };
        Limiter {
            source: source.to_owned(),
            limits,
            total: Arc::new(Semaphore::new(limits.total)),
            lanes: Mutex::default(),
            metrics: metrics.clone(),
            queued: metrics.queued.get_or_create(&labels).clone(),
            running: metrics.running.get_or_create(&labels).clone(),
        }
    }

    /// Waits for capacity, or fails at once with [`StorageError::Overloaded`] when `cell`'s queue is full.
    ///
    /// `cell` is normally the caller's cell. Background work names its own lane, e.g. `cell-a/warm` for a new range
    /// owner warming artifacts, so it cannot fill the queue its readers need.
    pub async fn acquire(&self, cell: &str) -> StorageResult<Permit> {
        let (admitted, running) = {
            let mut lanes = self.lanes.lock().unwrap();
            let lane = lanes.entry(cell.to_owned()).or_insert_with(|| Lane {
                admitted: Arc::new(Semaphore::new(self.limits.per_cell + self.limits.queue)),
                running: Arc::new(Semaphore::new(self.limits.per_cell)),
            });
            (lane.admitted.clone(), lane.running.clone())
        };
        let Ok(admitted) = admitted.try_acquire_owned() else {
            let labels = CellLabels {
                source: self.source.clone(),
                cell: cell.to_owned(),
            };
            self.metrics.rejected.get_or_create(&labels).inc();
            return Err(StorageError::Overloaded(ErrorContext {
                engine: "limiter",
                instance: self.source.clone(),
                file: None,
                partition: Some(format!("cell {cell}")),
                detail: "queue is full; retry with backoff".into(),
            }));
        };
        let waiting = Counted::new(self.queued.clone());
        let running = running.acquire_owned().await;
        let total = self.total.clone().acquire_owned().await;
        drop(waiting);
        Ok(Permit {
            _running: Counted::new(self.running.clone()),
            _places: [
                admitted,
                running.expect("limiter semaphores are never closed"),
                total.expect("limiter semaphores are never closed"),
            ],
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/health/protection.rs"]
mod tests;
