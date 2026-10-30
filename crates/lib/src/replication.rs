//! Shared replication event format and the ordered, duplicate-safe applier (B-08, `kv.md` §8, §17).
//!
//! Every dataset, including `cdn` retention, replicates the same [`Event`]s. Operations are only `SET` and `DELETE`;
//! what an event means locally is up to the dataset's [`ApplyHook`], so writers and relays stay generic.
//! [`writer`] assigns sequences and commits events (B-09); [`subscriptions`] carries them from a source's log to an
//! applier (B-10).

pub mod materialized;
pub mod subscriptions;
pub mod writer;

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

use crate::storage::{StorageEngine, StorageError, StorageResult, WriteBatch};
use crate::transport::admin::Admin;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub dataset: String,
    pub namespace: String,
    /// The road the event travels on, see [`stream`]. Transport only: a node merges its streams, and ordering,
    /// gap detection and resume all use `sequence`.
    pub stream: String,
    /// Orders authoritative changes within the namespace, starting at 1. The only replication counter.
    pub sequence: u64,
    /// Writer epoch. Fencing belongs to the write path; appliers pass it on unchanged.
    pub epoch: u64,
    /// Commit time in milliseconds since the Unix epoch. Set once by `bedrock-api`; every hop passes it on unchanged
    /// and reports `now - committed_at` as end-to-end lag.
    pub committed_at: u64,
    pub key: Vec<u8>,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Set(Vec<u8>),
    Delete,
}

/// Road `partition` of `dataset`'s stream pool, e.g. `kv-0`. Datasets never share a stream.
pub fn stream(dataset: &str, partition: u32) -> String {
    format!("{dataset}-{partition}")
}

/// How one dataset turns events into local state, e.g. KV keyspaces or CDN reference counts.
pub trait ApplyHook {
    /// Adds `event`'s changes to `batch`. Called once per event, in sequence order; `engine` still holds the state
    /// from before the event. The batch commits together with the namespace's applied sequence, or not at all.
    fn apply(
        &mut self,
        engine: &dyn StorageEngine,
        event: &Event,
        batch: &mut WriteBatch,
    ) -> StorageResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Already applied or already buffered.
    Duplicate,
    /// The namespace's applied sequence after this event and any buffered events it unblocked.
    Applied(u64),
    /// Buffered events wait for the `missing` sequences, which the caller requests from a relay or the writer.
    Gap { applied: u64, missing: Range<u64> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// The event is malformed or belongs to another dataset. Nothing was applied.
    Rejected(String),
    /// Nothing of the failing event was committed. It stays buffered and the next delivery retries it.
    Storage(StorageError),
}

impl From<StorageError> for ApplyError {
    fn from(error: StorageError) -> Self {
        ApplyError::Storage(error)
    }
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::Rejected(reason) => write!(f, "event rejected: {reason}"),
            ApplyError::Storage(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ApplyError {}

/// Backlog gauges for every applier on the node, labelled only by dataset (observability policy). Per-namespace
/// detail is [`Applier::backlog`]. A node falling behind shows as buffered events that do not drain.
#[derive(Clone, Default)]
pub struct ApplierMetrics {
    pending_events: Family<DatasetLabel, Gauge>,
    waiting_namespaces: Family<DatasetLabel, Gauge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct DatasetLabel {
    dataset: String,
}

impl ApplierMetrics {
    pub fn new(admin: &Admin) -> Self {
        let metrics = ApplierMetrics::default();
        admin.register(
            "replication_pending_events",
            "Events buffered until the sequences before them are applied",
            metrics.pending_events.clone(),
        );
        admin.register(
            "replication_waiting_namespaces",
            "Namespaces with buffered events they cannot apply yet",
            metrics.waiting_namespaces.clone(),
        );
        metrics
    }
}

/// Applies one dataset's events to its engine instance in sequence order, each at most once.
///
/// Each namespace's applied sequence commits in the same batch as the data, so after a crash both are there or
/// neither is, and a new applier resumes from the engine. Buffered events are lost on a crash; catch-up delivers
/// them again.
pub struct Applier<H> {
    dataset: String,
    engine: Arc<dyn StorageEngine>,
    hook: H,
    namespaces: BTreeMap<String, Progress>,
    pending_events: Gauge,
    waiting_namespaces: Gauge,
}

/// Out-of-order events buffered per namespace. One that does not fit is dropped and reported as a gap, so the
/// subscriber requests it again (B-10).
pub const MAX_PENDING: usize = 4096;

struct Progress {
    applied: u64,
    pending: BTreeMap<u64, Event>,
}

impl<H: ApplyHook> Applier<H> {
    pub fn new(
        dataset: impl Into<String>,
        engine: Arc<dyn StorageEngine>,
        hook: H,
        metrics: &ApplierMetrics,
    ) -> Self {
        let label = DatasetLabel {
            dataset: dataset.into(),
        };
        Applier {
            pending_events: metrics.pending_events.get_or_create(&label).clone(),
            waiting_namespaces: metrics.waiting_namespaces.get_or_create(&label).clone(),
            dataset: label.dataset,
            engine,
            hook,
            namespaces: BTreeMap::new(),
        }
    }

    /// Applies `event` and every buffered event it unblocks, or buffers it until the sequences before it arrive.
    pub fn offer(&mut self, event: Event) -> Result<Outcome, ApplyError> {
        let namespace = event.namespace.clone();
        let before = self.buffered(&namespace);
        let outcome = self.apply_in_order(event);
        let after = self.buffered(&namespace);
        self.pending_events.inc_by(after as i64 - before as i64);
        self.waiting_namespaces
            .inc_by(i64::from(after > 0) - i64::from(before > 0));
        outcome
    }

    /// Buffered event count per namespace that has any. Namespaces are too many for metric labels, so this feeds
    /// introspection instead.
    pub fn backlog(&self) -> impl Iterator<Item = (&str, usize)> {
        self.namespaces
            .iter()
            .filter(|(_, progress)| !progress.pending.is_empty())
            .map(|(namespace, progress)| (namespace.as_str(), progress.pending.len()))
    }

    fn buffered(&self, namespace: &str) -> usize {
        self.namespaces
            .get(namespace)
            .map_or(0, |progress| progress.pending.len())
    }

    fn apply_in_order(&mut self, event: Event) -> Result<Outcome, ApplyError> {
        if event.dataset != self.dataset {
            return Err(ApplyError::Rejected(format!(
                "event for dataset {:?} offered to the {:?} applier",
                event.dataset, self.dataset
            )));
        }
        if event.namespace.contains('\0') {
            return Err(ApplyError::Rejected("namespace ID contains NUL".into()));
        }
        if event.sequence == 0 {
            return Err(ApplyError::Rejected("sequences start at 1".into()));
        }

        let Applier {
            engine,
            hook,
            namespaces,
            ..
        } = self;
        let progress = match namespaces.entry(event.namespace.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(Progress {
                applied: engine.revision(&event.namespace)?.unwrap_or(0),
                pending: BTreeMap::new(),
            }),
        };
        let sequence = event.sequence;
        if sequence <= progress.applied {
            return Ok(Outcome::Duplicate);
        }
        // A redelivered buffered event keeps the buffered copy but still drains, so it retries a failed commit.
        let redelivered = progress.pending.contains_key(&sequence);
        if !redelivered && sequence != progress.applied + 1 && progress.pending.len() >= MAX_PENDING
        {
            let first = progress
                .pending
                .first_key_value()
                .map_or(sequence, |(&s, _)| s);
            return Ok(Outcome::Gap {
                applied: progress.applied,
                missing: progress.applied + 1..first.min(sequence),
            });
        }
        progress.pending.entry(sequence).or_insert(event);
        let applied_before = progress.applied;

        while let Some(entry) = progress.pending.first_entry()
            && *entry.key() == progress.applied + 1
        {
            let event = entry.remove();
            let mut batch = WriteBatch::default();
            let committed = hook
                .apply(engine.as_ref(), &event, &mut batch)
                .and_then(|()| {
                    batch.set_revision(event.namespace.as_str(), event.sequence);
                    engine.write(batch)
                });
            if let Err(error) = committed {
                progress.pending.insert(event.sequence, event);
                return Err(error.into());
            }
            progress.applied += 1;
        }

        if redelivered && progress.applied == applied_before {
            return Ok(Outcome::Duplicate);
        }
        Ok(match progress.pending.first_key_value() {
            None => Outcome::Applied(progress.applied),
            Some((&next, _)) => Outcome::Gap {
                applied: progress.applied,
                missing: progress.applied + 1..next,
            },
        })
    }

    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// The last sequence applied to `namespace`, 0 if none.
    pub fn applied(&self, namespace: &str) -> StorageResult<u64> {
        match self.namespaces.get(namespace) {
            Some(progress) => Ok(progress.applied),
            None => Ok(self.engine.revision(namespace)?.unwrap_or(0)),
        }
    }
}

// A replaced applier's buffer is gone, so it leaves the gauges.
impl<H> Drop for Applier<H> {
    fn drop(&mut self) {
        let waiting = self.namespaces.values().filter(|p| !p.pending.is_empty());
        let (namespaces, events) = waiting.fold((0, 0), |(n, e), p| (n + 1, e + p.pending.len()));
        self.pending_events.dec_by(events as i64);
        self.waiting_namespaces.dec_by(namespaces);
    }
}

/// The latest sequence per `(dataset, namespace)` that this node has seen. Every node (API, replica,
/// relay) keeps one in memory for gap detection, catch-up requests, resuming a subscription (the heads exchange,
/// B-10) and the API's head after a restart.
#[derive(Debug, Default)]
pub struct Heads {
    sequences: BTreeMap<(String, String), u64>,
}

impl Heads {
    pub fn observe(&mut self, event: &Event) {
        let sequence = self
            .sequences
            .entry((event.dataset.clone(), event.namespace.clone()))
            .or_default();
        *sequence = (*sequence).max(event.sequence);
    }

    pub fn sequence(&self, dataset: &str, namespace: &str) -> Option<u64> {
        self.sequences
            .get(&(dataset.to_owned(), namespace.to_owned()))
            .copied()
    }
}

#[cfg(test)]
#[path = "../tests/unit/replication.rs"]
mod tests;
