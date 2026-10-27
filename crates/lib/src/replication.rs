//! Shared replication event format and the ordered, duplicate-safe applier (B-08, `kv.md` §8, §17).
//!
//! Every dataset, including `cdn` retention, replicates the same [`Event`]s. Operations are only `SET` and `DELETE`;
//! what an event means locally is up to the dataset's [`ApplyHook`], so writers and relays stay generic.
//! [`writer`] assigns sequences and commits events (B-09); [`subscriptions`] carries them from a source's log to an
//! applier (B-10).

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
mod tests {
    use super::*;
    use crate::storage::memory::MemoryEngine;
    use crate::storage::{ErrorContext, KeyRange};

    /// splitmix64: seeded, so a failing seed replays exactly.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn shuffle<T>(&mut self, items: &mut [T]) {
            for i in (1..items.len()).rev() {
                items.swap(i, self.below(i + 1));
            }
        }
    }

    /// Stores each key's value under `{namespace}\0{key}`; a delete removes it. With `fail_percent`, some calls fail
    /// after filling the batch, as if the node crashed before committing it.
    struct Keyspace {
        rng: Rng,
        fail_percent: usize,
    }

    fn keyspace() -> Keyspace {
        Keyspace {
            rng: Rng(0),
            fail_percent: 0,
        }
    }

    impl ApplyHook for Keyspace {
        fn apply(
            &mut self,
            _: &dyn StorageEngine,
            event: &Event,
            batch: &mut WriteBatch,
        ) -> StorageResult<()> {
            let key = [event.namespace.as_bytes(), b"\0", &event.key].concat();
            match &event.operation {
                Operation::Set(value) => batch.put(key, value.clone()),
                Operation::Delete => batch.delete(key),
            };
            if self.rng.below(100) < self.fail_percent {
                return Err(StorageError::IoError(ErrorContext {
                    engine: "test",
                    instance: "kv".into(),
                    file: None,
                    partition: None,
                    detail: "crashed before commit".into(),
                }));
            }
            Ok(())
        }
    }

    fn new_applier<H: ApplyHook>(engine: Arc<dyn StorageEngine>, hook: H) -> Applier<H> {
        Applier::new("kv", engine, hook, &ApplierMetrics::default())
    }

    fn ev(namespace: &str, sequence: u64) -> Event {
        Event {
            dataset: "kv".into(),
            namespace: namespace.into(),
            stream: stream("kv", 0),
            sequence,
            epoch: 1,
            committed_at: 1_700_000_000_000 + sequence,
            key: b"k".to_vec(),
            operation: Operation::Set(sequence.to_be_bytes().to_vec()),
        }
    }

    /// Sets and deletes over a few keys in three namespaces, in writer order.
    fn history(rng: &mut Rng) -> Vec<Event> {
        let mut heads = Heads::default();
        (1..=60)
            .map(|_| {
                let namespace = ["a", "b", "c"][rng.below(3)];
                let sequence = heads.sequence("kv", namespace).unwrap_or(0) + 1;
                let mut e = ev(namespace, sequence);
                e.key = vec![b'x' + rng.below(3) as u8];
                if rng.below(4) == 0 {
                    e.operation = Operation::Delete;
                }
                heads.observe(&e);
                e
            })
            .collect()
    }

    /// `history` plus random duplicates, shuffled.
    fn delivery(rng: &mut Rng, history: &[Event]) -> Vec<Event> {
        let mut events = history.to_vec();
        for _ in 0..rng.below(history.len()) {
            events.push(history[rng.below(history.len())].clone());
        }
        rng.shuffle(&mut events);
        events
    }

    fn state(engine: &MemoryEngine) -> BTreeMap<Vec<u8>, Vec<u8>> {
        engine
            .scan(&KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn revisions(engine: &MemoryEngine) -> BTreeMap<String, u64> {
        engine.snapshot().unwrap().revisions().unwrap()
    }

    /// The state after applying `history` in order, up to each namespace's sequence in `through`.
    fn in_order(history: &[Event], through: &BTreeMap<String, u64>) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut hook = keyspace();
        let model = MemoryEngine::new("model");
        for e in history {
            if e.sequence <= through.get(&e.namespace).copied().unwrap_or(0) {
                let mut batch = WriteBatch::default();
                hook.apply(&model, e, &mut batch).unwrap();
                model.write(batch).unwrap();
            }
        }
        state(&model)
    }

    fn heads(history: &[Event]) -> BTreeMap<String, u64> {
        history
            .iter()
            .map(|e| (e.namespace.clone(), e.sequence))
            .collect()
    }

    #[test]
    fn shuffled_and_duplicated_events_apply_as_in_order() {
        for seed in 0..500 {
            let mut rng = Rng(seed);
            let history = history(&mut rng);
            let engine = Arc::new(MemoryEngine::new("kv"));
            let mut applier = new_applier(engine.clone(), keyspace());
            for e in delivery(&mut rng, &history) {
                applier.offer(e).unwrap();
            }
            assert_eq!(revisions(&engine), heads(&history), "seed {seed}");
            assert_eq!(
                state(&engine),
                in_order(&history, &heads(&history)),
                "seed {seed}"
            );
        }
    }

    #[test]
    fn crashes_never_split_data_and_revision() {
        for seed in 0..300 {
            let mut rng = Rng(seed);
            let history = history(&mut rng);
            let engine = Arc::new(MemoryEngine::new("kv"));
            let crashing = |seed| Keyspace {
                rng: Rng(seed),
                fail_percent: 20,
            };
            let mut applier = new_applier(engine.clone(), crashing(seed));
            for (i, e) in delivery(&mut rng, &history).into_iter().enumerate() {
                if rng.below(10) == 0 {
                    // Restart: buffered events are lost, progress comes back from the engine.
                    applier = new_applier(engine.clone(), crashing(seed + i as u64));
                }
                if let Err(error) = applier.offer(e) {
                    assert!(matches!(error, ApplyError::Storage(_)), "seed {seed}");
                }
                let applied = revisions(&engine);
                assert_eq!(state(&engine), in_order(&history, &applied), "seed {seed}");
            }

            // Catch-up delivers everything again.
            let mut applier = new_applier(engine.clone(), keyspace());
            for e in history.clone() {
                applier.offer(e).unwrap();
            }
            assert_eq!(revisions(&engine), heads(&history), "seed {seed}");
            assert_eq!(
                state(&engine),
                in_order(&history, &heads(&history)),
                "seed {seed}"
            );
        }
    }

    #[test]
    fn gaps_are_reported_buffered_and_filled() {
        let engine = Arc::new(MemoryEngine::new("kv"));
        let mut applier = new_applier(engine.clone(), keyspace());
        assert_eq!(applier.offer(ev("ns", 1)), Ok(Outcome::Applied(1)));
        let gap = |applied, missing| Ok(Outcome::Gap { applied, missing });
        assert_eq!(applier.offer(ev("ns", 4)), gap(1, 2..4));
        assert_eq!(applier.offer(ev("ns", 4)), Ok(Outcome::Duplicate));
        assert_eq!(applier.offer(ev("ns", 2)), gap(2, 3..4));
        assert_eq!(applier.offer(ev("other", 1)), Ok(Outcome::Applied(1)));
        assert_eq!(applier.offer(ev("ns", 3)), Ok(Outcome::Applied(4)));
        assert_eq!(applier.offer(ev("ns", 2)), Ok(Outcome::Duplicate));

        let applier = new_applier(engine.clone(), keyspace());
        assert_eq!(applier.applied("ns"), Ok(4));
        assert_eq!(applier.applied("unknown"), Ok(0));
    }

    #[test]
    fn full_buffer_drops_and_redelivery_fills() {
        let engine = Arc::new(MemoryEngine::new("kv"));
        let mut applier = new_applier(engine.clone(), keyspace());
        let last = MAX_PENDING as u64 + 1;
        for sequence in 2..=last {
            applier.offer(ev("ns", sequence)).unwrap();
        }
        let dropped = applier.offer(ev("ns", last + 1));
        assert_eq!(
            dropped,
            Ok(Outcome::Gap {
                applied: 0,
                missing: 1..2
            })
        );
        assert_eq!(applier.offer(ev("ns", 1)), Ok(Outcome::Applied(last)));
        assert_eq!(
            applier.offer(ev("ns", last + 1)),
            Ok(Outcome::Applied(last + 1))
        );
    }

    #[test]
    fn redelivery_retries_a_failed_commit() {
        /// Fails its first call, then behaves like `keyspace()`.
        struct FailOnce(bool, Keyspace);

        impl ApplyHook for FailOnce {
            fn apply(
                &mut self,
                engine: &dyn StorageEngine,
                e: &Event,
                batch: &mut WriteBatch,
            ) -> StorageResult<()> {
                if !std::mem::replace(&mut self.0, true) {
                    return Err(StorageError::IoError(ErrorContext {
                        engine: "test",
                        instance: "kv".into(),
                        file: None,
                        partition: None,
                        detail: "transient".into(),
                    }));
                }
                self.1.apply(engine, e, batch)
            }
        }

        let engine = Arc::new(MemoryEngine::new("kv"));
        let mut applier = new_applier(engine.clone(), FailOnce(false, keyspace()));
        assert!(matches!(
            applier.offer(ev("ns", 1)),
            Err(ApplyError::Storage(_))
        ));
        assert_eq!(applier.backlog().collect::<Vec<_>>(), [("ns", 1)]);
        assert_eq!(applier.offer(ev("ns", 1)), Ok(Outcome::Applied(1)));
        assert_eq!(revisions(&engine), BTreeMap::from([("ns".into(), 1)]));
        assert_eq!(applier.backlog().count(), 0);
    }

    #[test]
    fn backlog_gauges_follow_the_buffer() {
        let metrics = ApplierMetrics::default();
        let gauges = |metrics: &ApplierMetrics| {
            let label = DatasetLabel {
                dataset: "kv".into(),
            };
            (
                metrics.pending_events.get_or_create(&label).get(),
                metrics.waiting_namespaces.get_or_create(&label).get(),
            )
        };
        let engine = Arc::new(MemoryEngine::new("kv"));
        let mut applier = Applier::new("kv", engine, keyspace(), &metrics);
        applier.offer(ev("ns", 3)).unwrap();
        applier.offer(ev("ns", 2)).unwrap();
        applier.offer(ev("other", 2)).unwrap();
        assert_eq!(gauges(&metrics), (3, 2));
        assert_eq!(
            applier.backlog().collect::<Vec<_>>(),
            [("ns", 2), ("other", 1)]
        );

        applier.offer(ev("ns", 1)).unwrap();
        assert_eq!(gauges(&metrics), (1, 1));
        assert_eq!(applier.backlog().collect::<Vec<_>>(), [("other", 1)]);

        drop(applier);
        assert_eq!(gauges(&metrics), (0, 0));
    }

    #[test]
    fn committed_at_reaches_the_hook_unchanged() {
        #[derive(Default)]
        struct Recorder(Vec<Event>);

        impl ApplyHook for Recorder {
            fn apply(
                &mut self,
                _: &dyn StorageEngine,
                e: &Event,
                _: &mut WriteBatch,
            ) -> StorageResult<()> {
                self.0.push(e.clone());
                Ok(())
            }
        }

        let written: Vec<_> = (1..=3).map(|r| ev("ns", r)).collect();
        let mut applier = new_applier(Arc::new(MemoryEngine::new("kv")), Recorder::default());
        for i in [2, 0, 1] {
            applier.offer(written[i].clone()).unwrap();
        }
        assert_eq!(applier.hook.0, written);
    }

    #[test]
    fn rejects_foreign_and_malformed_events() {
        let engine = Arc::new(MemoryEngine::new("kv"));
        let mut applier = new_applier(engine.clone(), keyspace());
        let mut cdn = ev("ns", 1);
        cdn.dataset = "cdn".into();
        for e in [cdn, ev("a\0b", 1), ev("ns", 0)] {
            assert!(matches!(applier.offer(e), Err(ApplyError::Rejected(_))));
        }
        assert!(state(&engine).is_empty() && revisions(&engine).is_empty());
    }

    #[test]
    fn heads_keep_the_latest_seen() {
        let mut heads = Heads::default();
        heads.observe(&ev("ns", 5));
        heads.observe(&ev("ns", 3));
        assert_eq!(heads.sequence("kv", "ns"), Some(5));
        assert_eq!(heads.sequence("kv", "other"), None);
        assert_eq!(heads.sequence("cdn", "ns"), None);
    }
}
