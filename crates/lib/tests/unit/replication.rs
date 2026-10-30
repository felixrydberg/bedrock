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
