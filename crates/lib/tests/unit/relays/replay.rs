use super::*;
use crate::replication::Operation;
use crate::storage::memory::MemoryEngine;

fn event(dataset: &str, namespace: &str, sequence: u64) -> Event {
    Event {
        dataset: dataset.into(),
        namespace: namespace.into(),
        stream: format!("{dataset}-0"),
        sequence,
        epoch: 4,
        committed_at: 12,
        key: b"key".to_vec(),
        operation: Operation::Set(b"value".to_vec()),
    }
}

#[test]
fn restart_validates_holes_and_keeps_dataset_namespace_isolation() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    for revision in [1, 2, 4] {
        replay.append(&event("kv", "a/b", revision), 30).unwrap();
    }
    replay.append(&event("kv/a", "b", 1), 30).unwrap();
    replay.append(&event("cdn", "a/b", 7), 30).unwrap();
    drop(replay);
    let replay = Replay::open(engine, ReplayConfig::default()).unwrap();
    assert_eq!(replay.ranges("kv", "a/b"), [(1, 2), (4, 4)]);
    assert_eq!(replay.ranges("cdn", "a/b"), [(7, 7)]);
    assert_eq!(replay.ranges("kv/a", "b"), [(1, 1)]);
    assert_eq!(
        replay.read("kv", "a/b", 1, 10).unwrap(),
        [event("kv", "a/b", 1), event("kv", "a/b", 2)]
    );
    assert!(replay.read("kv", "a/b", 3, 10).unwrap().is_empty());
}

#[test]
fn duplicate_does_not_extend_window_and_conflict_disables_coverage() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let replay = Replay::open(
        engine,
        ReplayConfig {
            window_ms: 10,
            ..ReplayConfig::default()
        },
    )
    .unwrap();
    let original = event("kv", "n", 1);
    replay.append(&original, 0).unwrap();
    replay.append(&original, 9).unwrap();
    replay.append(&event("kv", "n", 2), 11).unwrap();
    assert_eq!(replay.ranges("kv", "n"), [(2, 2)]);
    let mut conflict = event("kv", "n", 2);
    conflict.committed_at += 1;
    assert!(matches!(
        replay.append(&conflict, 12),
        Err(StorageError::Corruption(_))
    ));
    assert!(!replay.healthy());
    assert!(replay.ranges("kv", "n").is_empty());
}

#[test]
fn capacity_evicts_low_priority_then_oldest_and_persists_eviction() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let retained = event("hi", "n", 1);
    let bytes = (key(&identity(&retained)).len() + encode(&retained, 0).len()) as u64;
    let config = ReplayConfig {
        max_bytes: bytes * 2,
        priorities: [("hi".into(), 1)].into(),
        ..ReplayConfig::default()
    };
    let replay = Replay::open(engine.clone(), config.clone()).unwrap();
    replay.append(&retained, 0).unwrap();
    replay.append(&event("lo", "n", 1), 1).unwrap();
    replay.append(&event("lo", "n", 2), 2).unwrap();
    assert_eq!(replay.ranges("hi", "n"), [(1, 1)]);
    assert_eq!(replay.ranges("lo", "n"), [(2, 2)]);
    drop(replay);
    let replay = Replay::open(engine, config).unwrap();
    assert_eq!(replay.ranges("lo", "n"), [(2, 2)]);
}

#[test]
fn checksum_and_identity_are_verified_before_recovery_or_delivery() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let original = event("kv", "n", 1);
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    replay.append(&original, 0).unwrap();
    let record_key = key(&identity(&original));
    let mut value = engine.get(&record_key).unwrap().unwrap();
    value[0] ^= 1;
    engine.put(&record_key, &value).unwrap();
    assert!(matches!(
        replay.read("kv", "n", 1, 1),
        Err(StorageError::Corruption(_))
    ));
    assert!(replay.ranges("kv", "n").is_empty());
    assert!(matches!(
        Replay::open(engine.clone(), ReplayConfig::default()),
        Err(StorageError::Corruption(_))
    ));
    engine
        .put(&record_key, &encode(&event("kv", "n", 2), 0))
        .unwrap();
    assert!(matches!(
        Replay::open(engine, ReplayConfig::default()),
        Err(StorageError::Corruption(_))
    ));
}

#[test]
fn adapter_corruption_disables_only_this_cache() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    let original = event("kv", "n", 1);
    replay.append(&original, 0).unwrap();
    engine.corrupt(&key(&identity(&original)));
    assert!(replay.read("kv", "n", 1, 10).is_err());
    assert!(!replay.healthy());
    assert!(replay.ranges("kv", "n").is_empty());
    assert!(replay.append(&event("kv", "n", 2), 1).is_err());
    engine.put(b"unrelated", b"still usable").unwrap();
    assert_eq!(
        engine.get(b"unrelated").unwrap(),
        Some(b"still usable".to_vec())
    );
}

#[test]
fn reduced_capacity_is_enforced_before_restart_advertises() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    replay.append(&event("kv", "n", 1), 0).unwrap();
    drop(replay);
    let replay = Replay::open(
        engine.clone(),
        ReplayConfig {
            max_bytes: 0,
            ..ReplayConfig::default()
        },
    )
    .unwrap();
    assert!(replay.ranges("kv", "n").is_empty());
    assert!(
        engine
            .get(&key(&identity(&event("kv", "n", 1))))
            .unwrap()
            .is_none()
    );
}

#[test]
fn idle_prune_and_oversized_event_keep_indexes_and_byte_count_consistent() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    let original = event("kv", "n", 1);
    let size = (key(&identity(&original)).len() + encode(&original, 0).len()) as u64;
    let replay = Replay::open(
        engine,
        ReplayConfig {
            max_bytes: size,
            window_ms: 10,
            ..ReplayConfig::default()
        },
    )
    .unwrap();
    replay.append(&original, 0).unwrap();
    assert_eq!(replay.bytes(), size);
    let mut oversized = event("kv", "n", 2);
    oversized.operation = Operation::Set(vec![0; 1000]);
    replay.append(&oversized, 1).unwrap();
    assert!(replay.ranges("kv", "n").is_empty());
    assert_eq!(replay.bytes(), 0);
    replay.append(&event("kv", "n", 3), 2).unwrap();
    replay.prune(13).unwrap();
    assert!(replay.ranges("kv", "n").is_empty());
    assert_eq!(replay.bytes(), 0);
    assert!(replay.healthy());
}

#[test]
fn rebuild_discards_corrupt_replay_preserves_other_data_and_accepts_new_history() {
    let engine = Arc::new(MemoryEngine::new("replay"));
    engine.put(b"other-dataset", b"preserved").unwrap();
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    let original = event("auth", "n", 1);
    replay.append(&original, 0).unwrap();
    let record_key = key(&identity(&original));
    let mut damaged = engine.get(&record_key).unwrap().unwrap();
    damaged[8] ^= 1;
    engine.put(&record_key, &damaged).unwrap();
    assert!(matches!(
        replay.read("auth", "n", 1, 1),
        Err(StorageError::Corruption(_))
    ));
    assert!(!replay.healthy());
    replay.rebuild().unwrap();
    assert!(replay.healthy());
    assert_eq!(replay.bytes(), 0);
    assert!(replay.ranges("auth", "n").is_empty());
    assert_eq!(
        engine.get(b"other-dataset").unwrap(),
        Some(b"preserved".to_vec())
    );
    let next = event("auth", "n", 2);
    replay.append(&next, 1).unwrap();
    assert_eq!(replay.ranges("auth", "n"), [(2, 2)]);
    assert_eq!(replay.read("auth", "n", 2, 1).unwrap(), [next]);
    let reopened = Replay::open(engine, ReplayConfig::default()).unwrap();
    assert_eq!(reopened.ranges("auth", "n"), [(2, 2)]);
}

struct RebuildFault {
    inner: MemoryEngine,
    fail_write: std::sync::atomic::AtomicBool,
    fail_health: std::sync::atomic::AtomicBool,
    writes: std::sync::atomic::AtomicUsize,
}

impl StorageEngine for RebuildFault {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn scan(&self, range: &KeyRange) -> StorageResult<crate::storage::Scan<'_>> {
        self.inner.scan(range)
    }
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.fail_write.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StorageError::Unavailable(context(
                "injected rebuild write failure",
            )));
        }
        self.inner.write(batch)
    }
    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.inner.revision(name)
    }
    fn snapshot(&self) -> StorageResult<Box<dyn crate::storage::Snapshot + '_>> {
        self.inner.snapshot()
    }
    fn restore(&self, from: &dyn crate::storage::Snapshot) -> StorageResult<()> {
        self.inner.restore(from)
    }
    fn stats(&self) -> StorageResult<crate::storage::StorageStats> {
        self.inner.stats()
    }
    fn disk_usage(&self) -> StorageResult<u64> {
        self.inner.disk_usage()
    }
    fn health(&self) -> StorageResult<()> {
        if self.fail_health.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StorageError::Unavailable(context(
                "injected rebuild health failure",
            )));
        }
        self.inner.health()
    }
}

#[test]
fn failed_rebuild_write_or_health_keeps_cache_disabled_until_success() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let engine = Arc::new(RebuildFault {
        inner: MemoryEngine::new("replay"),
        fail_write: AtomicBool::new(false),
        fail_health: AtomicBool::new(false),
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    replay.append(&event("auth", "n", 1), 0).unwrap();
    let retained_bytes = replay.bytes();
    engine.fail_write.store(true, Ordering::Relaxed);
    assert!(replay.rebuild().is_err());
    assert!(!replay.healthy());
    assert_eq!(replay.bytes(), retained_bytes);
    assert!(replay.ranges("auth", "n").is_empty());
    engine.fail_write.store(false, Ordering::Relaxed);
    engine.fail_health.store(true, Ordering::Relaxed);
    assert!(replay.rebuild().is_err());
    assert!(!replay.healthy());
    assert_eq!(replay.bytes(), retained_bytes);
    engine.fail_health.store(false, Ordering::Relaxed);
    // Successful prefix deletion and health clear the old failure together.
    replay.rebuild().unwrap();
    assert!(replay.healthy());
    assert_eq!(replay.bytes(), 0);
    replay.append(&event("auth", "n", 2), 1).unwrap();
    assert_eq!(replay.ranges("auth", "n"), [(2, 2)]);
}

fn batch_engine() -> Arc<RebuildFault> {
    Arc::new(RebuildFault {
        inner: MemoryEngine::new("batch"),
        fail_write: std::sync::atomic::AtomicBool::new(false),
        fail_health: std::sync::atomic::AtomicBool::new(false),
        writes: std::sync::atomic::AtomicUsize::new(0),
    })
}

#[test]
fn batch_commits_once_deduplicates_and_recovers_all_ranges() {
    use std::sync::atomic::Ordering;
    let engine = batch_engine();
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    let batch = [
        event("auth", "n", 1),
        event("auth", "n", 2),
        event("auth", "n", 1),
        event("cdn", "n", 5),
    ];
    replay.append_batch(&batch, 0).unwrap();
    assert_eq!(engine.writes.load(Ordering::Relaxed), 1);
    replay.append_batch(&batch, 1).unwrap();
    assert_eq!(engine.writes.load(Ordering::Relaxed), 1);
    assert_eq!(replay.ranges("auth", "n"), [(1, 2)]);
    assert_eq!(replay.ranges("cdn", "n"), [(5, 5)]);
    let reopened = Replay::open(engine, ReplayConfig::default()).unwrap();
    assert_eq!(reopened.bytes(), replay.bytes());
    assert_eq!(reopened.read("auth", "n", 1, 5).unwrap(), batch[..2]);
}

#[test]
fn batch_conflicts_with_itself_or_committed_data_publish_no_partial_candidates() {
    use std::sync::atomic::Ordering;
    let engine = batch_engine();
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    let original = event("auth", "n", 1);
    let mut conflict = original.clone();
    conflict.committed_at += 1;
    assert!(matches!(
        replay.append_batch(&[original.clone(), conflict.clone()], 0),
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(engine.writes.load(Ordering::Relaxed), 0);
    assert_eq!(replay.bytes(), 0);
    replay.rebuild().unwrap();
    replay.append(&original, 0).unwrap();
    let writes = engine.writes.load(Ordering::Relaxed);
    assert!(matches!(
        replay.append_batch(&[event("auth", "n", 2), conflict], 1),
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(engine.writes.load(Ordering::Relaxed), writes);
    assert!(
        engine
            .get(&key(&identity(&event("auth", "n", 2))))
            .unwrap()
            .is_none()
    );
    assert!(replay.ranges("auth", "n").is_empty());
}

#[test]
fn failed_batch_write_leaves_committed_data_and_metadata_unchanged() {
    use std::sync::atomic::Ordering;
    let engine = batch_engine();
    let replay = Replay::open(engine.clone(), ReplayConfig::default()).unwrap();
    replay.append(&event("auth", "n", 1), 0).unwrap();
    let retained_bytes = replay.bytes();
    engine.fail_write.store(true, Ordering::Relaxed);
    assert!(
        replay
            .append_batch(&[event("auth", "n", 2), event("auth", "n", 3)], 1)
            .is_err()
    );
    assert_eq!(replay.bytes(), retained_bytes);
    assert!(replay.ranges("auth", "n").is_empty());
    assert!(
        engine
            .get(&key(&identity(&event("auth", "n", 2))))
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .get(&key(&identity(&event("auth", "n", 3))))
            .unwrap()
            .is_none()
    );
    let reopened = Replay::open(engine, ReplayConfig::default()).unwrap();
    assert_eq!(reopened.ranges("auth", "n"), [(1, 1)]);
}

#[test]
fn batch_eviction_merges_candidate_and_existing_priority_and_age_indexes() {
    let engine = Arc::new(MemoryEngine::new("batch"));
    let high = event("hi", "n", 1);
    let size = (key(&identity(&high)).len() + encode(&high, 0).len()) as u64;
    let replay = Replay::open(
        engine,
        ReplayConfig {
            max_bytes: size * 2,
            window_ms: 10,
            priorities: [("hi".into(), 1)].into(),
        },
    )
    .unwrap();
    replay
        .append_batch(&[high, event("lo", "n", 1)], 0)
        .unwrap();
    replay
        .append_batch(&[event("lo", "n", 2), event("lo", "n", 3)], 1)
        .unwrap();
    assert_eq!(replay.ranges("hi", "n"), [(1, 1)]);
    assert_eq!(replay.ranges("lo", "n"), [(3, 3)]);
    assert_eq!(replay.bytes(), size * 2);
    replay.append_batch(&[event("hi", "n", 2)], 12).unwrap();
    assert_eq!(replay.ranges("hi", "n"), [(2, 2)]);
    assert!(replay.ranges("lo", "n").is_empty());
    assert_eq!(replay.bytes(), size);
}
