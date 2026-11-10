use std::collections::BTreeSet;
use std::sync::Mutex;

use super::*;
use crate::admin::Admin;
use crate::health::storage::{State, StorageHealth};
use crate::storage::memory::MemoryEngine;
use crate::storage::{
    ErrorContext, Scan, Snapshot, StorageError, StorageStats, WriteBatch, l2_key,
};

const ENGINE: &str = "engine";

/// A memory engine that fails verification on one key, and remembers which ranges were walked.
struct Damaged {
    inner: MemoryEngine,
    key: Vec<u8>,
    walked: Mutex<BTreeSet<Vec<u8>>>,
}

impl StorageEngine for Damaged {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        self.walked.lock().unwrap().insert(range.start.clone());
        let damaged = self.key.clone();
        let scan = self.inner.scan(range)?;
        Ok(Box::new(scan.map(move |entry| match entry {
            Ok((key, _)) if key == damaged => Err(StorageError::Corruption(ErrorContext {
                engine: "test",
                instance: "a".into(),
                file: None,
                partition: None,
                detail: "checksum".into(),
            })),
            entry => entry,
        })))
    }
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        self.inner.write(batch)
    }
    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.inner.revision(name)
    }
    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        self.inner.snapshot()
    }
    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        self.inner.restore(from)
    }
    fn stats(&self) -> StorageResult<StorageStats> {
        self.inner.stats()
    }
    fn disk_usage(&self) -> StorageResult<u64> {
        self.inner.disk_usage()
    }
    fn health(&self) -> StorageResult<()> {
        self.inner.health()
    }
    fn compact(&self, range: &KeyRange) -> StorageResult<()> {
        self.inner.compact(range)
    }
}

fn damaged(keys: &[Vec<u8>], key: &[u8]) -> Arc<Damaged> {
    let inner = MemoryEngine::new("a");
    let mut batch = WriteBatch::default();
    for key in keys {
        batch.put(key.clone(), "value");
    }
    inner.write(batch).unwrap();
    Arc::new(Damaged {
        inner,
        key: key.to_vec(),
        walked: Mutex::default(),
    })
}

#[test]
fn ranges_hold_every_key_exactly_once() {
    let ranges = ranges();
    assert_eq!(ranges.len() as u64, SLICES + 2);
    let keys = [
        b"a".to_vec(),
        b"l2".to_vec(),
        l2_key(0, "n", b"k"),
        l2_key(u64::MAX / SLICES, "n", b"k"),
        l2_key(u64::MAX / SLICES + 1, "n", b"k"),
        l2_key(u64::MAX, "n", b"k"),
        b"l3".to_vec(),
        b"z".to_vec(),
    ];
    for key in keys {
        let holders = ranges.iter().filter(|range| range.contains(&key)).count();
        assert_eq!(holders, 1, "{}", key.escape_ascii());
    }
}

#[test]
fn a_failed_read_of_a_cold_record_quarantines_through_the_handle() {
    let (hot, cold) = (l2_key(0, "n", b"hot"), l2_key(u64::MAX, "n", b"cold"));
    let engine = damaged(&[hot.clone(), cold.clone()], &cold);
    let health = StorageHealth::new(&Admin::new());
    let handle = health.wrap("a", ENGINE, engine);

    let ranges = ranges();
    assert!(handle.get(&cold).unwrap().is_some());
    assert!(verify(handle.as_ref(), &ranges[0]).unwrap() > 0);
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Healthy);

    let range = ranges.iter().find(|range| range.contains(&cold)).unwrap();
    assert!(matches!(
        verify(handle.as_ref(), range),
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Unhealthy);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn one_task_walks_the_engines_in_turn() {
    let (first, second) = (damaged(&[], b""), damaged(&[], b""));
    let walk = start(vec![
        ("a".into(), first.clone() as Arc<dyn StorageEngine>),
        ("b".into(), second.clone() as Arc<dyn StorageEngine>),
    ]);
    tokio::time::sleep(pass(1) - PAUSE / 2).await;
    assert_eq!(first.walked.lock().unwrap().len() as u64, SLICES + 2);
    assert!(second.walked.lock().unwrap().is_empty());

    tokio::time::sleep(pass(1)).await;
    assert_eq!(second.walked.lock().unwrap().len() as u64, SLICES + 2);
    walk.abort();
}
