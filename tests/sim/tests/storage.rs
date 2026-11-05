//! #25 / B-23: the simulated disk's crash and ambiguity semantics, which the simulation's fault model rests on.

use std::sync::Arc;

use bedrock_lib::health::storage::{State, StorageHealth};
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{
    Access, Fault, Faults, SimEngine, SimFactory, StoreAccess, StoreFault,
};
use bedrock_lib::storage::{KeyRange, Snapshot, StorageEngine, StorageError, WriteBatch};
use bedrock_lib::transport::admin::Admin;

fn engine(faults: Faults) -> SimEngine {
    SimEngine::new("test", faults, None, true)
}

fn committed(engine: &SimEngine, revision: u64) {
    let mut batch = WriteBatch::default();
    batch
        .put(format!("k{revision}"), "v")
        .set_revision("n", revision);
    engine.write(batch).unwrap();
}

#[test]
fn crash_takes_back_data_and_revision_together() {
    let engine = engine(Arc::new(|_| None));
    for revision in 1..=3 {
        committed(&engine, revision);
    }
    assert_eq!(engine.crash(2), 2);
    assert_eq!(engine.revision("n").unwrap(), Some(1));
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v".to_vec()));
    assert_eq!(engine.get(b"k2").unwrap(), None);
    assert_eq!(engine.get(b"k3").unwrap(), None);
    // What a crash left cannot be lost again by the next one.
    assert_eq!(engine.crash(1), 0);
    assert_eq!(engine.revision("n").unwrap(), Some(1));
}

#[test]
fn ambiguous_write_commits_and_reports_failure() {
    let engine = engine(Arc::new(|access| {
        (access == Access::Write).then_some(Fault::Ambiguous)
    }));
    assert!(matches!(
        engine.put(b"k", b"v"),
        Err(StorageError::IoError(_))
    ));
    assert_eq!(engine.frozen().get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn damage_fails_reads_until_the_record_is_rewritten() {
    let engine = engine(Arc::new(|_| None));
    engine.put(b"k", b"v").unwrap();
    engine.damage(b"k");
    assert_eq!(engine.health(), Ok(()));
    assert!(matches!(engine.get(b"k"), Err(StorageError::Corruption(_))));
    engine.put(b"k", b"w").unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(b"w".to_vec()));
}

#[tokio::test]
async fn lost_flush_reports_success_and_a_crash_shows_the_lie() {
    let factory = SimFactory::new(Arc::new(|access| {
        (access == StoreAccess::Flush).then_some(StoreFault::LostFlush)
    }));
    let db = factory.open("kv", "one").await.unwrap();
    db.write(vec![(b"k".to_vec(), Some(b"v".to_vec()))])
        .await
        .unwrap();
    db.flush().await.unwrap();
    factory.crash().unwrap();
    let db = factory.open("kv", "one").await.unwrap();
    assert_eq!(db.get(b"k").await.unwrap(), None);
}

#[tokio::test]
async fn ambiguous_flush_persists_and_reports_failure() {
    let factory = SimFactory::new(Arc::new(|access| {
        (access == StoreAccess::Flush).then_some(StoreFault::Ambiguous)
    }));
    let db = factory.open("kv", "one").await.unwrap();
    db.write(vec![(b"k".to_vec(), Some(b"v".to_vec()))])
        .await
        .unwrap();
    assert!(matches!(db.flush().await, Err(StorageError::IoError(_))));
    factory.crash().unwrap();
    let db = factory.open("kv", "one").await.unwrap();
    assert_eq!(db.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}

/// Found by seed 21641 (#25 / B-23): a crash took back the quarantine record and the rebuild together, the damage came back
/// with them, and the restart found a clean-looking engine. Only reading the record again can tell.
#[test]
fn a_crash_can_undo_a_quarantine_and_its_rebuild_but_the_record_still_fails_a_read() {
    let disk = Arc::new(engine(Arc::new(|_| None)));
    committed(&disk, 1);
    disk.damage(b"k1");

    let health = StorageHealth::new(&Admin::new());
    let handle = health.wrap("a", "engine", disk.clone());
    assert!(handle.get(b"k1").is_err());
    assert_eq!(health.get("a", "engine").unwrap().state, State::Unhealthy);
    assert_eq!(disk.revision("health:quarantined").unwrap(), Some(1));
    let source = SimEngine::new("source", Arc::new(|_| None), None, false);
    disk.restore(source.snapshot().unwrap().as_ref()).unwrap();
    assert_eq!(disk.get(b"k1").unwrap(), None);

    assert_eq!(disk.crash(2), 2);
    assert_eq!(disk.revision("health:quarantined").unwrap(), None);

    let health = StorageHealth::new(&Admin::new());
    let handle = health.wrap("a", "engine", disk);
    assert_eq!(health.get("a", "engine").unwrap().state, State::Healthy);
    let mut scan = handle.scan(&KeyRange::all()).unwrap();
    assert!(scan.any(|entry| entry.is_err()));
    drop(scan);
    assert_eq!(health.get("a", "engine").unwrap().state, State::Unhealthy);
}
