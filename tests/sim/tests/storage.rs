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

/// B-61: commits keep only what undoes them, and a crash must still land exactly where a full copy before each commit would.
#[test]
fn a_crash_returns_to_the_state_before_each_lost_commit() {
    let steps: Vec<fn(&SimEngine)> = vec![
        |e| {
            let mut batch = WriteBatch::default();
            batch
                .put("a", "1")
                .put("b", "1")
                .put("c", "1")
                .put("a", "2");
            batch.set_revision("n", 1);
            e.write(batch).unwrap();
        },
        |e| {
            let mut batch = WriteBatch::default();
            batch.delete("b").delete("x").set_revision("n", 2);
            e.write(batch).unwrap();
            // Damage is not a commit: losing the next commit keeps it, losing this one takes it too.
            e.damage(b"c");
        },
        |e| {
            let mut batch = WriteBatch::default();
            batch.put("d", "1").set_revision("m", 1);
            e.write(batch).unwrap();
        },
        |e| {
            let mut batch = WriteBatch::default();
            batch.delete_revision("m").delete_revision("absent");
            e.write(batch).unwrap();
        },
        |e| {
            let mut batch = WriteBatch::default();
            batch.delete_range(KeyRange::new("a", "c")).put("b", "3");
            batch.set_revision("n", 4);
            e.write(batch).unwrap();
        },
        |e| {
            let source = SimEngine::new("source", Arc::new(|_| None), None, false);
            source.put(b"z", b"1").unwrap();
            e.restore(source.snapshot().unwrap().as_ref()).unwrap();
        },
        |e| {
            let mut batch = WriteBatch::default();
            batch.put("c", "2").set_revision("n", 6);
            e.write(batch).unwrap();
        },
    ];
    for lose in 1..=steps.len() {
        let engine = engine(Arc::new(|_| None));
        let mut before = Vec::new();
        for step in &steps {
            before.push(engine.frozen());
            step(&engine);
        }
        assert_eq!(engine.crash(lose), lose);
        assert_eq!(engine.frozen(), before[steps.len() - lose], "losing {lose}");
    }
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
