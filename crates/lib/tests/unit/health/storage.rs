use super::*;
use crate::storage::memory::MemoryEngine;

const ENGINE: &str = "engine";

fn io() -> StorageError {
    StorageError::IoError(ErrorContext {
        engine: "test",
        instance: "a".into(),
        file: None,
        partition: None,
        detail: "read failed".into(),
    })
}

/// A memory engine whose reads fail with an I/O error while `failing` is set.
struct Flaky {
    inner: MemoryEngine,
    failing: AtomicBool,
}

impl StorageEngine for Flaky {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(io());
        }
        self.inner.get(key)
    }
    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        self.inner.scan(range)
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
}

#[test]
fn corruption_in_one_dataset_quarantines_only_that_dataset() {
    let admin = Admin::new();
    let health = StorageHealth::new(&admin);
    let raw_a = Arc::new(MemoryEngine::new("a"));
    let a = health.wrap("a", ENGINE, raw_a.clone());
    let b = health.wrap("b", ENGINE, Arc::new(MemoryEngine::new("b")));
    for engine in [&a, &b] {
        engine.put(b"k", b"v").unwrap();
        engine.put(b"other", b"v").unwrap();
    }
    raw_a.corrupt(b"k");

    // Corruption is reported as such, never as a miss, and then the whole instance is refused.
    assert!(matches!(a.get(b"k"), Err(StorageError::Corruption(_))));
    assert!(matches!(a.get(b"other"), Err(StorageError::Unavailable(_))));
    assert!(a.put(b"new", b"v").is_err());
    assert!(a.health().is_err());
    let report = health.get("a", ENGINE).unwrap();
    assert_eq!(report.state, State::Unhealthy);
    assert_eq!(report.errors, 1);
    assert!(
        report
            .last
            .is_some_and(|e| e.context().partition.as_deref() == Some("key k"))
    );

    // Dataset b keeps serving.
    assert_eq!(b.get(b"k").unwrap(), Some(b"v".to_vec()));
    b.put(b"new", b"v").unwrap();
    assert_eq!(health.get("b", ENGINE).unwrap().state, State::Healthy);
    assert_eq!(health.unhealthy(), BTreeSet::from(["a".to_string()]));

    let metrics = admin.render();
    assert!(metrics.contains(r#"bedrock_storage_health{dataset="a",state="Unhealthy"} 1"#));
    assert!(metrics.contains(r#"bedrock_storage_health{dataset="a",state="Healthy"} 0"#));
    assert!(metrics.contains(r#"bedrock_storage_health{dataset="b",state="Healthy"} 1"#));
    assert!(metrics.contains(r#"bedrock_storage_errors_total{dataset="a",kind="corruption"} 1"#));
}

#[test]
fn corruption_found_while_scanning_quarantines() {
    let health = StorageHealth::new(&Admin::new());
    let raw = Arc::new(MemoryEngine::new("a"));
    let engine = health.wrap("a", ENGINE, raw.clone());
    engine.put(b"k1", b"v").unwrap();
    engine.put(b"k2", b"v").unwrap();
    raw.corrupt(b"k2");
    let snapshot = engine.snapshot().unwrap();
    let entries: Vec<_> = snapshot.scan(&KeyRange::all()).unwrap().collect();
    assert!(entries[0].is_ok());
    assert!(matches!(entries[1], Err(StorageError::Corruption(_))));
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Unhealthy);
    assert!(snapshot.get(b"k1").is_err());
}

#[test]
fn repeated_io_errors_quarantine_and_a_success_resets_the_count() {
    let health = StorageHealth::new(&Admin::new());
    let flaky = Arc::new(Flaky {
        inner: MemoryEngine::new("a"),
        failing: AtomicBool::new(true),
    });
    let engine = health.wrap("a", ENGINE, flaky.clone());
    for _ in 1..CONSECUTIVE_FAILURES {
        assert!(matches!(engine.get(b"k"), Err(StorageError::IoError(_))));
    }
    flaky.failing.store(false, Ordering::Relaxed);
    assert_eq!(engine.get(b"k").unwrap(), None);
    flaky.failing.store(true, Ordering::Relaxed);
    for _ in 1..CONSECUTIVE_FAILURES {
        assert!(engine.get(b"k").is_err());
    }
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Healthy);
    assert!(engine.get(b"k").is_err());
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Unhealthy);

    // Neither a fenced writer nor a missing capability is a storage failure.
    let other = health.wrap("b", ENGINE, Arc::new(MemoryEngine::new("b")));
    for _ in 0..CONSECUTIVE_FAILURES {
        assert!(matches!(
            other.disk_usage(),
            Err(StorageError::Unsupported(_))
        ));
    }
    assert_eq!(health.get("b", ENGINE).unwrap().errors, 0);
}

#[test]
fn rebuild_tracks_attempts_and_restores_service() {
    let health = StorageHealth::new(&Admin::new());
    let raw = Arc::new(MemoryEngine::new("a"));
    let engine = health.wrap("a", ENGINE, raw.clone());
    // Only an unhealthy source can start rebuilding, and only a rebuilding one recovers.
    assert!(!health.rebuilding("a", ENGINE));
    assert!(!health.recovered("a", ENGINE));
    health.report("a", ENGINE, &io());
    raw.corrupt(b"k");
    assert!(engine.get(b"k").is_err());

    assert!(health.rebuilding("a", ENGINE));
    assert!(
        engine.get(b"k").is_err(),
        "a rebuilding source is not served"
    );
    // A failure during the rebuild sends it back to unhealthy.
    health.report(
        "a",
        ENGINE,
        &StorageError::Corruption(io().context().clone()),
    );
    assert_eq!(health.get("a", ENGINE).unwrap().state, State::Unhealthy);
    assert!(health.rebuilding("a", ENGINE));
    raw.delete(b"k").unwrap();
    assert!(health.recovered("a", ENGINE));

    assert_eq!(engine.get(b"k").unwrap(), None);
    let report = health.get("a", ENGINE).unwrap();
    assert_eq!((report.state, report.recoveries), (State::Healthy, 2));
    assert!(health.unhealthy().is_empty());
}
