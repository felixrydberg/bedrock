use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::storage::WriteBatch;

const SOURCE: (&str, &str) = ("auth", "engine");

fn corruption() -> StorageError {
    StorageError::Corruption(ErrorContext {
        engine: "test",
        instance: "auth".into(),
        file: None,
        partition: None,
        detail: "bad block".into(),
    })
}

/// Restores by rewriting the data through the replication handle, after `failures` failed attempts. Records whether
/// the serving handle answered at any point during a rebuild.
struct Scripted {
    health: Arc<StorageHealth>,
    replica: Arc<dyn StorageEngine>,
    serving: Arc<dyn StorageEngine>,
    failures: Mutex<u32>,
    served_while_rebuilding: AtomicBool,
    /// Storage fails again in the middle of the restore.
    fails_again: AtomicBool,
}

#[async_trait::async_trait]
impl Restore for Scripted {
    async fn restore(&self) -> StorageResult<()> {
        if self.serving.get(b"k").is_ok() {
            self.served_while_rebuilding.store(true, Ordering::Relaxed);
        }
        self.replica.put(b"k", b"rebuilt")?;
        if self.fails_again.load(Ordering::Relaxed) {
            self.health.report(SOURCE.0, SOURCE.1, &corruption());
        }
        let mut failures = self.failures.lock().unwrap();
        if *failures > 0 {
            *failures -= 1;
            return Err(StorageError::Unavailable(ErrorContext {
                engine: "test",
                instance: "auth".into(),
                file: None,
                partition: None,
                detail: "source unreachable".into(),
            }));
        }
        Ok(())
    }
}

fn setup(
    failures: u32,
) -> (
    Arc<Admin>,
    Arc<StorageHealth>,
    Arc<Scripted>,
    Reconciler<Rebuild>,
) {
    let admin = Admin::new();
    let health = StorageHealth::new(&admin);
    let raw = Arc::new(MemoryEngine::new("auth"));
    raw.put(b"k", b"old").unwrap();
    let scripted = Arc::new(Scripted {
        health: health.clone(),
        replica: health.wrap_replication(SOURCE.0, SOURCE.1, raw.clone()),
        serving: health.wrap(SOURCE.0, SOURCE.1, raw),
        failures: Mutex::new(failures),
        served_while_rebuilding: AtomicBool::new(false),
        fails_again: AtomicBool::new(false),
    });
    let restore: Arc<dyn Restore> = scripted.clone();
    let rebuild = Rebuild::new(
        health.clone(),
        BTreeMap::from([((SOURCE.0.to_owned(), SOURCE.1), restore)]),
    );
    let config = Config {
        burst: 10,
        refill: Duration::from_millis(1),
        repair_timeout: Duration::from_secs(1),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
        seed: 7,
    };
    let reconciler = Reconciler::new("rebuild", Arc::new(rebuild), config, &Metrics::new(&admin));
    (admin, health, scripted, reconciler)
}

fn state(health: &StorageHealth) -> State {
    health.get(SOURCE.0, SOURCE.1).unwrap().state
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn healthy_sources_need_no_rebuild() {
    let (_, health, scripted, mut reconciler) = setup(0);
    assert_eq!(reconciler.pass().await.unwrap(), Default::default());
    assert_eq!(state(&health), State::Healthy);
    assert_eq!(scripted.serving.get(b"k").unwrap(), Some(b"old".to_vec()));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn failed_rebuild_stays_quarantined_and_retries_until_validated() {
    let (admin, health, scripted, mut reconciler) = setup(1);
    health.report(SOURCE.0, SOURCE.1, &corruption());
    assert!(scripted.serving.get(b"k").is_err());

    let pass = reconciler.pass().await.unwrap();
    assert_eq!((pass.pending, pass.failed), (1, 1));
    assert_eq!(state(&health), State::Unhealthy);
    assert!(
        scripted.serving.get(b"k").is_err(),
        "an unvalidated rebuild is not served"
    );
    assert!(
        scripted.replica.put(b"k", b"x").is_err(),
        "a quarantined source refuses even replication"
    );
    // Backing off: the next pass waits.
    assert_eq!(reconciler.pass().await.unwrap().failed, 0);
    assert_eq!(state(&health), State::Unhealthy);

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(reconciler.pass().await.unwrap().repaired, 1);
    assert_eq!(state(&health), State::Healthy);
    assert_eq!(
        scripted.serving.get(b"k").unwrap(),
        Some(b"rebuilt".to_vec())
    );
    assert!(!scripted.served_while_rebuilding.load(Ordering::Relaxed));
    assert_eq!(health.get(SOURCE.0, SOURCE.1).unwrap().recoveries, 2);
    let metrics = admin.render();
    assert!(
        metrics.contains(r#"storage_health{dataset="auth",state="Healthy"} 1"#),
        "{metrics}"
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn storage_failing_again_during_its_rebuild_is_not_recovered() {
    let (_, health, scripted, mut reconciler) = setup(0);
    health.report(SOURCE.0, SOURCE.1, &corruption());
    scripted.fails_again.store(true, Ordering::Relaxed);
    assert_eq!(reconciler.pass().await.unwrap().failed, 1);
    assert_eq!(state(&health), State::Unhealthy);
    assert!(scripted.serving.get(b"k").is_err());
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn reinitialise_recreates_the_instance_without_old_data_or_revisions() {
    let engine = Arc::new(MemoryEngine::new("cdn"));
    let mut batch = WriteBatch::default();
    batch.put(b"a", b"1").set_revision("artifacts", 9);
    engine.write(batch).unwrap();
    Reinitialise {
        engine: engine.clone(),
        resubscribe: None,
    }
    .restore()
    .await
    .unwrap();
    assert_eq!(engine.get(b"a").unwrap(), None);
    assert_eq!(engine.revision("artifacts").unwrap(), None);

    // A record that cannot be read back fails validation.
    engine.corrupt(b"b");
    let check = Reinitialise {
        engine: Arc::new(Unrestorable(engine)),
        resubscribe: None,
    };
    assert!(matches!(
        check.restore().await,
        Err(StorageError::Corruption(_))
    ));
}

/// An engine whose restore keeps the old contents, so validation sees what it left behind.
struct Unrestorable(Arc<MemoryEngine>);

impl StorageEngine for Unrestorable {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.0.get(key)
    }
    fn scan(&self, range: &KeyRange) -> StorageResult<crate::storage::Scan<'_>> {
        self.0.scan(range)
    }
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        self.0.write(batch)
    }
    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.0.revision(name)
    }
    fn snapshot(&self) -> StorageResult<Box<dyn crate::storage::Snapshot + '_>> {
        self.0.snapshot()
    }
    fn restore(&self, _: &dyn crate::storage::Snapshot) -> StorageResult<()> {
        Ok(())
    }
    fn stats(&self) -> StorageResult<crate::storage::StorageStats> {
        self.0.stats()
    }
    fn disk_usage(&self) -> StorageResult<u64> {
        self.0.disk_usage()
    }
    fn health(&self) -> StorageResult<()> {
        self.0.health()
    }
}
