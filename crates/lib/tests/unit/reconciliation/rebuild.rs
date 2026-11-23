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
    async fn restore(&self, _: &[Range]) -> StorageResult<()> {
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

/// Fails again, then waits forever, as a real resubscribe waits for installs a quarantined source refuses.
struct Blocked {
    health: Arc<StorageHealth>,
}

#[async_trait::async_trait]
impl Restore for Blocked {
    async fn restore(&self, _: &[Range]) -> StorageResult<()> {
        self.health.report(SOURCE.0, SOURCE.1, &corruption());
        std::future::pending().await
    }
}

/// Found by the simulation (#25 / B-23, seed 43 of `corruption`).
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn storage_failing_again_abandons_a_blocked_rebuild_at_once() {
    let admin = Admin::new();
    let health = StorageHealth::new(&admin);
    health.report(SOURCE.0, SOURCE.1, &corruption());
    let restore: Arc<dyn Restore> = Arc::new(Blocked {
        health: health.clone(),
    });
    let rebuild = Rebuild::new(
        health.clone(),
        BTreeMap::from([((SOURCE.0.to_owned(), SOURCE.1), restore)]),
    );
    let config = Config {
        burst: 10,
        refill: Duration::from_millis(1),
        repair_timeout: Duration::from_secs(600),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
        seed: 7,
    };
    let mut reconciler =
        Reconciler::new("rebuild", Arc::new(rebuild), config, &Metrics::new(&admin));
    let started = crate::monotonic_now();
    assert_eq!(reconciler.pass().await.unwrap().failed, 1);
    let took = started.elapsed();
    assert!(took < Duration::from_secs(10), "waited {took:?}");
    assert_eq!(state(&health), State::Unhealthy);
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
    .restore(&[])
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
        check.restore(&[]).await,
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

mod ranges {
    //! #103 / B-62: verify, then narrow the quarantine to the damaged ranges.
    use super::*;
    use crate::storage::sim::{Fault, SimEngine};
    use crate::storage::{Scan, Snapshot, StorageStats, l2_key};

    const HALF: u64 = 1 << 63;

    struct Node {
        health: Arc<StorageHealth>,
        disk: Arc<SimEngine>,
        replica: Arc<dyn StorageEngine>,
        serving: Arc<dyn StorageEngine>,
    }

    fn low(n: u8) -> Vec<u8> {
        l2_key(10 + u64::from(n), "n", &[n])
    }

    fn high() -> Vec<u8> {
        l2_key(HALF + 5, "n", b"high")
    }

    /// Records in two low hashes, one high one, one outside L2 and a revision.
    fn node(disk: Arc<SimEngine>) -> Node {
        let health = StorageHealth::new(&Admin::new());
        let replica = health.wrap_replication(SOURCE.0, SOURCE.1, disk.clone());
        let serving = health.wrap(SOURCE.0, SOURCE.1, disk.clone());
        let mut batch = WriteBatch::default();
        batch
            .put(low(0), b"0")
            .put(low(1), b"1")
            .put(high(), b"h")
            .put(b"meta", b"m")
            .set_revision("kept", 3);
        replica.write(batch).unwrap();
        Node {
            health,
            disk,
            replica,
            serving,
        }
    }

    fn sim() -> Arc<SimEngine> {
        Arc::new(SimEngine::new("auth", Arc::new(|_| None), None, false))
    }

    fn reinitialise(node: &Node) -> Reinitialise {
        Reinitialise {
            engine: node.replica.clone(),
            resubscribe: None,
        }
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn damage_in_one_range_rebuilds_only_that_range_while_the_rest_serves() {
        let node = node(sim());
        // A bad block takes its neighbour with it.
        node.disk.damage_block(l2_span(10, 12));
        assert!(matches!(
            node.serving.get(&low(1)),
            Err(StorageError::Corruption(_))
        ));
        assert!(
            node.serving.get(&high()).is_err(),
            "while the extent is unknown the whole instance is refused"
        );

        assert!(node.health.rebuilding(SOURCE.0, SOURCE.1));
        let restore = reinitialise(&node);
        let ranges = restore.narrow().await.unwrap().expect("one L2 range");
        assert_eq!(ranges, vec![(10, 13)]);
        assert!(node.health.narrow(SOURCE.0, SOURCE.1, ranges.clone()));
        assert_eq!(node.serving.get(&high()), Ok(Some(b"h".to_vec())));
        assert_eq!(node.serving.get(b"meta"), Ok(Some(b"m".to_vec())));
        assert!(matches!(
            node.serving.get(&low(0)),
            Err(StorageError::Unavailable(_))
        ));

        restore.restore(&ranges).await.unwrap();
        assert!(node.health.recovered(SOURCE.0, SOURCE.1));
        // Without a relay nothing re-bootstraps, so the discarded range is empty; everything else stayed.
        assert_eq!(node.serving.get(&low(0)), Ok(None));
        assert_eq!(node.serving.get(&high()), Ok(Some(b"h".to_vec())));
        assert_eq!(node.serving.revision("kept"), Ok(Some(3)));
        assert_eq!(node.disk.revision("health:quarantined"), Ok(None));
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn a_restart_during_a_range_rebuild_still_refuses_the_range() {
        let first = node(sim());
        first.disk.damage(&low(0));
        assert!(first.serving.get(&low(0)).is_err());
        assert!(first.health.rebuilding(SOURCE.0, SOURCE.1));
        let ranges = reinitialise(&first).narrow().await.unwrap().unwrap();
        assert!(first.health.narrow(SOURCE.0, SOURCE.1, ranges));

        // The process stops before the range rebuilt.
        let disk = first.disk.clone();
        drop(first);
        let health = StorageHealth::new(&Admin::new());
        let replica = health.wrap_replication(SOURCE.0, SOURCE.1, disk.clone());
        let serving = health.wrap(SOURCE.0, SOURCE.1, disk);
        assert!(serving.get(&low(0)).is_err());
        assert_eq!(serving.get(&high()), Ok(Some(b"h".to_vec())));

        let restore: Arc<dyn Restore> = Arc::new(Reinitialise {
            engine: replica,
            resubscribe: None,
        });
        let source = (SOURCE.0.to_owned(), SOURCE.1);
        let rebuild = Rebuild::new(health.clone(), BTreeMap::from([(source.clone(), restore)]));
        rebuild.repair(&source, &()).await.unwrap();
        assert_eq!(
            health.get(SOURCE.0, SOURCE.1).unwrap().state,
            State::Healthy
        );
        assert_eq!(serving.get(&high()), Ok(Some(b"h".to_vec())));
    }

    fn same(ranges: &[Range]) -> StorageResult<Vec<Range>> {
        Ok(ranges.to_vec())
    }

    /// Forwards everything but [`StorageEngine::discard`].
    struct NoDiscard(Arc<SimEngine>);

    impl StorageEngine for NoDiscard {
        fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            self.0.get(key)
        }
        fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
            self.0.scan(range)
        }
        fn write(&self, batch: WriteBatch) -> StorageResult<()> {
            self.0.write(batch)
        }
        fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
            self.0.revision(name)
        }
        fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
            self.0.snapshot()
        }
        fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
            self.0.restore(from)
        }
        fn stats(&self) -> StorageResult<StorageStats> {
            self.0.stats()
        }
        fn disk_usage(&self) -> StorageResult<u64> {
            self.0.disk_usage()
        }
        fn health(&self) -> StorageResult<()> {
            self.0.health()
        }
        fn damaged(&self, failure: &StorageError) -> Option<KeyRange> {
            self.0.damaged(failure)
        }
    }

    /// Each fallback of #103 rebuilds the whole instance.
    #[test]
    fn verification_falls_back_to_the_whole_instance() {
        let case = |damage: &dyn Fn(&SimEngine)| {
            let disk = sim();
            let node = node(disk.clone());
            damage(&disk);
            verify(node.replica.as_ref(), &same).unwrap()
        };
        assert_eq!(
            case(&|_| {}),
            None,
            "a failure that names nothing to discard"
        );
        assert_eq!(case(&|d| d.damage(b"meta")), None, "a record outside L2");
        assert_eq!(case(&|d| d.damage_revision("kept")), None, "a revision");
        assert_eq!(
            case(&|d| d.damage_block(KeyRange::all())),
            None,
            "a block reaching outside L2"
        );
        assert_eq!(
            case(&|d| {
                for n in 0..=MAX_SPANS as u64 {
                    let key = l2_key(n * 1000, "n", b"x");
                    d.put(&key, b"x").unwrap();
                    d.damage(&key);
                }
            }),
            None,
            "too many damaged spans"
        );
        assert_eq!(
            case(&|d| d.damage_block(l2_span(0, HALF + 1))),
            None,
            "too much of the hash space"
        );

        let disk = sim();
        let _node = node(disk.clone());
        disk.damage(&low(0));
        assert_eq!(verify(&NoDiscard(disk), &same).unwrap(), None, "no discard");

        let failing = Arc::new(AtomicBool::new(false));
        let faults = {
            let failing = failing.clone();
            Arc::new(move |_| failing.load(Ordering::Relaxed).then_some(Fault::Io))
        };
        let node = node(Arc::new(SimEngine::new("auth", faults, None, false)));
        failing.store(true, Ordering::Relaxed);
        assert_eq!(
            verify(node.replica.as_ref(), &same).unwrap(),
            None,
            "verification fails"
        );
    }
}
