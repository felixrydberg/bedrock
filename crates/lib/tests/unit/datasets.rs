use super::*;
use crate::storage::memory::MemoryEngine;
use crate::storage::{
    ErrorContext, KeyRange, Scan, Snapshot, StorageError, StorageStats, WriteBatch,
};

#[test]
fn namespace_ids_are_nul_free() {
    let ns = Namespace::new("customer-123/store-A", "eu").unwrap();
    assert_eq!(ns.id(), "customer-123/store-A");
    assert_eq!(ns.home_region, "eu");
    assert!(Namespace::new("customer-123\0store-A", "eu").is_err());
}

fn dataset(name: &'static str) -> Dataset {
    Dataset {
        name,
        defaults: Policy {
            replication: ReplicationPolicy::Global,
            cache: CachePolicy {
                l1_recent_window_seconds: 60,
                l2_eviction: true,
                pinned_prefixes: vec!["critical/".into()],
            },
            budget: Budget {
                cpus: Some(2.0),
                memory_mb: Some(1024),
                disk_mb: None,
            },
        },
    }
}

#[test]
fn dataset_defaults_apply_without_overrides() {
    let d = dataset("quicksilver");
    assert_eq!(
        resolve(&d.defaults, &PolicyOverrides::default(), &Budget::default()),
        d.defaults
    );
}

#[test]
fn namespace_overrides_replace_defaults() {
    let d = dataset("kv");
    let overrides = PolicyOverrides {
        replication: Some(ReplicationPolicy::HomeOnly),
        l1_recent_window_seconds: Some(0),
        l2_eviction: Some(false),
        pinned_prefixes: Some(vec![]),
    };
    let p = resolve(&d.defaults, &overrides, &Budget::default());
    assert_eq!(p.replication, ReplicationPolicy::HomeOnly);
    assert_eq!(
        p.cache,
        CachePolicy {
            l1_recent_window_seconds: 0,
            l2_eviction: false,
            pinned_prefixes: vec![],
        }
    );
    assert_eq!(p.budget, d.defaults.budget);

    // A partial override keeps the other defaults.
    let p = resolve(
        &d.defaults,
        &PolicyOverrides {
            l2_eviction: Some(false),
            ..Default::default()
        },
        &Budget::default(),
    );
    assert!(!p.cache.l2_eviction);
    assert_eq!(p.replication, ReplicationPolicy::Global);
    assert_eq!(p.cache.pinned_prefixes, ["critical/"]);
}

#[test]
fn node_budget_caps_dataset_budget() {
    let d = dataset("kv");
    let node = Budget {
        cpus: Some(4.0),
        memory_mb: Some(512),
        disk_mb: Some(10_000),
    };
    let p = resolve(&d.defaults, &PolicyOverrides::default(), &node);
    assert_eq!(
        p.budget,
        Budget {
            cpus: Some(2.0),
            memory_mb: Some(512),
            disk_mb: Some(10_000),
        }
    );
}

fn memory(d: &Dataset) -> StorageResult<Arc<dyn StorageEngine>> {
    Ok(Arc::new(MemoryEngine::new(d.name)))
}

#[test]
fn opens_one_isolated_instance_per_enabled_dataset() {
    let hosted = [dataset("auth"), dataset("kv"), dataset("quicksilver")];
    let enabled = ["quicksilver".to_string(), "kv".into(), "kv".into()];
    let r = Registry::open(&hosted, &enabled, memory).unwrap();
    let names: Vec<_> = r.iter().map(|i| i.dataset.name).collect();
    assert_eq!(names, ["kv", "quicksilver"]);
    assert!(r.get("auth").is_none());
    assert!(r.is_healthy());

    let engine = |name| r.get(name).unwrap().engine.as_ref().unwrap();
    engine("kv").put(b"k", b"v").unwrap();
    assert_eq!(engine("quicksilver").get(b"k").unwrap(), None);
}

#[test]
fn failing_instance_does_not_stop_others() {
    let hosted = [dataset("kv"), dataset("quicksilver")];
    let enabled = ["kv".to_string(), "quicksilver".into()];
    let r = Registry::open(&hosted, &enabled, |d| {
        if d.name == "kv" {
            return Err(StorageError::IoError(ErrorContext {
                engine: "test",
                instance: d.name.into(),
                file: None,
                partition: None,
                detail: "disk gone".into(),
            }));
        }
        memory(d)
    })
    .unwrap();
    assert!(r.get("kv").unwrap().engine.is_err());
    assert!(r.get("quicksilver").unwrap().engine.is_ok());
    assert!(!r.is_healthy());
    let healthy = r.get("quicksilver").unwrap().engine.as_ref().unwrap();
    healthy.put(b"k", b"v").unwrap();
    assert_eq!(healthy.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn opened_but_unhealthy_engine_prevents_readiness() {
    struct Unhealthy;

    impl StorageEngine for Unhealthy {
        fn get(&self, _: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            unreachable!()
        }
        fn scan(&self, _: &KeyRange) -> StorageResult<Scan<'_>> {
            unreachable!()
        }
        fn write(&self, _: WriteBatch) -> StorageResult<()> {
            unreachable!()
        }
        fn revision(&self, _: &str) -> StorageResult<Option<u64>> {
            unreachable!()
        }
        fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
            unreachable!()
        }
        fn restore(&self, _: &dyn Snapshot) -> StorageResult<()> {
            unreachable!()
        }
        fn stats(&self) -> StorageResult<StorageStats> {
            unreachable!()
        }
        fn disk_usage(&self) -> StorageResult<u64> {
            unreachable!()
        }
        fn health(&self) -> StorageResult<()> {
            Err(StorageError::Unavailable(ErrorContext {
                engine: "test",
                instance: "cdn".into(),
                file: None,
                partition: None,
                detail: "unhealthy after opening".into(),
            }))
        }
    }

    let r = Registry::open(&[dataset("cdn")], &["cdn".into()], |_| {
        Ok(Arc::new(Unhealthy))
    })
    .unwrap();
    assert!(r.get("cdn").unwrap().engine.is_ok());
    assert!(!r.is_healthy());
}

#[test]
fn rejects_dataset_hosted_elsewhere() {
    let mut opened = 0;
    let err = Registry::open(&[dataset("kv")], &["kv".to_string(), "cdn".into()], |d| {
        opened += 1;
        memory(d)
    });
    assert!(err.is_err());
    assert_eq!(opened, 0);
}

#[test]
fn rejects_empty_dataset_list() {
    assert!(Registry::open(&[dataset("kv")], &[], |_| unreachable!()).is_err());
}

#[test]
#[cfg(not(madsim))]
fn engines_persist_in_separate_dataset_directories() {
    let dir = tempfile::tempdir().unwrap();
    let (kv, qs) = (dataset("kv"), dataset("quicksilver"));
    open_engine(dir.path(), &kv)
        .unwrap()
        .put(b"k", b"v")
        .unwrap();
    assert_eq!(
        open_engine(dir.path(), &kv).unwrap().get(b"k").unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(
        open_engine(dir.path(), &qs).unwrap().get(b"k").unwrap(),
        None
    );
}
