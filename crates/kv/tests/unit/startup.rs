use super::*;
use bedrock_lib::storage::memory::MemoryEngine;
use std::sync::Arc;

#[test]
fn opens_only_kv_datasets() {
    let all = ["quicksilver", "kv", "auth"].map(String::from);
    let r = Registry::open(DATASETS, &all, |d| Ok(Arc::new(MemoryEngine::new(d.name)))).unwrap();
    assert_eq!(r.iter().count(), 3);
    let qs = &r.get("quicksilver").unwrap().dataset.defaults;
    assert_eq!(qs.replication, ReplicationPolicy::FullReplica);
    assert!(!qs.cache.l2_eviction);
    assert!(r.get("kv").unwrap().dataset.defaults.cache.l2_eviction);
    assert!(Registry::open(DATASETS, &["kv".into(), "cdn".into()], |_| unreachable!()).is_err());
}
