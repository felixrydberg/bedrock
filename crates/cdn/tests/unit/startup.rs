use super::*;
use bedrock_lib::storage::memory::MemoryEngine;
use std::sync::Arc;

#[test]
fn opens_only_cdn() {
    let r = Registry::open(DATASETS, &["cdn".into()], |d| {
        Ok(Arc::new(MemoryEngine::new(d.name)))
    })
    .unwrap();
    assert!(r.get("cdn").unwrap().engine.is_ok());
    for kv in ["quicksilver", "kv", "auth"] {
        assert!(Registry::open(DATASETS, &[kv.into()], |_| unreachable!()).is_err());
    }
}
