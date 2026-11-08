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

#[test]
fn cdn_drains_nodes_through_its_ranges_and_refuses_one_namespace() {
    use bedrock_lib::reconciliation::drain::{Error, Outcome, Scope};
    let (view, _) = membership::View::new("cdn", "eu-1a");
    let hooks = drain_hooks(&std::sync::Arc::new(view));
    assert_eq!(hooks.len(), 1);
    let node = Scope::Node("cdn-1".into());
    assert_eq!(hooks[0].drain(&node, None), Ok(Outcome::NothingToMove));
    assert!(matches!(
        hooks[0].drain(&node, Some("pages")),
        Err(Error::Unsupported { .. })
    ));
}
