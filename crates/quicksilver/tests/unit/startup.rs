use super::*;
use bedrock_lib::storage::memory::MemoryEngine;
use std::sync::Arc;

#[test]
fn opens_only_quicksilver_datasets() {
    let all = ["quicksilver", "auth"].map(String::from);
    let r = Registry::open(DATASETS, &all, |d| Ok(Arc::new(MemoryEngine::new(d.name)))).unwrap();
    assert_eq!(r.iter().count(), 2);
    let qs = &r.get("quicksilver").unwrap().dataset.defaults;
    assert_eq!(qs.replication, ReplicationPolicy::FullReplica);
    assert!(!qs.cache.l2_eviction);
    assert!(r.get("auth").unwrap().dataset.defaults.cache.l2_eviction);
    for other in ["kv", "cdn"] {
        assert!(Registry::open(DATASETS, &[other.into()], |_| unreachable!()).is_err());
    }
}

#[test]
fn quicksilver_and_auth_drain_through_their_ranges() {
    use bedrock_lib::reconciliation::drain::{Error, Outcome, Scope};
    let (view, _) = membership::View::new("quicksilver", "eu-1a");
    let hooks = drain_hooks(&std::sync::Arc::new(view));
    let datasets: Vec<_> = hooks.iter().map(|h| h.dataset()).collect();
    assert_eq!(datasets, ["quicksilver", "auth"]);
    for hook in &hooks {
        let node = Scope::Node("qs-1".into());
        assert_eq!(hook.drain(&node, None), Ok(Outcome::NothingToMove));
        assert_eq!(
            hook.drain(&Scope::Cell("eu-1a".into()), None),
            Ok(Outcome::NothingToMove)
        );
        assert!(matches!(
            hook.drain(&node, Some("infrastructure")),
            Err(Error::Unsupported { .. })
        ));
    }
}
