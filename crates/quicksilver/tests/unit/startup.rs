use super::*;
use bedrock_lib::storage::memory::MemoryEngine;
use std::sync::Arc;

#[test]
fn opens_only_the_quicksilver_dataset() {
    let r = Registry::open(DATASETS, &["quicksilver".into()], |d| {
        Ok(Arc::new(MemoryEngine::new(d.name)))
    })
    .unwrap();
    assert_eq!(r.iter().count(), 1);
    let qs = &r.get("quicksilver").unwrap().dataset.defaults;
    assert_eq!(qs.replication, ReplicationPolicy::FullReplica);
    assert!(!qs.cache.l2_eviction);
    for other in ["auth", "kv", "cdn"] {
        assert!(Registry::open(DATASETS, &[other.into()], |_| unreachable!()).is_err());
    }
}

#[test]
fn quicksilver_drains_through_its_ranges() {
    use bedrock_lib::reconciliation::drain::{Error, Outcome, Scope};
    let hooks =
        bedrock_lib::reconciliation::drain::ranged_with("quicksilver", |_| Ok(Default::default()));
    let datasets: Vec<_> = hooks.iter().map(|h| h.dataset()).collect();
    assert_eq!(datasets, ["quicksilver"]);
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
