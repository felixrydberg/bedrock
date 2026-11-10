use super::*;
use crate::DATASETS;
use bedrock_lib::datasets::resolve;
use bedrock_lib::node::Budget;

#[test]
fn infrastructure_pins_prefixes_and_never_evicts_l2() {
    let qs = DATASETS
        .iter()
        .find(|d| d.name == bedrock_lib::datasets::capabilities::QUICKSILVER)
        .unwrap();
    let ns = infrastructure("eu");
    assert_eq!(ns.id(), "infrastructure");
    let p = resolve(&qs.defaults, &ns.overrides, &Budget::default());
    assert!(!p.cache.l2_eviction);
    assert_eq!(
        p.cache.pinned_prefixes,
        ["critical/ownership/", "critical/routing/"]
    );
}
