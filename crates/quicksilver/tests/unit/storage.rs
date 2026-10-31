use super::*;
use bedrock_lib::storage::KeyRange;

#[test]
fn rejects_nul_in_namespace_for_every_keyspace() {
    for ks in [Keyspace::L1, Keyspace::L2 { bucket: 7 }, Keyspace::Meta] {
        assert!(std::panic::catch_unwind(|| ks.prefix("ns\0other")).is_err());
    }
}

#[test]
fn namespace_prefix_excludes_nested_namespace() {
    for ks in [Keyspace::L1, Keyspace::L2 { bucket: 7 }, Keyspace::Meta] {
        let a = KeyRange::prefix(ks.prefix("customer-1"));
        assert!(a.contains(&ks.key("customer-1", b"store-A/x")));
        assert!(!a.contains(&ks.key("customer-1/store-A", b"x")));
    }
}

#[test]
fn keyspaces_and_buckets_are_disjoint_and_ordered() {
    let l1 = KeyRange::prefix(Keyspace::L1.prefix("ns"));
    assert!(!l1.contains(&Keyspace::Meta.key("ns", b"k")));
    assert!(!l1.contains(&Keyspace::L2 { bucket: 0 }.key("ns", b"k")));
    assert!(Keyspace::L2 { bucket: 9 }.key("ns", b"") < Keyspace::L2 { bucket: 10 }.key("ns", b""));
    assert!(
        !KeyRange::prefix(Keyspace::L2 { bucket: 1 }.prefix("ns"))
            .contains(&Keyspace::L2 { bucket: 256 }.key("ns", b""))
    );
}
