use super::*;

#[test]
fn namespace_actor_identity_uses_only_the_first_128_sha256_bits() {
    let regions =
        std::collections::BTreeMap::from([("eu".into(), crate::actors::Topology::default())]);
    let id = "11111111111111111111111111111111";
    let writer = actor(&regions, "eu", id).unwrap();
    // Independent Python hashlib.sha256 + RFC4648 base32 vector, not this module's encoder.
    assert_eq!(writer.id, "eu_rkbwmxzxtbzh6fhzflionsm73i");
    assert_eq!(writer.namespace, id);
    assert_eq!(writer.kind, Kind::Kv as i32);
}
