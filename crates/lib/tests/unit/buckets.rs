use super::*;

#[test]
fn bucket_identity_has_a_stable_cross_platform_vector() {
    assert_eq!(hash_of("auth", "users", b"key"), 0x65d9_029d_77ca_9355);
    assert_eq!(bucket_of("auth", "users", b"key"), 590493);
    assert_ne!(bucket_of("ab", "c", b"key"), bucket_of("a", "bc", b"key"));
    assert_ne!(
        bucket_of("auth", "users", b"key"),
        bucket_of("cdn", "users", b"key")
    );
}
