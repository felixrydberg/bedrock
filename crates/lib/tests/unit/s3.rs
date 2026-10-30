use super::*;

#[test]
fn checksum_validation_rejects_corruption_missing_metadata_and_invalid_digest() {
    let expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    verify_checksum("artifact", b"hello", Some(expected)).unwrap();
    for (body, metadata) in [
        (b"hello".as_slice(), None),
        (b"changed".as_slice(), Some(expected)),
        (b"hello".as_slice(), Some("invalid")),
    ] {
        assert!(matches!(
            verify_checksum("artifact", body, metadata),
            Err(StorageError::Corruption(_))
        ));
    }
    verify_checksum(
        "empty",
        b"",
        Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    )
    .unwrap();
}
