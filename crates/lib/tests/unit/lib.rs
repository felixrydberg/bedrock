use super::*;

#[test]
fn uuids_are_version_4_in_cloudflare_shape() {
    let id = uuid();
    assert_eq!(id.len(), 32);
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    assert_eq!(&id[12..13], "4");
    assert!("89ab".contains(&id[16..17]));
    assert_ne!(id, uuid());
}
