use super::*;

#[test]
fn prefix_range_end() {
    assert_eq!(KeyRange::prefix(*b"ab").end, Some(b"ac".to_vec()));
    assert_eq!(KeyRange::prefix([b'a', 0xFF]).end, Some(b"b".to_vec()));
    assert_eq!(KeyRange::prefix([0xFF, 0xFF]).end, None);
    assert_eq!(KeyRange::prefix(Vec::new()), KeyRange::all());

    let r = KeyRange::prefix([b'a', 0xFF]);
    assert!(r.contains(&[b'a', 0xFF]) && r.contains(&[b'a', 0xFF, 0xFF, 7]));
    assert!(!r.contains(b"a") && !r.contains(b"b"));
}

#[test]
fn inverted_range_is_empty() {
    assert!(KeyRange::new(*b"b", *b"a").bounds().is_none());
    assert!(KeyRange::new(*b"a", *b"a").bounds().is_none());
    assert!(!KeyRange::new(*b"b", *b"a").contains(b"b"));
}
