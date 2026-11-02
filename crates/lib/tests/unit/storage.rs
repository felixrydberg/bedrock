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

#[test]
fn l2_keys_sort_by_hash_across_namespaces() {
    let low = l2_key(5, "b", b"k");
    let high = l2_key(6, "a", b"k");
    assert!(low < high);
    assert!(l2_span(5, 6).contains(&low) && !l2_span(5, 6).contains(&high));
    assert!(l2_span(6, 0).contains(&high) && l2_span(6, 0).contains(&l2_key(u64::MAX, "z", b"")));
    assert!(!l2_span(0, 0).contains(b"l3/"));
    assert_eq!(l2_parts(&low), Some((&b"b"[..], &b"k"[..])));
    assert_eq!(l2_parts(b"l1/x"), None);
}
