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

#[test]
fn l2_spans_include_start_exclude_end_and_isolate_other_keyspaces() {
    let span = l2_span(255, 257);
    for namespace in ["", "a", "a/b"] {
        for hash in [255, 256] {
            assert!(span.contains(&l2_key(hash, namespace, b"\0binary")));
        }
        for hash in [254, 257] {
            assert!(!span.contains(&l2_key(hash, namespace, b"\0binary")));
        }
    }
    assert!(!span.contains(b"l1/anything"));
    assert!(!span.contains(b"l3/anything"));

    let last = l2_span(u64::MAX, 0);
    assert!(!last.contains(&l2_key(u64::MAX - 1, "ns", b"k")));
    assert!(last.contains(&l2_key(u64::MAX, "ns", b"k")));
    assert!(!last.contains(b"l3/"));
    assert!(l2_span(7, 7).bounds().is_none());
}

#[test]
fn l2_parts_preserves_binary_keys_and_rejects_truncated_records() {
    let physical = l2_key(0x0102_0304_0506_0708, "customer/store", b"\0key\xff");
    assert_eq!(
        physical,
        b"l2/\x01\x02\x03\x04\x05\x06\x07\x08/customer/store\0\0key\xff"
    );
    assert_eq!(
        l2_parts(&physical),
        Some((&b"customer/store"[..], &b"\0key\xff"[..]))
    );
    assert_eq!(l2_parts(&l2_key(0, "", b"")), Some((&b""[..], &b""[..])));
    for length in 0..=12 + "customer/store".len() {
        assert_eq!(l2_parts(&physical[..length]), None);
    }
    assert_eq!(l2_parts(b"l1/\x01\x02\x03\x04\x05\x06\x07\x08/ns\0k"), None);
    assert_eq!(l2_parts(b"l2/\x01\x02\x03\x04\x05\x06\x07\x08/ns"), None);
}
