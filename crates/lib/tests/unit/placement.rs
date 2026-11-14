use super::*;

fn kv(n: usize) -> Vec<pb::Member> {
    (0..n)
        .map(|i| pb::Member {
            node_id: format!("kv-{i}"),
            ..Default::default()
        })
        .collect()
}

#[test]
fn every_node_ranks_readers_the_same_way() {
    let members = kv(20);
    let hash = hash_of("kv", "users", b"alice");
    let forward: Vec<_> = rank(hash, &members)
        .into_iter()
        .map(|m| &m.node_id)
        .collect();
    let backward: Vec<_> = rank(hash, members.iter().rev())
        .into_iter()
        .map(|m| &m.node_id)
        .collect();
    assert_eq!(forward, backward);
    assert_eq!(forward.len(), 20);
}

#[test]
fn removing_a_reader_moves_only_the_keys_it_won() {
    let members = kv(50);
    let without: Vec<_> = members.iter().filter(|m| m.node_id != "kv-7").collect();
    let mut counts = BTreeMap::<&str, usize>::new();
    for i in 0..100_000u32 {
        let hash = hash_of("kv", "users", &i.to_be_bytes());
        let before = &rank(hash, &members)[0].node_id;
        let after = &rank(hash, without.iter().copied())[0].node_id;
        if before != "kv-7" {
            assert_eq!(before, after);
        }
        *counts.entry(before).or_default() += 1;
    }
    // 2,000 keys each on average.
    assert!(
        counts.values().all(|c| (1_700..=2_300).contains(c)),
        "{counts:?}"
    );
}

/// Run with `cargo test -p bedrock-lib --release placement -- --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_ranking_ten_thousand_readers() {
    let members = kv(10_000);
    let start = crate::monotonic_now();
    let rounds = 200u64;
    for i in 0..rounds {
        std::hint::black_box(rank(hash_of("kv", "users", &i.to_be_bytes()), &members));
    }
    println!(
        "rank over 10,000 readers: {:?} per request",
        start.elapsed() / rounds as u32
    );
}

#[test]
fn hash_has_a_stable_cross_platform_vector() {
    assert_eq!(hash_of("auth", "users", b"key"), 0x65d9_029d_77ca_9355);
    assert_ne!(hash_of("ab", "c", b"key"), hash_of("a", "bc", b"key"));
    assert_ne!(
        hash_of("auth", "users", b"key"),
        hash_of("cdn", "users", b"key")
    );
}

#[test]
fn spans_are_half_open_and_end_zero_is_the_top() {
    let span = |start, end| pb::Span { start, end };
    assert!(in_span(&span(10, 20), 10) && !in_span(&span(10, 20), 20));
    assert!(in_span(&span(10, 0), u64::MAX) && !in_span(&span(10, 0), 9));
    assert!(valid_span(&span(0, 0)) && valid_span(&span(1, 2)));
    assert!(!valid_span(&span(2, 2)) && !valid_span(&span(3, 2)));
}
