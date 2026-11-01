use super::*;
use crate::buckets::hash_of;

const R: usize = 3;

fn node(id: &str, domain: &str, cpus: f64, disk_mb: u64) -> pb::Member {
    pb::Member {
        node_id: id.into(),
        service: "quicksilver".into(),
        cell: "eu-1a".into(),
        roles: vec!["l2".into()],
        cpus,
        disk_mb,
        failure_domain: domain.into(),
        status: pb::member::Status::Ready as i32,
        ..Default::default()
    }
}

/// `n` equal members in `domains` failure domains of consecutive IDs, so ID order alone never spreads owners.
fn cell(n: usize, domains: usize) -> BTreeMap<String, pb::Member> {
    (0..n)
        .map(|i| {
            let id = format!("n{i:05}");
            let member = node(&id, &format!("d{}", i * domains / n), 4.0, 1000);
            (id, member)
        })
        .collect()
}

/// Completes every handoff, as the publisher's next pass does.
fn settle(directory: &mut Directory) {
    let promoted = promote(directory);
    directory.extend(promoted);
}

/// Plans, writes and completes handoffs until nothing changes.
fn converge(
    directory: &mut Directory,
    members: &BTreeMap<String, pb::Member>,
    unhealthy: &BTreeSet<String>,
) {
    for _ in 0..50 {
        let changes = assign(directory, members, unhealthy, R, usize::MAX);
        if changes.is_empty() {
            return;
        }
        directory.extend(changes);
        settle(directory);
    }
    panic!("placement did not converge");
}

fn size(directory: &Directory, start: u64) -> u128 {
    let end = directory
        .range((Excluded(start), Unbounded))
        .next()
        .map_or(SPACE, |(next, _)| u128::from(*next));
    end - u128::from(start)
}

fn loads(directory: &Directory) -> BTreeMap<String, u128> {
    let mut loads = BTreeMap::new();
    for (start, owners) in directory {
        for owner in owners.iter().filter(|o| o.holds()) {
            *loads.entry(owner.node.clone()).or_default() += size(directory, *start);
        }
    }
    loads
}

fn moving(directory: &Directory) -> usize {
    directory
        .values()
        .filter(|owners| owners.iter().any(|o| o.state != State::Ready))
        .count()
}

/// Every range has `R` ready owners in distinct domains, and every member holds within 15% of its scarcest-resource
/// share.
fn assert_placed(directory: &Directory, members: &BTreeMap<String, pb::Member>) {
    assert!(directory.contains_key(&0));
    for owners in directory.values() {
        assert_eq!(owners.len(), R);
        let domains: BTreeSet<_> = owners
            .iter()
            .map(|o| &members[&o.node].failure_domain)
            .collect();
        assert_eq!(domains.len(), R, "owners share a failure domain");
    }
    let (cpus, disk): (f64, f64) = members
        .values()
        .map(|m| (m.cpus, m.disk_mb as f64))
        .fold((0.0, 0.0), |a, b| (a.0 + b.0, a.1 + b.1));
    let weight = |m: &pb::Member| (m.cpus / cpus).min(m.disk_mb as f64 / disk);
    let total: f64 = members.values().map(weight).sum();
    let loads = loads(directory);
    for (id, member) in members {
        let target = weight(member) / total * R as f64 * SPACE as f64;
        let share = loads.get(id).copied().unwrap_or(0) as f64 / target;
        assert!(
            (0.85..=1.15).contains(&share),
            "{id} holds {share:.3} of its target"
        );
    }
}

/// The range of `before` that covers `start`.
fn covering(before: &Directory, start: u64) -> &Vec<Owner> {
    before.range(..=start).next_back().unwrap().1
}

#[test]
fn bootstraps_an_empty_directory() {
    let members = cell(12, 4);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    assert_placed(&directory, &members);
}

#[test]
fn the_same_inputs_give_the_same_changes() {
    let members = cell(40, 5);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let mut joined = members.clone();
    joined.insert("new".into(), node("new", "d9", 4.0, 1000));
    let first = assign(&directory, &joined, &BTreeSet::new(), R, 8);
    assert!(!first.is_empty());
    assert_eq!(first, assign(&directory, &joined, &BTreeSet::new(), R, 8));
}

#[test]
fn a_join_moves_only_ranges_the_joiner_takes() {
    let members = cell(40, 5);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let mut joined = members.clone();
    joined.insert("new".into(), node("new", "d2", 4.0, 1000));
    let changes = assign(&directory, &joined, &BTreeSet::new(), R, usize::MAX);
    assert!(!changes.is_empty());
    for (start, owners) in &changes {
        let before = covering(&directory, *start);
        let added: Vec<_> = owners.iter().filter(|o| !before.contains(o)).collect();
        let leaving = owners.iter().filter(|o| o.state == State::Leaving).count();
        // Either a plain split, or the joiner replacing one owner.
        assert!(added.is_empty() || leaving == 1, "{owners:?}");
        assert!(
            added
                .iter()
                .all(|o| o.node == "new" || o.state == State::Leaving)
        );
    }
    directory.extend(changes);
    settle(&mut directory);
    converge(&mut directory, &joined, &BTreeSet::new());
    assert_placed(&directory, &joined);
}

#[test]
fn a_removal_moves_only_the_removed_members_ranges() {
    let members = cell(40, 5);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let mut removed = members.clone();
    removed.remove("n00007");
    let changes = assign(&directory, &removed, &BTreeSet::new(), R, usize::MAX);
    assert!(!changes.is_empty());
    for (start, owners) in &changes {
        let before = covering(&directory, *start);
        assert!(before.iter().any(|o| o.node == "n00007"));
        let kept: Vec<_> = before.iter().filter(|o| o.node != "n00007").collect();
        assert!(kept.iter().all(|o| owners.contains(o)));
        assert_eq!(owners.len(), R);
        assert!(
            owners
                .iter()
                .all(|o| kept.contains(&o) || o.state == State::Populating)
        );
    }
    directory.extend(changes);
    settle(&mut directory);
    converge(&mut directory, &removed, &BTreeSet::new());
    assert_placed(&directory, &removed);
}

#[test]
fn draining_unhealthy_and_l1_members_get_nothing_new() {
    let mut members = cell(40, 5);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    members.get_mut("n00001").unwrap().status = pb::member::Status::Draining as i32;
    members.get_mut("n00002").unwrap().roles = vec!["l1".into()];
    let unhealthy = BTreeSet::from(["n00003".to_string()]);
    members.remove("n00004");
    members.insert("new".into(), node("new", "d1", 4.0, 1000));
    for _ in 0..10 {
        let changes = assign(&directory, &members, &unhealthy, R, usize::MAX);
        for owners in changes.values() {
            for owner in owners.iter().filter(|o| o.state == State::Populating) {
                assert!(!["n00001", "n00002", "n00003"].contains(&owner.node.as_str()));
            }
        }
        directory.extend(changes);
        settle(&mut directory);
    }
    // They keep what they held: moving it away is draining (B-21b), not assignment.
    assert!(loads(&directory)["n00001"] > 0);
}

#[test]
fn shares_a_failure_domain_only_when_there_are_too_few() {
    let members = cell(6, 2);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    for owners in directory.values() {
        assert_eq!(owners.len(), R);
        let domains: BTreeSet<_> = owners
            .iter()
            .map(|o| &members[&o.node].failure_domain)
            .collect();
        assert_eq!(domains.len(), 2);
    }
}

#[test]
fn bounds_the_ranges_moving_at_once() {
    let members = cell(40, 5);
    let mut directory = Directory::new();
    for _ in 0..3 {
        directory.extend(assign(&directory, &members, &BTreeSet::new(), R, 4));
        assert!(moving(&directory) <= 4, "{}", moving(&directory));
    }
    // A removal repairs within the bound too.
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let mut removed = members.clone();
    removed.remove("n00007");
    directory.extend(assign(&directory, &removed, &BTreeSet::new(), R, 2));
    assert_eq!(moving(&directory), 2);
}

#[test]
fn balances_ten_thousand_weighted_members() {
    let members: BTreeMap<String, pb::Member> = (0..10_000)
        .map(|i| {
            let id = format!("n{i:05}");
            let cpus = [2.0, 4.0, 8.0, 16.0][i % 4];
            let disk = [500_000, 1_000_000, 2_000_000][i % 3];
            (
                id.clone(),
                node(&id, &format!("rack{}", i % 50), cpus, disk),
            )
        })
        .collect();
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    assert_placed(&directory, &members);
    // A few ranges per member, not a ring of tokens.
    assert!(
        directory.len() < 5 * members.len(),
        "{} ranges",
        directory.len()
    );
}

#[test]
fn promotion_never_leaves_a_range_without_a_holder() {
    let owner = |node: &str, state| Owner {
        node: node.into(),
        state,
    };
    let directory = Directory::from([
        (0, vec![owner("a", State::Ready)]),
        (
            10,
            vec![owner("a", State::Leaving), owner("b", State::Populating)],
        ),
        (20, vec![owner("a", State::Leaving)]),
    ]);
    assert_eq!(
        promote(&directory),
        Directory::from([(10, vec![owner("b", State::Ready)])])
    );
}

#[test]
fn a_stale_route_is_rejected_and_the_retry_succeeds() {
    let members = cell(40, 5);
    let mut stale = Directory::new();
    converge(&mut stale, &members, &BTreeSet::new());
    let mut joined = members.clone();
    joined.insert("new".into(), node("new", "d2", 4.0, 1000));
    let mut current = stale.clone();
    converge(&mut current, &joined, &BTreeSet::new());
    let (start, _) = current
        .iter()
        .find(|(_, owners)| owners.iter().any(|o| o.node == "new"))
        .unwrap();
    let hash = *start;
    // During the handoff the leaving owner still serves.
    let moving = assign(&stale, &joined, &BTreeSet::new(), R, usize::MAX);
    for (start, owners) in &moving {
        for owner in owners {
            assert!(serves(&moving, &owner.node, *start));
        }
    }
    // Routed by the old view to the owner `new` replaced, the request is rejected; the next old owner serves it.
    let old = owners(&stale, hash);
    let gone = old
        .iter()
        .position(|o| !serves(&current, &o.node, hash))
        .expect("the join moved an owner");
    let retry = old.iter().skip(gone + 1).chain(&old[..gone]);
    assert!(retry.take(1).all(|o| serves(&current, &o.node, hash)));
    assert!(!serves(&current, "nobody", hash));
    assert!(owners(&Directory::new(), hash).is_empty());
}

fn kv(n: usize) -> Vec<pb::Member> {
    (0..n)
        .map(|i| node(&format!("kv-{i}"), "", 0.0, 0))
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
