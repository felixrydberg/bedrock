use super::*;
use bedrock_lib::placement::{owners, serves};

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

/// Completes every handoff, as the publisher's next pass does once every new owner's relay reports it ready.
fn settle(directory: &mut Directory, members: &BTreeMap<String, pb::Member>) {
    let ready = directory
        .iter()
        .flat_map(|(start, owners)| owners.iter().map(move |o| (o.node.clone(), *start)))
        .collect();
    let done = handoff(directory, members, &ready);
    directory.extend(done);
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
        settle(directory, members);
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
        for owner in owners.iter().filter(|o| holds(o)) {
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
    settle(&mut directory, &joined);
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
    settle(&mut directory, &removed);
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
        settle(&mut directory, &members);
    }
    // What they held hands off (#68 / B-21b).
    assert_eq!(loads(&directory).get("n00001").copied().unwrap_or(0), 0);
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
fn replaces_a_co_located_owner_when_another_domain_is_available() {
    let members = (0..4)
        .map(|i| {
            let id = format!("n{i:05}");
            let domain = if i < 3 { "d0" } else { "d1" };
            (id.clone(), node(&id, domain, 4.0, 1000))
        })
        .collect();
    let directory = Directory::from([(
        0,
        (0..3)
            .map(|i| Owner {
                node: format!("n{i:05}"),
                state: State::Ready,
            })
            .collect(),
    )]);

    let changes = assign(&directory, &members, &BTreeSet::new(), R, 1);
    let owners = &changes[&0];
    let held: Vec<_> = owners.iter().filter(|o| holds(o)).collect();
    let domains: BTreeSet<_> = held
        .iter()
        .map(|o| members[&o.node].failure_domain.as_str())
        .collect();
    assert_eq!(held.len(), R);
    assert_eq!(domains.len(), 2);
    assert_eq!(moving(&changes), 1);
    assert_eq!(
        owners.iter().filter(|o| o.state == State::Leaving).count(),
        1
    );
    assert_eq!(
        owners
            .iter()
            .filter(|o| o.state == State::Populating)
            .count(),
        1
    );
}

#[test]
fn scales_down_with_a_bounded_leaving_handoff() {
    let members = cell(4, 2);
    let directory = Directory::from([(
        0,
        (0..4)
            .map(|i| Owner {
                node: format!("n{i:05}"),
                state: State::Ready,
            })
            .collect(),
    )]);

    let changes = assign(&directory, &members, &BTreeSet::new(), R, 1);
    let owners = &changes[&0];
    assert_eq!(owners.iter().filter(|o| holds(o)).count(), R);
    assert_eq!(
        owners.iter().filter(|o| o.state == State::Leaving).count(),
        1
    );
    assert_eq!(moving(&changes), 1);
    let mut settled = changes;
    settle(&mut settled, &members);
    assert_eq!(settled[&0].len(), R);
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
fn handoff_marks_only_reported_owners_ready_and_never_leaves_a_range_without_a_holder() {
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
    let members = BTreeMap::from([
        ("a".into(), node("a", "a", 4.0, 1000)),
        ("b".into(), node("b", "b", 4.0, 1000)),
    ]);
    let ready = BTreeSet::from([("b".to_string(), 10)]);
    assert_eq!(
        handoff(&directory, &members, &ready),
        Directory::from([(10, vec![owner("b", State::Ready)])])
    );
    // Until its relay reports it ready, nothing moves and the leaving owner keeps the range.
    assert!(handoff(&directory, &members, &BTreeSet::new()).is_empty());
    assert!(
        handoff(
            &directory,
            &members,
            &BTreeSet::from([("b".to_string(), 0)])
        )
        .is_empty(),
        "readiness is per range"
    );
    // A ready owner that left the membership does not release the leaving one.
    let gone = BTreeMap::from([("a".into(), members["a"].clone())]);
    assert_eq!(
        handoff(&directory, &gone, &ready),
        Directory::from([(
            10,
            vec![owner("a", State::Leaving), owner("b", State::Ready)]
        )])
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
    let mut during_handoff = stale.clone();
    during_handoff.extend(moving.clone());
    for start in moving.keys() {
        for owner in owners(&stale, *start) {
            assert!(serves(&during_handoff, &owner.node, *start));
        }
    }
    // A populating owner may not hold the data yet.
    for (start, owners) in &moving {
        for owner in owners.iter().filter(|o| o.state == State::Populating) {
            assert!(!serves(&during_handoff, &owner.node, *start));
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

#[test]
fn handoff_keeps_leaving_owner_until_all_replacements_are_ready() {
    let owner = |node: &str, state| Owner {
        node: node.into(),
        state,
    };
    let members = BTreeMap::from([
        ("old".into(), node("old", "a", 4.0, 1000)),
        ("new-1".into(), node("new-1", "b", 4.0, 1000)),
        ("new-2".into(), node("new-2", "c", 4.0, 1000)),
    ]);
    let mut directory = Directory::from([(
        10,
        vec![
            owner("old", State::Leaving),
            owner("new-1", State::Populating),
            owner("new-2", State::Populating),
        ],
    )]);
    let first = handoff(
        &directory,
        &members,
        &BTreeSet::from([("new-1".into(), 10)]),
    );
    assert_eq!(
        first[&10],
        vec![
            owner("old", State::Leaving),
            owner("new-1", State::Ready),
            owner("new-2", State::Populating),
        ]
    );
    directory.extend(first);
    assert!(serves(&directory, "old", 10));
    assert!(serves(&directory, "new-1", 10));
    assert!(!serves(&directory, "new-2", 10));

    let second = handoff(
        &directory,
        &members,
        &BTreeSet::from([("new-2".into(), 10)]),
    );
    assert_eq!(
        second[&10],
        vec![owner("new-1", State::Ready), owner("new-2", State::Ready)]
    );
    directory.extend(second);
    assert!(!serves(&directory, "old", 10));
    assert!(serves(&directory, "new-2", 10));
}

/// #66 / B-61: neighbours with the same ready owners merge, whatever order they are listed in, and a chain merges into its
/// first range; a range mid-handoff, or with different owners, stays apart.
#[test]
fn neighbours_with_the_same_ready_owners_merge() {
    let owners = |nodes: &[&str], state: State| -> Vec<Owner> {
        nodes
            .iter()
            .map(|node| Owner {
                node: (*node).into(),
                state,
            })
            .collect()
    };
    let directory: Directory = BTreeMap::from([
        (0, owners(&["a", "b"], State::Ready)),
        (10, owners(&["b", "a"], State::Ready)),
        (20, owners(&["a", "b"], State::Ready)),
        (30, owners(&["a", "c"], State::Ready)),
        (40, owners(&["a", "c"], State::Populating)),
        (50, owners(&["a", "c"], State::Ready)),
        (60, Vec::new()),
        (70, Vec::new()),
    ]);
    assert_eq!(merges(&directory), [10, 20]);
    assert!(merges(&BTreeMap::new()).is_empty());
}

#[test]
fn a_drained_node_hands_its_ranges_off_within_the_rate_and_cancelling_stops_it() {
    use bedrock_lib::reconciliation::drain::{self, Drain, Scope, State as Drained};
    let members = cell(12, 4);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let held = |directory: &Directory| {
        directory
            .values()
            .flatten()
            .filter(|o| o.node == "n00001")
            .count()
    };
    assert!(held(&directory) > 0);
    let record = Drain {
        scope: Scope::Node("n00001".into()),
        datasets: Vec::new(),
        namespace: None,
        state: Drained::Draining,
        rate: 2,
    };
    for _ in 0..200 {
        let drained = drain::apply(&members, std::slice::from_ref(&record), "quicksilver");
        let changes = assign(&directory, &drained, &BTreeSet::new(), R, 2);
        directory.extend(changes);
        assert!(moving(&directory) <= 2, "moves exceeded the drain rate");
        settle(&mut directory, &members);
        if held(&directory) == 0 {
            break;
        }
    }
    assert_eq!(held(&directory), 0, "the node still holds ranges");
    for owners in directory.values() {
        assert_eq!(owners.len(), R, "a drain cost a range its redundancy");
    }
    // Cancelled: the record is gone and the node takes ranges again.
    converge(&mut directory, &members, &BTreeSet::new());
    assert!(loads(&directory)["n00001"] > 0);
}

#[test]
fn a_drain_with_no_member_to_take_the_range_keeps_the_owner() {
    let mut members = cell(3, 3);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    members.get_mut("n00001").unwrap().status = pb::member::Status::Draining as i32;
    let changes = assign(&directory, &members, &BTreeSet::new(), R, usize::MAX);
    assert!(
        changes.is_empty(),
        "the only copy of a range left: {changes:?}"
    );
}

/// Stored bytes of the volume test: the first 4 of 64 equal hash buckets hold 64 GiB each, the rest 1 GiB, each spread
/// evenly within its bucket.
fn stored_below(x: u128) -> f64 {
    let (buckets, width) = (64, SPACE / 64);
    let bytes = |i: u128| {
        if i < 4 {
            64.0 * (1u64 << 30) as f64
        } else {
            (1u64 << 30) as f64
        }
    };
    let full = x / width;
    let part = if full < buckets {
        bytes(full) * (x % width) as f64 / width as f64
    } else {
        0.0
    };
    (0..full).map(bytes).sum::<f64>() + part
}

fn stored(start: u128, end: u128) -> f64 {
    stored_below(end) - stored_below(start)
}

/// What each volume node publishes: the size and byte middle of every range it holds ready or leaving.
fn measure(directory: &Directory, members: &mut BTreeMap<String, pb::Member>) {
    for member in members.values_mut() {
        member.sizes.clear();
    }
    for (&start, owners) in directory {
        let (from, end) = (
            u128::from(start),
            u128::from(start) + size(directory, start),
        );
        let bytes = stored(from, end);
        let (mut low, mut high) = (from, end);
        while high - low > 1 {
            let mid = (low + high) / 2;
            if stored(from, mid) * 2.0 < bytes {
                low = mid
            } else {
                high = mid
            }
        }
        for owner in owners.iter().filter(|o| o.state != State::Populating) {
            members
                .get_mut(&owner.node)
                .unwrap()
                .sizes
                .push(pb::RangeSize {
                    span: Some(pb::Span {
                        start,
                        end: end as u64,
                    }),
                    bytes: bytes as u64,
                    middle: if high < end { high as u64 } else { 0 },
                });
        }
    }
}

#[test]
fn volume_members_balance_on_skewed_stored_bytes() {
    let mut members = cell(12, 4);
    for member in members.values_mut() {
        member.service = "volume".into();
    }
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    let count = members.len();
    // Placed by hash span, the members holding the hot buckets hold far more than their share of bytes.
    let shares = |directory: &Directory| {
        let mut held = BTreeMap::<String, f64>::new();
        for (&start, owners) in directory {
            let from = u128::from(start);
            for owner in owners.iter().filter(|o| holds(o)) {
                *held.entry(owner.node.clone()).or_default() +=
                    stored(from, from + size(directory, start));
            }
        }
        let target = R as f64 * stored(0, SPACE) / count as f64;
        held.into_values()
            .map(|bytes| bytes / target)
            .collect::<Vec<_>>()
    };
    assert!(shares(&directory).iter().any(|s| *s > 1.15));

    // Each move: the hashes, the owners leaving them and the owners taking them.
    type Move = (u128, u128, BTreeSet<String>, BTreeSet<String>);
    let mut moved_before: Vec<Move> = Vec::new();
    for pass in 0.. {
        assert!(pass < 100, "placement did not converge on bytes");
        measure(&directory, &mut members);
        let changes = assign(&directory, &members, &BTreeSet::new(), R, 16);
        // The same directory, membership and sizes give the same plan.
        assert_eq!(
            changes,
            assign(&directory, &members, &BTreeSet::new(), R, 16)
        );
        if changes.is_empty() {
            break;
        }
        directory.extend(changes.clone());
        assert!(moving(&directory) <= 16);
        let nodes = |owners: &[Owner], state| {
            owners
                .iter()
                .filter(|o| o.state == state)
                .map(|o| o.node.clone())
                .collect::<BTreeSet<_>>()
        };
        let moved: Vec<Move> = changes
            .iter()
            .map(|(start, owners)| {
                let from = u128::from(*start);
                (
                    from,
                    from + size(&directory, *start),
                    nodes(owners, State::Leaving),
                    nodes(owners, State::Populating),
                )
            })
            .collect();
        // With the stored bytes unchanged, a pass never moves hashes the previous pass moved off a member that just took
        // them, or back to one that just gave them up. Pass 0 stops at the cap of 16 moves, so other replicas of the
        // same hashes may still move.
        for (a, b, leaving, taking) in &moved {
            for (c, d, gave, took) in &moved_before {
                let overlap = a < d && c < b;
                assert!(
                    !overlap || (took.is_disjoint(leaving) && gave.is_disjoint(taking)),
                    "pass {pass} moved [{a:x}, {b:x}) back and forth"
                );
            }
        }
        moved_before = moved;
        settle(&mut directory, &members);
    }
    for share in shares(&directory) {
        assert!(
            (0.85..=1.15).contains(&share),
            "a member holds {share:.3} of its byte share"
        );
    }
}

#[test]
fn ranges_without_a_reported_size_count_by_hash_span() {
    let mut members = cell(12, 4);
    let mut directory = Directory::new();
    converge(&mut directory, &members, &BTreeSet::new());
    // Every range reports bytes in proportion to its span: the same plan as with no sizes at all, which moves nothing.
    measure(&directory, &mut members);
    for member in members.values_mut() {
        for size in &mut member.sizes {
            let span = size.span.unwrap();
            let end = if span.end == 0 {
                SPACE
            } else {
                u128::from(span.end)
            };
            size.bytes = ((end - u128::from(span.start)) >> 24) as u64;
            size.middle = 0;
        }
    }
    assert!(assign(&directory, &members, &BTreeSet::new(), R, 16).is_empty());
    // Dropping half of the reports changes nothing either: unreported ranges weigh their span at the same density.
    for member in members.values_mut().step_by(2) {
        member.sizes.clear();
    }
    assert!(assign(&directory, &members, &BTreeSet::new(), R, 16).is_empty());
}
