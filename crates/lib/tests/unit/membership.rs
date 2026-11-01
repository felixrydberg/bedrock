use super::*;

const GRACE: Duration = Duration::from_secs(15);

fn relays() -> Vec<String> {
    ["a", "b", "c"].map(String::from).to_vec()
}

fn entry(node_id: &str, status: pb::member::Status) -> pb::Member {
    pb::Member {
        node_id: node_id.into(),
        service: "kv".into(),
        cell: "eu-1a".into(),
        status: status as i32,
        address: format!("{node_id}:7000"),
        ..Default::default()
    }
}

fn members(entries: &[pb::Member]) -> Members {
    entries.iter().map(|m| (key(m), m.clone())).collect()
}

fn ready(node_id: &str) -> pb::Member {
    entry(node_id, pb::member::Status::Ready)
}

/// Relay `me`'s plan, given its own members and its siblings' lists (`None`: unreachable).
fn run(
    me: &str,
    local: &[pb::Member],
    remote: &[(&str, Option<&[pb::Member]>)],
    actual: &[pb::Member],
    absent: &mut BTreeMap<MemberKey, Instant>,
    now: Instant,
) -> Option<Vec<Change>> {
    let relays = relays();
    let remote = remote
        .iter()
        .map(|(relay, list)| (relay.to_string(), list.map(members)))
        .collect();
    let seen = Observed {
        me,
        relays: &relays,
        cell: "eu-1a",
        local: &members(local),
        remote: &remote,
    };
    plan(&seen, &members(actual), absent, now, GRACE)
}

#[tokio::test]
async fn only_the_lowest_relay_of_a_majority_publishes() {
    let now = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let node = [ready("kv-1")];
    // Alone of three: frozen.
    assert_eq!(
        run(
            "a",
            &node,
            &[("b", None), ("c", None)],
            &[],
            &mut absent,
            now
        ),
        None
    );
    // A majority whose lowest member is a sibling: that sibling publishes.
    assert_eq!(
        run(
            "b",
            &node,
            &[("a", Some(&[])), ("c", None)],
            &[],
            &mut absent,
            now
        ),
        None
    );
    // The lowest of the majority side publishes, even though a lower relay exists elsewhere.
    assert_eq!(
        run(
            "b",
            &node,
            &[("a", None), ("c", Some(&[]))],
            &[],
            &mut absent,
            now
        ),
        Some(vec![Change::Set(ready("kv-1"))])
    );
}

#[tokio::test]
async fn failover_between_relays_changes_nothing() {
    let now = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let published = [ready("kv-1")];
    // The node moved from relay `a` to sibling `c`; the entry is identical, so nothing is written.
    assert_eq!(
        run(
            "a",
            &[],
            &[("b", Some(&[])), ("c", Some(&published))],
            &published,
            &mut absent,
            now
        ),
        Some(Vec::new())
    );
    assert!(absent.is_empty());
}

#[tokio::test]
async fn an_absent_member_is_removed_after_the_grace_period() {
    let start = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let published = [ready("kv-1")];
    let siblings: &[(&str, Option<&[pb::Member]>)] = &[("b", Some(&[])), ("c", None)];
    assert_eq!(
        run("a", &[], siblings, &published, &mut absent, start),
        Some(Vec::new())
    );
    let almost = start + GRACE - Duration::from_millis(1);
    assert_eq!(
        run("a", &[], siblings, &published, &mut absent, almost),
        Some(Vec::new())
    );
    // Seen again: the clock restarts.
    assert_eq!(
        run("a", &published, siblings, &published, &mut absent, almost),
        Some(Vec::new())
    );
    assert!(absent.is_empty());
    assert_eq!(
        run("a", &[], siblings, &published, &mut absent, almost),
        Some(Vec::new())
    );
    let expired = almost + GRACE;
    assert_eq!(
        run("a", &[], siblings, &published, &mut absent, expired),
        Some(vec![Change::Delete(key(&ready("kv-1")))])
    );
}

#[tokio::test]
async fn clocks_run_without_quorum_and_a_drained_member_leaves_at_once() {
    let start = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let published = [ready("kv-1"), entry("kv-2", pb::member::Status::Draining)];
    let left = [entry("kv-2", pb::member::Status::Left)];
    // Frozen, yet the absence of kv-1 is timed.
    assert_eq!(
        run(
            "a",
            &left,
            &[("b", None), ("c", None)],
            &published,
            &mut absent,
            start
        ),
        None
    );
    assert_eq!(absent.len(), 2);
    assert_eq!(
        run(
            "a",
            &left,
            &[("b", Some(&[])), ("c", None)],
            &published,
            &mut absent,
            start + GRACE
        ),
        Some(vec![
            Change::Delete(key(&ready("kv-1"))),
            Change::Delete(key(&left[0])),
        ])
    );
}

#[tokio::test]
async fn status_changes_publish_and_foreign_entries_are_ignored() {
    let now = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let draining = entry("kv-1", pb::member::Status::Draining);
    let foreign = pb::Member {
        cell: "eu-1b".into(),
        ..ready("kv-9")
    };
    assert_eq!(
        run(
            "a",
            &[draining.clone(), foreign],
            &[("b", Some(&[])), ("c", Some(&[]))],
            &[ready("kv-1")],
            &mut absent,
            now
        ),
        Some(vec![Change::Set(draining)])
    );
}

#[test]
fn announced_entries_are_validated() {
    assert!(validate(&ready("kv-1")).is_ok());
    assert!(validate(&entry("kv-1", pb::member::Status::Left)).is_err());
    for service in ["quicksilver", "cdn"] {
        let member = pb::Member {
            service: service.into(),
            ..ready("kv-1")
        };
        assert!(validate(&member).is_ok());
    }
    assert!(
        validate(&pb::Member {
            service: "auth".into(),
            ..ready("kv-1")
        })
        .is_err()
    );
    assert!(validate(&ready("kv/1")).is_err());
    assert!(
        validate(&pb::Member {
            status: 9,
            ..ready("kv-1")
        })
        .is_err()
    );
}

#[test]
fn directory_entries_sit_beside_members_in_one_namespace() {
    let (view, _) = View::new("cdn", "eu-1a");
    let engine = &view.published.engine;
    let at = |key: &str| [b"cdn.eu-1a\0".as_slice(), key.as_bytes()].concat();
    let member = pb::Member {
        service: "cdn".into(),
        ..ready("cdn-1")
    };
    engine.put(&at("cdn-1"), &member.encode_to_vec()).unwrap();
    let owners = vec![
        Owner {
            node: "cdn-1".into(),
            state: State::Ready,
        },
        Owner {
            node: "cdn-2".into(),
            state: State::Leaving,
        },
    ];
    engine
        .put(&at("range/0000000000000000"), &encode(&owners[..1]))
        .unwrap();
    engine
        .put(&at("range/8000000000000000"), &encode(&owners))
        .unwrap();
    assert_eq!(
        view.members().unwrap().into_keys().collect::<Vec<_>>(),
        ["cdn-1"]
    );
    let directory = view.directory().unwrap();
    assert_eq!(directory[&0], owners[..1]);
    assert_eq!(directory[&(1 << 63)], owners);
    // An owner state this build does not know is corruption, not an empty range.
    let unknown = pb::Range {
        owners: vec![pb::range::Owner {
            node_id: "cdn-1".into(),
            state: 7,
        }],
    };
    engine
        .put(&at("range/0000000000000000"), &unknown.encode_to_vec())
        .unwrap();
    assert_eq!(view.directory().unwrap_err().code(), tonic::Code::DataLoss);
}

/// The publisher's directory passes for `service` until nothing changes; returns the number of passes.
fn settle(service: &str, actual: &Members, directory: &mut Directory) -> usize {
    for pass in 0..100 {
        let changes = place(service, actual, directory);
        if changes.is_empty() {
            return pass;
        }
        for change in changes {
            let Change::Range { start, owners, .. } = change else {
                panic!("placement wrote {change:?}");
            };
            directory.insert(start, owners);
        }
    }
    panic!("placement did not settle");
}

#[test]
fn placement_promotes_then_assigns_only_its_own_service() {
    let node = |id: &str, service: &str, domain: &str| pb::Member {
        service: service.into(),
        roles: vec!["l2".into()],
        failure_domain: domain.into(),
        ..ready(id)
    };
    let actual = members(&[
        node("cdn-1", "cdn", "a"),
        node("cdn-2", "cdn", "b"),
        node("cdn-3", "cdn", "c"),
        node("cdn-4", "cdn", "a"),
        node("qs-1", "quicksilver", "d"),
    ]);
    let mut directory = Directory::new();
    let first = place("cdn", &actual, &directory);
    let Some(Change::Range { owners, .. }) = first.first() else {
        panic!("nothing assigned: {first:?}");
    };
    assert!(owners.iter().all(|o| o.state == State::Populating));
    assert!(settle("cdn", &actual, &mut directory) >= 2);
    for owners in directory.values() {
        assert_eq!(owners.len(), REPLICAS);
        assert!(owners.iter().all(|o| o.state == State::Ready));
        assert!(owners.iter().all(|o| o.node.starts_with("cdn-")));
    }
    // A removed member's ranges are reassigned, still within the service.
    let mut removed = actual.clone();
    removed.remove(&("cdn".to_string(), "cdn-2".to_string()));
    settle("cdn", &removed, &mut directory);
    for owners in directory.values() {
        assert_eq!(owners.len(), REPLICAS);
        assert!(
            owners
                .iter()
                .all(|o| o.node != "cdn-2" && o.state == State::Ready)
        );
    }
}
