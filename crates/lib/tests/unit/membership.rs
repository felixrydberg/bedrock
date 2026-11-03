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
    // Unhealthy datasets must be the member's own, in order and without repeats.
    let health = |unhealthy: &[&str]| {
        validate(&pb::Member {
            datasets: vec!["auth".into(), "quicksilver".into()],
            unhealthy: unhealthy.iter().map(|d| d.to_string()).collect(),
            ..ready("kv-1")
        })
    };
    assert!(health(&["auth", "quicksilver"]).is_ok());
    assert!(health(&["quicksilver", "auth"]).is_err());
    assert!(health(&["auth", "auth"]).is_err());
    assert!(health(&["cdn"]).is_err());
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
        .put(&at("range/cdn/0000000000000000"), &encode(&owners[..1]))
        .unwrap();
    engine
        .put(&at("range/cdn/8000000000000000"), &encode(&owners))
        .unwrap();
    engine
        .put(&at("range/other/0000000000000000"), &encode(&owners))
        .unwrap();
    assert_eq!(
        view.members().unwrap().into_keys().collect::<Vec<_>>(),
        ["cdn-1"]
    );
    let directory = view.directory("cdn").unwrap();
    assert_eq!(directory.len(), 2, "each dataset has its own ranges");
    assert_eq!(directory[&0], owners[..1]);
    assert_eq!(directory[&(1 << 63)], owners);
    assert!(view.directory("absent").unwrap().is_empty());
    for invalid in ["range/cdn/000000000000000A", "range/0000000000000000"] {
        engine.put(&at(invalid), &encode(&owners[..1])).unwrap();
        assert_eq!(view.members().unwrap().len(), 1);
        assert_eq!(
            view.directory("cdn").unwrap_err().code(),
            tonic::Code::DataLoss
        );
        engine.delete(&at(invalid)).unwrap();
    }
    // An owner state this build does not know is corruption, not an empty range.
    let unknown = pb::Range {
        owners: vec![pb::range::Owner {
            node_id: "cdn-1".into(),
            state: 7,
        }],
    };
    engine
        .put(&at("range/cdn/0000000000000000"), &unknown.encode_to_vec())
        .unwrap();
    assert_eq!(
        view.directory("cdn").unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

/// Every owner in `directories` as its relay would report it once its handoff completed.
fn all_ready(service: &str, directories: &Directories) -> siblings::Ready {
    let mut ready = siblings::Ready::new();
    for (dataset, directory) in directories {
        for (start, owners) in directory {
            for owner in owners {
                ready
                    .entry(((service.into(), owner.node.clone()), dataset.clone()))
                    .or_default()
                    .push((*start, 0));
            }
        }
    }
    ready
}

/// The publisher's directory passes for `service` until nothing changes, with every relay reporting its members
/// ready; returns the number of passes.
fn settle(service: &str, actual: &Members, directories: &mut Directories) -> usize {
    for pass in 0..100 {
        let ready = all_ready(service, directories);
        let changes = place(service, actual, directories, &ready);
        if changes.is_empty() {
            return pass;
        }
        for change in changes {
            let Change::Range {
                dataset,
                start,
                owners,
                ..
            } = change
            else {
                panic!("placement wrote {change:?}");
            };
            directories
                .entry(dataset)
                .or_default()
                .insert(start, owners);
        }
    }
    panic!("placement did not settle");
}

#[test]
fn placement_completes_handoffs_then_assigns_only_its_own_service() {
    let node = |id: &str, service: &str, domain: &str| pb::Member {
        service: service.into(),
        roles: vec!["l2".into()],
        failure_domain: domain.into(),
        datasets: vec![service.into()],
        ..ready(id)
    };
    let actual = members(&[
        node("cdn-1", "cdn", "a"),
        node("cdn-2", "cdn", "b"),
        node("cdn-3", "cdn", "c"),
        node("cdn-4", "cdn", "a"),
        // Carries no dataset, so it owns nothing.
        pb::Member {
            datasets: Vec::new(),
            ..node("cdn-0", "cdn", "d")
        },
        node("qs-1", "quicksilver", "d"),
    ]);
    let mut directories = Directories::new();
    let first = place("cdn", &actual, &directories, &siblings::Ready::new());
    let Some(Change::Range {
        dataset, owners, ..
    }) = first.first()
    else {
        panic!("nothing assigned: {first:?}");
    };
    assert_eq!(dataset, "cdn");
    assert!(owners.iter().all(|o| o.state == State::Populating));
    assert!(settle("cdn", &actual, &mut directories) >= 2);
    assert_eq!(directories.len(), 1);
    for owners in directories["cdn"].values() {
        assert_eq!(owners.len(), REPLICAS);
        assert!(owners.iter().all(|o| o.state == State::Ready));
        assert!(
            owners
                .iter()
                .all(|o| o.node.starts_with("cdn-") && o.node != "cdn-0")
        );
    }
    // A member announcing unhealthy storage gets nothing new but keeps what it holds.
    let mut sick = actual.clone();
    sick.get_mut(&("cdn".to_string(), "cdn-4".to_string()))
        .unwrap()
        .unhealthy = vec!["cdn".into()];
    let mut joined = sick.clone();
    joined.insert(
        ("cdn".to_string(), "cdn-5".to_string()),
        node("cdn-5", "cdn", "b"),
    );
    let mut after = directories.clone();
    settle("cdn", &joined, &mut after);
    // Ranges may split, but every hash cdn-4 held stays with it and it gains none.
    for (start, owners) in &after["cdn"] {
        let before = placement::owners(&directories["cdn"], *start);
        let held = |owners: &[Owner]| owners.iter().any(|o| o.node == "cdn-4");
        assert_eq!(held(owners), held(before), "range {start:x}");
    }

    // A removed member's ranges are reassigned, still within the service.
    let mut removed = actual.clone();
    removed.remove(&("cdn".to_string(), "cdn-2".to_string()));
    settle("cdn", &removed, &mut directories);
    for owners in directories["cdn"].values() {
        assert_eq!(owners.len(), REPLICAS);
        assert!(
            owners
                .iter()
                .all(|o| o.node != "cdn-2" && o.state == State::Ready)
        );
    }
}

#[test]
fn placement_filters_members_and_health_per_dataset() {
    let node = |id: &str, datasets: &[&str], unhealthy: &[&str]| pb::Member {
        service: "quicksilver".into(),
        roles: vec!["l2".into()],
        failure_domain: id.into(),
        datasets: datasets.iter().map(|d| (*d).into()).collect(),
        unhealthy: unhealthy.iter().map(|d| (*d).into()).collect(),
        ..ready(id)
    };
    let actual = members(&[
        node("both-sick-auth", &["auth", "quicksilver"], &["auth"]),
        node("auth-only", &["auth"], &[]),
        node("qs-only", &["quicksilver"], &[]),
        node("both", &["auth", "quicksilver"], &[]),
        pb::Member {
            service: "cdn".into(),
            ..node("foreign", &["auth", "quicksilver"], &[])
        },
    ]);
    let changes = place(
        "quicksilver",
        &actual,
        &Directories::new(),
        &siblings::Ready::new(),
    );
    let assigned = |dataset: &str| -> BTreeSet<String> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::Range {
                    dataset: d, owners, ..
                } if d == dataset => Some(owners),
                _ => None,
            })
            .flatten()
            .map(|owner| {
                assert_eq!(owner.state, State::Populating);
                owner.node.clone()
            })
            .collect()
    };
    assert_eq!(
        assigned("quicksilver"),
        ["both-sick-auth", "qs-only", "both"]
            .map(String::from)
            .into_iter()
            .collect()
    );
    assert_eq!(
        assigned("auth"),
        ["auth-only", "both"]
            .map(String::from)
            .into_iter()
            .collect()
    );
    assert!(place("kv", &actual, &Directories::new(), &siblings::Ready::new()).is_empty());
}

#[test]
fn handoff_readiness_is_scoped_to_the_service_dataset_and_range() {
    let owner = |node: &str, state| Owner {
        node: node.into(),
        state,
    };
    let node = |id: &str, status: pb::member::Status| pb::Member {
        service: "quicksilver".into(),
        roles: vec!["l2".into()],
        failure_domain: id.into(),
        datasets: vec!["auth".into(), "quicksilver".into()],
        status: status as i32,
        ..ready(id)
    };
    let actual = members(&[
        node("old", pb::member::Status::Draining),
        node("a", pb::member::Status::Ready),
        node("b", pb::member::Status::Ready),
        node("c", pb::member::Status::Ready),
    ]);
    let initial = vec![
        owner("old", State::Leaving),
        owner("a", State::Populating),
        owner("b", State::Populating),
        owner("c", State::Populating),
    ];
    let directories = Directories::from([
        ("auth".into(), Directory::from([(0, initial.clone())])),
        (
            "quicksilver".into(),
            Directory::from([(0, initial.clone())]),
        ),
    ]);
    let ready = siblings::Ready::from([
        (
            (("quicksilver".into(), "a".into()), "auth".into()),
            vec![(0, 0)],
        ),
        ((("cdn".into(), "b".into()), "auth".into()), vec![(0, 0)]),
        (
            (("quicksilver".into(), "c".into()), "auth".into()),
            vec![(1, 0)],
        ),
    ]);
    assert_eq!(
        place("quicksilver", &actual, &directories, &ready),
        vec![Change::Range {
            service: "quicksilver".into(),
            dataset: "auth".into(),
            start: 0,
            owners: vec![
                owner("old", State::Leaving),
                owner("a", State::Ready),
                owner("b", State::Populating),
                owner("c", State::Populating),
            ],
        }]
    );
}

#[test]
fn malformed_dataset_range_entries_are_reported_as_data_loss() {
    let (view, _) = View::new("quicksilver", "eu-1a");
    let engine = &view.published.engine;
    let prefix = b"quicksilver.eu-1a\0";
    for suffix in [
        "range//0000000000000000",
        "range/auth!/0000000000000000",
        "range/auth/000000000000000",
        "range/auth/00000000000000000",
        "range/auth/000000000000000g",
        "range/auth/0000000000000000/extra",
    ] {
        let key = [prefix.as_slice(), suffix.as_bytes()].concat();
        engine.put(&key, &encode(&[])).unwrap();
        assert_eq!(
            view.directory("auth").unwrap_err().code(),
            tonic::Code::DataLoss,
            "{suffix}"
        );
        engine.delete(&key).unwrap();
    }
    let key = [prefix.as_slice(), b"range/auth/0000000000000000"].concat();
    engine.put(&key, &[0xff]).unwrap();
    assert_eq!(
        view.directory("auth").unwrap_err().code(),
        tonic::Code::DataLoss
    );
}
