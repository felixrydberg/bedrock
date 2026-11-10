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

#[test]
fn unreadable_service_is_excluded_and_its_absence_clock_restarts() {
    let old = crate::monotonic_now();
    let now = old + GRACE + GRACE;
    let mut bad = ready("bad");
    bad.service = "quicksilver".into();
    let mut healthy = ready("new");
    healthy.service = "cdn".into();
    let mut missing = ready("missing");
    missing.service = "cdn".into();
    let mut local = members(&[bad.clone(), healthy.clone()]);
    let mut remote = BTreeMap::from([("b".into(), Some(members(&[bad.clone()])))]);
    let mut absent = BTreeMap::from([(key(&bad), old), (key(&missing), old)]);
    retain_readable(
        &BTreeSet::from(["cdn"]),
        &mut local,
        &mut remote,
        &mut absent,
    );
    assert!(!local.contains_key(&key(&bad)));
    assert!(remote["b"].as_ref().unwrap().is_empty());
    assert!(!absent.contains_key(&key(&bad)));
    assert_eq!(absent[&key(&missing)], old);

    let relays = relays();
    let seen = Observed {
        me: "a",
        relays: &relays,
        cell: "eu-1a",
        local: &local,
        remote: &remote,
    };
    assert_eq!(
        plan(&seen, &members(&[missing.clone()]), &mut absent, now, GRACE),
        Some(vec![
            Change::Set(healthy.clone()),
            Change::Delete(key(&missing))
        ])
    );

    let recovered = members(&[bad.clone(), healthy]);
    assert_eq!(
        plan(&seen, &recovered, &mut absent, now, GRACE),
        Some(Vec::new())
    );
    assert_eq!(absent[&key(&bad)], now);
}

#[test]
fn obsolete_auth_drain_only_invalidates_its_service_view() {
    let (quicksilver, _) = Published::new("quicksilver", "eu-1a");
    let (cdn, _) = Published::new("cdn", "eu-1a");
    let record = Drain {
        scope: drain::Scope::Node("qs-1".into()),
        datasets: vec!["auth".into()],
        namespace: None,
        state: drain::State::Draining,
        rate: 1,
    };
    let mut key = quicksilver.prefix.clone();
    key.extend(Drain::key("old-auth").as_bytes());
    quicksilver.engine.put(&key, &record.encode()).unwrap();
    let hooks = |service| drain::ranged_with(service, |_| Ok(Directory::new()));
    assert!(
        quicksilver
            .validated("quicksilver", "eu-1a", &hooks("quicksilver"))
            .is_err()
    );
    assert!(cdn.validated("cdn", "eu-1a", &hooks("cdn")).is_ok());
    quicksilver.engine.delete(&key).unwrap();
    assert!(
        quicksilver
            .validated("quicksilver", "eu-1a", &hooks("quicksilver"))
            .is_ok()
    );
}

#[test]
fn snapshot_revision_requires_validated_progress_or_a_fresh_empty_stream() {
    assert_eq!(validated_revision(None, Some(0), true), Some(0));
    assert_eq!(validated_revision(None, Some(0), false), None);
    assert_eq!(validated_revision(None, None, true), None);
    assert_eq!(validated_revision(Some(2), Some(1), false), None);
    assert_eq!(validated_revision(Some(1), Some(2), false), Some(1));
}

#[test]
fn pushed_ranges_keep_the_revision_of_their_captured_directory() {
    let owner = Owner {
        node: "qs-1".into(),
        state: State::Ready,
    };
    let old = DirectoryAt {
        revision: 7,
        directories: Directories::from([(
            "quicksilver".into(),
            Directory::from([(0, vec![owner])]),
        )]),
    };
    let newer = DirectoryAt {
        revision: 8,
        directories: Directories::new(),
    };
    assert_eq!(
        old.ranges("quicksilver", "qs-1"),
        pb::Ranges {
            dataset: "quicksilver".into(),
            revision: 7,
            spans: vec![pb::Span { start: 0, end: 0 }]
        }
    );
    assert_eq!(newer.ranges("quicksilver", "qs-1").revision, 8);
    assert!(newer.ranges("quicksilver", "qs-1").spans.is_empty());
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
async fn planning_requires_a_majority_and_the_caller_supplies_raft_leadership() {
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
    // Raft chooses the publisher; a lower sibling address does not suppress the leader's plan.
    assert_eq!(
        run(
            "b",
            &node,
            &[("a", Some(&[])), ("c", None)],
            &[],
            &mut absent,
            now
        ),
        Some(vec![Change::Set(ready("kv-1"))])
    );
    // A majority can plan while a lower relay is unreachable.
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
async fn follower_observations_restart_grace_before_leadership_returns() {
    let start = crate::monotonic_now();
    let mut absent = BTreeMap::new();
    let published = members(&[ready("kv-1")]);
    let empty = Members::new();
    let relays = relays();
    let remote = BTreeMap::from([("b".into(), Some(Members::new())), ("c".into(), None)]);
    let mut observe = |leader, local: &Members, now| {
        publication_plan(
            &Observed {
                me: "a",
                relays: &relays,
                cell: "eu-1a",
                local,
                remote: &remote,
            },
            &published,
            &mut absent,
            now,
            GRACE,
            leader,
        )
    };
    assert_eq!(observe(true, &empty, start), Some(Vec::new()));
    // A follower may observe an expired absence, but cannot publish the deletion.
    assert_eq!(observe(false, &empty, start + GRACE), None);
    let reconnect = start + GRACE + Duration::from_secs(1);
    assert_eq!(observe(false, &published, reconnect), None);
    let disconnect = reconnect + Duration::from_secs(1);
    assert_eq!(observe(false, &empty, disconnect), None);
    // On re-election, only the latest continuous absence counts toward grace.
    assert_eq!(
        observe(true, &empty, disconnect + GRACE - Duration::from_millis(1)),
        Some(Vec::new())
    );
    assert_eq!(
        observe(true, &empty, disconnect + GRACE),
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
        let changes = place(service, actual, directories, &ready, &[]);
        if changes.is_empty() {
            return pass;
        }
        for change in changes {
            match change {
                Change::Range {
                    dataset,
                    start,
                    owners,
                    ..
                } => {
                    directories
                        .entry(dataset)
                        .or_default()
                        .insert(start, owners);
                }
                Change::Merge { dataset, start, .. } => {
                    directories.entry(dataset).or_default().remove(&start);
                }
                change => panic!("placement wrote {change:?}"),
            }
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
    let first = place("cdn", &actual, &directories, &siblings::Ready::new(), &[]);
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
fn quicksilver_placement_ignores_auth_dataset_membership() {
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
        &[],
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
    assert!(assigned("auth").is_empty());
    assert!(
        place(
            "kv",
            &actual,
            &Directories::new(),
            &siblings::Ready::new(),
            &[]
        )
        .is_empty()
    );
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
        datasets: vec!["quicksilver".into()],
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
    let directories = Directories::from([(
        "quicksilver".into(),
        Directory::from([(0, initial.clone())]),
    )]);
    let ready = siblings::Ready::from([
        (
            (("quicksilver".into(), "a".into()), "quicksilver".into()),
            vec![(0, 0)],
        ),
        (
            (("cdn".into(), "b".into()), "quicksilver".into()),
            vec![(0, 0)],
        ),
        (
            (("quicksilver".into(), "c".into()), "quicksilver".into()),
            vec![(1, 0)],
        ),
    ]);
    assert_eq!(
        place("quicksilver", &actual, &directories, &ready, &[]),
        vec![Change::Range {
            service: "quicksilver".into(),
            dataset: "quicksilver".into(),
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
        "range/quicksilver!/0000000000000000",
        "range/quicksilver/000000000000000",
        "range/quicksilver/00000000000000000",
        "range/quicksilver/000000000000000g",
        "range/quicksilver/0000000000000000/extra",
    ] {
        let key = [prefix.as_slice(), suffix.as_bytes()].concat();
        engine.put(&key, &encode(&[])).unwrap();
        assert_eq!(
            view.directory("quicksilver").unwrap_err().code(),
            tonic::Code::DataLoss,
            "{suffix}"
        );
        engine.delete(&key).unwrap();
    }
    let key = [prefix.as_slice(), b"range/quicksilver/0000000000000000"].concat();
    engine.put(&key, &[0xff]).unwrap();
    assert_eq!(
        view.directory("quicksilver").unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

/// Found reviewing #25 / B-23, and shown by `handoff.tla`: a ready owner reporting the dataset unhealthy may hold damage, so
/// it must not be what lets the last healthy copy go.
#[test]
fn a_leaving_owner_stays_while_the_only_ready_owner_is_unhealthy() {
    let node = |id: &str, unhealthy: &[&str]| pb::Member {
        service: "cdn".into(),
        roles: vec!["l2".into()],
        failure_domain: id.into(),
        datasets: vec!["cdn".into()],
        unhealthy: unhealthy.iter().map(|d| (*d).into()).collect(),
        ..ready(id)
    };
    let owners = vec![
        Owner {
            node: "good".into(),
            state: State::Leaving,
        },
        Owner {
            node: "sick".into(),
            state: State::Ready,
        },
    ];
    let directories = Directories::from([("cdn".into(), Directory::from([(0, owners)]))]);
    let actual = members(&[node("good", &[]), node("sick", &["cdn"])]);
    let leaves = |changes: Vec<Change>| {
        changes.iter().any(|change| {
            matches!(change, Change::Range { owners, .. }
                if !owners.iter().any(|owner| owner.node == "good"))
        })
    };
    let changes = place("cdn", &actual, &directories, &siblings::Ready::new(), &[]);
    assert!(!leaves(changes), "the only healthy copy was let go");

    let actual = members(&[node("good", &[]), node("sick", &[])]);
    let changes = place("cdn", &actual, &directories, &siblings::Ready::new(), &[]);
    assert!(leaves(changes), "a healthy ready owner lets it go");
}

/// Found by the simulation (#25 / B-23): a write that never reached the API left its base valid, so the plan's later writes
/// landed without it. With the range at 0 missing, part of the hash space had no owner.
#[tokio::test]
async fn a_failed_write_abandons_the_rest_of_its_services_plan() {
    let range = |service: &str, start| Change::Range {
        service: service.into(),
        dataset: service.into(),
        start,
        owners: Vec::new(),
    };
    let plan = vec![
        range("a", 0),
        range("b", 0),
        range("a", 1),
        range("b", 1),
        range("a", 2),
    ];
    let based = BTreeMap::from([("a".to_string(), 5), ("b".to_string(), 7)]);
    let attempts = Mutex::new(Vec::new());
    let written = commit(plan, based, |change, base| {
        let Change::Range { service, start, .. } = change else {
            unreachable!()
        };
        let landed = !(service == "a" && start == 1);
        attempts.lock().unwrap().push((service, start, base));
        async move { landed.then_some(base + 1) }
    })
    .await;
    assert_eq!(
        *attempts.lock().unwrap(),
        [
            ("a".to_string(), 0, 5),
            ("b".to_string(), 0, 7),
            ("a".to_string(), 1, 6),
            ("b".to_string(), 1, 8),
        ]
    );
    assert_eq!(
        written,
        BTreeMap::from([("a".to_string(), 6), ("b".to_string(), 9)])
    );
}

#[test]
fn drain_records_sit_beside_members_and_corruption_is_an_error() {
    use crate::reconciliation::drain::{Drain, Scope, State as Drained};
    let (view, _) = View::new("cdn", "eu-1a");
    let published = &view.published;
    let at = |key: &str| [b"cdn.eu-1a\0".as_slice(), key.as_bytes()].concat();
    let member = pb::Member {
        service: "cdn".into(),
        ..ready("cdn-1")
    };
    published
        .engine
        .put(&at("cdn-1"), &member.encode_to_vec())
        .unwrap();
    let record = Drain {
        scope: Scope::Node("cdn-1".into()),
        datasets: Vec::new(),
        namespace: None,
        state: Drained::Draining,
        rate: 1,
    };
    published
        .engine
        .put(&at(&Drain::key("d1")), &record.encode())
        .unwrap();
    // A drain is not a member, and no directory reads it.
    assert_eq!(published.members().unwrap().len(), 1);
    assert!(published.read().unwrap().1.is_empty());
    assert_eq!(published.drains().unwrap(), [("d1".into(), record)]);
    // Cancelling is a delete.
    published.engine.delete(&at(&Drain::key("d1"))).unwrap();
    assert!(published.drains().unwrap().is_empty());
    // An undecodable record is corruption, never a missing drain.
    published.engine.put(&at(&Drain::key("bad")), b"{").unwrap();
    assert_eq!(
        published.drains().unwrap_err().code(),
        tonic::Code::DataLoss
    );
}
