use super::*;

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

fn ready(node_id: &str) -> pb::Member {
    entry(node_id, pb::member::Status::Ready)
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

#[test]
fn drain_records_sit_beside_members_and_corruption_is_an_error() {
    use crate::reconciliation::drain::State as Drained;
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

#[test]
fn voter_identity_configuration_requires_complete_unique_processes() {
    let voters: Vec<String> = ["a:7000", "b:7000", "c:7000"].map(String::from).to_vec();
    let ids = BTreeMap::from([
        ("a:7000".to_string(), "relay-a".to_string()),
        ("b:7000".to_string(), "relay-b".to_string()),
        ("c:7000".to_string(), "relay-c".to_string()),
    ]);
    validate_voter_ids(&voters, &ids).unwrap();
    let mut invalid = ids.clone();
    invalid.remove("b:7000");
    assert!(validate_voter_ids(&voters, &invalid).is_err());
    invalid.insert("b:7000".into(), "relay-c".into());
    assert!(validate_voter_ids(&voters, &invalid).is_err());
    invalid.insert("b:7000".into(), "".into());
    assert!(validate_voter_ids(&voters, &invalid).is_err());
}
