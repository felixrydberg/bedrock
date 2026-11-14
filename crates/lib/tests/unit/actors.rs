use super::*;

const NAMESPACE_ID: &str = "0123456789abcdef0123456789abcdef";

fn dc(name: &str, cells: &[&str]) -> Dc {
    Dc {
        name: name.into(),
        cells: cells.iter().map(|c| c.to_string()).collect(),
    }
}

fn topology(dcs: Vec<Dc>) -> Topology {
    Topology {
        format_version: FORMAT_VERSION,
        region: "eu".into(),
        version: 1,
        dcs,
    }
}

/// `eu`: a two-cell DC, a four-cell DC and a one-cell DC.
fn eu() -> Topology {
    topology(vec![
        dc("eu-a", &["a1", "a2"]),
        dc("eu-b", &["c1", "c2", "c3", "c4"]),
        dc("eu-c", &["e1"]),
    ])
}

fn regions() -> Regions {
    BTreeMap::from([("eu".to_string(), eu())])
}

fn named(name: &str) -> ActorId {
    ActorId::from_name(&regions(), "eu", NAMESPACE_ID, name).unwrap()
}

fn compute(node: &str, cell: &str, cpus: f64) -> pb::Member {
    pb::Member {
        node_id: node.into(),
        service: "compute".into(),
        cell: cell.into(),
        roles: vec!["compute".into()],
        cpus,
        status: pb::member::Status::Ready as i32,
        ..Default::default()
    }
}

// The expected values below come from an independent Python implementation of SHA-256, FNV-1a and SplitMix64, so a
// change in any build or platform shows up as a failure rather than as a new baseline.
#[test]
fn ids_have_fixed_vectors() {
    for (name, text) in [
        ("alice", "eu_s7ocd7xj2wtai3s6qd4asuivty"),
        ("bob", "eu_o235mn6nq6bl3cqkoerea6uojm"),
        ("carol", "eu_nvb4a5pyvnwtzmu6om4zfln4pq"),
    ] {
        let id = named(name);
        assert_eq!(id.to_string(), text);
        assert_eq!(ActorId::parse(&regions(), text), Ok(id));
    }
    let id = ActorId::new(&regions(), "eu", 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff).unwrap();
    assert_eq!(id.to_string(), "eu_aaisem2ekvthpcezvk54zxpo74");
    // Both ends of the 128 bits round trip.
    for bits in [0, u128::MAX] {
        let id = ActorId::new(&regions(), "eu", bits).unwrap();
        assert_eq!(ActorId::parse(&regions(), &id.to_string()), Ok(id));
    }
}

#[tokio::test]
async fn unique_ids_differ_and_parse_back() {
    let a = ActorId::unique(&regions(), "eu").unwrap();
    let b = ActorId::unique(&regions(), "eu").unwrap();
    assert_ne!(a, b);
    assert_eq!(region_of(&a), "eu");
    assert_eq!(ActorId::parse(&regions(), &a.to_string()), Ok(a));
}

#[test]
fn malformed_ids_are_rejected() {
    let good = named("alice").to_string();
    let encoded = good.strip_prefix("eu_").unwrap();
    for (text, why) in [
        (encoded.to_string(), "no region"),
        ("eu".to_string(), "no ID"),
        (format!("eu_{}", &encoded[1..]), "too short"),
        (format!("eu_{encoded}a"), "too long"),
        (format!("eu_{}", encoded.to_uppercase()), "uppercase"),
        (
            format!("eu_1{}", &encoded[1..]),
            "character outside the alphabet",
        ),
        (format!("eu_{}b", &encoded[..25]), "nonzero padding"),
        (format!("EU_{encoded}"), "region not lowercase"),
        (format!("_{encoded}"), "empty region"),
        (format!("e u_{encoded}"), "region character"),
        (String::new(), "empty"),
    ] {
        assert!(
            matches!(ActorId::parse(&regions(), &text), Err(Error::Malformed(_))),
            "{why}: {text:?}"
        );
    }
}

#[test]
fn unknown_regions_are_rejected() {
    let text = named("alice").to_string().replace("eu_", "us_");
    let unknown = Err(Error::UnknownRegion("us".into()));
    assert_eq!(ActorId::parse(&regions(), &text), unknown);
    assert_eq!(
        ActorId::from_name(&regions(), "us", NAMESPACE_ID, "x"),
        unknown
    );
    assert_eq!(ActorId::unique(&regions(), "us"), unknown);
    // A topology of another region cannot place the ID.
    let mut other = eu();
    other.region = "us".into();
    assert_eq!(
        dc_of(&named("alice"), &other),
        Err(Error::UnknownRegion("eu".into()))
    );
}

#[test]
fn placement_has_fixed_vectors() {
    let topology = eu();
    // alice and bob rank the four-cell DC, carol the two-cell one.
    let alice = named("alice");
    assert_eq!(dc_of(&alice, &topology).unwrap().name, "eu-b");
    assert_eq!(
        cells_of(&alice, &topology).unwrap(),
        ["c4", "c3", "c2", "c1"]
    );
    assert_eq!(slots(&alice, &topology).unwrap(), ["c4", "c3", "c2"]);
    let bob = named("bob");
    assert_eq!(dc_of(&bob, &topology).unwrap().name, "eu-b");
    assert_eq!(cells_of(&bob, &topology).unwrap(), ["c2", "c3", "c1", "c4"]);
    // Fewer cells than slots wrap round.
    let carol = named("carol");
    assert_eq!(dc_of(&carol, &topology).unwrap().name, "eu-a");
    assert_eq!(cells_of(&carol, &topology).unwrap(), ["a1", "a2"]);
    assert_eq!(slots(&carol, &topology).unwrap(), ["a1", "a2", "a1"]);
}

#[test]
fn one_cell_dc_fills_every_slot() {
    let topology = topology(vec![dc("only", &["x"])]);
    assert_eq!(slots(&named("alice"), &topology).unwrap(), ["x", "x", "x"]);
}

#[test]
fn adding_a_cell_moves_only_the_actors_it_ranks_first() {
    let before = topology(vec![dc("d", &["c1", "c2", "c3", "c4"])]);
    let after = topology(vec![dc("d", &["c1", "c2", "c3", "c4", "c5"])]);
    let total = 10_000;
    let mut moved = 0;
    for i in 0..total {
        let id = named(&i.to_string());
        let old = cells_of(&id, &before).unwrap();
        let new = cells_of(&id, &after).unwrap();
        // The others keep their relative order, so a slot only changes when the new cell enters the first three.
        let without_new: Vec<_> = new.iter().copied().filter(|c| *c != "c5").collect();
        assert_eq!(without_new, old);
        if new[0] != old[0] {
            assert_eq!(new[0], "c5");
            moved += 1;
        }
    }
    // About 1/5 of the actors, within sampling noise.
    let share = moved as f64 / total as f64;
    assert!((0.17..0.23).contains(&share), "moved {share}");
}

#[test]
fn adding_a_dc_moves_about_its_share() {
    let before = topology(vec![dc("a", &["a1"]), dc("b", &["b1"]), dc("c", &["c1"])]);
    let after = topology(vec![
        dc("a", &["a1"]),
        dc("b", &["b1"]),
        dc("c", &["c1"]),
        dc("d", &["d1"]),
    ]);
    let mut moved = 0;
    for i in 0..10_000 {
        let id = named(&i.to_string());
        let (old, new) = (dc_of(&id, &before).unwrap(), dc_of(&id, &after).unwrap());
        if old.name != new.name {
            assert_eq!(new.name, "d");
            moved += 1;
        }
    }
    assert!((2_200..2_800).contains(&moved), "moved {moved}");
}

#[test]
fn wake_host_has_fixed_vectors() {
    let members: Vec<_> = (1..=4)
        .map(|i| compute(&format!("n{i}"), "c4", 4.0))
        .collect();
    let all = |_: &str| true;
    let wake = |name, members: &[pb::Member], healthy: &dyn Fn(&str) -> bool| {
        let id = named(name);
        wake_host(&id, &eu(), members, healthy)
            .unwrap()
            .map(|m| m.node_id.clone())
    };
    assert_eq!(wake("alice", &members, &all).as_deref(), Some("n4"));
    assert_eq!(wake("bob", &members, &all).as_deref(), Some("n4"));
    // Carol's DC has cells a1 and a2, and no member is in either.
    assert_eq!(wake("carol", &members, &all), None);
    // An unhealthy winner falls to the next-ranked member.
    assert_eq!(
        wake("alice", &members, &|n| n != "n4").as_deref(),
        Some("n1")
    );
}

#[test]
fn wake_host_takes_the_first_cell_with_a_healthy_compute_member() {
    let mut members = vec![
        compute("n1", "c4", 4.0),
        compute("n2", "c3", 4.0),
        compute("n3", "c2", 4.0),
    ];
    let id = named("alice"); // cells c4, c3, c2, c1
    let host = |members: &[pb::Member], healthy: &dyn Fn(&str) -> bool| {
        wake_host(&id, &eu(), members, healthy)
            .unwrap()
            .map(|m| m.node_id.clone())
    };
    assert_eq!(host(&members, &|_| true).as_deref(), Some("n1"));
    assert_eq!(host(&members, &|n| n != "n1").as_deref(), Some("n2"));
    assert_eq!(host(&members, &|n| n == "n3").as_deref(), Some("n3"));
    assert_eq!(host(&members, &|_| false), None);
    // Only ready members with the compute role count.
    members[0].status = pb::member::Status::Draining as i32;
    members[1].roles = vec!["volume".into()];
    assert_eq!(host(&members, &|_| true).as_deref(), Some("n3"));
}

#[test]
fn wake_host_is_weighted_by_capacity() {
    let members = [compute("n1", "c4", 1.0), compute("n2", "c4", 3.0)];
    let topology = topology(vec![dc("d", &["c4"])]);
    let total = 10_000;
    let big = (0..total)
        .filter(|i| {
            let id = named(&i.to_string());
            let topology = Topology {
                region: "eu".into(),
                ..topology.clone()
            };
            wake_host(&id, &topology, &members, |_| true)
                .unwrap()
                .unwrap()
                .node_id
                == "n2"
        })
        .count();
    let share = big as f64 / total as f64;
    assert!((0.72..0.78).contains(&share), "share {share}");
    // No reported capacity counts as one.
    let unset = [compute("n1", "c4", 0.0), compute("n2", "c4", 1.0)];
    let a = named("alice");
    assert_eq!(
        wake_host(&a, &topology, &unset, |_| true)
            .unwrap()
            .map(|m| &m.node_id),
        wake_host(
            &a,
            &topology,
            &[compute("n1", "c4", 1.0), compute("n2", "c4", 1.0)],
            |_| true
        )
        .unwrap()
        .map(|m| &m.node_id)
    );
}

#[test]
fn topology_records_are_validated() {
    let good = eu().encode_to_vec();
    assert_eq!(Topology::decode_record("eu", &good), Ok(eu()));
    assert_eq!(topology_key("eu"), b"topology/region/eu");

    let bad = |edit: &dyn Fn(&mut Topology)| {
        let mut record = eu();
        edit(&mut record);
        matches!(
            Topology::decode_record("eu", &record.encode_to_vec()),
            Err(Error::Malformed(_))
        )
    };
    assert!(matches!(
        Topology::decode_record("us", &good),
        Err(Error::Malformed(_))
    ));
    assert!(matches!(
        Topology::decode_record("eu", b"\xff"),
        Err(Error::Malformed(_))
    ));
    assert!(bad(&|t| t.format_version = 2));
    assert!(bad(&|t| t.dcs.clear()));
    assert!(bad(&|t| t.dcs[0].cells.clear()));
    assert!(bad(&|t| t.dcs[0].name.clear()));
    assert!(bad(&|t| t.dcs[1].name = "eu-a".into()));
    assert!(bad(&|t| t.dcs[1].cells.push("a1".into())));
    assert!(bad(&|t| t.dcs[1].cells.push(String::new())));
}

#[test]
fn actor_namespace_records_round_trip() {
    assert_eq!(
        actor_ns_key(NAMESPACE_ID),
        format!("actor_ns/{NAMESPACE_ID}").as_bytes()
    );
    let record = ActorNamespace {
        format_version: FORMAT_VERSION,
        worker: "chat".into(),
        class: "Room".into(),
        kind: Kind::Do as i32,
    };
    assert_eq!(
        ActorNamespace::decode_record(&record.encode_to_vec()),
        Ok(record.clone())
    );
    for edit in [
        ActorNamespace {
            format_version: 2,
            ..record.clone()
        },
        ActorNamespace { kind: 99, ..record },
    ] {
        assert!(matches!(
            ActorNamespace::decode_record(&edit.encode_to_vec()),
            Err(Error::Malformed(_))
        ));
    }
}

#[test]
fn kinds_have_stable_names() {
    for name in ["do", "d1", "queue", "cron", "workflow", "kv"] {
        assert_eq!(Kind::parse(name).unwrap().as_str(), name);
    }
    assert_eq!(Kind::parse("worker"), None);
}
