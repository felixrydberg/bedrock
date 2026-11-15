use std::collections::BTreeMap;
use std::sync::Arc;

use bedrock_lib::actors::{ActorId, Dc, Regions, Topology};
use bedrock_lib::client::Table;
use bedrock_lib::placement::{Owner, State as OwnerState};
use bedrock_lib::replication::pb::Member;

use super::*;

#[test]
fn an_entry_commits_on_two_voting_slots_once_the_noop_does() {
    let voting = [true; 3];
    assert_eq!(quorum_point(&[5, 3, 0], &voting, 3), Some(3));
    assert_eq!(quorum_point(&[7, 6, 5], &voting, 3), Some(6));
    // Two slots hold only entries before the no-op: an older epoch's entry never counts on its own.
    assert_eq!(quorum_point(&[5, 2, 2], &voting, 3), None);
    // A repaired slot's acknowledgement counts towards no quorum.
    assert_eq!(quorum_point(&[5, 5, 0], &[true, false, true], 3), None);
}

fn topology(cells: &[&str]) -> (Regions, Topology) {
    let topology = Topology {
        format_version: 1,
        region: "eu".into(),
        version: 1,
        dcs: vec![Dc {
            name: "dc1".into(),
            cells: cells.iter().map(|c| (*c).into()).collect(),
        }],
    };
    (BTreeMap::from([("eu".into(), topology.clone())]), topology)
}

/// A cell's `volume` table with one range whose owners are `nodes`, in order.
fn table(cell: &str, nodes: &[&str]) -> Arc<Table> {
    let members = nodes
        .iter()
        .map(|n| {
            let member = Member {
                node_id: (*n).into(),
                service: "volume".into(),
                cell: cell.into(),
                address: format!("{n}.{cell}:7000"),
                ..Default::default()
            };
            ((*n).to_string(), member)
        })
        .collect();
    let owners = nodes
        .iter()
        .map(|n| Owner {
            node: (*n).into(),
            state: OwnerState::Ready,
        })
        .collect();
    Arc::new(Table {
        revision: 1,
        members,
        directories: BTreeMap::from([("volume".into(), BTreeMap::from([(0, owners)]))]),
    })
}

#[test]
fn slots_map_to_the_owner_at_their_occurrence_in_the_cell() {
    // Two cells: one of them holds two slots, which take its first and second owner.
    let (regions, topology) = topology(&["a", "b"]);
    let id = ActorId::from_name(&regions, "eu", "ns", "x").unwrap();
    let tables = BTreeMap::from([
        ("a".into(), table("a", &["a1", "a2"])),
        ("b".into(), table("b", &["b1", "b2"])),
    ]);
    let slots = slot_nodes(&id, &topology, &tables).unwrap();
    let cells = bedrock_lib::actors::slots(&id, &topology).unwrap();
    let twice = cells[0];
    assert_eq!(cells[2], twice, "the third slot wraps to the first cell");
    assert_eq!(slots[0].address, format!("{twice}1.{twice}:7000"));
    assert_eq!(slots[2].address, format!("{twice}2.{twice}:7000"));
    assert_eq!(slots[1].identity.node_id, format!("{}1", cells[1]));
    assert_eq!(
        slots[1].identity.host,
        format!("{}1.{}", cells[1], cells[1])
    );

    // A missing sibling table is unavailable, not a guess.
    let tables = BTreeMap::from([("a".into(), table("a", &["a1", "a2"]))]);
    let missing = slot_nodes(&id, &topology, &tables);
    assert!(matches!(missing, Err(Error::Unavailable(_))));
}
