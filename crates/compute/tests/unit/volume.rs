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

/// A state whose log holds entries 1 ..= `n` of `bytes` each at epoch 1, the first being the claim's no-op.
fn state(n: u64, bytes: usize) -> State {
    let mut state = State {
        noop: 1,
        ..State::empty()
    };
    for seq in 1..=n {
        let entry = pb::Entry {
            seq,
            epoch: 1,
            changes: vec![pb::Change {
                key: Vec::new(),
                value: Some(vec![0; bytes]),
            }],
            ..Default::default()
        };
        state.pending += entry.encoded_len();
        state.log.push_back(entry);
    }
    state
}

#[test]
fn pruning_keeps_the_entry_each_slot_holds_last() {
    let mut state = state(3, 0);
    for i in 0..SLOTS {
        state.ack(i, 3, true, 0);
    }
    assert_eq!(state.commit, 3);
    // A commit-only append resends entry 3, so it stays.
    assert_eq!(state.base, 2);
    assert!(state.entry(3).is_some());
    assert_eq!(state.pending, 0);
}

#[test]
fn committed_entries_kept_for_a_lagging_slot_stay_within_the_byte_budget() {
    let mut state = state(30, 3 << 20);
    for i in 0..2 {
        state.ack(i, 30, true, 0);
    }
    assert_eq!(state.commit, 30);
    assert!(state.retained <= RETAIN_BYTES, "{}", state.retained);
    assert!(
        state.base > 0,
        "slot 2 holds nothing, but the budget drops entries"
    );
}

#[test]
fn a_join_guard_leaves_the_heartbeat_set_when_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let peers = Arc::new(bedrock_lib::transport::Peers::new(
            &bedrock_lib::admin::Admin::new(),
        ));
        let host = Host::start(peers, pb::Holder::default());
        let slot = |i: usize| Slot {
            cell: format!("c{i}"),
            address: format!("10.0.0.{i}:7000"),
            identity: tls::Identity {
                service: "volume".into(),
                cell: format!("c{i}"),
                node_id: format!("v{i}"),
                host: format!("10.0.0.{i}"),
            },
        };
        let slots = [slot(0), slot(1), slot(2)];
        // Registration is owned by the guard before any heartbeat await, so a cancelled open cannot leak it.
        let first = host.join(&slots);
        let second = host.join(&slots);
        assert_eq!(host.lock().len(), 3);
        drop(first);
        assert_eq!(host.lock().len(), 3, "another writer still uses the nodes");
        drop(second);
        assert!(host.lock().is_empty());
    });
}
