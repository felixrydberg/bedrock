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
        ..Default::default()
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
fn alarms_follow_the_commit_point_and_survive_log_pruning() {
    let mut state = state(8, 0);
    for (seq, at_ms) in [
        (2, Some(100)),
        (3, None),
        (4, Some(200)),
        (6, None),
        (7, Some(300)),
    ] {
        let entry = &mut state.log[seq - 1];
        entry.kind = Kind::Data as i32;
        entry.alarm = Some(pb::Alarm { at_ms });
    }
    state.pending = state.log.iter().map(Message::encoded_len).sum();

    state.ack(0, 2, true, 0);
    state.ack(1, 1, true, 0);
    assert_eq!(state.commit, 1);
    assert_eq!(state.alarm_ms, None, "the pending set is not committed");
    state.ack(1, 2, true, 0);
    assert_eq!(state.alarm_ms, Some(100));

    for i in 0..SLOTS {
        state.ack(i, 3, true, 0);
    }
    assert_eq!(
        state.alarm_ms, None,
        "the clear precedes a pending reschedule"
    );
    for i in 0..SLOTS {
        state.ack(i, 5, true, 0);
    }
    assert!(state.entry(4).is_none(), "the reschedule was pruned");
    assert_eq!(state.alarm_ms, Some(200));

    // A quorum commits both a clear and a reschedule in one advance; the later entry wins.
    for i in 0..SLOTS {
        state.ack(i, 8, true, 0);
    }
    assert_eq!(state.commit, 8);
    assert!(state.entry(7).is_none(), "the final reschedule was pruned");
    assert_eq!(state.alarm_ms, Some(300));
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
fn the_largest_committed_entry_stays_available_to_announce_its_commit() {
    let mut state = state(1, RETAIN_BYTES + 1);
    state.ack(0, 1, true, 0);
    state.ack(1, 1, true, 0);
    assert_eq!(state.commit, 1);
    assert!(
        state.entry(1).is_some(),
        "an idle commit-only append must still name entry 1"
    );
    assert_eq!(state.base, 0);
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
        let first = host.join(&slots, &pb::Actor::default());
        let second = host.join(&slots, &pb::Actor::default());
        assert_eq!(host.lock().len(), 3);
        drop(first);
        assert_eq!(host.lock().len(), 3, "another writer still uses the nodes");
        drop(second);
        assert!(host.lock().is_empty());

        // A guard that claimed ends those claims' leases: the nodes stay until a heartbeat carries them.
        let mut third = host.join(&slots, &pb::Actor::default());
        third.claimed.extend([4, 7]);
        drop(third);
        let nodes = host.lock();
        assert_eq!(nodes.len(), 3);
        for node in nodes.values() {
            assert_eq!(node.users, 0);
            let epochs: Vec<u64> = node.ended.iter().map(|e| e.epoch).collect();
            assert_eq!(epochs, [4, 7]);
        }
    });
}

/// #124 / S-16: a heartbeat drops the endings the slot recorded and moves the refused ones behind those not yet sent.
#[test]
fn refused_endings_wait_behind_the_unsent_ones() {
    let ended = |epoch| pb::Ended { actor: None, epoch };
    let mut queue: Vec<pb::Ended> = (1..=5).map(ended).collect();
    let sent: Vec<pb::Ended> = (1..=3).map(ended).collect();
    requeue(&mut queue, &sent, &[ended(2)]);
    let epochs: Vec<u64> = queue.iter().map(|e| e.epoch).collect();
    assert_eq!(epochs, [4, 5, 1, 3]);
}

#[test]
fn the_lease_deadline_follows_the_second_newest_acknowledged_heartbeat() {
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
        let _joined = host.join(&slots, &pb::Actor::default());
        assert_eq!(host.deadline(&slots), None, "no acknowledgement yet");
        let now = bedrock_lib::monotonic_now();
        let sent = [
            now,
            now + Duration::from_secs(1),
            now + Duration::from_secs(2),
        ];
        for (slot, sent) in slots.iter().zip(sent) {
            host.lock().get_mut(&slot.address).unwrap().acked = Some(sent);
        }
        // Two slots must have acknowledged within FENCE: the lease ends FENCE after the second newest.
        assert_eq!(host.deadline(&slots), Some(sent[1] + FENCE));
    });
}

/// A writer over unreachable slots whose heartbeats all count as just acknowledged, with no slot tasks.
fn writer(state: State) -> Writer {
    let peers = Arc::new(bedrock_lib::transport::Peers::new(
        &bedrock_lib::admin::Admin::new(),
    ));
    let host = Host::start(peers, pb::Holder::default());
    let slots: [Slot; SLOTS] = std::array::from_fn(|i| Slot {
        cell: format!("c{i}"),
        address: format!("10.0.0.{i}:7000"),
        identity: tls::Identity {
            service: "volume".into(),
            cell: format!("c{i}"),
            node_id: format!("v{i}"),
            host: format!("10.0.0.{i}"),
        },
    });
    let joined = host.join(&slots, &pb::Actor::default());
    for node in host.lock().values_mut() {
        node.acked = Some(bedrock_lib::monotonic_now());
    }
    Writer {
        shared: Arc::new(Shared {
            joined,
            actor: pb::Actor::default(),
            epoch: 1,
            version: 0,
            deleting: false,
            state: Mutex::new(state),
            changed: Notify::new(),
        }),
        tasks: Vec::new(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn a_waiter_woken_by_a_late_acknowledgement_after_the_fence_fails() {
    runtime().block_on(async {
        let writer = writer(state(3, 0));
        let waiter = writer.wait(|s| (s.commit >= 3).then_some(3));
        let fence_then_ack = async {
            tokio::task::yield_now().await;
            let mut state = writer.shared.lock();
            writer.shared.fence(&mut state, "test");
            for i in 0..2 {
                state.ack(i, 3, true, 0);
            }
            drop(state);
            writer.shared.changed.notify_waiters();
        };
        let (result, ()) = tokio::join!(waiter, fence_then_ack);
        assert!(matches!(result, Err(Error::Fenced(_))), "{result:?}");
        assert_eq!(writer.committed(), 3, "the late acknowledgement did commit");
    });
}

#[test]
fn a_writer_with_fewer_than_two_slots_taking_appends_stops_serving() {
    runtime().block_on(async {
        let writer = writer(state(1, 0));
        assert!(writer.serving());
        writer.shared.lose(0, "test");
        assert!(writer.serving(), "two slots still take appends");
        writer.shared.lose(2, "corrupt");
        assert!(!writer.serving());
        let pending = writer.wait(|s| (s.commit >= 1).then_some(()));
        // The error says why: the first reason, which a later fence does not replace.
        writer.shared.fence(&mut writer.shared.lock(), "later");
        let error = pending.await.unwrap_err().to_string();
        assert!(
            error.starts_with(
                "the writer stopped serving: slot 2 dropped: corrupt; fewer than two voting slots"
            ),
            "{error}"
        );
    });
}

/// #124 / S-16 seeds 1–3: one slot lost and another repaired leave one voter, so nothing commits again; the writer
/// fences, so a new claim can take the actor, rather than holding it unavailable while its lease lives.
#[test]
fn a_writer_left_with_one_voting_slot_stops_serving() {
    runtime().block_on(async {
        let writer = writer(state(1, 0));
        {
            let mut state = writer.shared.lock();
            state.ack(1, 1, false, 0);
            assert!(!writer.shared.fence_without_quorum(&mut state, "test"));
        }
        assert!(writer.serving(), "slots 0 and 2 still vote");
        writer.shared.lose(0, "test");
        assert!(!writer.serving(), "only slot 2 votes");
    });
}

#[test]
fn a_lost_slots_stale_acknowledgement_commits_nothing() {
    let mut state = state(10, 0);
    for i in 0..SLOTS {
        state.ack(i, 1, true, 0);
    }
    state.ack(2, 10, true, 0);
    assert_eq!(state.commit, 1);
    // Slot 2 stops taking appends (wiped, say); its 10 may be gone.
    state.lost[2] = true;
    state.ack(0, 5, true, 0);
    state.ack(1, 3, true, 0);
    assert_eq!(state.commit, 3, "only slots 0 and 1 count");
}

#[test]
fn an_unknown_refusal_code_counts_for_no_quorum() {
    let answer = |refused| {
        Ok(pb::Answer {
            refused,
            slot: Some(pb::Slot {
                voting: true,
                ..Default::default()
            }),
        })
    };
    assert!(counts(&answer(Refusal::None as i32)));
    let unknown = answer(99);
    assert!(!counts(&unknown));
    assert_eq!(refused(unknown.as_ref().unwrap()), None);
}

#[test]
fn one_tombstone_stops_a_claim_but_a_deletion_claims_past_it_until_two() {
    let answer = |refusal: Refusal| {
        Ok(pb::Answer {
            refused: refusal as i32,
            slot: Some(pb::Slot::default()),
        })
    };
    let one = vec![(0, answer(Refusal::Deleted)), (1, answer(Refusal::None))];
    assert!(matches!(buried(Kind::Noop, &one), Err(Error::Deleted)));
    assert!(matches!(buried(Kind::Move, &one), Err(Error::Deleted)));
    assert!(buried(Kind::Delete, &one).is_ok());
    let two = vec![(0, answer(Refusal::Deleted)), (2, answer(Refusal::Deleted))];
    assert!(matches!(buried(Kind::Delete, &two), Err(Error::Deleted)));
}
