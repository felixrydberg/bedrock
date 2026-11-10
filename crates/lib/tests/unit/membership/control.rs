use super::*;
use crate::admin::Admin;
use crate::storage::memory::MemoryEngine;

#[cfg(not(madsim))]
#[path = "../../support/allocations.rs"]
mod allocations;

fn config(id: usize) -> Config {
    Config {
        region: "eu".into(),
        dc: "sto".into(),
        cell: "sto-1".into(),
        me: ["a:7000", "b:7000", "c:7000"][id].into(),
        voters: vec!["a:7000".into(), "b:7000".into(), "c:7000".into()],
        voter_ids: BTreeMap::from([
            ("a:7000".into(), "relay-a".into()),
            ("b:7000".into(), "relay-b".into()),
            ("c:7000".into(), "relay-c".into()),
        ]),
        clock: || 1000,
        operator_token: Some("operator".into()),
        bootstrap: true,
    }
}

fn actor(engine: Arc<MemoryEngine>, id: usize) -> Actor {
    let config = config(id);
    let saved = Saved::open(&*engine, &config).unwrap();
    let applied = saved.state().applied;
    let term = saved.hard.term;
    let store = Store(Arc::new(RwLock::new(saved)));
    let raw = RawNode::new(
        &raft::Config {
            id: id as u64 + 1,
            applied,
            election_tick: 10,
            heartbeat_tick: 2,
            min_election_tick: 10 + (id + 1) * 3,
            max_election_tick: 11 + (id + 1) * 3,
            pre_vote: true,
            check_quorum: true,
            ..raft::Config::default()
        },
        store.clone(),
        &slog::Logger::root(slog::Discard, slog::o!()),
    )
    .unwrap();
    let (input, _) = mpsc::channel(256);
    let control = Arc::new(Control {
        config,
        id: id as u64 + 1,
        store,
        peers: Arc::new(Peers::new(&Admin::new())),
        input,
        leadership: RwLock::new(Leadership {
            leader: 0,
            term,
            ready: false,
            failed: None,
        }),
        changed: Arc::new(Watched::new(0)),
    });
    Actor {
        control,
        engine,
        raw,
        pending: BTreeMap::new(),
        serial: 0,
        proposal_time: 0,
        outbound: raft_core::Outbound::default(),
    }
}

#[tokio::test]
async fn runtime_step_acknowledges_only_after_the_vote_is_durable() {
    let engine = Arc::new(MemoryEngine::new("cell"));
    let mut actor = actor(engine.clone(), 0);
    let (input, receiver) = mpsc::channel(8);
    let task = tokio::spawn(async move {
        raft_core::run(&mut actor, receiver, Duration::from_millis(100)).await
    });
    let (reply, received) = oneshot::channel();
    let mut vote = Message {
        from: 2,
        to: 1,
        term: 1,
        ..Message::default()
    };
    vote.set_msg_type(MessageType::MsgRequestVote);
    assert!(input.send(Input::Step(vote, reply)).await.is_ok());
    received.await.unwrap().unwrap();
    let saved = Saved::open(&*engine, &config(0)).unwrap();
    assert_eq!((saved.hard.term, saved.hard.vote), (1, 2));
    drop(input);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn outbound_preserves_peer_order_reports_results_and_cancels_workers() {
    let (input, mut reports) = mpsc::channel::<raft_core::Input<()>>(8);
    let (observed, mut delivered) = mpsc::unbounded_channel();
    let outbound = raft_core::Outbound::start([2], input, move |to, message| {
        let observed = observed.clone();
        async move {
            observed.send((to, message.index)).unwrap();
            message.index != 2
        }
    });
    let messages = (1..=3)
        .map(|index| {
            let mut message = Message {
                to: 2,
                index,
                ..Message::default()
            };
            message.set_msg_type(if index == 3 {
                MessageType::MsgSnapshot
            } else {
                MessageType::MsgHeartbeat
            });
            message
        })
        .collect();
    assert!(outbound.send(messages).is_empty());
    for index in 1..=3 {
        assert_eq!(delivered.recv().await, Some((2, index)));
    }
    assert!(matches!(
        reports.recv().await,
        Some(raft_core::Input::Sent {
            to: 2,
            snapshot: false,
            ok: false
        })
    ));
    assert!(matches!(
        reports.recv().await,
        Some(raft_core::Input::Sent {
            to: 2,
            snapshot: true,
            ok: true
        })
    ));
    drop(outbound);
    assert!(reports.recv().await.is_none());

    let (input, mut reports) = mpsc::channel::<raft_core::Input<()>>(8);
    let (started, mut running) = mpsc::unbounded_channel();
    let outbound = raft_core::Outbound::start([2], input, move |_, _| {
        let started = started.clone();
        async move {
            started.send(()).unwrap();
            std::future::pending::<bool>().await
        }
    });
    assert!(
        outbound
            .send(vec![Message {
                to: 2,
                ..Message::default()
            }])
            .is_empty()
    );
    running.recv().await.unwrap();
    drop(outbound);
    assert!(reports.recv().await.is_none());
}

fn step_request(from: u64) -> Request<pb::ControlPacket> {
    let mut message = Message {
        from,
        to: 1,
        ..Message::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    Request::new(pb::ControlPacket {
        cell: "sto-1".into(),
        region: "eu".into(),
        dc: "sto".into(),
        voters: vec!["a:7000".into(), "b:7000".into(), "c:7000".into()],
        voter_ids: config(0).voter_ids,
        payload: message.write_to_bytes().unwrap(),
    })
}

#[cfg(not(madsim))]
#[test]
fn production_cell_raft_step_rejects_plaintext_voter_claims() {
    let actor = actor(Arc::new(MemoryEngine::new("cell")), 0);
    let error = validate_step(&actor.control, &step_request(2)).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
}

#[cfg(madsim)]
#[test]
fn simulation_cell_raft_step_keeps_plaintext_transport_available() {
    let actor = actor(Arc::new(MemoryEngine::new("cell")), 0);
    assert!(validate_step(&actor.control, &step_request(2)).is_ok());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn operator_handler_rejects_plaintext_even_with_a_valid_bearer() {
    use pb::cell_control_server::CellControl as _;
    let actor = actor(Arc::new(MemoryEngine::new("cell")), 0);
    let mut request = Request::new(write("plaintext", 0, 0, b""));
    request
        .metadata_mut()
        .insert("authorization", "Bearer operator".parse().unwrap());
    assert_eq!(
        Server(Some(actor.control))
            .write(request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}

#[test]
fn production_cell_raft_step_accepts_only_the_authenticated_voter() {
    let actor = actor(Arc::new(MemoryEngine::new("cell")), 0);
    let mut request = step_request(2);
    request.extensions_mut().insert(Caller {
        service: "relay".into(),
        cell: "sto-1".into(),
        node_id: "relay-b".into(),
        hosts: vec!["b".into()],
    });
    assert!(validate_step(&actor.control, &request).is_ok());

    let mut spoofed = step_request(2);
    spoofed.extensions_mut().insert(Caller {
        service: "relay".into(),
        cell: "sto-1".into(),
        node_id: "relay-c".into(),
        hosts: vec!["c".into()],
    });
    assert_eq!(
        validate_step(&actor.control, &spoofed).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );

    // A valid host SAN never authorizes another certificate's process identity.
    spoofed.extensions_mut().insert(Caller {
        service: "relay".into(),
        cell: "sto-1".into(),
        node_id: "relay-c".into(),
        hosts: vec!["b".into(), "c".into()],
    });
    assert_eq!(
        validate_step(&actor.control, &spoofed).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn voters_sharing_a_host_still_require_distinct_certificate_identities() {
    let mut actor = actor(Arc::new(MemoryEngine::new("cell")), 0);
    let control = Arc::get_mut(&mut actor.control).unwrap();
    control.config.voters = (1..=3).map(|port| format!("host:700{port}")).collect();
    control.config.voter_ids = control
        .config
        .voters
        .iter()
        .enumerate()
        .map(|(id, address)| (address.clone(), format!("relay-{id}")))
        .collect();
    let mut request = step_request(2);
    request.get_mut().voters = control.config.voters.clone();
    request.get_mut().voter_ids = control.config.voter_ids.clone();
    for (node_id, accepted) in [("relay-1", true), ("relay-2", false)] {
        request.extensions_mut().insert(Caller {
            service: "relay".into(),
            cell: "sto-1".into(),
            node_id: node_id.into(),
            hosts: vec!["host".into()],
        });
        assert_eq!(validate_step(control, &request).is_ok(), accepted);
    }
}

#[test]
fn voter_identity_configuration_requires_complete_unique_processes() {
    let config = config(0);
    validate_voter_ids(&config.voters, &config.voter_ids).unwrap();
    let mut invalid = config.voter_ids.clone();
    invalid.remove("b:7000");
    assert!(validate_voter_ids(&config.voters, &invalid).is_err());
    invalid.insert("b:7000".into(), "relay-c".into());
    assert!(validate_voter_ids(&config.voters, &invalid).is_err());
    invalid.insert("b:7000".into(), "".into());
    assert!(validate_voter_ids(&config.voters, &invalid).is_err());
}

struct Cluster {
    actors: Vec<Actor>,
    queues: Vec<(usize, usize, mpsc::Receiver<Message>)>,
    disks: Vec<Arc<MemoryEngine>>,
}

impl Cluster {
    fn new() -> Self {
        let disks: Vec<_> = (0..3)
            .map(|_| Arc::new(MemoryEngine::new("cell")))
            .collect();
        let mut actors: Vec<_> = disks
            .iter()
            .enumerate()
            .map(|(id, disk)| actor(disk.clone(), id))
            .collect();
        let mut queues = Vec::new();
        for (from, actor) in actors.iter_mut().enumerate() {
            for to in 0..3 {
                if from == to {
                    continue;
                }
                let (tx, rx) = mpsc::channel(256);
                actor.outbound.queues.insert(to as u64 + 1, tx);
                queues.push((from, to, rx));
            }
        }
        Self {
            actors,
            queues,
            disks,
        }
    }

    fn pump(&mut self, cut: impl Fn(usize, usize) -> bool) {
        for _ in 0..200 {
            let mut messages = Vec::new();
            for (from, to, rx) in &mut self.queues {
                while let Ok(message) = rx.try_recv() {
                    if !cut(*from, *to) {
                        messages.push((*to, message));
                    }
                }
            }
            if messages.is_empty() {
                return;
            }
            for (to, message) in messages {
                self.actors[to].raw.step(message).unwrap();
                self.actors[to].ready().unwrap();
            }
        }
        panic!("Raft messages did not settle");
    }

    fn elect(&mut self, id: usize) {
        self.actors[id].raw.campaign().unwrap();
        self.actors[id].ready().unwrap();
        self.pump(|_, _| false);
        assert!(self.actors[id].control.is_leader());
    }

    fn propose(
        &mut self,
        id: usize,
        request: pb::ControlWrite,
    ) -> oneshot::Receiver<Result<u64, Status>> {
        let (tx, rx) = oneshot::channel();
        self.actors[id].propose(request, tx);
        self.actors[id].ready().unwrap();
        rx
    }
}

fn write(identity: &str, expected: u64, term: u64, value: &[u8]) -> pb::ControlWrite {
    pb::ControlWrite {
        namespace: "cdn.sto-1".into(),
        key: b"node".to_vec(),
        value: Some(value.to_vec()),
        expected: Some(expected),
        idempotency_key: format!("1000:{identity}"),
        expected_term: term,
    }
}

#[test]
fn committed_state_cas_dedup_and_vote_log_survive_reopen() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    let request = write("first", 0, term, b"one");
    let mut first = cluster.propose(0, request.clone());
    cluster.pump(|_, _| false);
    assert_eq!(first.try_recv().unwrap().unwrap(), 1);
    let mut retry = cluster.propose(0, request);
    cluster.pump(|_, _| false);
    assert_eq!(
        retry.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::Aborted
    );
    let mut stale = cluster.propose(0, write("stale", 0, term, b"wrong"));
    cluster.pump(|_, _| false);
    assert_eq!(
        stale.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::Aborted
    );
    let drain = Drain {
        scope: Scope::Cell("sto-1".into()),
        datasets: Vec::new(),
        namespace: None,
        state: crate::reconciliation::drain::State::Draining,
        rate: 1,
    };
    let request = pb::ControlWrite {
        namespace: "cdn.sto-1".into(),
        key: b"drain/hold".to_vec(),
        value: Some(drain.encode()),
        expected: Some(1),
        idempotency_key: "1000:operator-hold".into(),
        expected_term: 0,
    };
    let mut drained = cluster.propose(0, request.clone());
    cluster.pump(|_, _| false);
    assert_eq!(drained.try_recv().unwrap().unwrap(), 2);
    let mut duplicate = cluster.propose(0, request.clone());
    assert_eq!(duplicate.try_recv().unwrap().unwrap(), 2);
    let cancel = pb::ControlWrite {
        value: None,
        expected: Some(2),
        idempotency_key: "1000:operator-cancel".into(),
        ..request.clone()
    };
    let mut cancelled = cluster.propose(0, cancel);
    cluster.pump(|_, _| false);
    assert_eq!(cancelled.try_recv().unwrap().unwrap(), 3);
    let mut late_retry = cluster.propose(0, request);
    assert_eq!(late_retry.try_recv().unwrap().unwrap(), 2);
    for (id, disk) in cluster.disks.iter().enumerate() {
        let saved = Saved::open(&**disk, &config(id)).unwrap();
        assert_eq!(saved.hard.term, term);
        assert_eq!(saved.hard.vote, 1);
        assert_eq!(saved.hard.commit, saved.state().applied);
        assert_eq!(saved.state().namespaces["cdn.sto-1"].head, 3);
        assert_eq!(
            saved.state().namespaces["cdn.sto-1"]
                .values
                .iter()
                .find(|p| p.key == b"node")
                .unwrap()
                .value,
            b"one"
        );
        assert_eq!(saved.state().namespaces["cdn.sto-1"].values.len(), 1);
        assert_eq!(saved.state().retries.len(), 2);
        assert_eq!(saved.snapshot.get_metadata().index, saved.state().applied);
    }
}

#[test]
fn minority_never_acknowledges_or_publishes_a_proposal() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    let mut result = cluster.propose(0, write("isolated", 0, term, b"uncommitted"));
    cluster.pump(|from, to| from == 0 || to == 0);
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);
    let durable = Saved::open(&*cluster.disks[0], &config(0)).unwrap();
    assert!(durable.entries.last().unwrap().index > durable.hard.commit);
    let reopened = actor(cluster.disks[0].clone(), 0);
    assert_eq!(reopened.control.head("cdn.sto-1").unwrap(), 0);
    assert_eq!(reopened.raw.raft.term, term);
}

#[test]
fn corrupted_voters_fail_closed_and_a_surviving_majority_can_commit() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    cluster.disks[2].corrupt(KEY);
    assert!(Saved::open(&*cluster.disks[2], &config(2)).is_err());
    let mut majority = cluster.propose(0, write("one-damaged", 0, term, b"safe"));
    cluster.pump(|from, to| from == 2 || to == 2);
    assert_eq!(majority.try_recv().unwrap().unwrap(), 1);

    cluster.disks[1].corrupt(KEY);
    assert!(Saved::open(&*cluster.disks[1], &config(1)).is_err());
    let mut minority = cluster.propose(0, write("two-damaged", 1, term, b"uncommitted"));
    cluster.pump(|from, to| from != 0 || to != 0);
    assert!(matches!(
        minority.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 1);
}

#[cfg(not(madsim))]
#[test]
fn heartbeat_and_rejected_proposal_allocations_do_not_copy_the_live_state() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    // Two MiB of real committed values makes a full-state copy exceed the traffic budget.
    for index in 0..32 {
        let mut request = write(&format!("large-{index}"), index, term, &vec![42; MAX_WRITE]);
        request.key = format!("node-{index:02}").into_bytes();
        let mut receipt = cluster.propose(0, request);
        cluster.pump(|_, _| false);
        assert_eq!(receipt.try_recv().unwrap().unwrap(), index + 1);
    }
    let commit = cluster.actors[1]
        .control
        .store
        .0
        .read()
        .unwrap()
        .hard
        .commit;
    let mut heartbeat = Message {
        from: 1,
        to: 2,
        term,
        commit,
        ..Message::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    let before = cluster.disks[1].get(KEY).unwrap();
    cluster.actors[1].raw.step(heartbeat).unwrap();
    assert!(cluster.actors[1].raw.has_ready());
    let (_, heartbeat_bytes) = allocations::measure(|| cluster.actors[1].ready().unwrap());
    let responses: Vec<_> = cluster
        .queues
        .iter_mut()
        .filter(|(from, to, _)| *from == 1 && *to == 0)
        .flat_map(|(_, _, rx)| std::iter::from_fn(|| rx.try_recv().ok()))
        .collect();
    assert!(
        responses
            .iter()
            .any(|m| m.get_msg_type() == MessageType::MsgHeartbeatResponse)
    );
    assert_eq!(cluster.disks[1].get(KEY).unwrap(), before);

    let mut invalid = write("invalid", 32, term, b"small");
    invalid.key.clear();
    let (tx, mut rx) = oneshot::channel();
    let (_, proposal_bytes) = allocations::measure(|| cluster.actors[0].propose(invalid, tx));
    assert_eq!(
        rx.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    eprintln!(
        "control allocations: heartbeat={heartbeat_bytes}, rejected proposal={proposal_bytes}"
    );
    // A generous message-processing allowance, independent of the two-MiB live view.
    assert!(
        heartbeat_bytes < 128 * 1024,
        "heartbeat allocated {heartbeat_bytes} bytes"
    );
    assert!(
        proposal_bytes < 128 * 1024,
        "proposal allocated {proposal_bytes} bytes"
    );
}

#[test]
fn newly_committed_leader_suffix_replaces_a_divergent_uncommitted_suffix() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    let mut old = cluster.propose(0, write("old-leader", 0, term, b"discarded"));
    cluster.pump(|from, to| from == 0 || to == 0);
    assert!(matches!(
        old.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // The other two replicas form a majority and elect a replacement while the old leader is isolated.
    for _ in 0..30 {
        for id in [1, 2] {
            cluster.actors[id].raw.tick();
            cluster.actors[id].ready().unwrap();
        }
        cluster.pump(|from, to| from == 0 || to == 0);
    }
    let leader = [1, 2]
        .into_iter()
        .find(|id| cluster.actors[*id].control.is_leader())
        .expect("majority elects a new leader");
    let new_term = cluster.actors[leader].raw.raft.term;
    assert!(new_term > term);
    let mut chosen = cluster.propose(leader, write("new-leader", 0, new_term, b"retained"));
    cluster.pump(|from, to| from == 0 || to == 0);
    assert_eq!(chosen.try_recv().unwrap().unwrap(), 1);
    // Reconnection replaces the old leader's index 2 and advances commit in the same Ready.
    for _ in 0..4 {
        cluster.actors[leader].raw.tick();
        cluster.actors[leader].ready().unwrap();
        cluster.pump(|_, _| false);
    }
    let reopened = actor(cluster.disks[0].clone(), 0);
    assert_eq!(reopened.control.head("cdn.sto-1").unwrap(), 1);
    assert_eq!(
        reopened.control.view("cdn.sto-1").unwrap().values[b"node".as_slice()],
        b"retained"
    );
}

#[test]
fn snapshot_keeps_live_values_retry_results_and_original_commit_time() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    for index in 0..300 {
        let mut result = cluster.propose(0, write(&format!("put-{index}"), index, term, b"value"));
        cluster.pump(|_, _| false);
        assert_eq!(result.try_recv().unwrap().unwrap(), index + 1);
    }
    let disk = Saved::open(&*cluster.disks[0], &config(0)).unwrap();
    assert!(disk.disk.truncated > 0);
    assert!(disk.entries.len() < 256);
    let snapshot = State::decode(unchecked(&disk.snapshot.data).unwrap().as_slice()).unwrap();
    let ns = &snapshot.namespaces["cdn.sto-1"];
    assert_eq!(ns.values.len(), 1);
    assert_eq!(ns.head, 300);
    assert_eq!(ns.events.len(), MAX_TAIL);
    assert!(ns.events.iter().all(|event| event.committed_at == 1000));
    assert!(snapshot.retries.is_empty());
    let view = cluster.actors[0].control.view("cdn.sto-1").unwrap();
    assert_eq!(view.sequence, 300);
    assert_eq!(view.values[b"node".as_slice()], b"value");
    assert_eq!(
        cluster.actors[0]
            .control
            .read("cdn.sto-1", 1, 1)
            .unwrap_err()
            .code(),
        tonic::Code::OutOfRange
    );
}

#[test]
fn lagging_voter_installs_a_real_snapshot_after_log_compaction() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    for index in 0..280 {
        let mut result = cluster.propose(
            0,
            write(&format!("snapshot-{index}"), index, term, b"current"),
        );
        cluster.pump(|from, to| from == 2 || to == 2);
        assert_eq!(result.try_recv().unwrap().unwrap(), index + 1);
    }
    assert_eq!(cluster.actors[2].control.head("cdn.sto-1").unwrap(), 0);
    assert!(
        cluster.actors[0]
            .control
            .store
            .0
            .read()
            .unwrap()
            .disk
            .truncated
            > 1
    );
    for _ in 0..10 {
        cluster.actors[0].raw.tick();
        cluster.actors[0].ready().unwrap();
        cluster.pump(|_, _| false);
    }
    let follower = actor(cluster.disks[2].clone(), 2);
    let view = follower.control.view("cdn.sto-1").unwrap();
    assert_eq!(view.sequence, 280);
    assert_eq!(view.values[b"node".as_slice()], b"current");
    assert_eq!(view.committed_at, 1000);
    assert_eq!(
        follower.control.read("cdn.sto-1", 280, 1).unwrap()[0].sequence,
        280
    );
}

#[test]
fn empty_corrupt_and_mismatched_control_disks_fail_closed() {
    let engine = MemoryEngine::new("control");
    let mut config = config(0);
    config.bootstrap = false;
    assert!(Saved::open(&engine, &config).is_err());
    config.bootstrap = true;
    Saved::open(&engine, &config).unwrap();
    config.voters[2] = "replacement:7000".into();
    assert!(
        Saved::open(&engine, &config)
            .unwrap_err()
            .contains("mismatch")
    );
    config = self::config(0);
    config
        .voter_ids
        .insert("b:7000".into(), "replacement-id".into());
    assert!(
        Saved::open(&engine, &config)
            .unwrap_err()
            .contains("mismatch")
    );
    config = self::config(0);
    config.dc = "different".into();
    assert!(
        Saved::open(&engine, &config)
            .unwrap_err()
            .contains("mismatch")
    );
    engine.put(KEY, b"broken").unwrap();
    assert!(Saved::open(&engine, &self::config(0)).is_err());
}

#[test]
fn expired_retry_cannot_become_a_fresh_mutation() {
    let state = State {
        namespaces: SERVICES
            .iter()
            .map(|s| (namespace(s, "sto-1"), Namespace::default()))
            .collect(),
        ..State::default()
    };
    let request = write("expired", 0, 1, b"stale");
    assert_eq!(
        validate_request(&config(0), &state, &request, 1000 + RETRY_MS)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut request = request;
    request.idempotency_key = format!("{}:future", 1000 + CLOCK_SKEW_MS + 1);
    assert_eq!(
        validate_request(&config(0), &state, &request, 1000)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn operator_route_cannot_write_members_or_other_cells() {
    let mut request = write("operator", 0, 0, b"bad");
    assert_eq!(
        validate_operator(&config(0), &request).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    request.key = b"drain/remove".to_vec();
    request.value = None;
    assert!(validate_operator(&config(0), &request).is_ok());
    request.namespace = "cdn.other-cell".into();
    assert_eq!(
        validate_operator(&config(0), &request).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn unsupported_operator_drains_are_rejected_before_the_raft_log() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let mut record = Drain {
        scope: Scope::Node("missing-node".into()),
        datasets: vec!["kv".into()],
        namespace: None,
        state: crate::reconciliation::drain::State::Draining,
        rate: 1,
    };
    let request = pb::ControlWrite {
        namespace: "cdn.sto-1".into(),
        key: Drain::key("unsupported").into_bytes(),
        value: Some(record.encode()),
        expected: Some(0),
        idempotency_key: "1000:unsupported".into(),
        expected_term: 0,
    };
    let mut result = cluster.propose(0, request);
    assert_eq!(
        result.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);

    record.datasets = vec!["cdn".into()];
    record.namespace = Some("tenant".into());
    let request = pb::ControlWrite {
        key: Drain::key("namespace").into_bytes(),
        value: Some(record.encode()),
        idempotency_key: "1000:namespace".into(),
        ..write("namespace", 0, 0, b"")
    };
    let mut result = cluster.propose(0, request);
    assert_eq!(
        result.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);

    record.namespace = None;
    let request = pb::ControlWrite {
        key: Drain::key("unknown-target").into_bytes(),
        value: Some(record.encode()),
        idempotency_key: "1000:unknown-target".into(),
        ..write("unknown-target", 0, 0, b"")
    };
    let mut result = cluster.propose(0, request);
    assert_eq!(
        result.try_recv().unwrap().unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);

    record.state = crate::reconciliation::drain::State::Drained;
    let request = pb::ControlWrite {
        key: Drain::key("forged").into_bytes(),
        value: Some(record.encode()),
        idempotency_key: "1000:forged-complete".into(),
        ..write("forged-complete", 0, 0, b"")
    };
    assert_eq!(
        validate_operator(&config(0), &request).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);
}

#[test]
fn planner_completion_is_committed_only_after_zero_units_on_a_quorum() {
    use crate::placement::{Owner, State as RangeState};
    use crate::reconciliation::drain::{Hook, Ranged, State as DrainState, completed};

    let record = Drain {
        scope: Scope::Node("node".into()),
        datasets: vec!["cdn".into()],
        namespace: None,
        state: DrainState::Draining,
        rate: 1,
    };
    let held: Vec<Arc<dyn Hook>> = vec![Arc::new(Ranged::new("cdn", || {
        Ok(BTreeMap::from([(
            0,
            vec![Owner {
                node: "node".into(),
                state: RangeState::Ready,
            }],
        )]))
    }))];
    assert_eq!(completed(&held, &record).unwrap(), None);

    let empty: Vec<Arc<dyn Hook>> = vec![Arc::new(Ranged::new("cdn", || Ok(BTreeMap::new())))];
    let completed = completed(&empty, &record).unwrap().unwrap();
    assert_eq!(completed.state, DrainState::Drained);

    let mut cluster = Cluster::new();
    cluster.elect(0);
    let term = cluster.actors[0].raw.raft.term;
    let request = pb::ControlWrite {
        namespace: "cdn.sto-1".into(),
        key: Drain::key("node").into_bytes(),
        value: Some(completed.encode()),
        expected: Some(0),
        idempotency_key: String::new(),
        expected_term: term,
    };
    let mut result = cluster.propose(0, request);
    cluster.pump(|from, to| from == 0 || to == 0);
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(cluster.actors[0].control.head("cdn.sto-1").unwrap(), 0);

    for _ in 0..2 {
        cluster.actors[0].raw.tick();
        cluster.actors[0].ready().unwrap();
    }
    cluster.pump(|_, _| false);
    assert_eq!(result.try_recv().unwrap().unwrap(), 1);
    for (id, disk) in cluster.disks.iter().enumerate() {
        let saved = Saved::open(&**disk, &config(id)).unwrap();
        let value = saved.state().namespaces["cdn.sto-1"]
            .values
            .iter()
            .find(|pair| pair.key.as_slice() == Drain::key("node").as_bytes())
            .unwrap();
        assert_eq!(
            Drain::decode(&value.value).unwrap().state,
            DrainState::Drained
        );
    }
}

#[test]
fn internal_planner_writes_do_not_fill_the_operator_retry_window() {
    let engine = MemoryEngine::new("control");
    let mut saved = Saved::open(&engine, &config(0)).unwrap();
    for index in 0..1100 {
        let request = write(&format!("internal-{index}"), index, 1, b"value");
        let mut entry = Entry {
            index: index + 1,
            term: 1,
            ..Default::default()
        };
        entry.set_data(
            Command {
                request: Some(request),
                at: 1000,
            }
            .encode_to_vec()
            .into(),
        );
        let mut results = Vec::new();
        saved.disk.apply(&entry, &mut results).unwrap();
        assert_eq!(results.pop().unwrap().1.unwrap(), index + 1);
    }
    assert_eq!(saved.state().namespaces["cdn.sto-1"].head, 1100);
    assert!(saved.state().retries.is_empty());
}
