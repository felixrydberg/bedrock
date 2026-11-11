use super::*;
use crate::admin::Admin;

fn hub() -> Hub {
    Hub::relay(
        "cell".into(),
        1,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        None,
    )
}

fn replay_event(sequence: u64) -> Event {
    Event {
        dataset: "auth".into(),
        namespace: "keys".into(),
        stream: "auth-0".into(),
        sequence,
        epoch: 1,
        committed_at: sequence,
        key: sequence.to_be_bytes().to_vec(),
        operation: crate::replication::Operation::Set(vec![42]),
    }
}

#[tokio::test]
async fn reconnect_resumes_after_only_the_contiguous_cached_prefix() {
    let replay = Arc::new(
        Replay::open(
            Arc::new(crate::storage::memory::MemoryEngine::new("replay")),
            Default::default(),
        )
        .unwrap(),
    );
    for sequence in [1, 2, 5, 6] {
        replay.append(&replay_event(sequence), 6).unwrap();
    }
    let hub = Hub::relay(
        "cell".into(),
        1,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        Some(replay),
    );
    let key = ("auth".into(), "keys".into());
    assert_eq!(hub.resume_from(&key, 1), 3);
    hub.encoded(replay_event(3));
    assert_eq!(hub.resume_from(&key, 1), 4);
    hub.encoded(replay_event(4));
    assert_eq!(hub.resume_from(&key, 1), 7);
    assert_eq!(hub.source_head(&key), Some(6));
    hub.observe_head(&key, 2);
    assert_eq!(hub.source_head(&key), Some(6));
    hub.observe_head(&key, 7);
    assert_eq!(hub.source_head(&key), Some(7));
    assert_eq!(hub.resume_from(&key, 8), 8);
}

fn subscriber(hub: &Hub, id: u64, capacity: usize) -> (Frames, Arc<Watched<bool>>) {
    let (tx, rx) = mpsc::channel(capacity);
    let bytes = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(Watched::new(false));
    let changed = Arc::new(Watched::new(0));
    let follows = BTreeMap::from([(
        (("auth".into(), "keys".into()), None),
        Interest {
            from: 1,
            acked: 0,
            generation: id,
            follow: id + 100,
            baseline: 0,
            confirming: false,
            bootstrap_acked: false,
            target: None,
            woken: Default::default(),
        },
    )]);
    hub.0.subscribers.lock().unwrap().insert(
        id,
        Subscriber {
            tx,
            bytes: bytes.clone(),
            events: events.clone(),
            closed: closed.clone(),
            follows,
            ranges: Default::default(),
            changed: changed.clone(),
            member: None,
            pushed: BTreeMap::new(),
            ranges_acked: BTreeMap::new(),
            moved: None,
        },
    );
    (
        Frames {
            rx: ReceiverStream::new(rx),
            bytes,
            events,
            changed,
            owner: Some((hub.clone(), id)),
        },
        closed,
    )
}

#[tokio::test]
async fn cell_bootstrap_preserves_coherent_boundary_live_order_and_wire_checksum() {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("membership".into(), "kv.cell".into()), None);
    {
        let mut subscribers = hub.0.subscribers.lock().unwrap();
        let follows = &mut subscribers.get_mut(&1).unwrap().follows;
        let interest = follows
            .remove(&(("auth".into(), "keys".into()), None))
            .unwrap();
        follows.insert(key.clone(), interest);
    }
    let view = crate::replication::materialized::View {
        dataset: key.0.0.clone(),
        namespace: key.0.1.clone(),
        sequence: 400,
        committed_at: 1234,
        values: (0u64..300)
            .rev()
            .map(|k| (k.to_be_bytes().to_vec(), vec![42]))
            .collect(),
        retries: Default::default(),
        pruned_through: 400,
        raft: None,
    };
    assert!(hub.control_bootstrap(1, &key, 1, view).await.unwrap());
    let mut keys = Vec::new();
    let mut chunks = 0;
    loop {
        let frame = tokio_stream::StreamExt::next(&mut frames)
            .await
            .unwrap()
            .unwrap();
        assert!(frame.encoded_len() <= MAX_BYTES);
        let Some(pb::relay_frame::Kind::Bootstrap(chunk)) = frame.kind else {
            panic!("expected bootstrap");
        };
        assert_eq!(
            (chunk.boundary, chunk.committed_at, chunk.follow),
            (400, 1234, 101)
        );
        assert_eq!((&chunk.dataset, &chunk.namespace), (&key.0.0, &key.0.1));
        assert_eq!(chunk.begin, chunks == 0);
        assert!(chunk.entries.len() <= 256);
        for entry in chunk.entries {
            assert_eq!(entry.value, vec![42]);
            keys.push(u64::from_be_bytes(entry.key.try_into().unwrap()));
        }
        chunks += 1;
        if chunk.end {
            // SHA-256 of the protocol's length-prefixed scope, boundary, timestamp and 300 live entries.
            let checksum: String = chunk.checksum.iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(
                checksum,
                "889f6c7362be120b0a63f0bea68c2b7e639e2c41021fd971c497e2a82f96674b"
            );
            break;
        }
        assert!(chunk.checksum.is_empty());
    }
    assert_eq!(keys, (0..300).collect::<Vec<_>>());
    assert_eq!(chunks, 4);
}

#[tokio::test]
async fn cell_bootstrap_rejects_an_entry_larger_than_the_bounded_frame() {
    let hub = hub();
    let (_frames, closed) = subscriber(&hub, 1, MAX_EVENTS);
    let key: StreamKey = (("auth".into(), "keys".into()), None);
    let view = crate::replication::materialized::View {
        dataset: key.0.0.clone(),
        namespace: key.0.1.clone(),
        sequence: 1,
        committed_at: 1234,
        values: BTreeMap::from([(vec![1], vec![42; MAX_BYTES])]),
        retries: Default::default(),
        pruned_through: 1,
        raft: None,
    };
    let error = hub.control_bootstrap(1, &key, 1, view).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(!closed.get());
}

#[tokio::test]
async fn cell_membership_uses_local_control_despite_central_history_and_unavailable_upstream() {
    use crate::membership::control::{Config, Control};
    let peers = Arc::new(Peers::new(&Admin::new()));
    let (control, actor) = Control::start(
        Arc::new(crate::storage::memory::MemoryEngine::new("cell-control")),
        peers.clone(),
        Config {
            region: "region".into(),
            dc: "dc".into(),
            cell: "cell".into(),
            me: "one:7000".into(),
            voters: vec!["one:7000".into(), "two:7000".into(), "three:7000".into()],
            voter_ids: ["one", "two", "three"]
                .into_iter()
                .map(|id| (format!("{id}:7000"), id.into()))
                .collect(),
            clock: || 1234,
            operator_token: None,
            bootstrap: true,
        },
    )
    .unwrap();
    // Reading the committed view must work without elections or any live control/product peer.
    actor.abort();
    let replay = Arc::new(
        Replay::open(
            Arc::new(crate::storage::memory::MemoryEngine::new("central-history")),
            Default::default(),
        )
        .unwrap(),
    );
    let mut central = replay_event(1);
    central.dataset = "membership".into();
    central.namespace = "kv.cell".into();
    replay.append(&central, 1).unwrap();
    let hub = Hub::relay(
        "one".into(),
        1,
        peers,
        vec!["unavailable:7000".into()],
        Vec::new(),
        Some(replay),
    )
    .with_cell("cell".into())
    .with_control(control);
    hub.publish(central);
    let key = (("membership".into(), "kv.cell".into()), None);
    hub.observe_head(&key.0, 99);
    assert_eq!(hub.source_head(&key.0), Some(0));
    assert!(hub.read(&key.0, 1).await.unwrap().is_empty());
    for namespace in ["kv.other", "unknown.cell", ""] {
        let foreign = ("membership".into(), namespace.into());
        assert_eq!(hub.source_head(&foreign), None);
        assert_eq!(
            hub.read(&foreign, 1).await.err().unwrap().code(),
            tonic::Code::InvalidArgument
        );
    }
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    {
        let mut subscribers = hub.0.subscribers.lock().unwrap();
        let follows = &mut subscribers.get_mut(&1).unwrap().follows;
        let mut interest = follows
            .remove(&(("auth".into(), "keys".into()), None))
            .unwrap();
        interest.from = 0;
        follows.insert(key.clone(), interest);
    }
    let sibling_follow = pb::Follow {
        dataset: "membership".into(),
        namespace: "cdn.cell".into(),
        from: 1,
        ..Default::default()
    };
    let mut shared = siblings::Shared::default();
    shared.live = Some(BTreeMap::from([(
        (("membership".into(), "cdn.cell".into()), None),
        sibling_follow,
    )]));
    hub.0.shared.lock().unwrap().insert("two".into(), shared);
    let (_product_frames, _) = subscriber(&hub, 2, MAX_EVENTS);
    let interests = hub.interests();
    assert_eq!(interests.len(), 1);
    assert_eq!(interests.values().next().unwrap().dataset, "auth");
    let (desired, datasets) = hub.desired();
    assert_eq!(
        desired,
        BTreeMap::from([(("auth".into(), "keys".into()), 1)])
    );
    assert!(datasets.is_empty());
    let pumping = hub.clone();
    let task = tokio::spawn(async move { pumping.pump(1, key, 1, Vec::new(), true).await });
    let mut began = false;
    let mut ended = false;
    loop {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            tokio_stream::StreamExt::next(&mut frames),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        match frame.kind.unwrap() {
            pb::relay_frame::Kind::Range(range) => {
                assert_eq!((range.first, range.last, range.head), (0, 0, 0));
                if range.recovered {
                    assert!(began && ended);
                    break;
                }
            }
            pb::relay_frame::Kind::Bootstrap(chunk) => {
                assert_eq!((chunk.boundary, chunk.committed_at), (0, 0));
                assert!(chunk.entries.is_empty());
                began |= chunk.begin;
                ended |= chunk.end;
            }
            _ => panic!("expected local bootstrap or range"),
        }
    }
    task.abort();
}

struct RecoveryCell {
    head: Arc<Watched<u64>>,
    advance_during_bootstrap: bool,
}

impl LocalControlSource for RecoveryCell {
    fn validate_namespace(&self, _: &str, _: &str) -> Result<(), Status> {
        Ok(())
    }
    fn identity(&self) -> pb::ControlAck {
        Default::default()
    }
    fn validate_peer(&self, _: &pb::ControlAck) -> Result<(), Status> {
        Ok(())
    }
    fn changed(&self) -> Arc<Watched<u64>> {
        self.head.clone()
    }
    fn head(&self, _: &str) -> Result<u64, Status> {
        Ok(self.head.get())
    }
    fn read(&self, namespace: &str, from: u64, _: usize) -> Result<Vec<Event>, Status> {
        Ok((from..=self.head.get())
            .map(|sequence| Event {
                dataset: "membership".into(),
                namespace: namespace.into(),
                ..replay_event(sequence)
            })
            .collect())
    }
    fn view(&self, namespace: &str) -> Result<crate::replication::materialized::View, Status> {
        let sequence = self.head.get();
        if self.advance_during_bootstrap {
            self.head.update(|head| *head += 1);
        }
        Ok(crate::replication::materialized::View {
            dataset: "membership".into(),
            namespace: namespace.into(),
            sequence,
            committed_at: 1234,
            values: Default::default(),
            retries: Default::default(),
            pruned_through: sequence,
            raft: None,
        })
    }
}

fn cell_recovery(boundary: u64, advance_during_bootstrap: bool) -> (Hub, Frames, StreamKey) {
    let hub = hub()
        .with_cell("cell".into())
        .with_control(Arc::new(RecoveryCell {
            head: Arc::new(Watched::new(boundary)),
            advance_during_bootstrap,
        }));
    let (frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("membership".into(), "kv.cell".into()), None);
    {
        let mut subscribers = hub.0.subscribers.lock().unwrap();
        let follows = &mut subscribers.get_mut(&1).unwrap().follows;
        let interest = follows
            .remove(&(("auth".into(), "keys".into()), None))
            .unwrap();
        follows.insert(key.clone(), interest);
    }
    (hub, frames, key)
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cell_recovery_never_acknowledged_gives_up_its_slot() {
    let (hub, _frames, key) = cell_recovery(5, false);
    let permits = hub.0.recoveries.available_permits();
    let pumping = hub.clone();
    let task = tokio::spawn(async move { pumping.pump(1, key, 1, Vec::new(), true).await });
    // Bootstrapped through 5, but the subscriber never acknowledges: the slot is held, then released after IDLE.
    tokio::time::sleep(IDLE / 2).await;
    assert_eq!(hub.0.recoveries.available_permits(), permits - 1);
    let error = tokio::time::timeout(IDLE + PROGRESS, task)
        .await
        .expect("a silent subscriber cannot hold a recovery slot forever")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(hub.0.recoveries.available_permits(), permits);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cell_bootstrap_ack_renews_recovery_while_the_tail_is_pending() {
    for boundary in [0, 5] {
        bootstrap_ack_renews_recovery(boundary).await;
    }
}

async fn bootstrap_ack_renews_recovery(boundary: u64) {
    let (hub, mut frames, key) = cell_recovery(boundary, true);
    let head = boundary + 1;
    let permits = hub.0.recoveries.available_permits();
    let pumping = hub.clone();
    let task = tokio::spawn(async move { pumping.pump(1, key, 1, Vec::new(), true).await });
    let mut next = async || {
        tokio::time::timeout(PROGRESS, tokio_stream::StreamExt::next(&mut frames))
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .kind
            .unwrap()
    };
    loop {
        if let pb::relay_frame::Kind::Bootstrap(chunk) = next().await
            && chunk.end
        {
            assert_eq!(chunk.boundary, boundary);
            break;
        }
    }

    assert!(
        matches!(next().await, pb::relay_frame::Kind::Delivery(d) if (d.from, d.through) == (head, head))
    );
    let acknowledge = |applied| {
        hub.acknowledge(
            1,
            pb::Ack {
                dataset: "membership".into(),
                namespace: "kv.cell".into(),
                applied,
                span: None,
            },
        );
    };
    tokio::time::sleep(IDLE / 2).await;
    acknowledge(boundary);
    tokio::time::sleep(IDLE / 2 + PROGRESS + Duration::from_secs(1)).await;
    assert_eq!(hub.0.recoveries.available_permits(), permits - 1);

    acknowledge(head);
    assert!(
        matches!(next().await, pb::relay_frame::Kind::Range(r) if r.recovered && r.head == head)
    );
    assert_eq!(hub.0.recoveries.available_permits(), permits);
    task.abort();
}

#[tokio::test]
async fn a_closed_recovery_does_not_trip_its_sources() {
    let peers = Arc::new(Peers::new(&Admin::new()));
    let sources = vec!["first:7000".to_owned(), "second:7000".to_owned()];
    let hub = Hub::relay(
        "cell".into(),
        1,
        peers.clone(),
        sources.clone(),
        Vec::new(),
        None,
    );
    let (frames, _) = subscriber(&hub, 1, 1);
    drop(frames);
    let stream = (("auth".into(), "keys".into()), None);

    hub.proxy(1, &stream, 1, &[], false).await.unwrap();
    for source in &sources {
        assert_eq!(peers.health().state(source), crate::health::State::Closed);
    }
}

#[tokio::test]
async fn full_queue_disconnects_only_stalled_subscriber_and_releases_interest() {
    let hub = hub();
    let (_slow, closed) = subscriber(&hub, 1, 2);
    let (mut fast, _) = subscriber(&hub, 2, 2);
    assert!(hub.send(1, pb::relay_frame::Kind::Tick(true)));
    assert!(hub.send(1, pb::relay_frame::Kind::Tick(true)));
    assert!(!hub.send(1, pb::relay_frame::Kind::Tick(true)));
    assert!(closed.get());
    assert!(hub.send(2, pb::relay_frame::Kind::Tick(true)));
    assert!(
        tokio_stream::StreamExt::next(&mut fast)
            .await
            .unwrap()
            .is_ok()
    );
    assert_eq!(fast.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(hub.0.subscribers.lock().unwrap().len(), 1);
    drop(fast);
    assert!(hub.idle());
}

#[tokio::test]
async fn byte_budget_disconnects_before_accepting_an_oversized_frame() {
    let hub = hub();
    let (_frames, closed) = subscriber(&hub, 1, MAX_EVENTS);
    assert!(!hub.send(
        1,
        pb::relay_frame::Kind::Delivery(pb::Delivery {
            dataset: "auth".into(),
            namespace: "keys".into(),
            from: 1,
            through: 1,
            events: vec![Bytes::from(vec![0; MAX_BYTES])],
            span: None,
            ..Default::default()
        })
    ));
    assert!(closed.get());
    assert!(hub.idle());
}

#[tokio::test]
async fn bootstrap_boundary_does_not_consume_the_event_acknowledgement_window() {
    let hub = hub();
    let (_frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("auth".into(), "keys".into()), None);
    hub.advance(1, &key, 1, 100_000);
    hub.baseline(1, &key, 1, 100_000);
    assert_eq!(hub.outstanding(1, &key), 0);
    assert_eq!(hub.current(1, &key, 1), Some((100_001, 0, false)));
    hub.advance(1, &key, 1, 100_256);
    assert_eq!(hub.outstanding(1, &key), 256);
}

#[tokio::test]
async fn unsubscribe_wakes_a_full_window_without_dropping_other_interests() {
    let hub = hub();
    let (_frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("auth".into(), "keys".into()), None);
    hub.advance(1, &key, 1, MAX_EVENTS as u64);
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .follows
        .insert(
            (("auth".into(), "other".into()), None),
            Interest {
                from: 1,
                acked: 0,
                generation: 2,
                follow: 0,
                baseline: 0,
                confirming: false,
                bootstrap_acked: false,
                target: None,
                woken: Default::default(),
            },
        );
    let pumping = hub.clone();
    let task = tokio::spawn(async move {
        pumping
            .pump(
                1,
                (("auth".into(), "keys".into()), None),
                1,
                Vec::new(),
                false,
            )
            .await
    });
    tokio::task::yield_now().await;
    hub.unfollow(
        1,
        pb::Unfollow {
            dataset: "auth".into(),
            namespace: "keys".into(),
            span: None,
        },
    );
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        hub.desired(),
        (
            BTreeMap::from([(("auth".into(), "other".into()), 1)]),
            BTreeSet::new()
        )
    );
}

#[tokio::test]
async fn replaced_generation_cannot_enqueue_into_the_new_subscription() {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("auth".into(), "keys".into()), None);
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .follows
        .get_mut(&key)
        .unwrap()
        .generation = 2;
    assert!(
        !hub.send_scoped(1, &key, 1, pb::relay_frame::Kind::Tick(true))
            .await
    );
    assert!(
        hub.send_scoped(1, &key, 2, pb::relay_frame::Kind::Tick(true))
            .await
    );
    assert!(
        tokio_stream::StreamExt::next(&mut frames)
            .await
            .unwrap()
            .is_ok()
    );
}

/// Found by the crash tests (#25 / B-23 Layer 3): a relay took deliveries still in flight for a follow it had ended as a
/// coverage gap in its next one, so each stream switch cost a reconnect. Frames now carry the follow's number.
#[tokio::test]
async fn deliveries_and_bootstraps_carry_the_subscribers_follow_number() {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = (("auth".into(), "keys".into()), None);
    let delivery = pb::relay_frame::Kind::Delivery(pb::Delivery {
        from: 1,
        through: 1,
        ..Default::default()
    });
    let bootstrap = pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
        begin: true,
        ..Default::default()
    });
    for kind in [delivery, bootstrap] {
        assert!(hub.send_scoped(1, &key, 1, kind).await);
        let frame = tokio_stream::StreamExt::next(&mut frames)
            .await
            .unwrap()
            .unwrap();
        let follow = match frame.kind.unwrap() {
            pb::relay_frame::Kind::Delivery(d) => d.follow,
            pb::relay_frame::Kind::Bootstrap(b) => b.follow,
            other => panic!("unexpected frame {other:?}"),
        };
        assert_eq!(follow, 101);
    }
}

#[tokio::test]
async fn queued_record_bound_applies_across_namespaces_not_only_frames() {
    let hub = hub();
    let (_frames, closed) = subscriber(&hub, 1, MAX_EVENTS);
    let delivery = |namespace: &str| {
        pb::relay_frame::Kind::Delivery(pb::Delivery {
            dataset: "auth".into(),
            namespace: namespace.into(),
            from: 1,
            through: 600,
            events: vec![Bytes::new(); 600],
            span: None,
            ..Default::default()
        })
    };
    assert!(hub.send(1, delivery("one")));
    assert!(!hub.send(1, delivery("two")));
    assert!(closed.get());
}

#[tokio::test]
async fn a_full_replay_persistence_queue_never_blocks_live_history() {
    let replay = Arc::new(
        Replay::open(
            Arc::new(crate::storage::memory::MemoryEngine::new("replay")),
            Default::default(),
        )
        .unwrap(),
    );
    let hub = Hub::relay(
        "cell".into(),
        1,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        Some(replay.clone()),
    );
    for sequence in 1u64..=2048 {
        hub.publish(Event {
            dataset: "auth".into(),
            namespace: "keys".into(),
            stream: "auth-0".into(),
            sequence,
            epoch: 1,
            committed_at: sequence,
            key: sequence.to_be_bytes().to_vec(),
            operation: crate::replication::Operation::Set(vec![42; 8192]),
        });
    }
    assert!(hub.0.replay_bytes.load(Ordering::Relaxed) <= MAX_BYTES);
    assert!(replay.ranges("auth", "keys").is_empty());
    let key = ("auth".into(), "keys".into());
    assert_eq!(hub.source_head(&key), Some(2048));
    assert_eq!(hub.read(&key, 2048).await.unwrap()[0].0, 2048);
}

#[tokio::test]
async fn bootstrap_waits_for_a_slow_reader_instead_of_disconnecting() {
    use crate::replication::materialized::Retention;
    use crate::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
    const KEYS: u64 = 1500;
    let writer = Writer::open(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(crate::storage::sim::MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    for key in 0..KEYS {
        writer
            .commit(Write {
                dataset: "auth".into(),
                namespace: "keys".into(),
                key: key.to_be_bytes().to_vec(),
                operation: crate::replication::Operation::Set(vec![42]),
                idempotency_key: None,
                expected: None,
            })
            .await
            .unwrap();
    }
    let hub = Hub::writer(
        "writer".into(),
        writer.clone(),
        Arc::new(Peers::new(&Admin::new())),
    );
    let (mut frames, closed) = subscriber(&hub, 1, MAX_EVENTS);
    let pumping = hub.clone();
    let task = tokio::spawn(async move {
        let key = (("auth".into(), "keys".into()), None);
        pumping.pump(1, key, 1, Vec::new(), true).await
    });
    // The reader stalls while the queue holds more than MAX_EVENTS worth of entries.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!closed.get());
    let mut entries = 0;
    loop {
        let frame = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_stream::StreamExt::next(&mut frames),
        )
        .await
        .unwrap()
        .expect("subscriber stayed connected")
        .unwrap();
        if let Some(pb::relay_frame::Kind::Bootstrap(b)) = frame.kind {
            entries += b.entries.len() as u64;
            if b.end {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(entries, KEYS);
    assert!(!closed.get());
    task.abort();
    writer.close().await.unwrap();
}

fn member(node_id: &str, status: pb::member::Status) -> pb::Member {
    pb::Member {
        node_id: node_id.into(),
        service: "kv".into(),
        cell: "eu-1a".into(),
        status: status as i32,
        ..Default::default()
    }
}

#[tokio::test]
async fn members_track_sessions_status_updates_and_graceful_leaves() {
    let hub = hub();
    let (_old, _) = subscriber(&hub, 1, 4);
    let (_new, _) = subscriber(&hub, 2, 4);
    let (_other, _) = subscriber(&hub, 3, 4);
    for (id, node) in [(1, "kv-1"), (2, "kv-1"), (3, "kv-2")] {
        hub.0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&id)
            .unwrap()
            .member = Some(member(node, pb::member::Status::Ready));
    }
    assert_eq!(hub.members().len(), 2);
    // Only the same identity may change, and a client cannot claim to have left.
    assert!(!hub.member(2, member("kv-2", pb::member::Status::Draining)));
    assert!(!hub.member(2, member("kv-1", pb::member::Status::Left)));
    let version = hub.member_version().get();
    // An update changes only the status; the rest of the entry stays as announced.
    assert!(hub.member(
        2,
        pb::Member {
            address: "elsewhere:7000".into(),
            ..member("kv-1", pb::member::Status::Draining)
        }
    ));
    assert_ne!(hub.member_version().get(), version);
    assert_eq!(
        hub.0.subscribers.lock().unwrap()[&2].member,
        Some(member("kv-1", pb::member::Status::Draining))
    );
    // Storage health changes too (B-14), but only for datasets the member announced.
    let with = |datasets: &[&str], unhealthy: &[&str]| pb::Member {
        datasets: datasets.iter().map(|d| d.to_string()).collect(),
        unhealthy: unhealthy.iter().map(|d| d.to_string()).collect(),
        ..member("kv-1", pb::member::Status::Draining)
    };
    assert!(!hub.member(2, with(&["auth"], &["auth"])));
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&2)
        .unwrap()
        .member = Some(with(&["auth"], &[]));
    assert!(hub.member(2, with(&["auth"], &["auth"])));
    assert_eq!(
        hub.0.subscribers.lock().unwrap()[&2].member,
        Some(with(&["auth"], &["auth"]))
    );
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&2)
        .unwrap()
        .member = Some(member("kv-1", pb::member::Status::Draining));
    // The newest session for an identity wins; closing it leaves a LEFT marker over the stale one.
    hub.remove(2);
    let key = ("kv".to_string(), "kv-1".to_string());
    assert_eq!(hub.members()[&key].status, pb::member::Status::Ready as i32);
    hub.remove(1);
    assert_eq!(hub.members()[&key].status, pb::member::Status::Left as i32);
    // An ungraceful close leaves nothing behind.
    hub.remove(3);
    assert!(
        !hub.members()
            .contains_key(&("kv".to_string(), "kv-2".to_string()))
    );
}

#[tokio::test]
async fn a_member_is_ready_for_a_range_only_after_acknowledging_its_push_and_catching_up_every_namespace()
 {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .member = Some(member("q1", pb::member::Status::Ready));
    let me = ("kv".to_string(), "q1".to_string());
    let whole = (0, 0);
    let site = (
        ("quicksilver".to_string(), "infrastructure".to_string()),
        Some(whole),
    );
    hub.observe_head(&site.0, 5);
    let ranges = |dataset: &str, revision| pb::Ranges {
        dataset: dataset.into(),
        revision,
        spans: vec![pb::Span { start: 0, end: 0 }],
    };
    let ack = |dataset: &str, revision| pb::RangesAck {
        dataset: dataset.into(),
        revision,
        refused: Vec::new(),
        reason: String::new(),
    };
    hub.push_ranges(&me, ranges("quicksilver", 7));
    hub.push_ranges(&me, ranges("quicksilver", 8));
    let pushed = tokio_stream::StreamExt::next(&mut frames)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(pushed.kind, Some(pb::relay_frame::Kind::Ranges(r)) if r.revision == 7));
    assert!(
        frames.rx.as_mut().try_recv().is_err(),
        "an unchanged set is not pushed again"
    );

    // Neither acknowledged nor followed yet.
    assert!(hub.ready().is_empty());
    hub.ranges_acked(1, ack("quicksilver", 7));
    assert!(hub.ready().is_empty(), "no stream for the range yet");
    let interest = |acked| Interest {
        from: acked + 1,
        acked,
        generation: 9,
        follow: 0,
        baseline: 0,
        confirming: false,
        bootstrap_acked: false,
        target: None,
        woken: Default::default(),
    };
    let set = |f: &dyn Fn(&mut Interest)| {
        f(hub
            .0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&1)
            .unwrap()
            .follows
            .get_mut(&site)
            .unwrap())
    };
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .follows
        .insert(site.clone(), interest(3));
    assert!(hub.ready().is_empty(), "behind the head");
    // The target is the head when the stream first could be ready; later writes do not move it.
    hub.observe_head(&site.0, 9);
    set(&|i| i.acked = 5);
    let expected = siblings::Ready::from([((me.clone(), "quicksilver".to_string()), vec![whole])]);
    assert_eq!(hub.ready(), expected);
    set(&|i| i.confirming = true);
    assert!(
        hub.ready().is_empty(),
        "a bootstrap awaiting confirmation is not ready"
    );
    set(&|i| i.confirming = false);
    assert_eq!(hub.ready(), expected);

    // Every fixed namespace counts, even one no write has reached the relay for yet.
    hub.push_ranges(&me, ranges("cdn", 3));
    hub.ranges_acked(1, ack("cdn", 3));
    let follow = |namespace: &str| {
        hub.0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&1)
            .unwrap()
            .follows
            .insert(
                (("cdn".to_string(), namespace.to_string()), Some(whole)),
                interest(0),
            );
    };
    follow("pages");
    assert_eq!(hub.ready(), expected, "`workers` is not followed yet");
    follow("workers");
    let mut both = expected.clone();
    both.insert((me, "cdn".to_string()), vec![whole]);
    assert_eq!(hub.ready(), both);
}

fn caller(service: &str, cell: &str, node_id: &str) -> Caller {
    Caller {
        service: service.into(),
        cell: cell.into(),
        node_id: node_id.into(),
        hosts: vec!["10.0.0.7".into()],
    }
}

#[test]
fn sessions_are_authorized_by_the_callers_certificate() {
    let member = pb::Member {
        node_id: "n7".into(),
        service: "kv".into(),
        cell: "c1".into(),
        address: "10.0.0.7:7000".into(),
        ..Default::default()
    };
    let denied = |result: Result<(), Status>| {
        assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied)
    };
    // A leaf of its own cell's relay, announcing itself.
    authorize(&caller("kv", "c1", "n7"), 1, "c1", 0, Some(&member)).unwrap();
    authorize(&caller("quicksilver", "c1", "q1"), 1, "c1", 0, None).unwrap();
    denied(authorize(
        &caller("kv", "c2", "n7"),
        1,
        "c1",
        0,
        Some(&member),
    ));
    // A relay reads published views as a leaf without a member, through relays of any tier and cell.
    authorize(&caller("relay", "c2", "r1"), 1, "c1", 0, None).unwrap();
    authorize(&caller("relay", "c1", "r1"), 2, "dc", 0, None).unwrap();
    denied(authorize(
        &caller("relay", "c1", "r1"),
        1,
        "c1",
        0,
        Some(&member),
    ));
    // The announced member must be the caller.
    denied(authorize(
        &caller("kv", "c1", "n8"),
        1,
        "c1",
        0,
        Some(&member),
    ));
    denied(authorize(
        &caller("cdn", "c1", "n7"),
        1,
        "c1",
        0,
        Some(&member),
    ));
    let elsewhere = pb::Member {
        address: "10.0.0.8:7000".into(),
        ..member.clone()
    };
    denied(authorize(
        &caller("kv", "c1", "n7"),
        1,
        "c1",
        0,
        Some(&elsewhere),
    ));
    // Relays of any cell reach upper tiers and the writer; services never do.
    authorize(&caller("relay", "c2", "r2"), 2, "dc", 1, None).unwrap();
    authorize(&caller("relay", "c2", "r2"), WRITER, "", 3, None).unwrap();
    denied(authorize(&caller("kv", "c1", "n7"), 2, "dc", 1, None));
    denied(authorize(&caller("kv", "c1", "n7"), WRITER, "", 0, None));
    // A relay without a configured cell admits no leaves.
    denied(authorize(&caller("kv", "", "n7"), 1, "", 0, None));
}

#[tokio::test]
async fn a_refused_push_is_pushed_again_on_the_next_pass() {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .member = Some(member("q1", pb::member::Status::Ready));
    let me = ("kv".to_string(), "q1".to_string());
    let ranges = |revision| pb::Ranges {
        dataset: "quicksilver".into(),
        revision,
        spans: vec![pb::Span {
            start: 0,
            end: 1 << 63,
        }],
    };
    let refused = |revision| pb::RangesAck {
        dataset: "quicksilver".into(),
        revision,
        refused: vec![pb::Span {
            start: 1 << 63,
            end: 0,
        }],
        reason: "still an owner".into(),
    };
    let mut next = async || match tokio_stream::StreamExt::next(&mut frames)
        .await
        .unwrap()
        .unwrap()
        .kind
    {
        Some(pb::relay_frame::Kind::Ranges(r)) => r.revision,
        other => panic!("expected a pushed set, got {other:?}"),
    };
    hub.push_ranges(&me, ranges(7));
    assert_eq!(next().await, 7);
    // A refusal of an older set does not forget the newer one.
    hub.ranges_acked(1, refused(6));
    hub.push_ranges(&me, ranges(8));
    // The current set is refused: the next pass pushes the same spans again.
    hub.ranges_acked(1, refused(7));
    hub.push_ranges(&me, ranges(9));
    let again = tokio::time::timeout(std::time::Duration::from_secs(1), next());
    assert_eq!(again.await.expect("the refused set is pushed again"), 9);
}

/// Found by the simulation (#25 / B-23): a stream started at a sibling's cursor skipped two revisions, and the relay kept
/// reporting its old head, so a subscriber arriving behind the cursor looked caught up and never fetched them.
#[test]
fn a_stream_started_past_this_relays_history_moves_its_head_to_the_cursor() {
    let hub = hub();
    let key = ("cdn".to_owned(), "workers".to_owned());
    hub.observe_head(&key, 124);
    assert_eq!(hub.resume_from(&key, 127), 127);
    assert_eq!(hub.source_head(&key), Some(126));

    let fresh = ("cdn".to_owned(), "pages".to_owned());
    assert_eq!(hub.resume_from(&fresh, 1), 1);
    assert_eq!(
        hub.source_head(&fresh),
        None,
        "a first follow proves nothing"
    );
}

/// A cell control group holding one published value in every membership namespace.
struct PublishedCell;

impl LocalControlSource for PublishedCell {
    fn validate_namespace(&self, cell: &str, namespace: &str) -> Result<(), Status> {
        let services = crate::membership::SERVICES;
        match services
            .iter()
            .any(|s| namespace == crate::membership::namespace(s, cell))
        {
            true => Ok(()),
            false => Err(Status::invalid_argument("outside this cell")),
        }
    }
    fn identity(&self) -> pb::ControlAck {
        Default::default()
    }
    fn validate_peer(&self, _: &pb::ControlAck) -> Result<(), Status> {
        Ok(())
    }
    fn changed(&self) -> Arc<Watched<u64>> {
        Arc::new(Watched::new(0))
    }
    fn head(&self, _: &str) -> Result<u64, Status> {
        Ok(3)
    }
    fn read(&self, _: &str, _: u64, _: usize) -> Result<Vec<Event>, Status> {
        Ok(Vec::new())
    }
    fn view(&self, namespace: &str) -> Result<crate::replication::materialized::View, Status> {
        Ok(crate::replication::materialized::View {
            dataset: "membership".into(),
            namespace: namespace.into(),
            sequence: 3,
            committed_at: 0,
            values: [(b"qs-1".to_vec(), namespace.as_bytes().to_vec())].into(),
            retries: Default::default(),
            pruned_through: 0,
            raft: None,
        })
    }
}

#[tokio::test]
async fn routes_return_a_services_published_namespace_to_callers_in_the_cell() {
    use pb::relay_server::Relay as _;
    let ask = |service: &str, from: Option<Caller>| {
        let mut request = Request::new(pb::RoutesRequest {
            service: service.into(),
        });
        if let Some(from) = from {
            request.extensions_mut().insert(from);
        }
        request
    };
    // Only a cell relay with a control group answers.
    let error = hub().routes(ask("quicksilver", None)).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    let hub = hub()
        .with_cell("cell".into())
        .with_control(Arc::new(PublishedCell));
    let reply = hub
        .routes(ask("quicksilver", Some(caller("kv", "cell", "kv-1"))))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.revision, 3);
    assert_eq!(
        reply.entries,
        [pb::StateEntry {
            key: b"qs-1".to_vec(),
            value: b"quicksilver.cell".to_vec(),
        }]
    );
    let foreign = hub.routes(ask("quicksilver", Some(caller("kv", "other", "kv-9"))));
    assert_eq!(
        foreign.await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let unknown = hub.routes(ask("nope", Some(caller("kv", "cell", "kv-1"))));
    assert_eq!(
        unknown.await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}
