use super::*;
use crate::transport::admin::Admin;

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
        namespace: "users".into(),
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
    let key = ("auth".into(), "users".into());
    assert_eq!(hub.resume_from(&key, 1), 3);
    hub.encoded(replay_event(3));
    assert_eq!(hub.resume_from(&key, 1), 4);
    hub.encoded(replay_event(4));
    assert_eq!(hub.resume_from(&key, 1), 7);
    assert_eq!(hub.resume_from(&key, 8), 8);
    assert_eq!(hub.source_head(&key), Some(6));
    hub.observe_head(&key, 2);
    assert_eq!(hub.source_head(&key), Some(6));
    hub.observe_head(&key, 7);
    assert_eq!(hub.source_head(&key), Some(7));
}

fn subscriber(hub: &Hub, id: u64, capacity: usize) -> (Frames, Arc<Watched<bool>>) {
    let (tx, rx) = mpsc::channel(capacity);
    let bytes = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(Watched::new(false));
    let changed = Arc::new(Watched::new(0));
    let follows = BTreeMap::from([(
        ("auth".into(), "users".into()),
        Interest {
            from: 1,
            acked: 0,
            generation: id,
            baseline: 0,
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
            changed: changed.clone(),
            member: None,
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
    assert!(hub.desired().is_empty());
}

#[tokio::test]
async fn byte_budget_disconnects_before_accepting_an_oversized_frame() {
    let hub = hub();
    let (_frames, closed) = subscriber(&hub, 1, MAX_EVENTS);
    assert!(!hub.send(
        1,
        pb::relay_frame::Kind::Delivery(pb::Delivery {
            dataset: "auth".into(),
            namespace: "users".into(),
            from: 1,
            through: 1,
            events: vec![Bytes::from(vec![0; MAX_BYTES])]
        })
    ));
    assert!(closed.get());
    assert!(hub.desired().is_empty());
}

#[tokio::test]
async fn bootstrap_boundary_does_not_consume_the_event_acknowledgement_window() {
    let hub = hub();
    let (_frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = ("auth".into(), "users".into());
    hub.advance(1, &key, 1, 100_000);
    hub.baseline(1, &key, 1, 100_000);
    assert_eq!(hub.outstanding(1, &key), 0);
    assert_eq!(hub.current(1, &key, 1), Some((100_001, 0)));
    hub.advance(1, &key, 1, 100_256);
    assert_eq!(hub.outstanding(1, &key), 256);
}

#[tokio::test]
async fn unsubscribe_wakes_a_full_window_without_dropping_other_interests() {
    let hub = hub();
    let (_frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = ("auth".into(), "users".into());
    hub.advance(1, &key, 1, MAX_EVENTS as u64);
    hub.0
        .subscribers
        .lock()
        .unwrap()
        .get_mut(&1)
        .unwrap()
        .follows
        .insert(
            ("auth".into(), "other".into()),
            Interest {
                from: 1,
                acked: 0,
                generation: 2,
                baseline: 0,
            },
        );
    let pumping = hub.clone();
    let task = tokio::spawn(async move {
        pumping
            .pump(
                1,
                pb::Follow {
                    dataset: "auth".into(),
                    namespace: "users".into(),
                    from: 1,
                    buckets: Vec::new(),
                },
                1,
                Vec::new(),
                false,
            )
            .await
    });
    tokio::task::yield_now().await;
    hub.unfollow(1, &key);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        hub.desired(),
        BTreeMap::from([(("auth".into(), "other".into()), 1)])
    );
}

#[tokio::test]
async fn replaced_generation_cannot_enqueue_into_the_new_subscription() {
    let hub = hub();
    let (mut frames, _) = subscriber(&hub, 1, MAX_EVENTS);
    let key = ("auth".into(), "users".into());
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
            namespace: "users".into(),
            stream: "auth-0".into(),
            sequence,
            epoch: 1,
            committed_at: sequence,
            key: sequence.to_be_bytes().to_vec(),
            operation: crate::replication::Operation::Set(vec![42; 8192]),
        });
    }
    assert!(hub.0.replay_bytes.load(Ordering::Relaxed) <= MAX_BYTES);
    assert!(replay.ranges("auth", "users").is_empty());
    let key = ("auth".into(), "users".into());
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
            factory: Arc::new(crate::storage::namespace::memory::MemoryFactory::default()),
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
                namespace: "users".into(),
                key: key.to_be_bytes().to_vec(),
                operation: crate::replication::Operation::Set(vec![42]),
                idempotency_key: None,
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
        let follow = pb::Follow {
            dataset: "auth".into(),
            namespace: "users".into(),
            from: 0,
            buckets: Vec::new(),
        };
        pumping.pump(1, follow, 1, Vec::new(), true).await
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
    let members = hub.members();
    assert_eq!(members.len(), 2);
    assert_eq!(
        members[&("kv".to_string(), "kv-2".to_string())].applied,
        BTreeMap::from([(("auth".to_string(), "users".to_string()), 0)])
    );
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
    // The newest session for an identity wins; closing it leaves a LEFT marker over the stale one.
    hub.remove(2);
    let key = ("kv".to_string(), "kv-1".to_string());
    assert_eq!(
        hub.members()[&key].member.status,
        pb::member::Status::Ready as i32
    );
    hub.remove(1);
    assert_eq!(
        hub.members()[&key].member.status,
        pb::member::Status::Left as i32
    );
    // An ungraceful close leaves nothing behind.
    hub.remove(3);
    assert!(
        !hub.members()
            .contains_key(&("kv".to_string(), "kv-2".to_string()))
    );
}
