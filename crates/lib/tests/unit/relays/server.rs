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
            changed: Arc::new(Watched::new(0)),
        },
    );
    (
        Frames {
            rx: ReceiverStream::new(rx),
            bytes,
            events,
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
    assert!(!hub.send_scoped(1, &key, 1, pb::relay_frame::Kind::Tick(true)));
    assert!(hub.send_scoped(1, &key, 2, pb::relay_frame::Kind::Tick(true)));
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
