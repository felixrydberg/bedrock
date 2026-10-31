//! Real relay protocol coverage shared by native and deterministic runtimes.
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::relays::Hub;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::transport::admin::Admin;
use bedrock_lib::transport::{Class, Peers};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const DEADLINE: Duration = Duration::from_secs(15);

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}

async fn writer() -> Arc<Writer> {
    Writer::open(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap()
}

async fn commit(writer: &Arc<Writer>, dataset: &str, namespace: &str, operation: Operation) -> u64 {
    writer
        .commit(Write {
            dataset: dataset.into(),
            namespace: namespace.into(),
            key: b"key".to_vec(),
            operation,
            idempotency_key: None,
        })
        .await
        .unwrap()
}

struct Server {
    address: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(not(madsim))]
async fn serve(hub: Hub, _: u16) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Server { address, task }
}

#[cfg(madsim)]
async fn serve(hub: Hub, port: u16) -> Server {
    let address = format!("127.0.0.1:{port}");
    let socket = address.parse().unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve(socket)
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    Server { address, task }
}

fn relay(id: &str, tier: u32, upstream: &str) -> Hub {
    Hub::relay(
        id.into(),
        tier,
        peers(),
        vec![upstream.into()],
        vec![upstream.into()],
        Some(Arc::new(
            Replay::open(Arc::new(MemoryEngine::new(id)), ReplayConfig::default()).unwrap(),
        )),
    )
}

fn targets(engine: &Arc<MemoryEngine>) -> Vec<Target> {
    [
        ("quicksilver", "site", b"q/".as_slice()),
        ("auth", "accounts", b"a/".as_slice()),
    ]
    .into_iter()
    .map(|(dataset, namespace, prefix)| Target {
        selection: Selection {
            dataset: dataset.into(),
            namespace: namespace.into(),
            buckets: Vec::new(),
        },
        engine: engine.clone(),
        prefix: prefix.to_vec(),
    })
    .collect()
}

async fn caught_up(session: &Session, revision: u64) {
    for (dataset, namespace) in [("quicksilver", "site"), ("auth", "accounts")] {
        tokio::time::timeout(DEADLINE, session.wait_for(dataset, namespace, revision))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{dataset}/{namespace} failed to catch up: {:?}",
                    session.error()
                )
            });
    }
}

#[tokio::test]
async fn two_scopes_cross_writer_top_cell_and_resume_durable_local_state() {
    let writer = writer().await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17101).await;
    let top = relay("top", 3, &source.address);
    let top_task = top.start();
    let top_server = serve(top, 17102).await;
    let cell = relay("cell", 1, &top_server.address);
    let cell_task = cell.start();
    let cell_server = serve(cell, 17103).await;
    assert_eq!(
        commit(
            &writer,
            "quicksilver",
            "site",
            Operation::Set(b"configuration".to_vec())
        )
        .await,
        1
    );
    assert_eq!(
        commit(
            &writer,
            "auth",
            "accounts",
            Operation::Set(b"credential".to_vec())
        )
        .await,
        1
    );
    let local = Arc::new(MemoryEngine::new("consumer"));
    let session =
        Session::start(peers(), vec![cell_server.address.clone()], targets(&local)).unwrap();
    caught_up(&session, 1).await;
    assert_eq!(
        local.get(b"q/key").unwrap(),
        Some(b"configuration".to_vec())
    );
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"credential".to_vec()));
    drop(session);
    assert_eq!(
        commit(&writer, "quicksilver", "site", Operation::Delete).await,
        2
    );
    assert_eq!(
        commit(
            &writer,
            "auth",
            "accounts",
            Operation::Set(b"rotated".to_vec())
        )
        .await,
        2
    );
    let resumed =
        Session::start(peers(), vec![cell_server.address.clone()], targets(&local)).unwrap();
    caught_up(&resumed, 2).await;
    assert_eq!(local.get(b"q/key").unwrap(), None);
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"rotated".to_vec()));
    top_task.abort();
    cell_task.abort();
    writer.close().await.unwrap();
}

fn request(kind: pb::relay_request::Kind) -> pb::RelayRequest {
    pb::RelayRequest { kind: Some(kind) }
}

async fn raw(
    address: &str,
    tier: u32,
) -> (
    mpsc::Sender<pb::RelayRequest>,
    tonic::Streaming<pb::RelayFrame>,
) {
    let (tx, rx) = mpsc::channel(16);
    tx.send(request(pb::relay_request::Kind::Hello(pb::Hello {
        version: 1,
        tier,
    })))
    .await
    .unwrap();
    let channel = peers().channel(address, Class::Control).unwrap();
    let response = pb::relay_client::RelayClient::new(channel)
        .session(ReceiverStream::new(rx))
        .await
        .unwrap();
    let mut stream = response.into_inner();
    assert!(matches!(
        frame(&mut stream).await,
        pb::relay_frame::Kind::Welcome(_)
    ));
    (tx, stream)
}

async fn frame(stream: &mut tonic::Streaming<pb::RelayFrame>) -> pb::relay_frame::Kind {
    tokio::time::timeout(DEADLINE, stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .kind
        .unwrap()
}

fn follow(namespace: &str, from: u64) -> pb::RelayRequest {
    request(pb::relay_request::Kind::Follow(pb::Follow {
        dataset: "auth".into(),
        namespace: namespace.into(),
        from,
        buckets: Vec::new(),
    }))
}

#[tokio::test]
async fn namespace_unfollow_preserves_the_other_namespace_on_the_same_session() {
    let writer = writer().await;
    let server = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17201).await;
    for namespace in ["one", "two"] {
        commit(
            &writer,
            "auth",
            namespace,
            Operation::Set(b"first".to_vec()),
        )
        .await;
    }
    let (tx, mut stream) = raw(&server.address, 0).await;
    for namespace in ["one", "two"] {
        tx.send(follow(namespace, 1)).await.unwrap();
    }
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < 2 {
        if let pb::relay_frame::Kind::Delivery(delivery) = frame(&mut stream).await {
            seen.insert(delivery.namespace.clone());
            tx.send(request(pb::relay_request::Kind::Ack(pb::Ack {
                dataset: "auth".into(),
                namespace: delivery.namespace,
                applied: delivery.through,
            })))
            .await
            .unwrap();
        }
    }
    tx.send(request(pb::relay_request::Kind::Unfollow(pb::Unfollow {
        dataset: "auth".into(),
        namespace: "one".into(),
    })))
    .await
    .unwrap();
    // This later follow's range is a protocol barrier: the earlier unfollow was processed.
    tx.send(follow("two", 2)).await.unwrap();
    loop {
        if let pb::relay_frame::Kind::Range(range) = frame(&mut stream).await
            && range.namespace == "two"
        {
            break;
        }
    }
    commit(
        &writer,
        "auth",
        "one",
        Operation::Set(b"unfollowed".to_vec()),
    )
    .await;
    commit(
        &writer,
        "auth",
        "two",
        Operation::Set(b"still followed".to_vec()),
    )
    .await;
    loop {
        if let pb::relay_frame::Kind::Delivery(delivery) = frame(&mut stream).await {
            assert_ne!(delivery.namespace, "one");
            if delivery.namespace == "two" && delivery.through == 2 {
                break;
            }
        }
    }
    drop(tx);
    writer.close().await.unwrap();
}

#[tokio::test]
async fn invalid_tier_and_customer_kv_subscriptions_are_rejected() {
    let writer = writer().await;
    let server = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17301).await;
    for tier in [4, 5] {
        let (tx, rx) = mpsc::channel(1);
        tx.send(request(pb::relay_request::Kind::Hello(pb::Hello {
            version: 1,
            tier,
        })))
        .await
        .unwrap();
        let channel = peers().channel(&server.address, Class::Control).unwrap();
        let result = pb::relay_client::RelayClient::new(channel)
            .session(ReceiverStream::new(rx))
            .await;
        assert!(matches!(result, Err(error) if error.code() == tonic::Code::FailedPrecondition));
    }
    let (tx, mut stream) = raw(&server.address, 0).await;
    tx.send(request(pb::relay_request::Kind::Follow(pb::Follow {
        dataset: "kv".into(),
        namespace: "customer".into(),
        from: 1,
        buckets: Vec::new(),
    })))
    .await
    .unwrap();
    assert!(
        matches!(frame(&mut stream).await, pb::relay_frame::Kind::Error(error) if error.code == tonic::Code::InvalidArgument as u32)
    );
    writer.close().await.unwrap();
}

#[tokio::test]
async fn recovery_leases_are_bounded_and_release_capacity_on_drop() {
    let writer = writer().await;
    commit(
        &writer,
        "auth",
        "accounts",
        Operation::Set(b"baseline".to_vec()),
    )
    .await;
    let mut leases = Vec::new();
    for _ in 0..4 {
        let (view, lease) = writer.begin_recovery("auth", "accounts").await.unwrap();
        assert_eq!(view.sequence, 1);
        leases.push(lease);
    }
    assert!(writer.begin_recovery("auth", "accounts").await.is_err());
    leases.pop();
    let (_, lease) = writer.begin_recovery("auth", "accounts").await.unwrap();
    assert!(
        writer
            .recovery_events("auth", "accounts", 1, &lease)
            .await
            .is_err()
    );
    commit(
        &writer,
        "auth",
        "accounts",
        Operation::Set(b"tail".to_vec()),
    )
    .await;
    let events = writer
        .recovery_events("auth", "accounts", 2, &lease)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 2);
    assert_eq!(events[0].committed_at, 1_700_000_000_000);
    assert_eq!(events[0].operation, Operation::Set(b"tail".to_vec()));
    writer.close().await.unwrap();
}

#[tokio::test]
async fn pruned_authoritative_cursor_bootstraps_through_two_tiers_then_applies_delete() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CLOCK: AtomicU64 = AtomicU64::new(1000);
    let writer = Writer::open(
        || CLOCK.load(Ordering::SeqCst),
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention {
                event_retention_ms: 1,
                retry_retention_ms: 1000,
            },
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    commit(
        &writer,
        "auth",
        "accounts",
        Operation::Set(b"state".to_vec()),
    )
    .await;
    CLOCK.store(2000, Ordering::SeqCst);
    writer.cleanup("auth", "accounts").await.unwrap();
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17401).await;
    let top = relay("top", 3, &source.address);
    let top_task = top.start();
    let top = serve(top, 17402).await;
    let cell = relay("cell", 1, &top.address);
    let cell_task = cell.start();
    let cell = serve(cell, 17403).await;
    let local = Arc::new(MemoryEngine::new("local"));
    let session = Session::start(
        peers(),
        vec![cell.address.clone()],
        vec![Target {
            selection: Selection {
                dataset: "auth".into(),
                namespace: "accounts".into(),
                buckets: Vec::new(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "accounts", 1))
        .await
        .unwrap_or_else(|_| panic!("bootstrap: {:?}", session.error()));
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"state".to_vec()));
    commit(&writer, "auth", "accounts", Operation::Delete).await;
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "accounts", 2))
        .await
        .unwrap_or_else(|_| panic!("tail: {:?}", session.error()));
    assert_eq!(local.get(b"a/key").unwrap(), None);
    top_task.abort();
    cell_task.abort();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn gc_protects_the_bootstrap_tail_until_lease_is_released() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CLOCK: AtomicU64 = AtomicU64::new(1000);
    let writer = Writer::open(
        || CLOCK.load(Ordering::SeqCst),
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention {
                event_retention_ms: 1,
                retry_retention_ms: 1000,
            },
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    commit(
        &writer,
        "auth",
        "accounts",
        Operation::Set(b"baseline".to_vec()),
    )
    .await;
    let (view, lease) = writer.begin_recovery("auth", "accounts").await.unwrap();
    commit(&writer, "auth", "accounts", Operation::Delete).await;
    CLOCK.store(3000, Ordering::SeqCst);
    writer.cleanup("auth", "accounts").await.unwrap();
    let tail = writer
        .recovery_events("auth", "accounts", view.sequence + 1, &lease)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].operation, Operation::Delete);
    assert!(
        writer
            .recovery_events("quicksilver", "accounts", view.sequence + 1, &lease)
            .await
            .is_err()
    );
    drop(lease);
    writer.cleanup("auth", "accounts").await.unwrap();
    let (new_view, _) = writer.begin_recovery("auth", "accounts").await.unwrap();
    assert_eq!(new_view.pruned_through, 2);
    assert!(new_view.values.is_empty());
    writer.close().await.unwrap();
}

#[tokio::test]
async fn empty_authoritative_bootstrap_is_confirmed_through_relays_before_readiness() {
    let writer = writer().await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17501).await;
    let cell = relay("cell", 1, &source.address);
    let upstream = cell.start();
    let cell = serve(cell, 17502).await;
    let local = Arc::new(MemoryEngine::new("local"));
    local.put(b"a/stale", b"old").unwrap();
    let session = Session::start(
        peers(),
        vec![cell.address.clone()],
        vec![Target {
            selection: Selection {
                dataset: "auth".into(),
                namespace: "accounts".into(),
                buckets: Vec::new(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "accounts", 0))
        .await
        .unwrap_or_else(|_| panic!("empty bootstrap was not confirmed: {:?}", session.error()));
    assert!(session.ready());
    assert_eq!(local.get(b"a/stale").unwrap(), None);
    upstream.abort();
    writer.close().await.unwrap();
}

#[derive(Default)]
struct AuthUnavailable(bedrock_lib::storage::namespace::memory::MemoryFactory);
#[async_trait::async_trait]
impl bedrock_lib::storage::namespace::NamespaceFactory for AuthUnavailable {
    async fn namespaces(&self) -> bedrock_lib::storage::StorageResult<Vec<(String, String)>> {
        bedrock_lib::storage::namespace::NamespaceFactory::namespaces(&self.0).await
    }
    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> bedrock_lib::storage::StorageResult<
        Arc<dyn bedrock_lib::storage::namespace::NamespaceDatabase>,
    > {
        if dataset == "auth" {
            return Err(bedrock_lib::storage::StorageError::Unavailable(
                bedrock_lib::storage::ErrorContext {
                    engine: "test",
                    instance: "auth".into(),
                    file: None,
                    partition: None,
                    detail: "injected namespace failure".into(),
                },
            ));
        }
        bedrock_lib::storage::namespace::NamespaceFactory::open(&self.0, dataset, namespace).await
    }
}

#[tokio::test]
async fn failed_auth_recovery_does_not_starve_quicksilver_on_the_same_connection() {
    let writer = Writer::open(
        || 1000,
        Persistence {
            factory: Arc::new(AuthUnavailable::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    commit(
        &writer,
        "quicksilver",
        "site",
        Operation::Set(b"healthy".to_vec()),
    )
    .await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17601).await;
    let local = Arc::new(MemoryEngine::new("local"));
    local.put(b"a/key", b"requires recovery").unwrap();
    let session = Session::start(peers(), vec![source.address.clone()], targets(&local)).unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("quicksilver", "site", 1))
        .await
        .unwrap();
    assert_eq!(local.get(b"q/key").unwrap(), Some(b"healthy".to_vec()));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            session.wait_for("auth", "accounts", 0)
        )
        .await
        .is_err()
    );
    commit(
        &writer,
        "quicksilver",
        "site",
        Operation::Set(b"still healthy".to_vec()),
    )
    .await;
    tokio::time::timeout(DEADLINE, session.wait_for("quicksilver", "site", 2))
        .await
        .unwrap();
    assert_eq!(
        local.get(b"q/key").unwrap(),
        Some(b"still healthy".to_vec())
    );
    assert!(!session.ready());
    writer.close().await.unwrap();
}

#[tokio::test]
async fn restarted_relay_serves_validated_persisted_replay_while_parent_is_unavailable() {
    let persisted = Arc::new(MemoryEngine::new("replay"));
    {
        let cache = Replay::open(persisted.clone(), ReplayConfig::default()).unwrap();
        for (sequence, key, operation) in [
            (1, b"obsolete".as_slice(), Operation::Set(b"old".to_vec())),
            (2, b"obsolete".as_slice(), Operation::Delete),
            (3, b"key".as_slice(), Operation::Set(b"restored".to_vec())),
        ] {
            cache
                .append(
                    &bedrock_lib::replication::Event {
                        dataset: "auth".into(),
                        namespace: "accounts".into(),
                        stream: "auth-0".into(),
                        sequence,
                        epoch: 1,
                        committed_at: 1_700_000_000_000,
                        key: key.to_vec(),
                        operation,
                    },
                    1_700_000_000_000,
                )
                .unwrap();
        }
    }
    let reopened = Arc::new(Replay::open(persisted, ReplayConfig::default()).unwrap());
    assert_eq!(reopened.ranges("auth", "accounts"), vec![(1, 3)]);

    // Reserve a native port without serving gRPC, so the unavailable parent cannot accidentally be another service.
    #[cfg(not(madsim))]
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    #[cfg(not(madsim))]
    let parent = unavailable.local_addr().unwrap().to_string();
    #[cfg(madsim)]
    let parent = "127.0.0.1:17999".to_owned();

    let relay = Hub::relay(
        "restarted-cell".into(),
        1,
        peers(),
        vec![parent],
        Vec::new(),
        Some(reopened),
    );
    let upstream = relay.start();
    let server = serve(relay, 17701).await;
    let local = Arc::new(MemoryEngine::new("local"));
    let session = Session::start(
        peers(),
        vec![server.address.clone()],
        vec![Target {
            selection: Selection {
                dataset: "auth".into(),
                namespace: "accounts".into(),
                buckets: Vec::new(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "accounts", 3))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "persisted replay did not serve catch-up: {:?}",
                session.error()
            )
        });
    assert!(session.ready());
    assert_eq!(local.get(b"a/obsolete").unwrap(), None);
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"restored".to_vec()));
    drop(session);
    upstream.abort();
}

#[tokio::test]
async fn stale_recovery_candidate_falls_back_to_the_writer_with_required_history() {
    use bedrock_lib::replication::log::Log;

    let authoritative = writer().await;
    let stale = writer().await;
    for writer in [&authoritative, &stale] {
        assert_eq!(
            commit(
                writer,
                "auth",
                "accounts",
                Operation::Set(b"baseline".to_vec())
            )
            .await,
            1
        );
    }
    let writer_server = serve(
        Hub::writer("writer".into(), authoritative.clone(), peers()),
        17801,
    )
    .await;
    let stale_server = serve(
        Hub::writer("stale-writer".into(), stale.clone(), peers()),
        17802,
    )
    .await;
    let local = Arc::new(MemoryEngine::new("consumer"));
    let target = || Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: "accounts".into(),
            buckets: Vec::new(),
        },
        engine: local.clone(),
        prefix: b"a/".to_vec(),
    };
    let first =
        Session::start(peers(), vec![writer_server.address.clone()], vec![target()]).unwrap();
    tokio::time::timeout(DEADLINE, first.wait_for("auth", "accounts", 1))
        .await
        .unwrap();
    drop(first);
    assert_eq!(
        commit(&authoritative, "auth", "accounts", Operation::Delete).await,
        2
    );
    assert_eq!(
        commit(
            &authoritative,
            "auth",
            "accounts",
            Operation::Set(b"latest".to_vec())
        )
        .await,
        3
    );

    // Persisted revision 3 establishes the known head but leaves the subscriber's missing revision 2 uncovered.
    let cache = Arc::new(
        Replay::open(
            Arc::new(MemoryEngine::new("replay")),
            ReplayConfig::default(),
        )
        .unwrap(),
    );
    let third = authoritative
        .log()
        .read("auth", "accounts", 3, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    cache.append(&third, third.committed_at).unwrap();
    assert_eq!(cache.ranges("auth", "accounts"), vec![(3, 3)]);
    let relay = Hub::relay(
        "cell".into(),
        1,
        peers(),
        vec![writer_server.address.clone()],
        vec![stale_server.address.clone()],
        Some(cache),
    );
    // Keep the aggregate live pump stopped so the missing event must traverse the temporary recovery chain.
    let cell = serve(relay, 17803).await;
    let resumed = Session::start(peers(), vec![cell.address.clone()], vec![target()]).unwrap();
    tokio::time::timeout(DEADLINE, resumed.wait_for("auth", "accounts", 3))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "stale candidate blocked recovery fallback: {:?}",
                resumed.error()
            )
        });
    assert!(resumed.ready());
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"latest".to_vec()));
    drop(resumed);
    authoritative.close().await.unwrap();
    stale.close().await.unwrap();
}
