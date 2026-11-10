//! Real relay protocol coverage shared by native and deterministic runtimes.
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::sim::MemoryFactory;
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
            expected: None,
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
            .add_service(pb::relay_server::RelayServer::new(hub.clone()))
            .add_service(pb::siblings_server::SiblingsServer::new(
                bedrock_lib::relays::siblings::Server(hub),
            ))
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
            .add_service(pb::relay_server::RelayServer::new(hub.clone()))
            .add_service(pb::siblings_server::SiblingsServer::new(
                bedrock_lib::relays::siblings::Server(hub),
            ))
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
        ("quicksilver", "infrastructure", b"q/".as_slice()),
        ("auth", "keys", b"a/".as_slice()),
    ]
    .into_iter()
    .map(|(dataset, namespace, prefix)| Target {
        selection: Selection {
            dataset: dataset.into(),
            namespace: namespace.into(),
        },
        engine: engine.clone(),
        prefix: prefix.to_vec(),
        span: None,
    })
    .collect()
}

async fn caught_up(session: &Session, revision: u64) {
    for (dataset, namespace) in [("quicksilver", "infrastructure"), ("auth", "keys")] {
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
            "infrastructure",
            Operation::Set(b"configuration".to_vec())
        )
        .await,
        1
    );
    assert_eq!(
        commit(
            &writer,
            "auth",
            "keys",
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
        commit(&writer, "quicksilver", "infrastructure", Operation::Delete).await,
        2
    );
    assert_eq!(
        commit(&writer, "auth", "keys", Operation::Set(b"rotated".to_vec())).await,
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
    hello(address, tier, false).await
}

async fn hello(
    address: &str,
    tier: u32,
    moved: bool,
) -> (
    mpsc::Sender<pb::RelayRequest>,
    tonic::Streaming<pb::RelayFrame>,
) {
    let (tx, rx) = mpsc::channel(16);
    tx.send(request(pb::relay_request::Kind::Hello(pb::Hello {
        version: 1,
        tier,
        member: None,
        moved,
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
        dataset: "cdn".into(),
        namespace: namespace.into(),
        from,
        span: None,
        from_namespaces: Default::default(),
        ..Default::default()
    }))
}

#[tokio::test]
async fn namespace_unfollow_preserves_the_other_namespace_on_the_same_session() {
    let writer = writer().await;
    let server = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17201).await;
    for namespace in ["pages", "workers"] {
        commit(&writer, "cdn", namespace, Operation::Set(b"first".to_vec())).await;
    }
    let (tx, mut stream) = raw(&server.address, 0).await;
    for namespace in ["pages", "workers"] {
        tx.send(follow(namespace, 1)).await.unwrap();
    }
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < 2 {
        if let pb::relay_frame::Kind::Delivery(delivery) = frame(&mut stream).await {
            seen.insert(delivery.namespace.clone());
            tx.send(request(pb::relay_request::Kind::Ack(pb::Ack {
                dataset: "cdn".into(),
                namespace: delivery.namespace,
                applied: delivery.through,
                span: None,
            })))
            .await
            .unwrap();
        }
    }
    tx.send(request(pb::relay_request::Kind::Unfollow(pb::Unfollow {
        dataset: "cdn".into(),
        namespace: "pages".into(),
        span: None,
    })))
    .await
    .unwrap();
    // This later follow's range is a protocol barrier: the earlier unfollow was processed.
    tx.send(follow("workers", 2)).await.unwrap();
    loop {
        if let pb::relay_frame::Kind::Range(range) = frame(&mut stream).await
            && range.namespace == "workers"
        {
            break;
        }
    }
    commit(
        &writer,
        "cdn",
        "pages",
        Operation::Set(b"unfollowed".to_vec()),
    )
    .await;
    commit(
        &writer,
        "cdn",
        "workers",
        Operation::Set(b"still followed".to_vec()),
    )
    .await;
    loop {
        if let pb::relay_frame::Kind::Delivery(delivery) = frame(&mut stream).await {
            assert_ne!(delivery.namespace, "pages");
            if delivery.namespace == "workers" && delivery.through == 2 {
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
    let hub = Hub::writer("writer".into(), writer.clone(), peers());
    let server = serve(hub.clone(), 17301).await;
    for tier in [4, 5] {
        let (tx, rx) = mpsc::channel(1);
        tx.send(request(pb::relay_request::Kind::Hello(pb::Hello {
            version: 1,
            tier,
            member: None,
            moved: false,
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
    let customer = pb::Follow {
        dataset: "kv".into(),
        namespace: "customer".into(),
        from: 1,
        span: None,
        from_namespaces: Default::default(),
        ..Default::default()
    };
    for kind in [
        pb::relay_request::Kind::Follow(customer.clone()),
        pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(customer),
            visited: Vec::new(),
        }),
    ] {
        tx.send(request(kind)).await.unwrap();
        assert!(
            matches!(frame(&mut stream).await, pb::relay_frame::Kind::Error(error) if error.code == tonic::Code::InvalidArgument as u32)
        );
    }
    assert_eq!(hub.requests("kv"), (0, 0), "a rejected dataset is no label");
    writer.close().await.unwrap();
}

#[tokio::test]
async fn recovery_leases_are_bounded_and_release_capacity_on_drop() {
    let writer = writer().await;
    commit(
        &writer,
        "auth",
        "keys",
        Operation::Set(b"baseline".to_vec()),
    )
    .await;
    let mut leases = Vec::new();
    for _ in 0..4 {
        let (view, lease) = writer.begin_recovery("auth", "keys").await.unwrap();
        assert_eq!(view.sequence, 1);
        leases.push(lease);
    }
    assert!(writer.begin_recovery("auth", "keys").await.is_err());
    leases.pop();
    let (_, lease) = writer.begin_recovery("auth", "keys").await.unwrap();
    assert!(
        writer
            .recovery_events("auth", "keys", 1, &lease)
            .await
            .is_err()
    );
    commit(&writer, "auth", "keys", Operation::Set(b"tail".to_vec())).await;
    let events = writer
        .recovery_events("auth", "keys", 2, &lease)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 2);
    assert_eq!(events[0].committed_at, 1_700_000_000_000);
    assert_eq!(events[0].operation, Operation::Set(b"tail".to_vec()));
    writer.close().await.unwrap();
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn recovery_lease_renews_on_progress_up_to_its_deadline() {
    let writer = writer().await;
    commit(&writer, "auth", "keys", Operation::Set(b"a".to_vec())).await;
    let (_, lease) = writer.begin_recovery("auth", "keys").await.unwrap();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_secs(50)).await;
        lease.renew();
    }
    // 150s exceeds the 60s base lease; progress kept it alive.
    commit(&writer, "auth", "keys", Operation::Set(b"b".to_vec())).await;
    assert_eq!(
        writer
            .recovery_events("auth", "keys", 2, &lease)
            .await
            .unwrap()
            .len(),
        1
    );
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(50)).await;
        lease.renew();
    }
    assert!(!lease.valid(), "renewal must stop at the absolute deadline");
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
    commit(&writer, "auth", "keys", Operation::Set(b"state".to_vec())).await;
    CLOCK.store(2000, Ordering::SeqCst);
    writer.cleanup("auth", "keys").await.unwrap();
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
                namespace: "keys".into(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
            span: None,
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "keys", 1))
        .await
        .unwrap_or_else(|_| panic!("bootstrap: {:?}", session.error()));
    assert_eq!(local.get(b"a/key").unwrap(), Some(b"state".to_vec()));
    commit(&writer, "auth", "keys", Operation::Delete).await;
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "keys", 2))
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
        "keys",
        Operation::Set(b"baseline".to_vec()),
    )
    .await;
    let (view, lease) = writer.begin_recovery("auth", "keys").await.unwrap();
    commit(&writer, "auth", "keys", Operation::Delete).await;
    CLOCK.store(3000, Ordering::SeqCst);
    writer.cleanup("auth", "keys").await.unwrap();
    let tail = writer
        .recovery_events("auth", "keys", view.sequence + 1, &lease)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].operation, Operation::Delete);
    assert!(
        writer
            .recovery_events("quicksilver", "keys", view.sequence + 1, &lease)
            .await
            .is_err()
    );
    drop(lease);
    writer.cleanup("auth", "keys").await.unwrap();
    let new_view = writer.bootstrap("auth", "keys").await.unwrap();
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
                namespace: "keys".into(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
            span: None,
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "keys", 0))
        .await
        .unwrap_or_else(|_| panic!("empty bootstrap was not confirmed: {:?}", session.error()));
    assert!(session.ready());
    assert_eq!(local.get(b"a/stale").unwrap(), None);
    upstream.abort();
    writer.close().await.unwrap();
}

#[derive(Default)]
struct AuthUnavailable(bedrock_lib::storage::sim::MemoryFactory);
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
        "infrastructure",
        Operation::Set(b"healthy".to_vec()),
    )
    .await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17601).await;
    let local = Arc::new(MemoryEngine::new("local"));
    local.put(b"a/key", b"requires recovery").unwrap();
    let session = Session::start(peers(), vec![source.address.clone()], targets(&local)).unwrap();
    tokio::time::timeout(
        DEADLINE,
        session.wait_for("quicksilver", "infrastructure", 1),
    )
    .await
    .unwrap();
    assert_eq!(local.get(b"q/key").unwrap(), Some(b"healthy".to_vec()));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            session.wait_for("auth", "keys", 0)
        )
        .await
        .is_err()
    );
    commit(
        &writer,
        "quicksilver",
        "infrastructure",
        Operation::Set(b"still healthy".to_vec()),
    )
    .await;
    tokio::time::timeout(
        DEADLINE,
        session.wait_for("quicksilver", "infrastructure", 2),
    )
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
                        namespace: "keys".into(),
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
    assert_eq!(reopened.ranges("auth", "keys"), vec![(1, 3)]);

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
                namespace: "keys".into(),
            },
            engine: local.clone(),
            prefix: b"a/".to_vec(),
            span: None,
        }],
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "keys", 3))
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
            commit(writer, "auth", "keys", Operation::Set(b"baseline".to_vec())).await,
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
            namespace: "keys".into(),
        },
        engine: local.clone(),
        prefix: b"a/".to_vec(),
        span: None,
    };
    let first =
        Session::start(peers(), vec![writer_server.address.clone()], vec![target()]).unwrap();
    tokio::time::timeout(DEADLINE, first.wait_for("auth", "keys", 1))
        .await
        .unwrap();
    drop(first);
    assert_eq!(
        commit(&authoritative, "auth", "keys", Operation::Delete).await,
        2
    );
    assert_eq!(
        commit(
            &authoritative,
            "auth",
            "keys",
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
        .read("auth", "keys", 3, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    cache.append(&third, third.committed_at).unwrap();
    assert_eq!(cache.ranges("auth", "keys"), vec![(3, 3)]);
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
    tokio::time::timeout(DEADLINE, resumed.wait_for("auth", "keys", 3))
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

async fn put(writer: &Arc<Writer>, namespace: &str, key: &[u8], operation: Operation) {
    writer
        .commit(Write {
            dataset: "cdn".into(),
            namespace: namespace.into(),
            key: key.to_vec(),
            operation,
            idempotency_key: None,
            expected: None,
        })
        .await
        .unwrap();
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn range_follow_streams_every_namespace_and_only_its_hashes_through_two_tiers() {
    use bedrock_lib::placement::hash_of;
    use bedrock_lib::storage::{KeyRange, l2_key};
    const HALF: u64 = 1 << 63;
    let writer = writer().await;
    let keys: Vec<Vec<u8>> = (0u8..16).map(|i| vec![b'k', i]).collect();
    // `workers` has no writes yet when the range is first followed.
    for key in &keys {
        put(&writer, "pages", key, Operation::Set(b"pages".to_vec())).await;
    }
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17501).await;
    let top = relay("top", 3, &source.address);
    let top_task = top.start();
    let top = serve(top, 17502).await;
    let cell = relay("cell", 1, &top.address);
    let cell_task = cell.start();
    let cell = serve(cell, 17503).await;
    let local = Arc::new(MemoryEngine::new("local"));
    let target = || Target {
        selection: Selection {
            dataset: "cdn".into(),
            namespace: String::new(),
        },
        engine: local.clone(),
        prefix: Vec::new(),
        span: Some((0, HALF)),
    };
    let stored = |namespace: &str, key: &[u8]| {
        local
            .get(&l2_key(hash_of("cdn", namespace, key), namespace, key))
            .unwrap()
    };
    let inside = |namespace: &str, key: &[u8]| hash_of("cdn", namespace, key) < HALF;
    let session = Session::start(peers(), vec![cell.address.clone()], vec![target()]).unwrap();
    let complete = |namespace: &'static str, value: &'static [u8]| {
        let keys = keys.clone();
        let stored = &stored;
        move || {
            keys.iter()
                .filter(|key| inside(namespace, key))
                .all(|key| stored(namespace, key).as_deref() == Some(value))
        }
    };
    until("namespace pages", complete("pages", b"pages")).await;
    assert!(keys.iter().filter(|key| inside("pages", key)).count() > 0);
    // Nothing outside the range arrives, in any namespace.
    for entry in local.scan(&KeyRange::prefix(*b"l2/")).unwrap() {
        let (key, _) = entry.unwrap();
        let hash = u64::from_be_bytes(key[3..11].try_into().unwrap());
        assert!(hash < HALF, "key outside the followed range");
    }
    // A fixed namespace first written after the follow streams too.
    for key in &keys {
        put(&writer, "workers", key, Operation::Set(b"workers".to_vec())).await;
    }
    until("namespace workers", complete("workers", b"workers")).await;
    drop(session);
    for key in &keys {
        put(&writer, "pages", key, Operation::Delete).await;
    }
    let resumed = Session::start(peers(), vec![cell.address.clone()], vec![target()]).unwrap();
    until("deletes after restart", || {
        keys.iter().all(|key| stored("pages", key).is_none()) && complete("workers", b"workers")()
    })
    .await;
    drop(resumed);
    top_task.abort();
    cell_task.abort();
    writer.close().await.unwrap();
}

async fn admin_get(admin: &Arc<Admin>, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(admin.clone().serve(listener));
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    task.abort();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response.split_once("\r\n\r\n").unwrap().1.to_owned()
}

/// The series of `name` whose labels include every one of `labels`.
fn sample(metrics: &str, name: &str, labels: &[&str]) -> Option<String> {
    metrics
        .lines()
        .find(|line| {
            line.starts_with(&format!("{name}{{")) && labels.iter().all(|l| line.contains(l))
        })
        .map(|line| line.rsplit(' ').next().unwrap().to_owned())
}

#[tokio::test]
async fn siblings_stay_warm_until_the_last_subscriber_leaves_and_a_moved_one_is_timed() {
    use bedrock_lib::relays::{introspection, siblings::Siblings};
    let grace = Duration::from_secs(20);
    let writer = writer().await;
    let source_hub = Hub::writer("writer".into(), writer.clone(), peers());
    let source = serve(source_hub.clone(), 18001).await;
    let (a, b) = (
        relay("a", 1, &source.address),
        relay("b", 1, &source.address),
    );
    let admin = Admin::new();
    introspection::register(&admin, &b, "cell", "c1", None);
    let (a_task, b_task) = (a.start(), b.start());
    let a_server = serve(a.clone(), 18002).await;
    let b_server = serve(b.clone(), 18003).await;
    let (_, a_watch) = Siblings::start(a.clone(), peers(), vec![b_server.address.clone()], grace);
    let (_, b_watch) = Siblings::start(b.clone(), peers(), vec![a_server.address.clone()], grace);

    commit(&writer, "auth", "keys", Operation::Set(b"one".to_vec())).await;
    commit(
        &writer,
        "quicksilver",
        "infrastructure",
        Operation::Set(b"one".to_vec()),
    )
    .await;
    let local = Arc::new(MemoryEngine::new("consumer"));
    let session = Session::start(peers(), vec![a_server.address.clone()], targets(&local)).unwrap();
    caught_up(&session, 1).await;
    until("the idle sibling to follow", || {
        source_hub.requests("auth").0 == 2 && source_hub.requests("quicksilver").0 == 2
    })
    .await;
    assert!(b.interests().is_empty(), "what it learned is not its own");
    let view = admin_get(&admin, "/introspect/siblings?dataset=auth").await;
    let view: serde_json::Value = serde_json::from_str(&view).unwrap();
    assert_eq!(view["items"][0]["state"], "followed", "{view}");
    assert_eq!(view["streams"][&a_server.address]["connected"], true);
    let metrics = admin_get(&admin, "/metrics").await;
    let auth = ["dataset=\"auth\""];
    assert_eq!(
        sample(&metrics, "bedrock_relay_sibling_streams", &[]).as_deref(),
        Some("1.0")
    );
    assert_eq!(
        sample(&metrics, "bedrock_relay_sibling_interests", &auth).as_deref(),
        Some("1.0")
    );

    // A subscriber arriving from another relay finds the sibling warm.
    let upstream = source_hub.requests("auth");
    commit(&writer, "auth", "keys", Operation::Set(b"two".to_vec())).await;
    let (tx, mut stream) = hello(&b_server.address, 0, true).await;
    tx.send(request(pb::relay_request::Kind::Follow(pb::Follow {
        dataset: "auth".into(),
        namespace: "keys".into(),
        from: 2,
        span: None,
        from_namespaces: Default::default(),
        ..Default::default()
    })))
    .await
    .unwrap();
    let through = loop {
        if let pb::relay_frame::Kind::Delivery(delivery) = frame(&mut stream).await {
            break delivery.through;
        }
    };
    assert_eq!(through, 2);
    tx.send(request(pb::relay_request::Kind::Ack(pb::Ack {
        dataset: "auth".into(),
        namespace: "keys".into(),
        applied: through,
        span: None,
    })))
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let metrics = admin_get(&admin, "/metrics").await;
            let count = "bedrock_relay_failover_catch_up_seconds_count";
            if sample(&metrics, count, &["dataset=\"auth\""]).as_deref() == Some("1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the moved subscriber's catch-up is measured");
    assert_eq!(source_hub.requests("auth"), upstream);
    assert_eq!(b.requests("auth"), (1, 0));
    assert!(!b.interests().is_empty());

    // With both subscribers gone, neither sibling keeps the other's interest alive.
    drop((tx, stream));
    session.stop().await;
    let idle = |hub: &Hub| {
        let (namespaces, datasets) = hub.desired();
        namespaces.is_empty() && datasets.is_empty()
    };
    until("both siblings to drop the follows", || idle(&a) && idle(&b)).await;
    for task in [a_task, b_task, a_watch, b_watch] {
        task.abort();
    }
    writer.close().await.unwrap();
}

#[tokio::test]
async fn introspection_matches_the_leaf_and_lag_is_reported_at_every_tier() {
    use bedrock_lib::relays::introspection;
    let writer = writer().await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 17901).await;
    let clock = || 1_700_000_000_250;
    let top = relay("top", 3, &source.address).with_clock(clock);
    let top_admin = Admin::new();
    introspection::register(&top_admin, &top, "region", "c1", None);
    let top_task = top.start();
    let top_server = serve(top, 17902).await;
    let cell = relay("cell", 1, &top_server.address).with_clock(clock);
    let cell_admin = Admin::new();
    introspection::register(&cell_admin, &cell, "cell", "c1", None);
    let cell_task = cell.start();
    let cell_server = serve(cell, 17903).await;
    for value in [b"one".as_slice(), b"two"] {
        commit(
            &writer,
            "quicksilver",
            "infrastructure",
            Operation::Set(value.to_vec()),
        )
        .await;
        commit(&writer, "auth", "keys", Operation::Set(value.to_vec())).await;
    }
    let local = Arc::new(MemoryEngine::new("consumer"));
    let session =
        Session::start(peers(), vec![cell_server.address.clone()], targets(&local)).unwrap();
    caught_up(&session, 2).await;

    // Replay persistence is asynchronous; the view must settle on what the leaf applied.
    let view = tokio::time::timeout(DEADLINE, async {
        loop {
            let body = admin_get(
                &cell_admin,
                "/introspect/namespaces?dataset=quicksilver&prefix=inf",
            )
            .await;
            let view: serde_json::Value = serde_json::from_str(&body).unwrap();
            if view["items"][0]["replay"] == serde_json::json!([[1, 2]]) {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replay range reaches the applied revision");
    let item = &view["items"][0];
    assert_eq!(view["items"].as_array().unwrap().len(), 1, "{view}");
    assert_eq!(item["namespace"], "infrastructure");
    assert_eq!(item["subscribers"], 1);
    assert_eq!(
        item["latest"],
        session.applied("quicksilver", "infrastructure").unwrap()
    );
    assert_eq!(item["head"], 2);
    assert_eq!(item["lag_ms"], 250);

    let subscribers: serde_json::Value =
        serde_json::from_str(&admin_get(&top_admin, "/introspect/subscribers").await).unwrap();
    assert_eq!(
        subscribers["items"].as_array().unwrap().len(),
        1,
        "the cell relay"
    );
    let upstream: serde_json::Value =
        serde_json::from_str(&admin_get(&cell_admin, "/introspect/upstream?dataset=auth").await)
            .unwrap();
    assert_eq!(upstream["connected"], top_server.address);
    assert_eq!(upstream["items"][0]["cursor"], 2);

    for (admin, tier) in [(&top_admin, "region"), (&cell_admin, "cell")] {
        let metrics = admin_get(admin, "/metrics").await;
        let labels = [
            &format!("tier=\"{tier}\"") as &str,
            "dataset=\"quicksilver\"",
        ];
        assert_eq!(
            sample(&metrics, "bedrock_relay_lag_seconds_count", &labels).as_deref(),
            Some("2"),
            "{metrics}"
        );
        assert_eq!(
            sample(&metrics, "bedrock_relay_lag_seconds_sum", &labels).as_deref(),
            Some("0.5"),
            "{metrics}"
        );
        assert_eq!(
            sample(&metrics, "bedrock_relay_events_in_total", &labels).as_deref(),
            Some("2"),
            "{metrics}"
        );
        assert_eq!(
            sample(&metrics, "bedrock_relay_subscribers", &["service=\"none\""]).as_deref(),
            Some("1.0"),
            "{metrics}"
        );
    }
    drop(session);
    top_task.abort();
    cell_task.abort();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn quarantined_dataset_rebuilds_from_a_verified_bootstrap_while_the_other_keeps_applying() {
    use bedrock_lib::health::storage::{State, StorageHealth};
    use bedrock_lib::storage::{ErrorContext, StorageError};
    use std::collections::BTreeMap;
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
        "quicksilver",
        "infrastructure",
        Operation::Set(b"configuration".to_vec()),
    )
    .await;
    commit(
        &writer,
        "auth",
        "keys",
        Operation::Set(b"credential".to_vec()),
    )
    .await;
    let source = serve(Hub::writer("writer".into(), writer.clone(), peers()), 18001).await;
    let cell = relay("cell", 1, &source.address);
    let cell_task = cell.start();
    let cell = serve(cell, 18002).await;

    let admin = Admin::new();
    let health = StorageHealth::new(&admin);
    let raw: BTreeMap<&'static str, Arc<MemoryEngine>> = BTreeMap::from([
        ("quicksilver", Arc::new(MemoryEngine::new("quicksilver"))),
        ("auth", Arc::new(MemoryEngine::new("auth"))),
    ]);
    let replication: BTreeMap<&'static str, Arc<dyn StorageEngine>> = raw
        .iter()
        .map(|(dataset, engine)| {
            (
                *dataset,
                health.wrap_replication(dataset, "engine", engine.clone()),
            )
        })
        .collect();
    let mut targets = targets(&raw["auth"]);
    for target in &mut targets {
        target.engine = replication[target.selection.dataset.as_str()].clone();
    }
    let session = Session::start(peers(), vec![cell.address.clone()], targets).unwrap();
    caught_up(&session, 1).await;

    // While auth's storage is damaged its key is deleted and the history that carried the delete expires.
    commit(&writer, "auth", "keys", Operation::Delete).await;
    CLOCK.store(2000, Ordering::SeqCst);
    writer.cleanup("auth", "keys").await.unwrap();
    health.report(
        "auth",
        "engine",
        &StorageError::Corruption(ErrorContext {
            engine: "test",
            instance: "auth".into(),
            file: None,
            partition: None,
            detail: "bad block".into(),
        }),
    );
    raw["auth"].put(b"a/ghost", b"leftover").unwrap();
    let rebuilding = bedrock_lib::reconciliation::rebuild::start(
        &admin,
        &health,
        &session,
        "engine",
        &replication,
        1,
    );
    commit(
        &writer,
        "quicksilver",
        "infrastructure",
        Operation::Set(b"rotated".to_vec()),
    )
    .await;
    tokio::time::timeout(
        DEADLINE,
        session.wait_for("quicksilver", "infrastructure", 2),
    )
    .await
    .expect("the healthy dataset keeps applying during the rebuild");
    tokio::time::timeout(DEADLINE, async {
        while health.get("auth", "engine").unwrap().state != State::Healthy {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("auth was not rebuilt: {:?}", session.error()));
    rebuilding.abort();

    let auth = health.wrap("auth", "engine", raw["auth"].clone());
    assert_eq!(
        auth.get(b"a/key").unwrap(),
        None,
        "the expired delete stays applied"
    );
    assert_eq!(
        auth.get(b"a/ghost").unwrap(),
        None,
        "the damaged instance was recreated"
    );
    session.wait_for("auth", "keys", 2).await;
    assert_eq!(
        raw["quicksilver"].get(b"q/key").unwrap(),
        Some(b"rotated".to_vec())
    );
    assert_eq!(
        health.get("quicksilver", "engine").unwrap().state,
        State::Healthy
    );
    drop(session);
    cell_task.abort();
    writer.close().await.unwrap();
}
