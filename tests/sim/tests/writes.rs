//! B-09 acceptance over the real write path: concurrent HTTP writes, some retried with the same idempotency key, commit
//! once each with gap-free sequences per namespace and reach a stream subscriber in order. Runs over real sockets in
//! normal builds and simulated under `--cfg madsim`. Restarts and a second writer are covered next to the writer.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::replication::subscriptions::Subscription;
use bedrock_lib::replication::{Applier, ApplierMetrics, ApplyHook, Event, Operation};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::{StorageEngine, StorageResult, WriteBatch};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use http_body_util::{BodyExt, Full};
use hyper::StatusCode;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::time::timeout;

const NAMESPACES: [&str; 2] = ["a", "b"];
const WRITES: usize = 200;
const WRITE_TOKEN: &str = "test-writer-token";
const AUTHORIZATION: &str = "Bearer test-writer-token";

/// Records every event it applies, in order.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Event>>>);

impl ApplyHook for Recorder {
    fn apply(&mut self, _: &dyn StorageEngine, e: &Event, _: &mut WriteBatch) -> StorageResult<()> {
        self.0.lock().unwrap().push(e.clone());
        Ok(())
    }
}

/// One request on its own connection.
async fn send(
    api: SocketAddr,
    method: &str,
    path: &str,
    idempotency_key: &str,
    body: String,
    authorization: &[&str],
) -> (StatusCode, String) {
    let stream = TcpStream::connect(api).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let mut request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "api")
        .header("idempotency-key", idempotency_key);
    for value in authorization {
        request = request.header("authorization", *value);
    }
    let request = request.body(Full::new(Bytes::from(body))).unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED {
        assert_eq!(response.headers()["www-authenticate"], "Bearer");
    }
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

/// The client side; `api` serves writes and `replication` serves the committed log.
async fn scenario(api: SocketAddr, replication: String) {
    let peers = Arc::new(Peers::new(&Admin::new()));
    let applied = Recorder::default();
    let applier = Applier::new(
        "kv",
        Arc::new(MemoryEngine::new("kv")),
        applied.clone(),
        &ApplierMetrics::default(),
    );
    let namespaces = NAMESPACES.map(String::from).to_vec();
    let subscription = Subscription::start(peers, replication, applier, namespaces);

    for method in ["PUT", "DELETE"] {
        for authorization in [
            vec![],
            vec!["Bearer wrong-token"],
            vec!["Bearer test-writer-token-extra"],
            vec!["Basic test-writer-token"],
            vec!["Bearer"],
            vec!["Bearer "],
            vec![AUTHORIZATION, "Bearer wrong-token"],
            vec!["Bearer wrong-token", AUTHORIZATION],
        ] {
            let (status, _) = send(
                api,
                method,
                "/v1/kv/a/key/0",
                "w0",
                "unauthorized".into(),
                &authorization,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {authorization:?}"
            );
        }
    }

    let (status, _) = send(
        api,
        "PUT",
        "/v1/kv/a/key/0",
        "",
        String::new(),
        &[AUTHORIZATION],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let writes: Vec<_> = (0..WRITES)
        .map(|i| {
            tokio::spawn(async move {
                let namespace = NAMESPACES[i % 2];
                let (method, body) = if i % 7 == 0 {
                    ("DELETE", String::new())
                } else {
                    ("PUT", i.to_string())
                };
                let path = format!("/v1/kv/{namespace}/key/{}", i % 5);
                let (status, sequence) = send(
                    api,
                    method,
                    &path,
                    &format!("w{i}"),
                    body.clone(),
                    &[AUTHORIZATION],
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{sequence}");
                if i % 10 == 0 {
                    let retried = send(
                        api,
                        method,
                        &path,
                        &format!("w{i}"),
                        body,
                        &["bEaReR test-writer-token"],
                    )
                    .await;
                    assert_eq!(retried, (StatusCode::OK, sequence.clone()), "retry of w{i}");
                }
                (namespace, sequence.parse::<u64>().unwrap(), i)
            })
        })
        .collect();
    let mut acked = BTreeMap::<&str, BTreeMap<u64, usize>>::new();
    for write in writes {
        let (namespace, sequence, i) = write.await.unwrap();
        let previous = acked.entry(namespace).or_default().insert(sequence, i);
        assert_eq!(
            previous, None,
            "{namespace} sequence {sequence} acknowledged twice"
        );
    }

    // A committed idempotency key still requires authorization on every retry.
    let (status, _) = send(api, "DELETE", "/v1/kv/a/key/0", "w0", String::new(), &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    for (namespace, acked) in &acked {
        let head = acked.len() as u64;
        assert_eq!(
            acked.keys().copied().collect::<Vec<_>>(),
            (1..=head).collect::<Vec<_>>()
        );
        timeout(
            Duration::from_secs(30),
            subscription.wait_for(namespace, head),
        )
        .await
        .unwrap_or_else(|_| panic!("{namespace} did not reach {head}"));
        let events: Vec<_> = applied
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.namespace == *namespace)
            .cloned()
            .collect();
        assert_eq!(
            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            (1..=head).collect::<Vec<_>>()
        );
        for event in events {
            let i = acked[&event.sequence];
            assert_eq!(event.key, format!("key/{}", i % 5).into_bytes());
            let expected = if i % 7 == 0 {
                Operation::Delete
            } else {
                Operation::Set(i.to_string().into())
            };
            assert_eq!(
                event.operation, expected,
                "{namespace} sequence {}",
                event.sequence
            );
        }
    }
}

async fn open_writer() -> Arc<bedrock_lib::replication::writer::Writer> {
    use bedrock_lib::replication::materialized::Retention;
    use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Writer};
    use bedrock_lib::storage::namespace::memory::MemoryFactory;
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

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn http_writes_commit_once_and_reach_subscribers_in_order() {
    use bedrock_lib::replication::subscriptions::Source;
    use bedrock_lib::replication::subscriptions::pb::replication_server::ReplicationServer;
    use tokio::net::TcpListener;

    let writer = open_writer().await;
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = http.local_addr().unwrap();
    tokio::spawn(bedrock_api::serve(
        http,
        bedrock_api::app(writer.clone(), WRITE_TOKEN.into()).unwrap(),
    ));
    let grpc = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replication = grpc.local_addr().unwrap().to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ReplicationServer::new(Source::new(writer.log())))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc)),
    );
    scenario(api, replication).await;
}

#[cfg(madsim)]
#[madsim::test]
async fn http_writes_commit_once_and_reach_subscribers_in_order() {
    simulated().await;
}

/// One seed must replay one schedule (B-23a). Append any seed that ever failed.
#[cfg(madsim)]
#[test]
fn same_seed_same_schedule() {
    for seed in [1, 2, 3] {
        madsim::runtime::Runtime::check_determinism(seed, madsim::Config::default(), simulated);
    }
}

#[cfg(madsim)]
async fn simulated() {
    use bedrock_lib::replication::subscriptions::Source;
    use bedrock_lib::replication::subscriptions::pb::replication_server::ReplicationServer;
    use tokio::net::TcpListener;

    let handle = madsim::runtime::Handle::current();
    let api: SocketAddr = "10.0.0.2:7002".parse().unwrap();
    let replication: SocketAddr = "10.0.0.2:7000".parse().unwrap();
    handle
        .create_node()
        .name("api")
        .ip(api.ip())
        .build()
        .spawn(async move {
            let writer = open_writer().await;
            let http = TcpListener::bind(api).await.unwrap();
            tokio::spawn(bedrock_api::serve(
                http,
                bedrock_api::app(writer.clone(), WRITE_TOKEN.into()).unwrap(),
            ));
            tonic::transport::Server::builder()
                .add_service(ReplicationServer::new(Source::new(writer.log())))
                .serve(replication)
                .await
                .unwrap();
        });
    handle
        .create_node()
        .name("client")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            // Let the api bind first.
            tokio::time::sleep(Duration::from_millis(10)).await;
            scenario(api, replication.to_string()).await;
        })
        .await
        .unwrap();
}
