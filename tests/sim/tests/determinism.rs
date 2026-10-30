//! B-23a spike and determinism check: one seed must reproduce one trace.
//!
//! The real `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` entry points boot as simulated nodes and stop
//! on a simulated Ctrl-C. Alongside them, two nodes talk over tonic (unary and server stream), the server writes every
//! message to the simulated S3, and an Axum router answers HTTP over simulated TCP. Run with `RUSTFLAGS="--cfg madsim" cargo test -p bedrock-sim`.
//! The proto is shared with `tests/transport.rs`, which also runs in normal builds, so `build.rs` compiles it in both.
#![cfg(madsim)]

use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use bedrock_lib::datasets::Dataset;
use bedrock_lib::node::{
    ApiConfig, Budget, Location, NodeConfig, RelayConfig, Role, ServiceConfig, Tier,
};
use bedrock_lib::s3::S3Config;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, Runtime};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep};
use tonic::{Request, Response, Status};

mod pb {
    tonic::include_proto!("sim");
}
use pb::Msg;
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};

/// Seeds the check always runs; append any seed that ever failed.
const SEEDS: &[u64] = &[1, 2, 3, 42];

const NODE_B: &str = "10.0.0.2:50051";
const S3: &str = "10.0.0.3:9000";
const HTTP: &str = "10.0.0.4:8080";
const BUCKET: &str = "bedrock";

/// Ordered events as `(simulated nanos since start, event)`; its hash is the run's fingerprint.
#[derive(Clone)]
struct Trace(Arc<Mutex<Vec<(u128, String)>>>, Instant);

impl Trace {
    // `tokio::time::Instant` is std's under madsim, which simulates its `now`.
    #[allow(clippy::disallowed_methods)]
    fn new() -> Self {
        Trace(Arc::default(), Instant::now())
    }
    fn record(&self, event: String) {
        let at = self.1.elapsed().as_nanos();
        self.0.lock().unwrap().push((at, event));
    }
    fn hash(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.0.lock().unwrap().hash(&mut h);
        h.finish()
    }
}

/// Random service delay so the interleaving depends on the seed.
async fn jitter() {
    sleep(Duration::from_millis(thread_rng().gen_range(1..20))).await;
}

fn node_config(name: &str, ip: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: "eu-1".into(),
            cell: "eu-1a".into(),
        },
        listen: format!("{ip}:7000").parse().unwrap(),
        admin_listen: format!("{ip}:7001").parse().unwrap(),
        data_dir: format!("/data/{name}").into(),
        budget: Budget::default(),
    }
}

fn service_config(node: NodeConfig, datasets: &[Dataset]) -> ServiceConfig {
    ServiceConfig {
        node,
        roles: vec![Role::L1],
        datasets: datasets.iter().map(|d| d.name.to_string()).collect(),
        relays: vec!["10.0.1.3:7000".into()],
        api: "10.0.1.4:7002".into(),
        s3_endpoint: format!("http://{S3}"),
    }
}

fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Config::builder()
        .endpoint_url(format!("http://{S3}"))
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

struct EchoNode {
    trace: Trace,
    s3: aws_sdk_s3::Client,
}

#[tonic::async_trait]
impl Echo for EchoNode {
    async fn send(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        let msg = req.into_inner();
        jitter().await;
        self.s3
            .put_object()
            .bucket(BUCKET)
            .key(format!("msg/{}", msg.id))
            .body(ByteStream::from(msg.body.clone().into_bytes()))
            .send()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        self.trace.record(format!("b stored {}", msg.id));
        Ok(Response::new(msg))
    }

    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<Msg, Status>>;

    async fn watch(&self, req: Request<Msg>) -> Result<Response<Self::WatchStream>, Status> {
        let from = req.into_inner().id;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for id in from..from + 3 {
                jitter().await;
                let _ = tx
                    .send(Ok(Msg {
                        id,
                        body: "tick".into(),
                    }))
                    .await;
            }
        });
        Ok(Response::new(rx.into()))
    }
}

async fn scenario() -> u64 {
    let handle = Handle::current();
    let trace = Trace::new();
    let addr = |s: &str| s.parse::<SocketAddr>().unwrap();

    // The production entry points, unchanged; only the runtime underneath is simulated.
    let services = [
        ("kv", "10.0.1.1"),
        ("cdn", "10.0.1.2"),
        ("relay", "10.0.1.3"),
        ("api", "10.0.1.4"),
    ];
    let running: Vec<_> = services
        .iter()
        .map(|&(name, ip)| {
            let node = node_config(name, ip);
            let fut: std::pin::Pin<Box<dyn Future<Output = _> + Send>> = match name {
                "kv" => Box::pin(bedrock_kv::run(service_config(node, bedrock_kv::DATASETS))),
                "cdn" => Box::pin(bedrock_cdn::run(service_config(
                    node,
                    bedrock_cdn::DATASETS,
                ))),
                "relay" => Box::pin(bedrock_relay::run(RelayConfig {
                    node,
                    tier: Tier::Cell,
                    upstreams: vec!["10.0.1.4:7000".into()],
                })),
                _ => Box::pin(bedrock_api::run(ApiConfig {
                    node,
                    write_listen: format!("{ip}:7002").parse().unwrap(),
                    write_token: "simulation-writer-token".into(),
                    retention: Default::default(),
                    s3: S3Config {
                        endpoint: format!("http://{S3}"),
                        bucket: BUCKET.into(),
                        region: "us-east-1".into(),
                        access_key_id: "sim".into(),
                        secret_access_key: "sim".into(),
                        max_attempts: 3,
                        attempt_timeout_ms: 30_000,
                    },
                })),
            };
            let t = trace.clone();
            let h = handle
                .create_node()
                .name(name)
                .ip(ip.parse().unwrap())
                .build();
            h.spawn(async move {
                fut.await.unwrap();
                t.record(format!("{name} stopped"));
            })
        })
        .collect();

    handle
        .create_node()
        .name("s3")
        .ip(addr(S3).ip())
        .build()
        .spawn(async move {
            aws_sdk_s3::server::SimServer::builder()
                .with_bucket(BUCKET)
                .serve(addr(S3))
                .await
                .unwrap();
        });

    let t = trace.clone();
    handle
        .create_node()
        .name("b")
        .ip(addr(NODE_B).ip())
        .build()
        .spawn(async move {
            let node = EchoNode {
                trace: t,
                s3: s3_client(),
            };
            tonic::transport::Server::builder()
                .add_service(EchoServer::new(node))
                .serve(addr(NODE_B))
                .await
                .unwrap();
        });

    let t = trace.clone();
    handle
        .create_node()
        .name("http")
        .ip(addr(HTTP).ip())
        .build()
        .spawn(async move {
            let router =
                axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));
            let listener = TcpListener::bind(addr(HTTP)).await.unwrap();
            loop {
                let (stream, peer) = listener.accept().await.unwrap();
                t.record(format!("http accepted {peer}"));
                let svc = TowerToHyperService::new(router.clone());
                tokio::spawn(
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc),
                );
            }
        });

    let t = trace.clone();
    let client = handle
        .create_node()
        .name("a")
        .ip("10.0.0.1".parse().unwrap())
        .build();
    client
        .spawn(async move {
            // Let the servers bind first.
            sleep(Duration::from_millis(10)).await;
            let mut echo = EchoClient::connect(format!("http://{NODE_B}"))
                .await
                .unwrap();

            let sends: Vec<_> = (0..5u32)
                .map(|id| {
                    let mut echo = echo.clone();
                    let t = t.clone();
                    tokio::spawn(async move {
                        let reply = echo
                            .send(Msg {
                                id,
                                body: format!("m{id}"),
                            })
                            .await
                            .unwrap();
                        t.record(format!("a acked {}", reply.into_inner().id));
                    })
                })
                .collect();
            for s in sends {
                s.await.unwrap();
            }

            let mut stream = echo
                .watch(Msg {
                    id: 100,
                    body: String::new(),
                })
                .await
                .unwrap()
                .into_inner();
            while let Some(msg) = stream.message().await.unwrap() {
                t.record(format!("a streamed {}", msg.id));
            }

            let listed = s3_client()
                .list_objects_v2()
                .bucket(BUCKET)
                .send()
                .await
                .unwrap();
            let mut keys: Vec<_> = listed.contents().iter().filter_map(|o| o.key()).collect();
            keys.sort();
            t.record(format!("a listed {keys:?}"));

            let (mut http, conn) = hyper::client::conn::http1::handshake(TokioIo::new(
                TcpStream::connect(addr(HTTP)).await.unwrap(),
            ))
            .await
            .unwrap();
            tokio::spawn(conn);
            let req = hyper::Request::get("/health")
                .body(Empty::<Bytes>::new())
                .unwrap();
            let body = http
                .send_request(req)
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap();
            t.record(format!("a http {:?}", body.to_bytes()));
        })
        .await
        .unwrap();

    // Every service must still be running here, so a `run` that returns early fails instead of passing.
    {
        let events = trace.0.lock().unwrap();
        let early: Vec<_> = events
            .iter()
            .filter(|(_, e)| e.ends_with(" stopped"))
            .collect();
        assert!(early.is_empty(), "stopped before Ctrl-C: {early:?}");
    }
    for (name, _) in services {
        handle.send_ctrl_c(name);
    }
    for r in running {
        r.await.unwrap();
    }

    let events = trace.0.lock().unwrap();
    for (name, _) in services {
        let stopped = format!("{name} stopped");
        assert!(
            events.iter().any(|(_, e)| *e == stopped),
            "{name} did not shut down cleanly"
        );
    }
    assert_eq!(
        events
            .iter()
            .filter(|(_, e)| e.starts_with("a streamed"))
            .count(),
        3
    );
    assert!(
        events.iter().any(|(_, e)| e.contains("msg/4")),
        "S3 listing missing writes"
    );
    assert!(events.iter().any(|(_, e)| e == "a http b\"ok\""));
    drop(events);
    trace.hash()
}

/// Runs on a fresh OS thread: madsim seeds std's `HashMap` keys once per thread, so reusing a thread would hide
/// `HashMap`-order nondeterminism between the two compared runs.
#[allow(clippy::disallowed_methods)] // test harness, not shared-core code
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap()
}

#[test]
fn same_seed_same_trace() {
    for &seed in SEEDS {
        assert_eq!(run(seed), run(seed), "seed {seed} is not deterministic");
    }
}

#[test]
fn trace_depends_on_seed() {
    let hashes: std::collections::BTreeSet<_> = SEEDS.iter().map(|&s| run(s)).collect();
    assert!(
        hashes.len() > 1,
        "trace ignores the seed, so the determinism check proves nothing"
    );
}
