//! #126 / S-18: the real built-in KV holder, Gateway and three durable volume slots.
//! Replay with BEDROCK_KV_ACTOR_SEED=42 and RUSTFLAGS="--cfg madsim".
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, IDLE, Placement};
use bedrock_compute::actors::serve::Actors;
use bedrock_compute::actors::volume::{Host, Slot};
use bedrock_compute::builtin::KvExecutor;
use bedrock_compute::runtime::{Error, RustExecutor};
use bedrock_gateway::GatewayConfig;
use bedrock_lib::actors::{self, ActorId, Dc, Topology, topology_key};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::kv::{
    self, Get, Lifecycle, List, Mutation, Record, Reply, Request, Stored, Write, request::Operation,
};
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::replication::pb as rpb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::namespace::{NamespaceDatabase, NamespaceFactory, NamespaceReader};
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory, SimEngine};
use bedrock_lib::storage::{ErrorContext, StorageEngine, StorageError, StorageResult};
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume as pb;
use bedrock_volume::{serve::Service, store::Store};
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.18.1";
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const DC: &str = "arn1";
const RELAYS: [&str; 3] = ["10.0.18.11", "10.0.18.12", "10.0.18.13"];
const QUICKSILVERS: [&str; 2] = ["10.0.18.21", "10.0.18.22"];
const VOLUMES: [&str; 3] = ["10.0.18.31", "10.0.18.32", "10.0.18.33"];
const GATEWAY: &str = "10.0.18.41";
const COMPUTES: [&str; 2] = ["10.0.18.51", "10.0.18.52"];
const NS: &str = "11111111111111111111111111111111";
const OTHER: &str = "22222222222222222222222222222222";
const REST_NS: &str = "33333333333333333333333333333333";
const CACHES: [&str; 2] = ["10.0.18.61", "10.0.18.62"];

fn node(name: &str, ip: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: DC.into(),
            cell: CELL.into(),
            failure_domain: String::new(),
        },
        listen: format!("{ip}:7000").parse().unwrap(),
        advertise: None,
        admin_listen: format!("{ip}:7001").parse().unwrap(),
        data_dir: format!("/data/{name}").into(),
        budget: Budget::default(),
        tls: None,
    }
}

fn addresses(ips: &[&str]) -> Vec<String> {
    ips.iter().map(|ip| format!("{ip}:7000")).collect()
}

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        s3: S3Config {
            endpoint: "http://10.0.18.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn relay(ip: &str) -> RelayConfig {
    RelayConfig {
        node: node(ip, ip),
        tier: Tier::Cell,
        upstreams: addresses(&[API]),
        replay_peers: addresses(&RELAYS.into_iter().filter(|p| *p != ip).collect::<Vec<_>>()),
        replay: Default::default(),
        membership: Some(MembershipSettings {
            voter_ids: RELAYS
                .iter()
                .map(|peer| (format!("{peer}:7000"), (*peer).into()))
                .collect(),
            bootstrap: true,
            grace_ms: 20_000,
            volume_replicas: 3,
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn service(name: &str, ip: &str) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip),
        roles: vec![Role::L2],
        datasets: vec!["quicksilver".into()],
        relays: addresses(&RELAYS),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.18.99:9000".into(),
    }
}

fn spawn<F>(name: &str, ip: &str, start: impl Fn() -> F + Send + Sync + 'static) -> NodeHandle
where
    F: Future<Output = Result<(), bedrock_lib::node::ConfigError>> + 'static,
{
    Handle::current()
        .create_node()
        .name(name)
        .ip(ip.parse().unwrap())
        .init(move || {
            let run = Box::pin(start());
            async move { run.await.unwrap() }
        })
        .build()
}

/// Control-plane databases remain available; customer persistence is unavailable after bootstrap.
/// This adapter only injects I/O failure. All namespace and KV behavior comes from production code.
struct NoCustomerS3 {
    inner: MemoryFactory,
    blocked: AtomicBool,
    attempts: AtomicU64,
}

impl NoCustomerS3 {
    fn check(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        if self.blocked.load(Ordering::Relaxed) && matches!(dataset, "kv" | "volume") {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            return Err(StorageError::Unavailable(ErrorContext {
                engine: "sim-no-customer-s3",
                instance: format!("{dataset}/{namespace}"),
                file: None,
                partition: None,
                detail: "customer S3 unavailable".into(),
            }));
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceFactory for NoCustomerS3 {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        self.inner.namespaces().await
    }
    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        self.check(dataset, namespace)?;
        self.inner.open(dataset, namespace).await
    }
    async fn open_reader(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Option<Arc<dyn NamespaceReader>>> {
        self.check(dataset, namespace)?;
        self.inner.open_reader(dataset, namespace).await
    }
    async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        self.check(dataset, namespace)?;
        self.inner.destroy(dataset, namespace).await
    }
}

fn slot(i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let name = format!("volume-{i}");
    Handle::current()
        .create_node()
        .name(&name)
        .ip(VOLUMES[i].parse().unwrap())
        .init(move || {
            let (engine, name) = (engine.clone(), name.clone());
            async move {
                let service =
                    Service::new(&Admin::new(), Arc::new(Store::new(engine)), &name, CELL, DC)
                        .unwrap();
                *service.directory.write().unwrap() = BTreeMap::from([(
                    0,
                    vec![Owner {
                        node: name,
                        state: State::Ready,
                    }],
                )]);
                tonic::transport::Server::builder()
                    .add_service(pb::volume_server::VolumeServer::new(service))
                    .serve(format!("{}:7000", VOLUMES[i]).parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

struct Fixed;

#[async_trait]
impl Placement for Fixed {
    async fn slots(&self, _actor: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        Ok((
            std::array::from_fn(|i| Slot {
                cell: CELL.into(),
                address: format!("{}:7000", VOLUMES[i]),
                identity: tls::Identity {
                    service: "volume".into(),
                    cell: CELL.into(),
                    node_id: format!("volume-{i}"),
                    host: VOLUMES[i].into(),
                },
            }),
            0,
        ))
    }
}

fn compute_name(ip: &str) -> String {
    format!("compute-{}", ip.rsplit('.').next().unwrap())
}

fn compute(ip: &'static str) -> NodeHandle {
    let name = compute_name(ip);
    Handle::current()
        .create_node()
        .name(&name)
        .ip(ip.parse().unwrap())
        .init(move || {
            let name = name.clone();
            async move {
                let admin = Admin::new();
                let peers = Arc::new(Peers::new(&admin));
                let holder = pb::Holder {
                    cell: CELL.into(),
                    host: name.clone(),
                };
                let client = Arc::new(Client::new(
                    &admin,
                    peers.clone(),
                    addresses(&RELAYS),
                    String::new(),
                ));
                let fallback = Arc::new(RustExecutor::new([]));
                let host = ActorHost::new(
                    &admin,
                    CELL,
                    DC,
                    Host::start(peers.clone(), holder.clone()),
                    Arc::new(Fixed),
                    Arc::new(KvExecutor::new(&admin, client.clone(), fallback.clone())),
                    fallback,
                );
                let member = rpb::Member {
                    node_id: name,
                    service: "compute".into(),
                    region: "eu".into(),
                    dc: DC.into(),
                    cell: CELL.into(),
                    roles: vec!["compute".into()],
                    status: rpb::member::Status::Ready as i32,
                    address: format!("{ip}:7000"),
                    ..Default::default()
                };
                let (_view, target) = bedrock_lib::membership::View::new("compute", CELL);
                let _session = bedrock_lib::relays::client::Session::join(
                    peers,
                    addresses(&RELAYS),
                    vec![target],
                    member,
                    BTreeMap::new(),
                )
                .unwrap();
                tonic::transport::Server::builder()
                    .add_service(
                        pb::actors_server::ActorsServer::new(
                            Actors::new(host, holder).with_kv(client),
                        )
                        .max_decoding_message_size(kv::MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(kv::MAX_MESSAGE_BYTES),
                    )
                    .serve(format!("{ip}:7000").parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

fn client() -> Client {
    let admin = Admin::new();
    Client::new(
        &admin,
        Arc::new(Peers::new(&admin)),
        addresses(&RELAYS),
        String::new(),
    )
}

async fn put(key: &[u8], value: Vec<u8>) -> Option<u64> {
    let key = std::str::from_utf8(key).unwrap();
    let retry = bedrock_lib::placement::hash_of(
        "quicksilver",
        "infrastructure",
        &[key.as_bytes(), &value].concat(),
    );
    let request = hyper::Request::put(format!("/v1/quicksilver/infrastructure/{key}"))
        .header("host", "api")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("idempotency-key", format!("{key}:{retry}"))
        .body(Full::new(Bytes::from(value)))
        .unwrap();
    match bedrock_lib::http::send(
        &format!("{API}:7002"),
        None,
        request,
        Duration::from_secs(30),
    )
    .await
    {
        Ok((status, body)) if status.is_success() => {
            std::str::from_utf8(&body).ok()?.trim().parse().ok()
        }
        _ => None,
    }
}

async fn rest(ip: &str, method: &str, path: &str, body: &str) -> (u16, Vec<u8>) {
    let identity = bedrock_lib::placement::hash_of("kv", path, body.as_bytes());
    let request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "kv")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .header("idempotency-key", format!("rest-{method}-{identity}"))
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap();
    let (status, body) = bedrock_lib::http::send(
        &format!("{ip}:7003"),
        None,
        request,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    (status.as_u16(), body.to_vec())
}

async fn until<T, F: Future<Output = Option<T>>>(what: &str, attempt: impl Fn() -> F) -> T {
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(240);
    loop {
        if let Some(done) = attempt().await {
            return done;
        }
        assert!(bedrock_lib::monotonic_now() < deadline, "{what} timed out");
        sleep(Duration::from_millis(500)).await;
    }
}

fn record(id: &str) -> Record {
    Record {
        format_version: 1,
        id: id.into(),
        tenant: "tenant".into(),
        title: id.into(),
        home_region: "eu".into(),
        storage: format!("kv/{id}"),
        storage_format: 1,
        storage_bytes: Some(512),
        permissions: Vec::new(),
        cache_ttl_seconds: 60,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
    }
}

fn request(operation: Operation) -> Request {
    Request {
        tenant: "tenant".into(),
        operation: Some(operation),
    }
}
fn set(key: &str, value: &str, expiration: u64) -> Mutation {
    Mutation {
        key: key.as_bytes().to_vec(),
        value: Some(Stored {
            value: value.as_bytes().to_vec(),
            metadata: Some("{\"kind\":\"test\"}".into()),
            expiration,
        }),
        ..Default::default()
    }
}
fn write(changes: Vec<Mutation>, retry: Option<&str>) -> Operation {
    Operation::Write(Write {
        changes,
        retry: retry.map(str::to_owned),
        expected: None,
    })
}
fn get(keys: &[&str], min_revision: u64) -> Operation {
    Operation::Get(Get {
        keys: keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
        min_revision,
    })
}
async fn call(client: &Client, namespace: &Record, operation: Operation) -> Reply {
    kv::call(client, namespace, request(operation))
        .await
        .unwrap()
}

#[allow(clippy::disallowed_methods)] // madsim's simulated system clock supplies API expiry timestamps.
fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn scenario() -> Vec<String> {
    let handle = Handle::current();
    let fingerprint = Fingerprint::new();
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        fingerprint.clone(),
    ));
    let databases = Arc::new(NoCustomerS3 {
        inner: MemoryFactory::default(),
        blocked: AtomicBool::new(false),
        attempts: AtomicU64::new(0),
    });
    let factories = databases.clone();
    install(Hooks {
        engine: Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || factories.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    let mut slot_nodes = Vec::new();
    for i in 0..3 {
        let disk = SimEngine::new(&format!("volume-{i}"), Arc::new(|_| None), None, false);
        slot_nodes.push(slot(i, Arc::new(disk.recorded(fingerprint.clone()))));
    }
    spawn("api", API, || bedrock_api::run(api()));
    for ip in RELAYS {
        spawn(&format!("relay-{ip}"), ip, move || {
            bedrock_relay::run(relay(ip))
        });
    }
    for (i, ip) in QUICKSILVERS.into_iter().enumerate() {
        let name = format!("qs-{i}");
        spawn(&name.clone(), ip, move || {
            bedrock_quicksilver::run(service(&name, ip))
        });
    }
    spawn("gateway", GATEWAY, || {
        bedrock_gateway::run(GatewayConfig {
            node: node("gateway", GATEWAY),
            relays: addresses(&RELAYS),
            internal_limit: bedrock_gateway::INTERNAL_LIMIT,
            actor_cache: bedrock_gateway::actors::CACHE,
        })
    });
    let compute_nodes: Vec<_> = COMPUTES.into_iter().map(compute).collect();
    for ip in CACHES {
        let name = format!("kv-{ip}");
        let mut cache_service = service(&name, ip);
        cache_service.datasets.clear();
        let config = bedrock_kv::KvConfig {
            manage_listen: Some(format!("{ip}:7003").parse().unwrap()),
            write_token: Some(TOKEN.into()),
            ..bedrock_kv::KvConfig::from(cache_service)
        };
        spawn(&name, ip, move || bedrock_kv::run(config.clone()));
    }

    let topology = Topology {
        format_version: actors::FORMAT_VERSION,
        region: "eu".into(),
        version: 1,
        dcs: vec![Dc {
            name: DC.into(),
            cells: vec![CELL.into()],
        }],
        previous_dcs: Vec::new(),
    };
    let regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let namespace = record(NS);
    let other_namespace = record(OTHER);
    let rest_namespace = record(REST_NS);
    let actor = kv::actor(&regions, "eu", NS).unwrap();
    let id = ActorId::parse(&regions, &actor.id).unwrap();
    let members: Vec<_> = COMPUTES
        .iter()
        .map(|ip| rpb::Member {
            node_id: compute_name(ip),
            cell: CELL.into(),
            roles: vec!["compute".into()],
            status: rpb::member::Status::Ready as i32,
            ..Default::default()
        })
        .collect();
    let wake = actors::wake_host(&id, &topology, &members, |_| true)
        .unwrap()
        .unwrap()
        .node_id
        .clone();
    let moved_ip = *COMPUTES.iter().find(|ip| compute_name(ip) != wake).unwrap();

    handle
        .create_node()
        .name("kv-caller")
        .ip("10.0.18.90".parse().unwrap())
        .build()
        .spawn(async move {
            let client = Arc::new(client());
            until("topology bootstrap", || async {
                put(&topology_key("eu"), topology.encode_to_vec()).await
            })
            .await;
            for namespace in [&namespace, &other_namespace, &rest_namespace] {
                until("namespace bootstrap", || async {
                    put(
                        format!("kv/namespaces/{}", namespace.id).as_bytes(),
                        namespace.encode_to_vec(),
                    )
                    .await
                })
                .await;
            }
            until("compute and Gateway members", || async {
                let fresh = self::client();
                (fresh.routes("compute", None).await.ok()?.members.len() == 2
                    && fresh.routes("gateway", None).await.ok()?.members.len() == 1
                    && fresh.routes("kv", None).await.ok()?.members.len() == 2)
                    .then_some(())
            })
            .await;
            // Reading through the real Quicksilver client confirms replicated directory readiness before blocking S3.
            until("namespace directory readable", || async {
                kv::record(&client, NS).await.ok()?;
                kv::record(&client, OTHER).await.ok()?;
                Topology::read(&client, "eu", 0).await.ok()
            })
            .await;
            databases.blocked.store(true, Ordering::Relaxed);
            assert!(
                databases.open("kv", NS).await.is_err(),
                "S3 failure injection is armed"
            );
            assert_eq!(databases.attempts.load(Ordering::Relaxed), 1);
            databases.attempts.store(0, Ordering::Relaxed);
            let mut trace = Vec::new();

            let root = format!("/client/v4/accounts/tenant/storage/kv/namespaces/{REST_NS}");
            let bulk_path = format!("{root}/bulk");
            let bulk = r#"[{"key":"cached","value":"rest-value","metadata":{"source":"rest"}},{"key":"ttl","value":"short","expiration_ttl":60}]"#;
            let response = until("REST bulk write", || async {
                let (status, body) = rest(CACHES[0], "PUT", &bulk_path, bulk).await;
                (status == 200).then_some(body)
            })
            .await;
            assert!(
                serde_json::from_slice::<serde_json::Value>(&response).unwrap()["success"]
                    .as_bool()
                    .unwrap()
            );
            let cached_path = format!("{root}/values/cached?min_revision=1");
            for cache in CACHES {
                let (status, body) = rest(cache, "GET", &cached_path, "").await;
                assert_eq!((status, body), (200, b"rest-value".to_vec()));
            }
            let (status, body) =
                rest(CACHES[0], "GET", &format!("{root}/metadata/cached"), "").await;
            assert_eq!(status, 200);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["result"],
                serde_json::json!({"source":"rest"})
            );
            let (status, body) = rest(
                CACHES[1],
                "POST",
                &format!("{root}/bulk/get"),
                r#"{"keys":["cached","absent"],"with_metadata":true,"min_revision":1}"#,
            )
            .await;
            assert_eq!(status, 200);
            let bulk_read: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                bulk_read["result"]["values"]["cached"]["value"],
                "rest-value"
            );
            assert!(bulk_read["result"]["values"]["absent"].is_null());
            let (status, body) = rest(
                CACHES[0],
                "GET",
                &format!("{root}/keys?prefix=c&limit=1"),
                "",
            )
            .await;
            assert_eq!(status, 200);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["result"][0]["name"],
                "cached"
            );
            let absent_path = format!("{root}/values/absent");
            assert_eq!(rest(CACHES[0], "GET", &absent_path, "").await.0, 404);
            for compute in &compute_nodes {
                Handle::current().kill(compute.id());
            }
            assert_eq!(
                rest(CACHES[1], "GET", &cached_path, "").await,
                (200, b"rest-value".to_vec()),
                "the cell cache serves without an awake holder"
            );
            let (bound_value, revision) = kv::get(&client, &rest_namespace, b"cached", 1)
                .await
                .unwrap();
            assert_eq!(revision, 1);
            assert_eq!(
                bound_value.unwrap().value,
                b"rest-value",
                "the Worker binding contract reads the cell cache"
            );
            for compute in &compute_nodes {
                Handle::current().restart(compute.id());
            }
            let update =
                r#"[{"key":"cached","value":"new-value"},{"key":"absent","value":"now-present"}]"#;
            until("REST bulk after holder restart", || async {
                (rest(CACHES[0], "PUT", &bulk_path, update).await.0 == 200).then_some(())
            })
            .await;
            assert_eq!(
                rest(
                    CACHES[1],
                    "GET",
                    &format!("{root}/values/cached?min_revision=2"),
                    ""
                )
                .await,
                (200, b"new-value".to_vec()),
                "minimum revision bypasses an older cached value"
            );
            assert_eq!(
                rest(
                    CACHES[0],
                    "GET",
                    &format!("{absent_path}?min_revision=2"),
                    ""
                )
                .await,
                (200, b"now-present".to_vec()),
                "absence was not cached"
            );
            assert_eq!(
                rest(
                    CACHES[0],
                    "POST",
                    &format!("{root}/bulk/delete"),
                    r#"["absent"]"#
                )
                .await
                .0,
                200
            );
            assert_eq!(
                rest(
                    CACHES[1],
                    "GET",
                    &format!("{absent_path}?min_revision=3"),
                    ""
                )
                .await
                .0,
                404
            );
            let single = format!("{root}/values/single");
            assert_eq!(rest(CACHES[0], "PUT", &single, "single-value").await.0, 200);
            assert_eq!(
                rest(CACHES[1], "GET", &format!("{single}?min_revision=4"), "").await,
                (200, b"single-value".to_vec())
            );
            assert_eq!(rest(CACHES[1], "DELETE", &single, "").await.0, 200);
            assert_eq!(
                rest(CACHES[0], "GET", &format!("{single}?min_revision=5"), "")
                    .await
                    .0,
                404
            );
            sleep(Duration::from_secs(61)).await;
            assert_eq!(rest(CACHES[0], "GET", &format!("{root}/values/ttl?min_revision=5"), "").await.0, 404);
            assert_eq!(rest(CACHES[1], "GET", &format!("{root}/values/cached?min_revision=5"), "").await,
                (200, b"new-value".to_vec()), "warm the cache again after the expiry check");
            let mut rest_retired = rest_namespace.clone();
            rest_retired.state = Lifecycle::Deleting as i32;
            rest_retired.deleting_at = now_seconds() * 1000;
            until("retire warm REST cache", || async {
                put(
                    format!("kv/namespaces/{REST_NS}").as_bytes(),
                    rest_retired.encode_to_vec(),
                )
                .await
            })
            .await;
            until("warm cache checks namespace deletion", || async {
                (rest(CACHES[1], "GET", &cached_path, "").await.0 == 404).then_some(())
            })
            .await;
            trace.push("REST bulk HRW cache minimum absence deletion".into());

            let empty = until("empty namespace holder ready", || async {
                kv::call(&client, &namespace, request(get(&["missing"], 0)))
                    .await
                    .ok()
            })
            .await;
            assert_eq!(
                empty.revision, 0,
                "the initial actor claim allocates no data revision"
            );
            assert!(empty.items[0].value.is_none());
            trace.push("empty revision0".into());

            let expires = now_seconds() + 61;
            let first_request = write(
                vec![
                    set("a", "first", 0),
                    set("b", "second", 0),
                    set("ttl", "expires", expires),
                ],
                Some("first"),
            );
            let first = until("first KV quorum write", || async {
                kv::call(&client, &namespace, request(first_request.clone()))
                    .await
                    .ok()
            })
            .await;
            assert_eq!(first.revision, 1, "claims are not namespace revisions");
            assert_eq!(
                databases.attempts.load(Ordering::Relaxed),
                0,
                "write never asks customer S3"
            );
            trace.push(format!("write {}", first.revision));

            let read = call(&client, &namespace, get(&["a", "missing"], 1)).await;
            assert_eq!(read.revision, 1);
            assert_eq!(read.items[0].value.as_ref().unwrap().value, b"first");
            assert!(
                read.items[1].value.is_none(),
                "absence carries namespace revision"
            );
            let wrong_tenant = Request {
                tenant: "someone-else".into(),
                operation: Some(get(&["a"], 1)),
            };
            assert_eq!(
                kv::call(&client, &namespace, wrong_tenant)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );

            let rejected = write(
                vec![
                    set("a", "must not leak", 0),
                    set("overflow", &"x".repeat(512), 0),
                ],
                None,
            );
            assert_eq!(
                kv::call(&client, &namespace, request(rejected))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::ResourceExhausted
            );
            let unchanged = call(&client, &namespace, get(&["a", "overflow"], 1)).await;
            assert_eq!(unchanged.revision, 1);
            assert_eq!(unchanged.items[0].value.as_ref().unwrap().value, b"first");
            assert!(
                unchanged.items[1].value.is_none(),
                "rejected bulk has no partial result"
            );
            trace.push("quota rejection atomic".into());

            let isolated = call(&client, &other_namespace, get(&["a"], 0)).await;
            assert_eq!(isolated.revision, 0);
            assert!(isolated.items[0].value.is_none());
            assert_eq!(
                call(
                    &client,
                    &other_namespace,
                    write(vec![set("a", "other tenant namespace", 0)], None)
                )
                .await
                .revision,
                1
            );
            assert_eq!(
                call(&client, &namespace, get(&["a"], 1)).await.items[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .value,
                b"first"
            );
            trace.push("namespace isolation".into());

            let second = call(
                &client,
                &namespace,
                write(
                    vec![
                        Mutation {
                            key: b"b".to_vec(),
                            value: None,
                            ..Default::default()
                        },
                        set("a", "replacement", 0),
                        set("a", "last wins", 0),
                        set("c", "third", 0),
                    ],
                    Some("second"),
                ),
            )
            .await;
            assert_eq!(second.revision, 2);
            let list = |after| {
                Operation::List(List {
                    prefix: Vec::new(),
                    after,
                    limit: 1,
                    min_revision: 2,
                })
            };
            let page = call(&client, &namespace, list(None)).await;
            assert_eq!(page.items.len(), 1);
            assert_eq!(page.items[0].key, b"a");
            assert!(page.items[0].value.as_ref().unwrap().value.is_empty());
            assert_eq!(
                page.items[0].value.as_ref().unwrap().metadata.as_deref(),
                Some("{\"kind\":\"test\"}")
            );
            let next = call(&client, &namespace, list(page.cursor)).await;
            assert_eq!(next.items[0].key, b"c", "deleted b is not listed");
            trace.push("list a,c".into());

            // Sleeping releases the first claim. Wake explicitly on the other host, then follow the Gateway redirect.
            sleep(IDLE + Duration::from_secs(3)).await;
            let direct =
                pb::actors_client::ActorsClient::connect(format!("http://{moved_ip}:7000"))
                    .await
                    .unwrap();
            let moved = until("wake namespace on another holder", || {
                let mut direct = direct.clone();
                let actor = actor.clone();
                async move {
                    let reply = direct
                        .invoke(pb::InvokeRequest {
                            actor: Some(actor),
                            payload: request(get(&["missing"], 2)).encode_to_vec(),
                            ..Default::default()
                        })
                        .await
                        .ok()?
                        .into_inner();
                    if reply.redirect || reply.code != 0 {
                        return None;
                    }
                    let reply = Reply::decode(reply.payload.as_slice()).ok()?;
                    (reply.code == 0).then_some(reply)
                }
            })
            .await;
            assert_eq!(
                moved.revision, 2,
                "a new volume claim leaves namespace revision unchanged"
            );
            assert!(moved.items[0].value.is_none());
            let retried = call(&client, &namespace, first_request).await;
            assert_eq!(
                (retried.revision, retried.committed_at),
                (first.revision, first.committed_at),
                "retry on another holder keeps original result"
            );
            let changed_retry = call(
                &client,
                &namespace,
                write(
                    vec![set("different-retry-body", "must not apply", 0)],
                    Some("first"),
                ),
            )
            .await;
            assert_eq!(
                (changed_retry.revision, changed_retry.written_keys),
                (first.revision, 3)
            );
            assert!(
                call(&client, &namespace, get(&["different-retry-body"], 2))
                    .await
                    .items[0]
                    .value
                    .is_none()
            );
            assert_eq!(
                call(&client, &namespace, get(&["a"], 2)).await.items[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .value,
                b"last wins",
                "retry did not apply its old mutation"
            );
            trace.push("move revision2 retry1".into());

            sleep(Duration::from_secs(62)).await;
            let expired = call(&client, &namespace, get(&["ttl", "b"], 2)).await;
            assert_eq!(expired.revision, 2);
            assert!(expired.items.iter().all(|item| item.value.is_none()));
            let after_expiry = call(
                &client,
                &namespace,
                Operation::List(List {
                    prefix: Vec::new(),
                    after: None,
                    limit: 1000,
                    min_revision: 2,
                }),
            )
            .await;
            assert_eq!(
                after_expiry
                    .items
                    .iter()
                    .map(|item| item.key.as_slice())
                    .collect::<Vec<_>>(),
                [b"a".as_slice(), b"c".as_slice()]
            );
            let compacted = call(&client, &namespace, Operation::Compact(true)).await;
            assert_eq!(
                compacted.revision, 2,
                "compaction does not allocate data revisions"
            );
            trace.push("expiry and compaction revision2".into());

            // The authoritative directory retires the second namespace before its actor tombstone is installed.
            let mut retired = other_namespace.clone();
            retired.state = Lifecycle::Deleting as i32;
            retired.deleting_at = now_seconds() * 1000;
            until("retired directory record", || async {
                put(
                    format!("kv/namespaces/{OTHER}").as_bytes(),
                    retired.encode_to_vec(),
                )
                .await
            })
            .await;
            until("retirement visible to holders", || async {
                let error = kv::call(
                    &client,
                    &retired,
                    request(write(vec![set("a", "revive", 0)], None)),
                )
                .await
                .err()?;
                (error.code() == tonic::Code::FailedPrecondition).then_some(())
            })
            .await;
            assert_eq!(
                kv::purge(&client, &retired).await.unwrap_err().code(),
                tonic::Code::FailedPrecondition,
                "actor deletion respects the same directory grace"
            );
            sleep(bedrock_kv::namespaces::PURGE_GRACE + Duration::from_secs(1)).await;
            kv::purge(&client, &retired).await.unwrap();
            kv::purge(&client, &retired).await.unwrap();
            let deleted = kv::call(
                &client,
                &retired,
                request(write(vec![set("a", "revive", 0)], None)),
            )
            .await
            .unwrap_err();
            assert_eq!(deleted.code(), tonic::Code::Unavailable);
            assert!(
                deleted.message().contains("deleted"),
                "volume tombstone prevents re-acquisition: {deleted}"
            );

            let admin = Admin::new();
            let directory = bedrock_kv::namespaces::Directory::new(
                &admin,
                client.clone(),
                bedrock_kv::namespaces::Api {
                    address: format!("{API}:7002"),
                    token: TOKEN.into(),
                    tls: None,
                },
                "eu".into(),
            );
            directory.purge("tenant", OTHER).await.unwrap();
            until("purged directory record removed", || async {
                (kv::record(&client, OTHER).await.err()?.code() == tonic::Code::NotFound)
                    .then_some(())
            })
            .await;
            assert_eq!(
                call(&client, &namespace, get(&["a"], 2)).await.items[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .value,
                b"last wins",
                "purge is isolated to the retired namespace"
            );
            trace.push("retire grace purge idempotent and isolated".into());

            // A single surviving slot cannot acknowledge a new data entry. Restarts retain each slot's fsynced state.
            for slot in &slot_nodes[..2] {
                Handle::current().kill(slot.id());
            }
            let minority = write(
                vec![
                    set("unacked-left", "minority", 0),
                    set("unacked-right", "minority", 0),
                ],
                Some("minority"),
            );
            let unavailable = kv::call(&client, &namespace, request(minority.clone()))
                .await
                .unwrap_err();
            assert_eq!(unavailable.code(), tonic::Code::Unavailable);
            for slot in &slot_nodes[..2] {
                Handle::current().restart(slot.id());
            }
            let recovered = until("quorum returns with committed state", || async {
                kv::call(
                    &client,
                    &namespace,
                    request(get(&["a", "unacked-left", "unacked-right"], 2)),
                )
                .await
                .ok()
            })
            .await;
            assert!((2..=3).contains(&recovered.revision));
            assert_eq!(
                recovered.items[0].value.as_ref().unwrap().value,
                b"last wins"
            );
            let left = recovered.items[1]
                .value
                .as_ref()
                .map(|value| value.value.as_slice());
            let right = recovered.items[2]
                .value
                .as_ref()
                .map(|value| value.value.as_slice());
            assert_eq!(left, right, "an ambiguous bulk is still atomic");
            assert_eq!(recovered.revision, if left.is_some() { 3 } else { 2 });
            assert!(left.is_none() || left == Some(b"minority".as_slice()));
            let resolved = call(&client, &namespace, minority.clone()).await;
            let retried = call(&client, &namespace, minority).await;
            assert_eq!(
                resolved.revision, 3,
                "retry applies or recovers the ambiguous batch once"
            );
            assert_eq!(
                (retried.revision, retried.committed_at),
                (resolved.revision, resolved.committed_at)
            );
            assert_eq!(
                call(
                    &client,
                    &namespace,
                    write(vec![set("healthy", "quorum", 0)], None)
                )
                .await
                .revision,
                4
            );
            trace.push(format!(
                "minority recovery{} retry3 next4",
                recovered.revision
            ));
            assert_eq!(
                databases.attempts.load(Ordering::Relaxed),
                0,
                "all holder I/O used volume quorum"
            );
            trace
        })
        .await
        .unwrap()
}

#[allow(clippy::disallowed_methods, clippy::disallowed_types)] // A fresh test thread resets the simulator's seeded map hashing.
fn run(seed: u64) -> Vec<String> {
    // The debug orchestration future includes every service and REST scenario.
    std::thread::Builder::new().stack_size(64 << 20).spawn(move || {
        let mut runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
        runtime.set_time_limit(Duration::from_secs(2400));
        runtime.block_on(Box::pin(scenario()))
    }).unwrap().join().unwrap_or_else(|failure| {
        eprintln!("Replay: BEDROCK_KV_ACTOR_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test kv_actor -- --nocapture");
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn kv_quorum_bulk_retry_move_reads_expiry_and_namespaces_replay() {
    let seeds = std::env::var("BEDROCK_KV_ACTOR_SEED")
        .map(|s| vec![s.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2]);
    for seed in seeds {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("KV actor seed {seed}: {first:?}");
    }
}
