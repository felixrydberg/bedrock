//! #15 / B-12 acceptance: cell membership from relay streams; #19 / B-16: the L2 placement directory published
//! beside it, with #13 / B-14 storage health in member updates; #22 / B-21: range handoff with data following
//! ownership while writes run. All through the real `bedrock-api` and `bedrock-relay` entry points. Replay one seed
//! with `BEDROCK_MEMBERSHIP_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::membership::{self, View};
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Tier,
};
use bedrock_lib::placement::hash_of;
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::relays::client::Session;
use bedrock_lib::replication::pb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::{KeyRange, StorageEngine, l2_parts};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::sync::Notify;
use tokio::time::sleep;

const API: &str = "10.0.2.1";
const CELL_RELAYS: [&str; 3] = ["10.0.2.11", "10.0.2.12", "10.0.2.13"];
const OTHER_RELAY: &str = "10.0.2.14";
const TOKEN: &str = "simulation-writer-token";
const GRACE: Duration = Duration::from_secs(20);
/// A silent peer is noticed within the relay stream deadline.
const IDLE: Duration = bedrock_lib::relays::IDLE;

fn node(name: &str, ip: &str, cell: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: "eu-1".into(),
            cell: cell.into(),
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

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API, "eu-1a"),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        s3: S3Config {
            endpoint: "http://10.0.2.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn relay(name: &str, ip: &str, cell: &str, relays: &[&str]) -> RelayConfig {
    RelayConfig {
        node: node(name, ip, cell),
        tier: Tier::Cell,
        upstreams: vec![format!("{API}:7000")],
        replay_peers: relays
            .iter()
            .filter(|peer| **peer != ip)
            .map(|peer| format!("{peer}:7000"))
            .collect(),
        replay: Default::default(),
        membership: Some(MembershipSettings {
            api: format!("{API}:7002"),
            grace_ms: GRACE.as_millis() as u64,
        }),
        write_token: Some(TOKEN.into()),
    }
}

/// A leaf's storage-health manager and its one dataset's engine, raw and wrapped.
type Storage = (
    Arc<StorageHealth>,
    Arc<MemoryEngine>,
    Arc<dyn StorageEngine>,
);

/// What a test reads from a leaf process, across its restarts.
#[derive(Default)]
struct Leaf {
    view: Mutex<Option<Arc<View>>>,
    revision: AtomicU64,
    drain: Notify,
    storage: Mutex<Option<Storage>>,
    /// The `cdn` ranges a holder keeps; it survives restarts.
    data: Option<Arc<MemoryEngine>>,
}

impl Leaf {
    fn directory_of(&self, dataset: &str) -> Directory {
        self.view
            .lock()
            .unwrap()
            .as_ref()
            .map(|view| view.directory(dataset).unwrap())
            .unwrap_or_default()
    }

    fn members(&self) -> Vec<String> {
        self.view
            .lock()
            .unwrap()
            .as_ref()
            .map(|view| view.members().unwrap().into_keys().collect())
            .unwrap_or_default()
    }

    /// The unhealthy datasets `node` announced, as this leaf's view has them.
    fn unhealthy(&self, node: &str) -> Vec<String> {
        self.view
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|view| view.members().unwrap().remove(node))
            .map(|member| member.unhealthy)
            .unwrap_or_default()
    }

    fn directory(&self) -> Directory {
        self.view
            .lock()
            .unwrap()
            .as_ref()
            .map(|view| view.directory("cdn").unwrap())
            .unwrap_or_default()
    }
}

/// Every range has three ready owners from `members`, or all of them when there are fewer.
fn placed(directory: &Directory, members: &[&str]) -> bool {
    directory.contains_key(&0)
        && directory.values().all(|owners| {
            owners.len() == members.len().min(3)
                && owners
                    .iter()
                    .all(|o| o.state == State::Ready && members.contains(&o.node.as_str()))
        })
}

fn spawn_leaf(
    name: &'static str,
    ip: &str,
    service: &'static str,
    cell: &'static str,
    relays: Vec<String>,
) -> (NodeHandle, Arc<Leaf>) {
    spawn_member(name, ip, service, cell, relays, Leaf::default())
}

/// A `bedrock-cdn` member carrying the `cdn` dataset. It runs the same session as the binary (`Session::join` with
/// the dataset's engine) over an engine the test can read.
fn spawn_holder(name: &'static str, ip: &str, relays: Vec<String>) -> (NodeHandle, Arc<Leaf>) {
    let leaf = Leaf {
        data: Some(Arc::new(MemoryEngine::new("cdn"))),
        ..Leaf::default()
    };
    spawn_member(name, ip, "cdn", "eu-1a", relays, leaf)
}

fn spawn_member(
    name: &'static str,
    ip: &str,
    service: &'static str,
    cell: &'static str,
    relays: Vec<String>,
    leaf: Leaf,
) -> (NodeHandle, Arc<Leaf>) {
    let leaf = Arc::new(leaf);
    let state = leaf.clone();
    let address = format!("{ip}:7000");
    let node = Handle::current()
        .create_node()
        .name(name)
        .ip(ip.parse().unwrap())
        .init(move || {
            let (state, relays, address) = (state.clone(), relays.clone(), address.clone());
            async move {
                let (view, target) = View::new(service, cell);
                let view = Arc::new(view);
                *state.view.lock().unwrap() = Some(view.clone());
                let admin = Admin::new();
                let peers = Arc::new(Peers::new(&admin));
                let storage = StorageHealth::new(&admin);
                let engine = state
                    .data
                    .clone()
                    .unwrap_or_else(|| Arc::new(MemoryEngine::new(service)));
                let wrapped = storage.wrap(service, "engine", engine.clone());
                *state.storage.lock().unwrap() = Some((storage.clone(), engine, wrapped.clone()));
                // Relays push ranges of the dataset named after the service; customer KV has none.
                let mut ranged: BTreeMap<String, Arc<dyn StorageEngine>> = BTreeMap::new();
                if service != "kv" {
                    ranged.insert(service.into(), wrapped);
                }
                let member = pb::Member {
                    node_id: name.into(),
                    service: service.into(),
                    datasets: vec![service.into()],
                    region: "eu".into(),
                    dc: "eu-1".into(),
                    cell: cell.into(),
                    roles: vec!["l2".into()],
                    status: pb::member::Status::Ready as i32,
                    address,
                    ..Default::default()
                };
                let session =
                    Session::join(peers.clone(), relays, vec![target], member, ranged).unwrap();
                let namespace = membership::namespace(service, cell);
                loop {
                    tokio::select! {
                        () = state.drain.notified() => break,
                        () = sleep(Duration::from_millis(100)) => {
                            if let Some(revision) = session.applied(membership::DATASET, &namespace) {
                                state.revision.store(revision, Ordering::Relaxed);
                            }
                            view.refresh(&session, &peers).unwrap();
                            session.set_unhealthy(&storage.unhealthy());
                        },
                    }
                }
                session.drain().await;
            }
        })
        .build();
    (node, leaf)
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
            let run = start();
            async move { run.await.unwrap() }
        })
        .build()
}

async fn until(what: &str, within: Duration, ready: impl Fn() -> bool) {
    let deadline = bedrock_lib::monotonic_now() + within;
    while !ready() {
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "{what} within {within:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// The view must not change at all for `period`.
async fn steady(what: &str, leaf: &Leaf, period: Duration, members: &[&str]) {
    // The leaf samples its applied revision every 100 ms.
    sleep(Duration::from_millis(200)).await;
    let revision = leaf.revision.load(Ordering::Relaxed);
    let deadline = bedrock_lib::monotonic_now() + period;
    while bedrock_lib::monotonic_now() < deadline {
        assert_eq!(leaf.members(), members, "{what}: membership changed");
        assert_eq!(
            leaf.revision.load(Ordering::Relaxed),
            revision,
            "{what}: a membership write happened"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn scenario() -> Vec<String> {
    let handle = Handle::current();
    let mut trace = Vec::new();
    let started = bedrock_lib::monotonic_now();
    let mark = |trace: &mut Vec<String>, event: &str| {
        trace.push(format!("{} {event}", started.elapsed().as_millis()));
    };
    spawn("api", API, || bedrock_api::run(api()));
    let relays: Vec<NodeHandle> = CELL_RELAYS
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            let name = format!("relay-{}", i + 1);
            let config = relay(&name, ip, "eu-1a", &CELL_RELAYS);
            spawn(&name, ip, move || bedrock_relay::run(config.clone()))
        })
        .collect();
    let other = relay("relay-b", OTHER_RELAY, "eu-1b", &[OTHER_RELAY]);
    spawn("relay-b", OTHER_RELAY, move || {
        bedrock_relay::run(other.clone())
    });
    let ordered = |first: usize| -> Vec<String> {
        (0..3)
            .map(|i| format!("{}:7000", CELL_RELAYS[(first + i) % 3]))
            .collect()
    };
    let (kv1_node, kv1) = spawn_leaf("kv-1", "10.0.2.21", "kv", "eu-1a", ordered(0));
    let (_, kv2) = spawn_leaf("kv-2", "10.0.2.22", "kv", "eu-1a", ordered(1));
    let (_, cdn1) = spawn_leaf("cdn-1", "10.0.2.23", "cdn", "eu-1a", ordered(0));
    let (_, kvb) = spawn_leaf(
        "kv-b",
        "10.0.2.24",
        "kv",
        "eu-1b",
        vec![format!("{OTHER_RELAY}:7000")],
    );
    let both = ["kv-1", "kv-2"];

    // A connecting node appears in every node's view; services and cells stay apart.
    until("joined", Duration::from_secs(60), || {
        kv1.members() == both
            && kv2.members() == both
            && cdn1.members() == ["cdn-1"]
            && kvb.members() == ["kv-b"]
    })
    .await;
    mark(&mut trace, "joined");

    // Killing the relay that holds kv-1 and cdn-1 (and publishes) changes nothing while they fail over.
    handle.kill(relays[0].id());
    steady(
        "relay failover",
        &kv2,
        GRACE + IDLE + Duration::from_secs(10),
        &both,
    )
    .await;
    assert_eq!(cdn1.members(), ["cdn-1"]);
    handle.restart(relays[0].id());
    sleep(Duration::from_secs(5)).await;
    mark(&mut trace, "relay failover");

    // A node restarted within the grace period rejoins with the same identity and no change.
    handle.kill(kv1_node.id());
    sleep(Duration::from_secs(5)).await;
    handle.restart(kv1_node.id());
    steady("restart within grace", &kv2, GRACE + IDLE, &both).await;
    until("restarted node's own view", Duration::from_secs(30), || {
        kv1.members() == both
    })
    .await;
    mark(&mut trace, "restarted");

    // Graceful shutdown removes the member without waiting out the grace period.
    kv1.drain.notify_one();
    until("drained member removed", GRACE / 2, || {
        kv2.members() == ["kv-2"]
    })
    .await;
    mark(&mut trace, "drained");
    handle.kill(kv1_node.id());
    handle.restart(kv1_node.id());
    until("rejoined", Duration::from_secs(30), || {
        kv2.members() == both
    })
    .await;
    mark(&mut trace, "rejoined");

    // Cut off from every relay, kv-1 is removed although its peers can still reach it.
    let network = madsim::net::NetSim::current();
    let cut: Vec<_> = relays.iter().map(NodeHandle::id).collect();
    for relay in &cut {
        network.clog_link(kv1_node.id(), *relay);
        network.clog_link(*relay, kv1_node.id());
    }
    sleep(GRACE - Duration::from_secs(1)).await;
    assert_eq!(kv2.members(), both, "removed before the grace period");
    until("cut-off member removed", IDLE + GRACE, || {
        kv2.members() == ["kv-2"]
    })
    .await;
    mark(&mut trace, "cut off");
    for relay in &cut {
        network.unclog_link(kv1_node.id(), *relay);
        network.unclog_link(*relay, kv1_node.id());
    }
    until("healed member rejoined", Duration::from_secs(60), || {
        kv2.members() == both
    })
    .await;
    mark(&mut trace, "healed");

    // A killed node is removed after the grace period, not before.
    handle.kill(kv1_node.id());
    sleep(GRACE - Duration::from_secs(1)).await;
    assert_eq!(kv2.members(), both, "removed before the grace period");
    until("killed member removed", IDLE + GRACE, || {
        kv2.members() == ["kv-2"]
    })
    .await;
    mark(&mut trace, "killed");

    // With every cell relay down, nodes keep the last view and keep serving it.
    for relay in &relays {
        handle.kill(relay.id());
    }
    steady("all relays down", &kv2, Duration::from_secs(60), &["kv-2"]).await;
    assert_eq!(cdn1.members(), ["cdn-1"]);
    assert_eq!(kvb.members(), ["kv-b"]);
    mark(&mut trace, "frozen");
    trace.push(format!("revision {}", kv2.revision.load(Ordering::Relaxed)));
    trace
}

/// Cell `eu-1a` places its CDN members' L2 ranges, and reassigns a killed member's after the grace period; `eu-1b`
/// keeps its own directory.
async fn placement() -> Vec<String> {
    let handle = Handle::current();
    let mut trace = Vec::new();
    let started = bedrock_lib::monotonic_now();
    spawn("api", API, || bedrock_api::run(api()));
    for (i, ip) in CELL_RELAYS.iter().enumerate() {
        let name = format!("relay-{}", i + 1);
        let config = relay(&name, ip, "eu-1a", &CELL_RELAYS);
        spawn(&name, ip, move || bedrock_relay::run(config.clone()));
    }
    let other = relay("relay-b", OTHER_RELAY, "eu-1b", &[OTHER_RELAY]);
    spawn("relay-b", OTHER_RELAY, move || {
        bedrock_relay::run(other.clone())
    });
    let relays: Vec<String> = CELL_RELAYS.iter().map(|ip| format!("{ip}:7000")).collect();
    let names = ["cdn-1", "cdn-2", "cdn-3", "cdn-4"];
    let leaves: Vec<(NodeHandle, Arc<Leaf>)> = names
        .iter()
        .zip(["10.0.2.31", "10.0.2.32", "10.0.2.33", "10.0.2.34"])
        .map(|(name, ip)| spawn_leaf(name, ip, "cdn", "eu-1a", relays.clone()))
        .collect();
    let (_, other) = spawn_leaf(
        "cdn-b",
        "10.0.2.35",
        "cdn",
        "eu-1b",
        vec![format!("{OTHER_RELAY}:7000")],
    );
    // Each handoff waits for its new owners' relays to report them ready, so rebalancing takes several passes and
    // can pass through placed states between moves. Wait until the directory stops changing.
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(120);
    let mut seen = (
        leaves[0].1.revision.load(Ordering::Relaxed),
        bedrock_lib::monotonic_now(),
    );
    loop {
        let now = bedrock_lib::monotonic_now();
        let revision = leaves[0].1.revision.load(Ordering::Relaxed);
        if revision != seen.0 {
            seen = (revision, now);
        }
        if now.duration_since(seen.1) >= Duration::from_secs(5)
            && leaves
                .iter()
                .all(|(_, leaf)| placed(&leaf.directory(), &names))
            && placed(&other.directory(), &["cdn-b"])
        {
            break;
        }
        assert!(now < deadline, "placement settled within 120s");
        sleep(Duration::from_millis(100)).await;
    }
    let ranges = leaves[0].1.directory().len();
    trace.push(format!(
        "{} placed {ranges} ranges",
        started.elapsed().as_millis()
    ));

    // A killed member keeps its ranges through the grace period, then they move to the others.
    handle.kill(leaves[3].0.id());
    sleep(GRACE - Duration::from_secs(1)).await;
    assert!(
        placed(&leaves[0].1.directory(), &names),
        "moved before the grace period"
    );
    until("reassigned", IDLE + GRACE, || {
        leaves[..3]
            .iter()
            .all(|(_, leaf)| placed(&leaf.directory(), &names[..3]))
    })
    .await;
    trace.push(format!("{} reassigned", started.elapsed().as_millis()));
    assert!(placed(&other.directory(), &["cdn-b"]));

    // Corruption quarantines cdn-1's dataset; every node's view shows it, then shows the recovery.
    let (storage, raw, engine) = leaves[0].1.storage.lock().unwrap().clone().unwrap();
    engine.put(b"k", b"v").unwrap();
    raw.corrupt(b"k");
    assert!(engine.get(b"k").is_err());
    until("quarantine published", Duration::from_secs(10), || {
        leaves[..3]
            .iter()
            .all(|(_, leaf)| leaf.unhealthy("cdn-1") == ["cdn"])
    })
    .await;
    trace.push(format!("{} quarantined", started.elapsed().as_millis()));
    assert!(storage.rebuilding("cdn", "engine"));
    raw.delete(b"k").unwrap();
    assert!(storage.recovered("cdn", "engine"));
    until("recovery published", Duration::from_secs(10), || {
        leaves[..3]
            .iter()
            .all(|(_, leaf)| leaf.unhealthy("cdn-1").is_empty())
    })
    .await;
    trace.push(format!("{} recovered", started.elapsed().as_millis()));
    assert!(placed(&leaves[0].1.directory(), &names[..3]));
    trace
}

const WRITER: &str = "10.0.3.50";
const NAMESPACES: [&str; 2] = ["pages", "workers"];
const KEYS: u64 = 24;

/// `(namespace, key)` → value.
type Values = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// The writer's record of what it committed, and a pause switch for data checks.
#[derive(Default)]
struct Model {
    values: Mutex<Values>,
    writes: AtomicU64,
    pause: AtomicBool,
    paused: AtomicBool,
}

/// One write through `bedrock-api`'s HTTP endpoint; `None` deletes.
async fn write(namespace: &str, key: &[u8], value: Option<&[u8]>, idempotency: u64) -> bool {
    let attempt = async {
        let stream = tokio::net::TcpStream::connect(format!("{API}:7002"))
            .await
            .ok()?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .ok()?;
        tokio::spawn(connection);
        let key = String::from_utf8(key.to_vec()).unwrap();
        let request = hyper::Request::builder()
            .method(if value.is_some() { "PUT" } else { "DELETE" })
            .uri(format!("/v1/cdn/{namespace}/{key}"))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", format!("handoff-{idempotency}"))
            .body(Full::new(Bytes::from(value.unwrap_or_default().to_vec())))
            .unwrap();
        let response = sender.send_request(request).await.ok()?;
        let ok = response.status().is_success();
        response.into_body().collect().await.ok()?;
        Some(ok)
    };
    tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .ok()
        .flatten()
        == Some(true)
}

/// Writes throughout: every 100 ms one key of one namespace is set, or every fifth time deleted. A failed write is
/// retried with the same idempotency key, so the record matches what was committed.
async fn writer(model: Arc<Model>) {
    loop {
        if model.pause.load(Ordering::SeqCst) {
            model.paused.store(true, Ordering::SeqCst);
            sleep(Duration::from_millis(50)).await;
            continue;
        }
        model.paused.store(false, Ordering::SeqCst);
        let n = model.writes.load(Ordering::SeqCst);
        let namespace = NAMESPACES[(n % 2) as usize];
        let key = format!("k{:02}", (n / 2 * 7) % KEYS).into_bytes();
        let value = (n % 5 != 4).then(|| format!("v{n}").into_bytes());
        while !write(namespace, &key, value.as_deref(), n).await {
            sleep(Duration::from_millis(200)).await;
        }
        {
            let mut values = model.values.lock().unwrap();
            match value {
                Some(value) => values.insert((namespace.into(), key), value),
                None => values.remove(&(namespace.into(), key)),
            };
        }
        model.writes.store(n + 1, Ordering::SeqCst);
        sleep(Duration::from_millis(100)).await;
    }
}

/// `(namespace, key)` → value of every key a holder stores, by its stored hash.
fn stored(engine: &MemoryEngine) -> BTreeMap<(String, Vec<u8>), (u64, Vec<u8>)> {
    engine
        .scan(&KeyRange::prefix(*b"l2/"))
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            let hash = u64::from_be_bytes(key[3..11].try_into().unwrap());
            let (namespace, key) = l2_parts(&key).unwrap();
            let namespace = String::from_utf8(namespace.to_vec()).unwrap();
            ((namespace, key.to_vec()), (hash, value))
        })
        .collect()
}

/// The directory's entries as `(start, end)` ranges with their owners; `end` 0 is 2⁶⁴.
fn entries(directory: &Directory) -> Vec<((u64, u64), Vec<bedrock_lib::placement::Owner>)> {
    let mut entries = directory.iter().peekable();
    let mut out = Vec::new();
    while let Some((&start, owners)) = entries.next() {
        let end = entries.peek().map_or(0, |(next, _)| **next);
        out.push(((start, end), owners.clone()));
    }
    out
}

fn within((start, end): (u64, u64), hash: u64) -> bool {
    hash >= start && (end == 0 || hash < end)
}

/// Whether, in `reference`'s directory, every `ready` holder in `holders` stores exactly the committed data of its
/// ranges and no holder stores a key outside the ranges it is listed for in any state.
fn follows_ownership(
    reference: &Leaf,
    holders: &[(&str, &Arc<Leaf>)],
    expected: &BTreeMap<(String, Vec<u8>), Vec<u8>>,
) -> Result<(), String> {
    let directory = entries(&reference.directory_of("cdn"));
    if directory.is_empty() {
        return Err("no directory".into());
    }
    for (name, leaf) in holders {
        let held = stored(leaf.data.as_ref().unwrap());
        for ((namespace, key), (hash, _)) in &held {
            if !directory.iter().any(|(range, owners)| {
                within(*range, *hash) && owners.iter().any(|o| o.node == *name)
            }) {
                return Err(format!(
                    "{name} keeps {namespace}/{key:?} outside its ranges"
                ));
            }
        }
        for (range, owners) in &directory {
            if !owners
                .iter()
                .any(|o| o.node == *name && o.state == State::Ready)
            {
                continue;
            }
            let mine = |hash: u64| within(*range, hash);
            let want: BTreeMap<_, _> = expected
                .iter()
                .filter(|((namespace, key), _)| mine(hash_of("cdn", namespace, key)))
                .collect();
            let have: BTreeMap<_, _> = held
                .iter()
                .filter(|(_, (hash, _))| mine(*hash))
                .map(|(at, (_, value))| (at, value))
                .collect();
            if want != have {
                return Err(format!("{name} differs from the writer in {range:x?}"));
            }
        }
    }
    Ok(())
}

/// Pauses the writer, then waits until data follows ownership in `reference`'s directory.
async fn check_data(what: &str, model: &Model, reference: &Leaf, holders: &[(&str, &Arc<Leaf>)]) {
    model.pause.store(true, Ordering::SeqCst);
    until("writer paused", Duration::from_secs(30), || {
        model.paused.load(Ordering::SeqCst)
    })
    .await;
    let expected = model.values.lock().unwrap().clone();
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(60);
    loop {
        match follows_ownership(reference, holders, &expected) {
            Ok(()) => break,
            Err(why) => assert!(
                bedrock_lib::monotonic_now() < deadline,
                "{what}: data does not follow ownership: {why}"
            ),
        }
        sleep(Duration::from_millis(200)).await;
    }
    model.pause.store(false, Ordering::SeqCst);
}

/// Every range of `reference`'s `cdn` directory has `min(3, members)` ready owners from `members`, and the
/// directory has not changed for 5 s.
async fn settled(what: &str, reference: &Leaf, members: &[&str]) {
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(180);
    let mut seen = (
        reference.revision.load(Ordering::Relaxed),
        bedrock_lib::monotonic_now(),
    );
    loop {
        let now = bedrock_lib::monotonic_now();
        let revision = reference.revision.load(Ordering::Relaxed);
        if revision != seen.0 {
            seen = (revision, now);
        }
        if now.duration_since(seen.1) >= Duration::from_secs(5)
            && placed(&reference.directory_of("cdn"), members)
        {
            return;
        }
        assert!(now < deadline, "{what}: placement settled within 180 s");
        sleep(Duration::from_millis(100)).await;
    }
}

/// A directory merge can become ready before its consumers join. Their stored parts must cover the whole range.
fn has_progress(revisions: &BTreeMap<String, u64>, namespace: &str, range: (u64, u64)) -> bool {
    let end = |value: u64| {
        if value == 0 {
            1u128 << 64
        } else {
            value.into()
        }
    };
    let mut covered = u128::from(range.0);
    // Revision names sort by their fixed-width range start, including overlapping remnants of a split.
    for (start, stop) in revisions.iter().filter_map(|(marker, value)| {
        if *value != 1 {
            return None;
        }
        let rest = marker.strip_prefix("relay:range:cdn:")?;
        let start = u64::from_str_radix(rest.get(..16)?, 16).ok()?;
        let stop = u64::from_str_radix(rest.get(17..33)?, 16).ok()?;
        (rest.get(34..)? == namespace).then_some((start, stop))
    }) {
        if u128::from(start) > covered {
            break;
        }
        covered = covered.max(end(stop));
        if covered >= end(range.1) {
            return true;
        }
    }
    false
}

#[test]
fn merged_range_progress_requires_gap_free_parts() {
    const HALF: u64 = 1 << 63;
    for (parts, range, expected) in [
        (vec![(0, HALF), (HALF, 0)], (0, 0), true),
        (vec![(0, HALF)], (0, 0), false),
        (vec![(0, HALF), (HALF + 1, 0)], (0, 0), false),
        (vec![(0, HALF + 1), (HALF, 0)], (0, 0), true),
        (vec![(0, 0)], (1, u64::MAX), true),
    ] {
        let revisions = parts
            .iter()
            .map(|(start, end)| (format!("relay:range:cdn:{start:016x}-{end:016x}:pages"), 1))
            .collect();
        assert_eq!(
            has_progress(&revisions, "pages", range),
            expected,
            "{parts:?}"
        );
        assert!(!has_progress(&revisions, "other", range));
    }
}

/// Runs throughout: no member's view lists a directory owner it never saw as a member (one revision order), and no
/// member's own view shows it `ready` for a range it holds no progress for in some namespace the writer has written
/// (no owner is ready before its copy). Another member's view may lag behind a revoke the owner already applied.
async fn invariants(leaves: Vec<(&'static str, Arc<Leaf>)>, model: Arc<Model>) {
    let mut seen: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    loop {
        sleep(Duration::from_millis(200)).await;
        let written: BTreeSet<String> = model
            .values
            .lock()
            .unwrap()
            .keys()
            .map(|(namespace, _)| namespace.clone())
            .collect();
        for (name, leaf) in &leaves {
            let Some(view) = leaf.view.lock().unwrap().clone() else {
                continue;
            };
            // The directory first: its owners were members at its revision or before.
            let (Ok(directory), Ok(members)) = (view.directory("cdn"), view.members()) else {
                continue;
            };
            let ever = seen.entry(name).or_default();
            ever.extend(members.into_keys());
            for (range, owners) in entries(&directory) {
                for owner in &owners {
                    assert!(
                        ever.contains(&owner.node),
                        "{name} sees owner {} it never saw as a member",
                        owner.node
                    );
                    if owner.state != State::Ready || owner.node != *name {
                        continue;
                    }
                    let revisions = leaf
                        .data
                        .as_ref()
                        .unwrap()
                        .snapshot()
                        .unwrap()
                        .revisions()
                        .unwrap();
                    for namespace in &written {
                        assert!(
                            has_progress(&revisions, namespace, range),
                            "{} is ready for {range:x?} without progress in {namespace}",
                            owner.node
                        );
                    }
                }
            }
        }
    }
}

/// #22 / B-21: data follows ownership through joins, failures, drains and relay faults, with writes running.
async fn replication() -> Vec<String> {
    let handle = Handle::current();
    let mut trace = Vec::new();
    let started = bedrock_lib::monotonic_now();
    let mark = |trace: &mut Vec<String>, event: &str, leaf: &Leaf| {
        trace.push(format!(
            "{} {event} at revision {} with {} ranges",
            started.elapsed().as_millis(),
            leaf.revision.load(Ordering::Relaxed),
            leaf.directory_of("cdn").len()
        ));
    };
    spawn("api", API, || bedrock_api::run(api()));
    let relays: Vec<NodeHandle> = CELL_RELAYS
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            let name = format!("relay-{}", i + 1);
            let config = relay(&name, ip, "eu-1a", &CELL_RELAYS);
            spawn(&name, ip, move || bedrock_relay::run(config.clone()))
        })
        .collect();
    let ordered = |first: usize| -> Vec<String> {
        (0..3)
            .map(|i| format!("{}:7000", CELL_RELAYS[(first + i) % 3]))
            .collect()
    };
    let model = Arc::new(Model::default());
    let writing = model.clone();
    Handle::current()
        .create_node()
        .name("writer")
        .ip(WRITER.parse().unwrap())
        .init(move || writer(writing.clone()))
        .build();
    let (_, q1) = spawn_holder("holder-1", "10.0.3.21", ordered(0));
    let (q2_node, q2) = spawn_holder("holder-2", "10.0.3.22", ordered(1));
    let (q3_node, q3) = spawn_holder("holder-3", "10.0.3.23", ordered(2));
    let all = |holders: &[(&'static str, &Arc<Leaf>)]| -> Vec<(&'static str, Arc<Leaf>)> {
        holders.iter().map(|(n, l)| (*n, (*l).clone())).collect()
    };

    settled(
        "initial placement",
        &q1,
        &["holder-1", "holder-2", "holder-3"],
    )
    .await;
    check_data(
        "initial placement",
        &model,
        &q1,
        &[("holder-1", &q1), ("holder-2", &q2), ("holder-3", &q3)],
    )
    .await;
    mark(&mut trace, "placed", &q1);

    // A joining member takes ranges; the others hand theirs off and drop what they no longer own.
    let (q4_node, q4) = spawn_holder("holder-4", "10.0.3.24", ordered(0));
    let watched = all(&[
        ("holder-1", &q1),
        ("holder-2", &q2),
        ("holder-3", &q3),
        ("holder-4", &q4),
    ]);
    tokio::spawn(invariants(watched, model.clone()));
    let four = ["holder-1", "holder-2", "holder-3", "holder-4"];
    settled("join", &q1, &four).await;
    assert!(
        entries(&q1.directory_of("cdn"))
            .iter()
            .any(|(_, owners)| owners.iter().any(|o| o.node == "holder-4")),
        "the joining member took no range"
    );
    let holders = [
        ("holder-1", &q1),
        ("holder-2", &q2),
        ("holder-3", &q3),
        ("holder-4", &q4),
    ];
    check_data("join", &model, &q1, &holders).await;
    mark(&mut trace, "joined", &q1);

    // A relay failover within the grace period moves nothing.
    let before = q2.directory_of("cdn");
    handle.kill(relays[0].id());
    sleep(GRACE - Duration::from_secs(2)).await;
    assert_eq!(
        q2.directory_of("cdn"),
        before,
        "relay failover moved ranges"
    );
    handle.restart(relays[0].id());
    settled("relay failover", &q2, &four).await;
    check_data("relay failover", &model, &q2, &holders).await;
    mark(&mut trace, "relay failover", &q2);

    // A member restarted within the grace period keeps its ranges and resumes them.
    let before = q1.directory_of("cdn");
    handle.kill(q2_node.id());
    sleep(Duration::from_secs(3)).await;
    handle.restart(q2_node.id());
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        q1.directory_of("cdn"),
        before,
        "a restart within grace moved ranges"
    );
    check_data("restart", &model, &q1, &holders).await;
    mark(&mut trace, "restarted", &q1);

    // A member cut off from every relay loses its ranges after the grace period and takes ranges again once healed.
    let network = madsim::net::NetSim::current();
    let relay_ids: Vec<_> = relays.iter().map(NodeHandle::id).collect();
    for relay in &relay_ids {
        network.clog_link(q3_node.id(), *relay);
        network.clog_link(*relay, q3_node.id());
    }
    settled("cut off", &q1, &["holder-1", "holder-2", "holder-4"]).await;
    check_data(
        "cut off",
        &model,
        &q1,
        &[("holder-1", &q1), ("holder-2", &q2), ("holder-4", &q4)],
    )
    .await;
    mark(&mut trace, "cut off", &q1);
    for relay in &relay_ids {
        network.unclog_link(q3_node.id(), *relay);
        network.unclog_link(*relay, q3_node.id());
    }
    settled("healed", &q1, &four).await;
    check_data("healed", &model, &q1, &holders).await;
    mark(&mut trace, "healed", &q1);

    // A killed member's ranges are rebuilt elsewhere after the grace period. The publishing relay dies mid-handoff, and
    // its siblings finish from published state (#41 / B-12b).
    handle.kill(q4_node.id());
    let three = ["holder-1", "holder-2", "holder-3"];
    until("a handoff to start", IDLE + GRACE, || {
        entries(&q1.directory_of("cdn"))
            .iter()
            .any(|(_, owners)| owners.iter().any(|o| o.state == State::Populating))
    })
    .await;
    handle.kill(relays[0].id());
    settled("killed", &q1, &three).await;
    handle.restart(relays[0].id());
    check_data(
        "killed",
        &model,
        &q1,
        &[("holder-1", &q1), ("holder-2", &q2), ("holder-3", &q3)],
    )
    .await;
    mark(&mut trace, "killed", &q1);

    // A draining member gets nothing new and leaves at once; the others take its ranges.
    q3.drain.notify_one();
    settled("drained", &q1, &["holder-1", "holder-2"]).await;
    check_data(
        "drained",
        &model,
        &q1,
        &[("holder-1", &q1), ("holder-2", &q2)],
    )
    .await;
    mark(&mut trace, "drained", &q1);

    // Without a relay majority nothing moves or is revoked, even when a member returns.
    handle.kill(relays[1].id());
    handle.kill(relays[2].id());
    let before = q1.directory_of("cdn");
    let revision = q1.revision.load(Ordering::Relaxed);
    handle.restart(q4_node.id());
    sleep(GRACE + IDLE).await;
    assert_eq!(
        q1.directory_of("cdn"),
        before,
        "placement moved without a majority"
    );
    assert_eq!(
        q1.revision.load(Ordering::Relaxed),
        revision,
        "a write happened without a majority"
    );
    mark(&mut trace, "frozen", &q1);
    handle.restart(relays[1].id());
    handle.restart(relays[2].id());
    settled("majority back", &q1, &["holder-1", "holder-2", "holder-4"]).await;
    check_data(
        "majority back",
        &model,
        &q1,
        &[("holder-1", &q1), ("holder-2", &q2), ("holder-4", &q4)],
    )
    .await;
    mark(&mut trace, "majority back", &q1);
    trace.push(format!("{} writes", model.writes.load(Ordering::SeqCst)));
    trace
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run<F: Future<Output = Vec<String>> + 'static>(seed: u64, scenario: fn() -> F) -> Vec<String> {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_MEMBERSHIP_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test membership -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_MEMBERSHIP_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42])
}

#[test]
fn membership_follows_relay_streams_and_replays_per_seed() {
    for seed in seeds() {
        let first = run(seed, scenario);
        assert_eq!(first, run(seed, scenario), "seed {seed} changed its trace");
        println!("membership seed {seed}: {first:?}");
    }
}

#[test]
fn data_follows_ownership_through_faults_and_replays_per_seed() {
    for seed in seeds() {
        let first = run(seed, replication);
        assert_eq!(
            first,
            run(seed, replication),
            "seed {seed} changed its trace"
        );
        println!("replication seed {seed}: {first:?}");
    }
}

#[test]
fn placement_follows_membership_and_replays_per_seed() {
    for seed in seeds() {
        let first = run(seed, placement);
        assert_eq!(first, run(seed, placement), "seed {seed} changed its trace");
        println!("placement seed {seed}: {first:?}");
    }
}
