//! #15 / B-12 acceptance: cell membership from relay streams, through the real `bedrock-api` and `bedrock-relay`
//! entry points. Replay one seed with `BEDROCK_MEMBERSHIP_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::membership::{self, View};
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Tier,
};
use bedrock_lib::relays::client::Session;
use bedrock_lib::replication::pb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};

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

/// What a test reads from a leaf process, across its restarts.
#[derive(Default)]
struct Leaf {
    view: Mutex<Option<Arc<View>>>,
    revision: AtomicU64,
    drain: Notify,
}

impl Leaf {
    fn members(&self) -> Vec<String> {
        self.view
            .lock()
            .unwrap()
            .as_ref()
            .map(|view| view.members().unwrap().into_keys().collect())
            .unwrap_or_default()
    }
}

fn spawn_leaf(
    name: &'static str,
    ip: &str,
    service: &'static str,
    cell: &'static str,
    relays: Vec<String>,
) -> (NodeHandle, Arc<Leaf>) {
    let leaf = Arc::new(Leaf::default());
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
                let peers = Arc::new(Peers::new(&Admin::new()));
                let member = pb::Member {
                    node_id: name.into(),
                    service: service.into(),
                    region: "eu".into(),
                    dc: "eu-1".into(),
                    cell: cell.into(),
                    status: pb::member::Status::Ready as i32,
                    address,
                    ..Default::default()
                };
                let session = Session::join(peers.clone(), relays, vec![target], member).unwrap();
                let namespace = membership::namespace(service, cell);
                loop {
                    tokio::select! {
                        () = state.drain.notified() => break,
                        () = sleep(Duration::from_millis(100)) => {
                            if let Some(revision) = session.applied(membership::DATASET, &namespace) {
                                state.revision.store(revision, Ordering::Relaxed);
                            }
                            view.refresh(&session, &peers).unwrap();
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
    let deadline = Instant::now() + within;
    while !ready() {
        assert!(Instant::now() < deadline, "{what} within {within:?}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// The view must not change at all for `period`.
async fn steady(what: &str, leaf: &Leaf, period: Duration, members: &[&str]) {
    // The leaf samples its applied revision every 100 ms.
    sleep(Duration::from_millis(200)).await;
    let revision = leaf.revision.load(Ordering::Relaxed);
    let deadline = Instant::now() + period;
    while Instant::now() < deadline {
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
    let started = Instant::now();
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

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> Vec<String> {
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

#[test]
fn membership_follows_relay_streams_and_replays_per_seed() {
    let seeds = std::env::var("BEDROCK_MEMBERSHIP_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42]);
    for seed in seeds {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("membership seed {seed}: {first:?}");
    }
}
