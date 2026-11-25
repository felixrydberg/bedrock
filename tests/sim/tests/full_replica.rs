//! #87 / B-40 acceptance: a cell's relays report `full_replica` completeness of `quicksilver`. Killing an owner reports
//! the cell degraded at once and through the grace period, until its ranges are rebuilt on the others; then coverage and
//! redundancy are complete again. Runs the real relay, API and Quicksilver entry points. Replay one seed with
//! `BEDROCK_FULL_REPLICA_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory};
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::time::sleep;

const API: &str = "10.0.5.1";
const RELAYS: [&str; 3] = ["10.0.5.11", "10.0.5.12", "10.0.5.13"];
const OWNERS: [&str; 4] = ["10.0.5.21", "10.0.5.22", "10.0.5.23", "10.0.5.24"];
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const GRACE: Duration = Duration::from_secs(20);

fn node(name: &str, ip: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: "eu-1".into(),
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

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        bulk_max_bytes: bedrock_lib::node::DEFAULT_BULK_MAX_BYTES,
        s3: S3Config {
            endpoint: "http://10.0.5.99:9000".into(),
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
        upstreams: vec![format!("{API}:7000")],
        replay_peers: RELAYS
            .iter()
            .filter(|peer| **peer != ip)
            .map(|peer| format!("{peer}:7000"))
            .collect(),
        replay: Default::default(),
        membership: Some(MembershipSettings {
            voter_ids: RELAYS
                .iter()
                .map(|peer| (format!("{peer}:7000"), (*peer).into()))
                .collect(),
            bootstrap: true,
            grace_ms: GRACE.as_millis() as u64,
            volume_replicas: 3,
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn owner(name: &str, ip: &str) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip),
        roles: vec![Role::L2],
        datasets: vec!["quicksilver".into()],
        relays: RELAYS.iter().map(|ip| format!("{ip}:7000")).collect(),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.5.99:9000".into(),
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
            let run = start();
            async move { run.await.unwrap() }
        })
        .build()
}

/// The `quicksilver` completeness gauges a relay reports: `(complete, redundant, degraded)`, `None` before its first
/// report or while it cannot be reached.
async fn report(relay: &str) -> Option<(f64, f64, bool)> {
    let request = hyper::Request::get("/metrics")
        .header("host", "admin")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let (_, body) = bedrock_lib::http::send(
        &format!("{relay}:7001"),
        None,
        request,
        Duration::from_secs(5),
    )
    .await
    .ok()?;
    let page = String::from_utf8(body.to_vec()).ok()?;
    let gauge = |name: &str| -> Option<f64> {
        page.lines()
            .filter(|line| line.starts_with(name) && line.contains(r#"dataset="quicksilver""#))
            .find_map(|line| line.rsplit(' ').next()?.parse().ok())
    };
    Some((
        gauge("bedrock_full_replica_complete_ratio")?,
        gauge("bedrock_full_replica_redundant_ratio")?,
        gauge("bedrock_full_replica_degraded")? == 1.0,
    ))
}

async fn until(what: &str, within: Duration, done: impl Fn(Option<(f64, f64, bool)>) -> bool) {
    let deadline = bedrock_lib::monotonic_now() + within;
    loop {
        let last = report(RELAYS[0]).await;
        if done(last) {
            return;
        }
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "{what} within {within:?}; last report {last:?}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

/// One `quicksilver` write through the API, retried with the same idempotency key until it commits.
async fn write(key: &str) {
    let within = Duration::from_secs(60);
    let deadline = bedrock_lib::monotonic_now() + within;
    let idempotency = format!("full-replica-{key}");
    loop {
        let request = hyper::Request::put(format!("/v1/quicksilver/infrastructure/{key}"))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", &idempotency)
            .body(Full::new(Bytes::from(key.as_bytes().to_vec())))
            .unwrap();
        let last = match bedrock_lib::http::send(
            &format!("{API}:7002"),
            None,
            request,
            Duration::from_secs(5),
        )
        .await
        {
            Ok((status, _)) if status.is_success() => return,
            Ok((status, _)) => format!("HTTP {status}"),
            Err(error) => error,
        };
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "write {key} (idempotency key {idempotency}) within {within:?}; last {last}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

async fn scenario() -> Vec<String> {
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        Fingerprint::new(),
    ));
    let namespaces = Arc::new(MemoryFactory::default());
    install(Hooks {
        engine: Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || namespaces.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    let handle = Handle::current();
    spawn("api", API, || bedrock_api::run(api()));
    for ip in RELAYS {
        spawn(ip, ip, move || bedrock_relay::run(relay(ip)));
    }
    let owners: Vec<NodeHandle> = OWNERS
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            let config = owner(&format!("quicksilver-{}", i + 1), ip);
            spawn(&config.node.node_id.clone(), ip, move || {
                bedrock_quicksilver::run(config.clone())
            })
        })
        .collect();
    let victim = owners[3].id();
    // Admin pages and the API are reached from a client process.
    let client = handle
        .create_node()
        .name("client")
        .ip("10.0.5.90".parse().unwrap())
        .build();
    client
        .spawn(async move { check(handle, victim).await })
        .await
        .unwrap()
}

async fn check(handle: Handle, victim: madsim::task::NodeId) -> Vec<String> {
    let started = bedrock_lib::monotonic_now();
    let mut trace = Vec::new();
    let mut mark = |event: &str| trace.push(format!("{} {event}", started.elapsed().as_millis()));
    let complete = |r: Option<(f64, f64, bool)>| r == Some((1.0, 1.0, false));

    until(
        "four owners cover every range three times",
        Duration::from_secs(120),
        complete,
    )
    .await;
    mark("complete");
    for key in ["routing/a", "routing/b", "ownership/c", "config/d"] {
        write(key).await;
    }
    mark("written");

    // A killed owner stops holding at once: its ranges lose a copy, but every key is still held.
    handle.kill(victim);
    let killed = bedrock_lib::monotonic_now();
    until("degraded after the kill", Duration::from_secs(10), |r| {
        r.is_some_and(|(complete, _, degraded)| complete == 1.0 && degraded)
    })
    .await;
    mark("degraded");

    // Nothing is rebuilt within the grace period, so the cell stays degraded.
    sleep(
        (killed + GRACE - Duration::from_secs(2))
            .saturating_duration_since(bedrock_lib::monotonic_now()),
    )
    .await;
    let (_, redundant, degraded) = report(RELAYS[0]).await.unwrap();
    assert!(
        degraded && redundant < 1.0,
        "degraded through the grace period"
    );
    mark("still degraded");

    // After the grace period the others rebuild its ranges, and the cell is complete again.
    until(
        "rebuilt on the other owners",
        GRACE + Duration::from_secs(120),
        |r| complete(r) && bedrock_lib::monotonic_now() >= killed + GRACE,
    )
    .await;
    mark("rebuilt");
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
            "Replay: BEDROCK_FULL_REPLICA_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test full_replica -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn a_killed_owner_is_degraded_until_rebuilt_and_replays_per_seed() {
    let seeds = std::env::var("BEDROCK_FULL_REPLICA_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42]);
    for seed in seeds {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("full_replica seed {seed}: {first:?}");
    }
}
