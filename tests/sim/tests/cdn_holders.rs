//! #82 / B-34 acceptance over the real entry points: after a publish writes ref keys through the API, each artifact is
//! held, verified, by exactly two of the cell's four `bedrock-cdn` nodes, the owners of its range, and a held-only ask
//! of any other node finds nothing. Runs the real relay, API and CDN entry points. Replay one seed with
//! `BEDROCK_CDN_HOLDERS_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_cdn::artifact_storage::{Artifacts, Hash, Memory, Origin};
use bedrock_cdn::artifact_transfer::Transfer;
use bedrock_cdn::paths::ref_key;
use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection;
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory};
use bedrock_lib::transport::Peers;
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::time::sleep;

const API: &str = "10.0.6.1";
const RELAYS: [&str; 3] = ["10.0.6.11", "10.0.6.12", "10.0.6.13"];
const HOLDERS: [&str; 4] = ["10.0.6.21", "10.0.6.22", "10.0.6.23", "10.0.6.24"];
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const ARTIFACTS: usize = 8;

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
            endpoint: "http://10.0.6.99:9000".into(),
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
        // `cdn` places two owners per range by default.
        membership: Some(MembershipSettings {
            voter_ids: RELAYS
                .iter()
                .map(|peer| (format!("{peer}:7000"), (*peer).into()))
                .collect(),
            bootstrap: true,
            grace_ms: 20_000,
            replicas: Default::default(),
            max_moving: Default::default(),
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn holder(name: &str, ip: &str) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip),
        roles: vec![Role::L2],
        datasets: vec!["cdn".into()],
        relays: RELAYS.iter().map(|ip| format!("{ip}:7000")).collect(),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.6.99:9000".into(),
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

/// One ref key written through the API, retried with the same idempotency key until it commits.
async fn retain(sha256: &Hash) {
    let key = String::from_utf8(ref_key(sha256, "d1").unwrap()).unwrap();
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(60);
    loop {
        let request = hyper::Request::put(format!("/v1/cdn/deployments/{key}"))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", format!("cdn-holders-{key}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let sent = bedrock_lib::http::send(
            &format!("{API}:7002"),
            None,
            request,
            Duration::from_secs(5),
        )
        .await;
        if sent.is_ok_and(|(status, _)| status.is_success()) {
            return;
        }
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "retain {key} within 60 s"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

fn artifact(i: usize) -> Bytes {
    format!("artifact {i}").repeat(1000).into()
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
    for (i, ip) in HOLDERS.iter().enumerate() {
        let config = holder(&format!("cdn-{}", i + 1), ip);
        spawn(&config.node.node_id.clone(), ip, move || {
            bedrock_cdn::run(config.clone())
        });
    }
    handle
        .create_node()
        .name("client")
        .ip("10.0.6.90".parse().unwrap())
        .build()
        .spawn(check())
        .await
        .unwrap()
}

async fn check() -> Vec<String> {
    let started = bedrock_lib::monotonic_now();
    let mut trace = Vec::new();
    // A client that only asks holders for their copies; its own S3 is empty.
    let admin = Admin::new();
    let asker = Transfer::new(
        Arc::new(Artifacts::new(
            Arc::new(Memory::default()),
            Arc::new(Memory::default()),
            StorageHealth::new(&admin),
        )),
        Arc::new(Peers::new(&admin)),
        &protection::Metrics::new(&admin),
    );
    // The publish uploads first (B-33), then retains.
    let s3 = Memory::shared();
    let mut hashes = Vec::new();
    for i in 0..ARTIFACTS {
        let bytes = artifact(i);
        let sha256 = Hash::of(&bytes);
        s3.put(&sha256, bytes).await.unwrap();
        hashes.push(sha256);
    }
    for sha256 in &hashes {
        retain(sha256).await;
    }
    trace.push(format!("{} retained", started.elapsed().as_millis()));

    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(180);
    for (i, sha256) in hashes.iter().enumerate() {
        loop {
            let mut held = Vec::new();
            for ip in HOLDERS {
                // Held-only: a node without a verified copy answers nothing and reads no S3.
                if let Ok(Some(bytes)) = asker.held(&format!("{ip}:7000"), sha256).await {
                    assert_eq!(bytes, artifact(i));
                    held.push(ip);
                }
            }
            assert!(
                held.len() <= 2,
                "only the two owners hold {sha256}: {held:?}"
            );
            if held.len() == 2 {
                trace.push(format!(
                    "{} {i} held by {held:?}",
                    started.elapsed().as_millis()
                ));
                break;
            }
            assert!(
                bedrock_lib::monotonic_now() < deadline,
                "two verified holders of {sha256}; now {held:?}"
            );
            sleep(Duration::from_millis(500)).await;
        }
    }
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
            "Replay: BEDROCK_CDN_HOLDERS_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test cdn_holders -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn every_retained_artifact_has_two_verified_holders_and_replays_per_seed() {
    let seeds = std::env::var("BEDROCK_CDN_HOLDERS_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42]);
    for seed in seeds {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("cdn_holders seed {seed}: {first:?}");
    }
}
