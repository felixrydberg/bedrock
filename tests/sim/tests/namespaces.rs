//! #56 / B-25 acceptance: customer-KV namespaces managed through `bedrock-kv` and read from Quicksilver range owners
//! through the shared client, in two cells. Every cell resolves the same record and storage identity, an empty
//! namespace creates no database, concurrent creates commit through conditional retries, deletion is visible at the
//! revision it returned, and a killed holder or a stale routing table only costs a retry. Replay one seed with
//! `BEDROCK_NAMESPACES_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_kv::KvConfig;
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
use serde_json::Value;
use tokio::time::sleep;

const API: &str = "10.0.3.1";
const TOKEN: &str = "simulation-writer-token";
/// Per cell: relays, Quicksilver owners and one kv node serving management on port 7003.
const CELLS: [(&str, [&str; 3], [&str; 2], &str); 2] = [
    (
        "eu-1a",
        ["10.0.3.11", "10.0.3.12", "10.0.3.13"],
        ["10.0.3.21", "10.0.3.22"],
        "10.0.3.31",
    ),
    (
        "eu-1b",
        ["10.0.3.41", "10.0.3.42", "10.0.3.43"],
        ["10.0.3.51", "10.0.3.52"],
        "10.0.3.61",
    ),
];

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
            endpoint: "http://10.0.3.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn relay(ip: &str, cell: &str, relays: &[&str]) -> RelayConfig {
    let mut relay_node = node(ip, ip, cell);
    relay_node.node_id = ip.into();
    RelayConfig {
        node: relay_node,
        tier: Tier::Cell,
        upstreams: vec![format!("{API}:7000")],
        replay_peers: relays
            .iter()
            .filter(|peer| **peer != ip)
            .map(|peer| format!("{peer}:7000"))
            .collect(),
        replay: Default::default(),
        membership: Some(MembershipSettings {
            voter_ids: relays
                .iter()
                .map(|peer| (format!("{peer}:7000"), (*peer).into()))
                .collect(),
            bootstrap: true,
            grace_ms: 20_000,
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn service(name: &str, ip: &str, cell: &str, relays: &[&str], datasets: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip, cell),
        roles: vec![Role::L2],
        datasets: datasets.iter().map(|d| (*d).into()).collect(),
        relays: relays.iter().map(|ip| format!("{ip}:7000")).collect(),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.3.99:9000".into(),
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

/// One management request to the kv node at `ip`, from a client process.
async fn call(ip: &str, method: &str, path: &str, body: &str) -> (u16, Value) {
    let request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "kv")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap();
    let address = format!("{ip}:7003");
    match bedrock_lib::http::send(&address, None, request, Duration::from_secs(30)).await {
        Ok((status, body)) => (
            status.as_u16(),
            serde_json::from_slice(&body)
                .unwrap_or(Value::String(String::from_utf8_lossy(&body).into())),
        ),
        Err(error) => (0, Value::String(error)),
    }
}

/// Retries `request` until it answers `status`, as an operator would while the cluster settles.
async fn until<F: Future<Output = (u16, Value)>>(
    what: &str,
    within: Duration,
    status: u16,
    request: impl Fn() -> F,
) -> Value {
    let deadline = bedrock_lib::monotonic_now() + within;
    loop {
        let (got, body) = request().await;
        if got == status {
            return body;
        }
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "{what} within {within:?}: last {got} {body}"
        );
        sleep(Duration::from_millis(500)).await;
    }
}

fn create(account: &str, title: &str) -> String {
    serde_json::json!({ "account": account, "title": title, "home_region": "eu", "storage_bytes": 1_000_000 })
        .to_string()
}

/// The record without the revision it was read at, which differs between reads.
fn record(body: &Value) -> Value {
    let mut record = body.clone();
    record.as_object_mut().unwrap().remove("revision");
    record
}

async fn scenario() -> Vec<String> {
    let handle = Handle::current();
    let mut trace = Vec::new();
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        Fingerprint::new(),
    ));
    let databases = Arc::new(MemoryFactory::default());
    let factory = databases.clone();
    install(Hooks {
        engine: Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || factory.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    spawn("api", API, || bedrock_api::run(api()));
    let mut holders = Vec::new();
    for (cell, relays, quicksilvers, kv) in CELLS {
        for ip in relays {
            let config = relay(ip, cell, &relays);
            spawn(&format!("relay-{ip}"), ip, move || {
                bedrock_relay::run(config.clone())
            });
        }
        for (i, ip) in quicksilvers.iter().enumerate() {
            let name = format!("qs-{cell}-{i}");
            let config = service(&name, ip, cell, &relays, &["quicksilver"]);
            holders.push(spawn(&name, ip, move || {
                bedrock_quicksilver::run(config.clone())
            }));
        }
        let config = KvConfig {
            manage_listen: Some(format!("{kv}:7003").parse().unwrap()),
            write_token: Some(TOKEN.into()),
            ..KvConfig::from(service(&format!("kv-{cell}"), kv, cell, &relays, &[]))
        };
        spawn(&format!("kv-{cell}"), kv, move || {
            bedrock_kv::run(config.clone())
        });
    }
    let [(_, _, _, kv_a), (_, _, _, kv_b)] = CELLS;
    let client = handle
        .create_node()
        .name("client")
        .ip("10.0.3.90".parse().unwrap())
        .build();
    let trace = client
        .spawn(async move {
            // EXPERIMENT: one infrastructure write before any read.
            let request = hyper::Request::put("/v1/quicksilver/infrastructure/seed")
                .header("host", "api")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Full::new(Bytes::from_static(b"x")))
                .unwrap();
            let put = bedrock_lib::http::send(
                &format!("{API}:7002"),
                None,
                request,
                Duration::from_secs(30),
            )
            .await;
            eprintln!("EXPERIMENT seed write: {put:?}");
            // Creating waits until cell a's Quicksilver ranges are placed and ready.
            let created = until("first create", Duration::from_secs(180), 201, || async {
                call(
                    kv_a,
                    "POST",
                    "/v1/namespaces",
                    &create("customer-123", "store-A"),
                )
                .await
            })
            .await;
            let id = created["id"].as_str().unwrap().to_owned();
            let revision = created["revision"].as_u64().unwrap();
            assert_eq!(created["storage"], format!("kv/{id}"));
            assert_eq!(created["state"], "active");
            trace.push(format!("created {id} at {revision}"));

            // An empty namespace is only its record: the writer opened no database for it.
            let opened = databases.namespaces().await.unwrap();
            assert!(
                !opened.iter().any(|(dataset, _)| dataset == "kv"),
                "{opened:?}"
            );

            // Both cells resolve the same record and storage identity once they hold that revision.
            let path = format!("/v1/namespaces/{id}?min_revision={revision}");
            let in_a = until("cell a read", Duration::from_secs(60), 200, || {
                call(kv_a, "GET", &path, "")
            })
            .await;
            let in_b = until("cell b read", Duration::from_secs(60), 200, || {
                call(kv_b, "GET", &path, "")
            })
            .await;
            assert_eq!(record(&in_a), record(&created));
            assert_eq!(record(&in_b), record(&created));

            // Concurrent creates in both cells all commit, each with its own ID, through conditional retries.
            let creates = (0..3).map(|i| {
                let kv = [kv_a, kv_b][i % 2];
                tokio::spawn(async move {
                    call(
                        kv,
                        "POST",
                        "/v1/namespaces",
                        &create("customer-456", &format!("store-{i}")),
                    )
                    .await
                })
            });
            let mut ids = vec![id.clone()];
            let mut head = revision;
            for created in creates.collect::<Vec<_>>() {
                let (status, body) = created.await.unwrap();
                assert_eq!(status, 201, "{body}");
                ids.push(body["id"].as_str().unwrap().to_owned());
                head = head.max(body["revision"].as_u64().unwrap());
            }
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 4);
            trace.push(format!("concurrent creates through {head}"));

            // Listing in the other cell returns every namespace in ID order, filtered by account.
            let listed = until("list", Duration::from_secs(60), 200, || async {
                let (status, body) = call(kv_b, "GET", "/v1/namespaces", "").await;
                let count = body["namespaces"].as_array().map_or(0, Vec::len);
                if count == ids.len() {
                    (status, body)
                } else {
                    (0, body)
                }
            })
            .await;
            let listed: Vec<_> = listed["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n["id"].as_str().unwrap().to_owned())
                .collect();
            assert_eq!(listed, ids);
            let (_, mine) = call(kv_b, "GET", "/v1/namespaces?account=customer-123", "").await;
            assert_eq!(mine["namespaces"].as_array().unwrap().len(), 1);

            // Deleting in cell b is visible in cell a at the revision it returned.
            let target = format!("/v1/namespaces/{id}");
            let deleted = until("delete", Duration::from_secs(60), 200, || {
                call(kv_b, "DELETE", &target, "")
            })
            .await;
            assert_eq!(deleted["state"], "deleting");
            let at = deleted["revision"].as_u64().unwrap();
            let path = format!("/v1/namespaces/{id}?min_revision={at}");
            let seen = until("deletion seen", Duration::from_secs(60), 200, || {
                call(kv_a, "GET", &path, "")
            })
            .await;
            assert_eq!(seen["state"], "deleting");
            trace.push(format!("deleted at {at}"));
            // Deleting again changes nothing; an unknown ID is not found.
            let again = call(kv_a, "DELETE", &format!("/v1/namespaces/{id}"), "").await;
            assert_eq!(
                (again.0, &again.1["state"]),
                (200, &Value::from("deleting"))
            );
            let unknown = call(
                kv_a,
                "GET",
                &format!("/v1/namespaces/{}", "0".repeat(32)),
                "",
            )
            .await;
            assert_eq!(unknown.0, 404);
            // A wrong token never reaches the directory.
            let request = hyper::Request::get("/v1/namespaces")
                .header("host", "kv")
                .header("authorization", "Bearer wrong")
                .body(Full::new(Bytes::new()))
                .unwrap();
            let (status, _) = bedrock_lib::http::send(
                &format!("{kv_a}:7003"),
                None,
                request,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(status.as_u16(), 401);
            (trace, id)
        })
        .await
        .unwrap();
    let (mut trace, id) = trace;

    // A killed holder in cell a: reads go to its sibling owner, then to a fresher table once ranges move.
    handle.kill(holders[0].id());
    let path = format!("/v1/namespaces/{id}");
    let client = handle
        .create_node()
        .name("client-2")
        .ip("10.0.3.91".parse().unwrap())
        .build();
    let state = client
        .spawn(async move {
            let found = until(
                "read after a holder died",
                Duration::from_secs(60),
                200,
                || call(kv_a, "GET", &path, ""),
            )
            .await;
            found["state"].as_str().unwrap().to_owned()
        })
        .await
        .unwrap();
    assert_eq!(state, "deleting");
    trace.push("read survived a killed holder".into());
    trace
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> Vec<String> {
    std::thread::spawn(move || Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario()))
        .join()
        .unwrap_or_else(|failure| {
            eprintln!(
                "Replay: BEDROCK_NAMESPACES_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test namespaces -- --nocapture"
            );
            std::panic::resume_unwind(failure)
        })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_NAMESPACES_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42])
}

#[test]
fn namespaces_resolve_alike_in_every_cell_and_replay_per_seed() {
    for seed in seeds() {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("namespaces seed {seed}: {first:?}");
    }
}
