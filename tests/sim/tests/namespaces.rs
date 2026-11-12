//! #56 / B-25 acceptance: customer-KV namespaces managed through `bedrock-kv`'s Cloudflare-compatible API and read
//! from Quicksilver range owners through the shared client, in two cells. Every cell resolves the same namespace, an
//! empty namespace creates no database, titles are unique per tenant, concurrent creates commit through conditional
//! retries, renames and deletions are visible at the revision they returned, and a killed holder or a stale routing
//! table only costs a retry. Replay one seed with
//! `BEDROCK_NAMESPACES_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_kv::KvConfig;
use bedrock_kv::namespaces::{
    CACHE_TTL_SECONDS, FORMAT_VERSION, Lifecycle, PURGE_GRACE, Record, storage as storage_of,
};
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
use prost::Message;
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
            dc: "arn1".into(),
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

/// Cloudflare's namespace collection of `tenant`.
fn namespaces(tenant: &str) -> String {
    format!("/client/v4/accounts/{tenant}/storage/kv/namespaces")
}

fn create(title: &str) -> String {
    serde_json::json!({ "title": title, "storage_bytes": 1_000_000 }).to_string()
}

/// The namespace an answer carries, without the revision it was read at, which differs between reads.
fn record(body: &Value) -> Value {
    assert_eq!(body["success"], true, "{body}");
    let mut record = body["result"].clone();
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
            let mine = namespaces("customer-123");
            let theirs = namespaces("customer-456");
            // Nothing was ever written to `infrastructure`: owners prove its revision 0, so the very first create works.
            // Creating waits until cell a's Quicksilver ranges are placed and ready.
            let first = create("store-A");
            let created = until("first create", Duration::from_secs(180), 200, || {
                call(kv_a, "POST", &mine, &first)
            })
            .await;
            let id = created["result"]["id"].as_str().unwrap().to_owned();
            let revision = created["result"]["revision"].as_u64().unwrap();
            assert_eq!(revision, 1, "the first directory write of a fresh cluster");
            assert_eq!(
                record(&created),
                serde_json::json!({ "id": id, "title": "store-A", "jurisdiction": "eu" })
            );
            trace.push(format!("created {id} at {revision}"));

            // An empty namespace is only its record: the writer opened no database for it.
            let opened = databases.namespaces().await.unwrap();
            assert!(
                !opened.iter().any(|(dataset, _)| dataset == "kv"),
                "{opened:?}"
            );

            // Both cells resolve the same namespace once they hold that revision.
            let path = format!("{mine}/{id}?min_revision={revision}");
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
            // Another tenant does not see it, and a tenant's titles are unique.
            let foreign = call(kv_a, "GET", &format!("{theirs}/{id}"), "").await;
            assert_eq!(foreign.0, 404, "{}", foreign.1);
            let duplicate = call(kv_b, "POST", &mine, &create("store-A")).await;
            assert_eq!(duplicate.0, 400, "{}", duplicate.1);
            assert_eq!(duplicate.1["success"], false);

            // Concurrent creates in both cells all commit, each with its own ID, through conditional retries.
            let creates = (0..3).map(|i| {
                let kv = [kv_a, kv_b][i % 2];
                let theirs = theirs.clone();
                tokio::spawn(async move {
                    call(kv, "POST", &theirs, &create(&format!("store-{i}"))).await
                })
            });
            let mut ids = Vec::new();
            let mut head = revision;
            for created in creates.collect::<Vec<_>>() {
                let (status, body) = created.await.unwrap();
                assert_eq!(status, 200, "{body}");
                ids.push(body["result"]["id"].as_str().unwrap().to_owned());
                head = head.max(body["result"]["revision"].as_u64().unwrap());
            }
            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 3);
            trace.push(format!("concurrent creates through {head}"));

            // Listing in the other cell at the creates' revision pages a tenant's namespaces in ID order, or by title.
            let at_head = format!("{theirs}?min_revision={head}");
            let listed = until("list", Duration::from_secs(60), 200, || {
                call(kv_b, "GET", &at_head, "")
            })
            .await;
            assert_eq!(listed["result_info"]["total_count"], ids.len());
            let nobody = format!("{}?min_revision={head}", namespaces("customer-789"));
            let (status, empty) = call(kv_b, "GET", &nobody, "").await;
            assert_eq!(
                (status, empty["result_info"]["total_count"].clone()),
                (200, 0.into())
            );
            assert!(
                empty["result_info"]["revision"].as_u64().unwrap() >= head,
                "{empty}"
            );
            let listed_ids = |body: &Value| -> Vec<String> {
                body["result"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n["id"].as_str().unwrap().to_owned())
                    .collect()
            };
            assert_eq!(listed_ids(&listed), ids);
            let (_, second) = call(kv_b, "GET", &format!("{theirs}?per_page=2&page=2"), "").await;
            assert_eq!(listed_ids(&second), ids[2..]);
            assert_eq!(second["result_info"]["count"], 1);
            let (_, by_title) = call(
                kv_b,
                "GET",
                &format!("{theirs}?order=title&direction=desc"),
                "",
            )
            .await;
            assert_eq!(by_title["result"][0]["title"], "store-2");
            let (_, own) = call(kv_b, "GET", &mine, "").await;
            assert_eq!(own["result_info"]["total_count"], 1);

            // A rename in cell a is visible in cell b at the revision it returned; the ID stays.
            let target = format!("{mine}/{id}");
            let renamed = until("rename", Duration::from_secs(60), 200, || {
                call(kv_a, "PUT", &target, r#"{"title":"store-renamed"}"#)
            })
            .await;
            let at = renamed["result"]["revision"].as_u64().unwrap();
            let path = format!("{mine}/{id}?min_revision={at}");
            let seen = until("rename seen", Duration::from_secs(60), 200, || {
                call(kv_b, "GET", &path, "")
            })
            .await;
            assert_eq!(seen["result"]["title"], "store-renamed");
            assert_eq!(seen["result"]["id"], id.as_str());

            // Deleting in cell b is visible in cell a at the revision it returned.
            let target = format!("{mine}/{id}");
            let deleted = until("delete", Duration::from_secs(60), 200, || {
                call(kv_b, "DELETE", &target, "")
            })
            .await;
            let at = deleted["result"]["revision"].as_u64().unwrap();
            let path = format!("{mine}/{id}?min_revision={at}");
            until("deletion seen", Duration::from_secs(60), 404, || {
                call(kv_a, "GET", &path, "")
            })
            .await;
            trace.push(format!("deleted at {at}"));
            // Deleting again changes nothing; the title is free again; an unknown ID is not found.
            assert_eq!(call(kv_a, "DELETE", &target, "").await.0, 200);
            let again = create("store-renamed");
            let reused = until("title reuse", Duration::from_secs(60), 200, || {
                call(kv_a, "POST", &mine, &again)
            })
            .await;
            assert_ne!(reused["result"]["id"], id.as_str());

            // A deletion an older binary recorded with no time, under its `delete:{id}` key: deleting it again stamps
            // the time even while that key's result is retained, so the purge after the grace goes through.
            let legacy = create("store-legacy");
            let legacy = until("legacy create", Duration::from_secs(60), 200, || {
                call(kv_a, "POST", &mine, &legacy)
            })
            .await;
            let legacy = legacy["result"]["id"].as_str().unwrap().to_owned();
            let unstamped = Record {
                format_version: FORMAT_VERSION,
                id: legacy.clone(),
                tenant: "customer-123".into(),
                title: "store-legacy".into(),
                home_region: "eu".into(),
                storage: storage_of(&legacy),
                storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
                storage_bytes: Some(1_000_000),
                permissions: Vec::new(),
                cache_ttl_seconds: CACHE_TTL_SECONDS,
                state: Lifecycle::Deleting as i32,
                created_at: 1,
                deleting_at: 0,
            };
            let request = hyper::Request::put(format!(
                "/v1/quicksilver/infrastructure/kv/namespaces/{legacy}"
            ))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", format!("delete:{legacy}"))
            .body(Full::new(Bytes::from(unstamped.encode_to_vec())))
            .unwrap();
            let (status, _) = bedrock_lib::http::send(
                &format!("{API}:7002"),
                None,
                request,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
            assert_eq!(status.as_u16(), 200);
            let legacy_path = format!("{mine}/{legacy}");
            until("legacy delete", Duration::from_secs(60), 200, || {
                call(kv_a, "DELETE", &legacy_path, "")
            })
            .await;
            sleep(PURGE_GRACE + Duration::from_secs(60)).await;
            let purge = format!("{legacy_path}/purge");
            until("legacy purge", Duration::from_secs(60), 200, || {
                call(kv_a, "POST", &purge, "")
            })
            .await;
            trace.push("legacy deletion purged".into());
            let unknown = call(kv_a, "GET", &format!("{mine}/{}", "0".repeat(32)), "").await;
            assert_eq!(unknown.0, 404);
            // A wrong token never reaches the directory.
            let request = hyper::Request::get(mine.as_str())
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
            let title = listed["result"][0]["title"].as_str().unwrap().to_owned();
            (trace, format!("{theirs}/{}", ids[0]), title)
        })
        .await
        .unwrap();
    let (mut trace, path, title) = trace;

    // A killed holder in cell a: reads go to its sibling owner, then to a fresher table once ranges move.
    handle.kill(holders[0].id());
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
            found["result"]["title"].as_str().unwrap().to_owned()
        })
        .await
        .unwrap();
    assert_eq!(state, title);
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
