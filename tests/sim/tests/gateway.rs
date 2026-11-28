//! #89 / B-57 part 1 acceptance: routing between cells through `bedrock-gateway`. One DC with two cells, each with
//! cell relays, Quicksilver owners and a cell Gateway, under two DC relays with a DC Gateway. A node in cell a reads a
//! key of cell b with no table or relay of cell b: the shared client finds the DC's Gateways in Quicksilver, the DC
//! Gateway learns cell b from `Relay.Cells`, resolves its Gateways with `Relay.Routes` and forwards, and cell b's
//! Gateway reads its holders, for a key and for a list. A read inside cell a passes no Gateway. Once the DC Gateway is
//! killed, the client falls back to the region's Gateway, which walks region relays, then DC relays. Replay one seed
//! with `BEDROCK_GATEWAY_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_gateway::GatewayConfig;
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, Location as Target, gateways_key};
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::replication::pb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory};
use bedrock_lib::transport::Peers;
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.7.1";
const TOKEN: &str = "simulation-writer-token";
const REGION_RELAY: &str = "10.0.7.3";
const DC_RELAYS: [&str; 2] = ["10.0.7.5", "10.0.7.6"];
const DC_GATEWAY: &str = "10.0.7.71";
const REGION_GATEWAY: &str = "10.0.7.72";
/// Per cell: relays, Quicksilver owners and the cell Gateway.
const CELLS: [(&str, [&str; 3], [&str; 2], &str); 2] = [
    (
        "eu-1a",
        ["10.0.7.11", "10.0.7.12", "10.0.7.13"],
        ["10.0.7.21", "10.0.7.22"],
        "10.0.7.31",
    ),
    (
        "eu-1b",
        ["10.0.7.41", "10.0.7.42", "10.0.7.43"],
        ["10.0.7.51", "10.0.7.52"],
        "10.0.7.61",
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

fn addresses(ips: &[&str]) -> Vec<String> {
    ips.iter().map(|ip| format!("{ip}:7000")).collect()
}

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API, "eu-1a"),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        bulk_max_bytes: bedrock_lib::node::DEFAULT_BULK_MAX_BYTES,
        s3: S3Config {
            endpoint: "http://10.0.7.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn region_relay() -> RelayConfig {
    RelayConfig {
        node: node(REGION_RELAY, REGION_RELAY, ""),
        tier: Tier::Region,
        upstreams: addresses(&[API]),
        replay_peers: Vec::new(),
        replay: Default::default(),
        membership: None,
        write_token: None,
    }
}

fn dc_relay(ip: &str) -> RelayConfig {
    RelayConfig {
        node: node(ip, ip, ""),
        tier: Tier::Dc,
        upstreams: addresses(&[REGION_RELAY]),
        replay_peers: addresses(
            &DC_RELAYS
                .into_iter()
                .filter(|peer| *peer != ip)
                .collect::<Vec<_>>(),
        ),
        replay: Default::default(),
        membership: None,
        write_token: None,
    }
}

fn cell_relay(ip: &str, cell: &str, relays: &[&str]) -> RelayConfig {
    RelayConfig {
        node: node(ip, ip, cell),
        tier: Tier::Cell,
        upstreams: addresses(&DC_RELAYS),
        replay_peers: addresses(
            &relays
                .iter()
                .copied()
                .filter(|peer| *peer != ip)
                .collect::<Vec<_>>(),
        ),
        replay: Default::default(),
        membership: Some(MembershipSettings {
            voter_ids: relays
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

fn quicksilver(name: &str, ip: &str, cell: &str, relays: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip, cell),
        roles: vec![Role::L2],
        datasets: vec!["quicksilver".into()],
        relays: addresses(relays),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.7.99:9000".into(),
    }
}

fn gateway(name: &str, ip: &str, cell: &str, relays: &[&str]) -> GatewayConfig {
    GatewayConfig {
        node: node(name, ip, cell),
        relays: addresses(relays),
        internal_limit: bedrock_gateway::INTERNAL_LIMIT,
        actor_cache: bedrock_gateway::actors::CACHE,
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
            // Boxed: the binaries' `run` futures are too large for a debug build's stack.
            let run = Box::pin(start());
            async move { run.await.unwrap() }
        })
        .build()
}

/// Writes `value` at `key` of `quicksilver/infrastructure`, returning the committed revision.
async fn put(key: &[u8], value: Vec<u8>) -> Option<u64> {
    let key = String::from_utf8(key.to_vec()).unwrap();
    let request = hyper::Request::put(format!("/v1/quicksilver/infrastructure/{key}"))
        .header("host", "api")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("idempotency-key", key.clone())
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

/// The `/metrics` page of the process at `ip`.
async fn metrics(ip: &str) -> String {
    let request = hyper::Request::get("/metrics")
        .header("host", "admin")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let (_, body) =
        bedrock_lib::http::send(&format!("{ip}:7001"), None, request, Duration::from_secs(5))
            .await
            .unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

/// `gateway_requests` served for Quicksilver by the Gateway at `ip`, in `role`.
async fn served(ip: &str, role: &str) -> u64 {
    let wanted = format!(
        r#"bedrock_gateway_requests_total{{role="{role}",service="quicksilver",outcome="served"}} "#
    );
    metrics(ip)
        .await
        .lines()
        .find_map(|line| line.strip_prefix(&wanted)?.parse().ok())
        .unwrap_or(0)
}

async fn until<T, F: Future<Output = Option<T>>>(
    what: &str,
    within: Duration,
    attempt: impl Fn() -> F,
) -> T {
    let deadline = bedrock_lib::monotonic_now() + within;
    loop {
        if let Some(done) = attempt().await {
            return done;
        }
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "{what} within {within:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
}

async fn scenario() -> Vec<String> {
    let handle = Handle::current();
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        Fingerprint::new(),
    ));
    let databases = Arc::new(MemoryFactory::default());
    install(Hooks {
        engine: Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || databases.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    spawn("api", API, || bedrock_api::run(api()));
    let config = region_relay();
    spawn("region-relay", REGION_RELAY, move || {
        bedrock_relay::run(config.clone())
    });
    for ip in DC_RELAYS {
        let config = dc_relay(ip);
        spawn(&format!("dc-relay-{ip}"), ip, move || {
            bedrock_relay::run(config.clone())
        });
    }
    for (cell, relays, quicksilvers, gateway_ip) in CELLS {
        for ip in relays {
            let config = cell_relay(ip, cell, &relays);
            spawn(&format!("relay-{ip}"), ip, move || {
                bedrock_relay::run(config.clone())
            });
        }
        for (i, ip) in quicksilvers.iter().enumerate() {
            let name = format!("qs-{cell}-{i}");
            let config = quicksilver(&name, ip, cell, &relays);
            spawn(&name, ip, move || bedrock_quicksilver::run(config.clone()));
        }
        let name = format!("gw-{cell}");
        let config = gateway(&name, gateway_ip, cell, &relays);
        spawn(&name, gateway_ip, move || {
            bedrock_gateway::run(config.clone())
        });
    }
    // An upper-tier Gateway's certificate names its DC or region where a cell Gateway's names its cell.
    let config = gateway("gw-arn1", DC_GATEWAY, "arn1", &DC_RELAYS);
    let dc_gateway = spawn("gw-arn1", DC_GATEWAY, move || {
        bedrock_gateway::run(config.clone())
    });
    let config = gateway("gw-eu", REGION_GATEWAY, "eu", &[REGION_RELAY]);
    spawn("gw-eu", REGION_GATEWAY, move || {
        bedrock_gateway::run(config.clone())
    });

    let [
        (home_cell, home_relays, _, home_gateway),
        (far_cell, _, _, far_gateway),
    ] = CELLS;
    let caller = handle
        .create_node()
        .name("caller")
        .ip("10.0.7.90".parse().unwrap())
        .build();
    caller
        .spawn(async move {
            let mut trace = Vec::new();
            for (dc, node_id, cell, ip) in [
                ("arn1", "gw-arn1", "arn1", DC_GATEWAY),
                ("", "gw-eu", "eu", REGION_GATEWAY),
            ] {
                let listed = pb::Gateways {
                    members: vec![pb::Member {
                        node_id: node_id.into(),
                        cell: cell.into(),
                        address: format!("{ip}:7000"),
                        ..Default::default()
                    }],
                }
                .encode_to_vec();
                let key = gateways_key("eu", dc);
                until("gateway list write", Duration::from_secs(60), || {
                    put(&key, listed.clone())
                })
                .await;
            }
            let revision = until("probe write", Duration::from_secs(60), || {
                put(b"probe", b"v".to_vec())
            })
            .await;
            trace.push(format!("written at {revision}"));

            // The caller knows only its own cell's relays.
            let admin = Admin::new();
            let client = Client::new(
                &admin,
                Arc::new(Peers::new(&admin)),
                addresses(&home_relays),
                String::new(),
            )
            .with_home(Target {
                region: "eu".into(),
                dc: "arn1".into(),
                cell: home_cell.into(),
            });
            let target = |cell: &str| Target {
                region: "eu".into(),
                dc: "arn1".into(),
                cell: cell.into(),
            };
            // Until the DC Gateway has learned cell b, a read may fall back to the region's Gateway; wait for the DC's.
            let far = until("read of cell b", Duration::from_secs(240), || async {
                let found = client
                    .get_in(
                        &target(far_cell),
                        "quicksilver",
                        "quicksilver",
                        "infrastructure",
                        b"probe",
                        revision,
                    )
                    .await
                    .ok()?;
                (served(DC_GATEWAY, "dc").await >= 1).then_some(found)
            })
            .await;
            assert_eq!(far.value.as_deref(), Some(&b"v"[..]));
            assert!(far.revision >= revision);
            trace.push(format!("cell b answered at {}", far.revision));
            assert!(
                served(far_gateway, "cell").await >= 1,
                "cell b's Gateway served it"
            );

            let near = client
                .get_in(
                    &target(home_cell),
                    "quicksilver",
                    "quicksilver",
                    "infrastructure",
                    b"probe",
                    revision,
                )
                .await
                .unwrap();
            assert_eq!(near.value.as_deref(), Some(&b"v"[..]));
            assert_eq!(
                served(home_gateway, "cell").await,
                0,
                "a read in the cell passes no Gateway"
            );
            // No table of another service or cell was fetched: only Quicksilver's, of the caller's own cell.
            let refreshes = admin.render();
            assert!(!refreshes.contains(r#"service="gateway""#), "{refreshes}");
            trace.push("cell a answered directly".into());

            // A list of cell b is the whole cell's page, merged by its Gateway.
            let page = client
                .list_in(
                    &target(far_cell),
                    "quicksilver",
                    "quicksilver",
                    "infrastructure",
                    b"pro",
                    b"",
                    0,
                    revision,
                )
                .await
                .unwrap();
            assert_eq!(page.entries, [(b"probe".to_vec(), b"v".to_vec())]);
            trace.push(format!("cell b listed at {}", page.revision));

            // Without the DC's Gateway the read falls back to the region's.
            let before = served(REGION_GATEWAY, "region").await;
            Handle::current().kill(dc_gateway.id());
            let fallback = until(
                "read through the region",
                Duration::from_secs(60),
                || async {
                    client
                        .get_in(
                            &target(far_cell),
                            "quicksilver",
                            "quicksilver",
                            "infrastructure",
                            b"probe",
                            revision,
                        )
                        .await
                        .ok()
                },
            )
            .await;
            assert_eq!(fallback.value.as_deref(), Some(&b"v"[..]));
            assert!(
                served(REGION_GATEWAY, "region").await > before,
                "the region Gateway forwarded it"
            );
            trace.push("the region's Gateway answered".into());
            trace
        })
        .await
        .unwrap()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> Vec<String> {
    std::thread::spawn(move || Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(Box::pin(scenario())))
        .join()
        .unwrap_or_else(|failure| {
            eprintln!(
                "Replay: BEDROCK_GATEWAY_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test gateway -- --nocapture"
            );
            std::panic::resume_unwind(failure)
        })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_GATEWAY_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42])
}

#[test]
fn a_node_reads_another_cell_through_gateways_and_its_own_cell_directly() {
    for seed in seeds() {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("gateway seed {seed}: {first:?}");
    }
}
