//! #147 / S-20 acceptance: the `bedrock-compute` binary in a one-cell DC, next to three real `bedrock-volume` nodes, a
//! cell Gateway, cell relays and Quicksilver owners. The binary joins the cell relay as a `compute` member, takes events
//! from the Gateway and places slots from its own cell's `volume` routes.
//!
//! - A request through the Gateway wakes an actor on the wake host, and a second one reaches the same instance.
//! - With the holder killed, the other host serves the actor's next value: no gap, no repeat.
//! - The killed host leaves the cell's `compute` members, and once restarted rejoins them. While the actor is awake on
//!   the other host, the rejoined host redirects to it and the Gateway reaches that one instance; after the actor
//!   sleeps, the restarted host wakes it again from where it stopped.
//!
//! Replay one seed with `BEDROCK_COMPUTE_BINARY_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::ComputeConfig;
use bedrock_compute::actors::host::IDLE;
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::actors::volume::HEARTBEAT;
use bedrock_compute::runtime::{Error, Handler, Instance, RustExecutor};
use bedrock_gateway::GatewayConfig;
use bedrock_lib::actors::{self, ActorId, Dc, Kind, Regions, Topology, topology_key};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::COMPUTE;
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::replication::pb as rpb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory};
use bedrock_lib::transport::Peers;
use bedrock_lib::volume as pb;
use bedrock_volume::VolumeConfig;
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.9.1";
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const DC: &str = "arn1";
const RELAYS: [&str; 3] = ["10.0.9.11", "10.0.9.12", "10.0.9.13"];
const QUICKSILVERS: [&str; 2] = ["10.0.9.21", "10.0.9.22"];
const VOLUMES: [&str; 3] = ["10.0.9.31", "10.0.9.32", "10.0.9.33"];
const GATEWAY: &str = "10.0.9.41";
const COMPUTES: [&str; 2] = ["10.0.9.51", "10.0.9.52"];

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
            endpoint: "http://10.0.9.99:9000".into(),
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

fn service(name: &str, ip: &str, datasets: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip),
        roles: vec![Role::L2],
        datasets: datasets.iter().map(|d| (*d).into()).collect(),
        relays: addresses(&RELAYS),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.9.99:9000".into(),
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

/// A test D1 handler: each event commits the counter plus one and answers "{host}#{instance} {counter}", where `instance`
/// counts the instances this process started.
struct Counter {
    instance: String,
    storage: Storage,
    value: Option<u64>,
}

#[async_trait]
impl Instance for Counter {
    async fn invoke(&mut self, _request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let current = match self.value {
            Some(value) => value,
            None => self.storage.get(vec![b"n".to_vec()]).await?[0]
                .value
                .as_deref()
                .map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap())),
        };
        let next = current + 1;
        self.storage
            .commit(vec![pb::Change {
                key: b"n".to_vec(),
                value: Some(next.to_be_bytes().to_vec()),
            }])
            .await?;
        self.value = Some(next);
        Ok(format!("{} {next}", self.instance).into_bytes())
    }
}

fn compute_name(ip: &str) -> String {
    format!("compute-{}", ip.rsplit('.').next().unwrap())
}

/// A `bedrock-compute` process running [`Counter`]; a restart starts its instance count again.
fn compute(ip: &'static str) -> NodeHandle {
    let name = compute_name(ip);
    spawn(&name.clone(), ip, move || {
        let (name, instances) = (name.clone(), Arc::new(AtomicU64::new(0)));
        let counter: Handler = Arc::new(move |_, storage| {
            let instance = instances.fetch_add(1, Ordering::SeqCst) + 1;
            Box::new(Counter {
                instance: format!("{name}#{instance}"),
                storage,
                value: None,
            })
        });
        bedrock_compute::run(
            ComputeConfig {
                node: node(&compute_name(ip), ip),
                relays: addresses(&RELAYS),
            },
            Arc::new(RustExecutor::new([(Kind::D1, counter)])),
        )
    })
}

/// Writes `value` at `key` of `quicksilver/infrastructure`, returning the committed revision.
async fn put(key: &[u8], value: Vec<u8>) -> Option<u64> {
    let key = std::str::from_utf8(key).unwrap();
    let request = hyper::Request::put(format!("/v1/quicksilver/infrastructure/{key}"))
        .header("host", "api")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("idempotency-key", key)
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

/// A client with no cached tables: one caches a table until it is told the table is stale.
fn client() -> Client {
    let admin = Admin::new();
    Client::new(
        &admin,
        Arc::new(Peers::new(&admin)),
        addresses(&RELAYS),
        String::new(),
    )
}

/// The cell's `compute` members, by node ID.
async fn compute_members() -> Option<Vec<String>> {
    let table = client().routes(COMPUTE, None).await.ok()?;
    Some(table.members.keys().cloned().collect())
}

/// The host at `ip`'s (wakes, awake actors it dropped because its lease lapsed or its storage failed).
async fn wakes_and_expiries(ip: &str) -> (u64, u64) {
    let request = hyper::Request::get("/metrics")
        .header("host", "admin")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let (_, body) =
        bedrock_lib::http::send(&format!("{ip}:7001"), None, request, Duration::from_secs(5))
            .await
            .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let sum = |series: &str| {
        body.lines()
            .filter(|line| line.starts_with(series))
            .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
            .sum()
    };
    (
        sum("bedrock_compute_actor_wake_seconds_count{"),
        sum("bedrock_compute_actor_lease_expiries_total{"),
    )
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

fn topology() -> Topology {
    Topology {
        format_version: actors::FORMAT_VERSION,
        region: "eu".into(),
        version: 1,
        dcs: vec![Dc {
            name: DC.into(),
            cells: vec![CELL.into()],
        }],
        previous_dcs: Vec::new(),
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
    for ip in RELAYS {
        spawn(&format!("relay-{ip}"), ip, move || {
            bedrock_relay::run(relay(ip))
        });
    }
    for (i, ip) in QUICKSILVERS.into_iter().enumerate() {
        let name = format!("qs-{i}");
        spawn(&name.clone(), ip, move || {
            bedrock_quicksilver::run(service(&name, ip, &["quicksilver"]))
        });
    }
    for (i, ip) in VOLUMES.into_iter().enumerate() {
        let name = format!("volume-{i}");
        spawn(&name.clone(), ip, move || {
            bedrock_volume::run(VolumeConfig {
                service: service(&name, ip, &[]),
                s3: None,
                sibling_relays: BTreeMap::new(),
                cache_mb: 64,
            })
        });
    }
    spawn("gw", GATEWAY, || {
        bedrock_gateway::run(GatewayConfig {
            node: node("gw", GATEWAY),
            relays: addresses(&RELAYS),
            internal_limit: bedrock_gateway::INTERNAL_LIMIT,
            actor_cache: bedrock_gateway::actors::CACHE,
        })
    });
    let hosts: BTreeMap<String, NodeHandle> = COMPUTES
        .into_iter()
        .map(|ip| (compute_name(ip), compute(ip)))
        .collect();

    let topology = topology();
    let regions: Regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let id = ActorId::from_name(&regions, "eu", "ns", "counter").unwrap();
    let members: Vec<rpb::Member> = COMPUTES
        .iter()
        .map(|ip| rpb::Member {
            node_id: compute_name(ip),
            cell: CELL.into(),
            roles: vec![COMPUTE.into()],
            status: rpb::member::Status::Ready as i32,
            ..Default::default()
        })
        .collect();
    let wake = actors::wake_host(&id, &topology, &members, |_| true)
        .unwrap()
        .unwrap()
        .node_id
        .clone();
    let wake_ip = *COMPUTES.iter().find(|ip| compute_name(ip) == wake).unwrap();
    let other = COMPUTES
        .iter()
        .map(|ip| compute_name(ip))
        .find(|name| *name != wake)
        .unwrap();
    let actor = pb::Actor {
        namespace: "ns".into(),
        id: id.to_string(),
        kind: Kind::D1 as i32,
    };

    handle
        .create_node()
        .name("worker")
        .ip("10.0.9.90".parse().unwrap())
        .build()
        .spawn(async move {
            let mut trace = Vec::new();
            let key = topology_key("eu");
            let revision = until("topology write", Duration::from_secs(60), || {
                put(&key, topology.encode_to_vec())
            })
            .await;
            trace.push(format!("topology at {revision}"));
            let joined = until("both hosts join", Duration::from_secs(240), || async {
                compute_members().await.filter(|m| m.len() == 2)
            })
            .await;
            trace.push(format!("members {joined:?}"));

            let client = client();
            let call = || async {
                let reply =
                    bedrock_compute::bindings::call(&client, actor.clone(), b"incr".to_vec())
                        .await
                        .ok()?;
                Some(String::from_utf8(reply).unwrap())
            };

            // A request wakes the actor on the wake host; the next reaches the same instance.
            let first = until("first call", Duration::from_secs(240), call).await;
            assert_eq!(first, format!("{wake}#1 1"));
            let second = call().await.expect("the awake actor serves");
            assert_eq!(second, format!("{wake}#1 2"), "the same instance");
            trace.extend([first, second]);

            // The holder dies: once its lease lapses the other host serves the next value.
            Handle::current().kill(hosts[&wake].id());
            let taken = until("the other host takes over", Duration::from_secs(120), call).await;
            // The surviving host sleeps the actor no earlier than this.
            let asleep = bedrock_lib::monotonic_now() + IDLE + HEARTBEAT;
            assert_eq!(
                taken,
                format!("{other}#1 3"),
                "one writer: no gap, no repeat"
            );
            trace.push(taken);

            // The killed host leaves the cell's members, and rejoins once restarted.
            until(
                "the killed host leaves",
                Duration::from_secs(120),
                || async { compute_members().await.filter(|m| *m == [other.clone()]) },
            )
            .await;
            Handle::current().restart(hosts[&wake].id());
            let rejoined = until("the host rejoins", Duration::from_secs(120), || async {
                compute_members().await.filter(|m| m.len() == 2)
            })
            .await;
            trace.push(format!("rejoined {rejoined:?}"));
            // Once the actor sleeps, the Gateway's cached host wakes it again.
            tokio::time::sleep_until(asleep).await;
            let kept = call().await.expect("the holder still serves");
            let instance = format!("{other}#");
            assert!(
                kept.starts_with(&instance) && kept.ends_with(" 4"),
                "one writer: {kept}"
            );
            let instance = kept.split(' ').next().unwrap().to_owned();
            assert_eq!(
                wakes_and_expiries(
                    COMPUTES
                        .iter()
                        .find(|ip| compute_name(ip) == other)
                        .unwrap()
                )
                .await,
                (2, 0),
                "{other} woke a second instance after a sleep, not a dropped one"
            );
            trace.push(kept);

            // The rejoined host serves no second instance while the actor is awake elsewhere: it redirects.
            let direct = pb::actors_client::ActorsClient::connect(format!("http://{wake_ip}:7000"))
                .await
                .unwrap();
            let invoke = || {
                let (mut direct, actor) = (direct.clone(), actor.clone());
                async move {
                    direct
                        .invoke(pb::InvokeRequest {
                            actor: Some(actor),
                            payload: b"incr".to_vec(),
                            host: String::new(),
                            alarm_ms: None,
                        })
                        .await
                        .ok()
                        .map(tonic::Response::into_inner)
                }
            };
            let redirected = until(
                "an answer from the rejoined host",
                Duration::from_secs(30),
                invoke,
            )
            .await;
            assert!(redirected.redirect, "{redirected:?}");
            assert_eq!(redirected.holder.unwrap().host, other);
            let same = call().await.expect("the holder still serves");
            assert_eq!(same, format!("{instance} 5"), "the same instance");
            trace.push(same);

            // Once the actor sleeps, the restarted host wakes it from where it stopped.
            sleep(IDLE + Duration::from_secs(3)).await;
            let woken = until(
                "wake on the restarted host",
                Duration::from_secs(60),
                || async {
                    let reply = invoke().await?;
                    (reply.code == 0 && !reply.redirect)
                        .then(|| String::from_utf8(reply.payload).unwrap())
                },
            )
            .await;
            assert_eq!(woken, format!("{wake}#1 6"));
            let followed = call().await.expect("the Gateway follows the move");
            assert_eq!(followed, format!("{wake}#1 7"));
            trace.extend([woken, followed]);
            trace
        })
        .await
        .unwrap()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> Vec<String> {
    std::thread::spawn(move || {
        let mut runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
        // A stalled scenario fails with its seed instead of hanging.
        runtime.set_time_limit(Duration::from_secs(900));
        runtime.block_on(Box::pin(scenario()))
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_COMPUTE_BINARY_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test compute_binary -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_COMPUTE_BINARY_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2])
}

#[test]
fn the_compute_binary_serves_one_instance_across_a_kill_and_a_rejoin_and_seeds_replay() {
    for seed in seeds() {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("compute binary seed {seed}: {first:?}");
    }
}
