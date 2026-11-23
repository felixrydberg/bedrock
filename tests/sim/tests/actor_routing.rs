//! #116 / S-08 acceptance: actor routing through `bedrock-gateway`. One DC with two cells, each with cell relays,
//! Quicksilver owners, a cell Gateway and two Compute hosts joined to their cell relay, under two DC relays with a DC
//! Gateway; three standalone volume slots hold the actors. A Worker binding call leaves a node in cell a for cell a's
//! Gateway, which reads the topology, picks `dc_of` and `cells_of`, and reaches cell b's Gateway through the DC's.
//!
//! - A request to a sleeping actor wakes it on the wake host.
//! - A handler failure in another cell runs once: no forwarding hop sends the event again.
//! - Once the actor slept and woke on another host, the cached location costs exactly one redirect.
//! - With its holder killed, the stale cache costs failed attempts, never a second writer: the counter the actor commits
//!   continues without a gap or a repeat on the wake host.
//!
//! Replay one seed with `BEDROCK_ACTOR_ROUTING_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, IDLE, Placement};
use bedrock_compute::actors::serve::Actors;
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::actors::volume::{Host, Slot};
use bedrock_compute::runtime::{Error, Handler, Instance, RustExecutor};
use bedrock_gateway::GatewayConfig;
use bedrock_lib::actors::{self, ActorId, Dc, Kind, Regions, Topology, topology_key};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, gateways_key};
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::replication::pb as rpb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory, SimEngine};
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume as pb;
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.8.1";
const TOKEN: &str = "simulation-writer-token";
const REGION_RELAY: &str = "10.0.8.3";
const DC_RELAYS: [&str; 2] = ["10.0.8.5", "10.0.8.6"];
const DC_GATEWAY: &str = "10.0.8.71";
const SLOTS: [&str; 3] = ["10.0.8.101", "10.0.8.102", "10.0.8.103"];
/// Per cell: relays, Quicksilver owners, the cell Gateway and two Compute hosts.
type CellPlan = (
    &'static str,
    [&'static str; 3],
    [&'static str; 2],
    &'static str,
    [&'static str; 2],
);
const CELLS: [CellPlan; 2] = [
    (
        "eu-1a",
        ["10.0.8.11", "10.0.8.12", "10.0.8.13"],
        ["10.0.8.21", "10.0.8.22"],
        "10.0.8.31",
        ["10.0.8.81", "10.0.8.82"],
    ),
    (
        "eu-1b",
        ["10.0.8.41", "10.0.8.42", "10.0.8.43"],
        ["10.0.8.51", "10.0.8.52"],
        "10.0.8.61",
        ["10.0.8.83", "10.0.8.84"],
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
        s3: S3Config {
            endpoint: "http://10.0.8.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn relay(ip: &str, tier: Tier, upstreams: &[&str], relays: &[&str], cell: &str) -> RelayConfig {
    let peers: Vec<&str> = relays.iter().copied().filter(|p| *p != ip).collect();
    RelayConfig {
        node: node(ip, ip, cell),
        tier,
        upstreams: addresses(upstreams),
        replay_peers: addresses(&peers),
        replay: Default::default(),
        membership: (tier == Tier::Cell).then(|| MembershipSettings {
            voter_ids: relays
                .iter()
                .map(|peer| (format!("{peer}:7000"), (*peer).into()))
                .collect(),
            bootstrap: true,
            grace_ms: 20_000,
            volume_replicas: 3,
        }),
        write_token: (tier == Tier::Cell).then(|| TOKEN.into()),
    }
}

fn quicksilver(name: &str, ip: &str, cell: &str, relays: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip, cell),
        roles: vec![Role::L2],
        datasets: vec!["quicksilver".into()],
        relays: addresses(relays),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.8.99:9000".into(),
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

fn slot_node(i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let name = format!("volume-{i}");
    Handle::current()
        .create_node()
        .name(&name)
        .ip(SLOTS[i].parse().unwrap())
        .init(move || {
            let (engine, name) = (engine.clone(), name.clone());
            async move {
                let store = Arc::new(Store::new(engine));
                let service =
                    Service::new(&Admin::new(), store, &name, CELLS[i % 2].0, "arn1").unwrap();
                *service.directory.write().unwrap() = BTreeMap::from([(
                    0,
                    vec![Owner {
                        node: name,
                        state: State::Ready,
                    }],
                )]);
                tonic::transport::Server::builder()
                    .add_service(VolumeServer::new(service))
                    .serve(format!("{}:7000", SLOTS[i]).parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

/// Every actor's slots are the three standalone slot nodes.
struct Fixed;

#[async_trait]
impl Placement for Fixed {
    async fn slots(&self, _actor: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        let slots = std::array::from_fn(|i| Slot {
            cell: CELLS[i % 2].0.into(),
            address: format!("{}:7000", SLOTS[i]),
            identity: tls::Identity {
                service: "volume".into(),
                cell: CELLS[i % 2].0.into(),
                node_id: format!("volume-{i}"),
                host: SLOTS[i].into(),
            },
        });
        Ok((slots, 0))
    }
}

/// Built-in `kv`: each event commits the counter plus one and answers "{host} {counter}"; `fail` then fails, so the
/// next value shows how often it ran.
struct Counter {
    host: String,
    storage: Storage,
    value: Option<u64>,
}

#[async_trait]
impl Instance for Counter {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let current = match self.value {
            Some(value) => value,
            None => {
                let values = self.storage.get(vec![b"n".to_vec()]).await?;
                values[0]
                    .value
                    .as_deref()
                    .map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap()))
            }
        };
        let next = current + 1;
        self.storage
            .commit(vec![pb::Change {
                key: b"n".to_vec(),
                value: Some(next.to_be_bytes().to_vec()),
            }])
            .await?;
        self.value = Some(next);
        if request == b"fail" {
            return Err(Error::Handler("asked to fail".into()));
        }
        Ok(format!("{} {next}", self.host).into_bytes())
    }
}

fn compute_name(ip: &str) -> String {
    format!("compute-{}", ip.rsplit('.').next().unwrap())
}

/// A Compute host: an actor host serving `Actors` and joined to its cell relays as a `compute` member.
fn compute_node(ip: &'static str, cell: &'static str, relays: [&'static str; 3]) -> NodeHandle {
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
                    cell: cell.into(),
                    host: name.clone(),
                };
                let counter: Handler = {
                    let name = name.clone();
                    Arc::new(move |_, storage| {
                        Box::new(Counter {
                            host: name.clone(),
                            storage,
                            value: None,
                        })
                    })
                };
                let host = ActorHost::new(
                    &admin,
                    cell,
                    "arn1",
                    Host::start(peers.clone(), holder.clone()),
                    Arc::new(Fixed),
                    Arc::new(RustExecutor::new([(Kind::Kv, counter)])),
                    Arc::new(RustExecutor::new(Vec::<(Kind, Handler)>::new())),
                );
                let member = rpb::Member {
                    node_id: name,
                    service: "compute".into(),
                    region: "eu".into(),
                    dc: "arn1".into(),
                    cell: cell.into(),
                    roles: vec!["compute".into()],
                    status: rpb::member::Status::Ready as i32,
                    address: format!("{ip}:7000"),
                    ..Default::default()
                };
                let (_view, target) = bedrock_lib::membership::View::new("compute", cell);
                let _session = bedrock_lib::relays::client::Session::join(
                    peers,
                    addresses(&relays),
                    vec![target],
                    member,
                    BTreeMap::new(),
                )
                .unwrap();
                tonic::transport::Server::builder()
                    .add_service(pb::actors_server::ActorsServer::new(Actors::new(
                        host, holder,
                    )))
                    .serve(format!("{ip}:7000").parse().unwrap())
                    .await
                    .unwrap();
            }
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

/// Redirects counted by the Gateway at `ip`.
async fn redirects(ip: &str) -> u64 {
    let request = hyper::Request::get("/metrics")
        .header("host", "admin")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let (_, body) =
        bedrock_lib::http::send(&format!("{ip}:7001"), None, request, Duration::from_secs(5))
            .await
            .unwrap();
    String::from_utf8(body.to_vec())
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("bedrock_gateway_actor_redirects_total{"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
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
            name: "arn1".into(),
            cells: CELLS.iter().map(|c| c.0.into()).collect(),
        }],
        previous_dcs: Vec::new(),
    }
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
    let databases = Arc::new(MemoryFactory::default());
    install(Hooks {
        engine: Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || databases.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    for i in 0..3 {
        let disk = SimEngine::new(&format!("volume-{i}"), Arc::new(|_| None), None, false);
        slot_node(i, Arc::new(disk.recorded(fingerprint.clone())));
    }
    spawn("api", API, || bedrock_api::run(api()));
    let config = relay(REGION_RELAY, Tier::Region, &[API], &[], "");
    spawn("region-relay", REGION_RELAY, move || {
        bedrock_relay::run(config.clone())
    });
    for ip in DC_RELAYS {
        let config = relay(ip, Tier::Dc, &[REGION_RELAY], &DC_RELAYS, "");
        spawn(&format!("dc-relay-{ip}"), ip, move || {
            bedrock_relay::run(config.clone())
        });
    }
    let mut hosts = BTreeMap::new();
    for (cell, relays, quicksilvers, gateway_ip, computes) in CELLS {
        for ip in relays {
            let config = relay(ip, Tier::Cell, &DC_RELAYS, &relays, cell);
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
        for ip in computes {
            hosts.insert(compute_name(ip), compute_node(ip, cell, relays));
        }
    }
    let config = gateway("gw-arn1", DC_GATEWAY, "arn1", &DC_RELAYS);
    spawn("gw-arn1", DC_GATEWAY, move || {
        bedrock_gateway::run(config.clone())
    });

    // The routed actor's first cell is b, so a binding call from cell a crosses the DC Gateway.
    let topology = topology();
    let regions: Regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let id = (0..)
        .map(|i| ActorId::from_name(&regions, "eu", "ns", &format!("counter-{i}")).unwrap())
        .find(|id| actors::cells_of(id, &topology).unwrap()[0] == "eu-1b")
        .unwrap();
    let members: Vec<rpb::Member> = CELLS
        .iter()
        .flat_map(|(cell, _, _, _, computes)| {
            computes.iter().map(|ip| rpb::Member {
                node_id: compute_name(ip),
                cell: (*cell).into(),
                roles: vec!["compute".into()],
                status: rpb::member::Status::Ready as i32,
                ..Default::default()
            })
        })
        .collect();
    let wake = actors::wake_host(&id, &topology, &members, |_| true)
        .unwrap()
        .unwrap()
        .node_id
        .clone();
    // Another host, in cell a, that the actor later moves to.
    let other_ip = CELLS[0].4[0];
    let other = compute_name(other_ip);
    let actor = pb::Actor {
        namespace: "ns".into(),
        id: id.to_string(),
        kind: Kind::Kv as i32,
    };

    let [
        (home_cell, home_relays, _, home_gateway, _),
        (far_cell, far_relays, _, _, _),
    ] = CELLS;
    let caller = handle
        .create_node()
        .name("worker")
        .ip("10.0.8.90".parse().unwrap())
        .build();
    caller
        .spawn(async move {
            let mut trace = Vec::new();
            let listed = rpb::Gateways {
                members: vec![rpb::Member {
                    node_id: "gw-arn1".into(),
                    cell: "arn1".into(),
                    address: format!("{DC_GATEWAY}:7000"),
                    ..Default::default()
                }],
            }
            .encode_to_vec();
            let key = gateways_key("eu", "arn1");
            until("gateway list write", Duration::from_secs(60), || {
                put(&key, listed.clone())
            })
            .await;
            let key = topology_key("eu");
            let revision = until("topology write", Duration::from_secs(60), || {
                put(&key, topology.encode_to_vec())
            })
            .await;
            trace.push(format!("topology at {revision}"));

            let admin = Admin::new();
            let client = Client::new(
                &admin,
                Arc::new(Peers::new(&admin)),
                addresses(&home_relays),
                String::new(),
            );
            // Both cells publish both Compute hosts before the first request, so the wake host is the planned one.
            for (cell, relays) in [(home_cell, home_relays), (far_cell, far_relays)] {
                until("compute members", Duration::from_secs(240), || async {
                    // A fresh client: one caches a table until it is told the table is stale.
                    let admin = Admin::new();
                    let table = Client::new(
                        &admin,
                        Arc::new(Peers::new(&admin)),
                        Vec::new(),
                        String::new(),
                    )
                    .routes_at(cell, &addresses(&relays), "compute", None)
                    .await
                    .ok()?;
                    (table.members.len() == 2).then_some(())
                })
                .await;
            }
            let call = || async {
                let reply =
                    bedrock_compute::bindings::call(&client, actor.clone(), b"incr".to_vec())
                        .await
                        .ok()?;
                Some(String::from_utf8(reply).unwrap())
            };

            // A sleeping actor wakes on the wake host in its first cell.
            let first = until("first call", Duration::from_secs(120), call).await;
            assert_eq!(first, format!("{wake} 1"));
            assert_eq!(redirects(home_gateway).await, 0);
            trace.push(first);

            // A failing event reaches the caller as the host's failure and runs once.
            let failed = bedrock_compute::bindings::call(&client, actor.clone(), b"fail".to_vec())
                .await
                .unwrap_err();
            assert_eq!(failed.code(), tonic::Code::Aborted, "{failed}");
            let second = call().await.expect("the actor still serves");
            assert_eq!(
                second,
                format!("{wake} 3"),
                "the failed event ran exactly once"
            );
            trace.push(second);

            // It sleeps, then wakes on another host; the Gateway's cached location costs one redirect.
            sleep(IDLE + Duration::from_secs(3)).await;
            let direct =
                pb::actors_client::ActorsClient::connect(format!("http://{other_ip}:7000"))
                    .await
                    .unwrap();
            let moved = until("wake on another host", Duration::from_secs(30), || {
                let mut direct = direct.clone();
                let actor = actor.clone();
                async move {
                    let reply = direct
                        .invoke(pb::InvokeRequest {
                            actor: Some(actor),
                            payload: b"incr".to_vec(),
                            host: String::new(),
                        })
                        .await
                        .ok()?
                        .into_inner();
                    (!reply.redirect).then(|| String::from_utf8(reply.payload).unwrap())
                }
            })
            .await;
            assert_eq!(moved, format!("{other} 4"));
            let followed = call().await.expect("one redirect is followed");
            assert_eq!(followed, format!("{other} 5"));
            assert_eq!(
                redirects(home_gateway).await,
                1,
                "moved actor costs one redirect"
            );
            trace.push(followed);
            drop(direct);

            // Its holder dies: the stale cache fails until the lease lapses, then the wake host serves the next value.
            Handle::current().kill(hosts[&other].id());
            let recovered = until("recovery on the wake host", Duration::from_secs(60), call).await;
            assert_eq!(
                recovered,
                format!("{wake} 6"),
                "one writer: no gap and no repeat"
            );
            trace.push(recovered);
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
            "Replay: BEDROCK_ACTOR_ROUTING_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_routing -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_ACTOR_ROUTING_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2])
}

#[test]
fn actors_wake_on_the_wake_host_and_moves_cost_one_redirect_and_seeds_replay() {
    for seed in seeds() {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its trace");
        println!("actor routing seed {seed}: {first:?}");
    }
}
