//! #114 / S-06 end to end: real relays place a one-cell DC's `volume` ranges on real `bedrock-volume` nodes, and a
//! Compute holder commits through `bedrock_compute`'s volume client while
//!
//! - the cell's first three owners bootstrap ready from an empty directory,
//! - a drain hands one owner's range to a new node, which copies from the sealed leaving owner and becomes ready only
//!   through the generation it reported to its relay, and
//! - an owner loses its disk and repairs from its peers through the topology record and the cell's routes.
//!
//! Every acknowledged commit is read back afterwards, and a seed replays the same trace. Replay one seed with
//! `BEDROCK_VOLUME_CELL_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_compute::actors::volume::{Host, Slot, Writer, slot_nodes};
use bedrock_lib::actors::{self, ActorId, Dc, FORMAT_VERSION, Topology};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::VOLUME;
use bedrock_lib::node::{
    ApiConfig, Budget, Location, MembershipSettings, NodeConfig, RelayConfig, Role, ServiceConfig,
    Tier,
};
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::reconciliation::drain::{self, Drain, Scope};
use bedrock_lib::replication::pb as rpb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{Disks, Fingerprint, MemoryFactory};
use bedrock_lib::transport::Peers;
use bedrock_lib::volume as pb;
use bedrock_volume::VolumeConfig;
use bedrock_volume::store;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.4.1";
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const DC: &str = "arn1";
const RELAYS: [&str; 3] = ["10.0.4.11", "10.0.4.12", "10.0.4.13"];
const QUICKSILVERS: [&str; 2] = ["10.0.4.21", "10.0.4.22"];
const VOLUMES: [(&str, &str); 4] = [
    ("v1", "10.0.4.31"),
    ("v2", "10.0.4.32"),
    ("v3", "10.0.4.33"),
    ("v4", "10.0.4.34"),
];
const OBSERVER: &str = "10.0.4.50";
const HOLDERS: [&str; 2] = ["10.0.4.60", "10.0.4.61"];
/// Each phase waits for this many more acknowledged commits.
const STEP: usize = 8;

fn node(name: &str, ip: &str, data_dir: &str) -> NodeConfig {
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
        data_dir: data_dir.into(),
        budget: Budget::default(),
        tls: None,
    }
}

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API, "/data/api"),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        bulk_max_bytes: bedrock_lib::node::DEFAULT_BULK_MAX_BYTES,
        s3: S3Config {
            endpoint: "http://10.0.4.99:9000".into(),
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
        node: node(ip, ip, &format!("/data/{ip}")),
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
            grace_ms: 20_000,
            replicas: Default::default(),
            max_moving: Default::default(),
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn service(name: &str, ip: &str, data_dir: &str, datasets: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip, data_dir),
        roles: vec![Role::L2],
        datasets: datasets.iter().map(|d| (*d).into()).collect(),
        relays: RELAYS.iter().map(|ip| format!("{ip}:7000")).collect(),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.4.99:9000".into(),
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
            // Boxed: `bedrock_volume::run`'s future is too large for a debug build's stack.
            let run = Box::pin(start());
            async move { run.await.unwrap() }
        })
        .build()
}

/// A volume node. Its disk is `/data/{name}-{disk}`; raising `disk` before a restart gives it an empty one.
fn volume(name: &'static str, ip: &'static str, disk: Arc<AtomicU64>) -> NodeHandle {
    spawn(name, ip, move || {
        let data_dir = format!("/data/{name}-{}", disk.load(Ordering::SeqCst));
        bedrock_volume::run(VolumeConfig {
            service: service(name, ip, &data_dir, &[]),
            s3: None,
            sibling_relays: BTreeMap::new(),
            cache_mb: 64,
        })
    })
}

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

fn topology() -> Topology {
    Topology {
        format_version: FORMAT_VERSION,
        region: "eu".into(),
        version: 1,
        dcs: vec![Dc {
            name: DC.into(),
            cells: vec![CELL.into()],
        }],
        previous_dcs: Vec::new(),
    }
}

fn client() -> Client {
    let admin = Admin::new();
    Client::new(
        &admin,
        Arc::new(Peers::new(&admin)),
        RELAYS.iter().map(|ip| format!("{ip}:7000")).collect(),
        String::new(),
    )
}

/// The current `volume` directory. A new client each time: a client caches its table until told it is stale.
async fn directory() -> Directory {
    match client().routes(VOLUME, None).await {
        Ok(table) => table.directories.get(VOLUME).cloned().unwrap_or_default(),
        Err(_) => Directory::new(),
    }
}

/// The owners of the whole hash space, when one entry covers it and all are ready.
fn ready_owners(directory: &Directory) -> Option<Vec<String>> {
    let mut owners = Vec::new();
    for entry in directory.values() {
        if entry.iter().any(|o| o.state != State::Ready) {
            return None;
        }
        let mut nodes: Vec<String> = entry.iter().map(|o| o.node.clone()).collect();
        nodes.sort();
        if !owners.is_empty() && owners != nodes {
            return None;
        }
        owners = nodes;
    }
    (!owners.is_empty()).then_some(owners)
}

/// The actor's slots from the current routes, through a new client as in [`directory`].
async fn slots() -> Option<[Slot; 3]> {
    let table = client().routes(VOLUME, None).await.ok()?;
    let topology = topology();
    let regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let id = ActorId::parse(&regions, &actor().id).unwrap();
    let tables = BTreeMap::from([(CELL.to_owned(), table)]);
    slot_nodes(&id, &topology, &tables).ok()
}

/// One `PUT` through the API's write listener; the committed sequence on success.
async fn put(dataset: &str, namespace: &str, key: &str, value: Vec<u8>, id: &str) -> Option<u64> {
    let attempt = async {
        let tcp = tokio::net::TcpStream::connect(format!("{API}:7002"))
            .await
            .ok()?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .ok()?;
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .method("PUT")
            .uri(format!("/v1/{dataset}/{namespace}/{key}"))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", id)
            .body(Full::new(Bytes::from(value)))
            .unwrap();
        let response = sender.send_request(request).await.ok()?;
        let ok = response.status().is_success();
        let body = response.into_body().collect().await.ok()?.to_bytes();
        ok.then(|| std::str::from_utf8(&body).unwrap().parse().unwrap())
    };
    tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .ok()
        .flatten()
}

#[allow(clippy::disallowed_types, clippy::disallowed_methods)] // The operator retry timestamp, as in `membership.rs`.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn control_write(request: rpb::ControlWrite) {
    let peers = Peers::new(&Admin::new());
    for attempt in 0..30 {
        let address = format!("{}:7000", RELAYS[attempt % 3]);
        let channel = peers
            .channel(&address, bedrock_lib::transport::Class::Control)
            .unwrap();
        let mut client = rpb::cell_control_client::CellControlClient::new(channel);
        let mut call = tonic::Request::new(request.clone());
        call.metadata_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        match tokio::time::timeout(Duration::from_secs(12), client.write(call)).await {
            Ok(Ok(_)) => return,
            Ok(Err(status)) if status.code() == tonic::Code::InvalidArgument => {
                panic!("the cell refused the operator write: {status}")
            }
            _ => {}
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("the cell did not take the operator write");
}

async fn until<F: Future<Output = bool>>(what: &str, within: Duration, check: impl Fn() -> F) {
    let deadline = bedrock_lib::monotonic_now() + within;
    while !check().await {
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "{what} within {within:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
}

/// What the holder acknowledged, by commit number.
#[derive(Default)]
struct Holder {
    writing: AtomicBool,
    acked: Mutex<Vec<u64>>,
    reopened: AtomicU64,
}

impl Holder {
    fn count(&self) -> usize {
        self.acked.lock().unwrap().len()
    }
}

fn change(n: u64) -> Vec<pb::Change> {
    vec![pb::Change {
        key: format!("k{n:04}").into_bytes(),
        value: Some(n.to_be_bytes().to_vec()),
    }]
}

fn names(slots: &[Slot; 3]) -> Vec<String> {
    slots.iter().map(|s| s.identity.node_id.clone()).collect()
}

/// A writer and the slot nodes it opened on.
async fn open(host: &Arc<Host>) -> Option<(Writer, Vec<String>)> {
    let slots = slots().await?;
    let on = names(&slots);
    match Writer::open(host.clone(), actor(), slots, 0).await {
        Ok(w) => Some((w, on)),
        Err(_) => None,
    }
}

/// Commits one change at a time, reopening on new slots whenever the writer stops. With `follow`, it also reopens as
/// soon as the routes move the actor's slots, as a Compute host does; without, it keeps its slots until they fail.
async fn hold(holder: Arc<Holder>, follow: bool) {
    let host = Host::start(
        Arc::new(Peers::new(&Admin::new())),
        pb::Holder {
            cell: CELL.into(),
            host: "compute-1".into(),
        },
    );
    let mut writer: Option<(Writer, Vec<String>)> = None;
    let mut n = 0;
    while holder.writing.load(Ordering::SeqCst) {
        // A Compute host reopens on the actor's new slots when the routes move them.
        let now = if follow {
            slots().await.map(|s| names(&s))
        } else {
            None
        };
        if let (Some((_, on)), Some(now)) = (&writer, &now)
            && on != now
        {
            let (old, _) = writer.take().unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(5), old.release(0)).await;
        }
        if writer.as_ref().is_none_or(|(w, _)| !w.serving()) {
            writer = open(&host).await;
            if writer.is_none() {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
            holder.reopened.fetch_add(1, Ordering::SeqCst);
        }
        n += 1;
        // A commit waits for a quorum of its slots; after a handoff or a wipe that quorum may be gone, and a Compute
        // host would reopen on the new routes.
        let commit = writer.as_ref().unwrap().0.commit(change(n), None, n);
        match tokio::time::timeout(Duration::from_secs(5), commit).await {
            Ok(Ok(_)) => holder.acked.lock().unwrap().push(n),
            Ok(Err(_)) | Err(_) => writer = None,
        }
        sleep(Duration::from_millis(200)).await;
    }
    if let Some((writer, _)) = writer {
        let _ = writer.release(0).await;
    }
}

/// Each volume node's actor claim record, on every disk it had.
fn claims(disks: &Disks, generations: &[Arc<AtomicU64>]) -> Vec<String> {
    let key = store::claim_key(&store::Actor::new(&actor()).unwrap());
    let mut out = Vec::new();
    for ((name, _), disk) in VOLUMES.iter().zip(generations) {
        for d in 0..=disk.load(Ordering::SeqCst) {
            let dir = std::path::PathBuf::from(format!("/data/{name}-{d}"));
            let record = disks
                .get(&dir, "volume")
                .and_then(|engine| engine.get(&key).unwrap())
                .map(|bytes| pb::ClaimRecord::decode(bytes.as_slice()).unwrap());
            out.push(format!("{name}-{d}: {record:?}"));
        }
    }
    out
}

async fn scenario(follow: bool) -> Vec<String> {
    let handle = Handle::current();
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        Fingerprint::new(),
    ));
    let namespaces = Arc::new(MemoryFactory::default());
    let opened = disks.clone();
    install(Hooks {
        engine: Box::new(move |dir, name| opened.open(dir, name) as Arc<dyn StorageEngine>),
        namespaces: Box::new(move || namespaces.clone() as Arc<dyn NamespaceFactory>),
        rpc: Box::new(|_| Box::pin(async { Ok(None) })),
    });
    let mut trace = Vec::new();
    spawn("api", API, || bedrock_api::run(api()));
    for ip in RELAYS {
        spawn(&format!("relay-{ip}"), ip, move || {
            bedrock_relay::run(relay(ip))
        });
    }
    for (i, ip) in QUICKSILVERS.into_iter().enumerate() {
        let name = format!("qs-{i}");
        let config = service(&name, ip, &format!("/data/{name}"), &["quicksilver"]);
        spawn(&name, ip, move || bedrock_quicksilver::run(config.clone()));
    }
    let generations: Vec<Arc<AtomicU64>> = (0..4).map(|_| Arc::default()).collect();
    let mut nodes: Vec<NodeHandle> = VOLUMES[..3]
        .iter()
        .zip(&generations)
        .map(|((name, ip), disk)| volume(name, ip, disk.clone()))
        .collect();
    let observer = handle
        .create_node()
        .name("observer")
        .ip(OBSERVER.parse().unwrap())
        .build();

    // The topology record, which repair reads to find an actor's cells.
    observer
        .spawn(async {
            let key = String::from_utf8(actors::topology_key("eu")).unwrap();
            until("the topology write", Duration::from_secs(60), || async {
                put(
                    actors::DATASET,
                    actors::NAMESPACE,
                    &key,
                    topology().encode_to_vec(),
                    "topology-eu",
                )
                .await
                .is_some()
            })
            .await;
        })
        .await
        .unwrap();
    let observed = |want: &'static [&'static str]| {
        observer.spawn(async move {
            until(
                &format!("owners {want:?}"),
                Duration::from_secs(300),
                || async {
                    ready_owners(&directory().await).is_some_and(|owners| {
                        owners == want.iter().map(|s| s.to_string()).collect::<Vec<_>>()
                    })
                },
            )
            .await;
        })
    };
    observed(&["v1", "v2", "v3"]).await.unwrap();
    trace.push("bootstrapped v1 v2 v3".to_owned());

    let holder = Arc::new(Holder::default());
    holder.writing.store(true, Ordering::SeqCst);
    let computer = handle
        .create_node()
        .name("compute-1")
        .ip(HOLDERS[0].parse().unwrap())
        .build();
    let holding = computer.spawn(hold(holder.clone(), follow));
    let progress = |what: &'static str, from: usize| {
        let holder = holder.clone();
        observer.spawn(async move {
            until(what, Duration::from_secs(300), || {
                let holder = holder.clone();
                async move { holder.count() >= from + STEP }
            })
            .await;
        })
    };
    progress("commits before the handoff", 0).await.unwrap();
    trace.push(format!("acked {}", holder.count()));

    // Handoff: drain v3 into the new node v4.
    nodes.push(volume(VOLUMES[3].0, VOLUMES[3].1, generations[3].clone()));
    let record = Drain {
        scope: Scope::Node("v3".into()),
        datasets: Vec::new(),
        namespace: None,
        state: drain::State::Draining,
        rate: 1,
    };
    observer
        .spawn(control_write(rpb::ControlWrite {
            namespace: bedrock_lib::membership::namespace(VOLUME, CELL),
            key: Drain::key("v3").into_bytes(),
            value: Some(record.encode()),
            expected: None,
            expected_term: 0,
            idempotency_key: format!("{}:drain-v3", now()),
        }))
        .await
        .unwrap();
    observed(&["v1", "v2", "v4"]).await.unwrap();
    trace.push("handed v3 to v4".to_owned());
    progress("commits after the handoff", holder.count())
        .await
        .unwrap();
    trace.push(format!("acked {}", holder.count()));

    // v1 loses its disk and repairs from v2 and v4.
    handle.kill(nodes[0].id());
    generations[0].fetch_add(1, Ordering::SeqCst);
    sleep(Duration::from_millis(100)).await;
    handle.restart(nodes[0].id());
    trace.push("wiped v1".to_owned());
    let from = holder.count();
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(300);
    while holder.count() < from + STEP {
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "no commits after the wipe within 300 s; slots:\n{}",
            claims(&disks, &generations).join("\n")
        );
        sleep(Duration::from_millis(500)).await;
    }
    sleep(Duration::from_secs(30)).await;
    // v1 repaired: its new disk holds the actor again.
    let key = store::claim_key(&store::Actor::new(&actor()).unwrap());
    let repaired = disks
        .get(std::path::Path::new("/data/v1-1"), "volume")
        .and_then(|engine| engine.get(&key).unwrap());
    assert!(
        repaired.is_some(),
        "v1 did not repair; slots:\n{}",
        claims(&disks, &generations).join("\n")
    );
    progress("commits after v1 repaired", holder.count())
        .await
        .unwrap();
    holder.writing.store(false, Ordering::SeqCst);
    holding.await.unwrap();
    let acked = holder.acked.lock().unwrap().clone();
    trace.push(format!(
        "acked {acked:?} reopened {}",
        holder.reopened.load(Ordering::SeqCst)
    ));

    // A new holder reads every acknowledged commit.
    let reader = handle
        .create_node()
        .name("compute-2")
        .ip(HOLDERS[1].parse().unwrap())
        .build();
    let read = reader
        .spawn(async move {
            let host = Host::start(
                Arc::new(Peers::new(&Admin::new())),
                pb::Holder {
                    cell: CELL.into(),
                    host: "compute-2".into(),
                },
            );
            let mut writer = None;
            for _ in 0..120 {
                writer = open(&host).await;
                if writer.is_some() {
                    break;
                }
                sleep(Duration::from_millis(500)).await;
            }
            let (writer, _) = writer.expect("a new holder opens");
            let keys = acked
                .iter()
                .map(|n| format!("k{n:04}").into_bytes())
                .collect();
            let values = writer.read(keys).await.unwrap();
            assert_eq!(values.len(), acked.len(), "a read answers every key");
            for (n, value) in acked.iter().zip(&values) {
                assert_eq!(
                    value.value.as_deref(),
                    Some(&n.to_be_bytes()[..]),
                    "acknowledged commit {n} was lost"
                );
            }
            values.len()
        })
        .await
        .unwrap();
    trace.push(format!("read {read}"));
    trace
}

#[allow(clippy::disallowed_methods, clippy::disallowed_types)] // A fresh test thread, with room for `run`'s futures, resets the simulator's seeded std map hashing.
fn run(seed: u64, follow: bool) -> Vec<String> {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(move || {
            Runtime::with_seed_and_config(seed, madsim::Config::default())
                .block_on(Box::pin(scenario(follow)))
        })
        .unwrap()
        .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_VOLUME_CELL_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test volume_cell -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

fn seeds() -> Vec<u64> {
    std::env::var("BEDROCK_VOLUME_CELL_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 3])
}

#[test]
fn a_cell_hands_off_and_repairs_volume_ranges_without_losing_commits_and_replays_per_seed() {
    for seed in seeds() {
        let first = run(seed, true);
        assert_eq!(first, run(seed, true), "seed {seed} changed its trace");
        println!("volume cell seed {seed}: {first:?}");
    }
}

/// The holder keeps its slots through the handoff, so v4 copies v3's sealed log while v1 and v2 commit past it. After
/// v1 is wiped, v2 and v4 are the voting slots, and v4 must catch up for a claim to commit.
#[test]
fn a_slot_left_behind_by_a_handoff_catches_up_after_another_is_wiped() {
    for seed in seeds() {
        let first = run(seed, false);
        assert_eq!(first, run(seed, false), "seed {seed} changed its trace");
        println!("volume cell (kept slots) seed {seed}: {first:?}");
    }
}
