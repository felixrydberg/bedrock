//! #124 / S-16: a fixed actor workload against real `bedrock-volume` and `bedrock-compute` binaries in a one-cell DC,
//! with relays, Quicksilver owners and a cell Gateway, under faults. The history checker is `bedrock_sim::actors`.
//!
//! - **Load.** Every seed sends the same calls to the same counter actors through the Gateway: increments, reads from
//!   the slots, answers from memory, an alarm each, and pauses long enough for an actor to sleep, so the next call
//!   wakes it. Some actors are then deleted at the volume `Writer`, and called again. The seed picks only when each
//!   call arrives, when each fault starts and each lease participant's clock rate.
//! - **Faults**, one after another: S3 slows down, the holder of a busy actor is killed mid-append and restarted, a
//!   volume node is lost and comes back, another loses its disk and repairs, a third hands its range to a new node.
//!   Every Compute and volume node's clock runs at a seeded rate within ±10%. Last, an actor's holder is cut off from
//!   its slots while another host claims it: at the slowest allowed holder clock it stops answering before the other
//!   serves.
//! - **Checks**: no lost acknowledged commit, one serving instance per actor, no stale read below `min_seq`, deleted
//!   actors stay deleted, acknowledged alarms fire. Each has a deliberate bug that breaks it, and a seed replays its
//!   trace. Heartbeats the volume nodes took are reported (`statefull/v2.md` §12 question 7).
//!
//! Losing a cell of a three-cell DC waits for sibling-cell placement in the binary (#148 / S-20b; user decision
//! 2026-11-25). Run seeds with `BEDROCK_ACTOR_WORKLOAD_SEED=42` (or `=1,2,3`) and the documented simulation flags; each
//! runs twice, each time in a fresh process.
#![cfg(madsim)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::ComputeConfig;
use bedrock_compute::actors::host::IDLE;
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::actors::volume::{self, Host, Writer, slot_nodes};
use bedrock_compute::runtime::{Error, Handler, Instance, RustExecutor};
use bedrock_gateway::GatewayConfig;
use bedrock_lib::actors::{self, ActorId, Dc, Kind, Regions, Topology, topology_key};
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
use bedrock_sim::actors::{Answer, Call, History, Op, Violation};
use bedrock_volume::VolumeConfig;
use bedrock_volume::cold::db::Memory;
use bedrock_volume::store;
use http_body_util::Full;
use hyper::body::Bytes;
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use madsim::time::TimeHandle;
use prost::Message;
use tokio::time::sleep;

const API: &str = "10.0.9.1";
const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const DC: &str = "arn1";
const RELAYS: [&str; 3] = ["10.0.9.11", "10.0.9.12", "10.0.9.13"];
const QUICKSILVERS: [&str; 2] = ["10.0.9.21", "10.0.9.22"];
/// `v4` joins for the handoff.
const VOLUMES: [(&str, &str); 4] = [
    ("v1", "10.0.9.31"),
    ("v2", "10.0.9.32"),
    ("v3", "10.0.9.33"),
    ("v4", "10.0.9.34"),
];
const GATEWAY: &str = "10.0.9.41";
const COMPUTES: [&str; 3] = ["10.0.9.51", "10.0.9.52", "10.0.9.53"];
const CLIENT: &str = "10.0.9.90";

/// The workload's counter actors; the last [`DELETED`] are deleted at the end.
const ACTORS: usize = 6;
const DELETED: usize = 2;
/// How far ahead an `alarm` call sets the actor's alarm: past [`IDLE`], so most fire on a sleeping actor.
const ALARM_IN: Duration = Duration::from_secs(15);
/// Clock rates of Compute and volume nodes stay within this of simulated time. Slots wait 5 s after a heartbeat and a
/// holder stops at 4 s, so a holder clock may run up to 1.25 times slower than a slot's.
const DRIFT: f64 = 0.1;

/// One step of every actor's plan.
#[derive(Debug, Clone, Copy)]
enum Step {
    Call(&'static [u8]),
    /// Long enough for the actor to sleep.
    Pause,
}

use Step::{Call as C, Pause};

/// Every actor's plan; the seed spaces the steps.
const PLAN: [Step; 24] = [
    C(b"incr"),
    C(b"incr"),
    C(b"get"),
    C(b"peek"),
    C(b"alarm"),
    C(b"incr"),
    Pause,
    C(b"get"),
    C(b"incr"),
    C(b"peek"),
    C(b"incr"),
    C(b"incr"),
    C(b"get"),
    Pause,
    C(b"incr"),
    C(b"get"),
    C(b"incr"),
    C(b"incr"),
    C(b"peek"),
    C(b"incr"),
    Pause,
    C(b"get"),
    C(b"incr"),
    C(b"get"),
];

/// A deliberate bug: each breaks one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bug {
    /// Every third increment answers without committing.
    Uncommitted,
    /// `get` answers the value the instance woke with instead of reading the slots.
    StaleRead,
    /// The cut-off holder's clock runs at half speed, past the lease margin.
    SlowHolder,
    /// The harness records one actor's deletion but deletes another.
    WrongDelete,
    /// `alarm` acknowledges an alarm it never commits.
    DroppedAlarm,
}

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

fn addresses(ips: &[&str]) -> Vec<String> {
    ips.iter().map(|ip| format!("{ip}:7000")).collect()
}

fn api() -> ApiConfig {
    ApiConfig {
        node: node("api", API, "/data/api"),
        write_listen: format!("{API}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Default::default(),
        bulk_max_bytes: bedrock_lib::node::DEFAULT_BULK_MAX_BYTES,
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
        node: node(ip, ip, &format!("/data/{ip}")),
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

fn service(name: &str, ip: &str, data_dir: &str, datasets: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip, data_dir),
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

/// Runs `node`'s clock at a seeded rate within [`DRIFT`], or at `rate`.
fn drift(node: &NodeHandle, rate: Option<f64>) -> f64 {
    let rate = rate.unwrap_or_else(|| 1.0 + thread_rng().gen_range(-DRIFT..=DRIFT));
    TimeHandle::current().set_clock_rate(node.id(), rate);
    rate
}

/// What the instances of every host share with the harness, all in this process's memory.
#[derive(Default)]
struct Shared {
    /// Instances created so far.
    instances: AtomicU64,
    /// Each instance's host, by actor ID and claim epoch, which is the instance's number.
    hosts: Mutex<BTreeMap<(String, u64), String>>,
    /// Heartbeats of volume nodes' runs that ended, which their counters no longer hold.
    beats: AtomicU64,
    /// Alarms that fired, by actor ID and time.
    fired: Mutex<BTreeSet<(String, u64)>>,
    /// The actor whose next increment stops at its append, for the holder kill.
    armed: Mutex<Option<String>>,
    /// The host of the increment that reached it.
    appending: Mutex<Option<String>>,
    reached: tokio::sync::Notify,
    /// Set once that increment's commit returned, however it ended.
    returned: std::sync::atomic::AtomicBool,
}

/// The counter actor: `incr` commits the counter plus one, `alarm` also sets an alarm [`ALARM_IN`] ahead, `get` reads the
/// counter from the slots and `peek` answers from memory. Answers are "{instance} {value}", and `alarm` adds its time.
/// A fired alarm counts one and clears.
struct Counter {
    actor: String,
    instance: u64,
    storage: Storage,
    value: Option<u64>,
    /// The value the instance woke with.
    woke: u64,
    increments: u64,
    shared: Arc<Shared>,
    bug: Option<Bug>,
}

#[allow(clippy::disallowed_types, clippy::disallowed_methods)] // Alarm times are wall time, as the actor host's.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl Counter {
    async fn stored(&self) -> Result<u64, Error> {
        let values = self.storage.get(vec![b"n".to_vec()]).await?;
        Ok(values[0]
            .value
            .as_deref()
            .map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap())))
    }

    async fn current(&mut self) -> Result<u64, Error> {
        if let Some(value) = self.value {
            return Ok(value);
        }
        let value = self.stored().await?;
        (self.value, self.woke) = (Some(value), value);
        Ok(value)
    }

    fn change(value: u64) -> Vec<pb::Change> {
        vec![pb::Change {
            key: b"n".to_vec(),
            value: Some(value.to_be_bytes().to_vec()),
        }]
    }

    fn answer(&self, value: u64) -> Vec<u8> {
        format!("{} {value}", self.instance).into_bytes()
    }
}

#[async_trait]
impl Instance for Counter {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let current = self.current().await?;
        match &request[..] {
            b"incr" => {
                self.increments += 1;
                let next = current + 1;
                let armed = {
                    let mut armed = self.shared.armed.lock().unwrap();
                    armed.take_if(|actor| *actor == self.actor).is_some()
                };
                if armed {
                    let host = self.shared.hosts.lock().unwrap()
                        [&(self.actor.clone(), self.instance)]
                        .clone();
                    *self.shared.appending.lock().unwrap() = Some(host);
                    self.shared.reached.notify_one();
                }
                if !(self.bug == Some(Bug::Uncommitted) && self.increments.is_multiple_of(3)) {
                    let committed = self.storage.commit(Self::change(next)).await;
                    if armed {
                        self.shared.returned.store(true, Ordering::SeqCst);
                    }
                    committed?;
                }
                self.value = Some(next);
                Ok(self.answer(next))
            }
            b"alarm" => {
                let (next, at) = (current + 1, now_ms() + ALARM_IN.as_millis() as u64);
                if self.bug == Some(Bug::DroppedAlarm) {
                    self.storage.commit(Self::change(next)).await?;
                } else {
                    self.storage
                        .commit_with_alarm(Self::change(next), Some(at))
                        .await?;
                }
                self.value = Some(next);
                Ok(format!("{} {next} {at}", self.instance).into_bytes())
            }
            b"get" if self.bug == Some(Bug::StaleRead) => Ok(self.answer(self.woke)),
            b"get" => {
                let stored = self.stored().await?;
                Ok(self.answer(stored))
            }
            b"peek" => Ok(self.answer(current)),
            other => Err(Error::Handler(format!("unknown request {other:?}"))),
        }
    }

    /// Records the delivery first: the commit that clears the alarm may land although its host dies before it answers.
    async fn alarm(&mut self, at_ms: u64) -> Result<(), Error> {
        self.shared
            .fired
            .lock()
            .unwrap()
            .insert((self.actor.clone(), at_ms));
        let next = self.current().await? + 1;
        self.storage
            .commit_with_alarm(Self::change(next), None)
            .await?;
        self.value = Some(next);
        Ok(())
    }
}

fn compute_name(ip: &str) -> String {
    format!("compute-{}", ip.rsplit('.').next().unwrap())
}

/// A `bedrock-compute` process running [`Counter`] as the built-in `d1` kind.
fn compute(ip: &'static str, shared: Arc<Shared>, bug: Option<Bug>) -> NodeHandle {
    let name = compute_name(ip);
    spawn(&name.clone(), ip, move || {
        let (name, shared) = (name.clone(), shared.clone());
        let counter: Handler = Arc::new(move |actor, storage| {
            shared.instances.fetch_add(1, Ordering::SeqCst);
            let instance = storage.epoch();
            shared
                .hosts
                .lock()
                .unwrap()
                .insert((actor.id.clone(), instance), name.clone());
            Box::new(Counter {
                actor: actor.id,
                instance,
                storage,
                value: None,
                woke: 0,
                increments: 0,
                shared: shared.clone(),
                bug,
            })
        });
        bedrock_compute::run(
            ComputeConfig {
                node: node(
                    &compute_name(ip),
                    ip,
                    &format!("/data/{}", compute_name(ip)),
                ),
                relays: addresses(&RELAYS),
            },
            Arc::new(RustExecutor::new([(Kind::D1, counter)])),
        )
    })
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

async fn directory() -> Directory {
    match client().routes(VOLUME, None).await {
        Ok(table) => table.directories.get(VOLUME).cloned().unwrap_or_default(),
        Err(_) => Directory::new(),
    }
}

/// The ready owners of the whole hash space, sorted, when every entry has the same ones.
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

async fn owned_by(want: &[&str]) {
    let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
    until(
        &format!("ready owners {want:?}"),
        Duration::from_secs(300),
        || async { ready_owners(&directory().await).filter(|owners| *owners == want) },
    )
    .await;
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

fn actor(name: &str) -> pb::Actor {
    let topology = topology();
    let regions: Regions = BTreeMap::from([("eu".to_owned(), topology)]);
    pb::Actor {
        namespace: "ns".into(),
        id: ActorId::from_name(&regions, "eu", "ns", name)
            .unwrap()
            .to_string(),
        kind: Kind::D1 as i32,
    }
}

/// The history, and when the run started on the client's clock.
struct Recorder {
    start: tokio::time::Instant,
    history: Mutex<History>,
}

impl Recorder {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    /// Sends `request` to `actor` at `address` (the Gateway, or one host), records the call and returns its answer and
    /// the alarm time an `alarm` answer carries.
    async fn call(
        &self,
        address: &str,
        actor: &pb::Actor,
        request: &[u8],
    ) -> Option<(Answer, Option<u64>)> {
        let start = self.now();
        let reply = async {
            let mut client =
                pb::actors_client::ActorsClient::connect(format!("http://{address}:7000"))
                    .await
                    .ok()?;
            let invoke = client.invoke(pb::InvokeRequest {
                actor: Some(actor.clone()),
                payload: request.to_vec(),
                host: String::new(),
                alarm_ms: None,
            });
            let reply = tokio::time::timeout(Duration::from_secs(10), invoke)
                .await
                .ok()?
                .ok()?
                .into_inner();
            if reply.code != 0 || reply.redirect {
                return None;
            }
            let text = String::from_utf8(reply.payload).ok()?;
            let mut words = text.split(' ').map(|w| w.parse::<u64>().unwrap());
            let answer = Answer {
                instance: words.next()?,
                value: words.next()?,
            };
            Some((answer, words.next()))
        }
        .await;
        let op = match request {
            b"get" => Op::Get,
            b"peek" => Op::Peek,
            _ => Op::Incr,
        };
        let mut history = self.history.lock().unwrap();
        history.calls.push(Call {
            actor: actor.id.clone(),
            op,
            start,
            end: self.now(),
            answer: reply.map(|(answer, _)| answer),
        });
        if let Some((_, Some(at))) = reply {
            history.alarms.insert((actor.id.clone(), at));
        }
        reply
    }
}

/// One actor's plan through the Gateway, each step after a seeded gap.
async fn drive(recorder: Arc<Recorder>, actor: pb::Actor) {
    for step in PLAN {
        sleep(Duration::from_millis(thread_rng().gen_range(0..1500))).await;
        match step {
            Pause => sleep(IDLE + Duration::from_secs(3)).await,
            C(request) => {
                recorder.call(GATEWAY, &actor, request).await;
            }
        }
    }
}

/// Deletes `actor` at its slots through a volume client of the client node, as host `holder`, the actor's last holder:
/// where the Gateway would deliver the request. A slot that missed that holder's `RELEASE` refuses every other claimant
/// while the host heartbeats it for other actors. A refusal naming another live holder is followed to it, as a Gateway
/// follows a redirect. Returns the last refusal if it never went through.
async fn delete(actor: &pb::Actor, holder: &str) -> Result<(), String> {
    let as_host = |host: &str| {
        Host::start(
            Arc::new(Peers::new(&Admin::new())),
            pb::Holder {
                cell: CELL.into(),
                host: host.into(),
            },
        )
    };
    let (mut name, mut host) = (holder.to_owned(), as_host(holder));
    let topology = topology();
    let regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let id = ActorId::parse(&regions, &actor.id).unwrap();
    let mut last = String::new();
    for _ in 0..120 {
        let table = client().routes(VOLUME, None).await;
        if let Ok(table) = table {
            let tables = BTreeMap::from([(CELL.to_owned(), table)]);
            if let Ok(slots) = slot_nodes(&id, &topology, &tables) {
                match Writer::delete(host.clone(), actor.clone(), slots, 1, now_ms()).await {
                    Ok(()) => return Ok(()),
                    Err(volume::Error::Held(holder)) => {
                        last = format!("held by {}", holder.host);
                        if holder.host != name {
                            name = holder.host;
                            host = as_host(&name);
                        }
                    }
                    Err(error @ volume::Error::Unavailable(_)) => last = error.to_string(),
                    Err(error) => panic!("deleting {} failed: {error}", actor.id),
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

/// Each volume disk's claim record of `actor`, for a failure report.
fn claims(disks: &Disks, generations: &[Arc<AtomicU64>], actor: &pb::Actor) -> Vec<String> {
    let key = store::claim_key(&store::Actor::new(actor).unwrap());
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

/// The cluster's processes.
struct Cluster {
    volumes: Vec<NodeHandle>,
    disks: Vec<Arc<AtomicU64>>,
    computes: BTreeMap<String, NodeHandle>,
    shared: Arc<Shared>,
}

/// Seeded faults, one after another while the workload runs; returns what happened.
async fn faults(cluster: &mut Cluster, recorder: &Recorder, busy: &pb::Actor) -> Vec<String> {
    let handle = Handle::current();
    let mut trace = Vec::new();
    let gap = || Duration::from_millis(thread_rng().gen_range(1_000..8_000));

    // S3 slows down for a while: flushes back up, commits and wakes do not wait for it.
    sleep(gap()).await;
    Memory::shared().slow(Duration::from_secs(2));
    trace.push(format!("{:?} s3 slow", recorder.now()));
    sleep(Duration::from_secs(30)).await;
    Memory::shared().slow(Duration::ZERO);
    trace.push(format!("{:?} s3 back", recorder.now()));

    // The holder of a busy actor dies once an increment reached its append, within the append's network round trip,
    // and comes back.
    sleep(gap()).await;
    *cluster.shared.armed.lock().unwrap() = Some(busy.id.clone());
    let kill = async {
        tokio::time::timeout(Duration::from_secs(120), cluster.shared.reached.notified())
            .await
            .expect("no increment of the busy actor reached its append");
        let holder = cluster.shared.appending.lock().unwrap().take().unwrap();
        // Under one network hop (madsim sends take 1 ms or more), so no slot has answered the append yet.
        sleep(Duration::from_micros(thread_rng().gen_range(0..1_000))).await;
        assert!(
            !cluster.shared.returned.load(Ordering::SeqCst),
            "the append returned before its holder was killed"
        );
        handle.kill(cluster.computes[&holder].id());
        trace.push(format!("{:?} killed {holder} mid-append", recorder.now()));
        holder
    };
    let (_, holder) = tokio::join!(recorder.call(GATEWAY, busy, b"incr"), kill);
    sleep(Duration::from_secs(10)).await;
    handle.restart(cluster.computes[&holder].id());

    // A volume node is lost and comes back with its disk.
    sleep(gap()).await;
    let ended = heartbeats(VOLUMES[0].1).await;
    cluster.shared.beats.fetch_add(ended, Ordering::SeqCst);
    handle.kill(cluster.volumes[0].id());
    trace.push(format!("{:?} lost v1", recorder.now()));
    sleep(Duration::from_secs(15)).await;
    handle.restart(cluster.volumes[0].id());
    owned_by(&["v1", "v2", "v3"]).await;

    // Another loses its disk and repairs from its peers.
    sleep(gap()).await;
    let ended = heartbeats(VOLUMES[1].1).await;
    cluster.shared.beats.fetch_add(ended, Ordering::SeqCst);
    handle.kill(cluster.volumes[1].id());
    cluster.disks[1].fetch_add(1, Ordering::SeqCst);
    sleep(Duration::from_millis(100)).await;
    handle.restart(cluster.volumes[1].id());
    trace.push(format!("{:?} wiped v2", recorder.now()));
    owned_by(&["v1", "v2", "v3"]).await;

    // A third hands its range to a new node.
    sleep(gap()).await;
    let v4 = volume(VOLUMES[3].0, VOLUMES[3].1, cluster.disks[3].clone());
    drift(&v4, None);
    cluster.volumes.push(v4);
    let record = Drain {
        scope: Scope::Node("v3".into()),
        datasets: Vec::new(),
        namespace: None,
        state: drain::State::Draining,
        rate: 1,
    };
    control_write(rpb::ControlWrite {
        namespace: bedrock_lib::membership::namespace(VOLUME, CELL),
        key: Drain::key("v3").into_bytes(),
        value: Some(record.encode()),
        expected: None,
        expected_term: 0,
        idempotency_key: format!("{}:drain-v3", now_ms()),
    })
    .await;
    owned_by(&["v1", "v2", "v4"]).await;
    trace.push(format!("{:?} handed v3 to v4", recorder.now()));
    trace
}

/// The holder of `target` on `computes[0]` is cut off from every volume node, while the client polls it from memory and
/// asks `computes[1]` to increment until that host claims the actor. Returns the cut-off host's last answer and the
/// other host's first, on the client's clock.
async fn cut_off(cluster: &Cluster, recorder: &Recorder, target: &pb::Actor) -> String {
    let (held, other) = (COMPUTES[0], COMPUTES[1]);
    until("the cut-off actor wakes", Duration::from_secs(120), || {
        recorder.call(held, target, b"incr")
    })
    .await;
    let net = NetSim::current();
    let node = cluster.computes[&compute_name(held)].id();
    for volume in &cluster.volumes {
        net.clog_link(node, volume.id());
        net.clog_link(volume.id(), node);
    }
    let cut = recorder.now();
    let taken = async {
        until("the other host claims", Duration::from_secs(60), || async {
            let answer = recorder.call(other, target, b"incr").await;
            if answer.is_none() {
                sleep(Duration::from_millis(50)).await;
            }
            answer.map(|_| recorder.now())
        })
        .await
    };
    let polled = async {
        let mut last = None;
        while recorder.now() < cut + Duration::from_secs(20) {
            if recorder.call(held, target, b"peek").await.is_some() {
                last = Some(recorder.now());
            }
            sleep(Duration::from_millis(20)).await;
        }
        last
    };
    let (taken, last) = tokio::join!(taken, polled);
    for volume in &cluster.volumes {
        net.unclog_link(node, volume.id());
        net.unclog_link(volume.id(), node);
    }
    format!("cut off at {cut:?}: last answer from memory {last:?}, other host from {taken:?}")
}

/// Each volume node's heartbeats taken since it last started, from its metrics.
async fn heartbeats(ip: &str) -> u64 {
    let request = hyper::Request::get("/metrics")
        .header("host", "admin")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let Ok((_, body)) =
        bedrock_lib::http::send(&format!("{ip}:7001"), None, request, Duration::from_secs(5)).await
    else {
        return 0;
    };
    String::from_utf8(body.to_vec())
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("bedrock_volume_heartbeats"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

async fn all_heartbeats() -> Vec<u64> {
    let mut beats = Vec::new();
    for (_, ip) in VOLUMES {
        beats.push(heartbeats(ip).await);
    }
    beats
}

/// One run: the trace, or the first violation.
async fn scenario(bug: Option<Bug>, faulted: bool) -> Result<Vec<String>, Violation> {
    let handle = Handle::current();
    let disks = Arc::new(Disks::new(
        Arc::new(|_| None),
        None,
        false,
        Fingerprint::new(),
    ));
    let databases = Arc::new(MemoryFactory::default());
    let disk_view = disks.clone();
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
            bedrock_quicksilver::run(service(
                &name,
                ip,
                &format!("/data/{name}"),
                &["quicksilver"],
            ))
        });
    }
    let mut trace = Vec::new();
    let disks: Vec<Arc<AtomicU64>> = (0..VOLUMES.len()).map(|_| Arc::default()).collect();
    let volumes: Vec<NodeHandle> = VOLUMES[..3]
        .iter()
        .zip(&disks)
        .map(|((name, ip), disk)| volume(name, ip, disk.clone()))
        .collect();
    spawn("gw", GATEWAY, || {
        bedrock_gateway::run(GatewayConfig {
            node: node("gw", GATEWAY, "/data/gw"),
            relays: addresses(&RELAYS),
            internal_limit: bedrock_gateway::INTERNAL_LIMIT,
            actor_cache: bedrock_gateway::actors::CACHE,
        })
    });
    let shared = Arc::new(Shared::default());
    let computes: BTreeMap<String, NodeHandle> = COMPUTES
        .into_iter()
        .map(|ip| (compute_name(ip), compute(ip, shared.clone(), bug)))
        .collect();
    // The cut-off holder runs at the slowest allowed clock, or past the margin with the bug.
    let slowest = if bug == Some(Bug::SlowHolder) {
        0.5
    } else {
        1.0 - DRIFT
    };
    let mut rates = Vec::new();
    for (i, node) in computes.values().enumerate() {
        rates.push(drift(node, (i == 0).then_some(slowest)));
    }
    for node in &volumes {
        rates.push(drift(node, None));
    }
    trace.push(format!("rates {rates:.3?}"));
    let mut cluster = Cluster {
        volumes,
        disks,
        computes,
        shared: shared.clone(),
    };
    let client = handle
        .create_node()
        .name("client")
        .ip(CLIENT.parse().unwrap())
        .build();
    client
        .spawn(async move {
            let key = topology_key("eu");
            let revision = until("topology write", Duration::from_secs(60), || {
                put(&key, topology().encode_to_vec())
            })
            .await;
            trace.push(format!("topology at {revision}"));
            owned_by(&["v1", "v2", "v3"]).await;
            let recorder = Arc::new(Recorder {
                start: bedrock_lib::monotonic_now(),
                history: Mutex::default(),
            });
            let targets: Vec<pb::Actor> = (0..ACTORS).map(|i| actor(&format!("a{i}"))).collect();
            if bug != Some(Bug::SlowHolder) {
                let drivers: Vec<_> = targets
                    .iter()
                    .map(|target| tokio::spawn(drive(recorder.clone(), target.clone())))
                    .collect();
                if faulted {
                    trace.extend(faults(&mut cluster, &recorder, &targets[0]).await);
                }
                for driver in drivers {
                    driver.await.unwrap();
                }
                // Every actor recovers from the faults: an increment and a read go through, and the check holds them.
                for target in &targets {
                    for request in [&b"incr"[..], b"get"] {
                        until(&format!("{} recovers its {:?}", &target.id[..8], String::from_utf8_lossy(request)), Duration::from_secs(120), || {
                            recorder.call(GATEWAY, target, request)
                        })
                        .await;
                    }
                }
                // Asleep again, so no lease holds up the deletions; then each deleted actor is called again.
                sleep(IDLE + Duration::from_secs(3)).await;
                for (i, target) in targets[ACTORS - DELETED..].iter().enumerate() {
                    let deleted = if bug == Some(Bug::WrongDelete) && i == 0 {
                        &targets[0]
                    } else {
                        target
                    };
                    let holder = {
                        let history = recorder.history.lock().unwrap();
                        let last = history
                            .calls
                            .iter()
                            .rev()
                            .find_map(|c| c.answer.filter(|_| c.actor == deleted.id));
                        last.map_or_else(
                            || "deleter".to_owned(),
                            |a| cluster.shared.hosts.lock().unwrap()[&(deleted.id.clone(), a.instance)]
                                .clone(),
                        )
                    };
                    if let Err(refused) = delete(deleted, &holder).await {
                        panic!(
                            "{} was not deleted: {refused}
{}",
                            deleted.id,
                            claims(&disk_view, &cluster.disks, deleted).join(
                                "
"
                            )
                        );
                    }
                    let at = recorder.now();
                    recorder
                        .history
                        .lock()
                        .unwrap()
                        .deleted
                        .insert(target.id.clone(), at);
                    for request in [&b"incr"[..], b"get"] {
                        recorder.call(GATEWAY, target, request).await;
                    }
                }
                // Every alarm is due by now, plus time for a Wake.
                sleep(ALARM_IN + Duration::from_secs(10)).await;
            }
            if faulted || bug == Some(Bug::SlowHolder) {
                trace.push(cut_off(&cluster, &recorder, &actor("fence")).await);
            }
            // Heartbeat volume, for `statefull/v2.md` §12 question 7: each node's counter plus what its killed runs took,
            // over the run. Nothing holds an actor before the run starts.
            let beats: Vec<u64> = all_heartbeats().await;
            let total = beats.iter().sum::<u64>() + cluster.shared.beats.load(Ordering::SeqCst);
            let over = recorder.now();
            trace.insert(
                2,
                format!(
                    "{total} heartbeats over {over:?}, {beats:?} by the volume nodes' last runs: {:.2}/s in all for {} Compute hosts",
                    total as f64 / over.as_secs_f64(),
                    COMPUTES.len()
                ),
            );
            let mut history = recorder.history.lock().unwrap().clone();
            history.fired = cluster.shared.fired.lock().unwrap().clone();
            let answered = history.calls.iter().filter(|c| c.answer.is_some()).count();
            trace.push(format!(
                "{} calls, {answered} answered, {} alarms fired, {} instances",
                history.calls.len(),
                history.fired.len(),
                cluster.shared.instances.load(Ordering::SeqCst)
            ));
            for call in &history.calls {
                trace.push(format!(
                    "{} {:?} {:?}-{:?} {:?}",
                    &call.actor[..8],
                    call.op,
                    call.start,
                    call.end,
                    call.answer
                ));
            }
            if let Err(violation) = history.check() {
                // The trace explains a failing seed.
                for line in &trace {
                    eprintln!("  {line}");
                }
                return Err(violation);
            }
            Ok(trace)
        })
        .await
        .unwrap()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64, bug: Option<Bug>, faulted: bool) -> Result<Vec<String>, Violation> {
    std::thread::spawn(move || {
        let mut runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
        // A stalled scenario fails with its seed instead of hanging.
        runtime.set_time_limit(Duration::from_secs(1800));
        runtime.block_on(Box::pin(scenario(bug, faulted)))
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_ACTOR_WORKLOAD_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_workload -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

/// Seeds that still fail, with what they found and where the fix belongs. The check fails until this is empty.
const OPEN: &[(u64, &str)] = &[];
/// Seeds that found a bug, now fixed, with the focused test that holds the fix.
const REGRESSIONS: &[(u64, &str)] = &[
    (
        1,
        "traced (2 and 3 as well, once every actor had to recover): the busy actor's writer kept one voting slot, one \
         slot missing its claim and another repaired, so nothing committed, but it fenced only below two slots and held \
         the actor while its lease lived. Fixed by fencing below two voters; held by crates/compute/tests/unit/volume.rs \
         a_writer_left_with_one_voting_slot_stops_serving. Then the fenced writer's host kept heartbeating its slot for \
         other actors, so its promise stayed leased and blocked every other claimant; fixed by ending the leases of \
         claims a host stopped running; held by crates/volume/tests/unit/store.rs an_ended_claim_holds_no_lease",
    ),
    (
        3,
        "traced: after a handoff the writer kept appending to the old owner, which refused it as no owner of the range \
         but still took its heartbeats, so the writer served without committing and held its other slot's lease. \
         Fixed by dropping a slot that answers FAILED_PRECONDITION; held by this seed only",
    ),
    (
        2,
        "traced: a slot behind S3 waited out BEHIND_WAIT on an append gap, as long as the holder's call, so it never \
         heard its LogGap; with another slot repaired the actor never reached quorum again. Fixed by `serve::GAP_WAIT` \
         and `GAP_LOOKUP`; held by crates/volume/tests/unit/cold.rs \
         an_append_past_what_s3_holds_answers_its_gap_at_once",
    ),
];
/// Seeds run on every change.
const SAMPLE: &[u64] = &[4];

fn seeds() -> Vec<u64> {
    match std::env::var("BEDROCK_ACTOR_WORKLOAD_SEED") {
        Ok(value) => value.split(',').map(|v| v.parse().unwrap()).collect(),
        Err(_) => OPEN
            .iter()
            .chain(REGRESSIONS)
            .map(|(seed, _)| *seed)
            .chain(SAMPLE.iter().copied())
            .collect(),
    }
}

/// Runs `seed` in a fresh process of this test binary and returns its trace, or the panic it ended with. A seed replays
/// across processes, as `scripts/vopr.sh replay` requires: a second run of the same seed in one process can differ
/// (state outlives a run somewhere in the simulator stack; not yet traced).
#[allow(clippy::disallowed_types)] // a separate process, not part of the simulation
fn in_process(seed: u64) -> Result<Vec<String>, String> {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "one_seed", "--nocapture"])
        .env(CHILD, seed.to_string())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trace: Vec<String> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("TRACE "))
        .map(str::to_owned)
        .collect();
    if output.status.success() && !trace.is_empty() {
        Ok(trace)
    } else {
        Err(format!(
            "{stdout}{}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

const CHILD: &str = "BEDROCK_ACTOR_WORKLOAD_CHILD";

/// One seed for [`in_process`]; does nothing in a normal test run.
#[test]
fn one_seed() {
    if let Ok(seed) = std::env::var(CHILD) {
        let seed = seed.parse().unwrap();
        let trace = run(seed, None, true).unwrap_or_else(|v| panic!("seed {seed} broke {v}"));
        for line in trace {
            println!("TRACE {line}");
        }
    }
}

#[test]
fn the_actor_workload_holds_every_check_under_faults_and_seeds_replay() {
    for seed in seeds() {
        let first = in_process(seed).unwrap_or_else(|failure| panic!("seed {seed}: {failure}"));
        let second = in_process(seed).unwrap_or_else(|failure| panic!("seed {seed}: {failure}"));
        if let Some(i) = (0..first.len().max(second.len())).find(|&i| first.get(i) != second.get(i))
        {
            panic!(
                "seed {seed} changed its trace at line {i}: {:?} then {:?}",
                first.get(i),
                second.get(i)
            );
        }
        println!("actor workload seed {seed}: {}", first[..4].join("; "));
        if std::env::var_os("BEDROCK_ACTOR_WORKLOAD_TRACE").is_some() {
            for line in &first {
                println!("  {line}");
            }
        }
    }
}

/// Every check breaks under its deliberate bug.
#[test]
fn every_check_fails_under_its_deliberate_bug() {
    for (bug, broken) in [
        (Bug::Uncommitted, "no lost acknowledged commit"),
        (Bug::StaleRead, "no stale read"),
        (Bug::SlowHolder, "one serving instance"),
        (Bug::WrongDelete, "deleted stays deleted"),
        (Bug::DroppedAlarm, "alarms fire"),
    ] {
        match run(1, Some(bug), false) {
            Err(violation) => assert!(
                violation.to_string().starts_with(broken),
                "{bug:?} broke {violation} instead of {broken}"
            ),
            Ok(_) => panic!("{bug:?} broke no check"),
        }
    }
}
