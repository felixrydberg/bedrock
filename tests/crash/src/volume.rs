//! #124 / S-16: real `bedrock-volume` processes killed at every append delivery boundary. Three volume nodes hold one
//! actor's slots in one cell, behind the harness's proxies; the harness is the actor's holder, through
//! `bedrock_compute`'s volume client with a `compute` certificate. Each round SIGKILLs one slot, rotating:
//!
//! - **before delivery**: its proxy holds the append, which commits on the other two;
//! - **during the write**: the append reached the slot and its reply is held; the kill lands within a few
//!   milliseconds, around the slot's fsync;
//! - **after the write**: the slot has had time to sync, and its reply is still held;
//! - **between appends**.
//!
//! The slot restarts on its disk, and every few rounds the holder releases and a new writer reopens the actor, which
//! must recover every acknowledged commit. The history goes to `bedrock_sim::actors`. As the module docs of the crate
//! say, SIGKILL keeps the page cache, so a write the slot handed to the kernel survives; fsync loss is the simulation's
//! (`tests/sim/tests/volume.rs`, the buffered disk of `bedrock_lib::storage::sim`).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bedrock_compute::actors::volume::{Host, Writer, slot_nodes};
use bedrock_lib::actors::{self as ids, ActorId, Dc, Topology};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::VOLUME;
use bedrock_lib::placement::State;
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::tls::{Identity, Tls};
use bedrock_lib::volume as pb;
use bedrock_sim::actors::{Answer, Call, History, Op, Violation};
use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::sleep;

use super::proxy::Mode;
use super::{CELL, Cluster, Kind, Node, Options, certificate_paths, ready, submit};

/// The holder's node ID and certificate name.
pub const HOLDER: &str = "holder";
/// Rounds: each slot at each boundary, twice.
const ROUNDS: usize = 24;
/// Commits after each restart, so the restarted slot catches up before the next kill.
const BETWEEN: usize = 5;
/// The holder releases and reopens every this many rounds.
const REOPEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    BeforeDelivery,
    DuringWrite,
    AfterWrite,
    Between,
}

const BOUNDARIES: [Boundary; 4] = [
    Boundary::BeforeDelivery,
    Boundary::DuringWrite,
    Boundary::AfterWrite,
    Boundary::Between,
];

fn topology() -> Topology {
    Topology {
        format_version: ids::FORMAT_VERSION,
        region: "eu".into(),
        version: 1,
        dcs: vec![Dc {
            name: "arn1".into(),
            cells: vec![CELL.into()],
        }],
        previous_dcs: Vec::new(),
    }
}

/// The holder's side: its volume client, the actor and the counter it last committed.
struct Holder {
    host: Arc<Host>,
    client: Client,
    actor: pb::Actor,
    id: ActorId,
    started: Instant,
    history: History,
}

impl Holder {
    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    /// Opens the actor once its three slots are placed and one quorum takes the claim; returns the writer and its
    /// recovered counter, recorded as a `Get`.
    async fn open(&mut self) -> Writer {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let start = self.now();
            match self.try_open().await {
                Ok((writer, value)) => {
                    let instance = writer.epoch();
                    self.record(Op::Get, start, Some(Answer { instance, value }));
                    return writer;
                }
                Err(error) => assert!(Instant::now() < deadline, "the actor did not open: {error}"),
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn try_open(&self) -> Result<(Writer, u64), String> {
        let table = self
            .client
            .routes(VOLUME, None)
            .await
            .map_err(|status| status.to_string())?;
        let tables = BTreeMap::from([(CELL.to_owned(), table)]);
        let slots = slot_nodes(&self.id, &topology(), &tables).map_err(|e| e.to_string())?;
        let writer = Writer::open(self.host.clone(), self.actor.clone(), slots, 1)
            .await
            .map_err(|e| e.to_string())?;
        let values = writer
            .read(vec![b"n".to_vec()])
            .await
            .map_err(|e| e.to_string())?;
        let value = values[0]
            .value
            .as_deref()
            .map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap()));
        Ok((writer, value))
    }

    fn record(&mut self, op: Op, start: Duration, answer: Option<Answer>) {
        let end = self.now();
        self.history.calls.push(Call {
            actor: self.actor.id.clone(),
            op,
            start,
            end,
            answer,
        });
    }

    /// Commits the counter plus one; `false` when the outcome is unknown.
    async fn commit(&mut self, writer: &Writer, value: &mut u64) -> bool {
        let start = self.now();
        let next = *value + 1;
        let change = pb::Change {
            key: b"n".to_vec(),
            value: Some(next.to_be_bytes().to_vec()),
        };
        let committed = tokio::time::timeout(
            Duration::from_secs(10),
            writer.commit(vec![change], None, 0),
        )
        .await;
        let ok = matches!(committed, Ok(Ok(_)));
        let answer = ok.then_some(Answer {
            instance: writer.epoch(),
            value: next,
        });
        self.record(Op::Incr, start, answer);
        if ok {
            *value = next;
        }
        ok
    }

    /// Commits until `count` are acknowledged, reopening the actor when its writer stops serving; fails the run when
    /// that takes longer than a minute. Unknown outcomes on the way stay in the history as such.
    async fn progress(&mut self, writer: &mut Writer, value: &mut u64, count: usize) -> usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        let (mut acked, mut reopened) = (0, 0);
        while acked < count {
            assert!(
                Instant::now() < deadline,
                "{acked} of {count} commits acknowledged within a minute of a restart"
            );
            if !writer.serving() {
                *writer = self.open().await;
                *value = self.history.calls.last().unwrap().answer.unwrap().value;
                reopened += 1;
            }
            if self.commit(writer, value).await {
                acked += 1;
            }
        }
        reopened
    }
}

/// Releases `writer`, failing the run if the release does not return.
async fn release(writer: Writer) {
    tokio::time::timeout(Duration::from_secs(30), writer.release(0))
        .await
        .expect("the release did not return within 30 s")
        .ok();
}

/// Whether three ready owners hold every `volume` range; otherwise what the relays answered. A new client each time: a
/// client keeps a table until told it is stale.
async fn ready_owners(peers: &Arc<Peers>, relays: Vec<String>) -> Result<(), String> {
    let client = Client::new(&Admin::new(), peers.clone(), relays, String::new());
    let table = client
        .routes(VOLUME, None)
        .await
        .map_err(|status| status.to_string())?;
    let directory = table.directories.get(VOLUME).cloned().unwrap_or_default();
    let all_ready = directory
        .values()
        .all(|owners| owners.len() == 3 && owners.iter().all(|o| o.state == State::Ready));
    if all_ready && !directory.is_empty() {
        Ok(())
    } else {
        Err(format!("{directory:?}"))
    }
}

async fn until_ready(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready(node).await {
        assert!(Instant::now() < deadline, "{} did not come back", node.name);
        sleep(Duration::from_millis(200)).await;
    }
}

/// Kills one slot at the chosen boundary of a commit, then restarts it. Returns whether the commit was acknowledged.
async fn kill_at(
    cluster: &Cluster,
    holder: &mut Holder,
    writer: &Writer,
    value: &mut u64,
    node: &Node,
    boundary: Boundary,
    rng: &mut StdRng,
) -> bool {
    let acknowledged = match boundary {
        Boundary::BeforeDelivery => {
            node.proxy.set(Mode::Stall);
            let acked = holder.commit(writer, value).await;
            cluster.kill(node);
            acked
        }
        Boundary::DuringWrite | Boundary::AfterWrite => {
            node.proxy.set(Mode::StallReplies);
            let delay = match boundary {
                Boundary::DuringWrite => Duration::from_micros(rng.gen_range(0..3_000)),
                _ => Duration::from_millis(200),
            };
            let killing = async {
                sleep(delay).await;
                cluster.kill(node);
            };
            let (acked, ()) = tokio::join!(holder.commit(writer, value), killing);
            acked
        }
        Boundary::Between => {
            let acked = holder.commit(writer, value).await;
            cluster.kill(node);
            acked
        }
    };
    node.proxy.set(Mode::Open);
    cluster.start(node);
    until_ready(node).await;
    acknowledged
}

/// Stops the cluster however the run ends: a failed assertion would otherwise leave its processes running, holding the
/// heavy-job lock a `flock` around the harness handed them.
struct Stopping<'a>(&'a Cluster);

impl Drop for Stopping<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// What a volume run saw.
#[derive(Debug)]
pub struct VolumeOutcome {
    pub kills: usize,
    pub acknowledged: usize,
    pub reopened: usize,
    pub value: u64,
}

/// Runs the rounds and checks the history.
pub async fn run(options: Options) -> Result<VolumeOutcome, Violation> {
    let mut rng = StdRng::seed_from_u64(options.seed);
    let cluster = Cluster::with(
        options.clone(),
        &[
            (Kind::Relay, "relay", 3),
            (Kind::Quicksilver, "quicksilver", 2),
            (Kind::Volume, "volume", 3),
        ],
    )
    .await;
    cluster.create_bucket().await;
    let _stopping = Stopping(&cluster);
    for node in &cluster.nodes {
        cluster.start(node);
    }
    let admin = Admin::new();
    // Relays answer routes to member services only, so the holder's `compute` certificate asks for them too.
    let tls = Tls::start(
        &certificate_paths(&options.dir, HOLDER),
        Identity {
            service: "compute".into(),
            cell: CELL.into(),
            node_id: HOLDER.into(),
            host: "127.0.0.1".into(),
        },
        &admin,
    )
    .await
    .unwrap();
    let peers = Arc::new(Peers::new(&admin).with_tls(Some(tls)));
    let client = Client::new(
        &admin,
        peers.clone(),
        cluster.addresses(Kind::Relay),
        String::new(),
    );
    let deadline = Instant::now() + Duration::from_secs(180);
    while let Err(answer) = ready_owners(&peers, cluster.addresses(Kind::Relay)).await {
        assert!(
            Instant::now() < deadline,
            "the volume nodes did not take the hash space ({answer}); logs in {}",
            options.dir.display()
        );
        sleep(Duration::from_millis(500)).await;
    }
    // Slots check claims against the region's topology record in Quicksilver.
    let topology = topology();
    let stream = ("quicksilver".to_owned(), "infrastructure".to_owned());
    let record = topology.encode_to_vec();
    while submit(
        &cluster.tls,
        &cluster.write_address(),
        &stream,
        "topology-eu",
        &ids::topology_key("eu"),
        Some(&record),
    )
    .await
    .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "the topology write did not commit"
        );
        sleep(Duration::from_millis(500)).await;
    }
    let regions = BTreeMap::from([("eu".to_owned(), topology.clone())]);
    let id = ActorId::from_name(&regions, "eu", "ns", "crash").unwrap();
    let mut holder = Holder {
        host: Host::start(
            peers,
            pb::Holder {
                cell: CELL.into(),
                host: HOLDER.into(),
            },
        ),
        client,
        actor: pb::Actor {
            namespace: "ns".into(),
            id: id.to_string(),
            kind: ids::Kind::Kv as i32,
        },
        id,
        started: Instant::now(),
        history: History::default(),
    };
    let volumes: Vec<&Arc<Node>> = cluster.of(Kind::Volume).collect();
    let (mut kills, mut reopened) = (0, 0);
    let mut writer = holder.open().await;
    let mut value = holder.history.calls[0].answer.unwrap().value;
    for round in 0..ROUNDS {
        let node = volumes[round % volumes.len()];
        let boundary = BOUNDARIES[(round / volumes.len()) % BOUNDARIES.len()];
        if !writer.serving() {
            writer = holder.open().await;
            value = holder.history.calls.last().unwrap().answer.unwrap().value;
            reopened += 1;
        }
        kill_at(
            &cluster,
            &mut holder,
            &writer,
            &mut value,
            node,
            boundary,
            &mut rng,
        )
        .await;
        kills += 1;
        reopened += holder.progress(&mut writer, &mut value, BETWEEN).await;
        if round % REOPEN == REOPEN - 1 {
            release(writer).await;
            writer = holder.open().await;
            value = holder.history.calls.last().unwrap().answer.unwrap().value;
            reopened += 1;
        }
    }
    release(writer).await;
    release(holder.open().await).await;
    holder.history.check()?;
    let calls = &holder.history.calls;
    Ok(VolumeOutcome {
        kills,
        acknowledged: calls
            .iter()
            .filter(|c| c.op == Op::Incr && c.answer.is_some())
            .count(),
        reopened,
        value: calls.last().unwrap().answer.unwrap().value,
    })
}
