//! #14/B-17: real relay nodes fail over after idle partitions and task-killing crashes.
#![cfg(madsim)]

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::health::State;
use bedrock_lib::node::{Budget, Location, NodeConfig, Role, ServiceConfig};
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::storage::{
    KeyRange, Scan, Snapshot, StorageEngine, StorageResult, StorageStats, WriteBatch,
};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::time::{sleep, timeout};

const WRITER: &str = "10.0.0.1:7000";
const TOP: &str = "10.0.0.2:7000";
const OTHER_TOP: &str = "10.0.0.3:7000";
const CELL: &str = "10.0.0.4:7000";
const OTHER_CELL: &str = "10.0.0.5:7000";
const DEADLINE: Duration = Duration::from_secs(90);

/// Records committed batches rather than simulated or wall-clock timestamps.
struct Recorded {
    engine: MemoryEngine,
    batches: Mutex<Vec<String>>,
}

impl Recorded {
    fn new() -> Self {
        Self {
            engine: MemoryEngine::new("auth"),
            batches: Mutex::default(),
        }
    }
    fn hash(&self) -> u64 {
        let mut hash = DefaultHasher::new();
        self.batches.lock().unwrap().hash(&mut hash);
        hash.finish()
    }
}

impl StorageEngine for Recorded {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.engine.get(key)
    }
    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        self.engine.scan(range)
    }
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        let trace = format!("{batch:?}");
        self.engine.write(batch)?;
        self.batches.lock().unwrap().push(trace);
        Ok(())
    }
    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.engine.revision(name)
    }
    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        self.engine.snapshot()
    }
    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        self.engine.restore(from)
    }
    fn stats(&self) -> StorageResult<StorageStats> {
        self.engine.stats()
    }
    fn disk_usage(&self) -> StorageResult<u64> {
        self.engine.disk_usage()
    }
    fn health(&self) -> StorageResult<()> {
        self.engine.health()
    }
}

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}

fn serve(name: &str, address: &str, hub: Hub, upstream: bool) -> NodeHandle {
    let socket: std::net::SocketAddr = address.parse().unwrap();
    let node = Handle::current()
        .create_node()
        .name(name)
        .ip(socket.ip())
        .build();
    node.spawn(async move {
        let task = upstream.then(|| hub.start());
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve(socket)
            .await
            .unwrap();
        if let Some(task) = task {
            task.abort();
        }
    });
    node
}

fn relay(name: &str, tier: u32, sources: &[&str], peers: Arc<Peers>) -> Hub {
    Hub::relay(
        name.into(),
        tier,
        peers,
        sources.iter().map(|s| (*s).into()).collect(),
        Vec::new(),
        Some(Arc::new(
            Replay::open(Arc::new(MemoryEngine::new(name)), ReplayConfig::default()).unwrap(),
        )),
    )
}

async fn caught_up(session: &Session, revision: u64) {
    timeout(DEADLINE, session.wait_for("auth", "accounts", revision))
        .await
        .unwrap_or_else(|_| panic!("revision {revision} stalled: {:?}", session.error()));
}

async fn phase(writer: &Arc<Writer>, session: &Session, first: u64, last: u64) -> Option<Vec<u8>> {
    let mut expected = None;
    for revision in first..=last {
        let value = [
            revision.to_be_bytes(),
            thread_rng().r#gen::<u64>().to_be_bytes(),
        ]
        .concat();
        let operation = if revision.is_multiple_of(7) {
            Operation::Delete
        } else {
            Operation::Set(value.clone())
        };
        expected = if revision.is_multiple_of(7) {
            None
        } else {
            Some(value)
        };
        assert_eq!(
            writer
                .commit(Write {
                    dataset: "auth".into(),
                    namespace: "accounts".into(),
                    key: b"key".to_vec(),
                    operation,
                    idempotency_key: None
                })
                .await
                .unwrap(),
            revision
        );
        sleep(Duration::from_millis(thread_rng().gen_range(1..8))).await;
    }
    caught_up(session, last).await;
    expected
}

async fn scenario() -> u64 {
    let handle = Handle::current();
    let writer = Writer::open(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let source = serve(
        "writer",
        WRITER,
        Hub::writer("writer".into(), writer.clone(), peers()),
        false,
    );
    let top = serve("top", TOP, relay("top", 3, &[WRITER], peers()), true);
    let other_top = serve(
        "other-top",
        OTHER_TOP,
        relay("other-top", 3, &[WRITER], peers()),
        true,
    );
    let first_cell_peers = peers();
    let cell = serve(
        "cell",
        CELL,
        relay("cell", 1, &[TOP, OTHER_TOP], first_cell_peers),
        true,
    );
    let kv = cell.spawn(async {
        bedrock_kv::run(ServiceConfig {
            node: NodeConfig {
                node_id: "kv-neighbor".into(),
                location: Location {
                    region: "eu".into(),
                    dc: "eu-1".into(),
                    cell: "eu-1a".into(),
                    failure_domain: String::new(),
                },
                listen: "10.0.0.4:7100".parse().unwrap(),
                advertise: None,
                admin_listen: "10.0.0.4:7101".parse().unwrap(),
                data_dir: "/data/kv-neighbor".into(),
                budget: Budget::default(),
            },
            roles: vec![Role::L1],
            datasets: vec!["quicksilver".into()],
            relays: vec![CELL.into()],
            api: WRITER.into(),
            s3_endpoint: "http://10.0.0.99:9000".into(),
            subscriptions: Vec::new(),
        })
        .await
        .unwrap();
    });
    let other_cell_peers = peers();
    let other_cell = serve(
        "other-cell",
        OTHER_CELL,
        relay("other-cell", 1, &[TOP, OTHER_TOP], other_cell_peers.clone()),
        true,
    );
    let leaf = handle
        .create_node()
        .name("leaf")
        .ip("10.0.0.10".parse().unwrap())
        .build();
    let cell_id = cell.id();
    let leaf_id = leaf.id();
    let result = leaf
        .spawn(async move {
            sleep(Duration::from_millis(20)).await;
            let engine = Arc::new(Recorded::new());
            let admin = Admin::new();
            let leaf_peers = Arc::new(Peers::new(&admin));
            let session = Session::start(
                leaf_peers.clone(),
                vec![CELL.into(), OTHER_CELL.into()],
                vec![Target {
                    selection: Selection {
                        dataset: "auth".into(),
                        namespace: "accounts".into(),
                        buckets: Vec::new(),
                    },
                    engine: engine.clone(),
                    prefix: b"accounts\0".to_vec(),
                }],
            )
            .unwrap();
            phase(&writer, &session, 1, 6).await;
            assert!(!kv.is_finished(), "co-located KV entry point did not start");
            kv.abort();
            assert!(kv.await.unwrap_err().is_cancelled());
            let expected = phase(&writer, &session, 7, 12).await;
            assert_eq!(engine.get(b"accounts\0key").unwrap(), expected);
            sleep(Duration::from_secs(35)).await;
            assert!(
                session.ready(),
                "healthy idle ticks must keep the session ready"
            );
            assert_eq!(leaf_peers.health().state(CELL), State::Closed);

            // Preserve both nodes and their tasks; only the existing transport link stops carrying progress.
            let network = madsim::net::NetSim::current();
            network.clog_link(cell_id, leaf_id);
            network.clog_link(leaf_id, cell_id);
            timeout(Duration::from_secs(45), async {
                while leaf_peers.health().state(CELL) != State::Open {
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("idle partition was not detected by the stream deadline");
            let expected = phase(&writer, &session, 13, 24).await;
            assert_eq!(engine.get(b"accounts\0key").unwrap(), expected);

            // Killing this node aborts its accept loop, outbound loop and all namespace pumps.
            Handle::current().kill("top");
            let expected = phase(&writer, &session, 25, 36).await;
            assert_eq!(engine.get(b"accounts\0key").unwrap(), expected);
            assert_ne!(other_cell_peers.health().state(TOP), State::Closed);

            network.unclog_link(cell_id, leaf_id);
            network.unclog_link(leaf_id, cell_id);
            for _ in 0..8 {
                leaf_peers.health().trip(CELL);
                leaf_peers.health().trip(OTHER_CELL);
            }
            Handle::current().kill("other-cell");
            let expected = phase(&writer, &session, 37, 48).await;
            assert_eq!(engine.get(b"accounts\0key").unwrap(), expected);
            let fail_open = admin
                .render()
                .lines()
                .find_map(|line| {
                    line.strip_prefix("bedrock_health_fail_open_total ")
                        .and_then(|value| value.parse::<u64>().ok())
                })
                .unwrap_or(0);
            assert!(
                fail_open > 0,
                "all-open breakers must still permit recovery attempts"
            );
            assert!(session.ready());

            // Server-to-client ticks still arrive. Only the server's incoming-progress deadline can end this stream.
            network.clog_link(leaf_id, cell_id);
            timeout(Duration::from_secs(45), async {
                while session.ready() {
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("one-way idle partition left a dead subscriber registered");
            assert!(!session.ready());
            network.unclog_link(leaf_id, cell_id);
            timeout(DEADLINE, async {
                while !session.ready() {
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("healed subscriber did not become ready again");
            let expected = phase(&writer, &session, 49, 52).await;
            assert_eq!(engine.get(b"accounts\0key").unwrap(), expected);
            assert!(!engine.batches.lock().unwrap().is_empty());
            drop(session);
            writer.close().await.unwrap();
            engine.hash()
        })
        .await
        .unwrap();
    for node in [source, top, other_top, cell, other_cell, leaf] {
        handle.kill(node.id());
    }
    result
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario()))
        .join().unwrap_or_else(|failure| {
            eprintln!("Replay: BEDROCK_RELAY_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test relays -- --nocapture");
            std::panic::resume_unwind(failure)
        })
}

#[test]
fn relay_faults_replay_the_same_committed_application_trace() {
    let seeds = std::env::var("BEDROCK_RELAY_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42]);
    for seed in seeds {
        let first = run(seed);
        assert_eq!(first, run(seed), "seed {seed} changed its committed trace");
        println!("relay seed {seed}: {first:016x}");
    }
}
