//! B-10 acceptance: while a writer commits, one replica subscribes from the start and a second installs the first's
//! snapshot and subscribes from its boundary. A failed source read drops both streams and a subscriber restarts; each
//! resumes from its cursor. Every sequence must be applied exactly once, in order, and both replicas must end equal.
//! Runs over real HTTP/2 in normal builds and simulated under `--cfg madsim`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::subscriptions::pb::replication_server::ReplicationServer;
use bedrock_lib::replication::subscriptions::{Log, MemoryLog, Source, Subscription, Watched};
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::replication::{Applier, ApplierMetrics, ApplyHook, Event, Operation, stream};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::storage::{KeyRange, StorageEngine, StorageResult, WriteBatch};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tonic::Status;

const NAMESPACES: [&str; 3] = ["a", "b", "c"];
const WRITES: u64 = 3000;

/// A [`MemoryLog`] whose next read can be made to fail, dropping every stream that hits it.
#[derive(Default)]
struct FlakyLog {
    log: MemoryLog,
    fail: AtomicBool,
}

impl Log for FlakyLog {
    async fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>, Status> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("injected read failure"));
        }
        self.log.read(dataset, namespace, from, limit).await
    }

    fn head(&self, dataset: &str, namespace: &str) -> Arc<Watched<u64>> {
        self.log.head(dataset, namespace)
    }
}

/// Stores values under `{namespace}\0{key}` and records every sequence it applies.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<(String, u64)>>>);

impl ApplyHook for Recorder {
    fn apply(
        &mut self,
        _: &dyn StorageEngine,
        e: &Event,
        batch: &mut WriteBatch,
    ) -> StorageResult<()> {
        let key = [e.namespace.as_bytes(), b"\0", &e.key].concat();
        match &e.operation {
            Operation::Set(value) => batch.put(key, value.clone()),
            Operation::Delete => batch.delete(key),
        };
        self.0
            .lock()
            .unwrap()
            .push((e.namespace.clone(), e.sequence));
        Ok(())
    }
}

impl Recorder {
    fn sequences(&self, namespace: &str) -> Vec<u64> {
        let applied = self.0.lock().unwrap();
        applied
            .iter()
            .filter(|(n, _)| n == namespace)
            .map(|&(_, s)| s)
            .collect()
    }
}

/// splitmix64: seeded, so a failing run replays exactly under madsim.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % n
    }
}

/// Commits sets and deletes over a few keys per namespace, pausing now and then so subscribers interleave. Halfway it
/// waits for `resume`, so the second half is sure to be read after the faults are set up.
async fn write(log: Arc<FlakyLog>, resume: oneshot::Receiver<()>) -> BTreeMap<String, u64> {
    let mut rng = Rng(7);
    let mut heads = BTreeMap::<String, u64>::new();
    let mut resume = Some(resume);
    for i in 0..WRITES {
        if i == WRITES / 2 {
            resume.take().unwrap().await.unwrap();
        }
        let namespace = NAMESPACES[rng.below(3) as usize];
        let head = heads.entry(namespace.into()).or_default();
        *head += 1;
        let key = vec![b'x' + rng.below(4) as u8];
        let operation = if rng.below(5) == 0 {
            Operation::Delete
        } else {
            Operation::Set(i.to_be_bytes().to_vec())
        };
        log.log
            .append(Event {
                dataset: "kv".into(),
                namespace: namespace.into(),
                stream: stream("kv", 0),
                sequence: *head,
                epoch: 1,
                committed_at: 1_700_000_000_000 + i,
                key,
                operation,
            })
            .unwrap();
        if rng.below(20) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }
    heads
}

fn applier(engine: Arc<MemoryEngine>, hook: Recorder) -> Applier<Recorder> {
    Applier::new("kv", engine, hook, &ApplierMetrics::default())
}

fn namespaces() -> Vec<String> {
    NAMESPACES.map(String::from).to_vec()
}

fn state(engine: &MemoryEngine) -> Vec<(Vec<u8>, Vec<u8>)> {
    engine
        .scan(&KeyRange::all())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

async fn caught_up(subscription: &Subscription, heads: &BTreeMap<String, u64>) {
    for (namespace, &head) in heads {
        timeout(
            Duration::from_secs(30),
            subscription.wait_for(namespace, head),
        )
        .await
        .unwrap_or_else(|_| panic!("{namespace} did not reach {head}"));
    }
}

/// The subscriber side; `server` serves `log`.
async fn scenario(server: String, log: Arc<FlakyLog>) {
    let peers = Arc::new(Peers::new(&Admin::new()));
    let (resume, paused) = oneshot::channel();
    let writer = tokio::spawn(write(log.clone(), paused));

    let first = Arc::new(MemoryEngine::new("kv"));
    let first_applied = Recorder::default();
    let start = |engine: &Arc<MemoryEngine>, hook: &Recorder| {
        let applier = applier(engine.clone(), hook.clone());
        Subscription::start(peers.clone(), server.clone(), applier, namespaces())
    };
    let subscription = start(&first, &first_applied);

    // Mid-stream, the second replica installs the first's snapshot and subscribes from its boundary.
    timeout(Duration::from_secs(30), subscription.wait_for("a", 100))
        .await
        .unwrap();
    let second = Arc::new(MemoryEngine::new("kv"));
    second.restore(first.snapshot().unwrap().as_ref()).unwrap();
    let boundary = second.snapshot().unwrap().revisions().unwrap();
    let second_applied = Recorder::default();
    let second_subscription = start(&second, &second_applied);

    // The next source read fails and ends a stream, and the first subscriber restarts from its engine.
    log.fail.store(true, Ordering::SeqCst);
    subscription.stop().await;
    let subscription = start(&first, &first_applied);
    resume.send(()).unwrap();

    let heads = writer.await.unwrap();
    caught_up(&subscription, &heads).await;
    caught_up(&second_subscription, &heads).await;
    assert!(!log.fail.load(Ordering::SeqCst), "no read failed");

    for (namespace, &head) in &heads {
        let after = boundary.get(namespace).copied().unwrap_or(0);
        assert!(
            after > 0 || namespace != "a",
            "snapshot taken before any write"
        );
        let expected: Vec<u64> = (1..=head).collect();
        assert_eq!(first_applied.sequences(namespace), expected, "{namespace}");
        let expected: Vec<u64> = (after + 1..=head).collect();
        assert_eq!(second_applied.sequences(namespace), expected, "{namespace}");
    }
    assert_eq!(state(&first), state(&second));
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_then_subscribe_under_concurrent_writes() {
    let log = Arc::new(FlakyLog::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ReplicationServer::new(Source::new(log.clone())))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    scenario(addr.to_string(), log).await;
}

#[cfg(madsim)]
#[madsim::test]
async fn snapshot_then_subscribe_under_concurrent_writes() {
    simulated().await;
}

/// One seed must replay one schedule (B-23a). Append any seed that ever failed.
#[cfg(madsim)]
#[test]
fn same_seed_same_schedule() {
    for seed in [1, 2, 3, 42] {
        madsim::runtime::Runtime::check_determinism(seed, madsim::Config::default(), simulated);
    }
}

#[cfg(madsim)]
async fn simulated() {
    let handle = madsim::runtime::Handle::current();
    let server: std::net::SocketAddr = "10.0.0.2:7000".parse().unwrap();
    let log = Arc::new(FlakyLog::default());
    let served = log.clone();
    handle
        .create_node()
        .name("source")
        .ip(server.ip())
        .build()
        .spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ReplicationServer::new(Source::new(served)))
                .serve(server)
                .await
                .unwrap();
        });
    handle
        .create_node()
        .name("replicas")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            // Let the source bind first.
            sleep(Duration::from_millis(10)).await;
            scenario(server.to_string(), log).await;
        })
        .await
        .unwrap();
}

/// An expired cursor requires a consistent materialized-state bootstrap, then an
/// ordinary subscription from target+1. The service lifecycle controls readiness.
async fn materialized_recovery_scenario(server: String, writer: Arc<Writer>) {
    let write = Write {
        dataset: "kv".into(),
        namespace: "a".into(),
        key: b"k".to_vec(),
        operation: Operation::Set(b"one".to_vec()),
        idempotency_key: None,
    };
    assert_eq!(writer.commit(write.clone()).await.unwrap(), 1);
    // Model a cursor older than the source's retained stream window.
    writer.log().prune("kv", "a", 1).unwrap();
    let engine = Arc::new(MemoryEngine::new("kv"));
    let peers = Arc::new(Peers::new(&Admin::new()));
    let first = Subscription::start(
        peers.clone(),
        server.clone(),
        applier(engine.clone(), Recorder::default()),
        vec!["a".into()],
    );
    let reason = timeout(Duration::from_secs(5), first.recovery_required())
        .await
        .unwrap();
    assert_eq!(reason.code(), tonic::Code::OutOfRange);
    first.stop().await;
    writer
        .bootstrap("kv", "a")
        .await
        .unwrap()
        .install(engine.as_ref(), "a", KeyRange::prefix(b"a\0"))
        .unwrap();
    let recorded = Recorder::default();
    let resumed = Subscription::start(
        peers,
        server,
        applier(engine.clone(), recorded.clone()),
        vec!["a".into()],
    );
    assert_eq!(
        writer
            .commit(Write {
                operation: Operation::Delete,
                ..write
            })
            .await
            .unwrap(),
        2
    );
    timeout(Duration::from_secs(5), resumed.wait_for("a", 2))
        .await
        .unwrap();
    assert_eq!(recorded.sequences("a"), vec![2]);
    assert_eq!(engine.revision("a").unwrap(), Some(2));
    assert_eq!(engine.get(b"a\0k").unwrap(), None);
    resumed.stop().await;
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn expired_cursor_bootstraps_then_resumes_after_materialized_boundary() {
    let writer = Writer::open(
        || 123,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ReplicationServer::new(Source::new(writer.log())))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    materialized_recovery_scenario(addr.to_string(), writer).await;
    server.abort();
}

#[cfg(madsim)]
#[madsim::test]
async fn expired_cursor_bootstraps_then_resumes_after_materialized_boundary() {
    let handle = madsim::runtime::Handle::current();
    let addr: std::net::SocketAddr = "10.0.0.2:7000".parse().unwrap();
    let writer = Writer::open(
        || 123,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let log = writer.log();
    handle
        .create_node()
        .name("source")
        .ip(addr.ip())
        .build()
        .spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ReplicationServer::new(Source::new(log)))
                .serve(addr)
                .await
                .unwrap();
        });
    handle
        .create_node()
        .name("replica")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            sleep(Duration::from_millis(10)).await;
            materialized_recovery_scenario(addr.to_string(), writer).await;
        })
        .await
        .unwrap();
}
