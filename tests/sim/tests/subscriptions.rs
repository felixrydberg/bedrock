//! B-10 acceptance through Relay.Session: snapshot boundaries, source disconnects,
//! subscriber restarts and obsolete-cursor recovery preserve committed ordering.
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::replication::pb;
use bedrock_lib::storage::{
    KeyRange, Scan, Snapshot, StorageEngine, StorageResult, StorageStats, WriteBatch, WriteOp,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
const NAMESPACES: [&str; 3] = ["a", "b", "c"];
const WRITES: u64 = 3000;
/// Records successful commits together with their selector-scoped revisions.
struct Recorded {
    engine: MemoryEngine,
    batches: Mutex<Vec<WriteBatch>>,
}
impl Recorded {
    fn new() -> Self {
        Self {
            engine: MemoryEngine::new("auth"),
            batches: Mutex::default(),
        }
    }
    fn operations(&self, namespace: &str) -> Vec<WriteOp> {
        let prefix = [namespace.as_bytes(), b"\0"].concat();
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flat_map(|b| b.ops.iter())
            .filter(|op| match op {
                WriteOp::Put { key, .. } | WriteOp::Delete { key } => key.starts_with(&prefix),
                WriteOp::DeleteRange(_) => false,
            })
            .cloned()
            .collect()
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
        self.engine.write(batch.clone())?;
        self.batches.lock().unwrap().push(batch);
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

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}
async fn open_writer() -> Arc<Writer> {
    Writer::open(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap()
}
fn start(server: &str, engine: Arc<Recorded>) -> Session {
    Session::start(
        peers(),
        vec![server.into()],
        NAMESPACES
            .into_iter()
            .map(|namespace| Target {
                selection: Selection {
                    dataset: "auth".into(),
                    namespace: namespace.into(),
                    buckets: Vec::new(),
                },
                engine: engine.clone(),
                prefix: [namespace.as_bytes(), b"\0"].concat(),
            })
            .collect(),
    )
    .unwrap()
}
async fn caught_up(session: &Session, namespace: &str, revision: u64) {
    timeout(
        Duration::from_secs(30),
        session.wait_for("auth", namespace, revision),
    )
    .await
    .unwrap_or_else(|_| panic!("{namespace} stalled at {revision}: {:?}", session.error()));
}
async fn write(
    writer: Arc<Writer>,
    resume: oneshot::Receiver<()>,
) -> BTreeMap<String, Vec<WriteOp>> {
    let mut rng = Rng(7);
    let mut expected = BTreeMap::<String, Vec<WriteOp>>::new();
    let mut resume = Some(resume);
    for i in 0..WRITES {
        if i == WRITES / 2 {
            resume.take().unwrap().await.unwrap();
        }
        let namespace = NAMESPACES[rng.below(3) as usize];
        let key = vec![b'x' + rng.below(4) as u8];
        let physical = [namespace.as_bytes(), b"\0", &key].concat();
        let operation = if rng.below(5) == 0 {
            Operation::Delete
        } else {
            Operation::Set(i.to_be_bytes().to_vec())
        };
        let op = match &operation {
            Operation::Set(value) => WriteOp::Put {
                key: physical,
                value: value.clone(),
            },
            Operation::Delete => WriteOp::Delete { key: physical },
        };
        let sequence = writer
            .commit(Write {
                dataset: "auth".into(),
                namespace: namespace.into(),
                key,
                operation,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let events = expected.entry(namespace.into()).or_default();
        events.push(op);
        assert_eq!(sequence, events.len() as u64);
        if rng.below(20) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }
    expected
}
#[cfg(not(madsim))]
mod sockets {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    /// Cancellation closes established HTTP/2 sockets, not only the listener.
    pub struct Socket {
        stream: tokio::net::TcpStream,
        stopped: Pin<Box<dyn Future<Output = ()> + Send>>,
        closed: bool,
    }
    impl Socket {
        pub fn new(
            stream: tokio::net::TcpStream,
            mut stop: tokio::sync::watch::Receiver<bool>,
        ) -> Self {
            Self {
                stream,
                closed: false,
                stopped: Box::pin(async move {
                    while !*stop.borrow_and_update() {
                        if stop.changed().await.is_err() {
                            break;
                        }
                    }
                }),
            }
        }
        fn closed(&mut self, cx: &mut Context<'_>) -> bool {
            self.closed = self.closed || self.stopped.as_mut().poll(cx).is_ready();
            self.closed
        }
    }
    impl tonic::transport::server::Connected for Socket {
        type ConnectInfo = ();
        fn connect_info(&self) {}
    }
    impl AsyncRead for Socket {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.closed(cx) {
                return Poll::Ready(Ok(()));
            }
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Socket {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.closed(cx) {
                return Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
            }
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
}
#[cfg(not(madsim))]
async fn source(
    address: std::net::SocketAddr,
    writer: Arc<Writer>,
    mut restart: mpsc::Receiver<oneshot::Sender<()>>,
) {
    use tokio_stream::StreamExt;
    loop {
        let hub = Hub::writer("writer".into(), writer.clone(), peers());
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener)
            .map(move |stream| stream.map(|stream| sockets::Socket::new(stream, stopped.clone())));
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(pb::relay_server::RelayServer::new(hub))
                .serve_with_incoming(incoming),
        );
        let command = restart.recv().await;
        let _ = stop.send(true);
        task.abort();
        let _ = task.await;
        if let Some(ack) = command {
            sleep(Duration::from_millis(10)).await;
            ack.send(()).unwrap();
        } else {
            return;
        }
    }
}
#[cfg(madsim)]
async fn source(
    address: std::net::SocketAddr,
    writer: Arc<Writer>,
    mut restart: mpsc::Receiver<oneshot::Sender<()>>,
) {
    let handle = madsim::runtime::Handle::current();
    let node = handle
        .create_node()
        .name("source")
        .ip(address.ip())
        .init(move || {
            let hub = Hub::writer("writer".into(), writer.clone(), peers());
            async move {
                tonic::transport::Server::builder()
                    .add_service(pb::relay_server::RelayServer::new(hub))
                    .serve(address)
                    .await
                    .unwrap();
            }
        })
        .build();
    while let Some(ack) = restart.recv().await {
        handle.kill(node.id());
        sleep(Duration::from_millis(10)).await;
        handle.restart(node.id());
        ack.send(()).unwrap();
    }
    handle.kill(node.id());
}
async fn scenario(server: String, writer: Arc<Writer>, restart: mpsc::Sender<oneshot::Sender<()>>) {
    let (resume, paused) = oneshot::channel();
    let writing = tokio::spawn(write(writer.clone(), paused));
    let first = Arc::new(Recorded::new());
    let session = start(&server, first.clone());
    caught_up(&session, "a", 100).await;
    let second = Arc::new(Recorded::new());
    // Extract per-namespace boundaries from the same atomic snapshot copied to the second replica.
    let snapshot = first.snapshot().unwrap();
    second.restore(snapshot.as_ref()).unwrap();
    let boundary = snapshot.revisions().unwrap();
    drop(snapshot);
    let second_session = start(&server, second.clone());
    let (ack, restarted) = oneshot::channel();
    restart.send(ack).await.unwrap();
    session.stop().await;
    let session = start(&server, first.clone());
    resume.send(()).unwrap();
    restarted.await.unwrap();
    let expected = writing.await.unwrap();
    for (namespace, ops) in expected {
        caught_up(&session, &namespace, ops.len() as u64).await;
        caught_up(&second_session, &namespace, ops.len() as u64).await;
        assert_eq!(
            first.operations(&namespace),
            ops,
            "{namespace}: duplicate or reordered application"
        );
        // Locate the opaque cursor by the namespace's committed data batch, without reproducing its hash.
        let cursor = first
            .batches
            .lock()
            .unwrap()
            .iter()
            .find(|batch| {
                batch.ops.iter().any(|op| match op {
                    WriteOp::Put { key, .. } | WriteOp::Delete { key } => {
                        key.starts_with(&[namespace.as_bytes(), b"\0"].concat())
                    }
                    WriteOp::DeleteRange(_) => false,
                })
            })
            .unwrap()
            .revisions
            .keys()
            .find(|name| name.starts_with("relay:cursor:"))
            .unwrap()
            .clone();
        let after = boundary.get(&cursor).copied().unwrap_or(0) as usize;
        assert_eq!(
            second.operations(&namespace),
            ops[after..],
            "{namespace}: snapshot cursor was not resumed"
        );
    }
    assert_eq!(
        first.snapshot().unwrap().revisions().unwrap(),
        second.snapshot().unwrap().revisions().unwrap()
    );
    let state = |engine: &Recorded| {
        engine
            .scan(&KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
    };
    assert_eq!(state(&first), state(&second));
    session.stop().await;
    second_session.stop().await;
    writer.close().await.unwrap();
}
/// An obsolete Follow cursor triggers automatic bootstrap over the same session.
async fn materialized_recovery_scenario(server: String, writer: Arc<Writer>) {
    let write = Write {
        dataset: "auth".into(),
        namespace: "a".into(),
        key: b"k".to_vec(),
        operation: Operation::Set(b"one".to_vec()),
        idempotency_key: None,
    };
    assert_eq!(writer.commit(write.clone()).await.unwrap(), 1);
    writer.log().prune("auth", "a", 1).unwrap();
    let engine = Arc::new(Recorded::new());
    let session = start(&server, engine.clone());
    caught_up(&session, "a", 1).await;
    assert_eq!(engine.get(b"a\0k").unwrap(), Some(b"one".to_vec()));
    assert!(engine.batches.lock().unwrap().iter().any(|b| {
        b.ops
            .contains(&WriteOp::DeleteRange(KeyRange::prefix(b"a\0")))
    }));
    let installed = engine.snapshot().unwrap().revisions().unwrap();
    session.stop().await;
    let resumed = start(&server, engine.clone());
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
    caught_up(&resumed, "a", 2).await;
    assert_eq!(
        engine.operations("a"),
        vec![
            WriteOp::Put {
                key: b"a\0k".to_vec(),
                value: b"one".to_vec()
            },
            WriteOp::Delete {
                key: b"a\0k".to_vec()
            }
        ]
    );
    assert_eq!(engine.get(b"a\0k").unwrap(), None);
    let revisions = engine.snapshot().unwrap().revisions().unwrap();
    let cursor = installed
        .iter()
        .find(|(name, value)| name.starts_with("relay:cursor:") && **value == 1)
        .unwrap()
        .0;
    assert_eq!(revisions[cursor], 2);
    resumed.stop().await;
    writer.close().await.unwrap();
}
#[cfg(not(madsim))]
async fn run(recovery: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let writer = open_writer().await;
    let (restart, commands) = mpsc::channel(1);
    let serving = tokio::spawn(source(addr, writer.clone(), commands));
    if recovery {
        materialized_recovery_scenario(addr.to_string(), writer).await;
    } else {
        scenario(addr.to_string(), writer, restart).await;
    }
    serving.abort();
    let _ = serving.await;
}
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_then_subscribe_under_concurrent_writes() {
    run(false).await;
}
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn expired_cursor_bootstraps_then_resumes_after_materialized_boundary() {
    run(true).await;
}
#[cfg(madsim)]
async fn simulated(recovery: bool) {
    let handle = madsim::runtime::Handle::current();
    let addr: std::net::SocketAddr = "10.0.0.2:7000".parse().unwrap();
    let writer = open_writer().await;
    let (restart, commands) = mpsc::channel(1);
    let serving = tokio::spawn(source(addr, writer.clone(), commands));
    let replica_restart = restart.clone();
    handle
        .create_node()
        .name("replicas")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            sleep(Duration::from_millis(10)).await;
            if recovery {
                materialized_recovery_scenario(addr.to_string(), writer).await;
            } else {
                scenario(addr.to_string(), writer, replica_restart).await;
            }
        })
        .await
        .unwrap();
    drop(restart);
    serving.await.unwrap();
}
#[cfg(madsim)]
#[madsim::test]
async fn snapshot_then_subscribe_under_concurrent_writes() {
    simulated(false).await;
}
#[cfg(madsim)]
#[madsim::test]
async fn expired_cursor_bootstraps_then_resumes_after_materialized_boundary() {
    simulated(true).await;
}
#[cfg(madsim)]
#[test]
fn same_seed_same_schedule() {
    for seed in [1, 2, 3, 42] {
        madsim::runtime::Runtime::check_determinism(seed, madsim::Config::default(), || {
            simulated(false)
        });
    }
}
