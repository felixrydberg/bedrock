//! #80 / B-32 acceptance over the real write and relay path: deployments published in bulk reach, in every cell, exactly
//! the holders of each key's range, with ref keys placed by their artifact's content hash; a shared artifact stays
//! retained until its last deployment is released; and a holder whose local state is wiped rebuilds identical rows and
//! ref keys through range bootstrap while writes continue. Runs over real sockets in normal builds and simulated under
//! `--cfg madsim`. Bootstrap validation (corruption, interruption, lease expiry) is dataset-generic and covered by the
//! relay client and server tests in `bedrock-lib`.

use std::sync::Arc;
use std::time::Duration;

use bedrock_cdn::artifact_storage::Hash;
use bedrock_cdn::paths::{PathRow, path_key, ref_key};
use bedrock_cdn::retention::retained;
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{content, hash_of, in_span};
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::sim::MemoryFactory;
use bedrock_lib::storage::{KeyRange, StorageEngine, l2_parts};
use bedrock_lib::transport::Peers;

const NAMESPACE: &str = "deployments";
const HALF: u64 = 1 << 63;
const HALVES: [(u64, u64); 2] = [(0, HALF), (HALF, 0)];
const DEADLINE: Duration = Duration::from_secs(30);

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(not(madsim))]
async fn serve(hub: Hub, _: u16) -> (String, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, Server(task))
}

#[cfg(madsim)]
async fn serve(hub: Hub, port: u16) -> (String, Server) {
    let address = format!("127.0.0.1:{port}");
    let socket = address.parse().unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve(socket)
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    (address, Server(task))
}

async fn relay(id: &str, tier: u32, upstream: &str, port: u16) -> (String, Server, Server) {
    let hub = Hub::relay(
        id.into(),
        tier,
        peers(),
        vec![upstream.into()],
        vec![upstream.into()],
        Some(Arc::new(
            Replay::open(Arc::new(MemoryEngine::new(id)), ReplayConfig::default()).unwrap(),
        )),
    );
    let upstream = Server(hub.start());
    let (address, server) = serve(hub, port).await;
    (address, server, upstream)
}

/// `(hash, namespace, key, value)` of one stored record.
type Row = (u64, Vec<u8>, Vec<u8>, Vec<u8>);

/// A `bedrock-cdn` range holder: a local engine following one hash range of `cdn` through its cell relay.
struct Holder {
    engine: Arc<MemoryEngine>,
    session: Session,
    span: (u64, u64),
}

impl Holder {
    fn start(cell: &str, span: (u64, u64)) -> Self {
        let engine = Arc::new(MemoryEngine::new("cdn"));
        let target = Target {
            selection: Selection {
                dataset: "cdn".into(),
                namespace: String::new(),
            },
            engine: engine.clone(),
            prefix: Vec::new(),
            span: Some(span),
        };
        let session = Session::start(peers(), vec![cell.into()], vec![target]).unwrap();
        Holder {
            engine,
            session,
            span,
        }
    }

    /// Every stored `(hash, namespace, key, value)` in the hash-first layout.
    fn rows(&self) -> Vec<Row> {
        self.engine
            .scan(&KeyRange::prefix(*b"l2/"))
            .unwrap()
            .map(|entry| {
                let (physical, value) = entry.unwrap();
                let hash = u64::from_be_bytes(physical[3..11].try_into().unwrap());
                let (namespace, key) = l2_parts(&physical).unwrap();
                (hash, namespace.to_vec(), key.to_vec(), value)
            })
            .collect()
    }
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Every path row and ref key of a deployment, or their deletions, in one bulk.
async fn publish(writer: &Arc<Writer>, deployment: &str, files: &[(&str, Hash)], release: bool) {
    let changes = files
        .iter()
        .flat_map(|(path, sha256)| {
            let row = PathRow {
                sha256: *sha256,
                content_type: "text/plain".into(),
                size: 1,
            };
            [
                (path_key(deployment, path).unwrap(), row.encode().unwrap()),
                (ref_key(sha256, deployment).unwrap(), Vec::new()),
            ]
        })
        .map(|(key, value)| (key, (!release).then_some(value)))
        .collect();
    writer
        .commit(Write {
            dataset: "cdn".into(),
            namespace: NAMESPACE.into(),
            key: Vec::new(),
            operation: Operation::Bulk(changes),
            idempotency_key: None,
            expected: None,
        })
        .await
        .unwrap();
}

async fn scenario() {
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
    let (source, _source) =
        serve(Hub::writer("writer".into(), writer.clone(), peers()), 18001).await;
    let (top, _top, _top_upstream) = relay("top", 3, &source, 18002).await;
    // Two cells, each with one holder per half of the hash space.
    let mut cells = Vec::new();
    for (i, cell) in ["cell-a", "cell-b"].into_iter().enumerate() {
        let (address, server, upstream) = relay(cell, 1, &top, 18003 + i as u16).await;
        let holders = HALVES.map(|span| Holder::start(&address, span));
        cells.push((address, holders, server, upstream));
    }
    // One artifact in each half; `shared` is in both deployments.
    let shared = Hash([0xf0; 32]);
    let own = Hash([0x10; 32]);
    assert!(content(&shared.0) >= HALF && content(&own.0) < HALF);
    publish(
        &writer,
        "d1",
        &[("/index.html", shared), ("/a.css", own)],
        false,
    )
    .await;
    publish(&writer, "d2", &[("/index.html", shared)], false).await;
    let all = || cells.iter().flat_map(|(_, holders, ..)| holders.iter());
    let holding = |sha: Hash| {
        let holders: Vec<_> = all()
            .map(|h| retained(h.engine.as_ref(), &sha).unwrap())
            .collect();
        holders
    };
    until("both deployments", || {
        holding(shared) == [false, true, false, true] && holding(own) == [true, false, true, false]
    })
    .await;
    // Each holder keeps exactly the keys of its range; ref keys sit at their artifact's position.
    for holder in all() {
        for (hash, namespace, key, _) in holder.rows() {
            assert_eq!(namespace, NAMESPACE.as_bytes());
            assert_eq!(hash, hash_of("cdn", NAMESPACE, &key));
            let span = pb::Span {
                start: holder.span.0,
                end: holder.span.1,
            };
            assert!(in_span(&span, hash), "{key:?} outside {:?}", holder.span);
            if key.starts_with(b"ref/") {
                let sha = if key.starts_with(format!("ref/{shared}/").as_bytes()) {
                    shared
                } else {
                    own
                };
                assert_eq!(hash, content(&sha.0));
            }
        }
    }

    // Releasing d1 keeps the artifact d2 shares and drops the one only d1 used.
    publish(
        &writer,
        "d1",
        &[("/index.html", shared), ("/a.css", own)],
        true,
    )
    .await;
    until("d1 released", || {
        holding(shared) == [false, true, false, true] && holding(own) == [false; 4]
    })
    .await;

    // Wipe cell-a's upper holder while deployments keep being published, then let it rebuild.
    let (address, holders, ..) = &mut cells[0];
    let upper = &mut holders[1];
    let old = std::mem::replace(upper, Holder::start(address, HALVES[1]));
    old.session.stop().await;
    let publishing = {
        let writer = writer.clone();
        tokio::spawn(async move {
            for i in 0..20u8 {
                let sha = Hash([0x80 | i; 32]);
                publish(
                    &writer,
                    &format!("w{i}"),
                    &[("/", sha), ("/shared", shared)],
                    false,
                )
                .await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    publishing.await.unwrap();
    let [(_, a, ..), (_, b, ..)] = &cells[..] else {
        unreachable!()
    };
    // The last deployment's ref key proves both holders caught up, not only that they agree on an earlier state.
    let last = Hash([0x80 | 19; 32]);
    until("the wiped holder rebuilt", || {
        a[1].session.ready()
            && b[1].session.ready()
            && retained(a[1].engine.as_ref(), &last).unwrap()
            && a[1].rows() == b[1].rows()
    })
    .await;
    assert!(!a[1].rows().is_empty());
    assert!(retained(a[1].engine.as_ref(), &shared).unwrap());
    for (_, holders, ..) in cells {
        for holder in holders {
            holder.session.stop().await;
        }
    }
    writer.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn ref_keys_reach_their_artifacts_holders_retain_shared_artifacts_and_rebuild_after_a_wipe() {
    scenario().await;
}

#[cfg(madsim)]
#[madsim::test]
async fn ref_keys_reach_their_artifacts_holders_retain_shared_artifacts_and_rebuild_after_a_wipe() {
    scenario().await;
}
