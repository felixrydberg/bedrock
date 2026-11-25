//! #115 / S-07: the actor host in Compute against three `bedrock-volume` slots over the simulated network.
//!
//! - A request to a sleeping actor wakes it, with its key/value and SQLite state; an idle actor sleeps after 10 s.
//! - Two `Acquire`s on one host join one wake; two hosts racing to wake an actor leave exactly one serving.
//! - A holder cut off from its slots stops answering, even from memory, within 4 s; another host then takes over.
//! - An event cancelled mid-commit stops the actor; the next call wakes it from its slots.
//! - A built-in actor and a stub execution backend run through the same `Executor`/`Instance` seam, and built-in kinds
//!   never call the sandbox.
//! - A seed replays its trace.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, Placement};
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::actors::volume::{self, Host, Slot};
use bedrock_compute::runtime::{Error, ExecutionSpec, Executor, Handler, Instance, RustExecutor};
use bedrock_lib::actors::Kind;
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::sim::{Fingerprint, SimEngine};
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_lib::volume::{self as pb};
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::sync::Notify;
use tokio::time::sleep;

const SLOT_IPS: [&str; 3] = ["10.0.1.1", "10.0.1.2", "10.0.1.3"];
const CELLS: [&str; 3] = ["cell-1", "cell-2", "cell-3"];
const LETTERS: &[u8] = b"bcdefghijklmnopqrstuvwxyz";

fn actor(case: usize, kind: Kind) -> pb::Actor {
    let letter = (LETTERS[case] as char).to_string();
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", letter.repeat(25)),
        kind: kind as i32,
    }
}

fn slot_node(i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let name = format!("volume-{i}");
    Handle::current()
        .create_node()
        .name(&name)
        .ip(SLOT_IPS[i].parse().unwrap())
        .init(move || {
            let (engine, name) = (engine.clone(), name.clone());
            async move {
                let store = Arc::new(Store::new(engine));
                let service = Service::new(&Admin::new(), store, &name, CELLS[i], "dc1").unwrap();
                let owner = Owner {
                    node: name,
                    state: State::Ready,
                };
                *service.directory.write().unwrap() = BTreeMap::from([(0, vec![owner])]);
                tonic::transport::Server::builder()
                    .add_service(VolumeServer::new(service))
                    .serve(format!("{}:7000", SLOT_IPS[i]).parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

/// Every actor's slots are the three slot nodes.
struct Fixed;

#[async_trait]
impl Placement for Fixed {
    async fn slots(&self, _actor: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        let slots = std::array::from_fn(|i| Slot {
            cell: CELLS[i].into(),
            address: format!("{}:7000", SLOT_IPS[i]),
            identity: tls::Identity {
                service: "volume".into(),
                cell: CELLS[i].into(),
                node_id: format!("volume-{i}"),
                host: SLOT_IPS[i].into(),
            },
        });
        Ok((slots, 0))
    }
}

fn number(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
}

/// Built-in `kv`: `incr` commits the counter plus one, `peek` answers from memory with no storage call.
struct Counter {
    storage: Storage,
    value: Option<u64>,
}

#[async_trait]
impl Instance for Counter {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        if request == b"incr" {
            let current = match self.value {
                Some(value) => value,
                None => {
                    let values = self.storage.get(vec![b"n".to_vec()]).await?;
                    values[0].value.as_deref().map_or(0, number)
                }
            };
            let next = current + 1;
            let change = pb::Change {
                key: b"n".to_vec(),
                value: Some(next.to_be_bytes().to_vec()),
            };
            self.storage.commit(vec![change]).await?;
            self.value = Some(next);
        }
        Ok(self.value.unwrap_or(0).to_be_bytes().to_vec())
    }
}

/// Holds one real event at the input gate until the test releases it.
struct LatchedCounter {
    counter: Counter,
    started: Arc<Notify>,
    resume: Arc<Notify>,
}

#[async_trait]
impl Instance for LatchedCounter {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        if request == b"block" {
            self.started.notify_one();
            self.resume.notified().await;
            self.counter.invoke(b"incr".to_vec()).await
        } else {
            self.counter.invoke(request).await
        }
    }
}

/// Built-in `d1`: each event inserts a row through the SQLite VFS and answers the row count. `big` tries a row too
/// large for one entry, ignores the failure and answers the count, which must not include it.
struct Rows(Storage);

impl Rows {
    async fn count(&self, row: Option<&[u8]>) -> Result<Result<i64, String>, volume::Error> {
        let count = self
            .0
            .sql(|tx| {
                tx.execute_batch("CREATE TABLE IF NOT EXISTS t (v BLOB)")?;
                if let Some(row) = row {
                    tx.execute("INSERT INTO t VALUES (?1)", [row])?;
                }
                tx.query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0))
            })
            .await?;
        Ok(count.map_err(|e| e.to_string()))
    }
}

#[async_trait]
impl Instance for Rows {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let count = if request == b"big" {
            let big = vec![1; volume::MAX_ENTRY_BYTES + 1];
            match self.count(Some(&big)).await {
                Err(volume::Error::TooLarge(_)) => {}
                other => panic!("an oversized transaction: {:?}", other.map(|r| r.ok())),
            }
            self.count(None).await?
        } else {
            self.count(Some(&request)).await?
        };
        let count = count.map_err(Error::Handler)?;
        Ok((count as u64).to_be_bytes().to_vec())
    }
}

/// The stand-in for S-10's sandbox: counts its starts.
struct Stub(Arc<AtomicUsize>);

struct StubInstance;

#[async_trait]
impl Instance for StubInstance {
    async fn invoke(&mut self, _request: Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(b"stub".to_vec())
    }
}

#[async_trait]
impl Executor for Stub {
    async fn start(&self, spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error> {
        assert!(
            spec.builtin().is_none(),
            "a built-in kind reached the sandbox"
        );
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(StubInstance))
    }
}

fn rust() -> RustExecutor {
    let counter: Handler = Arc::new(|_, storage| {
        Box::new(Counter {
            storage,
            value: None,
        })
    });
    let rows: Handler = Arc::new(|_, storage| Box::new(Rows(storage)));
    RustExecutor::new([(Kind::Kv, counter), (Kind::D1, rows)])
}

/// An actor host named `name` in the first slot's cell, on the current node.
fn host(name: &str, sandbox: Arc<AtomicUsize>) -> ActorHost {
    let admin = Admin::new();
    let holder = pb::Holder {
        cell: CELLS[0].into(),
        host: name.into(),
    };
    let volume = Host::start(Arc::new(Peers::new(&admin)), holder);
    ActorHost::new(
        &admin,
        CELLS[0],
        "dc1",
        volume,
        Arc::new(Fixed),
        Arc::new(rust()),
        Arc::new(Stub(sandbox)),
    )
}

fn compute(n: usize) -> NodeHandle {
    Handle::current()
        .create_node()
        .name(format!("compute-{n}"))
        .ip(format!("10.0.0.{}", n + 10).parse().unwrap())
        .build()
}

/// Invokes `request`, retrying while the claim is contended or a previous holder's lease is live.
async fn call(host: &ActorHost, actor: &pb::Actor, request: &[u8]) -> Vec<u8> {
    for _ in 0..60 {
        match host.acquire(actor).await {
            Ok(awake) => match awake.invoke(request.to_vec()).await {
                Ok(reply) => return reply,
                Err(Error::Volume(_)) => {}
                Err(error) => panic!("invoke failed: {error}"),
            },
            Err(Error::Redirect(_) | Error::Volume(_)) => {}
            Err(error) => panic!("acquire failed: {error}"),
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("the actor never answered");
}

/// Wakes, sleeps and wakes again a key/value and a SQLite actor; two `Acquire`s on one host share one wake. The
/// second wake is on another host and claims at once, so the first host released both actors.
async fn wake_and_sleep(fingerprint: &Fingerprint, next: &mut usize, sandbox: &Arc<AtomicUsize>) {
    let (counter, rows) = (actor(*next, Kind::Kv), actor(*next + 1, Kind::D1));
    *next += 2;
    let (first, second) = (compute(*next), compute(*next + 1));
    *next += 2;
    let (c, r, s) = (counter.clone(), rows.clone(), sandbox.clone());
    let mut trace = first
        .spawn(async move {
            let host = host("wake", s);
            let (a, b) = tokio::join!(host.acquire(&c), host.acquire(&c));
            assert!(
                Arc::ptr_eq(&a.unwrap(), &b.unwrap()),
                "two wakes of one actor"
            );
            let mut trace = Vec::new();
            for _ in 0..3 {
                trace.push(number(&call(&host, &c, b"incr").await));
                trace.push(number(&call(&host, &r, &[7; 3000]).await));
            }
            // A transaction too large to commit leaves no trace in the pages the actor reads next.
            trace.push(number(&call(&host, &r, b"big").await));
            assert_eq!(host.awake().await, 2);
            sleep(Duration::from_secs(12)).await;
            assert_eq!(host.awake().await, 0, "idle actors did not sleep");
            trace
        })
        .await
        .unwrap();
    let s = sandbox.clone();
    let woken = second
        .spawn(async move {
            let host = host("wake-2", s);
            let mut trace = Vec::new();
            for (actor, request) in [(&counter, &b"incr"[..]), (&rows, &[8; 3000][..])] {
                // Woken with empty caches: the counter and every row come back from the slots.
                let awake = host
                    .acquire(actor)
                    .await
                    .expect("a released actor claims at once");
                trace.push(number(&awake.invoke(request.to_vec()).await.unwrap()));
            }
            trace
        })
        .await
        .unwrap();
    trace.extend(woken);
    assert_eq!(trace, [1, 1, 2, 2, 3, 3, 3, 4, 4]);
    fingerprint.mix(("wake", trace));
    Handle::current().kill(first.id());
    Handle::current().kill(second.id());
}

/// Two hosts on two nodes wake one actor at once: exactly one serves, and the other is redirected to it.
async fn race(fingerprint: &Fingerprint, next: &mut usize, sandbox: &Arc<AtomicUsize>) {
    let target = actor(*next, Kind::Kv);
    *next += 1;
    let nodes = [compute(*next), compute(*next + 1)];
    *next += 2;
    let racers = nodes.iter().enumerate().map(|(i, node)| {
        let (target, sandbox) = (target.clone(), sandbox.clone());
        node.spawn(async move {
            let host = host(&format!("racer-{i}"), sandbox);
            let outcome = loop {
                match host.acquire(&target).await {
                    Ok(awake) => break Ok(awake),
                    Err(Error::Redirect(holder)) => break Err(holder.host),
                    // Contended epochs: try again.
                    Err(Error::Volume(volume::Error::Unavailable(_))) => {
                        sleep(Duration::from_millis(50)).await
                    }
                    Err(error) => panic!("acquire failed: {error}"),
                }
            };
            sleep(Duration::from_secs(2)).await;
            match outcome {
                Ok(awake) => (true, awake.serving(), String::new()),
                Err(holder) => (false, false, holder),
            }
        })
    });
    let mut outcomes = Vec::new();
    for racer in racers.collect::<Vec<_>>() {
        outcomes.push(racer.await.unwrap());
    }
    let serving: Vec<usize> = (0..2).filter(|&i| outcomes[i].1).collect();
    assert_eq!(serving.len(), 1, "racers left {outcomes:?}");
    let (winner, loser) = (serving[0], 1 - serving[0]);
    assert!(!outcomes[loser].0, "the loser also woke the actor");
    assert_eq!(outcomes[loser].2, format!("racer-{winner}"));
    fingerprint.mix(("race", winner));
    for node in nodes {
        Handle::current().kill(node.id());
    }
}

/// A holder cut off from every slot stops answering, even from memory, within 4 s; another host then takes over.
async fn cut_off(
    fingerprint: &Fingerprint,
    next: &mut usize,
    sandbox: &Arc<AtomicUsize>,
    slots: &[NodeHandle],
) {
    let target = actor(*next, Kind::Kv);
    *next += 1;
    let (first, second) = (compute(*next), compute(*next + 1));
    *next += 2;
    let ids: Vec<_> = slots.iter().map(NodeHandle::id).collect();
    let (id, s) = (first.id(), sandbox.clone());
    let to = target.clone();
    let stopped = first
        .spawn(async move {
            let host = host("cut", s);
            assert_eq!(number(&call(&host, &to, b"incr").await), 1);
            let awake = host.acquire(&to).await.unwrap();
            for &slot in &ids {
                NetSim::current().clog_link(id, slot);
                NetSim::current().clog_link(slot, id);
            }
            let cut = bedrock_lib::monotonic_now();
            // Polled every 10 ms: the holder stops serving by the fence deadline, and from then on it answers nothing,
            // not even from memory, although the host drops the instance only at its next check.
            let poll = Duration::from_millis(10);
            let mut answered = 0u32;
            while awake.serving() {
                answered += u32::from(awake.invoke(b"peek".to_vec()).await.is_ok());
                sleep(poll).await;
            }
            let stopped = cut.elapsed();
            assert!(
                awake.invoke(b"peek".to_vec()).await.is_err(),
                "answered after the holder stopped serving"
            );
            assert!(
                stopped <= volume::FENCE + poll,
                "served for {stopped:?} after the cut"
            );
            answered
        })
        .await
        .unwrap();
    let (s, to) = (sandbox.clone(), target.clone());
    let value = second
        .spawn(async move { number(&call(&host("after", s), &to, b"incr").await) })
        .await
        .unwrap();
    assert_eq!(value, 2, "the next holder lost the cut-off holder's commit");
    fingerprint.mix(("cut off", stopped, value));
    Handle::current().kill(first.id());
    Handle::current().kill(second.id());
}

/// An event cancelled during its commit stops the actor, whose memory may no longer match its slots; the next call
/// wakes it again and counts on from what the slots hold. The cancelled event left the busy count, so it sleeps.
async fn cancelled(fingerprint: &Fingerprint, next: &mut usize, sandbox: &Arc<AtomicUsize>) {
    let target = actor(*next, Kind::Kv);
    *next += 1;
    let node = compute(*next);
    *next += 1;
    let s = sandbox.clone();
    let trace = node
        .spawn(async move {
            let host = host("cancel", s);
            let first = number(&call(&host, &target, b"incr").await);
            let awake = host.acquire(&target).await.unwrap();
            let cut =
                tokio::time::timeout(Duration::from_micros(1), awake.invoke(b"incr".to_vec()))
                    .await;
            assert!(cut.is_err(), "the commit answered within a microsecond");
            assert!(!awake.serving(), "a cancelled event left the actor serving");
            let after = number(&call(&host, &target, b"incr").await);
            let again = number(&call(&host, &target, b"incr").await);
            assert!(
                (2..=3).contains(&after) && again == after + 1,
                "counted {first}, {after}, {again}"
            );
            sleep(Duration::from_secs(12)).await;
            assert_eq!(host.awake().await, 0, "the actor did not sleep");
            (first, after, again)
        })
        .await
        .unwrap();
    fingerprint.mix(("cancelled", trace));
    Handle::current().kill(node.id());
}

/// Cancelling administrative deletion while an event owns the input gate keeps the live actor reachable for cleanup.
async fn cancelled_delete(fingerprint: &Fingerprint, next: &mut usize, sandbox: &Arc<AtomicUsize>) {
    let target = actor(*next, Kind::Kv);
    *next += 1;
    let node = compute(*next);
    *next += 1;
    let sandbox = sandbox.clone();
    let trace = node
        .spawn(async move {
            let started = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            let (signal, latch) = (started.clone(), resume.clone());
            let counter: Handler = Arc::new(move |_, storage| {
                Box::new(LatchedCounter {
                    counter: Counter {
                        storage,
                        value: None,
                    },
                    started: signal.clone(),
                    resume: latch.clone(),
                })
            });
            let admin = Admin::new();
            let volume = Host::start(
                Arc::new(Peers::new(&admin)),
                pb::Holder {
                    cell: CELLS[0].into(),
                    host: "cancel-delete".into(),
                },
            );
            let host = ActorHost::new(
                &admin,
                CELLS[0],
                "dc1",
                volume,
                Arc::new(Fixed),
                Arc::new(RustExecutor::new([(Kind::Kv, counter)])),
                Arc::new(Stub(sandbox)),
            );
            let awake = host.acquire(&target).await.unwrap();
            let event = {
                let awake = awake.clone();
                tokio::spawn(async move { awake.invoke(b"block".to_vec()).await })
            };
            started.notified().await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), host.delete(&target))
                    .await
                    .is_err(),
                "deletion must wait for the active input gate"
            );
            resume.notify_one();
            assert_eq!(number(&event.await.unwrap().unwrap()), 1);
            assert_eq!(
                host.awake().await,
                1,
                "cancelled deletion lost the live actor from its cell"
            );
            let same = host.acquire(&target).await.unwrap();
            assert!(
                Arc::ptr_eq(&awake, &same),
                "the still-serving instance remains reachable"
            );
            assert_eq!(number(&same.invoke(b"peek".to_vec()).await.unwrap()), 1);
            sleep(Duration::from_secs(12)).await;
            assert_eq!(
                host.awake().await,
                0,
                "the retained actor remains eligible for normal idle cleanup"
            );
            let next = number(&call(&host, &target, b"incr").await);
            assert_eq!(next, 2, "reacquisition recovers the committed event");
            next
        })
        .await
        .unwrap();
    fingerprint.mix(("cancelled delete", trace));
    Handle::current().kill(node.id());
}

/// A customer-code actor starts in the sandbox stub through the same seam; an unregistered built-in kind fails without
/// reaching it.
async fn seam(fingerprint: &Fingerprint, next: &mut usize, sandbox: &Arc<AtomicUsize>) {
    let (object, queue) = (actor(*next, Kind::Do), actor(*next + 1, Kind::Queue));
    *next += 2;
    let node = compute(*next);
    *next += 1;
    let s = sandbox.clone();
    let before = sandbox.load(Ordering::SeqCst);
    assert_eq!(before, 0, "built-in kinds started the sandbox");
    let reply = node
        .spawn(async move {
            let host = host("seam", s);
            let reply = call(&host, &object, b"hi").await;
            match host.acquire(&queue).await {
                Err(Error::Unsupported(_)) => {}
                other => panic!("a kind without a handler: {:?}", other.err()),
            }
            reply
        })
        .await
        .unwrap();
    assert_eq!(reply, b"stub");
    assert_eq!(sandbox.load(Ordering::SeqCst), 1);
    fingerprint.mix(("seam", reply));
    Handle::current().kill(node.id());
}

async fn scenario() -> u64 {
    let fingerprint = Fingerprint::new();
    let slots: Vec<NodeHandle> = (0..3)
        .map(|i| {
            let disk = SimEngine::new(&format!("volume-{i}"), Arc::new(|_| None), None, false);
            slot_node(i, Arc::new(disk.recorded(fingerprint.clone())))
        })
        .collect();
    // Slots treat every holder as live for their first lease after starting.
    sleep(Duration::from_secs(6)).await;
    let sandbox = Arc::new(AtomicUsize::new(0));
    let mut next = 0;
    wake_and_sleep(&fingerprint, &mut next, &sandbox).await;
    race(&fingerprint, &mut next, &sandbox).await;
    cut_off(&fingerprint, &mut next, &sandbox, &slots).await;
    cancelled(&fingerprint, &mut next, &sandbox).await;
    cancelled_delete(&fingerprint, &mut next, &sandbox).await;
    seam(&fingerprint, &mut next, &sandbox).await;
    for node in &slots {
        Handle::current().kill(node.id());
    }
    fingerprint.value()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || {
        let mut runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
        // A stalled scenario fails with its seed instead of hanging.
        runtime.set_time_limit(Duration::from_secs(300));
        runtime.block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_host"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn actors_wake_sleep_race_and_fence_through_one_seam_and_seeds_replay() {
    let traces: Vec<u64> = (1..=3).map(run).collect();
    for (seed, trace) in (1..=3).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
}
