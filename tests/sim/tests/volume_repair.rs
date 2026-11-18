//! #114 / S-06: range handoff and slot repair on `bedrock-volume` services over the simulated network. One actor has
//! slots in cells a, b and c; `d` is the node a handoff brings into c's cell. The node side is the real [`Repair`]:
//! [`Peers`] here only names the other slots and reaches them over the network.
//!
//! Four scripted traces are the TLC counterexamples of the deliberate bugs in `docs/spec/volume.tla` (`volume-handoff`:
//! `populating`, `unsealed`; `volume-wipe`: `early`, `self-epoch`). With the protocol each one loses no acknowledged
//! commit; with the matching bug it does. A seeded scenario then wipes c and hands its range to d while a holder
//! commits, checks that no repaired slot counts before the rule allows, and replays its trace.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bedrock_lib::actors::SLOTS;
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Directory, Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::sim::Fingerprint;
use bedrock_lib::volume::volume_client::VolumeClient;
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_lib::volume::{self as pb, Refusal};
use bedrock_volume::repair::{self, Peers, Repair, Source};
use bedrock_volume::serve::{LEASE, Service};
use bedrock_volume::store::Store;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tonic::Status;

const NAMES: [&str; 4] = ["a", "b", "c", "d"];
const CELLS: [&str; 4] = ["cell-a", "cell-b", "cell-c", "cell-c"];
const IPS: [&str; 4] = ["10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.4"];
const CALL: Duration = Duration::from_millis(500);
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const D: usize = 3;

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

fn directory(owners: &[(usize, State)]) -> Directory {
    let owners = owners
        .iter()
        .map(|(i, state)| Owner {
            node: NAMES[*i].into(),
            state: *state,
        })
        .collect();
    BTreeMap::from([(0, owners)])
}

/// A store that holds its whole range, as a cell's first owner's does.
fn healthy() -> Arc<Store> {
    let store = Store::new(Arc::new(MemoryEngine::new("volume")));
    store.initialize(true).unwrap();
    store.hold(0, 0, 0).unwrap();
    Arc::new(store)
}

/// An empty disk.
fn empty() -> Arc<Store> {
    Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))))
}

/// The four nodes. A node's store can be swapped for an empty one (a lost disk) before a restart. c and d share cell
/// c's directory.
struct Cluster {
    stores: Vec<Arc<Mutex<Arc<Store>>>>,
    directories: Vec<Arc<RwLock<Directory>>>,
    nodes: Vec<NodeHandle>,
}

impl Cluster {
    fn new() -> Self {
        let stores: Vec<_> = [healthy(), healthy(), healthy(), empty()]
            .into_iter()
            .map(|s| Arc::new(Mutex::new(s)))
            .collect();
        let cell_c = Arc::new(RwLock::new(directory(&[(C, State::Ready)])));
        let directories = vec![
            Arc::new(RwLock::new(directory(&[(A, State::Ready)]))),
            Arc::new(RwLock::new(directory(&[(B, State::Ready)]))),
            cell_c.clone(),
            cell_c,
        ];
        let nodes = (0..4)
            .map(|i| {
                let (store, directory) = (stores[i].clone(), directories[i].clone());
                Handle::current()
                    .create_node()
                    .name(NAMES[i])
                    .ip(IPS[i].parse().unwrap())
                    .init(move || {
                        let store = store.lock().unwrap().clone();
                        let directory = directory.clone();
                        async move {
                            let mut service =
                                Service::new(&Admin::new(), store, NAMES[i], CELLS[i], "dc1")
                                    .unwrap()
                                    .with_repair();
                            service.directory = directory;
                            tonic::transport::Server::builder()
                                .add_service(VolumeServer::new(service))
                                .serve(format!("{}:7000", IPS[i]).parse().unwrap())
                                .await
                                .unwrap();
                        }
                    })
                    .build()
            })
            .collect();
        Cluster {
            stores,
            directories,
            nodes,
        }
    }

    fn store(&self, i: usize) -> Arc<Store> {
        self.stores[i].lock().unwrap().clone()
    }

    fn view(&self, i: usize) -> Directory {
        self.directories[i].read().unwrap().clone()
    }

    fn set(&self, i: usize, owners: &[(usize, State)]) {
        *self.directories[i].write().unwrap() = directory(owners);
    }

    /// Node `i` loses its disk and restarts empty.
    async fn wipe(&self, i: usize) {
        Handle::current().kill(self.nodes[i].id());
        *self.stores[i].lock().unwrap() = empty();
        sleep(Duration::from_millis(100)).await;
        Handle::current().restart(self.nodes[i].id());
    }

    /// The node's repair, copying from `peers` (`gate`: a peer whose copy waits to be released).
    fn repair(&self, i: usize, gate: Option<Gate>) -> Repair {
        let peers = Arc::new(Network {
            peers: [B, A].into_iter().filter(|p| *p != i).collect(),
            gate,
        });
        let store = self.store(i);
        repair::decide(&store, &self.view(i), NAMES[i]).unwrap();
        Repair::new(store, peers, NAMES[i], CELLS[i])
    }
}

/// Holds a repair's copy from `node` until released: `reached` fires when the copy gets there.
#[derive(Clone)]
struct Gate {
    node: usize,
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl Gate {
    fn at(node: usize) -> Self {
        Gate {
            node,
            reached: Arc::default(),
            release: Arc::default(),
        }
    }
}

/// The actor's slots are in cells a, b and c; a repair copies from `peers`, in order.
struct Network {
    peers: Vec<usize>,
    gate: Option<Gate>,
}

type Boxed<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

impl Peers for Network {
    fn sources<'a, 'b>(&'a self, start: u64, end: u64) -> Boxed<'b, Vec<Source>>
    where
        'a: 'b,
    {
        let sources = self
            .peers
            .iter()
            .map(|&i| Source {
                cell: CELLS[i].into(),
                node: NAMES[i].into(),
                start,
                end,
            })
            .collect();
        Box::pin(async move { Ok(sources) })
    }

    fn slots<'a, 'c, 'b>(&'a self, _actor: &'c pb::Actor) -> Boxed<'b, [String; SLOTS]>
    where
        'a: 'b,
        'c: 'b,
    {
        Box::pin(async { Ok(["cell-a", "cell-b", "cell-c"].map(String::from)) })
    }

    fn page<'a, 'c, 'b>(
        &'a self,
        source: &'c Source,
        request: pb::RepairRequest,
    ) -> Boxed<'b, pb::RepairReply>
    where
        'a: 'b,
        'c: 'b,
    {
        Box::pin(async move {
            let i = NAMES.iter().position(|n| *n == source.node).unwrap();
            if let Some(gate) = self.gate.as_ref().filter(|g| g.node == i)
                && request.after.is_empty()
            {
                gate.reached.notify_one();
                gate.release.notified().await;
            }
            let mut client = client(i)
                .await
                .ok_or_else(|| Status::unavailable("no answer"))?;
            Ok(client.repair(request).await?.into_inner())
        })
    }
}

async fn client(i: usize) -> Option<VolumeClient<tonic::transport::Channel>> {
    timeout(
        CALL,
        VolumeClient::connect(format!("http://{}:7000", IPS[i])),
    )
    .await
    .ok()?
    .ok()
}

fn refused(answer: &pb::Answer) -> Refusal {
    Refusal::try_from(answer.refused).unwrap()
}

fn slot_of(answer: &pb::Answer) -> pb::Slot {
    answer.slot.clone().unwrap()
}

/// Accepted, and by a voting slot.
fn counts(answer: &Option<pb::Answer>) -> bool {
    answer
        .as_ref()
        .is_some_and(|a| refused(a) == Refusal::None && slot_of(a).voting)
}

/// A holder driven step by step. It commits an entry of its own epoch once two voting slots hold it (Raft's Figure 8
/// rule); `committed` collects every entry any holder acknowledged.
struct Holder {
    name: pb::Holder,
    epoch: u64,
    /// Voting slots that answered a claim, with their log ends.
    grants: Vec<(u64, u64, usize)>,
    log: Vec<pb::Entry>,
    /// What each node acknowledged, as a voting slot.
    acked: [u64; 4],
    commit: u64,
}

impl Holder {
    fn new(name: &str, epoch: u64) -> Self {
        Holder {
            name: pb::Holder {
                cell: "cell-a".into(),
                host: name.into(),
            },
            epoch,
            grants: Vec::new(),
            log: Vec::new(),
            acked: [0; 4],
            commit: 0,
        }
    }

    async fn reserve(&self, i: usize) -> Option<pb::Answer> {
        let request = pb::ReserveRequest {
            actor: Some(actor()),
            epoch: self.epoch,
            version: 0,
        };
        let answer = timeout(CALL, client(i).await?.reserve(request))
            .await
            .ok()?;
        answer.ok().map(|a| a.into_inner())
    }

    async fn claim(&mut self, i: usize) -> Option<pb::Answer> {
        let request = pb::ClaimRequest {
            actor: Some(actor()),
            holder: Some(self.name.clone()),
            epoch: self.epoch,
            version: 0,
        };
        let answer = timeout(CALL, client(i).await?.claim(request))
            .await
            .ok()?
            .ok()
            .map(|a| a.into_inner());
        if counts(&answer) {
            let end = slot_of(answer.as_ref().unwrap()).end.unwrap_or_default();
            self.grants.push((end.epoch, end.seq, i));
        }
        answer
    }

    /// With two voting grants, reads the most advanced log and appends the no-op; `false` without a quorum.
    async fn recover(&mut self) -> bool {
        if self.grants.len() < 2 {
            return false;
        }
        let (_, end, from) = self.grants.iter().copied().max().unwrap();
        if end == 0 {
            self.log.clear();
            self.push(pb::entry::Kind::Noop);
            return true;
        }
        let reply = client(from)
            .await
            .unwrap()
            .log(pb::LogRequest {
                actor: Some(actor()),
                holder: Some(self.name.clone()),
                epoch: self.epoch,
                from: 1,
                limit: 1000,
            })
            .await
            .unwrap()
            .into_inner();
        self.log = reply.entries;
        assert_eq!(
            self.log.len() as u64,
            end,
            "the log ends where the claim said"
        );
        self.push(pb::entry::Kind::Noop);
        true
    }

    fn push(&mut self, kind: pb::entry::Kind) {
        let seq = self.log.len() as u64 + 1;
        self.log.push(pb::Entry {
            seq,
            epoch: self.epoch,
            // Which holder wrote it: two holders can hold one epoch, as the model's entries carry theirs.
            committed_at: u64::from(self.name.host.as_bytes()[1]) * 1000 + seq,
            kind: kind as i32,
            ..Default::default()
        });
    }

    /// Sends the whole log to node `i` and returns its answer; a voting acceptance may commit more.
    async fn append(&mut self, i: usize, committed: &mut Vec<pb::Entry>) -> Option<pb::Answer> {
        let request = pb::AppendRequest {
            actor: Some(actor()),
            holder: Some(self.name.clone()),
            epoch: self.epoch,
            version: 0,
            prev_epoch: 0,
            entries: self.log.clone(),
            commit: self.commit,
        };
        let answer = timeout(CALL, client(i).await?.append(request))
            .await
            .ok()?
            .ok()
            .map(|a| a.into_inner());
        if counts(&answer) {
            self.acked[i] = self.log.len() as u64;
            let point = (1..=self.log.len() as u64)
                .filter(|s| self.log[*s as usize - 1].epoch == self.epoch)
                .filter(|s| self.acked.iter().filter(|a| **a >= *s).count() >= 2)
                .max()
                .unwrap_or(0);
            for entry in &self.log[..point as usize] {
                if !committed.contains(entry) {
                    committed.push(entry.clone());
                }
            }
            self.commit = self.commit.max(point);
        }
        answer
    }

    /// Every acknowledged commit is in this holder's recovered log.
    fn check(&self, committed: &[pb::Entry]) {
        for entry in committed {
            assert_eq!(
                self.log.get(entry.seq as usize - 1),
                Some(entry),
                "an acknowledged commit was lost"
            );
        }
    }
}

/// Runs `scenario` on a fresh simulator; it starts once every node has outlived its startup lease window.
#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run<F: Future<Output = u64> + Send + 'static>(seed: u64, scenario: fn(Cluster) -> F) -> u64 {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(async move {
            let cluster = Cluster::new();
            let holder = Handle::current()
                .create_node()
                .name("compute")
                .ip("10.0.0.9".parse().unwrap())
                .build();
            holder
                .spawn(async move {
                    sleep(LEASE + Duration::from_secs(1)).await;
                    scenario(cluster).await
                })
                .await
                .unwrap()
        })
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test volume_repair"
        );
        std::panic::resume_unwind(failure)
    })
}

/// `volume-handoff`, bug `populating`: a populating owner that answered would let a claim recover from it, empty.
async fn populating(cluster: Cluster) -> u64 {
    let mut committed = Vec::new();
    let (mut h1, mut h2) = (Holder::new("h1", 1), Holder::new("h2", 1));
    for i in [B, C] {
        assert!(counts(&h1.reserve(i).await));
        h1.claim(i).await;
    }
    assert!(h1.recover().await);
    for i in [B, C] {
        h1.append(i, &mut committed).await;
    }
    assert_eq!(committed.len(), 1, "h1 committed its no-op");
    h2.reserve(A).await;
    cluster.set(C, &[(C, State::Leaving), (D, State::Populating)]);
    let reserved = counts(&h2.reserve(D).await);
    assert!(!reserved, "a populating owner takes no reservation");
    if reserved {
        h2.claim(A).await;
        h2.claim(D).await;
        if h2.recover().await {
            h2.check(&committed);
        }
    }
    0
}

/// `volume-handoff`, bug `unsealed`: the new owner copies its leaving owner while it still takes a claim and appends.
async fn unsealed(cluster: Cluster) -> u64 {
    let mut committed = Vec::new();
    let (mut h1, mut h2) = (Holder::new("h1", 1), Holder::new("h2", 1));
    h1.reserve(B).await;
    h2.reserve(A).await;
    cluster.set(C, &[(C, State::Leaving), (D, State::Populating)]);
    let d = cluster.repair(D, None);
    let view = cluster.view(D);
    assert!(d.tick(&view, &BTreeMap::new()).await.unwrap());
    if counts(&h1.reserve(C).await) {
        for i in [B, C] {
            h1.claim(i).await;
        }
        if h1.recover().await {
            for i in [B, C] {
                h1.append(i, &mut committed).await;
            }
        }
    }
    // The relay counts d ready from its report, and c leaves.
    cluster.set(D, &[(D, State::Ready)]);
    assert!(counts(&h2.reserve(D).await));
    for i in [A, D] {
        h2.claim(i).await;
    }
    assert!(h2.recover().await);
    h2.check(&committed);
    0
}

/// `volume-wipe`, bug `early`: c, wiped after promising h2, copies b, then a; voting at once, it completes h1's quorum.
async fn early(cluster: Cluster) -> u64 {
    let mut committed = Vec::new();
    let (mut h1, mut h2) = (Holder::new("h1", 1), Holder::new("h2", 2));
    for i in [B, A] {
        assert!(counts(&h1.reserve(i).await));
    }
    for i in [B, A] {
        h1.claim(i).await;
    }
    for i in [B, A] {
        assert!(counts(&h2.reserve(i).await));
    }
    h2.claim(C).await;
    cluster.wipe(C).await;
    let gate = Gate::at(A);
    let c = cluster.repair(C, Some(gate.clone()));
    let directory = cluster.view(C);
    let repairing = tokio::spawn(async move { c.tick(&directory, &BTreeMap::new()).await });
    // c has copied b and waits before a.
    gate.reached.notified().await;
    h2.claim(B).await;
    assert!(h1.recover().await);
    assert!(h2.recover().await);
    h1.append(A, &mut committed).await;
    gate.release.notify_one();
    assert!(repairing.await.unwrap().unwrap(), "c holds its range again");
    let answer = h1.append(C, &mut committed).await;
    assert!(
        answer.is_some_and(|a| refused(&a) == Refusal::None && !slot_of(&a).voting),
        "the repaired slot takes the append without voting"
    );
    h2.check(&committed);
    0
}

/// `volume-wipe`, bug `self-epoch`: holders that claim at an epoch they did not reserve leave a repaired slot a floor
/// below a promise it lost, so h1's late appends re-enable its vote and commit an entry h2 never recovered.
async fn self_epoch(cluster: Cluster) -> u64 {
    // The protocol reserves; the deliberate bug in this file's negative check sets this to false.
    const RESERVE: bool = true;
    let mut committed = Vec::new();
    let (mut h1, mut h2) = (Holder::new("h1", 1), Holder::new("h2", 2));
    for i in [B, A] {
        if RESERVE {
            assert!(counts(&h1.reserve(i).await));
        }
        h1.claim(i).await;
    }
    assert!(h1.recover().await);
    for i in [B, A] {
        h1.append(i, &mut committed).await;
    }
    assert_eq!(committed.len(), 1, "h1 committed its no-op");
    h1.push(pb::entry::Kind::Release);
    h1.append(B, &mut committed).await;
    if RESERVE {
        for i in [C, A] {
            assert!(counts(&h2.reserve(i).await));
        }
    }
    h2.claim(C).await;
    cluster.wipe(C).await;
    let c = cluster.repair(C, None);
    let view = cluster.view(C);
    assert!(c.tick(&view, &BTreeMap::new()).await.unwrap());
    h2.claim(A).await;
    assert!(h2.recover().await);
    // h1 has released, but its last appends still arrive.
    h1.append(C, &mut committed).await;
    h1.append(C, &mut committed).await;
    h2.check(&committed);
    0
}

#[test]
fn a_populating_owner_takes_no_reservation() {
    run(1, populating);
}

#[test]
fn a_new_owner_copies_only_from_a_sealed_leaving_owner() {
    run(1, unsealed);
}

#[test]
fn a_repaired_slot_does_not_vote_before_a_newer_claim_commits() {
    run(1, early);
}

#[test]
fn a_repaired_slot_keeps_the_reservations_it_copied_as_its_floor() {
    run(1, self_epoch);
}

/// A holder commits while c loses its disk and repairs, then hands its range to d. Every answer from c and d votes only
/// once the rule allows, and the next holder recovers every acknowledged commit.
async fn wipe_then_handoff(cluster: Cluster) -> u64 {
    let fingerprint = Fingerprint::new();
    let mut committed = Vec::new();
    let mut h1 = Holder::new("h1", 1);
    for i in [A, B, C] {
        h1.reserve(i).await;
        h1.claim(i).await;
    }
    assert!(h1.recover().await);
    let wipe_at = thread_rng().gen_range(1..8u32);
    let handoff_at = wipe_at + thread_rng().gen_range(2..8u32);
    let mut repairing = None;
    // Which node holds c's slot.
    let mut third = C;
    for n in 0..16u32 {
        if n == wipe_at {
            cluster.wipe(C).await;
            let c = cluster.repair(C, None);
            let directory = cluster.view(C);
            repairing = Some(tokio::spawn(async move {
                c.tick(&directory, &BTreeMap::new()).await
            }));
        }
        if n == handoff_at {
            let repaired = repairing.take().unwrap().await.unwrap().unwrap();
            assert!(repaired, "c holds its range again");
            cluster.set(C, &[(C, State::Leaving), (D, State::Populating)]);
            let d = cluster.repair(D, None);
            let directory = cluster.view(D);
            assert!(d.tick(&directory, &BTreeMap::new()).await.unwrap());
            cluster.set(D, &[(D, State::Ready)]);
            third = D;
        }
        h1.push(pb::entry::Kind::Data);
        // Seeded arrival order.
        let mut order = [A, B, third];
        let first = thread_rng().gen_range(0..3);
        order.swap(0, first);
        for i in order {
            let answer = h1.append(i, &mut committed).await;
            if i != A && i != B && n >= wipe_at {
                assert!(
                    answer.as_ref().is_none_or(|a| !slot_of(a).voting),
                    "slot {} voted before a claim above its floor committed",
                    NAMES[i]
                );
            }
            fingerprint.mix((n, i, answer.map(|a| (a.refused, slot_of(&a).voting))));
        }
    }
    if let Some(repairing) = repairing {
        let _ = repairing.await;
    }
    // h1 stops; h2 waits out its lease, reserves above it and recovers.
    sleep(LEASE + Duration::from_secs(1)).await;
    let mut h2 = Holder::new("h2", 2);
    for i in [A, B, third] {
        h2.reserve(i).await;
        h2.claim(i).await;
    }
    assert!(h2.recover().await);
    h2.check(&committed);
    for _ in 0..2 {
        for i in [A, B, third] {
            h2.append(i, &mut committed).await;
        }
    }
    let answer = h2.append(third, &mut committed).await;
    assert!(
        answer.is_some_and(|a| slot_of(&a).voting),
        "the slot votes again once a claim above its floor committed"
    );
    fingerprint.mix((committed.len(), h2.log.len()));
    for node in &cluster.nodes {
        Handle::current().kill(node.id());
    }
    fingerprint.value()
}

#[test]
fn a_wiped_slot_repairs_and_hands_off_without_losing_commits_and_seeds_replay() {
    // `BEDROCK_VOLUME_REPAIR_SEEDS=500` sweeps seeds 1..=500.
    let last: u64 = std::env::var("BEDROCK_VOLUME_REPAIR_SEEDS").map_or(4, |n| n.parse().unwrap());
    let traces: Vec<u64> = (1..=last)
        .map(|seed| run(seed, wipe_then_handoff))
        .collect();
    for (seed, trace) in (1..=last).zip(&traces) {
        assert_eq!(
            run(seed, wipe_then_handoff),
            *trace,
            "seed {seed} did not replay its trace"
        );
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
}
