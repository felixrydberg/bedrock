//! The holder's client of an actor's three `bedrock-volume` slots (#112 / S-04, `statefull/v2.md` §5,
//! `docs/spec/volume.tla`). The holder is the actor's only writer and its sequencer.
//!
//! - **Open** ([`Writer::open`]). Reserve an epoch at two voting slots, retrying above the highest one a slot names, then
//!   claim at it. With two voting grants, read the most advanced log (highest last-entry epoch, then sequence) from its
//!   slot, append a no-op at the new epoch and open once the no-op commits. A repaired slot's answers count towards no
//!   quorum, and a claim refused for a live lease names the holder ([`Error::Held`]).
//! - **Appends.** One task per slot sends the log from that slot's next entry, so a slow slot never holds up the other
//!   two; a gap backs the slot off until its log matches, which also replaces an old epoch's uncommitted tail. An entry
//!   commits once two voting slots hold it and the claim's no-op (Raft's Figure 8 rule). Commits wait for room in a
//!   window bounded by entries and bytes. Appends carry the commit point; an idle slot learns it within [`HEARTBEAT`].
//! - **Lease.** [`Host`] heartbeats every volume node it holds actors on once per [`HEARTBEAT`]. A writer stops serving
//!   for good once fewer than two of its slots acknowledged a heartbeat sent within [`FENCE`], or a slot promised a newer
//!   epoch, or fewer than two slots still take its appends. A lease task wakes at that deadline, so a later heartbeat
//!   cannot hide a lapse. Slots refuse another claim
//!   for 5 s after the host's last heartbeat, so the writer stops first.
//! - **Reads** go to the nearest slot (the holder's cell first) with `min_seq` = the commit point, and to the next slot
//!   when one is behind or not promised to this writer's epoch. A slot's overlay also holds this writer's own appended
//!   but uncommitted entries, so a read may see this writer's pending writes, never another holder's: a claim truncates
//!   an old epoch's tail before a slot takes its appends. The actor host keeps pending pages in its cache, returns no
//!   storage call before its commit, and drops an instance whose commit failed (`super::storage`, #115 / S-07), so a
//!   pending write that is later lost never leaves the host (user decision 2026-11-15).
//! - **Placement.** [`slot_nodes`] maps an actor to its slots' nodes through each slot cell's `volume` routing table.
//!   Fetching sibling cells' tables through `Relay.Routes` comes with the `bedrock-compute` binary's relay membership,
//!   after #114 / S-06 (user decision 2026-11-17).
//!
//! Topology transitions (joint majorities of two slot sets) are not here yet: a writer uses one slot set and carries its
//! topology version (user decision 2026-11-15).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bedrock_lib::actors::{self, ActorId, SLOTS, Topology};
use bedrock_lib::client::Table;
use bedrock_lib::placement;
use bedrock_lib::transport::{Class, Peers, host_of, tls};
use bedrock_lib::volume::volume_client::VolumeClient;
use bedrock_lib::volume::{self as pb, Refusal, entry::Kind};
use prost::Message;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Heartbeat period per volume node, and how long an idle slot may wait to learn the commit point.
pub const HEARTBEAT: Duration = Duration::from_secs(1);
/// A writer stops serving once fewer than two slots acknowledged a heartbeat sent within this; slots wait 5 s.
pub const FENCE: Duration = Duration::from_secs(4);
/// Uncommitted entries a writer keeps in flight.
// ponytail: fixed window; tune entries and bytes with S-18's measurements (§12.6).
pub const WINDOW_ENTRIES: u64 = 128;
/// Encoded bytes of uncommitted entries in flight.
pub const WINDOW_BYTES: usize = 8 << 20;
/// The largest entry a writer admits, and the most one append carries: below the 4 MiB default decode limit of the
/// volume server, with room for the request's other fields.
pub const MAX_ENTRY_BYTES: usize = 3 << 20;
/// Pages one SQLite read miss fetches, aligned (user decision 2026-11-15).
pub const CHUNK_PAGES: u32 = 64;
const CALL: Duration = Duration::from_secs(2);
const RETRY: Duration = Duration::from_millis(100);
/// Committed entries, and their encoded bytes, kept for a lagging slot. A slot further behind waits for S-06's repair.
// ponytail: a slot down for good pins at most this much; S-05's truncation bounds what a slot needs.
const RETAIN: u64 = 4096;
const RETAIN_BYTES: usize = 64 << 20;
/// Reserve and claim rounds before [`Writer::open`] gives up.
const ATTEMPTS: usize = 8;
/// Log entries per recovery page. `Store::log` bounds a page by entries only, so the client's decode limit covers a
/// page of the largest entries.
const LOG_PAGE: u32 = 64;
/// The most keys one `Scan` answer carries (`Store::scan` clamps to it).
const SCAN_LIMIT: u32 = 1000;
const LOG_PAGE_BYTES: usize = LOG_PAGE as usize * (MAX_ENTRY_BYTES + 1024) + (64 << 10);
const VOLUME: &str = bedrock_lib::datasets::capabilities::VOLUME;

#[derive(Debug)]
pub enum Error {
    /// Another holder's lease is live on a slot; send the request there.
    Held(pb::Holder),
    /// The writer stopped serving: its slots did not acknowledge heartbeats, a slot promised a newer epoch, or it
    /// released. A commit that fails this way may still have committed.
    Fenced,
    /// A slot holds a newer topology version; read the topology again.
    StaleVersion,
    /// No quorum, or no slot caught up with the commit point, answered.
    Unavailable(String),
    /// A slot's explicit failure: corruption (`DataLoss`), an unsupported read (`Unimplemented`) or a refused request.
    Rpc(Status),
    /// The actor or the topology cannot be placed.
    Placement(actors::Error),
    /// An entry of this many encoded bytes is larger than [`MAX_ENTRY_BYTES`].
    TooLarge(usize),
    /// The actor moved to another DC (S-17): it is served there, never here again.
    Moved,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held(h) => write!(f, "held by {} in {}", h.host, h.cell),
            Self::Fenced => write!(f, "the writer stopped serving"),
            Self::StaleVersion => write!(f, "a slot holds a newer topology version"),
            Self::Unavailable(detail) => write!(f, "unavailable: {detail}"),
            Self::Rpc(status) => write!(f, "slot failed: {status}"),
            Self::Placement(error) => write!(f, "placement: {error}"),
            Self::TooLarge(bytes) => {
                write!(f, "an entry of {bytes} bytes exceeds {MAX_ENTRY_BYTES}")
            }
            Self::Moved => write!(f, "the actor moved to another DC"),
        }
    }
}

impl std::error::Error for Error {}

/// One slot's node: its cell, address and the identity it must present.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slot {
    pub cell: String,
    pub address: String,
    pub identity: tls::Identity,
}

/// The nodes of `id`'s three slots. `tables` holds each slot cell's `volume` routing table by cell. Slot `k` is the
/// owner, in directory order, at the number of earlier slots in the same cell (a DC with fewer than three cells wraps).
pub fn slot_nodes(
    id: &ActorId,
    topology: &Topology,
    tables: &BTreeMap<String, Arc<Table>>,
) -> Result<[Slot; SLOTS], Error> {
    let cells = actors::slots(id, topology).map_err(Error::Placement)?;
    let hash = ActorId::hash_of(&id.to_string()).map_err(Error::Placement)?;
    let mut nodes = Vec::with_capacity(SLOTS);
    for (k, cell) in cells.iter().enumerate() {
        let n = cells[..k].iter().filter(|c| *c == cell).count();
        let missing =
            |what: &str| Error::Unavailable(format!("cell {cell} has no {what} for slot {k}"));
        let table = tables.get(*cell).ok_or_else(|| missing("volume table"))?;
        let owner = table
            .directories
            .get(VOLUME)
            .and_then(|d| placement::owners(d, hash).get(n))
            .ok_or_else(|| missing("range owner"))?;
        let member = table
            .members
            .get(&owner.node)
            .ok_or_else(|| missing("member record"))?;
        nodes.push(Slot {
            cell: (*cell).to_owned(),
            address: member.address.clone(),
            identity: tls::Identity {
                service: member.service.clone(),
                cell: member.cell.clone(),
                node_id: member.node_id.clone(),
                host: host_of(&member.address).into(),
            },
        });
    }
    Ok(nodes.try_into().expect("one node per slot"))
}

async fn call<T, Fut>(
    peers: &Peers,
    slot: &Slot,
    f: impl FnOnce(VolumeClient<Channel>) -> Fut,
) -> Result<T, Status>
where
    Fut: Future<Output = Result<tonic::Response<T>, Status>>,
{
    let channel = peers
        .pinned(&slot.address, Class::Control, slot.identity.clone())
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let reply = peers
        .call(
            &slot.address,
            Class::Control,
            CALL,
            f(VolumeClient::new(channel)),
        )
        .await?;
    Ok(reply.into_inner())
}

/// Runs `calls` concurrently and returns `(index, output)` as they finish, until `enough` holds or all have.
async fn gather<F>(
    calls: impl IntoIterator<Item = F>,
    enough: impl Fn(&[(usize, F::Output)]) -> bool,
) -> Vec<(usize, F::Output)>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel();
    for (i, call) in calls.into_iter().enumerate() {
        let tx = tx.clone();
        tokio::spawn(async move {
            let _ = tx.send((i, call.await));
        });
    }
    drop(tx);
    let mut done = Vec::new();
    while !enough(&done) {
        match rx.recv().await {
            Some(output) => done.push(output),
            None => break,
        }
    }
    done
}

/// The reply exceeded the client's decode limit.
fn too_large(status: &Status) -> bool {
    status.code() == Code::OutOfRange
}

/// Why the slot refused, `Some(Refusal::None)` for an acceptance. A code this client does not know (a newer slot) is
/// `None`: never an acceptance, a grant or an acknowledgement.
fn refused(answer: &pb::Answer) -> Option<Refusal> {
    Refusal::try_from(answer.refused).ok()
}

fn slot_of(answer: &pb::Answer) -> pb::Slot {
    answer.slot.clone().unwrap_or_default()
}

/// Accepted by a voting slot.
fn counts(answer: &Result<pb::Answer, Status>) -> bool {
    answer
        .as_ref()
        .is_ok_and(|a| refused(a) == Some(Refusal::None) && slot_of(a).voting)
}

fn quorum(answers: &[(usize, Result<pb::Answer, Status>)]) -> usize {
    answers.iter().filter(|(_, a)| counts(a)).count()
}

/// The highest sequence two counted slots hold, once it reaches the claim's no-op: an older epoch's entry commits only
/// under one of this claim's (Raft's Figure 8 rule). A slot counts while it votes and still takes this writer's
/// appends.
fn quorum_point(acked: &[u64; SLOTS], counted: &[bool; SLOTS], noop: u64) -> Option<u64> {
    let mut held: Vec<u64> = (0..SLOTS)
        .filter(|&i| counted[i])
        .map(|i| acked[i])
        .collect();
    held.sort_unstable_by(|a, b| b.cmp(a));
    held.get(1).copied().filter(|&seq| seq >= noop)
}

/// A Compute host's heartbeats: one per [`HEARTBEAT`] to every volume node holding a slot of an actor it holds.
pub struct Host {
    peers: Arc<Peers>,
    holder: pb::Holder,
    nodes: Mutex<BTreeMap<String, Node>>,
}

struct Node {
    slot: Slot,
    users: usize,
    /// When the newest acknowledged heartbeat was sent.
    acked: Option<Instant>,
}

impl Host {
    /// Starts heartbeating as `holder` (this host's cell and node ID); stops once the host is dropped.
    pub fn start(peers: Arc<Peers>, holder: pb::Holder) -> Arc<Self> {
        let host = Arc::new(Host {
            peers,
            holder,
            nodes: Mutex::default(),
        });
        let weak = Arc::downgrade(&host);
        tokio::spawn(async move {
            loop {
                sleep(HEARTBEAT).await;
                let Some(host) = weak.upgrade() else { break };
                let slots: Vec<Slot> = host.lock().values().map(|n| n.slot.clone()).collect();
                for slot in slots {
                    let host = host.clone();
                    tokio::spawn(async move { host.beat(slot).await });
                }
            }
        });
        host
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Node>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn beat(&self, slot: Slot) {
        let sent = bedrock_lib::monotonic_now();
        let request = pb::HeartbeatRequest {
            holder: Some(self.holder.clone()),
        };
        let beat = call(&self.peers, &slot, |mut c| async move {
            c.heartbeat(request).await
        });
        if beat.await.is_ok()
            && let Some(node) = self.lock().get_mut(&slot.address)
        {
            node.acked = node.acked.max(Some(sent));
        }
    }

    /// Adds `slots` to the heartbeat set. The guard leaves it on drop, also when the caller is cancelled.
    fn join(self: &Arc<Self>, slots: &[Slot; SLOTS]) -> Joined {
        let mut nodes = self.lock();
        for slot in slots {
            nodes
                .entry(slot.address.clone())
                .or_insert_with(|| Node {
                    slot: slot.clone(),
                    users: 0,
                    acked: None,
                })
                .users += 1;
        }
        Joined {
            host: self.clone(),
            slots: slots.clone(),
        }
    }

    /// Heartbeats `slots` once now.
    async fn beat_all(self: &Arc<Self>, slots: &[Slot]) {
        let beats = slots.iter().map(|slot| {
            let (host, slot) = (self.clone(), slot.clone());
            async move { host.beat(slot).await }
        });
        gather(beats, |_| false).await;
    }

    fn leave(&self, slots: &[Slot]) {
        let mut nodes = self.lock();
        for slot in slots {
            if let Some(node) = nodes.get_mut(&slot.address) {
                node.users -= 1;
                if node.users == 0 {
                    nodes.remove(&slot.address);
                }
            }
        }
    }

    /// When fewer than two of `slots` will have acknowledged a heartbeat sent within [`FENCE`], unless another is
    /// acknowledged first; `None` when fewer than two ever have.
    fn deadline(&self, slots: &[Slot]) -> Option<Instant> {
        let nodes = self.lock();
        let mut acked: Vec<Instant> = slots
            .iter()
            .filter_map(|s| nodes.get(&s.address).and_then(|n| n.acked))
            .collect();
        acked.sort_unstable_by(|a, b| b.cmp(a));
        acked.get(1).map(|sent| *sent + FENCE)
    }

    /// How many of `slots` acknowledged a heartbeat sent within [`FENCE`].
    fn live(&self, slots: &[Slot]) -> usize {
        let now = bedrock_lib::monotonic_now();
        let nodes = self.lock();
        slots
            .iter()
            .filter(|s| {
                nodes
                    .get(&s.address)
                    .and_then(|n| n.acked)
                    .is_some_and(|sent| now.duration_since(sent) < FENCE)
            })
            .count()
    }
}

/// The writer's log and each slot's progress.
struct State {
    /// Entries `base + 1 ..= last`.
    log: VecDeque<pb::Entry>,
    base: u64,
    /// The epoch of entry `base`.
    base_epoch: u64,
    noop: u64,
    /// Per slot: the next entry to send, the last entry it holds, whether it votes and the commit point it knows.
    next: [u64; SLOTS],
    acked: [u64; SLOTS],
    voting: [bool; SLOTS],
    told: [u64; SLOTS],
    commit: u64,
    /// Encoded bytes of this writer's entries above the commit point, and of committed entries still kept.
    pending: usize,
    retained: usize,
    /// Slots this writer stopped appending to (corrupt, behind what it keeps, or promised elsewhere); S-06 repairs them.
    lost: [bool; SLOTS],
    fenced: bool,
}

enum Next {
    Send(pb::AppendRequest),
    Idle,
}

impl State {
    fn last(&self) -> u64 {
        self.base + self.log.len() as u64
    }

    fn entry(&self, seq: u64) -> Option<&pb::Entry> {
        let i = seq.checked_sub(self.base + 1)?;
        self.log.get(usize::try_from(i).ok()?)
    }

    /// The epoch of entry `seq`, which lies between `base` and the log end.
    fn epoch_at(&self, seq: u64) -> u64 {
        if seq == self.base {
            self.base_epoch
        } else {
            self.entry(seq).expect("within the log").epoch
        }
    }

    /// Slot `i` holds the log through `through` and knows commit point `told`.
    fn ack(&mut self, i: usize, through: u64, voting: bool, told: u64) {
        self.acked[i] = self.acked[i].max(through);
        self.next[i] = self.next[i].max(through + 1);
        self.voting[i] = voting;
        self.told[i] = self.told[i].max(told);
        // A lost slot's last acknowledgement is stale: it may have lost those entries since.
        let counted = std::array::from_fn(|i| self.voting[i] && !self.lost[i]);
        if let Some(point) = quorum_point(&self.acked, &counted, self.noop)
            && point > self.commit
        {
            for seq in self.commit + 1..=point {
                let size = self.entry(seq).map_or(0, Message::encoded_len);
                if seq >= self.noop {
                    self.pending -= size;
                }
                self.retained += size;
            }
            self.commit = point;
        }
        // Keep what a slot still lacks, including the entry it holds last, which a commit-only append resends; but no
        // more than RETAIN committed entries or RETAIN_BYTES.
        let slowest = (0..SLOTS)
            .filter(|&i| !self.lost[i])
            .map(|i| self.acked[i])
            .min()
            .unwrap_or(self.commit);
        let floor = self
            .commit
            .min(slowest.saturating_sub(1))
            .max(self.commit.saturating_sub(RETAIN));
        while self.base < self.commit && (self.base < floor || self.retained > RETAIN_BYTES) {
            let entry = self
                .log
                .pop_front()
                .expect("committed entries lie within the log");
            self.retained -= entry.encoded_len();
            self.base = entry.seq;
            self.base_epoch = entry.epoch;
        }
    }

    /// Slot `i`'s next append: the log from its next entry, or after an idle wait the commit point alone (its last
    /// entry again, which the slot skips).
    fn request(&self, shared: &Shared, i: usize, idle: bool) -> Next {
        let last = self.last();
        let from = if self.next[i] <= last {
            self.next[i].max(self.base + 1)
        } else if idle && self.commit > self.told[i] && self.acked[i] > self.base {
            self.acked[i]
        } else {
            return Next::Idle;
        };
        let commit_only = self.next[i] > last;
        let prev_epoch = self.epoch_at(from - 1);
        let mut entries = Vec::new();
        let mut bytes = 0;
        for seq in from..=last {
            let entry = self.entry(seq).expect("within the log");
            bytes += entry.encoded_len();
            if !entries.is_empty() && (bytes > MAX_ENTRY_BYTES || commit_only) {
                break;
            }
            entries.push(entry.clone());
        }
        Next::Send(pb::AppendRequest {
            actor: Some(shared.actor.clone()),
            holder: Some(shared.joined.host.holder.clone()),
            epoch: shared.epoch,
            version: shared.version,
            prev_epoch,
            entries,
            commit: self.commit,
        })
    }
}

/// A writer's place in its host's heartbeat set, left on drop.
struct Joined {
    host: Arc<Host>,
    slots: [Slot; SLOTS],
}

impl Drop for Joined {
    fn drop(&mut self) {
        self.host.leave(&self.slots);
    }
}

struct Shared {
    joined: Joined,
    actor: pb::Actor,
    epoch: u64,
    version: u64,
    state: Mutex<State>,
    /// Woken on every change to `state`. Not a `watch`: it picks its waiter shard with tokio's thread RNG, which the
    /// simulation seed does not decide; `Notify` wakes in order.
    changed: Notify,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn fence(&self, state: &mut State) {
        state.fenced = true;
        self.changed.notify_waiters();
    }

    /// Slot `i` takes no more appends from this writer; with fewer than two left, no entry can commit.
    fn lose(&self, i: usize) {
        let mut state = self.lock();
        state.lost[i] = true;
        if state.lost.iter().filter(|lost| !**lost).count() < 2 {
            self.fence(&mut state);
        }
    }

    /// The slots still taking this writer's appends.
    fn usable(&self, state: &State) -> Vec<Slot> {
        (0..SLOTS)
            .filter(|&i| !state.lost[i])
            .map(|i| self.joined.slots[i].clone())
            .collect()
    }
}

/// Sends slot `i` the log until the writer stops.
async fn replicate(shared: Arc<Shared>, i: usize) {
    let mut idle = false;
    // When an idle slot is sent the commit point alone. Absolute, so other slots' acknowledgements, which wake this
    // task, cannot keep pushing it back.
    let mut due: Option<Instant> = None;
    loop {
        // Registered before reading the state, so no change between the two is missed.
        let mut changed = std::pin::pin!(shared.changed.notified());
        changed.as_mut().enable();
        let next = {
            let state = shared.lock();
            if state.fenced {
                return;
            }
            state.request(&shared, i, idle)
        };
        idle = false;
        let request = match next {
            Next::Send(request) => {
                due = None;
                request
            }
            Next::Idle => {
                let at = *due.get_or_insert_with(|| bedrock_lib::monotonic_now() + HEARTBEAT);
                tokio::select! {
                    biased;
                    () = changed => {}
                    () = sleep_until(at) => {
                        idle = true;
                        due = None;
                    }
                }
                continue;
            }
        };
        let from = request.entries[0].seq;
        let last = request.entries.last().expect("an append carries entries");
        let (through, release) = (last.seq, last.kind == Kind::Release as i32);
        let answer = call(
            &shared.joined.host.peers,
            &shared.joined.slots[i],
            |mut c| async move {
                if release {
                    c.release(request).await
                } else {
                    c.append(request).await
                }
            },
        )
        .await;
        let answer = match answer {
            Ok(answer) => answer,
            // The slot refuses this request itself, so a retry cannot help: stop rather than hold every commit.
            Err(status)
                if matches!(
                    status.code(),
                    Code::InvalidArgument
                        | Code::ResourceExhausted
                        | Code::OutOfRange
                        | Code::Unimplemented
                ) =>
            {
                shared.fence(&mut shared.lock());
                return;
            }
            // A corrupt slot is dropped until it repairs.
            Err(status) if status.code() == Code::DataLoss => return shared.lose(i),
            Err(_) => {
                sleep(RETRY).await;
                continue;
            }
        };
        let slot = slot_of(&answer);
        let mut state = shared.lock();
        match refused(&answer) {
            Some(Refusal::None) => {
                // The commit point the slot learned, which is below the one sent while it lacks entries up to it.
                state.ack(i, through, slot.voting, slot.committed);
                shared.changed.notify_waiters();
            }
            Some(Refusal::LogGap) => {
                if from <= state.base + 1 {
                    drop(state);
                    return shared.lose(i);
                }
                let end = slot.end.unwrap_or_default().seq;
                state.next[i] = (from - 1).min(end + 1);
            }
            // A newer claim or topology took the slot: stop serving. A slot that lost our promise in a wipe is
            // dropped until it repairs.
            Some(refusal @ (Refusal::NotPromised | Refusal::StaleVersion)) => {
                if refusal == Refusal::StaleVersion || slot.epoch > shared.epoch {
                    return shared.fence(&mut state);
                }
                drop(state);
                return shared.lose(i);
            }
            _ => {
                drop(state);
                sleep(RETRY).await;
            }
        }
    }
}

/// Fences the writer once its lease deadline passes without a heartbeat extending it.
async fn lease(shared: Arc<Shared>) {
    loop {
        let usable = shared.usable(&shared.lock());
        let deadline = shared.joined.host.deadline(&usable);
        match deadline {
            Some(deadline) if deadline > bedrock_lib::monotonic_now() => {
                sleep_until(deadline).await
            }
            _ => {
                shared.fence(&mut shared.lock());
                return;
            }
        }
    }
}

/// An open actor: the holder's writer over its three slots.
pub struct Writer {
    shared: Arc<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Writer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Writer {
    /// Reserves, claims and recovers `actor` on `slots` as `host`'s holder, at topology `version`; opens once the claim's
    /// no-op commits. [`Error::Held`] names a holder whose lease is still live.
    pub async fn open(
        host: Arc<Host>,
        actor: pb::Actor,
        slots: [Slot; SLOTS],
        version: u64,
    ) -> Result<Self, Error> {
        Self::open_as(host, actor, slots, version, Kind::Noop).await
    }

    /// The mover of a DC change (S-17, `Move` in `volume_dc.tla`): claims `actor` on its old DC's `slots` like
    /// [`Writer::open`], but its claim entry is `MOVE` instead of the no-op. Returns the `MOVE`'s epoch and sequence once
    /// it committed; from then on no claim in this DC serves the actor. A claimant that finds a `MOVE` commits it again.
    pub async fn move_out(
        host: Arc<Host>,
        actor: pb::Actor,
        slots: [Slot; SLOTS],
        version: u64,
    ) -> Result<(u64, u64), Error> {
        let writer = Self::open_as(host, actor, slots, version, Kind::Move).await?;
        let seq = writer.shared.lock().noop;
        // The slots flush and truncate only what they know committed: tell two of them before stopping.
        let told =
            writer.wait(|s| (s.told.iter().filter(|t| **t >= seq).count() >= 2).then_some(()));
        let told = tokio::time::timeout(FENCE, told).await;
        writer.shared.fence(&mut writer.shared.lock());
        match told {
            Ok(result) => result?,
            Err(_) => {
                return Err(Error::Unavailable(
                    "the slots did not learn that MOVE committed".into(),
                ));
            }
        }
        Ok((writer.shared.epoch, seq))
    }

    /// Claims and recovers `actor`, then opens once the claim's entry of `kind` (the no-op, or `MOVE`) commits. A
    /// serving claim refuses an actor that moved: one whose slots truncated a committed `MOVE` or whose recovered log
    /// holds one.
    async fn open_as(
        host: Arc<Host>,
        actor: pb::Actor,
        slots: [Slot; SLOTS],
        version: u64,
        kind: Kind,
    ) -> Result<Self, Error> {
        // Leaves the heartbeat set on every early return and on cancellation.
        let joined = host.join(&slots);
        host.beat_all(&slots).await;
        if host.live(&slots) < 2 {
            return Err(Error::Unavailable(
                "fewer than two slots answered a heartbeat".into(),
            ));
        }
        let peers = host.peers.clone();
        let holder = host.holder.clone();
        let mut epoch = 1;
        for _ in 0..ATTEMPTS {
            let reserves = slots.iter().map(|slot| {
                let (peers, slot) = (peers.clone(), slot.clone());
                let request = pb::ReserveRequest {
                    actor: Some(actor.clone()),
                    epoch,
                    version,
                };
                async move {
                    call(
                        &peers,
                        &slot,
                        |mut c| async move { c.reserve(request).await },
                    )
                    .await
                }
            });
            let answers = gather(reserves, |a| quorum(a) >= 2).await;
            let mut seen = epoch;
            let mut unimported = false;
            for (_, answer) in &answers {
                if let Ok(answer) = answer {
                    if refused(answer) == Some(Refusal::StaleVersion) {
                        return Err(Error::StaleVersion);
                    }
                    unimported |= refused(answer) == Some(Refusal::NotImported);
                    let slot = slot_of(answer);
                    seen = seen.max(slot.reserved).max(slot.epoch);
                }
            }
            if quorum(&answers) < 2 {
                // An import needs only two slots, so the third may refuse while the other two grant.
                if unimported {
                    return Err(not_imported());
                }
                epoch = seen + 1;
                sleep(RETRY).await;
                continue;
            }
            let claims =
                slots.iter().map(|slot| {
                    let (peers, slot) = (peers.clone(), slot.clone());
                    let request = pb::ClaimRequest {
                        actor: Some(actor.clone()),
                        holder: Some(holder.clone()),
                        epoch,
                        version,
                    };
                    async move {
                        call(&peers, &slot, |mut c| async move { c.claim(request).await }).await
                    }
                });
            let answers = gather(claims, |a| quorum(a) >= 2).await;
            if quorum(&answers) >= 2 {
                let granted = answers.iter().filter(|(_, a)| counts(a)).map(|(i, a)| {
                    (
                        slot_of(a.as_ref().expect("counted"))
                            .end
                            .unwrap_or_default(),
                        *i,
                    )
                });
                let (end, from) = granted
                    .max_by_key(|(end, i)| (end.epoch, end.seq, std::cmp::Reverse(*i)))
                    .expect("two grants");
                let truncated_move = answers
                    .iter()
                    .filter(|(_, a)| counts(a))
                    .any(|(_, a)| slot_of(a.as_ref().expect("counted")).moved.is_some());
                let state =
                    recover(&peers, &slots[from], &actor, &holder, epoch, end, kind).await?;
                let recovered_move = state
                    .log
                    .iter()
                    .rev()
                    .skip(1)
                    .any(|e| e.kind == Kind::Move as i32);
                if kind != Kind::Move && (truncated_move || recovered_move) {
                    return Err(Error::Moved);
                }
                return Self::start(Arc::new(Shared {
                    joined,
                    actor,
                    epoch,
                    version,
                    state: Mutex::new(state),
                    changed: Notify::new(),
                }))
                .await;
            }
            for (_, answer) in answers
                .iter()
                .filter_map(|(i, a)| Some((i, a.as_ref().ok()?)))
            {
                let slot = slot_of(answer);
                match refused(answer) {
                    Some(Refusal::LeaseLive) => {
                        return Err(Error::Held(slot.holder.unwrap_or_default()));
                    }
                    Some(Refusal::StaleVersion) => return Err(Error::StaleVersion),
                    Some(Refusal::NotImported) => return Err(not_imported()),
                    _ => seen = seen.max(slot.reserved).max(slot.epoch),
                }
            }
            epoch = seen + 1;
            sleep(RETRY).await;
        }
        Err(Error::Unavailable(
            "no epoch was reserved and claimed at a quorum".into(),
        ))
    }

    /// Starts the slot tasks and waits for the no-op to commit.
    async fn start(shared: Arc<Shared>) -> Result<Self, Error> {
        let mut tasks: Vec<_> = (0..SLOTS)
            .map(|i| tokio::spawn(replicate(shared.clone(), i)))
            .collect();
        tasks.push(tokio::spawn(lease(shared.clone())));
        let writer = Writer { shared, tasks };
        let noop = writer.shared.lock().noop;
        match tokio::time::timeout(FENCE, writer.wait(|s| (s.commit >= noop).then_some(()))).await {
            Ok(Ok(())) => Ok(writer),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Unavailable(
                "the claim's no-op did not commit".into(),
            )),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.shared.epoch
    }

    /// The commit point: every entry through it is acknowledged by two voting slots.
    pub fn committed(&self) -> u64 {
        self.shared.lock().commit
    }

    /// Whether this writer may still serve: it has not stopped, and two of the slots still taking its appends
    /// acknowledged a heartbeat sent within [`FENCE`]. Once false it stays false; the actor host claims again.
    pub fn serving(&self) -> bool {
        let mut state = self.shared.lock();
        if !state.fenced && self.shared.joined.host.live(&self.shared.usable(&state)) < 2 {
            self.shared.fence(&mut state);
        }
        !state.fenced
    }

    /// Waits until `ready` returns a value, failing once the writer stops serving, even when an acknowledgement that
    /// arrived after the fence made it ready.
    async fn wait<R>(&self, mut ready: impl FnMut(&mut State) -> Option<R>) -> Result<R, Error> {
        loop {
            let mut changed = std::pin::pin!(self.shared.changed.notified());
            changed.as_mut().enable();
            {
                let mut state = self.shared.lock();
                if state.fenced {
                    return Err(Error::Fenced);
                }
                if let Some(value) = ready(&mut state) {
                    return Ok(value);
                }
            }
            if !self.serving() {
                return Err(Error::Fenced);
            }
            tokio::select! {
                biased;
                () = changed => {}
                () = sleep(HEARTBEAT) => {}
            }
        }
    }

    /// Commits `changes` and the alarm change as one entry; returns its sequence once two voting slots hold it.
    /// `committed_at` is carried unchanged.
    pub async fn commit(
        &self,
        changes: Vec<pb::Change>,
        alarm: Option<pb::Alarm>,
        committed_at: u64,
    ) -> Result<u64, Error> {
        self.append(Kind::Data, changes, alarm, committed_at).await
    }

    /// Appends the `RELEASE` entry and stops: after it commits, a new claim needs no lease wait.
    pub async fn release(self, committed_at: u64) -> Result<u64, Error> {
        let seq = self
            .append(Kind::Release, Vec::new(), None, committed_at)
            .await?;
        self.shared.fence(&mut self.shared.lock());
        Ok(seq)
    }

    async fn append(
        &self,
        kind: Kind,
        changes: Vec<pb::Change>,
        alarm: Option<pb::Alarm>,
        committed_at: u64,
    ) -> Result<u64, Error> {
        let mut entry = Some(pb::Entry {
            seq: 0,
            epoch: self.shared.epoch,
            committed_at,
            kind: kind as i32,
            changes,
            alarm,
        });
        // The sequence adds at most ten bytes.
        let size = entry.as_ref().map_or(0, Message::encoded_len) + 10;
        if size > MAX_ENTRY_BYTES {
            return Err(Error::TooLarge(size));
        }
        let seq = self
            .wait(|state| {
                let in_flight = state.last() - state.commit;
                let room = in_flight == 0
                    || (in_flight < WINDOW_ENTRIES && state.pending + size <= WINDOW_BYTES);
                if !room {
                    return None;
                }
                let mut entry = entry.take().expect("pushed once");
                entry.seq = state.last() + 1;
                // Counted with its sequence, as the commit releases it.
                state.pending += entry.encoded_len();
                state.log.push_back(entry);
                Some(state.last())
            })
            .await?;
        self.shared.changed.notify_waiters();
        self.wait(|state| (state.commit >= seq).then_some(seq))
            .await
    }

    /// `keys` at the commit point or later from the nearest slot caught up with it; a missing key has no value. A value
    /// may come from this writer's own pending commit (see the module docs).
    ///
    /// A reply too large to decode is split: each half is read on its own, at the commit point when it is sent.
    pub async fn read(&self, keys: Vec<Vec<u8>>) -> Result<Vec<pb::Value>, Error> {
        match self.read_once(keys.clone()).await {
            Err(Error::Rpc(status)) if too_large(&status) && keys.len() > 1 => {
                let (first, second) = keys.split_at(keys.len() / 2);
                let mut values = Box::pin(self.read(first.to_vec())).await?;
                values.extend(Box::pin(self.read(second.to_vec())).await?);
                Ok(values)
            }
            other => other,
        }
    }

    async fn read_once(&self, keys: Vec<Vec<u8>>) -> Result<Vec<pb::Value>, Error> {
        let request = pb::ReadRequest {
            actor: Some(self.shared.actor.clone()),
            keys,
            min_seq: self.committed(),
        };
        let reply = self
            .nearest(
                |mut c| {
                    let request = request.clone();
                    async move { c.read(request).await }
                },
                |r: &pb::ReadReply| r.answer.as_ref(),
            )
            .await?;
        Ok(reply.values)
    }

    /// The aligned [`CHUNK_PAGES`] SQLite pages around `page`, keyed by big-endian `u32` page number.
    pub async fn read_chunk(&self, page: u32) -> Result<Vec<pb::Value>, Error> {
        let first = page & !(CHUNK_PAGES - 1);
        let keys = (first..=first + (CHUNK_PAGES - 1))
            .map(|p| p.to_be_bytes().to_vec())
            .collect();
        self.read(keys).await
    }

    /// Up to `limit` live keys from `start` in order at the commit point or later, and whether more follow. Like
    /// [`Writer::read`], it may include this writer's own pending commits.
    /// A reply too large to decode is asked again with half the limit, so it returns fewer keys and `more`.
    pub async fn scan(&self, start: Vec<u8>, limit: u32) -> Result<(Vec<pb::Value>, bool), Error> {
        // Above the slot's own clamp, halving would ask for the same page again.
        let mut limit = limit.min(SCAN_LIMIT);
        loop {
            match self.scan_once(start.clone(), limit).await {
                Err(Error::Rpc(status)) if too_large(&status) && limit > 1 => limit /= 2,
                other => return other,
            }
        }
    }

    async fn scan_once(&self, start: Vec<u8>, limit: u32) -> Result<(Vec<pb::Value>, bool), Error> {
        let request = pb::ScanRequest {
            actor: Some(self.shared.actor.clone()),
            start,
            limit,
            min_seq: self.committed(),
        };
        let reply = self
            .nearest(
                |mut c| {
                    let request = request.clone();
                    async move { c.scan(request).await }
                },
                |r: &pb::ScanReply| r.answer.as_ref(),
            )
            .await?;
        Ok((reply.values, reply.more))
    }

    /// Tries the slots in the holder's cell first, then the rest in slot order, until one promised to this epoch is
    /// caught up. A slot still promised to an older epoch may hold that epoch's uncommitted tail.
    async fn nearest<T, Fut>(
        &self,
        f: impl Fn(VolumeClient<Channel>) -> Fut,
        answer: impl Fn(&T) -> Option<&pb::Answer>,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let shared = &self.shared;
        let mut order: Vec<usize> = (0..SLOTS).collect();
        order.sort_by_key(|&i| shared.joined.slots[i].cell != shared.joined.host.holder.cell);
        let mut last = String::from("no slot answered");
        for i in order {
            // A writer that stopped serving may be behind a newer holder.
            if !self.serving() {
                return Err(Error::Fenced);
            }
            match call(&shared.joined.host.peers, &shared.joined.slots[i], &f).await {
                Ok(reply) => match answer(&reply).map(|a| (refused(a), slot_of(a).epoch)) {
                    Some((Some(Refusal::None), epoch)) if epoch == shared.epoch => {
                        if !self.serving() {
                            return Err(Error::Fenced);
                        }
                        return Ok(reply);
                    }
                    Some((Some(Refusal::None), epoch)) => {
                        last = format!("slot {i} is promised to epoch {epoch}");
                    }
                    refusal => last = format!("slot {i} refused: {refusal:?}"),
                },
                Err(status) if transient(&status) => last = format!("slot {i}: {status}"),
                Err(status) => return Err(Error::Rpc(status)),
            }
        }
        Err(Error::Unavailable(last))
    }
}

impl State {
    fn empty() -> Self {
        State {
            log: VecDeque::new(),
            base: 0,
            base_epoch: 0,
            noop: 0,
            next: [1; SLOTS],
            acked: [0; SLOTS],
            voting: [true; SLOTS],
            told: [0; SLOTS],
            commit: 0,
            pending: 0,
            retained: 0,
            lost: [false; SLOTS],
            fenced: false,
        }
    }
}

fn not_imported() -> Error {
    Error::Unavailable("the actor is moving into this DC and is not imported yet".into())
}

/// Where the actor's first `MOVE` is, and the alarm as of the flushed point, from the first slot that truncated it: S3
/// then holds it, since a slot truncates only after the flush is durable. `None` while no answering slot has; a slot's
/// explicit failure, when no slot answered with the `MOVE`, is returned rather than taken for an unflushed one.
pub async fn moved(
    host: &Host,
    actor: &pb::Actor,
    slots: &[Slot; SLOTS],
) -> Result<Option<(pb::LogEnd, Option<u64>)>, Error> {
    let reads = slots.iter().map(|slot| {
        let (peers, slot) = (host.peers.clone(), slot.clone());
        let request = pb::ReadRequest {
            actor: Some(actor.clone()),
            ..Default::default()
        };
        async move { call(&peers, &slot, |mut c| async move { c.read(request).await }).await }
    });
    let answers = gather(reads, |_| false).await;
    let mut failed = None;
    for (_, reply) in answers {
        match reply {
            Ok(reply) => {
                let slot = reply.answer.as_ref().map(slot_of).unwrap_or_default();
                if let Some(moved) = slot.moved {
                    return Ok(Some((moved, slot.flushed_alarm_ms)));
                }
            }
            Err(status) if !transient(&status) => failed = Some(status),
            Err(_) => {}
        }
    }
    failed.map_or(Ok(None), |status| Err(Error::Rpc(status)))
}

/// A failure a retry may get past: the slot did not answer, or is not ready.
fn transient(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::FailedPrecondition
    )
}

/// Imports `actor` into the `slots` of its new DC at `seq`, whose entry has `epoch`, with `alarm_ms` (S-17, `Import` in
/// `volume_dc.tla`): from the flushed `MOVE` of a DC change, or a restore's record. Done once two voting slots hold it;
/// the third may follow later. Repeating it is a no-op.
pub async fn import(
    host: &Host,
    actor: &pb::Actor,
    slots: &[Slot; SLOTS],
    (epoch, seq, alarm_ms): (u64, u64, Option<u64>),
    version: u64,
) -> Result<(), Error> {
    let imports = slots.iter().map(|slot| {
        let (peers, slot) = (host.peers.clone(), slot.clone());
        let request = pb::ImportRequest {
            actor: Some(actor.clone()),
            epoch,
            seq,
            alarm_ms,
            version,
        };
        async move {
            call(
                &peers,
                &slot,
                |mut c| async move { c.import(request).await },
            )
            .await
        }
    });
    let answers = gather(imports, |_| false).await;
    if quorum(&answers) >= 2 {
        return Ok(());
    }
    Err(Error::Unavailable(
        "fewer than two slots of the new DC took the import".into(),
    ))
}

/// How long [`handover`] waits for the old DC to flush the `MOVE`.
pub const HANDOVER_FLUSH: Duration = Duration::from_secs(60);

/// Moves `actor` from its `old` DC's slots to its `new` DC's (S-17, `docs/spec/volume_dc.tla`): commits `MOVE` in the
/// old DC, waits until S3 holds it, then imports the new DC's slots at the log's first `MOVE`, which an earlier
/// handover may have committed. Every import starts at that one point, so the new slots agree. From the first `MOVE`
/// on, the old DC never serves the actor; the new DC serves it once imported. Safe to repeat after any failure;
/// returns the imported point as (epoch, sequence). A new DC whose slots held the actor before is refused before any
/// `MOVE` commits (see [`importable`]).
pub async fn handover(
    host: Arc<Host>,
    actor: &pb::Actor,
    old: [Slot; SLOTS],
    new: &[Slot; SLOTS],
    version: u64,
) -> Result<(u64, u64), Error> {
    let earlier = moved(&host, actor, &old).await?;
    let point = earlier.map(|(first, alarm_ms)| pb::Import {
        epoch: first.epoch,
        seq: first.seq,
        alarm_ms,
    });
    importable(&host, actor, new, point).await?;
    Writer::move_out(host.clone(), actor.clone(), old.clone(), version).await?;
    let deadline = bedrock_lib::monotonic_now() + HANDOVER_FLUSH;
    let (first, alarm_ms) = loop {
        if let Some(found) = moved(&host, actor, &old).await? {
            break found;
        }
        if bedrock_lib::monotonic_now() >= deadline {
            return Err(Error::Unavailable(
                "the old DC did not flush the MOVE in time".into(),
            ));
        }
        sleep(HEARTBEAT).await;
    };
    import(
        &host,
        actor,
        new,
        (first.epoch, first.seq, alarm_ms),
        version,
    )
    .await?;
    Ok((first.epoch, first.seq))
}

/// Checks, before any `MOVE` commits, that two of the new DC's `slots` can take the import at `point` (the first `MOVE`
/// of an earlier attempt, `None` for a first one): each is blank or already imported there. A slot that held the actor
/// before, as when it returns to a DC it left, refuses any other import, so the handover stops here, with the actor still
/// served by its old DC, instead of leaving it fenced there and unimportable here.
async fn importable(
    host: &Host,
    actor: &pb::Actor,
    slots: &[Slot; SLOTS],
    point: Option<pb::Import>,
) -> Result<(), Error> {
    let reads = slots.iter().map(|slot| {
        let (peers, slot) = (host.peers.clone(), slot.clone());
        let request = pb::ReadRequest {
            actor: Some(actor.clone()),
            ..Default::default()
        };
        async move { call(&peers, &slot, |mut c| async move { c.read(request).await }).await }
    });
    let (mut fit, mut held) = (0, 0);
    for (_, reply) in gather(reads, |_| false).await {
        let Some(answer) = reply.ok().and_then(|r| r.answer) else {
            continue;
        };
        let slot = slot_of(&answer);
        let blank = slot.imported.is_none()
            && slot.epoch == 0
            && slot.reserved == 0
            && slot.end.is_none()
            && slot.moved.is_none();
        if blank || (slot.imported.is_some() && slot.imported == point) {
            fit += 1;
        } else if slot.imported.is_none() || point.is_some() {
            held += 1;
        }
        // Imported while no old slot that answered names the first `MOVE`: unknown until one does.
    }
    if fit >= 2 {
        Ok(())
    } else if fit + (SLOTS - fit - held) >= 2 {
        Err(Error::Unavailable(
            "too few slots of the new DC answered".into(),
        ))
    } else {
        Err(Error::Rpc(Status::failed_precondition(
            "the new DC's slots held the actor before; a return to a DC it left is not supported",
        )))
    }
}

/// Reads the most advanced log, which ends at `end`, from `slot` and appends the claim's entry of `kind` (the no-op, or
/// `MOVE`) at `epoch`.
async fn recover(
    peers: &Peers,
    slot: &Slot,
    actor: &pb::Actor,
    holder: &pb::Holder,
    epoch: u64,
    end: pb::LogEnd,
    kind: Kind,
) -> Result<State, Error> {
    let mut log: Vec<pb::Entry> = Vec::new();
    // 0 asks for the first entry after the slot's flushed point.
    let mut from = 0;
    let mut flushed = pb::LogEnd::default();
    loop {
        let request = pb::LogRequest {
            actor: Some(actor.clone()),
            holder: Some(holder.clone()),
            epoch,
            from,
            limit: LOG_PAGE,
        };
        let reply = call(peers, slot, |c| async move {
            c.max_decoding_message_size(LOG_PAGE_BYTES)
                .log(request)
                .await
        })
        .await
        .map_err(|s| Error::Unavailable(format!("reading the log: {s}")))?;
        if reply.answer.as_ref().and_then(refused) != Some(Refusal::None) {
            return Err(Error::Unavailable(
                "the recovery slot took a newer claim".into(),
            ));
        }
        if from == 0 {
            let slot = reply.answer.as_ref().map(slot_of).unwrap_or_default();
            // The slot may have truncated a `MOVE` since it answered the claim; only the mover recovers past it.
            if kind != Kind::Move && slot.moved.is_some() {
                return Err(Error::Moved);
            }
            flushed = pb::LogEnd {
                seq: slot.flushed,
                epoch: slot.flushed_epoch,
            };
        }
        for entry in reply.entries {
            if log.last().is_some_and(|last| entry.seq != last.seq + 1) {
                return Err(Error::Unavailable("the recovered log has a gap".into()));
            }
            log.push(entry);
        }
        if !reply.more {
            break;
        }
        from = log.last().map_or(0, |e| e.seq + 1);
    }
    let base = log.first().map_or(end.seq, |e| e.seq - 1);
    if base + log.len() as u64 != end.seq {
        return Err(Error::Unavailable(
            "the recovered log does not end where the claim said".into(),
        ));
    }
    // The log starts after the slot's flushed point, or is empty and ends there.
    let base_epoch = if log.is_empty() {
        end.epoch
    } else if base == flushed.seq {
        flushed.epoch
    } else {
        return Err(Error::Unavailable(
            "the recovered log does not start after the flushed point".into(),
        ));
    };
    let noop = pb::Entry {
        seq: end.seq + 1,
        epoch,
        kind: kind as i32,
        ..Default::default()
    };
    // Everything through the flushed point is committed.
    let mut state = State {
        log: log.into(),
        base,
        base_epoch,
        commit: base,
        noop: noop.seq,
        pending: noop.encoded_len(),
        next: [noop.seq; SLOTS],
        ..State::empty()
    };
    state.log.push_back(noop);
    Ok(state)
}

#[cfg(test)]
#[path = "../../tests/unit/volume.rs"]
mod tests;
