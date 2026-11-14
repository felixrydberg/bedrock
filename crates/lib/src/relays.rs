//! Pushed-data relay sessions, bounded fan-out and disposable replay (#14/B-17, #16/B-18).
pub mod client;
pub mod introspection;
pub mod ranges;
pub mod replay;
pub mod siblings;
pub mod sources;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use prost::{Message, bytes::Bytes};
use sha2::Digest;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::placement::{hash_of, in_span, valid_span};
use crate::replication::Event;
use crate::replication::log::Watched;
use crate::replication::pb;
use crate::replication::writer::{RecoveryLease, Writer};
use crate::transport::{Caller, Class, Peers, host_of};
use replay::Replay;

pub const PROGRESS: Duration = Duration::from_secs(10);
pub const IDLE: Duration = Duration::from_secs(30);
/// How long a recovery waits for a slot before its subscriber has to retry.
const RECOVERY_WAIT: Duration = Duration::from_secs(5);
const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_EVENTS: usize = 1024;
type Key = (String, String);
/// A subscriber's stream: one namespace, optionally narrowed to a hash range `(start, end)`, `end` 0 meaning 2⁶⁴
/// (#22 / B-21).
pub type StreamKey = (Key, Option<(u64, u64)>);
/// The whole hash space: what a relay follows upstream for a dataset its subscribers follow by range.
const WHOLE: (u64, u64) = (0, 0);
/// The lowest cursor needed per namespace, and the datasets followed by range.
pub type Wanted = (BTreeMap<Key, u64>, BTreeSet<String>);
type Encoded = (u64, EncodedEvent);
type ReplayItem = (Event, u64, usize);
#[derive(Clone)]
struct EncodedEvent {
    bytes: Bytes,
    hash: u64,
}

fn span(range: Option<(u64, u64)>) -> Option<pb::Span> {
    range.map(|(start, end)| pb::Span { start, end })
}

fn range(span: Option<&pb::Span>) -> Option<(u64, u64)> {
    span.map(|s| (s.start, s.end))
}

/// Whether a stream selects the key with `hash`.
fn selects(stream: &StreamKey, hash: u64) -> bool {
    span(stream.1).is_none_or(|s| in_span(&s, hash))
}

/// Customer KV persists through the writer but has no pushed delivery contract.
pub fn pushed(dataset: &str) -> bool {
    crate::datasets::capabilities::pushed(dataset)
}

/// Whether a follow, or a sibling's interest, selects a pushed namespace or hash range.
fn selectable(follow: &pb::Follow) -> bool {
    pushed(&follow.dataset)
        && !follow.namespace.contains('\0')
        && follow.span.as_ref().is_none_or(valid_span)
        && !(follow.namespace.is_empty() && follow.span.is_none())
}

/// The writer's tier rank, above every relay tier.
pub const WRITER: u32 = 4;

pub fn tier_rank(tier: crate::node::Tier) -> u32 {
    match tier {
        crate::node::Tier::Cell => 1,
        crate::node::Tier::Dc => 2,
        crate::node::Tier::Region => 3,
    }
}

/// A cell-local source of committed dataset history and coherent recovery views.
pub trait LocalControlSource: Send + Sync {
    fn validate_namespace(&self, cell: &str, namespace: &str) -> Result<(), Status>;
    fn identity(&self) -> pb::ControlAck;
    fn validate_peer(&self, peer: &pb::ControlAck) -> Result<(), Status>;
    fn changed(&self) -> Arc<Watched<u64>>;
    fn head(&self, namespace: &str) -> Result<u64, Status>;
    fn read(&self, namespace: &str, from: u64, limit: usize) -> Result<Vec<Event>, Status>;
    fn view(&self, namespace: &str) -> Result<crate::replication::materialized::View, Status>;
}

#[derive(Clone)]
pub struct Hub(Arc<Inner>);

struct Inner {
    id: String,
    tier: u32,
    /// The relay's cell; with TLS, leaf callers must belong to it (B-55).
    cell: OnceLock<String>,
    control: OnceLock<Arc<dyn LocalControlSource>>,
    peers: Arc<Peers>,
    upstreams: Vec<String>,
    replay_peers: Vec<String>,
    writer: Option<Arc<Writer>>,
    replay: Option<Arc<Replay>>,
    subscribers: Mutex<BTreeMap<u64, Subscriber>>,
    heads: Mutex<BTreeMap<Key, Arc<Watched<Option<u64>>>>>,
    /// Counts what arrives from upstream per namespace, so only that namespace's streams wake for it (#66 / B-61).
    arrivals: Mutex<BTreeMap<Key, Arc<Watched<u64>>>>,
    recent: Mutex<Recent>,
    changed: Arc<Watched<u64>>,
    ids: AtomicU64,
    recoveries: Arc<tokio::sync::Semaphore>,
    clock: Mutex<Option<fn() -> u64>>,
    replay_queue: mpsc::Sender<ReplayItem>,
    replay_pending: Mutex<Option<mpsc::Receiver<ReplayItem>>>,
    replay_bytes: AtomicUsize,
    /// Draining members whose stream closed, kept as `LEFT` so cell relays remove them without grace (B-12).
    departed: Mutex<BTreeMap<MemberKey, (pb::Member, tokio::time::Instant)>>,
    members_changed: Arc<Watched<u64>>,
    /// What each sibling shares, by address (#41 / B-12b).
    shared: Mutex<BTreeMap<String, siblings::Shared>>,
    stats: introspection::Stats,
}

/// `(service, node_id)`.
pub type MemberKey = (String, String);

#[derive(Default)]
struct Recent {
    records: BTreeMap<(Key, u64), EncodedEvent>,
    order: VecDeque<(Key, u64)>,
    bytes: usize,
}

struct Subscriber {
    tx: mpsc::Sender<Result<pb::RelayFrame, Status>>,
    bytes: Arc<AtomicUsize>,
    events: Arc<AtomicUsize>,
    closed: Arc<Watched<bool>>,
    follows: BTreeMap<StreamKey, Interest>,
    /// Range follows across every namespace of a dataset.
    ranges: BTreeSet<(String, (u64, u64))>,
    changed: Arc<Watched<u64>>,
    member: Option<pb::Member>,
    /// The range set last pushed to this leaf member per dataset, and the latest one it acknowledged (#22 / B-21).
    pushed: BTreeMap<String, pb::Ranges>,
    ranges_acked: BTreeMap<String, u64>,
    /// When a session that replaced one on another relay opened (#41 / B-12b).
    moved: Option<tokio::time::Instant>,
}
struct Interest {
    from: u64,
    acked: u64,
    generation: u64,
    /// The subscriber's number for the follow, stamped on its deliveries and bootstraps (follows.tla).
    follow: u64,
    baseline: u64,
    /// A bootstrap was sent and its recovered confirmation not yet.
    confirming: bool,
    /// Acknowledgement of the queued bootstrap end, including when the revision did not advance.
    bootstrap_acked: bool,
    /// The head this stream must be acknowledged through to count as ready, fixed when it first could.
    target: Option<u64>,
    /// Wakes this stream's pump: its acknowledgements and its end. A subscriber-wide signal would wake every stream the
    /// subscriber follows on each acknowledgement, a cost that grows with the square of its ranges (B-61).
    woken: Arc<Watched<u64>>,
}

/// Accounts serialized queue bytes when tonic drains a subscriber's bounded queue.
pub struct Frames {
    rx: ReceiverStream<Result<pb::RelayFrame, Status>>,
    bytes: Arc<AtomicUsize>,
    events: Arc<AtomicUsize>,
    changed: Arc<Watched<u64>>,
    owner: Option<(Hub, u64)>,
}
impl Drop for Frames {
    fn drop(&mut self) {
        if let Some((hub, id)) = self.owner.take() {
            hub.remove(id);
        }
    }
}
/// Gives back `amount` of a queue budget. Taking more than was added would wrap and hold backpressure on forever.
fn release(counter: &AtomicUsize, amount: usize) {
    let before = counter.fetch_sub(amount, Ordering::Relaxed);
    assert!(before >= amount, "queue accounting underflowed");
}

impl Stream for Frames {
    type Item = Result<pb::RelayFrame, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = Pin::new(&mut self.rx).poll_next(cx);
        if let Poll::Ready(Some(Ok(ref frame))) = item {
            release(&self.bytes, frame.encoded_len());
            release(&self.events, queued_events(frame));
            self.changed.update(|v| *v = v.wrapping_add(1));
        }
        item
    }
}
fn queued_events(frame: &pb::RelayFrame) -> usize {
    match &frame.kind {
        Some(pb::relay_frame::Kind::Delivery(d)) => d.events.len(),
        Some(pb::relay_frame::Kind::Bootstrap(b)) => b.entries.len(),
        _ => 0,
    }
}

impl Hub {
    pub fn writer(id: String, writer: Arc<Writer>, peers: Arc<Peers>) -> Self {
        Self::new(
            id,
            WRITER,
            peers,
            Vec::new(),
            Vec::new(),
            Some(writer),
            None,
        )
    }
    pub fn relay(
        id: String,
        tier: u32,
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        replay_peers: Vec<String>,
        replay: Option<Arc<Replay>>,
    ) -> Self {
        Self::new(id, tier, peers, upstreams, replay_peers, None, replay)
    }
    fn new(
        id: String,
        tier: u32,
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        replay_peers: Vec<String>,
        writer: Option<Arc<Writer>>,
        replay: Option<Arc<Replay>>,
    ) -> Self {
        let (replay_queue, replay_pending) = mpsc::channel(MAX_EVENTS);
        Self(Arc::new(Inner {
            id,
            tier,
            cell: OnceLock::new(),
            control: OnceLock::new(),
            peers,
            upstreams,
            replay_peers,
            writer,
            replay,
            subscribers: Mutex::default(),
            heads: Mutex::default(),
            arrivals: Mutex::default(),
            recent: Mutex::default(),
            changed: Arc::new(Watched::new(0)),
            ids: AtomicU64::new(1),
            recoveries: Arc::new(tokio::sync::Semaphore::new(4)),
            clock: Mutex::new(None),
            replay_queue,
            replay_pending: Mutex::new(Some(replay_pending)),
            replay_bytes: AtomicUsize::new(0),
            departed: Mutex::default(),
            members_changed: Arc::new(Watched::new(0)),
            shared: Mutex::default(),
            stats: Default::default(),
        }))
    }
    pub fn with_clock(self, clock: fn() -> u64) -> Self {
        *self.0.clock.lock().unwrap() = Some(clock);
        self
    }
    pub fn with_cell(self, cell: String) -> Self {
        let _ = self.0.cell.set(cell);
        self
    }
    pub fn with_control(self, control: Arc<dyn LocalControlSource>) -> Self {
        let _ = self.0.control.set(control);
        self
    }
    /// Cell control never follows membership through the product replication hierarchy.
    pub(super) fn external(&self, dataset: &str) -> bool {
        dataset != crate::membership::DATASET || self.0.control.get().is_none()
    }
    fn local_control(&self, key: &Key) -> Result<Option<&Arc<dyn LocalControlSource>>, Status> {
        if self.external(&key.0) {
            return Ok(None);
        }
        if let Some(control) = self.0.control.get() {
            control.validate_namespace(self.cell(), &key.1)?;
        }
        Ok(self.0.control.get())
    }
    fn cell(&self) -> &str {
        self.0.cell.get().map_or("", String::as_str)
    }
    /// Caller owns and aborts the upstream task on shutdown.
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let hub = self.clone();
        let pending = self.0.replay_pending.lock().unwrap().take();
        tokio::spawn(async move {
            if let Some(pending) = pending {
                tokio::select! {biased;
                    ()=hub.upstream()=>{},
                    ()=hub.persist_replay(pending)=>{},
                }
            } else {
                hub.upstream().await
            }
        })
    }
    async fn persist_replay(&self, mut pending: mpsc::Receiver<ReplayItem>) {
        let mut rebuild_after = crate::monotonic_now();
        while let Some((event, mut now, size)) = pending.recv().await {
            release(&self.0.replay_bytes, size);
            let mut events = vec![event];
            while events.len() < 256 {
                let Ok((event, at, size)) = pending.try_recv() else {
                    break;
                };
                release(&self.0.replay_bytes, size);
                now = now.max(at);
                events.push(event);
            }
            let Some(replay) = self.0.replay.clone() else {
                continue;
            };
            let can_rebuild = crate::monotonic_now() >= rebuild_after;
            let operation = move || {
                if !replay.healthy() {
                    if !can_rebuild {
                        return Ok(());
                    }
                    replay.rebuild()?;
                }
                replay.append_batch(&events, now)
            };
            #[cfg(not(madsim))]
            let result = tokio::task::spawn_blocking(operation)
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string()));
            #[cfg(madsim)]
            let result = operation().map_err(|e| e.to_string());
            if let Err(error) = result {
                rebuild_after = crate::monotonic_now() + Duration::from_secs(1);
                tracing::warn!(%error,"replay persistence disabled; forwarding continues");
            }
        }
    }
    fn changed(&self) {
        self.0.changed.update(|v| *v = v.wrapping_add(1));
    }
    /// Bumped whenever [`Hub::members`] may have changed.
    pub fn member_version(&self) -> Arc<Watched<u64>> {
        self.0.members_changed.clone()
    }
    fn members_changed(&self) {
        self.0.members_changed.update(|v| *v = v.wrapping_add(1));
    }
    /// Members on this relay's leaf sessions; the newest session wins a duplicate identity. Graceful leavers stay
    /// `LEFT` for [`IDLE`].
    pub fn members(&self) -> siblings::Members {
        let mut members = BTreeMap::new();
        let now = crate::monotonic_now();
        let mut departed = self.0.departed.lock().unwrap();
        let before = departed.len();
        departed.retain(|_, (_, at)| now.duration_since(*at) < IDLE);
        if departed.len() != before {
            self.members_changed();
        }
        for (key, (member, _)) in departed.iter() {
            members.insert(key.clone(), member.clone());
        }
        drop(departed);
        for sub in self.0.subscribers.lock().unwrap().values() {
            if let Some(member) = &sub.member {
                members.insert(crate::membership::key(member), member.clone());
            }
        }
        members
    }

    /// The newest session of each leaf member, as [`Hub::members`] resolves a duplicate identity.
    fn newest(subscribers: &BTreeMap<u64, Subscriber>) -> BTreeMap<MemberKey, u64> {
        subscribers
            .iter()
            .filter_map(|(id, sub)| Some((crate::membership::key(sub.member.as_ref()?), *id)))
            .collect()
    }

    /// Pushes a leaf member its range set of one dataset from the published directory (#22 / B-21), unless its
    /// session already has that set. A matching set is suppressed even if its revision differs; a missing session is
    /// ignored. Relays push only published state.
    pub fn push_ranges(&self, member: &MemberKey, frame: pb::Ranges) {
        let id = {
            let mut subscribers = self.0.subscribers.lock().unwrap();
            let Some(&id) = Self::newest(&subscribers).get(member) else {
                return;
            };
            let sub = subscribers.get_mut(&id).unwrap();
            if sub
                .pushed
                .get(&frame.dataset)
                .is_some_and(|pushed| pushed.spans == frame.spans)
            {
                return;
            }
            sub.pushed.insert(frame.dataset.clone(), frame.clone());
            id
        };
        self.send(id, pb::relay_frame::Kind::Ranges(frame));
    }

    fn ranges_acked(&self, id: u64, ack: pb::RangesAck) {
        let mut subscribers = self.0.subscribers.lock().unwrap();
        let Some(sub) = subscribers.get_mut(&id) else {
            return;
        };
        if !ack.refused.is_empty() {
            tracing::warn!(
                member = ?sub.member.as_ref().map(crate::membership::key),
                dataset = %ack.dataset,
                refused = ?ack.refused,
                reason = %ack.reason,
                "a member refused a revoke that its directory view does not confirm"
            );
            // Forget the refused set, so the next publish pass pushes it again.
            if sub
                .pushed
                .get(&ack.dataset)
                .is_some_and(|pushed| pushed.revision == ack.revision)
            {
                sub.pushed.remove(&ack.dataset);
            }
        }
        let acked = sub.ranges_acked.entry(ack.dataset).or_default();
        *acked = (*acked).max(ack.revision);
    }

    /// The ranges each leaf member of this relay is ready for, per dataset (#22 / B-21, `docs/spec/handoff.tla`,
    /// `ReadyFor`): the member acknowledged the latest set pushed to it, and in every namespace this relay knows the
    /// range's stream finished any bootstrap and was acknowledged through the head seen once that held. The member
    /// sends its stream changes before `RangesAck`, so a stream it dropped is gone by then and stale acknowledgements
    /// never count. A pushed dataset whose namespaces the relay has not learned yet is not ready.
    pub fn ready(&self) -> siblings::Ready {
        let datasets: BTreeSet<String> = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .values()
            .flat_map(|sub| sub.pushed.keys().cloned())
            // An unpushed ranged dataset (`volume`) has no stream to finish. Its owners report readiness themselves
            // (S-06); until then they stay populating and never vote.
            .filter(|dataset| pushed(dataset))
            .collect();
        let heads: BTreeMap<String, Vec<(String, u64)>> = datasets
            .into_iter()
            .map(|dataset| {
                let heads = self
                    .namespaces(&dataset)
                    .into_iter()
                    .map(|namespace| {
                        let head = self.source_head(&(dataset.clone(), namespace.clone()));
                        (namespace, head.unwrap_or(0))
                    })
                    .collect();
                (dataset, heads)
            })
            .collect();
        let mut ready = siblings::Ready::new();
        let mut subscribers = self.0.subscribers.lock().unwrap();
        for (member, id) in Self::newest(&subscribers) {
            let Subscriber {
                pushed: frames,
                ranges_acked,
                follows,
                ..
            } = subscribers.get_mut(&id).unwrap();
            for (dataset, frame) in frames.iter() {
                // A dataset pushed since `heads` was read is not ready this pass.
                let Some(namespaces) = heads.get(dataset) else {
                    continue;
                };
                if ranges_acked
                    .get(dataset)
                    .is_none_or(|r| *r < frame.revision)
                {
                    continue;
                }
                let spans: Vec<(u64, u64)> = frame
                    .spans
                    .iter()
                    .map(|s| (s.start, s.end))
                    .filter(|range| {
                        namespaces.iter().all(|(namespace, head)| {
                            follows
                                .get_mut(&((dataset.clone(), namespace.clone()), Some(*range)))
                                .is_some_and(|interest| {
                                    !interest.confirming
                                        && interest.acked >= *interest.target.get_or_insert(*head)
                                })
                        })
                    })
                    .collect();
                if !spans.is_empty() {
                    ready.insert((member.clone(), dataset.clone()), spans);
                }
            }
        }
        ready
    }
    fn head(&self, key: &Key) -> Arc<Watched<Option<u64>>> {
        let mut heads = self.0.heads.lock().unwrap();
        heads
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Watched::new(None)))
            .clone()
    }
    fn source_head(&self, key: &Key) -> Option<u64> {
        match self.local_control(key) {
            Ok(Some(control)) => return control.head(&key.1).ok(),
            Err(_) => return None,
            Ok(None) => {}
        }
        if let Some(writer) = &self.0.writer {
            return Some(writer.log().head(&key.0, &key.1).get());
        }
        let head = self.head(key);
        if head.get().is_none()
            && let Some(last) = self
                .0
                .replay
                .as_ref()
                .and_then(|replay| replay.ranges(&key.0, &key.1).last().map(|range| range.1))
        {
            head.update(|current| {
                if current.is_none() {
                    *current = Some(last)
                }
            });
        }
        head.get()
    }
    fn arrival(&self, key: &Key) -> Arc<Watched<u64>> {
        self.0
            .arrivals
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone()
    }
    /// Wakes the streams of `key`: something arrived from upstream, possibly below the head they last saw.
    fn arrived(&self, key: &Key) {
        self.arrival(key).update(|v| *v = v.wrapping_add(1));
    }
    fn observe_head(&self, key: &Key, sequence: u64) {
        self.head(key)
            .update(|head| *head = Some(head.unwrap_or(0).max(sequence)));
    }
    /// Live subscriptions start after the contiguous prefix the relay can already serve. A cursor proves what precedes it
    /// exists, so the head moves up to it: a stream started past this relay's history, as one for a sibling's subscribers
    /// is, delivers nothing from before the cursor, and a subscriber behind it must see it is behind to fetch the rest.
    fn resume_from(&self, key: &Key, from: u64) -> u64 {
        let mut ranges = self
            .0
            .replay
            .as_ref()
            .map(|r| r.ranges(&key.0, &key.1))
            .unwrap_or_default();
        let recent = self.0.recent.lock().unwrap();
        let mut span: Option<(u64, u64)> = None;
        for ((_, sequence), _) in recent
            .records
            .range((key.clone(), from)..)
            .take_while(|((scope, _), _)| scope == key)
        {
            match span.as_mut() {
                Some((_, last)) if last.checked_add(1) == Some(*sequence) => *last = *sequence,
                _ => {
                    if let Some(span) = span.take() {
                        ranges.push(span)
                    }
                    span = Some((*sequence, *sequence));
                }
            }
        }
        if let Some(span) = span {
            ranges.push(span)
        }
        drop(recent);
        ranges.sort_unstable();
        let mut next = from;
        for (first, last) in ranges {
            if first > next {
                break;
            }
            if last >= next {
                next = last.saturating_add(1);
            }
        }
        if next > 1 {
            self.observe_head(key, next - 1);
        }
        next
    }
    fn remove(&self, id: u64) {
        let removed = self.0.subscribers.lock().unwrap().remove(&id);
        if let Some(sub) = removed {
            sub.closed.update(|v| *v = true);
            sub.changed.update(|v| *v = v.wrapping_add(1));
            for interest in sub.follows.values() {
                interest.woken.update(|v| *v = v.wrapping_add(1));
            }
            if let Some(mut member) = sub.member {
                self.0.stats.disconnected(&member);
                if member.status == pb::member::Status::Draining as i32 {
                    member.status = pb::member::Status::Left as i32;
                    self.0.departed.lock().unwrap().insert(
                        crate::membership::key(&member),
                        (member, crate::monotonic_now()),
                    );
                }
                self.members_changed();
            }
        }
        self.changed();
    }
    /// The first member must arrive on a leaf Hello; later ones may change only its status, unhealthy datasets and range
    /// sizes.
    fn member(&self, id: u64, member: pb::Member) -> bool {
        if crate::membership::validate(&member).is_err() {
            return false;
        }
        let mut subscribers = self.0.subscribers.lock().unwrap();
        let Some(sub) = subscribers.get_mut(&id) else {
            return false;
        };
        let Some(current) = sub.member.as_mut().filter(|current| {
            crate::membership::key(current) == crate::membership::key(&member)
                && member
                    .unhealthy
                    .iter()
                    .all(|d| current.datasets.contains(d))
        }) else {
            return false;
        };
        current.status = member.status;
        current.unhealthy = member.unhealthy;
        current.sizes = member.sizes;
        drop(subscribers);
        self.members_changed();
        true
    }
    /// Immediate send for control frames: a full queue disconnects the subscriber.
    fn send(&self, id: u64, kind: pb::relay_frame::Kind) -> bool {
        match self.enqueue(id, pb::RelayFrame { kind: Some(kind) }, None) {
            Ok(sent) => sent,
            Err(_) => {
                self.drop_full(id);
                false
            }
        }
    }
    /// Waits for queue budget; a subscriber that drains nothing for IDLE is disconnected.
    async fn send_scoped(
        &self,
        id: u64,
        key: &StreamKey,
        generation: u64,
        mut kind: pb::relay_frame::Kind,
    ) -> bool {
        let Some((changes, follow)) = self.0.subscribers.lock().unwrap().get(&id).map(|s| {
            let follow = s.follows.get(key).map_or(0, |interest| interest.follow);
            (s.changed.clone(), follow)
        }) else {
            return false;
        };
        match &mut kind {
            pb::relay_frame::Kind::Delivery(d) => d.follow = follow,
            pb::relay_frame::Kind::Bootstrap(b) => b.follow = follow,
            _ => {}
        }
        let mut frame = pb::RelayFrame { kind: Some(kind) };
        let queued = tokio::time::timeout(IDLE, async move {
            loop {
                let version = changes.get();
                match self.enqueue(id, frame, Some((key, generation))) {
                    Ok(sent) => return sent,
                    Err(rejected) => frame = *rejected,
                }
                changes.wait_until(|v| *v != version).await;
            }
        })
        .await;
        queued.unwrap_or_else(|_| {
            self.drop_full(id);
            false
        })
    }
    /// `Ok(false)`: subscriber or scope is gone. `Err`: the frame does not fit the queue budget yet.
    fn enqueue(
        &self,
        id: u64,
        frame: pb::RelayFrame,
        scope: Option<(&StreamKey, u64)>,
    ) -> Result<bool, Box<pb::RelayFrame>> {
        let len = frame.encoded_len();
        let events = queued_events(&frame);
        let mut subscribers = self.0.subscribers.lock().unwrap();
        let Some(sub) = subscribers.get_mut(&id) else {
            return Ok(false);
        };
        if let Some((key, generation)) = scope
            && sub
                .follows
                .get(key)
                .is_none_or(|f| f.generation != generation)
        {
            return Ok(false);
        }
        if sub.bytes.load(Ordering::Relaxed).saturating_add(len) > MAX_BYTES
            || sub.events.load(Ordering::Relaxed).saturating_add(events) > MAX_EVENTS
        {
            return Err(Box::new(frame));
        }
        let position = match &frame.kind {
            Some(pb::relay_frame::Kind::Delivery(d)) => Some((d.through.saturating_add(1), None)),
            Some(pb::relay_frame::Kind::Bootstrap(b)) if b.end => {
                Some((b.boundary.saturating_add(1), Some(b.boundary)))
            }
            _ => None,
        };
        let confirming = match &frame.kind {
            Some(pb::relay_frame::Kind::Bootstrap(b)) if b.begin => Some(true),
            Some(pb::relay_frame::Kind::Range(r)) if r.recovered => Some(false),
            _ => None,
        };
        let delivery = match &frame.kind {
            Some(pb::relay_frame::Kind::Delivery(d)) => Some(d.dataset.clone()),
            _ => None,
        };
        sub.bytes.fetch_add(len, Ordering::Relaxed);
        sub.events.fetch_add(events, Ordering::Relaxed);
        match sub.tx.try_send(Ok(frame)) {
            Ok(()) => {
                if let Some(dataset) = delivery {
                    self.0.stats.sent(dataset, events, len);
                }
                // Published under the lock, so no acknowledgement can observe the frame first.
                if let Some((key, _)) = scope
                    && let Some(interest) = sub.follows.get_mut(key)
                {
                    if let Some((from, baseline)) = position {
                        interest.from = from;
                        if let Some(baseline) = baseline {
                            interest.baseline = baseline;
                            interest.bootstrap_acked = false;
                        }
                    }
                    if let Some(confirming) = confirming {
                        interest.confirming = confirming;
                        if confirming {
                            interest.target = None;
                        }
                    }
                }
                Ok(true)
            }
            Err(error) => {
                release(&sub.bytes, len);
                release(&sub.events, events);
                drop(subscribers);
                match error {
                    mpsc::error::TrySendError::Full(Ok(frame)) => Err(Box::new(frame)),
                    _ => {
                        self.remove(id);
                        Ok(false)
                    }
                }
            }
        }
    }
    fn current(&self, id: u64, key: &StreamKey, generation: u64) -> Option<(u64, u64, bool)> {
        self.0
            .subscribers
            .lock()
            .unwrap()
            .get(&id)?
            .follows
            .get(key)
            .filter(|f| f.generation == generation)
            .map(|f| (f.from, f.acked, f.bootstrap_acked))
    }
    /// Where a recovery asks its next source to start, and whether that is a full recovery. A bootstrap sent but not yet
    /// confirmed makes it one: only a full recovery ends in the confirmation the subscriber is waiting for.
    fn recovery_start(
        &self,
        id: u64,
        key: &StreamKey,
        generation: u64,
        force: bool,
    ) -> Option<(u64, bool)> {
        let subscribers = self.0.subscribers.lock().unwrap();
        let interest = subscribers
            .get(&id)?
            .follows
            .get(key)
            .filter(|interest| interest.generation == generation)?;
        Some(if force || interest.confirming {
            (0, true)
        } else {
            (interest.from, false)
        })
    }
    fn advance(&self, id: u64, key: &StreamKey, generation: u64, through: u64) {
        if let Some(interest) = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&id)
            .and_then(|sub| sub.follows.get_mut(key))
            .filter(|f| f.generation == generation)
        {
            let next = through.saturating_add(1);
            assert!(next >= interest.from, "a subscription moved backwards");
            interest.from = next;
        }
    }
    fn baseline(&self, id: u64, key: &StreamKey, generation: u64, boundary: u64) {
        if let Some(interest) = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&id)
            .and_then(|s| s.follows.get_mut(key))
            .filter(|i| i.generation == generation)
        {
            interest.baseline = boundary;
        }
    }
    fn outstanding(&self, id: u64, key: &StreamKey) -> u64 {
        self.0
            .subscribers
            .lock()
            .unwrap()
            .get(&id)
            .and_then(|s| s.follows.get(key))
            .map_or(0, |i| {
                i.from
                    .saturating_sub(i.acked.max(i.baseline).saturating_add(1))
            })
    }
    fn error(&self, id: u64, key: &StreamKey, error: Status) {
        self.send(
            id,
            pb::relay_frame::Kind::Error(pb::RelayError {
                dataset: key.0.0.clone(),
                namespace: key.0.1.clone(),
                code: error.code() as u32,
                detail: error.message().into(),
                span: span(key.1),
            }),
        );
    }
    fn acknowledge(&self, id: u64, ack: pb::Ack) {
        if let Some(sub) = self.0.subscribers.lock().unwrap().get_mut(&id) {
            let key = ((ack.dataset, ack.namespace), range(ack.span.as_ref()));
            if let Some(interest) = sub.follows.get_mut(&key) {
                if ack.applied < interest.from {
                    interest.acked = interest.acked.max(ack.applied);
                    if interest.confirming && ack.applied >= interest.baseline {
                        interest.bootstrap_acked = true;
                    }
                }
                interest.woken.update(|v| *v = v.wrapping_add(1));
            }
        }
    }
    /// An empty namespace with a range unfollows that range in every namespace of the dataset.
    fn unfollow(&self, id: u64, unfollow: pb::Unfollow) {
        let range = range(unfollow.span.as_ref());
        if let Some(sub) = self.0.subscribers.lock().unwrap().get_mut(&id) {
            match range.filter(|_| unfollow.namespace.is_empty()) {
                Some(range) => {
                    sub.ranges.remove(&(unfollow.dataset.clone(), range));
                    sub.follows.retain(|((dataset, _), r), interest| {
                        let kept = *dataset != unfollow.dataset || *r != Some(range);
                        if !kept {
                            interest.woken.update(|v| *v = v.wrapping_add(1));
                        }
                        kept
                    });
                }
                None => {
                    if let Some(interest) = sub
                        .follows
                        .remove(&((unfollow.dataset, unfollow.namespace), range))
                    {
                        interest.woken.update(|v| *v = v.wrapping_add(1));
                    }
                }
            }
            sub.changed.update(|v| *v = v.wrapping_add(1));
        }
        self.changed();
    }
    fn encoded(&self, event: Event) -> EncodedEvent {
        let key = (
            (event.dataset.clone(), event.namespace.clone()),
            event.sequence,
        );
        let mut recent = self.0.recent.lock().unwrap();
        if let Some(bytes) = recent.records.get(&key) {
            return bytes.clone();
        }
        let hash = hash_of(&event.dataset, &event.namespace, &event.key);
        let bytes = EncodedEvent {
            bytes: Bytes::from(pb::Event::from(event).encode_to_vec()),
            hash,
        };
        recent.bytes += bytes.bytes.len();
        recent.records.insert(key.clone(), bytes.clone());
        recent.order.push_back(key);
        while recent.records.len() > 4096 || recent.bytes > MAX_BYTES {
            if let Some(key) = recent.order.pop_front()
                && let Some(bytes) = recent.records.remove(&key)
            {
                recent.bytes -= bytes.bytes.len();
            }
        }
        bytes
    }
    fn publish(&self, event: Event) {
        let key = (event.dataset.clone(), event.namespace.clone());
        let sequence = event.sequence;
        if self.0.replay.is_some() {
            let now = self
                .0
                .clock
                .lock()
                .unwrap()
                .map_or(event.committed_at, |clock| clock());
            // Replay is disposable. A full persistence queue drops cache entries, never live traffic.
            let size = 128
                + event.dataset.len()
                + event.namespace.len()
                + event.stream.len()
                + event.key.len()
                + match &event.operation {
                    crate::replication::Operation::Set(value) => value.len(),
                    crate::replication::Operation::Delete => 0,
                };
            let mut queued = self.0.replay_bytes.load(Ordering::Relaxed);
            let mut reserved = false;
            while let Some(next) = queued.checked_add(size).filter(|next| *next <= MAX_BYTES) {
                match self.0.replay_bytes.compare_exchange_weak(
                    queued,
                    next,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        reserved = true;
                        break;
                    }
                    Err(current) => queued = current,
                }
            }
            if reserved
                && self
                    .0
                    .replay_queue
                    .try_send((event.clone(), now, size))
                    .is_err()
            {
                release(&self.0.replay_bytes, size);
            }
        }
        let committed_at = event.committed_at;
        let bytes = self.encoded(event).bytes.len();
        let now = self.0.clock.lock().unwrap().map(|clock| clock());
        self.0
            .stats
            .received(&key, sequence, committed_at, bytes, now);
        self.observe_head(&key, sequence);
    }
    async fn read(&self, key: &Key, from: u64) -> Result<Vec<Encoded>, Status> {
        if let Some(control) = self.local_control(key)? {
            return control.read(&key.1, from, 256).map(|events| {
                events
                    .into_iter()
                    .map(|e| {
                        let hash = hash_of(&e.dataset, &e.namespace, &e.key);
                        (
                            e.sequence,
                            EncodedEvent {
                                bytes: Bytes::from(pb::Event::from(e).encode_to_vec()),
                                hash,
                            },
                        )
                    })
                    .collect()
            });
        }
        if let Some(writer) = &self.0.writer {
            return writer
                .log()
                .read(&key.0, &key.1, from, 256)
                .await
                .map(|events| {
                    events
                        .into_iter()
                        .map(|e| (e.sequence, self.encoded(e)))
                        .collect()
                });
        }
        let recent: Vec<_> = self
            .0
            .recent
            .lock()
            .unwrap()
            .records
            .range((key.clone(), from)..)
            .take_while(|((k, _), _)| k == key)
            .take(256)
            .map(|((_, s), b)| (*s, b.clone()))
            .collect();
        if recent.first().is_some_and(|(s, _)| *s == from) {
            let mut next = from;
            return Ok(recent
                .into_iter()
                .take_while(|(s, _)| {
                    let ok = *s == next;
                    next = next.saturating_add(1);
                    ok
                })
                .collect());
        }
        if let Some(replay) = &self.0.replay {
            if let Some(clock) = *self.0.clock.lock().unwrap() {
                let _ = replay.prune(clock());
            }
            if let Ok(events) = replay.read(&key.0, &key.1, from, 256)
                && !events.is_empty()
            {
                return Ok(events
                    .into_iter()
                    .map(|e| (e.sequence, self.encoded(e)))
                    .collect());
            }
        }
        Err(Status::out_of_range("local replay does not cover cursor"))
    }
    async fn follow(&self, id: u64, follow: pb::Follow, visited: Vec<String>, force: bool) {
        let key = (
            (follow.dataset.clone(), follow.namespace.clone()),
            range(follow.span.as_ref()),
        );
        if !selectable(&follow) {
            self.error(
                id,
                &key,
                Status::invalid_argument("invalid pushed subscription scope"),
            );
            return;
        }
        if let Err(error) = self.local_control(&key.0) {
            self.error(id, &key, error);
            return;
        }
        match key.1.filter(|_| follow.namespace.is_empty()) {
            Some(range) => self.follow_range(id, follow, range, visited),
            None => self.follow_stream(id, key, follow.from, follow.follow, visited, force),
        }
    }

    /// The namespaces of `dataset`, fixed in code ([`crate::placement::namespaces`]).
    fn namespaces(&self, dataset: &str) -> Vec<String> {
        crate::placement::namespaces(dataset)
            .iter()
            .map(|namespace| namespace.to_string())
            .collect()
    }

    /// Follows `range` in every namespace of the dataset.
    // ponytail: one stream task per subscriber, namespace and range; drive idle namespaces from one task per range if
    // namespace counts grow.
    fn follow_range(&self, id: u64, follow: pb::Follow, range: (u64, u64), visited: Vec<String>) {
        let dataset = follow.dataset.clone();
        match self.0.subscribers.lock().unwrap().get_mut(&id) {
            Some(sub) => sub.ranges.insert((dataset.clone(), range)),
            None => return,
        };
        // The upstream loop follows the dataset as soon as one subscriber follows it by range.
        self.changed();
        for namespace in self.namespaces(&dataset) {
            // 0 asks for a fresh baseline; a namespace the subscriber has not seen starts at 1.
            let from = follow.from_namespaces.get(&namespace).copied().unwrap_or(1);
            let key = ((dataset.clone(), namespace), Some(range));
            self.follow_stream(id, key, from, follow.follow, visited.clone(), from == 0);
        }
    }

    fn follow_stream(
        &self,
        id: u64,
        key: StreamKey,
        from: u64,
        follow: u64,
        visited: Vec<String>,
        force: bool,
    ) {
        // Seed the validated cache's lower bound before upstream traffic can arrive.
        self.source_head(&key.0);
        let generation = self.0.ids.fetch_add(1, Ordering::Relaxed);
        {
            let mut subscribers = self.0.subscribers.lock().unwrap();
            let Some(sub) = subscribers.get_mut(&id) else {
                return;
            };
            let replaced = sub.follows.insert(
                key.clone(),
                Interest {
                    from: from.max(1),
                    acked: from.max(1) - 1,
                    generation,
                    follow,
                    baseline: from.max(1) - 1,
                    confirming: false,
                    bootstrap_acked: false,
                    target: None,
                    woken: Arc::default(),
                },
            );
            if let Some(replaced) = replaced {
                replaced.woken.update(|v| *v = v.wrapping_add(1));
            }
            sub.changed.update(|v| *v = v.wrapping_add(1));
        }
        self.changed();
        let hub = self.clone();
        tokio::spawn(async move {
            let result = hub.pump(id, key.clone(), generation, visited, force).await;
            if let Err(error) = result {
                hub.error(id, &key, error);
            }
            if let Some(sub) = hub.0.subscribers.lock().unwrap().get_mut(&id)
                && sub
                    .follows
                    .get(&key)
                    .is_some_and(|i| i.generation == generation)
            {
                sub.follows.remove(&key);
            }
            hub.changed();
        });
    }
    async fn pump(
        &self,
        id: u64,
        key: StreamKey,
        generation: u64,
        visited: Vec<String>,
        force: bool,
    ) -> Result<(), Status> {
        let control = self.local_control(&key.0)?.cloned();
        let control_changes = control.as_ref().map(|control| control.changed());
        let opened = crate::monotonic_now();
        let Some((changes, mut moved)) =
            self.0.subscribers.lock().unwrap().get(&id).and_then(|s| {
                // A stream followed long after the session opened is new work, not failover.
                let moved = s.moved.filter(|at| opened.duration_since(*at) < IDLE);
                let interest = s.follows.get(&key).filter(|i| i.generation == generation)?;
                Some((interest.woken.clone(), moved))
            })
        else {
            return Ok(());
        };
        let mut announced = false;
        let mut forced = force;
        let mut lease: Option<Arc<RecoveryLease>> = None;
        let mut local_recovery = false;
        let mut renewed_through = 0;
        let mut local_renewed_through = None;
        let mut local_deadline = crate::monotonic_now();
        let mut recovery_permit = None;
        // One timer for the whole stream: a new sleep on every pass leaves one behind per event, for its full length (B-61).
        let mut recheck = std::pin::pin!(tokio::time::sleep(PROGRESS));
        // Only what concerns this stream wakes it: its own interest (`changes`), its namespace's head or arrivals, and the
        // recheck. The hub-wide signal moves on every follow, unfollow and stream end, and waking every stream for each
        // costs the square of the stream count, which grows as ranges split (#66 / B-61).
        let arrival = self.arrival(&key.0);
        loop {
            let arrived = arrival.get();
            let control_version = control_changes.as_ref().map(|changed| changed.get());
            let local_version = changes.get();
            let Some((from, acked, bootstrap_acked)) = self.current(id, &key, generation) else {
                return Ok(());
            };
            let h = if let Some(control) = &control {
                Some(control.head(&key.0.1)?)
            } else {
                self.source_head(&key.0)
            };
            if let Some(at) = moved
                && h.is_some_and(|head| acked >= head)
            {
                self.0.stats.caught_up(&key.0.0, at);
                moved = None;
            }
            if !announced && let Some(head) = h {
                let ranges = if control.is_some() {
                    Vec::new()
                } else {
                    self.0
                        .replay
                        .as_ref()
                        .map(|r| r.ranges(&key.0.0, &key.0.1))
                        .unwrap_or_default()
                };
                let (first, last) = ranges.first().copied().unwrap_or((0, 0));
                if !self
                    .send_scoped(
                        id,
                        &key,
                        generation,
                        pb::relay_frame::Kind::Range(pb::ReplayRange {
                            dataset: key.0.0.clone(),
                            namespace: key.0.1.clone(),
                            first,
                            last,
                            head,
                            recovered: false,
                            span: span(key.1),
                        }),
                    )
                    .await
                {
                    return Ok(());
                }
                announced = true;
            }
            if local_recovery {
                // Like the writer's lease: acknowledgement progress renews the slot, silence for IDLE gives it up.
                if local_renewed_through.map_or(bootstrap_acked, |through| acked > through) {
                    local_renewed_through = Some(acked);
                    local_deadline = crate::monotonic_now() + IDLE;
                } else if crate::monotonic_now() >= local_deadline {
                    return Err(Status::deadline_exceeded(
                        "cell recovery acknowledgement stalled",
                    ));
                }
            }
            if local_recovery && h.is_some_and(|head| acked >= head) {
                if !self
                    .send_scoped(
                        id,
                        &key,
                        generation,
                        pb::relay_frame::Kind::Range(pb::ReplayRange {
                            dataset: key.0.0.clone(),
                            namespace: key.0.1.clone(),
                            head: h.unwrap_or(0),
                            recovered: true,
                            span: span(key.1),
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    return Ok(());
                }
                local_recovery = false;
                drop(recovery_permit.take());
            }
            if let Some(pin) = &lease {
                if !pin.valid() {
                    return Err(Status::deadline_exceeded(
                        "bootstrap recovery lease expired",
                    ));
                }
                if acked > renewed_through {
                    pin.renew();
                    renewed_through = acked;
                }
                if acked >= h.unwrap_or(u64::MAX) {
                    self.send_scoped(
                        id,
                        &key,
                        generation,
                        pb::relay_frame::Kind::Range(pb::ReplayRange {
                            dataset: key.0.0.clone(),
                            namespace: key.0.1.clone(),
                            first: 0,
                            last: 0,
                            head: h.unwrap_or(0),
                            recovered: true,
                            span: span(key.1),
                        }),
                    )
                    .await;
                    lease = None;
                    drop(recovery_permit.take());
                }
            }
            if self.outstanding(id, &key) >= MAX_EVENTS as u64 {
                let wait = async {
                    while self.outstanding(id, &key) >= MAX_EVENTS as u64 {
                        let version = changes.get();
                        if self.current(id, &key, generation).is_none() {
                            return;
                        }
                        if self.outstanding(id, &key) < MAX_EVENTS as u64 {
                            break;
                        }
                        changes.wait_until(|v| *v != version).await;
                    }
                };
                if tokio::time::timeout(IDLE, wait).await.is_err() {
                    self.remove(id);
                    return Ok(());
                }
                continue;
            }
            if forced || h.is_some_and(|head| from <= head) {
                let read = if forced {
                    Err(Status::out_of_range("fresh baseline requested"))
                } else if let (Some(writer), Some(pin)) = (&self.0.writer, &lease) {
                    writer
                        .recovery_events(&key.0.0, &key.0.1, from, pin)
                        .await
                        .map(|events| {
                            events
                                .into_iter()
                                .map(|e| (e.sequence, self.encoded(e)))
                                .collect()
                        })
                        .map_err(|e| Status::unavailable(e.to_string()))
                } else {
                    self.read(&key.0, from).await
                };
                if self.current(id, &key, generation).is_none() {
                    return Ok(());
                }
                match read {
                    Ok(mut events) if !events.is_empty() => {
                        events.truncate(
                            (MAX_EVENTS as u64).saturating_sub(self.outstanding(id, &key)) as usize,
                        );
                        let mut bytes = 128 + key.0.0.len() + key.0.1.len();
                        let fitting = events
                            .iter()
                            .take_while(|(_, event)| {
                                bytes = bytes.saturating_add(event.bytes.len() + 16);
                                bytes <= MAX_BYTES
                            })
                            .count();
                        events.truncate(fitting);
                        let Some(&(through, _)) = events.last() else {
                            return Err(Status::resource_exhausted(
                                "event cannot fit the bounded delivery frame",
                            ));
                        };
                        let selected = events
                            .into_iter()
                            .filter_map(|(_, event)| {
                                selects(&key, event.hash).then_some(event.bytes)
                            })
                            .collect();
                        if !self
                            .send_scoped(
                                id,
                                &key,
                                generation,
                                pb::relay_frame::Kind::Delivery(pb::Delivery {
                                    dataset: key.0.0.clone(),
                                    namespace: key.0.1.clone(),
                                    from,
                                    through,
                                    events: selected,
                                    span: span(key.1),
                                    ..Default::default()
                                }),
                            )
                            .await
                        {
                            return Ok(());
                        }
                        self.advance(id, &key, generation, through);
                        continue;
                    }
                    Err(error) if error.code() == tonic::Code::OutOfRange => {
                        if let Some(control) = &control {
                            if recovery_permit.is_none() {
                                recovery_permit = Some(self.recovery_permit().await?);
                            }
                            let view = control.view(&key.0.1)?;
                            let boundary = view.sequence;
                            if !self.control_bootstrap(id, &key, generation, view).await? {
                                return Ok(());
                            }
                            self.advance(id, &key, generation, boundary);
                            self.baseline(id, &key, generation, boundary);
                            local_recovery = true;
                            local_renewed_through = None;
                            local_deadline = crate::monotonic_now() + IDLE;
                            forced = false;
                            continue;
                        }
                        if let Some(writer) = &self.0.writer {
                            recovery_permit = Some(self.recovery_permit().await?);
                            let (mut view, pin) =
                                writer
                                    .begin_recovery(&key.0.0, &key.0.1)
                                    .await
                                    .map_err(|e| Status::unavailable(e.to_string()))?;
                            let mut hash = client::bootstrap_digest(
                                &key.0.0,
                                &key.0.1,
                                view.sequence,
                                view.committed_at,
                            );
                            let base = pb::Bootstrap {
                                dataset: key.0.0.clone(),
                                namespace: key.0.1.clone(),
                                boundary: view.sequence,
                                committed_at: view.committed_at,
                                begin: false,
                                end: false,
                                entries: Vec::new(),
                                checksum: Vec::new(),
                                span: span(key.1),
                                ..Default::default()
                            };
                            if !self
                                .send_scoped(
                                    id,
                                    &key,
                                    generation,
                                    pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                                        begin: true,
                                        ..base.clone()
                                    }),
                                )
                                .await
                            {
                                return Ok(());
                            }
                            loop {
                                if self.current(id, &key, generation).is_none() {
                                    return Ok(());
                                }
                                if !pin.valid() {
                                    return Err(Status::deadline_exceeded(
                                        "bootstrap lease expired during transfer",
                                    ));
                                }
                                let page = tokio::time::timeout(IDLE, view.next_page())
                                    .await
                                    .map_err(|_| {
                                        Status::deadline_exceeded("bootstrap scan stalled")
                                    })?
                                    .map_err(|e| Status::unavailable(e.to_string()))?;
                                if !pin.valid() {
                                    return Err(Status::deadline_exceeded(
                                        "bootstrap lease expired during transfer",
                                    ));
                                }
                                if page.is_empty() {
                                    break;
                                }
                                let mut entries = Vec::new();
                                for (k, v) in page {
                                    if !selects(&key, hash_of(&key.0.0, &key.0.1, &k)) {
                                        continue;
                                    }
                                    for part in [&k, &v] {
                                        hash.update((part.len() as u64).to_be_bytes());
                                        hash.update(part);
                                    }
                                    entries.push(pb::StateEntry { key: k, value: v });
                                }
                                // Empty filtered pages still make scan progress visible downstream.
                                let chunk = pb::Bootstrap {
                                    entries,
                                    ..base.clone()
                                };
                                if chunk.encoded_len().saturating_add(16) > MAX_BYTES {
                                    return Err(Status::resource_exhausted(
                                        "state entry cannot fit bounded bootstrap frame",
                                    ));
                                }
                                if !self
                                    .send_scoped(
                                        id,
                                        &key,
                                        generation,
                                        pb::relay_frame::Kind::Bootstrap(chunk),
                                    )
                                    .await
                                {
                                    return Ok(());
                                }
                                pin.renew();
                                tokio::task::yield_now().await;
                            }
                            if !pin.valid() {
                                return Err(Status::deadline_exceeded(
                                    "bootstrap lease expired during transfer",
                                ));
                            }
                            if !self
                                .send_scoped(
                                    id,
                                    &key,
                                    generation,
                                    pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                                        end: true,
                                        checksum: hash.finalize().to_vec(),
                                        ..base
                                    }),
                                )
                                .await
                            {
                                return Ok(());
                            }
                            self.send_scoped(
                                id,
                                &key,
                                generation,
                                pb::relay_frame::Kind::Range(pb::ReplayRange {
                                    dataset: key.0.0.clone(),
                                    namespace: key.0.1.clone(),
                                    first: 0,
                                    last: 0,
                                    head: self.source_head(&key.0).unwrap_or(view.sequence),
                                    recovered: false,
                                    span: span(key.1),
                                }),
                            )
                            .await;
                            self.advance(id, &key, generation, view.sequence);
                            self.baseline(id, &key, generation, view.sequence);
                            lease = Some(pin);
                            forced = false;
                            continue;
                        }
                        self.proxy(id, &key, generation, &visited, forced).await?;
                        forced = false;
                        continue;
                    }
                    Err(error) => return Err(error),
                    _ => {}
                }
            }
            if let Some(changed) = &control_changes {
                tokio::select! {biased;
                    _=changes.wait_until(|v|*v!=local_version)=>{},
                    _=changed.wait_until(|v|Some(*v)!=control_version)=>{},
                    _=recheck.as_mut(), if local_recovery=>recheck.as_mut().reset(crate::monotonic_now()+PROGRESS),
                }
            } else if let Some(writer) = &self.0.writer {
                let head = writer.log().head(&key.0.0, &key.0.1);
                tokio::select! {biased;
                    _=changes.wait_until(|v|*v!=local_version)=>{},
                    _=head.wait_until(|v|Some(*v)!=h)=>{},
                    _=recheck.as_mut()=>recheck.as_mut().reset(crate::monotonic_now()+PROGRESS),
                }
            } else {
                tokio::select! {biased;
                    _=arrival.wait_until(|v|*v!=arrived)=>{},
                    _=changes.wait_until(|v|*v!=local_version)=>{},
                    _=recheck.as_mut()=>recheck.as_mut().reset(crate::monotonic_now()+PROGRESS),
                }
            }
        }
    }

    /// A coherent cell snapshot needs no writer recovery lease: a trimmed tail restarts here.
    async fn control_bootstrap(
        &self,
        id: u64,
        key: &StreamKey,
        generation: u64,
        view: crate::replication::materialized::View,
    ) -> Result<bool, Status> {
        let mut hash =
            client::bootstrap_digest(&key.0.0, &key.0.1, view.sequence, view.committed_at);
        let base = pb::Bootstrap {
            dataset: key.0.0.clone(),
            namespace: key.0.1.clone(),
            boundary: view.sequence,
            committed_at: view.committed_at,
            span: span(key.1),
            ..Default::default()
        };
        if !self
            .send_scoped(
                id,
                key,
                generation,
                pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                    begin: true,
                    ..base.clone()
                }),
            )
            .await
        {
            return Ok(false);
        }
        let mut entries = Vec::new();
        let mut bytes = base.encoded_len() + 128;
        for (k, v) in view.values {
            if !selects(key, hash_of(&key.0.0, &key.0.1, &k)) {
                continue;
            }
            let entry = pb::StateEntry { key: k, value: v };
            let size = entry.encoded_len() + 16;
            if base.encoded_len().saturating_add(128).saturating_add(size) > MAX_BYTES {
                return Err(Status::resource_exhausted(
                    "cell state entry cannot fit bounded bootstrap frame",
                ));
            }
            if !entries.is_empty()
                && (entries.len() >= 256 || bytes.saturating_add(size) > 64 * 1024)
            {
                if !self
                    .send_scoped(
                        id,
                        key,
                        generation,
                        pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                            entries: std::mem::take(&mut entries),
                            ..base.clone()
                        }),
                    )
                    .await
                {
                    return Ok(false);
                }
                bytes = base.encoded_len() + 128;
                tokio::task::yield_now().await;
            }
            for part in [&entry.key, &entry.value] {
                hash.update((part.len() as u64).to_be_bytes());
                hash.update(part);
            }
            bytes = bytes.saturating_add(size);
            entries.push(entry);
        }
        if !entries.is_empty()
            && !self
                .send_scoped(
                    id,
                    key,
                    generation,
                    pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                        entries,
                        ..base.clone()
                    }),
                )
                .await
        {
            return Ok(false);
        }
        Ok(self
            .send_scoped(
                id,
                key,
                generation,
                pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                    end: true,
                    checksum: hash.finalize().to_vec(),
                    ..base
                }),
            )
            .await)
    }

    /// A slot among this relay's concurrent recoveries. Waiting keeps them moving: a refusal costs the subscriber its next
    /// retry, however soon a slot frees.
    async fn recovery_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        let refused = || Status::resource_exhausted("relay recovery capacity exhausted");
        let permit = self.0.recoveries.clone().acquire_owned();
        tokio::time::timeout(RECOVERY_WAIT, permit)
            .await
            .map_err(|_| refused())?
            .map_err(|_| refused())
    }

    async fn proxy(
        &self,
        id: u64,
        key: &StreamKey,
        generation: u64,
        visited: &[String],
        force: bool,
    ) -> Result<(), Status> {
        // Only the relay a recovery entered takes a slot: relays forwarding to each other would otherwise wait on each other's.
        let _permit = if visited.is_empty() {
            Some(self.recovery_permit().await?)
        } else {
            None
        };
        if visited.contains(&self.0.id) || visited.len() >= 8 {
            return Err(Status::failed_precondition("recovery forwarding loop"));
        }
        let mut visited = visited.to_vec();
        visited.push(self.0.id.clone());
        let candidates: Vec<_> = self
            .0
            .replay_peers
            .iter()
            .chain(&self.0.upstreams)
            .cloned()
            .collect();
        let mut last = Status::unavailable("no recovery source");
        for peer in self.0.peers.health().route(&candidates) {
            let result = async {
                let (tx, rx) = mpsc::channel(16);
                tx.send(pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                        version: 1,
                        tier: 0,
                        member: None,
                        moved: false,
                    })),
                })
                .await
                .unwrap();
                let Some((from, force)) = self.recovery_start(id, key, generation, force) else {
                    return Ok(());
                };
                let request = pb::Follow {
                    dataset: key.0.0.clone(),
                    namespace: key.0.1.clone(),
                    from,
                    span: span(key.1),
                    from_namespaces: Default::default(),
                    ..Default::default()
                };
                let requested_from = request.from;
                tx.send(pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Recover(pb::Recover {
                        follow: Some(request),
                        visited: visited.clone(),
                    })),
                })
                .await
                .unwrap();
                let channel = self
                    .0
                    .peers
                    .channel(peer, Class::Control)
                    .map_err(|e| Status::unavailable(e.to_string()))?;
                let mut client = pb::relay_client::RelayClient::new(channel);
                let mut stream = self
                    .0
                    .peers
                    .call(
                        peer,
                        Class::Control,
                        Duration::from_secs(5),
                        client.session(ReceiverStream::new(rx)),
                    )
                    .await?
                    .into_inner();
                let welcome=tokio::time::timeout(IDLE,stream.message()).await
                    .map_err(|_|Status::deadline_exceeded("recovery handshake timeout"))??;
                if !matches!(welcome.and_then(|f|f.kind),Some(pb::relay_frame::Kind::Welcome(w)) if w.version==1 && w.tier>0 && w.tier<=4) {
                    return Err(Status::failed_precondition("invalid recovery handshake"));
                }
                let mut target = None;
                let mut recovering=force;
                let mut progress=tokio::time::interval(PROGRESS);
                loop {
                    if self.current(id, key, generation).is_none() {
                        return Ok(());
                    }
                    let frame={
                        let mut read=std::pin::pin!(tokio::time::timeout(IDLE,stream.message()));
                        loop {tokio::select!{biased;
                            result=&mut read=>break result.map_err(|_|Status::deadline_exceeded("recovery source idle"))??.ok_or_else(||Status::unavailable("recovery source closed"))?,
                            _=progress.tick()=>tx.send(pb::RelayRequest{kind:Some(pb::relay_request::Kind::Progress(true))}).await.map_err(|_|Status::unavailable("recovery requests closed"))?,
                        }}
                    };
                    match frame.kind {
                        Some(pb::relay_frame::Kind::Welcome(_)) => {
                            return Err(Status::failed_precondition("invalid recovery handshake"));
                        }
                        Some(pb::relay_frame::Kind::Range(range)) => {
                            if !force && range.head < requested_from {
                                return Err(Status::unavailable("recovery source is behind requested cursor"));
                            }
                            target = Some(range.head);
                            if range.recovered {
                                recovering=false;
                                if !self.send_scoped(id,key,generation,pb::relay_frame::Kind::Range(range.clone())).await{return Ok(())}
                            }
                            if !recovering && self
                                .current(id, key, generation)
                                .is_some_and(|(from, acked, _)| {
                                    from > range.head && acked >= range.head
                                })
                            {
                                return Ok(());
                            }
                        }
                        Some(pb::relay_frame::Kind::Delivery(ref d)) => {
                            let through = d.through;
                            if !self.send_scoped(id, key, generation, frame.kind.unwrap()).await {
                                return Ok(());
                            }
                            self.advance(id, key, generation, through);
                            tokio::time::timeout(IDLE, async {
                                loop {
                                    let Some((_, acked, _)) = self.current(id, key, generation)
                                    else {
                                        return Err(Status::cancelled("subscription closed"));
                                    };
                                    if acked >= through {
                                        return Ok(());
                                    }
                                    tokio::select!{
                                        _=tokio::time::sleep(Duration::from_millis(10))=>{},
                                        _=progress.tick()=>tx.send(pb::RelayRequest{kind:Some(pb::relay_request::Kind::Progress(true))}).await.map_err(|_|Status::unavailable("recovery requests closed"))?,
                                    }
                                }
                            })
                            .await
                            .map_err(|_| {
                                Status::deadline_exceeded("recovery acknowledgement stalled")
                            })??;
                            tx.send(pb::RelayRequest {
                                kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                                    dataset: key.0.0.clone(),
                                    namespace: key.0.1.clone(),
                                    applied: through,
                                    span: span(key.1),
                                })),
                            })
                            .await
                            .map_err(|_| Status::unavailable("recovery requests closed"))?;
                            if !recovering && target.is_some_and(|t| through >= t) {
                                return Ok(());
                            }
                        }
                        Some(pb::relay_frame::Kind::Bootstrap(ref b)) => {
                            recovering=true;
                            let end = b.end;
                            let boundary = b.boundary;
                            if !self.send_scoped(id, key, generation, frame.kind.unwrap()).await {
                                return Ok(());
                            }
                            if end {
                                self.advance(id, key, generation, boundary);
                                self.baseline(id,key,generation,boundary);
                                // Keep this connection (and its lease) until downstream install is acknowledged.
                                tokio::time::timeout(IDLE, async {
                                    loop {
                                        let Some((_, acked, _)) = self.current(id, key, generation)
                                        else {
                                            return Ok(());
                                        };
                                        if acked >= boundary {
                                            break;
                                        }
                                        tokio::select!{
                                            _=tokio::time::sleep(Duration::from_millis(10))=>{},
                                            _=progress.tick()=>tx.send(pb::RelayRequest{kind:Some(pb::relay_request::Kind::Progress(true))}).await.map_err(|_|Status::unavailable("recovery requests closed"))?,
                                        }
                                    }
                                    Ok::<(), Status>(())
                                })
                                .await
                                .map_err(|_| {
                                    Status::deadline_exceeded("bootstrap acknowledgement stalled")
                                })??;
                                tx.send(pb::RelayRequest {
                                    kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                                        dataset: key.0.0.clone(),
                                        namespace: key.0.1.clone(),
                                        applied: boundary,
                                        span: span(key.1),
                                    })),
                                })
                                .await
                                .map_err(|_| Status::unavailable("recovery requests closed"))?;
                            }
                        }
                        Some(pb::relay_frame::Kind::Error(e)) => {
                            return Err(Status::new(tonic::Code::from_i32(e.code as i32), e.detail));
                        }
                        _ => {}
                    }
                }
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    self.0.peers.health().trip(peer);
                    last = error;
                }
            }
        }
        Err(last)
    }

    /// What this relay's own subscribers need upstream: their lowest cursor per namespace, and the datasets some
    /// subscriber follows by range. The relay follows such a dataset whole, in every namespace, and never also by
    /// namespace.
    fn own(&self) -> Wanted {
        let mut namespaces = BTreeMap::new();
        let mut datasets = BTreeSet::new();
        for sub in self.0.subscribers.lock().unwrap().values() {
            datasets.extend(
                sub.ranges
                    .iter()
                    .filter(|(dataset, _)| self.external(dataset))
                    .map(|(dataset, _)| dataset.clone()),
            );
            for ((key, _), interest) in &sub.follows {
                if !self.external(&key.0) {
                    continue;
                }
                namespaces
                    .entry(key.clone())
                    .and_modify(|from: &mut u64| *from = (*from).min(interest.from))
                    .or_insert(interest.from);
            }
        }
        (namespaces, datasets)
    }

    /// What to follow upstream: [`Hub::own`] plus the interests siblings share, which behave like one more subscriber
    /// (#41 / B-12b). Such a stream starts after this relay's history, or at the sibling's cursor if that is later.
    // ponytail: rebuilds the union, like `own`, though the upstream loop calls it only when the hub-wide version moved.
    pub fn desired(&self) -> Wanted {
        let (mut namespaces, mut datasets) = self.own();
        let (followed, _) = self.derived(&namespaces, &datasets);
        for follow in followed {
            if !self.external(&follow.dataset) {
                continue;
            }
            let ranged = follow.namespace.is_empty();
            let scopes = if ranged {
                self.namespaces(&follow.dataset)
            } else {
                vec![follow.namespace.clone()]
            };
            for namespace in scopes {
                let cursor = if ranged {
                    follow.from_namespaces.get(&namespace).copied()
                } else {
                    Some(follow.from)
                };
                let key = (follow.dataset.clone(), namespace);
                let next = self.source_head(&key).unwrap_or(0).saturating_add(1);
                namespaces
                    .entry(key)
                    .or_insert(next.max(cursor.unwrap_or(1)));
            }
            if ranged {
                datasets.insert(follow.dataset);
            }
        }
        (namespaces, datasets)
    }
    fn idle(&self) -> bool {
        let (namespaces, datasets) = self.desired();
        namespaces.is_empty() && datasets.is_empty()
    }
    async fn upstream(&self) {
        let mut last = None;
        loop {
            if self.idle() {
                let version = self.0.changed.get();
                if self.idle() {
                    self.0.changed.wait_until(|v| *v != version).await;
                    continue;
                }
            }
            for peer in self.0.peers.health().route(&self.0.upstreams) {
                let result = self.upstream_session(peer, &mut last).await;
                self.0.stats.connected(None);
                if let Err(error) = result {
                    tracing::warn!(%peer,%error,"relay upstream failed");
                    self.0.peers.health().trip(peer);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    /// `last` is the upstream of the latest session that got through its handshake.
    async fn upstream_session(&self, peer: &str, last: &mut Option<String>) -> Result<(), Status> {
        let (tx, rx) = mpsc::channel(1024);
        tx.send(pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                tier: self.0.tier,
                member: None,
                moved: last.as_deref().is_some_and(|last| last != peer),
            })),
        })
        .await
        .unwrap();
        let channel = self
            .0
            .peers
            .channel(peer, Class::Control)
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client = pb::relay_client::RelayClient::new(channel);
        let mut stream = self
            .0
            .peers
            .call(
                peer,
                Class::Control,
                Duration::from_secs(5),
                client.session(ReceiverStream::new(rx)),
            )
            .await?
            .into_inner();
        let welcome = tokio::time::timeout(IDLE, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("upstream handshake timeout"))??;
        if !matches!(welcome.and_then(|f|f.kind),Some(pb::relay_frame::Kind::Welcome(w)) if w.version==1 && w.tier>self.0.tier && w.tier<=4)
        {
            return Err(Status::failed_precondition(
                "upstream must be a higher tier",
            ));
        }
        self.0.stats.connected(Some(peer));
        *last = Some(peer.to_owned());
        // Next expected revision per namespace, from namespace follows and whole-dataset follows alike.
        let mut sent = BTreeMap::<Key, u64>::new();
        let mut sent_datasets = BTreeSet::<String>::new();
        // The number of the follow each namespace is in. Frames of a follow this session ended can arrive after the next
        // one began, and are dropped (follows.tla).
        let mut follows = BTreeMap::<Key, u64>::new();
        let mut numbered = 0;
        let mut progress = tokio::time::interval(PROGRESS);
        let mut received = crate::monotonic_now();
        // Re-armed when it fires rather than per message, which would leave a timer behind for each one (B-61).
        let mut idle = std::pin::pin!(tokio::time::sleep_until(received + IDLE));
        // The hub-wide version the follows were last planned at. Everything `desired` reads moves it, so a frame that
        // left it alone needs no new plan: planning walks every subscriber's follows (#66 / B-61).
        let mut planned = None;
        loop {
            let version = self.0.changed.get();
            if planned != Some(version) {
                planned = Some(version);
                let (namespaces, datasets) = self.desired();
                let mut requests = Vec::new();
                for key in sent
                    .keys()
                    .filter(|key| {
                        !sent_datasets.contains(&key.0)
                            && (datasets.contains(&key.0) || !namespaces.contains_key(*key))
                    })
                    .cloned()
                    .collect::<Vec<_>>()
                {
                    requests.push(pb::relay_request::Kind::Unfollow(pb::Unfollow {
                        dataset: key.0.clone(),
                        namespace: key.1.clone(),
                        span: None,
                    }));
                    sent.remove(&key);
                    follows.remove(&key);
                }
                for dataset in sent_datasets
                    .difference(&datasets)
                    .cloned()
                    .collect::<Vec<_>>()
                {
                    requests.push(pb::relay_request::Kind::Unfollow(pb::Unfollow {
                        dataset: dataset.clone(),
                        namespace: String::new(),
                        span: span(Some(WHOLE)),
                    }));
                    sent.retain(|key, _| key.0 != dataset);
                    follows.retain(|key, _| key.0 != dataset);
                    sent_datasets.remove(&dataset);
                }
                for (key, from) in namespaces
                    .iter()
                    .filter(|(key, _)| !datasets.contains(&key.0))
                {
                    if !sent.contains_key(key) {
                        let from = self.resume_from(key, *from);
                        numbered += 1;
                        requests.push(pb::relay_request::Kind::Follow(pb::Follow {
                            dataset: key.0.clone(),
                            namespace: key.1.clone(),
                            from,
                            span: None,
                            from_namespaces: Default::default(),
                            follow: numbered,
                        }));
                        sent.insert(key.clone(), from);
                        follows.insert(key.clone(), numbered);
                    }
                }
                for dataset in &datasets {
                    if sent_datasets.insert(dataset.clone()) {
                        numbered += 1;
                        let mut from_namespaces = BTreeMap::new();
                        for namespace in self.namespaces(dataset) {
                            let key = (dataset.clone(), namespace);
                            let from = namespaces.get(&key).copied().unwrap_or_else(|| {
                                self.source_head(&key).unwrap_or(0).saturating_add(1)
                            });
                            let from = self.resume_from(&key, from);
                            sent.insert(key.clone(), from);
                            follows.insert(key.clone(), numbered);
                            from_namespaces.insert(key.1, from);
                        }
                        requests.push(pb::relay_request::Kind::Follow(pb::Follow {
                            dataset: dataset.clone(),
                            namespace: String::new(),
                            from: 0,
                            span: span(Some(WHOLE)),
                            from_namespaces,
                            follow: numbered,
                        }));
                    }
                }
                for kind in requests {
                    tx.send(pb::RelayRequest { kind: Some(kind) })
                        .await
                        .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                }
            }
            let message = tokio::select! {biased;
                _=self.0.changed.wait_until(|v|*v!=version)=>continue,
                _=idle.as_mut()=>{
                    if received+IDLE>crate::monotonic_now() {
                        idle.as_mut().reset(received+IDLE);
                        continue;
                    }
                    return Err(Status::deadline_exceeded("upstream idle"));
                },
                _=progress.tick()=>{
                    tx.send(pb::RelayRequest{kind:Some(pb::relay_request::Kind::Progress(true))}).await.map_err(|_|Status::unavailable("upstream requests closed"))?;
                    continue;
                },
                message=self.0.peers.call(peer,Class::Control,IDLE,stream.message())=>message?,
            };
            let Some(frame) = message else {
                return Err(Status::unavailable("upstream closed"));
            };
            received = crate::monotonic_now();
            match frame.kind {
                Some(pb::relay_frame::Kind::Range(range)) => {
                    let key = (range.dataset, range.namespace);
                    self.0.stats.upstream_head(&key, range.head);
                    self.observe_head(&key, range.head);
                    self.arrived(&key);
                }
                Some(pb::relay_frame::Kind::Delivery(d)) => {
                    let key = (d.dataset.clone(), d.namespace.clone());
                    if follows.get(&key) != Some(&d.follow) {
                        continue;
                    }
                    if sent.get(&key) != Some(&d.from) {
                        return Err(Status::data_loss("upstream coverage gap"));
                    }
                    let mut next = d.from;
                    for bytes in d.events {
                        let event: Event = pb::Event::decode(bytes)
                            .map_err(|e| Status::data_loss(e.to_string()))?
                            .into();
                        if event.dataset != key.0
                            || event.namespace != key.1
                            || event.sequence != next
                        {
                            return Err(Status::data_loss("upstream event gap/scope"));
                        }
                        next = next
                            .checked_add(1)
                            .ok_or_else(|| Status::out_of_range("sequence overflow"))?;
                        self.publish(event);
                    }
                    if next != d.through.saturating_add(1) {
                        return Err(Status::data_loss("upstream incomplete coverage"));
                    }
                    self.arrived(&key);
                    sent.insert(key, d.through.saturating_add(1));
                    tx.send(pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                            dataset: d.dataset,
                            namespace: d.namespace,
                            applied: d.through,
                            span: d.span,
                        })),
                    })
                    .await
                    .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                }
                Some(pb::relay_frame::Kind::Bootstrap(b)) if b.end => {
                    let key = (b.dataset.clone(), b.namespace.clone());
                    if follows.get(&key) != Some(&b.follow) {
                        continue;
                    }
                    self.observe_head(&key, b.boundary);
                    self.arrived(&key);
                    sent.insert(key, b.boundary.saturating_add(1));
                    tx.send(pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                            dataset: b.dataset,
                            namespace: b.namespace,
                            applied: b.boundary,
                            span: b.span,
                        })),
                    })
                    .await
                    .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                }
                Some(pb::relay_frame::Kind::Error(e)) => {
                    return Err(Status::new(tonic::Code::from_i32(e.code as i32), e.detail));
                }
                _ => {}
            }
        }
    }
}

/// Who may open a session with tier `tier` to a relay of tier `own_tier` (B-55): relays reach the writer and upper
/// tiers, and any relay as a leaf without a member; Quicksilver, KV and CDN processes are leaves of their own cell's
/// relays, and a leaf's announced member must be the caller itself.
pub fn authorize(
    caller: &Caller,
    own_tier: u32,
    own_cell: &str,
    tier: u32,
    member: Option<&pb::Member>,
) -> Result<(), Status> {
    // A relay also reads published views through its upstreams as a leaf without a member (B-12).
    if own_tier == WRITER || tier > 0 || (caller.service == "relay" && member.is_none()) {
        return caller.require(&["relay"], None);
    }
    if own_cell.is_empty() {
        return Err(Status::permission_denied(
            "this relay has no cell for leaves",
        ));
    }
    caller.require(&crate::membership::SERVICES, Some(own_cell))?;
    match member {
        Some(m)
            if (m.service.as_str(), m.cell.as_str(), m.node_id.as_str())
                != (
                    caller.service.as_str(),
                    caller.cell.as_str(),
                    caller.node_id.as_str(),
                )
                || !caller.has_host(host_of(&m.address)) =>
        {
            Err(Status::permission_denied(
                "announced member does not match the caller's certificate",
            ))
        }
        _ => Ok(()),
    }
}

#[tonic::async_trait]
impl pb::relay_server::Relay for Hub {
    async fn routes(
        &self,
        request: Request<pb::RoutesRequest>,
    ) -> Result<Response<pb::RoutesReply>, Status> {
        if let Some(caller) = Caller::of(&request) {
            caller.require(&crate::membership::SERVICES, Some(self.cell()))?;
        }
        let Some(control) = self.0.control.get() else {
            return Err(Status::unavailable("only a cell relay serves routes"));
        };
        let service = &request.get_ref().service;
        if !crate::membership::SERVICES.contains(&service.as_str()) {
            return Err(Status::invalid_argument("unknown service"));
        }
        let namespace = crate::membership::namespace(service, self.cell());
        control.validate_namespace(self.cell(), &namespace)?;
        let view = control.view(&namespace)?;
        Ok(Response::new(pb::RoutesReply {
            revision: view.sequence,
            entries: view
                .values
                .into_iter()
                .map(|(key, value)| pb::StateEntry { key, value })
                .collect(),
        }))
    }

    type SessionStream = Frames;
    async fn session(
        &self,
        request: Request<Streaming<pb::RelayRequest>>,
    ) -> Result<Response<Frames>, Status> {
        let caller = Caller::of(&request).cloned();
        let mut requests = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(5), requests.message())
            .await
            .map_err(|_| Status::deadline_exceeded("hello timeout"))??;
        let (tier, member, moved) = match first.and_then(|r| r.kind) {
            Some(pb::relay_request::Kind::Hello(hello))
                if hello.version == 1
                    && hello.tier < self.0.tier
                    && hello.member.as_ref().is_none_or(|member| {
                        hello.tier == 0 && crate::membership::validate(member).is_ok()
                    }) =>
            {
                (hello.tier, hello.member, hello.moved)
            }
            _ => {
                return Err(Status::failed_precondition(
                    "v1 hello must identify a lower downstream tier; only a leaf may carry a valid member",
                ));
            }
        };
        if let Some(caller) = &caller {
            authorize(caller, self.0.tier, self.cell(), tier, member.as_ref())
                .inspect_err(|status| tracing::warn!(?caller, %status, "relay session rejected"))?;
        }
        let (tx, rx) = mpsc::channel(MAX_EVENTS);
        let bytes = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(Watched::new(false));
        let changed = Arc::new(Watched::new(0));
        let id = self.0.ids.fetch_add(1, Ordering::Relaxed);
        if let Some(member) = &member {
            self.0
                .departed
                .lock()
                .unwrap()
                .remove(&crate::membership::key(member));
        }
        self.0.subscribers.lock().unwrap().insert(
            id,
            Subscriber {
                tx,
                bytes: bytes.clone(),
                events: events.clone(),
                closed: closed.clone(),
                follows: BTreeMap::new(),
                ranges: BTreeSet::new(),
                changed: changed.clone(),
                member,
                pushed: BTreeMap::new(),
                ranges_acked: BTreeMap::new(),
                moved: moved.then(crate::monotonic_now),
            },
        );
        self.members_changed();
        self.send(
            id,
            pb::relay_frame::Kind::Welcome(pb::Welcome {
                version: 1,
                tier: self.0.tier,
            }),
        );
        let hub = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PROGRESS);
            tick.tick().await;
            let mut received = crate::monotonic_now();
            // Re-armed when it fires rather than per message, which would leave a timer behind for each one (B-61).
            let mut idle = std::pin::pin!(tokio::time::sleep_until(received + IDLE));
            loop {
                tokio::select! {biased;
                    _=closed.wait_until(|v|*v)=>break,
                    _=idle.as_mut()=>if received+IDLE>crate::monotonic_now() {idle.as_mut().reset(received+IDLE)} else {break},
                    // A full queue skips the tick; stalled readers are disconnected by scoped sends after IDLE.
                    _=tick.tick()=>if matches!(hub.enqueue(id,pb::RelayFrame{kind:Some(pb::relay_frame::Kind::Tick(true))},None),Ok(false)){break},
                    request=requests.message()=>match request {
                        Ok(Some(request))=>{received=crate::monotonic_now();match request.kind {
                            // Only a valid scope counts: a dataset becomes a metric label, so the caller must not pick it.
                            Some(pb::relay_request::Kind::Follow(f))=>{if selectable(&f){hub.0.stats.followed(&f.dataset)};hub.follow(id,f,Vec::new(),false).await},
                            Some(pb::relay_request::Kind::Recover(r))=>{
                                if tier!=0 {break}
                                if let Some(f)=r.follow {if selectable(&f){hub.0.stats.recovering(&f.dataset)};let force=f.from==0 && !f.namespace.is_empty();hub.follow(id,f,r.visited,force).await}
                            }
                            Some(pb::relay_request::Kind::Unfollow(f))=>{
                                hub.unfollow(id,f);
                            }
                            Some(pb::relay_request::Kind::Ack(a))=>{
                                hub.acknowledge(id,a);
                            }
                            Some(pb::relay_request::Kind::Progress(true))=>{},
                            Some(pb::relay_request::Kind::Member(m))=>if !hub.member(id,m){break},
                            Some(pb::relay_request::Kind::Ranges(a))=>hub.ranges_acked(id,a),
                            _=>break,
                        }},
                        _=>break,
                    }
                }
            }
            hub.remove(id);
        });
        Ok(Response::new(Frames {
            rx: ReceiverStream::new(rx),
            bytes,
            events,
            changed,
            owner: Some((self.clone(), id)),
        }))
    }
}

#[cfg(test)]
#[path = "../tests/unit/relays/server.rs"]
mod tests;
