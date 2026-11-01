//! Pushed-data relay sessions, bounded fan-out and disposable replay (#14/B-17, #16/B-18).
pub mod client;
pub mod replay;
pub mod siblings;
pub mod sources;

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use prost::{Message, bytes::Bytes};
use sha2::Digest;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::buckets::{COUNT, bucket_of};
use crate::replication::Event;
use crate::replication::log::{Log, Watched};
use crate::replication::pb;
use crate::replication::writer::{RecoveryLease, Writer};
use crate::transport::{Class, Peers};
use replay::Replay;

pub const PROGRESS: Duration = Duration::from_secs(10);
pub const IDLE: Duration = Duration::from_secs(30);
const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_EVENTS: usize = 1024;
type Key = (String, String);
type Encoded = (u64, EncodedEvent);
type ReplayItem = (Event, u64, usize);
#[derive(Clone)]
struct EncodedEvent {
    bytes: Bytes,
    bucket: u32,
}

/// Customer KV persists through the writer but has no pushed delivery contract.
pub fn pushed(dataset: &str) -> bool {
    matches!(
        dataset,
        "quicksilver" | "auth" | "cdn" | crate::membership::DATASET
    )
}

pub fn tier_rank(tier: crate::node::Tier) -> u32 {
    match tier {
        crate::node::Tier::Cell => 1,
        crate::node::Tier::Dc => 2,
        crate::node::Tier::Region => 3,
    }
}

#[derive(Clone)]
pub struct Hub(Arc<Inner>);

struct Inner {
    id: String,
    tier: u32,
    peers: Arc<Peers>,
    upstreams: Vec<String>,
    replay_peers: Vec<String>,
    writer: Option<Arc<Writer>>,
    replay: Option<Arc<Replay>>,
    subscribers: Mutex<BTreeMap<u64, Subscriber>>,
    heads: Mutex<BTreeMap<Key, Arc<Watched<Option<u64>>>>>,
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
}

/// `(service, node_id)`.
pub type MemberKey = (String, String);

/// A member connected to this relay and its acknowledged revisions, for range readiness (#22 / B-21).
#[derive(Debug, Clone, PartialEq)]
pub struct Connected {
    pub member: pb::Member,
    pub applied: BTreeMap<Key, u64>,
}

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
    follows: BTreeMap<Key, Interest>,
    changed: Arc<Watched<u64>>,
    member: Option<pb::Member>,
}
struct Interest {
    from: u64,
    acked: u64,
    generation: u64,
    baseline: u64,
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
impl Stream for Frames {
    type Item = Result<pb::RelayFrame, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = Pin::new(&mut self.rx).poll_next(cx);
        if let Poll::Ready(Some(Ok(ref frame))) = item {
            self.bytes.fetch_sub(frame.encoded_len(), Ordering::Relaxed);
            self.events
                .fetch_sub(queued_events(frame), Ordering::Relaxed);
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
        Self::new(id, 4, peers, Vec::new(), Vec::new(), Some(writer), None)
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
            peers,
            upstreams,
            replay_peers,
            writer,
            replay,
            subscribers: Mutex::default(),
            heads: Mutex::default(),
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
        }))
    }
    pub fn with_clock(self, clock: fn() -> u64) -> Self {
        *self.0.clock.lock().unwrap() = Some(clock);
        self
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
            self.0.replay_bytes.fetch_sub(size, Ordering::Relaxed);
            let mut events = vec![event];
            while events.len() < 256 {
                let Ok((event, at, size)) = pending.try_recv() else {
                    break;
                };
                self.0.replay_bytes.fetch_sub(size, Ordering::Relaxed);
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
    pub fn members(&self) -> BTreeMap<MemberKey, Connected> {
        let mut members = BTreeMap::new();
        let now = crate::monotonic_now();
        let mut departed = self.0.departed.lock().unwrap();
        let before = departed.len();
        departed.retain(|_, (_, at)| now.duration_since(*at) < IDLE);
        if departed.len() != before {
            self.members_changed();
        }
        for (key, (member, _)) in departed.iter() {
            members.insert(
                key.clone(),
                Connected {
                    member: member.clone(),
                    applied: BTreeMap::new(),
                },
            );
        }
        drop(departed);
        for sub in self.0.subscribers.lock().unwrap().values() {
            if let Some(member) = &sub.member {
                members.insert(
                    crate::membership::key(member),
                    Connected {
                        member: member.clone(),
                        applied: sub
                            .follows
                            .iter()
                            .map(|(key, interest)| (key.clone(), interest.acked))
                            .collect(),
                    },
                );
            }
        }
        members
    }
    fn head(&self, key: &Key) -> Arc<Watched<Option<u64>>> {
        let mut heads = self.0.heads.lock().unwrap();
        heads
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Watched::new(None)))
            .clone()
    }
    fn source_head(&self, key: &Key) -> Option<u64> {
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
    fn observe_head(&self, key: &Key, sequence: u64) {
        self.head(key)
            .update(|head| *head = Some(head.unwrap_or(0).max(sequence)));
    }
    /// Live subscriptions start after the contiguous prefix the relay can already serve.
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
        next
    }
    fn remove(&self, id: u64) {
        let removed = self.0.subscribers.lock().unwrap().remove(&id);
        if let Some(sub) = removed {
            sub.closed.update(|v| *v = true);
            sub.changed.update(|v| *v = v.wrapping_add(1));
            if let Some(mut member) = sub.member {
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
    /// The first member must arrive on a leaf Hello; later ones may change only its status.
    fn member(&self, id: u64, member: pb::Member) -> bool {
        if crate::membership::validate(&member).is_err() {
            return false;
        }
        let mut subscribers = self.0.subscribers.lock().unwrap();
        let Some(sub) = subscribers.get_mut(&id) else {
            return false;
        };
        let Some(current) = sub
            .member
            .as_mut()
            .filter(|current| crate::membership::key(current) == crate::membership::key(&member))
        else {
            return false;
        };
        current.status = member.status;
        drop(subscribers);
        self.members_changed();
        true
    }
    /// Immediate send for control frames: a full queue disconnects the subscriber.
    fn send(&self, id: u64, kind: pb::relay_frame::Kind) -> bool {
        match self.enqueue(id, pb::RelayFrame { kind: Some(kind) }, None) {
            Ok(sent) => sent,
            Err(_) => {
                self.remove(id);
                false
            }
        }
    }
    /// Waits for queue budget; a subscriber that drains nothing for IDLE is disconnected.
    async fn send_scoped(
        &self,
        id: u64,
        key: &Key,
        generation: u64,
        kind: pb::relay_frame::Kind,
    ) -> bool {
        let Some(changes) = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .get(&id)
            .map(|s| s.changed.clone())
        else {
            return false;
        };
        let mut frame = pb::RelayFrame { kind: Some(kind) };
        let queued = tokio::time::timeout(IDLE, async move {
            loop {
                let version = changes.get();
                match self.enqueue(id, frame, Some((key, generation))) {
                    Ok(sent) => return sent,
                    Err(rejected) => frame = rejected,
                }
                changes.wait_until(|v| *v != version).await;
            }
        })
        .await;
        queued.unwrap_or_else(|_| {
            self.remove(id);
            false
        })
    }
    /// `Ok(false)`: subscriber or scope is gone. `Err`: the frame does not fit the queue budget yet.
    fn enqueue(
        &self,
        id: u64,
        frame: pb::RelayFrame,
        scope: Option<(&Key, u64)>,
    ) -> Result<bool, pb::RelayFrame> {
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
            return Err(frame);
        }
        let position = match &frame.kind {
            Some(pb::relay_frame::Kind::Delivery(d)) => Some((d.through.saturating_add(1), None)),
            Some(pb::relay_frame::Kind::Bootstrap(b)) if b.end => {
                Some((b.boundary.saturating_add(1), Some(b.boundary)))
            }
            _ => None,
        };
        sub.bytes.fetch_add(len, Ordering::Relaxed);
        sub.events.fetch_add(events, Ordering::Relaxed);
        match sub.tx.try_send(Ok(frame)) {
            Ok(()) => {
                // Published under the lock, so no acknowledgement can observe the frame first.
                if let Some((key, _)) = scope
                    && let Some(interest) = sub.follows.get_mut(key)
                    && let Some((from, baseline)) = position
                {
                    interest.from = from;
                    if let Some(baseline) = baseline {
                        interest.baseline = baseline;
                    }
                }
                Ok(true)
            }
            Err(error) => {
                sub.bytes.fetch_sub(len, Ordering::Relaxed);
                sub.events.fetch_sub(events, Ordering::Relaxed);
                drop(subscribers);
                match error {
                    mpsc::error::TrySendError::Full(Ok(frame)) => Err(frame),
                    _ => {
                        self.remove(id);
                        Ok(false)
                    }
                }
            }
        }
    }
    fn current(&self, id: u64, key: &Key, generation: u64) -> Option<(u64, u64)> {
        self.0
            .subscribers
            .lock()
            .unwrap()
            .get(&id)?
            .follows
            .get(key)
            .filter(|f| f.generation == generation)
            .map(|f| (f.from, f.acked))
    }
    fn advance(&self, id: u64, key: &Key, generation: u64, through: u64) {
        if let Some(interest) = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .get_mut(&id)
            .and_then(|sub| sub.follows.get_mut(key))
            .filter(|f| f.generation == generation)
        {
            interest.from = through.saturating_add(1);
        }
    }
    fn baseline(&self, id: u64, key: &Key, generation: u64, boundary: u64) {
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
    fn outstanding(&self, id: u64, key: &Key) -> u64 {
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
    fn error(&self, id: u64, key: &Key, error: Status) {
        self.send(
            id,
            pb::relay_frame::Kind::Error(pb::RelayError {
                dataset: key.0.clone(),
                namespace: key.1.clone(),
                code: error.code() as u32,
                detail: error.message().into(),
            }),
        );
    }
    fn acknowledge(&self, id: u64, ack: pb::Ack) {
        if let Some(sub) = self.0.subscribers.lock().unwrap().get_mut(&id) {
            if let Some(interest) = sub.follows.get_mut(&(ack.dataset, ack.namespace))
                && ack.applied < interest.from
            {
                interest.acked = interest.acked.max(ack.applied);
            }
            sub.changed.update(|v| *v = v.wrapping_add(1));
        }
    }
    fn unfollow(&self, id: u64, key: &Key) {
        if let Some(sub) = self.0.subscribers.lock().unwrap().get_mut(&id) {
            sub.follows.remove(key);
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
        let bucket = bucket_of(&event.dataset, &event.namespace, &event.key);
        let bytes = EncodedEvent {
            bytes: Bytes::from(pb::Event::from(event).encode_to_vec()),
            bucket,
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
                self.0.replay_bytes.fetch_sub(size, Ordering::Relaxed);
            }
        }
        self.encoded(event);
        self.observe_head(&key, sequence);
    }
    async fn read(&self, key: &Key, from: u64) -> Result<Vec<Encoded>, Status> {
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
    async fn follow(&self, id: u64, mut follow: pb::Follow, visited: Vec<String>, force: bool) {
        let key = (follow.dataset.clone(), follow.namespace.clone());
        if !pushed(&key.0)
            || key.1.is_empty()
            || key.1.contains('\0')
            || follow.buckets.iter().any(|b| *b >= COUNT)
        {
            self.error(
                id,
                &key,
                Status::invalid_argument("invalid pushed subscription scope"),
            );
            return;
        }
        follow.buckets.sort_unstable();
        follow.buckets.dedup();
        // Seed the validated cache's lower bound before upstream traffic can arrive.
        self.source_head(&key);
        let generation = self.0.ids.fetch_add(1, Ordering::Relaxed);
        {
            let mut subscribers = self.0.subscribers.lock().unwrap();
            let Some(sub) = subscribers.get_mut(&id) else {
                return;
            };
            sub.follows.insert(
                key.clone(),
                Interest {
                    from: follow.from.max(1),
                    acked: follow.from.max(1) - 1,
                    generation,
                    baseline: follow.from.max(1) - 1,
                },
            );
            sub.changed.update(|v| *v = v.wrapping_add(1));
        }
        self.changed();
        let hub = self.clone();
        tokio::spawn(async move {
            let result = hub.pump(id, follow, generation, visited, force).await;
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
        follow: pb::Follow,
        generation: u64,
        visited: Vec<String>,
        force: bool,
    ) -> Result<(), Status> {
        let key = (follow.dataset.clone(), follow.namespace.clone());
        let Some(changes) = self
            .0
            .subscribers
            .lock()
            .unwrap()
            .get(&id)
            .map(|s| s.changed.clone())
        else {
            return Ok(());
        };
        let mut announced = false;
        let mut forced = force;
        let mut lease: Option<Arc<RecoveryLease>> = None;
        let mut renewed_through = 0;
        let mut recovery_permit = None;
        loop {
            let version = self.0.changed.get();
            let local_version = changes.get();
            let Some((from, acked)) = self.current(id, &key, generation) else {
                return Ok(());
            };
            let h = self.source_head(&key);
            if !announced && let Some(head) = h {
                let ranges = self
                    .0
                    .replay
                    .as_ref()
                    .map(|r| r.ranges(&key.0, &key.1))
                    .unwrap_or_default();
                let (first, last) = ranges.first().copied().unwrap_or((0, 0));
                if !self
                    .send_scoped(
                        id,
                        &key,
                        generation,
                        pb::relay_frame::Kind::Range(pb::ReplayRange {
                            dataset: key.0.clone(),
                            namespace: key.1.clone(),
                            first,
                            last,
                            head,
                            recovered: false,
                        }),
                    )
                    .await
                {
                    return Ok(());
                }
                announced = true;
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
                            dataset: key.0.clone(),
                            namespace: key.1.clone(),
                            first: 0,
                            last: 0,
                            head: h.unwrap_or(0),
                            recovered: true,
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
                        .recovery_events(&key.0, &key.1, from, pin)
                        .await
                        .map(|events| {
                            events
                                .into_iter()
                                .map(|e| (e.sequence, self.encoded(e)))
                                .collect()
                        })
                        .map_err(|e| Status::unavailable(e.to_string()))
                } else {
                    self.read(&key, from).await
                };
                if self.current(id, &key, generation).is_none() {
                    return Ok(());
                }
                match read {
                    Ok(mut events) if !events.is_empty() => {
                        events.truncate(
                            (MAX_EVENTS as u64).saturating_sub(self.outstanding(id, &key)) as usize,
                        );
                        let mut bytes = 128 + key.0.len() + key.1.len();
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
                                (follow.buckets.is_empty()
                                    || follow.buckets.binary_search(&event.bucket).is_ok())
                                .then_some(event.bytes)
                            })
                            .collect();
                        if !self
                            .send_scoped(
                                id,
                                &key,
                                generation,
                                pb::relay_frame::Kind::Delivery(pb::Delivery {
                                    dataset: key.0.clone(),
                                    namespace: key.1.clone(),
                                    from,
                                    through,
                                    events: selected,
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
                        if let Some(writer) = &self.0.writer {
                            recovery_permit =
                                Some(self.0.recoveries.clone().try_acquire_owned().map_err(
                                    |_| Status::resource_exhausted("recovery capacity exhausted"),
                                )?);
                            let (view, pin) = writer
                                .begin_recovery(&key.0, &key.1)
                                .await
                                .map_err(|e| Status::unavailable(e.to_string()))?;
                            let mut hash = client::bootstrap_digest(
                                &key.0,
                                &key.1,
                                view.sequence,
                                view.committed_at,
                            );
                            let base = pb::Bootstrap {
                                dataset: key.0.clone(),
                                namespace: key.1.clone(),
                                boundary: view.sequence,
                                committed_at: view.committed_at,
                                begin: false,
                                end: false,
                                entries: Vec::new(),
                                checksum: Vec::new(),
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
                            let mut entries = Vec::new();
                            let mut bytes = 0;
                            for (k, v) in view.values {
                                if !pin.valid() {
                                    return Err(Status::deadline_exceeded(
                                        "bootstrap lease expired during transfer",
                                    ));
                                }
                                if !follow.buckets.is_empty()
                                    && follow
                                        .buckets
                                        .binary_search(&bucket_of(&key.0, &key.1, &k))
                                        .is_err()
                                {
                                    continue;
                                }
                                for part in [&k, &v] {
                                    hash.update((part.len() as u64).to_be_bytes());
                                    hash.update(part);
                                }
                                bytes += k.len() + v.len();
                                entries.push(pb::StateEntry { key: k, value: v });
                                if entries.len() >= 256 || bytes >= 64 * 1024 {
                                    if !self
                                        .send_scoped(
                                            id,
                                            &key,
                                            generation,
                                            pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                                                entries: std::mem::take(&mut entries),
                                                ..base.clone()
                                            }),
                                        )
                                        .await
                                    {
                                        return Ok(());
                                    }
                                    pin.renew();
                                    bytes = 0;
                                    tokio::task::yield_now().await;
                                }
                            }
                            if !entries.is_empty()
                                && !self
                                    .send_scoped(
                                        id,
                                        &key,
                                        generation,
                                        pb::relay_frame::Kind::Bootstrap(pb::Bootstrap {
                                            entries,
                                            ..base.clone()
                                        }),
                                    )
                                    .await
                            {
                                return Ok(());
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
                                    dataset: key.0.clone(),
                                    namespace: key.1.clone(),
                                    first: 0,
                                    last: 0,
                                    head: self.source_head(&key).unwrap_or(view.sequence),
                                    recovered: false,
                                }),
                            )
                            .await;
                            self.advance(id, &key, generation, view.sequence);
                            self.baseline(id, &key, generation, view.sequence);
                            lease = Some(pin);
                            forced = false;
                            continue;
                        }
                        self.proxy(id, &follow, generation, &visited, forced)
                            .await?;
                        forced = false;
                        continue;
                    }
                    Err(error) => return Err(error),
                    _ => {}
                }
            }
            if let Some(writer) = &self.0.writer {
                let head = writer.log().head(&key.0, &key.1);
                tokio::select! {biased;
                    _=self.0.changed.wait_until(|v|*v!=version)=>{},
                    _=changes.wait_until(|v|*v!=local_version)=>{},
                    _=head.wait_until(|v|Some(*v)!=h)=>{},
                    _=tokio::time::sleep(PROGRESS)=>{},
                }
            } else {
                tokio::select! {biased;
                    _=self.0.changed.wait_until(|v|*v!=version)=>{},
                    _=changes.wait_until(|v|*v!=local_version)=>{},
                    _=tokio::time::sleep(PROGRESS)=>{},
                }
            }
        }
    }

    async fn proxy(
        &self,
        id: u64,
        follow: &pb::Follow,
        generation: u64,
        visited: &[String],
        force: bool,
    ) -> Result<(), Status> {
        let _permit = self
            .0
            .recoveries
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("relay recovery capacity exhausted"))?;
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
        let key = (follow.dataset.clone(), follow.namespace.clone());
        let mut last = Status::unavailable("no recovery source");
        for peer in self.0.peers.health().route(&candidates) {
            let result = async {
                let (tx, rx) = mpsc::channel(16);
                tx.send(pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                        version: 1,
                        tier: 0,
                        member: None,
                    })),
                })
                .await
                .unwrap();
                let mut request = follow.clone();
                request.from = if force {
                    0
                } else {
                    self.current(id, &key, generation)
                        .ok_or_else(|| Status::cancelled("subscription closed"))?
                        .0
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
                    if self.current(id, &key, generation).is_none() {
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
                                if !self.send_scoped(id,&key,generation,pb::relay_frame::Kind::Range(range.clone())).await{return Ok(())}
                            }
                            if !recovering && self
                                .current(id, &key, generation)
                                .is_some_and(|(from, acked)| {
                                    from > range.head && acked >= range.head
                                })
                            {
                                return Ok(());
                            }
                        }
                        Some(pb::relay_frame::Kind::Delivery(ref d)) => {
                            let through = d.through;
                            if !self.send_scoped(id, &key, generation, frame.kind.unwrap()).await {
                                return Ok(());
                            }
                            self.advance(id, &key, generation, through);
                            tokio::time::timeout(IDLE, async {
                                loop {
                                    let Some((_, acked)) = self.current(id, &key, generation)
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
                                    dataset: key.0.clone(),
                                    namespace: key.1.clone(),
                                    applied: through,
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
                            if !self.send_scoped(id, &key, generation, frame.kind.unwrap()).await {
                                return Ok(());
                            }
                            if end {
                                self.advance(id, &key, generation, boundary);
                                self.baseline(id,&key,generation,boundary);
                                // Keep this connection (and its lease) until downstream install is acknowledged.
                                tokio::time::timeout(IDLE, async {
                                    loop {
                                        let Some((_, acked)) = self.current(id, &key, generation)
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
                                        dataset: key.0.clone(),
                                        namespace: key.1.clone(),
                                        applied: boundary,
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

    fn desired(&self) -> BTreeMap<Key, u64> {
        let mut desired = BTreeMap::new();
        for sub in self.0.subscribers.lock().unwrap().values() {
            for (key, interest) in &sub.follows {
                desired
                    .entry(key.clone())
                    .and_modify(|from: &mut u64| *from = (*from).min(interest.from))
                    .or_insert(interest.from);
            }
        }
        desired
    }
    async fn upstream(&self) {
        loop {
            if self.desired().is_empty() {
                let version = self.0.changed.get();
                if self.desired().is_empty() {
                    self.0.changed.wait_until(|v| *v != version).await;
                    continue;
                }
            }
            for peer in self.0.peers.health().route(&self.0.upstreams) {
                if let Err(error) = self.upstream_session(peer).await {
                    tracing::warn!(%peer,%error,"relay upstream failed");
                    self.0.peers.health().trip(peer);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn upstream_session(&self, peer: &str) -> Result<(), Status> {
        let (tx, rx) = mpsc::channel(1024);
        tx.send(pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                tier: self.0.tier,
                member: None,
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
        let mut sent = BTreeMap::<Key, u64>::new();
        let mut progress = tokio::time::interval(PROGRESS);
        let mut received = crate::monotonic_now();
        loop {
            let version = self.0.changed.get();
            let desired = self.desired();
            for (key, from) in &desired {
                if !sent.contains_key(key) {
                    let from = self.resume_from(key, *from);
                    tx.send(pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Follow(pb::Follow {
                            dataset: key.0.clone(),
                            namespace: key.1.clone(),
                            from,
                            buckets: Vec::new(),
                        })),
                    })
                    .await
                    .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                    sent.insert(key.clone(), from);
                }
            }
            for key in sent
                .keys()
                .filter(|key| !desired.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>()
            {
                tx.send(pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Unfollow(pb::Unfollow {
                        dataset: key.0.clone(),
                        namespace: key.1.clone(),
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                sent.remove(&key);
            }
            let message = tokio::select! {biased;
                _=self.0.changed.wait_until(|v|*v!=version)=>continue,
                _=tokio::time::sleep_until(received+IDLE)=>return Err(Status::deadline_exceeded("upstream idle")),
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
                    self.observe_head(&(range.dataset, range.namespace), range.head);
                    self.changed();
                }
                Some(pb::relay_frame::Kind::Delivery(d)) => {
                    let key = (d.dataset.clone(), d.namespace.clone());
                    let expected = sent.get(&key).copied().unwrap_or(d.from);
                    if d.from != expected {
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
                    sent.insert(key, d.through.saturating_add(1));
                    self.changed();
                    tx.send(pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                            dataset: d.dataset,
                            namespace: d.namespace,
                            applied: d.through,
                        })),
                    })
                    .await
                    .map_err(|_| Status::unavailable("upstream request queue closed"))?;
                }
                Some(pb::relay_frame::Kind::Bootstrap(b)) if b.end => {
                    let key = (b.dataset.clone(), b.namespace.clone());
                    self.observe_head(&key, b.boundary);
                    self.changed();
                    sent.insert(key, b.boundary.saturating_add(1));
                    tx.send(pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                            dataset: b.dataset,
                            namespace: b.namespace,
                            applied: b.boundary,
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

#[tonic::async_trait]
impl pb::relay_server::Relay for Hub {
    type SessionStream = Frames;
    async fn session(
        &self,
        request: Request<Streaming<pb::RelayRequest>>,
    ) -> Result<Response<Frames>, Status> {
        let mut requests = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(5), requests.message())
            .await
            .map_err(|_| Status::deadline_exceeded("hello timeout"))??;
        let (tier, member) = match first.and_then(|r| r.kind) {
            Some(pb::relay_request::Kind::Hello(hello))
                if hello.version == 1
                    && hello.tier < self.0.tier
                    && hello.member.as_ref().is_none_or(|member| {
                        hello.tier == 0 && crate::membership::validate(member).is_ok()
                    }) =>
            {
                (hello.tier, hello.member)
            }
            _ => {
                return Err(Status::failed_precondition(
                    "v1 hello must identify a lower downstream tier; only a leaf may carry a valid member",
                ));
            }
        };
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
                changed: changed.clone(),
                member,
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
            loop {
                tokio::select! {biased;
                    _=closed.wait_until(|v|*v)=>break,
                    _=tokio::time::sleep_until(received+IDLE)=>break,
                    // A full queue skips the tick; stalled readers are disconnected by scoped sends after IDLE.
                    _=tick.tick()=>if matches!(hub.enqueue(id,pb::RelayFrame{kind:Some(pb::relay_frame::Kind::Tick(true))},None),Ok(false)){break},
                    request=requests.message()=>match request {
                        Ok(Some(request))=>{received=crate::monotonic_now();match request.kind {
                            Some(pb::relay_request::Kind::Follow(f))=>hub.follow(id,f,Vec::new(),false).await,
                            Some(pb::relay_request::Kind::Recover(r))=>{
                                if tier!=0 {break}
                                if let Some(f)=r.follow {let force=f.from==0;hub.follow(id,f,r.visited,force).await}
                            }
                            Some(pb::relay_request::Kind::Unfollow(f))=>{
                                hub.unfollow(id,&(f.dataset,f.namespace));
                            }
                            Some(pb::relay_request::Kind::Ack(a))=>{
                                hub.acknowledge(id,a);
                            }
                            Some(pb::relay_request::Kind::Progress(true))=>{},
                            Some(pb::relay_request::Kind::Member(m))=>if !hub.member(id,m){break},
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
