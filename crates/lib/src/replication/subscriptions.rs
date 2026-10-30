//! Subscriptions to `(dataset, namespace)` from a sequence, over `lib/transport` (B-10, `kv.md` §8, §13, §17).
//!
//! A subscription is a cursor over the source's ordered [`Log`]. Catch-up and live delivery are the same read from
//! the cursor, so there is no switch-over where a concurrent commit could be missed or sent twice.
//!
//! The subscriber's cursor is its applied sequence, which the [`Applier`] commits with the data. That makes
//! snapshot-then-subscribe exact: installed state (an engine `restore`, or B-11 materialized-state bootstrap) carries the
//! namespace's revision with its data, and the subscription starts right after it. A reconnect or a restart resumes
//! the same way.
//!
//! The subscriber acknowledges what it applied, and the source sends at most [`WINDOW`] events past the last
//! acknowledgement per namespace, so a slow subscriber holds back only itself. The source keeps nothing about a
//! subscriber beyond its connection. Relays (B-17) serve the same protocol from their replay log (B-18).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use super::{Applier, ApplyError, ApplyHook, Event, Operation, Outcome};
use crate::storage::StorageError;
use crate::transport::{Class, Peers};

pub mod pb {
    tonic::include_proto!("bedrock.replication");
}

use pb::replication_client::ReplicationClient;
use pb::replication_server::Replication;

/// Events a source sends per namespace past the subscriber's last acknowledgement.
pub const WINDOW: u64 = 1024;
/// Events per [`pb::Batch`].
const BATCH: u64 = 256;
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_millis(100);

/// A source's committed history: the writer's durable log (B-09) or a relay's replay log (B-18).
pub trait Log: Send + Sync + 'static {
    /// Up to `limit` committed events of the namespace from sequence `from`, consecutive and in order; empty when
    /// `from` is past the head.
    fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Event>, Status>> + Send;

    /// The namespace's head sequence (0 before its first commit), updated after every commit.
    fn head(&self, dataset: &str, namespace: &str) -> Arc<Watched<u64>>;
}

/// A value whose changes can be awaited. `tokio::sync::watch` spreads its waiters over shards picked by an RNG the
/// simulation does not seed, so a seed would not replay; this is a lock and a [`Notify`].
pub struct Watched<T> {
    value: Mutex<T>,
    changed: Notify,
}

impl<T: Clone> Watched<T> {
    pub fn new(value: T) -> Self {
        Watched {
            value: Mutex::new(value),
            changed: Notify::new(),
        }
    }

    pub fn get(&self) -> T {
        self.value.lock().unwrap().clone()
    }

    pub fn update(&self, f: impl FnOnce(&mut T)) {
        f(&mut self.value.lock().unwrap());
        self.changed.notify_waiters();
    }

    /// Waits until `ready` holds for the value, and returns it.
    pub async fn wait_until(&self, mut ready: impl FnMut(&T) -> bool) -> T {
        loop {
            let mut notified = std::pin::pin!(self.changed.notified());
            // Registered before the check, so an update right after it still wakes this waiter.
            notified.as_mut().enable();
            {
                let value = self.value.lock().unwrap();
                if ready(&value) {
                    return value.clone();
                }
            }
            notified.await;
        }
    }
}

/// A [`Log`] held in memory, e.g. the writer's recent tail.
#[derive(Default)]
pub struct MemoryLog {
    namespaces: Mutex<BTreeMap<(String, String), NamespaceLog>>,
    retention: Option<(fn() -> u64, u64)>,
}

struct NamespaceLog {
    events: Vec<Event>,
    base_sequence: u64,
    head: Arc<Watched<u64>>,
}

impl NamespaceLog {
    fn new() -> Self {
        NamespaceLog {
            events: Vec::new(),
            base_sequence: 0,
            head: Arc::new(Watched::new(0)),
        }
    }
}

impl MemoryLog {
    /// Enforces the writer's event window on reads, independently of physical GC.
    pub fn with_retention(clock: fn() -> u64, retention_ms: u64) -> Self {
        Self {
            namespaces: Mutex::default(),
            retention: Some((clock, retention_ms)),
        }
    }

    /// Initializes an empty log at an already-installed state boundary.
    pub fn reset(&self, dataset: &str, namespace: &str, boundary: u64) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let log = namespaces
            .entry((dataset.to_owned(), namespace.to_owned()))
            .or_insert_with(NamespaceLog::new);
        if !log.events.is_empty() || log.head.get() != 0 {
            return Err("cannot reset a non-empty log".into());
        }
        log.base_sequence = boundary;
        log.head.update(|head| *head = boundary);
        Ok(())
    }

    /// Drops expired durable events while preserving the namespace head.
    pub fn prune(&self, dataset: &str, namespace: &str, through: u64) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let Some(log) = namespaces.get_mut(&(dataset.to_owned(), namespace.to_owned())) else {
            return if through == 0 {
                Ok(())
            } else {
                Err("unknown namespace log".into())
            };
        };
        let head = log.head.get();
        if through > head {
            return Err(format!("cannot prune through {through} past head {head}"));
        }
        let drop_count = usize::try_from(through.saturating_sub(log.base_sequence))
            .unwrap_or(usize::MAX)
            .min(log.events.len());
        log.events.drain(..drop_count);
        log.base_sequence = log.base_sequence.max(through);
        Ok(())
    }

    /// Commits `event`, which must carry its namespace's next sequence.
    pub fn append(&self, event: Event) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let log = namespaces
            .entry((event.dataset.clone(), event.namespace.clone()))
            .or_insert_with(NamespaceLog::new);
        let next = log
            .head
            .get()
            .checked_add(1)
            .ok_or_else(|| "sequence overflow".to_owned())?;
        if event.sequence != next {
            return Err(format!("expected sequence {next}, got {}", event.sequence));
        }
        log.events.push(event);
        log.head.update(|head| *head = next);
        Ok(())
    }
}

impl Log for MemoryLog {
    async fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>, Status> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let Some(log) = namespaces.get_mut(&(dataset.to_owned(), namespace.to_owned())) else {
            return Ok(Vec::new());
        };
        if let Some((clock, window)) = self.retention {
            let now = clock();
            let expired = log
                .events
                .partition_point(|event| now.saturating_sub(event.committed_at) >= window);
            if expired > 0 {
                log.base_sequence = log.events[expired - 1].sequence;
                log.events.drain(..expired);
            }
        }
        let first = log.base_sequence.saturating_add(1);
        if from.max(1) <= log.base_sequence {
            return Err(Status::out_of_range(format!(
                "sequence {} is outside retained history; bootstrap at or after {}",
                from.max(1),
                log.base_sequence
            )));
        }
        let start = usize::try_from(from.max(first) - first).unwrap_or(usize::MAX);
        let events = log.events.get(start..).unwrap_or_default();
        Ok(events.iter().take(limit).cloned().collect())
    }

    fn head(&self, dataset: &str, namespace: &str) -> Arc<Watched<u64>> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let key = (dataset.to_owned(), namespace.to_owned());
        let log = namespaces.entry(key).or_insert_with(NamespaceLog::new);
        log.head.clone()
    }
}

/// Serves subscriptions from a [`Log`]. Add `pb::replication_server::ReplicationServer::new(Source::new(log))` to the
/// node's tonic server.
pub struct Source<L> {
    log: Arc<L>,
}

impl<L> Source<L> {
    pub fn new(log: Arc<L>) -> Self {
        Source { log }
    }
}

/// A subscriber's position in one namespace, as its requests set it.
#[derive(Clone, Copy, PartialEq)]
struct Cursor {
    /// Bumped by every `Subscribe`, which moves the read position back to `from`.
    generation: u64,
    from: u64,
    acked: u64,
    /// The subscriber went away.
    closed: bool,
}

impl Cursor {
    fn at(generation: u64, from: u64) -> Self {
        Cursor {
            generation,
            from,
            acked: from - 1,
            closed: false,
        }
    }
}

#[tonic::async_trait]
impl<L: Log> Replication for Source<L> {
    type SubscribeStream = ReceiverStream<Result<pb::Batch, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<pb::Request>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut requests = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let log = self.log.clone();
        tokio::spawn(async move {
            let mut cursors = BTreeMap::<(String, String), Arc<Watched<Cursor>>>::new();
            while let Ok(Some(request)) = requests.message().await {
                match request.kind {
                    Some(pb::request::Kind::Subscribe(s)) => {
                        let from = s.from.max(1);
                        let key = (s.dataset, s.namespace);
                        if let Some(cursor) = cursors.get(&key) {
                            cursor.update(|c| *c = Cursor::at(c.generation + 1, from));
                            continue;
                        }
                        let cursor = Arc::new(Watched::new(Cursor::at(0, from)));
                        let (dataset, namespace) = key.clone();
                        tokio::spawn(pump(
                            log.clone(),
                            dataset,
                            namespace,
                            cursor.clone(),
                            tx.clone(),
                        ));
                        cursors.insert(key, cursor);
                    }
                    Some(pb::request::Kind::Ack(a)) => {
                        if let Some(cursor) = cursors.get(&(a.dataset, a.namespace)) {
                            cursor.update(|c| c.acked = c.acked.max(a.applied));
                        }
                    }
                    None => {}
                }
            }
            for cursor in cursors.values() {
                cursor.update(|c| c.closed = true);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Sends one namespace's events from the cursor, up to the head and at most [`WINDOW`] past the acknowledgement.
async fn pump<L: Log>(
    log: Arc<L>,
    dataset: String,
    namespace: String,
    cursor: Arc<Watched<Cursor>>,
    tx: mpsc::Sender<Result<pb::Batch, Status>>,
) {
    let head = log.head(&dataset, &namespace);
    let (mut generation, mut next) = (None, 0);
    loop {
        let c = cursor.get();
        if c.closed {
            return;
        }
        if generation != Some(c.generation) {
            generation = Some(c.generation);
            next = c.from;
        }
        let h = head.get();
        let last = h
            .min(c.acked.saturating_add(WINDOW))
            .min(next.saturating_add(BATCH - 1));
        if next <= last {
            let limit = (last - next + 1) as usize;
            let events = match log.read(&dataset, &namespace, next, limit).await {
                Ok(events) => events,
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            };
            if let (Some(first), Some(end)) = (events.first(), events.last()) {
                if first.sequence != next {
                    let detail = format!("log read from {next} returned {}", first.sequence);
                    let _ = tx.send(Err(Status::data_loss(detail))).await;
                    return;
                }
                next = end.sequence + 1;
                let events = events.into_iter().map(pb::Event::from).collect();
                if tx.send(Ok(pb::Batch { events })).await.is_err() {
                    return;
                }
                continue;
            }
        }
        // A change since the reads above ends the wait at once. `biased`: the default random branch order draws from
        // tokio's RNG, which the simulation does not seed.
        tokio::select! {
            biased;
            _ = cursor.wait_until(|now| *now != c) => {}
            _ = head.wait_until(|&now| now != h) => {}
            () = tx.closed() => return,
        }
    }
}

/// Feeds one dataset's namespaces from an upstream into an [`Applier`] until dropped.
pub struct Subscription {
    applied: Arc<Applied>,
    recovery: Arc<Watched<Option<Status>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Subscription {
    /// Subscribes `applier`'s dataset to `namespaces` at `upstream` (`host:port`), each from the sequence after its
    /// applied one. After a failure it reconnects and resumes the same way.
    pub fn start<H: ApplyHook + Send + 'static>(
        peers: Arc<Peers>,
        upstream: String,
        applier: Applier<H>,
        namespaces: Vec<String>,
    ) -> Self {
        let applied = Arc::<Applied>::default();
        let recovery = Arc::new(Watched::new(None));
        let task = tokio::spawn(run(
            peers,
            upstream,
            applier,
            namespaces,
            applied.clone(),
            recovery.clone(),
        ));
        Subscription {
            applied,
            recovery,
            task,
        }
    }

    /// Waits until `namespace` has applied at least `revision`, for a minimum-revision read. Callers bound the
    /// wait with a timeout.
    pub async fn wait_for(&self, namespace: &str, revision: u64) {
        let applied = self.applied.get(namespace);
        applied.wait_until(|&a| a >= revision).await;
    }

    /// Resolves when the source has pruned a required cursor. Applying has stopped: callers stop
    /// this subscription, recover/install its namespaces, then start again from installed revisions.
    pub async fn recovery_required(&self) -> Status {
        self.recovery.wait_until(Option::is_some).await.unwrap()
    }

    /// Stops applying and waits for the task to finish before its applier can be replaced.
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Each namespace's applied sequence.
#[derive(Default)]
struct Applied(Mutex<BTreeMap<String, Arc<Watched<u64>>>>);

impl Applied {
    fn get(&self, namespace: &str) -> Arc<Watched<u64>> {
        let mut namespaces = self.0.lock().unwrap();
        let applied = namespaces.entry(namespace.to_owned());
        applied.or_insert_with(|| Arc::new(Watched::new(0))).clone()
    }

    fn set(&self, namespace: &str, sequence: u64) {
        self.get(namespace).update(|a| *a = (*a).max(sequence));
    }
}

enum Ended {
    Upstream(Status),
    Storage(StorageError),
}

async fn run<H: ApplyHook>(
    peers: Arc<Peers>,
    upstream: String,
    mut applier: Applier<H>,
    namespaces: Vec<String>,
    applied: Arc<Applied>,
    recovery: Arc<Watched<Option<Status>>>,
) {
    loop {
        match session(&peers, &upstream, &mut applier, &namespaces, &applied).await {
            Ended::Upstream(status) => {
                if status.code() == tonic::Code::OutOfRange {
                    tracing::warn!(upstream, %status, "subscription requires materialized-state bootstrap");
                    recovery.update(|reason| *reason = Some(status));
                    return;
                }
                tracing::warn!(upstream, %status, "subscription stream ended, reconnecting");
                peers.health().trip(&upstream);
            }
            Ended::Storage(error) => {
                tracing::warn!(upstream, %error, "applying failed, resubscribing");
            }
        }
        // ponytail: one upstream and a fixed delay; B-17 walks the upstream list through `Health::route`.
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn session<H: ApplyHook>(
    peers: &Peers,
    upstream: &str,
    applier: &mut Applier<H>,
    namespaces: &[String],
    applied: &Applied,
) -> Ended {
    let (requests, rx) = mpsc::unbounded_channel();
    for namespace in namespaces {
        let from = match applier.applied(namespace) {
            Ok(sequence) => sequence + 1,
            Err(error) => return Ended::Storage(error),
        };
        applied.set(namespace, from - 1);
        let _ = requests.send(subscribe(applier.dataset(), namespace, from));
    }

    let channel = match peers.channel(upstream, Class::Control) {
        Ok(channel) => channel,
        Err(error) => return Ended::Upstream(Status::unavailable(error.to_string())),
    };
    let mut client = ReplicationClient::new(channel);
    let open = client.subscribe(UnboundedReceiverStream::new(rx));
    let mut stream = match peers
        .call(upstream, Class::Control, OPEN_TIMEOUT, open)
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => return Ended::Upstream(status),
    };

    loop {
        // The stream idles between commits, so reads have no deadline; HTTP/2 keepalive ends it if the upstream dies.
        let batch = match stream.message().await {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ended::Upstream(Status::unavailable("stream ended")),
            Err(status) => return Ended::Upstream(status),
        };
        if let Err(error) = apply_batch(applier, applied, batch, &requests, upstream) {
            return Ended::Storage(error);
        }
    }
}

fn apply_batch<H: ApplyHook>(
    applier: &mut Applier<H>,
    applied: &Applied,
    batch: pb::Batch,
    requests: &mpsc::UnboundedSender<pb::Request>,
    upstream: &str,
) -> Result<(), StorageError> {
    let mut acks = BTreeMap::new();
    let mut gaps = BTreeMap::new();
    for event in batch.events {
        let namespace = event.namespace.clone();
        match applier.offer(event.into()) {
            Ok(Outcome::Applied(sequence)) => {
                applied.set(&namespace, sequence);
                gaps.remove(&namespace);
                acks.insert(namespace, sequence);
            }
            Ok(Outcome::Duplicate) => {}
            Ok(Outcome::Gap {
                applied: sequence,
                missing,
            }) => {
                applied.set(&namespace, sequence);
                acks.insert(namespace.clone(), sequence);
                gaps.insert(namespace, missing.start);
            }
            Err(ApplyError::Rejected(reason)) => {
                tracing::warn!(upstream, reason, "dropped a malformed event");
            }
            Err(ApplyError::Storage(error)) => return Err(error),
        }
    }
    // Retry only the gap left after the batch; a later batch can retry again if it persists.
    for (namespace, from) in gaps {
        let _ = requests.send(subscribe(applier.dataset(), &namespace, from));
    }
    for (namespace, sequence) in acks {
        let _ = requests.send(ack(applier.dataset(), &namespace, sequence));
    }
    Ok(())
}

fn subscribe(dataset: &str, namespace: &str, from: u64) -> pb::Request {
    pb::Request {
        kind: Some(pb::request::Kind::Subscribe(pb::Subscribe {
            dataset: dataset.into(),
            namespace: namespace.into(),
            from,
        })),
    }
}

fn ack(dataset: &str, namespace: &str, applied: u64) -> pb::Request {
    pb::Request {
        kind: Some(pb::request::Kind::Ack(pb::Ack {
            dataset: dataset.into(),
            namespace: namespace.into(),
            applied,
        })),
    }
}

impl From<Event> for pb::Event {
    fn from(e: Event) -> Self {
        pb::Event {
            dataset: e.dataset,
            namespace: e.namespace,
            stream: e.stream,
            sequence: e.sequence,
            epoch: e.epoch,
            committed_at: e.committed_at,
            key: e.key,
            value: match e.operation {
                Operation::Set(value) => Some(value),
                Operation::Delete => None,
            },
        }
    }
}

impl From<pb::Event> for Event {
    fn from(e: pb::Event) -> Self {
        Event {
            dataset: e.dataset,
            namespace: e.namespace,
            stream: e.stream,
            sequence: e.sequence,
            epoch: e.epoch,
            committed_at: e.committed_at,
            key: e.key,
            operation: e.value.map_or(Operation::Delete, Operation::Set),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replication/subscriptions.rs"]
mod tests;
