//! One bounded relay session per process, with atomic, selector-scoped local progress.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::placement::{hash_of, in_span, valid_span};
use crate::replication::log::Watched;
use crate::replication::pb;
use crate::storage::{KeyRange, StorageEngine, WriteBatch, l2_key, l2_parts, l2_span};
use crate::transport::{Class, Peers};

const TIMEOUT: Duration = Duration::from_secs(5);
const IDLE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Deserialize)]
pub struct Selection {
    pub dataset: String,
    pub namespace: String,
}

/// A whole namespace stored under `prefix`. With an empty namespace and `span` set, instead a hash range `(start,
/// end)` (`end` 0 meaning 2⁶⁴) followed in every namespace of the dataset and stored in the hash-first L2 layout
/// ([`l2_key`]); `prefix` is then unused (#22 / B-21).
pub struct Target {
    pub selection: Selection,
    pub engine: Arc<dyn StorageEngine>,
    pub prefix: Vec<u8>,
    pub span: Option<(u64, u64)>,
}

/// `(dataset, namespace, range)`: the stream one consumer applies.
type Scope = (String, String, Option<(u64, u64)>);
/// A range followed in every namespace of a dataset, with the engine its namespaces' consumers write to.
type Groups = BTreeMap<(String, (u64, u64)), Arc<dyn StorageEngine>>;

fn span(range: Option<(u64, u64)>) -> Option<pb::Span> {
    range.map(|(start, end)| pb::Span { start, end })
}

fn range(span: Option<&pb::Span>) -> Option<(u64, u64)> {
    span.map(|s| (s.start, s.end))
}

/// Revision marking that a range consumer exists, so a restart resumes every namespace it held.
fn marker_prefix(dataset: &str, (start, end): (u64, u64)) -> String {
    format!("relay:range:{dataset}:{start:016x}-{end:016x}:")
}

#[derive(Default)]
struct Progress {
    connected: bool,
    scopes: BTreeMap<Scope, (u64, Option<u64>, bool)>,
    error: Option<Status>,
}

/// Owns its reconnecting task. Drop aborts it; `stop` joins before storage reuse.
pub struct Session {
    progress: Arc<Mutex<Progress>>,
    changes: Arc<Watched<u64>>,
    task: tokio::task::JoinHandle<()>,
    draining: Option<Arc<Watched<bool>>>,
}

impl Session {
    pub fn start(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        targets: Vec<Target>,
    ) -> Result<Self, Status> {
        Self::open(peers, upstreams, targets, None)
    }

    /// Like [`Session::start`], announcing `member` to the relays, which makes this process a cell member (B-12).
    pub fn join(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        targets: Vec<Target>,
        member: pb::Member,
    ) -> Result<Self, Status> {
        crate::membership::validate(&member)?;
        if member.status != pb::member::Status::Ready as i32 {
            return Err(Status::invalid_argument("a joining member must be ready"));
        }
        Self::open(peers, upstreams, targets, Some(member))
    }

    fn open(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        targets: Vec<Target>,
        member: Option<pb::Member>,
    ) -> Result<Self, Status> {
        if upstreams.is_empty() {
            return Err(Status::invalid_argument("upstreams are required"));
        }
        let mut consumers = BTreeMap::new();
        let mut groups = Groups::new();
        let mut state = Progress::default();
        for target in targets {
            let resumed = match target.span {
                Some(range) => {
                    let dataset = target.selection.dataset.clone();
                    if !target.selection.namespace.is_empty()
                        || !super::pushed(&dataset)
                        || !valid_span(&pb::Span {
                            start: range.0,
                            end: range.1,
                        })
                    {
                        return Err(Status::invalid_argument("invalid relay range selection"));
                    }
                    let resumed = Consumer::resumed(&target, range)?;
                    if groups.insert((dataset, range), target.engine).is_some() {
                        return Err(Status::invalid_argument("duplicate relay range"));
                    }
                    resumed
                }
                None => vec![Consumer::new(target)?],
            };
            for consumer in resumed {
                let scope = consumer.scope();
                state.scopes.insert(
                    scope.clone(),
                    (consumer.applied, None, consumer.validated()),
                );
                if consumers.insert(scope, consumer).is_some() {
                    return Err(Status::invalid_argument("duplicate relay scope"));
                }
            }
        }
        let progress = Arc::new(Mutex::new(state));
        let changes = Arc::new(Watched::new(0));
        let draining = member.as_ref().map(|_| Arc::new(Watched::new(false)));
        let task = tokio::spawn(run(
            peers,
            upstreams,
            consumers,
            groups,
            progress.clone(),
            changes.clone(),
            member.zip(draining.clone()),
        ));
        Ok(Self {
            progress,
            changes,
            task,
            draining,
        })
    }

    pub fn ready(&self) -> bool {
        let state = self.progress.lock().unwrap();
        state.connected
            && state.scopes.values().all(|(applied, head, validated)| {
                *validated && head.is_some_and(|head| *applied >= head)
            })
    }

    /// Most recent connection, protocol or local storage failure, for readiness diagnostics.
    pub fn error(&self) -> Option<Status> {
        self.progress.lock().unwrap().error.clone()
    }

    /// The validated revision applied for a whole-namespace scope.
    pub fn applied(&self, dataset: &str, namespace: &str) -> Option<u64> {
        self.progress
            .lock()
            .unwrap()
            .scopes
            .get(&(dataset.to_owned(), namespace.to_owned(), None))
            .filter(|(_, _, validated)| *validated)
            .map(|(applied, _, _)| *applied)
    }

    /// Graceful shutdown of a member: announces `draining`, closes the stream once the relay has it, then stops.
    /// Cell relays remove a drained member without waiting out the grace period (B-12).
    pub async fn drain(mut self) {
        if let Some(draining) = &self.draining {
            draining.update(|d| *d = true);
            // A finished task must not be polled again.
            if tokio::time::timeout(IDLE, &mut self.task).await.is_ok() {
                return;
            }
        }
        self.stop().await;
    }

    /// Stop and join the task before replacing or reusing its local storage.
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    /// The caller supplies its read deadline. Only durable coverage satisfies the wait.
    pub async fn wait_for(&self, dataset: &str, namespace: &str, revision: u64) {
        let scope = (dataset.to_owned(), namespace.to_owned(), None);
        loop {
            let generation = self.changes.get();
            if self
                .progress
                .lock()
                .unwrap()
                .scopes
                .get(&scope)
                .is_some_and(|(applied, _, validated)| *validated && *applied >= revision)
            {
                return;
            }
            self.changes.wait_until(|now| *now != generation).await;
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct BootstrapState {
    boundary: u64,
    committed_at: u64,
    digest: Sha256,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

struct Consumer {
    target: Target,
    /// Range consumers only: the revision marking that this namespace of the range is held.
    marker: Option<String>,
    revision: String,
    pending_confirmation: String,
    active: String,
    active_prefix: String,
    applied: u64,
    recover: bool,
    awaiting_confirmation: bool,
    bootstrap: Option<BootstrapState>,
}

fn storage(error: crate::storage::StorageError) -> Status {
    let mut status = Status::internal(error.to_string());
    status
        .metadata_mut()
        .insert("bedrock-local-error", "true".parse().unwrap());
    status
}

impl Consumer {
    /// The consumers of every namespace a range target held before a restart.
    fn resumed(target: &Target, range: (u64, u64)) -> Result<Vec<Self>, Status> {
        let prefix = marker_prefix(&target.selection.dataset, range);
        let revisions = target
            .engine
            .snapshot()
            .map_err(storage)?
            .revisions()
            .map_err(storage)?;
        revisions
            .iter()
            .filter(|(_, value)| **value == 1)
            .filter_map(|(name, _)| name.strip_prefix(&prefix))
            .map(|namespace| Self::in_range(target, namespace, range))
            .collect()
    }

    /// The consumer of one namespace of a range target.
    fn in_range(target: &Target, namespace: &str, range: (u64, u64)) -> Result<Self, Status> {
        Self::new(Target {
            selection: Selection {
                dataset: target.selection.dataset.clone(),
                namespace: namespace.to_owned(),
            },
            engine: target.engine.clone(),
            prefix: Vec::new(),
            span: Some(range),
        })
    }

    fn new(target: Target) -> Result<Self, Status> {
        let selection = &target.selection;
        if !super::pushed(&selection.dataset)
            || selection.namespace.is_empty()
            || selection.dataset.contains('\0')
            || selection.namespace.contains('\0')
        {
            return Err(Status::invalid_argument("invalid relay selection"));
        }
        let mut hash = Sha256::new();
        for part in [
            selection.dataset.as_bytes(),
            selection.namespace.as_bytes(),
            &target.prefix,
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        let scope = format!("{:x}", hash.finalize());
        let active_prefix = format!("relay:active:{scope}:");
        let selector = target
            .span
            .map(|(start, end)| format!("l2:{start:016x}-{end:016x}"))
            .unwrap_or_default();
        let marker = target
            .span
            .map(|range| marker_prefix(&selection.dataset, range) + &selection.namespace);
        let active = format!("{active_prefix}{selector}");
        let revision = format!("relay:cursor:{scope}:{selector}");
        let pending_confirmation = format!("relay:pending:{scope}:{selector}");
        let snapshot = target.engine.snapshot().map_err(storage)?;
        let revisions = snapshot.revisions().map_err(storage)?;
        let matches = revisions.get(&active) == Some(&1);
        let previous = revisions
            .iter()
            .any(|(name, value)| name.starts_with(&active_prefix) && *value == 1);
        if revisions
            .iter()
            .filter(|(name, value)| name.starts_with(&active_prefix) && **value == 1)
            .count()
            > 1
        {
            return Err(Status::data_loss("multiple active relay selectors"));
        }
        drop(snapshot);
        let populated = !stored(&target, true)?.is_empty();
        let snapshot = target.engine.snapshot().map_err(storage)?;
        let applied = if matches {
            snapshot
                .revision(&revision)
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("relay selector has no committed cursor"))?
        } else {
            0
        };
        let awaiting_confirmation =
            matches && snapshot.revision(&pending_confirmation).map_err(storage)? == Some(1);
        let recover = awaiting_confirmation
            || (!matches
                && (previous
                    || populated
                    || snapshot
                        .revision(&selection.namespace)
                        .map_err(storage)?
                        .is_some()));
        drop(snapshot);
        Ok(Self {
            target,
            marker,
            revision,
            pending_confirmation,
            active,
            active_prefix,
            applied,
            recover,
            awaiting_confirmation,
            bootstrap: None,
        })
    }

    fn scope(&self) -> Scope {
        (
            self.target.selection.dataset.clone(),
            self.target.selection.namespace.clone(),
            self.target.span,
        )
    }

    /// Where `key` is stored: under the namespace prefix, or hash-first for a range.
    fn physical(&self, key: &[u8]) -> Vec<u8> {
        let s = &self.target.selection;
        match self.target.span {
            Some(_) => l2_key(hash_of(&s.dataset, &s.namespace, key), &s.namespace, key),
            None => [self.target.prefix.as_slice(), key].concat(),
        }
    }

    fn validated(&self) -> bool {
        !self.recover && !self.awaiting_confirmation && self.bootstrap.is_none()
    }

    fn reconnect(&mut self) {
        self.bootstrap = None;
        self.recover |= self.awaiting_confirmation;
    }

    fn range(&mut self, range: &pb::ReplayRange) -> Result<(), Status> {
        if range.dataset != self.target.selection.dataset
            || range.namespace != self.target.selection.namespace
            || self::range(range.span.as_ref()) != self.target.span
            || (range.first == 0) != (range.last == 0)
            || range.first > range.last
            || range.last > range.head
        {
            return Err(Status::data_loss("invalid replay range"));
        }
        if range.recovered
            && self.awaiting_confirmation
            && self.bootstrap.is_none()
            && self.applied >= range.head
            && !self.recover
        {
            let mut batch = WriteBatch::default();
            batch.set_revision(&self.pending_confirmation, 0);
            self.target.engine.write(batch).map_err(storage)?;
            self.awaiting_confirmation = false;
        }
        Ok(())
    }

    fn selected(&self, key: &[u8]) -> bool {
        let s = &self.target.selection;
        span(self.target.span)
            .is_none_or(|span| in_span(&span, hash_of(&s.dataset, &s.namespace, key)))
    }

    fn follow(&self) -> pb::Follow {
        pb::Follow {
            dataset: self.target.selection.dataset.clone(),
            namespace: self.target.selection.namespace.clone(),
            from: self.applied.saturating_add(1),
            span: span(self.target.span),
            from_namespaces: BTreeMap::new(),
        }
    }

    /// Where a range follow should start this namespace: its next revision, or 0 for a fresh baseline.
    fn resume(&self) -> u64 {
        if self.recover || self.awaiting_confirmation {
            0
        } else {
            self.applied.saturating_add(1)
        }
    }

    fn request(&self) -> pb::RelayRequest {
        let kind = if self.recover || self.awaiting_confirmation {
            let mut follow = self.follow();
            follow.from = 0;
            pb::relay_request::Kind::Recover(pb::Recover {
                follow: Some(follow),
                visited: Vec::new(),
            })
        } else {
            pb::relay_request::Kind::Follow(self.follow())
        };
        pb::RelayRequest { kind: Some(kind) }
    }

    fn commit(&mut self, mut batch: WriteBatch, boundary: u64) -> Result<(), Status> {
        let revisions = self
            .target
            .engine
            .snapshot()
            .map_err(storage)?
            .revisions()
            .map_err(storage)?;
        for name in revisions
            .keys()
            .filter(|name| name.starts_with(&self.active_prefix))
        {
            batch.set_revision(name, 0);
        }
        if let Some(marker) = &self.marker {
            batch.set_revision(marker, 1);
        }
        batch
            .set_revision(&self.active, 1)
            .set_revision(&self.revision, boundary)
            .set_revision(
                &self.pending_confirmation,
                u64::from(self.awaiting_confirmation),
            );
        self.target.engine.write(batch).map_err(storage)?;
        self.applied = boundary;
        self.recover = false;
        Ok(())
    }

    fn delivery_events(&self, delivery: &pb::Delivery) -> Result<Vec<pb::Event>, Status> {
        if delivery.dataset != self.target.selection.dataset
            || delivery.namespace != self.target.selection.namespace
            || range(delivery.span.as_ref()) != self.target.span
            || delivery.from == 0
            || delivery.through < delivery.from
        {
            return Err(Status::data_loss("invalid relay coverage"));
        }
        let mut events = Vec::with_capacity(delivery.events.len());
        let mut previous = delivery.from - 1;
        for encoded in &delivery.events {
            let event = pb::Event::decode(encoded.clone())
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if event.dataset != self.target.selection.dataset
                || event.namespace != self.target.selection.namespace
                || event.sequence <= previous
                || event.sequence < delivery.from
                || event.sequence > delivery.through
                || !self.selected(&event.key)
            {
                return Err(Status::data_loss("event outside selected ordered coverage"));
            }
            previous = event.sequence;
            events.push(event);
        }
        if self.target.span.is_none()
            && (events.len() as u64 != delivery.through - delivery.from + 1
                || events
                    .iter()
                    .enumerate()
                    .any(|(i, e)| e.sequence != delivery.from + i as u64))
        {
            return Err(Status::data_loss("unfiltered delivery omitted an event"));
        }
        Ok(events)
    }

    /// Returns false for a gap: the caller reissues Follow at the committed cursor.
    fn delivery(&mut self, delivery: pb::Delivery) -> Result<bool, Status> {
        let events = self.delivery_events(&delivery)?;
        if self.bootstrap.is_some() || self.recover {
            return Err(Status::data_loss("delivery before baseline installation"));
        }
        if delivery.through <= self.applied {
            return Ok(true);
        }
        if delivery.from != self.applied.saturating_add(1) {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        for event in events {
            let key = self.physical(&event.key);
            match event.value {
                Some(value) => {
                    batch.put(key, value);
                }
                None => {
                    batch.delete(key);
                }
            }
        }
        self.commit(batch, delivery.through)?;
        Ok(true)
    }

    fn bootstrap(&mut self, chunk: pb::Bootstrap) -> Result<bool, Status> {
        if chunk.dataset != self.target.selection.dataset
            || chunk.namespace != self.target.selection.namespace
            || range(chunk.span.as_ref()) != self.target.span
        {
            return Err(Status::data_loss("bootstrap scope changed"));
        }
        if chunk.begin {
            if self.bootstrap.is_some() || chunk.boundary < self.applied {
                return Err(Status::data_loss(
                    "invalid bootstrap boundary or nested begin",
                ));
            }
            self.bootstrap = Some(BootstrapState {
                boundary: chunk.boundary,
                committed_at: chunk.committed_at,
                digest: bootstrap_digest(
                    &chunk.dataset,
                    &chunk.namespace,
                    chunk.boundary,
                    chunk.committed_at,
                ),
                entries: BTreeMap::new(),
            });
            self.awaiting_confirmation = true;
        }
        for entry in &chunk.entries {
            if !self.selected(&entry.key) {
                return Err(Status::data_loss("bootstrap entry outside selection"));
            }
        }
        let Some(state) = self.bootstrap.as_mut() else {
            return Err(Status::data_loss("bootstrap without begin"));
        };
        if state.boundary != chunk.boundary
            || state.committed_at != chunk.committed_at
            || (!chunk.end && !chunk.checksum.is_empty())
        {
            return Err(Status::data_loss("bootstrap header changed"));
        }
        for entry in chunk.entries {
            if state
                .entries
                .last_key_value()
                .is_some_and(|(key, _)| key >= &entry.key)
            {
                return Err(Status::data_loss("bootstrap keys are not strictly sorted"));
            }
            for part in [&entry.key, &entry.value] {
                state.digest.update((part.len() as u64).to_be_bytes());
                state.digest.update(part);
            }
            state.entries.insert(entry.key, entry.value);
        }
        if !chunk.end {
            return Ok(false);
        }
        let state = self.bootstrap.take().unwrap();
        if &state.digest.finalize()[..] != chunk.checksum.as_slice() {
            return Err(Status::data_loss("bootstrap checksum mismatch"));
        }
        let mut batch = WriteBatch::default();
        match self.target.span {
            None => {
                batch.delete_range(KeyRange::prefix(self.target.prefix.clone()));
            }
            Some(_) => {
                for key in stored(&self.target, false)? {
                    batch.delete(key);
                }
            }
        }
        for (key, value) in state.entries {
            batch.put(self.physical(&key), value);
        }
        self.commit(batch, state.boundary)?;
        Ok(true)
    }
}

/// The stored keys of a consumer's target: every key under a namespace prefix, or this namespace's keys in a range.
/// With `first`, stops after one.
fn stored(target: &Target, first: bool) -> Result<Vec<Vec<u8>>, Status> {
    let (scan, namespace) = match target.span {
        Some((start, end)) => (
            l2_span(start, end),
            Some(target.selection.namespace.as_bytes()),
        ),
        None => (KeyRange::prefix(target.prefix.clone()), None),
    };
    let mut keys = Vec::new();
    for entry in target.engine.scan(&scan).map_err(storage)? {
        let (key, _) = entry.map_err(storage)?;
        if namespace.is_none_or(|ns| l2_parts(&key).is_some_and(|(n, _)| n == ns)) {
            keys.push(key);
            if first {
                break;
            }
        }
    }
    Ok(keys)
}

pub(super) fn bootstrap_digest(
    dataset: &str,
    namespace: &str,
    boundary: u64,
    committed_at: u64,
) -> Sha256 {
    let mut digest = Sha256::new();
    for part in [dataset.as_bytes(), namespace.as_bytes()] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.update(boundary.to_be_bytes());
    digest.update(committed_at.to_be_bytes());
    digest
}

fn changed(progress: &Mutex<Progress>, changes: &Watched<u64>, update: impl FnOnce(&mut Progress)) {
    update(&mut progress.lock().unwrap());
    changes.update(|version| *version = version.wrapping_add(1));
}

async fn send(
    requests: &mpsc::Sender<pb::RelayRequest>,
    request: pb::RelayRequest,
) -> Result<(), Status> {
    tokio::time::timeout(TIMEOUT, requests.send(request))
        .await
        .map_err(|_| Status::deadline_exceeded("relay request stalled"))?
        .map_err(|_| Status::unavailable("relay request stream closed"))
}

type Membership = (pb::Member, Arc<Watched<bool>>);

async fn run(
    peers: Arc<Peers>,
    upstreams: Vec<String>,
    mut consumers: BTreeMap<Scope, Consumer>,
    groups: Groups,
    progress: Arc<Mutex<Progress>>,
    changes: Arc<Watched<u64>>,
    member: Option<Membership>,
) {
    let drained = || member.as_ref().is_some_and(|(_, draining)| draining.get());
    loop {
        for upstream in peers.health().route(&upstreams) {
            if drained() {
                return;
            }
            for consumer in consumers.values_mut() {
                consumer.reconnect();
            }
            changed(&progress, &changes, |state| {
                state.connected = false;
                for (_, head, _) in state.scopes.values_mut() {
                    *head = None;
                }
            });
            let error = connection(
                &peers,
                upstream,
                &mut consumers,
                &groups,
                &progress,
                &changes,
                member.as_ref(),
            )
            .await;
            if drained() {
                return;
            }
            changed(&progress, &changes, |state| {
                state.connected = false;
                state.error = Some(error.clone());
            });
            // Local failures do not poison a healthy upstream's breaker.
            if error.metadata().get("bedrock-local-error").is_none()
                && matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Unknown
                        | tonic::Code::Internal
                        | tonic::Code::ResourceExhausted
                        | tonic::Code::DataLoss
                )
            {
                peers.health().trip(upstream);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connection(
    peers: &Peers,
    upstream: &str,
    consumers: &mut BTreeMap<Scope, Consumer>,
    groups: &Groups,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
    member: Option<&Membership>,
) -> Status {
    match connected(
        peers, upstream, consumers, groups, progress, changes, member,
    )
    .await
    {
        Ok(()) => Status::unavailable("relay stream ended"),
        Err(error) => error,
    }
}

async fn connected(
    peers: &Peers,
    upstream: &str,
    consumers: &mut BTreeMap<Scope, Consumer>,
    groups: &Groups,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
    member: Option<&Membership>,
) -> Result<(), Status> {
    let (requests, receiver) = mpsc::channel(16);
    send(
        &requests,
        pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                tier: 0,
                member: member.map(|(member, _)| member.clone()),
            })),
        },
    )
    .await?;
    let channel = peers
        .channel(upstream, Class::Control)
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let mut client = pb::relay_client::RelayClient::new(channel);
    let mut stream = peers
        .call(
            upstream,
            Class::Control,
            TIMEOUT,
            client.session(ReceiverStream::new(receiver)),
        )
        .await?
        .into_inner();
    let welcome = peers
        .call(upstream, Class::Control, TIMEOUT, stream.message())
        .await?;
    match welcome.and_then(|frame| frame.kind) {
        Some(pb::relay_frame::Kind::Welcome(w)) if w.version == 1 && w.tier > 0 && w.tier <= 4 => {}
        _ => return Err(Status::failed_precondition("invalid relay handshake")),
    }
    for consumer in consumers.values().filter(|c| c.target.span.is_none()) {
        send(&requests, consumer.request()).await?;
    }
    // One follow per range; the relay starts every namespace, each where its consumer left off.
    for (dataset, range) in groups.keys() {
        let from_namespaces = consumers
            .values()
            .filter(|c| c.target.selection.dataset == *dataset && c.target.span == Some(*range))
            .map(|c| (c.target.selection.namespace.clone(), c.resume()))
            .collect();
        let follow = pb::Follow {
            dataset: dataset.clone(),
            namespace: String::new(),
            from: 0,
            span: span(Some(*range)),
            from_namespaces,
        };
        send(
            &requests,
            pb::RelayRequest {
                kind: Some(pb::relay_request::Kind::Follow(follow)),
            },
        )
        .await?;
    }
    changed(progress, changes, |state| {
        state.connected = true;
        state.error = None;
    });
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    let mut intake = Intake::default();
    let mut draining = std::pin::pin!(async {
        match member {
            Some((_, draining)) => draining.wait_until(|d| *d).await,
            None => std::future::pending().await,
        }
    });
    loop {
        // Keep this read's deadline when sending progress; outbound activity cannot prove the source is alive.
        let frame = {
            let mut reading =
                std::pin::pin!(peers.call(upstream, Class::Control, IDLE, stream.message()));
            loop {
                tokio::select! {
                    biased;
                    _ = &mut draining => break None,
                    _ = tick.tick() => {
                        send(&requests, pb::RelayRequest {
                            kind: Some(pb::relay_request::Kind::Progress(true)),
                        }).await?;
                        for scope in std::mem::take(&mut intake.retries) {
                            send(&requests, consumers.get(&scope).unwrap().request()).await?;
                        }
                    },
                    frame = &mut reading => break Some(frame?),
                }
            }
        };
        let Some(frame) = frame else {
            let (member, _) = member.unwrap();
            send(
                &requests,
                pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Member(pb::Member {
                        status: pb::member::Status::Draining as i32,
                        ..member.clone()
                    })),
                },
            )
            .await?;
            // Half-close; the relay ends the stream once it has processed the status.
            drop(requests);
            while peers
                .call(upstream, Class::Control, TIMEOUT, stream.message())
                .await?
                .is_some()
            {}
            return Ok(());
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        let (scope, action) = match frame.kind {
            Some(pb::relay_frame::Kind::Tick(true)) => continue,
            Some(pb::relay_frame::Kind::Delivery(d)) => (
                (
                    d.dataset.clone(),
                    d.namespace.clone(),
                    range(d.span.as_ref()),
                ),
                Action::Delivery(d),
            ),
            Some(pb::relay_frame::Kind::Range(r)) => (
                (
                    r.dataset.clone(),
                    r.namespace.clone(),
                    range(r.span.as_ref()),
                ),
                Action::Range(r),
            ),
            Some(pb::relay_frame::Kind::Bootstrap(b)) => (
                (
                    b.dataset.clone(),
                    b.namespace.clone(),
                    range(b.span.as_ref()),
                ),
                Action::Bootstrap(b),
            ),
            Some(pb::relay_frame::Kind::Error(e)) => (
                (
                    e.dataset.clone(),
                    e.namespace.clone(),
                    range(e.span.as_ref()),
                ),
                Action::Error(e),
            ),
            _ => return Err(Status::data_loss("unexpected relay frame")),
        };
        // A range stream's first frame for a namespace creates that namespace's consumer.
        if !consumers.contains_key(&scope)
            && let (dataset, namespace, Some(range)) = &scope
            && let Some(engine) = groups.get(&(dataset.clone(), *range))
        {
            let target = Target {
                selection: Selection {
                    dataset: dataset.clone(),
                    namespace: String::new(),
                },
                engine: engine.clone(),
                prefix: Vec::new(),
                span: Some(*range),
            };
            let consumer = Consumer::in_range(&target, namespace, *range)?;
            changed(progress, changes, |state| {
                state.scopes.insert(
                    scope.clone(),
                    (consumer.applied, None, consumer.validated()),
                );
            });
            consumers.insert(scope.clone(), consumer);
        }
        let consumer = consumers
            .get_mut(&scope)
            .ok_or_else(|| Status::data_loss("unsolicited relay scope"))?;
        let effect = intake.apply(&scope, consumer, action)?;
        changed(progress, changes, |state| {
            let entry = state.scopes.get_mut(&scope).unwrap();
            entry.0 = consumer.applied;
            entry.2 = consumer.validated();
            if effect.head.is_some() {
                entry.1 = effect.head;
            }
            if let Some(error) = effect.error {
                state.error = Some(error);
            } else if intake.failed.is_empty() {
                state.error = None;
            }
        });
        if effect.refollow {
            send(&requests, consumer.request()).await?;
        }
        if effect.acknowledge {
            send(
                &requests,
                pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                        dataset: scope.0,
                        namespace: scope.1,
                        applied: consumer.applied,
                        span: span(scope.2),
                    })),
                },
            )
            .await?;
        }
    }
}

enum Action {
    Delivery(pb::Delivery),
    Range(pb::ReplayRange),
    Bootstrap(pb::Bootstrap),
    Error(pb::RelayError),
}

#[derive(Default)]
struct Effect {
    acknowledge: bool,
    refollow: bool,
    head: Option<u64>,
    error: Option<Status>,
}

#[derive(Default)]
struct Intake {
    failed: BTreeSet<Scope>,
    retries: BTreeSet<Scope>,
}

impl Intake {
    fn fail(&mut self, scope: &Scope, consumer: &mut Consumer, error: Status) -> Effect {
        consumer.recover = true;
        consumer.awaiting_confirmation = true;
        consumer.bootstrap = None;
        let mut batch = WriteBatch::default();
        batch.set_revision(&consumer.pending_confirmation, 1);
        // A failed engine may reject this marker too; the in-process gate still isolates that scope.
        let _ = consumer.target.engine.write(batch);
        self.failed.insert(scope.clone());
        self.retries.insert(scope.clone());
        Effect {
            error: Some(error),
            ..Effect::default()
        }
    }

    fn apply(
        &mut self,
        scope: &Scope,
        consumer: &mut Consumer,
        action: Action,
    ) -> Result<Effect, Status> {
        let action = match action {
            Action::Error(error) => {
                return Ok(self.fail(
                    scope,
                    consumer,
                    Status::new(tonic::Code::from_i32(error.code as i32), error.detail),
                ));
            }
            other => other,
        };
        // Drain the previous subscription until a retry installs a fresh baseline. Coverage stays validated.
        if self.failed.contains(scope) && consumer.recover {
            match &action {
                Action::Delivery(delivery) => {
                    consumer.delivery_events(delivery)?;
                    return Ok(Effect::default());
                }
                Action::Bootstrap(chunk) if !chunk.begin && consumer.bootstrap.is_none() => {
                    return Ok(Effect::default());
                }
                _ => {}
            }
        }
        let result = match action {
            Action::Delivery(delivery) => consumer.delivery(delivery).map(|applied| Effect {
                acknowledge: applied,
                refollow: !applied,
                ..Effect::default()
            }),
            Action::Range(range) => consumer.range(&range).map(|()| Effect {
                head: Some(range.head),
                ..Effect::default()
            }),
            Action::Bootstrap(chunk) => consumer.bootstrap(chunk).map(|acknowledge| Effect {
                acknowledge,
                ..Effect::default()
            }),
            Action::Error(_) => unreachable!(),
        };
        match result {
            Err(error) if error.metadata().get("bedrock-local-error").is_some() => {
                Ok(self.fail(scope, consumer, error))
            }
            Err(error) => Err(error),
            Ok(effect) => {
                if consumer.validated() {
                    self.failed.remove(scope);
                    self.retries.remove(scope);
                }
                Ok(effect)
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/relays/client.rs"]
mod tests;
