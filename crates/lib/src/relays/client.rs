//! One bounded relay session per process, with atomic, selector-scoped local progress.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;

use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::ranges::{self, Range};
use crate::placement::{hash_of, in_span, valid_span};
use crate::replication::log::Watched;
use crate::replication::pb;
use crate::storage::{KeyRange, StorageEngine, WriteBatch, l2_key, l2_parts, l2_span};
use crate::storage::{Snapshot, StorageResult};
use crate::transport::{Class, Peers};

const TIMEOUT: Duration = Duration::from_secs(5);
const IDLE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
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

/// The ranges this process follows, and those relays push to a member (#22 / B-21).
struct Pushed {
    groups: Groups,
    /// Dataset → the engine its pushed ranges are stored in; datasets the process does not carry are absent.
    ranged: BTreeMap<String, Arc<dyn StorageEngine>>,
    /// The member's node ID and the scope of its own membership view, which confirms revokes.
    me: Option<(String, Scope)>,
    /// The latest pushed set per dataset, applied once the member's view reaches its revision.
    pending: BTreeMap<String, pb::Ranges>,
    /// Ranges unfollowed on this connection; frames the relay sent before the unfollow are dropped.
    retired: BTreeSet<(String, Range)>,
}

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

/// The ranges of `dataset` held in `engine` before a restart, from their consumers' markers. Returns an internal
/// status on storage failure and a data-loss status for malformed active markers.
fn held(engine: &dyn StorageEngine, dataset: &str) -> Result<BTreeSet<Range>, Status> {
    let prefix = format!("relay:range:{dataset}:");
    let revisions = engine
        .snapshot()
        .map_err(storage)?
        .revisions()
        .map_err(storage)?;
    revisions
        .iter()
        .filter(|(_, value)| **value == 1)
        .filter_map(|(name, _)| name.strip_prefix(&prefix))
        .map(|rest| {
            marker_range(rest).ok_or_else(|| Status::data_loss("invalid relay range marker"))
        })
        .collect()
}

/// The range at the start of a marker, after its `relay:range:{dataset}:` prefix.
fn marker_range(rest: &str) -> Option<Range> {
    let start = u64::from_str_radix(rest.get(..16)?, 16).ok()?;
    let end = u64::from_str_radix(rest.get(17..33)?, 16).ok()?;
    (rest.get(16..17)? == "-" && rest.get(33..34)? == ":").then_some((start, end))
}

/// One consumer's validated position in `snapshot`: what the snapshot holds for this scope is the stream's state at that
/// revision. `None` until it has validated progress. For the checkers of #25 / B-23, simulated and real.
pub fn applied_at(
    snapshot: &dyn Snapshot,
    dataset: &str,
    namespace: &str,
    prefix: &[u8],
    span: Option<Range>,
) -> StorageResult<Option<u64>> {
    let names = Names::of(dataset, namespace, prefix, span);
    let revisions = snapshot.revisions()?;
    let validated =
        revisions.get(&names.active) == Some(&1) && revisions.get(&names.pending) != Some(&1);
    Ok(validated
        .then(|| revisions.get(&names.revision).copied())
        .flatten())
}

/// `(namespace, range, revision)` of every range consumer of `dataset` with validated progress in `snapshot`.
pub fn applied_ranges(
    snapshot: &dyn Snapshot,
    dataset: &str,
) -> StorageResult<Vec<(String, Range, u64)>> {
    let prefix = format!("relay:range:{dataset}:");
    let mut applied = Vec::new();
    for (name, value) in snapshot.revisions()? {
        let Some(rest) = name.strip_prefix(&prefix).filter(|_| value == 1) else {
            continue;
        };
        let (Some(range), Some(namespace)) = (marker_range(rest), rest.get(34..)) else {
            continue;
        };
        if let Some(revision) = applied_at(snapshot, dataset, namespace, &[], Some(range))? {
            applied.push((namespace.to_owned(), range, revision));
        }
    }
    Ok(applied)
}

/// The revision names of one consumer's progress.
struct Names {
    active_prefix: String,
    active: String,
    revision: String,
    pending: String,
    marker: Option<String>,
}

impl Names {
    fn of(dataset: &str, namespace: &str, prefix: &[u8], span: Option<Range>) -> Self {
        let mut hash = Sha256::new();
        let range = span.map(|(start, end)| [start.to_be_bytes(), end.to_be_bytes()].concat());
        // A range is part of the identity, so one namespace can be held in several ranges.
        for part in [dataset.as_bytes(), namespace.as_bytes(), prefix]
            .into_iter()
            .chain(range.as_deref())
        {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        let scope = format!("{:x}", hash.finalize());
        let active_prefix = format!("relay:active:{scope}:");
        let selector = span
            .map(|(start, end)| format!("l2:{start:016x}-{end:016x}"))
            .unwrap_or_default();
        Names {
            active: format!("{active_prefix}{selector}"),
            active_prefix,
            revision: format!("relay:cursor:{scope}:{selector}"),
            pending: format!("relay:pending:{scope}:{selector}"),
            marker: span.map(|range| marker_prefix(dataset, range) + namespace),
        }
    }
}

/// Rebuild generation requested per dataset ([`Rebuilder`]).
type Rebuilds = Arc<Watched<BTreeMap<String, u64>>>;

#[derive(Default)]
struct Progress {
    connected: bool,
    scopes: BTreeMap<Scope, (u64, Option<u64>, bool)>,
    error: Option<Status>,
    /// The latest rebuild generation each scope was validated after.
    rebuilt: BTreeMap<Scope, u64>,
}

/// Owns its reconnecting task. Drop aborts it; `stop` joins before storage reuse.
pub struct Session {
    progress: Arc<Mutex<Progress>>,
    changes: Arc<Watched<u64>>,
    rebuilds: Rebuilds,
    task: tokio::task::JoinHandle<()>,
    member: Option<Membership>,
}

impl Session {
    /// Starts a reconnecting relay session for `targets`. Returns an invalid-argument status for missing upstreams or
    /// invalid or duplicate selections, an internal status for local storage errors, and a data-loss status for
    /// corrupt persisted selector state during startup.
    pub fn start(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        targets: Vec<Target>,
    ) -> Result<Self, Status> {
        Self::open(peers, upstreams, targets, None, BTreeMap::new())
    }

    /// Like [`Session::start`], announcing `member` to the relays, which makes this process a cell member (B-12).
    /// Relays push the member the L2 ranges it owns per dataset; `ranged` maps each dataset it carries to the engine
    /// those ranges are stored in (#22 / B-21). `targets` must include the member's own membership view
    /// ([`crate::membership::View`]), which confirms revokes. The member announces only the ranged datasets in
    /// `ranged`, so placement never gives it ranges it would not follow.
    /// Returns an invalid-argument status if `member` is invalid or not ready, or if `ranged` is not empty and `targets`
    /// lack the membership view, and propagates startup errors as in [`Session::start`].
    pub fn join(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        targets: Vec<Target>,
        mut member: pb::Member,
        ranged: BTreeMap<String, Arc<dyn StorageEngine>>,
    ) -> Result<Self, Status> {
        member
            .datasets
            .retain(|d| !super::pushed(d) || ranged.contains_key(d));
        member.unhealthy.retain(|d| member.datasets.contains(d));
        crate::membership::validate(&member)?;
        if member.status != pb::member::Status::Ready as i32 {
            return Err(Status::invalid_argument("a joining member must be ready"));
        }
        Self::open(peers, upstreams, targets, Some(member), ranged)
    }

    fn open(
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        mut targets: Vec<Target>,
        member: Option<pb::Member>,
        ranged: BTreeMap<String, Arc<dyn StorageEngine>>,
    ) -> Result<Self, Status> {
        if upstreams.is_empty() {
            return Err(Status::invalid_argument("upstreams are required"));
        }
        // Ranges held before a restart resume where they left off until the relays push the current set. One that cannot
        // be read waits for that push: the session must start, because storage is rebuilt through it (#23 / B-22).
        for (dataset, engine) in &ranged {
            let ranges = match held(engine.as_ref(), dataset) {
                Err(error) if local(&error) => {
                    tracing::warn!(%dataset, %error, "held ranges unreadable; waiting for the relays");
                    continue;
                }
                ranges => ranges?,
            };
            for range in ranges {
                let target = Target {
                    selection: Selection {
                        dataset: dataset.clone(),
                        namespace: String::new(),
                    },
                    engine: engine.clone(),
                    prefix: Vec::new(),
                    span: Some(range),
                };
                match Consumer::resumed(&target, range) {
                    Err(error) if local(&error) => {
                        tracing::warn!(%dataset, %error, "held range unreadable; waiting for the relays");
                    }
                    resumed => {
                        resumed?;
                        targets.push(target);
                    }
                }
            }
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
                    // Damage found in a later range quarantines the engine after the read above passed.
                    let resumed = match Consumer::resumed(&target, range) {
                        Err(error) if local(&error) => {
                            tracing::warn!(%dataset, %error, "held range unreadable; waiting for the relays");
                            continue;
                        }
                        resumed => resumed?,
                    };
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
        let pushed = Pushed {
            groups,
            ranged,
            me: member.as_ref().map(|m| {
                let view = crate::membership::namespace(&m.service, &m.cell);
                (
                    m.node_id.clone(),
                    (crate::membership::DATASET.to_owned(), view, None),
                )
            }),
            pending: BTreeMap::new(),
            retired: BTreeSet::new(),
        };
        // Without its own view a member could never confirm a revoke or acknowledge a pushed set.
        if !pushed.ranged.is_empty()
            && !pushed
                .me
                .as_ref()
                .is_some_and(|(_, view)| consumers.contains_key(view))
        {
            return Err(Status::invalid_argument(
                "pushed ranges need the member's membership view",
            ));
        }
        let progress = Arc::new(Mutex::new(state));
        let changes = Arc::new(Watched::new(0));
        let rebuilds = Rebuilds::default();
        let member = member.map(|member| Arc::new(Watched::new(member)));
        let task = tokio::spawn(run(
            peers,
            upstreams,
            consumers,
            pushed,
            progress.clone(),
            changes.clone(),
            rebuilds.clone(),
            member.clone(),
        ));
        Ok(Self {
            progress,
            changes,
            rebuilds,
            task,
            member,
        })
    }

    pub fn ready(&self) -> bool {
        self.ready_except(&BTreeSet::new())
    }

    /// Whether the session is connected and every scope outside `datasets` is validated and applied through its known
    /// head. A connected session with no remaining scopes is ready. Exclude quarantined datasets (B-14) as needed.
    pub fn ready_except(&self, datasets: &BTreeSet<String>) -> bool {
        let state = self.progress.lock().unwrap();
        state.connected
            && state
                .scopes
                .iter()
                .filter(|((dataset, _, _), _)| !datasets.contains(dataset))
                .all(|(_, (applied, head, validated))| {
                    *validated && head.is_some_and(|head| *applied >= head)
                })
    }

    /// Most recent connection, protocol or local storage failure, for readiness diagnostics.
    pub fn error(&self) -> Option<Status> {
        self.progress.lock().unwrap().error.clone()
    }

    /// The validated revision applied for a whole-namespace scope, or `None` if it is absent or not validated.
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
        if let Some(member) = &self.member {
            member.update(|m| m.status = pb::member::Status::Draining as i32);
            // A finished task must not be polled again.
            if tokio::time::timeout(IDLE, &mut self.task).await.is_ok() {
                return;
            }
        }
        self.stop().await;
    }

    /// Announces the datasets whose local storage is quarantined or rebuilding (#13 / B-14). The relays publish them
    /// with this member's entry. Datasets the member did not announce are dropped: the relays would refuse them.
    pub fn set_unhealthy(&self, unhealthy: &BTreeSet<String>) {
        let Some(member) = &self.member else { return };
        let mut current = member.get();
        let announced = current.datasets.clone();
        current.unhealthy = unhealthy
            .iter()
            .filter(|d| announced.contains(d))
            .cloned()
            .collect();
        if current.unhealthy != member.get().unhealthy {
            member.update(|m| m.unhealthy = current.unhealthy);
        }
    }

    /// Stop and join the task before replacing or reusing its local storage.
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    /// Waits for validated durable coverage of a whole-namespace scope through `revision`. The caller supplies its
    /// read deadline; an absent scope can wait indefinitely.
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

    /// Re-bootstraps `dataset`'s subscriptions on request, for rebuilding its local storage (#23 / B-22).
    pub fn rebuilder(&self, dataset: &str) -> Rebuilder {
        Rebuilder {
            dataset: dataset.to_owned(),
            progress: self.progress.clone(),
            changes: self.changes.clone(),
            rebuilds: self.rebuilds.clone(),
        }
    }
}

/// Makes one dataset's subscriptions install fresh verified bootstraps, as after a local storage failure.
pub struct Rebuilder {
    dataset: String,
    progress: Arc<Mutex<Progress>>,
    changes: Arc<Watched<u64>>,
    rebuilds: Rebuilds,
}

impl Rebuilder {
    /// Abandons each subscription's current coverage, recovers it from a new bootstrap, and returns once every one is
    /// validated again: bootstrap checksum verified, then confirmed through the relay's head. The caller bounds the wait.
    pub async fn rebuild(&self) {
        let mut generation = 0;
        self.rebuilds.update(|requested| {
            let entry = requested.entry(self.dataset.clone()).or_default();
            *entry += 1;
            generation = *entry;
        });
        loop {
            let seen = self.changes.get();
            {
                let state = self.progress.lock().unwrap();
                let rebuilt = state
                    .scopes
                    .iter()
                    .filter(|((dataset, _, _), _)| *dataset == self.dataset)
                    .all(|(scope, (_, _, validated))| {
                        *validated && state.rebuilt.get(scope) >= Some(&generation)
                    });
                if rebuilt {
                    return;
                }
            }
            self.changes.wait_until(|now| *now != seen).await;
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
    /// The rebuild generation this consumer has started recovering for.
    rebuild: u64,
}

/// Whether `status` came from this process's own storage, as [`storage`] marks it.
fn local(status: &Status) -> bool {
    status.metadata().get("bedrock-local-error").is_some()
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
        let Names {
            active_prefix,
            active,
            revision,
            pending: pending_confirmation,
            marker,
        } = Names::of(
            &selection.dataset,
            &selection.namespace,
            &target.prefix,
            target.span,
        );
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
            rebuild: 0,
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

    /// Nothing this consumer has applied counts until a fresh baseline installs.
    fn distrust(&mut self) {
        self.recover = true;
        self.awaiting_confirmation = true;
        self.bootstrap = None;
    }

    fn reconnect(&mut self) {
        self.bootstrap = None;
        self.recover |= self.awaiting_confirmation;
    }

    /// Whether a rebuild of this consumer's dataset was requested since it last started one.
    fn rebuild_requested(&self, requested: &BTreeMap<String, u64>) -> bool {
        requested
            .get(&self.target.selection.dataset)
            .is_some_and(|generation| *generation > self.rebuild)
    }

    /// Starts recovering for a requested rebuild: no coverage counts until a fresh bootstrap installs.
    fn start_rebuild(&mut self, requested: &BTreeMap<String, u64>) -> bool {
        if !self.rebuild_requested(requested) {
            return false;
        }
        self.rebuild = requested[&self.target.selection.dataset];
        self.recover = true;
        self.bootstrap = None;
        true
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
            batch.delete_revision(&self.pending_confirmation);
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
            ..Default::default()
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

    /// Stops holding this stream: a restart no longer resumes it.
    /// Deletes this consumer's names. Setting them to 0 instead would keep every retired name, and each commit reads them
    /// all, so a node would slow with its range history (B-61). Nothing reads a cursor without its active flag.
    fn retire(&self, batch: &mut WriteBatch) {
        batch
            .delete_revision(&self.active)
            .delete_revision(&self.revision)
            .delete_revision(&self.pending_confirmation);
        if let Some(marker) = &self.marker {
            batch.delete_revision(marker);
        }
    }

    /// Retires cursors on parts of this range. A split that committed but reported failure leaves them behind while this
    /// consumer carries on, and this batch moves the data past what they claim.
    fn overtake(&self, revisions: &BTreeMap<String, u64>, batch: &mut WriteBatch) {
        let Some(span) = self.target.span else {
            return;
        };
        let s = &self.target.selection;
        let prefix = format!("relay:range:{}:", s.dataset);
        for rest in revisions
            .iter()
            .filter(|(_, value)| **value == 1)
            .filter_map(|(name, _)| name.strip_prefix(&prefix))
        {
            let (Some(part), Some(namespace)) = (marker_range(rest), rest.get(34..)) else {
                continue;
            };
            if namespace == s.namespace && part != span && ranges::within(part, span) {
                let names = Names::of(&s.dataset, namespace, &[], Some(part));
                batch
                    .delete_revision(&names.active)
                    .delete_revision(&names.revision)
                    .delete_revision(&names.pending);
                if let Some(marker) = &names.marker {
                    batch.delete_revision(marker);
                }
            }
        }
    }

    /// Records that `range`, a part of this consumer's range a split kept, is held: its data stays either way.
    fn mark(&self, batch: &mut WriteBatch, range: Range) {
        let s = &self.target.selection;
        let names = Names::of(&s.dataset, &s.namespace, &[], Some(range));
        if let Some(marker) = &names.marker {
            batch.set_revision(marker, 1);
        }
    }

    /// Hands this range consumer's coverage to `range`, a part of its range a split kept: every revision applied for
    /// the whole range is applied for the part. Unvalidated progress is not handed on; the part bootstraps.
    fn carry(&self, batch: &mut WriteBatch, range: Range) {
        self.mark(batch, range);
        let s = &self.target.selection;
        let names = Names::of(&s.dataset, &s.namespace, &[], Some(range));
        if self.validated() {
            batch
                .set_revision(&names.active, 1)
                .set_revision(&names.revision, self.applied)
                .delete_revision(&names.pending);
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
        assert!(self.bootstrap.is_none(), "committed during a bootstrap");
        // Only a recovery, which abandoned its coverage, may land behind it.
        assert!(
            self.recover || boundary >= self.applied,
            "applied revision moved backwards"
        );
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
            batch.delete_revision(name);
        }
        self.overtake(&revisions, &mut batch);
        if let Some(marker) = &self.marker {
            batch.set_revision(marker, 1);
        }
        batch
            .set_revision(&self.active, 1)
            .set_revision(&self.revision, boundary);
        if self.awaiting_confirmation {
            batch.set_revision(&self.pending_confirmation, 1);
        } else {
            batch.delete_revision(&self.pending_confirmation);
        }
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

    /// Applies ordered selected coverage. Returns true for applied or already-applied coverage and false for a gap,
    /// so the caller can reissue Follow at the committed cursor. Invalid coverage returns a data-loss status; storage
    /// failures return an internal status.
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

    /// Validates bootstrap chunks and atomically replaces the selected keys at completion. Returns false until the
    /// final chunk and true after installation. Invalid chunks or checksums return a data-loss status; storage
    /// failures return an internal status.
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

/// This process's current entry; its status is `draining` once [`Session::drain`] starts.
type Membership = Arc<Watched<pb::Member>>;

fn draining(member: &pb::Member) -> bool {
    member.status == pb::member::Status::Draining as i32
}

/// The entry once it differs from what the relay has, or at once while draining.
async fn announce(member: Option<&Membership>, sent: Option<pb::Member>) -> pb::Member {
    match member {
        Some(member) => {
            member
                .wait_until(|m| draining(m) || Some(m) != sent.as_ref())
                .await
        }
        None => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    peers: Arc<Peers>,
    upstreams: Vec<String>,
    mut consumers: BTreeMap<Scope, Consumer>,
    mut pushed: Pushed,
    progress: Arc<Mutex<Progress>>,
    changes: Arc<Watched<u64>>,
    rebuilds: Rebuilds,
    member: Option<Membership>,
) {
    let drained = || member.as_ref().is_some_and(|m| draining(&m.get()));
    let mut last = None;
    loop {
        for upstream in peers.health().route(&upstreams) {
            if drained() {
                return;
            }
            let requested = rebuilds.get();
            for consumer in consumers.values_mut() {
                consumer.reconnect();
                consumer.start_rebuild(&requested);
            }
            changed(&progress, &changes, |state| {
                state.connected = false;
                for (scope, (_, head, validated)) in state.scopes.iter_mut() {
                    *head = None;
                    *validated = consumers[scope].validated();
                }
            });
            // A new session starts with the relay's current set.
            pushed.pending.clear();
            pushed.retired.clear();
            let error = connection(
                &peers,
                upstream,
                &mut consumers,
                &mut pushed,
                &progress,
                &changes,
                &rebuilds,
                member.as_ref(),
                &mut last,
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

#[allow(clippy::too_many_arguments)]
async fn connection(
    peers: &Peers,
    upstream: &str,
    consumers: &mut BTreeMap<Scope, Consumer>,
    pushed: &mut Pushed,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
    rebuilds: &Rebuilds,
    member: Option<&Membership>,
    last: &mut Option<String>,
) -> Status {
    match connected(
        peers, upstream, consumers, pushed, progress, changes, rebuilds, member, last,
    )
    .await
    {
        Ok(()) => Status::unavailable("relay stream ended"),
        Err(error) => error,
    }
}

#[allow(clippy::too_many_arguments)]
async fn connected(
    peers: &Peers,
    upstream: &str,
    consumers: &mut BTreeMap<Scope, Consumer>,
    pushed: &mut Pushed,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
    rebuilds: &Rebuilds,
    member: Option<&Membership>,
    last: &mut Option<String>,
) -> Result<(), Status> {
    let (requests, receiver) = mpsc::channel(16);
    let mut sent = member.map(|m| m.get());
    send(
        &requests,
        pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                tier: 0,
                member: sent.clone(),
                moved: last.as_deref().is_some_and(|last| last != upstream),
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
    *last = Some(upstream.to_owned());
    for consumer in consumers.values().filter(|c| c.target.span.is_none()) {
        send(&requests, consumer.request()).await?;
    }
    // One follow per range; the relay starts every namespace, each where its consumer left off.
    for (dataset, range) in pushed.groups.keys() {
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
            ..Default::default()
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
    loop {
        // Keep this read's deadline when sending progress; outbound activity cannot prove the source is alive.
        let frame = {
            let mut reading =
                std::pin::pin!(peers.call(upstream, Class::Control, IDLE, stream.message()));
            loop {
                tokio::select! {
                    biased;
                    update = announce(member, sent.clone()) => {
                        if draining(&update) {
                            break None;
                        }
                        send(&requests, pb::RelayRequest {
                            kind: Some(pb::relay_request::Kind::Member(update.clone())),
                        }).await?;
                        sent = Some(update);
                    },
                    _ = tick.tick() => {
                        send(&requests, pb::RelayRequest {
                            kind: Some(pb::relay_request::Kind::Progress(true)),
                        }).await?;
                        for scope in intake.due(consumers) {
                            send(&requests, consumers.get(&scope).unwrap().request()).await?;
                        }
                    },
                    requested = rebuilds.wait_until(|r| consumers.values().any(|c| c.rebuild_requested(r))) => {
                        for (scope, consumer) in consumers.iter_mut() {
                            if !consumer.start_rebuild(&requested) {
                                continue;
                            }
                            // Drain the current subscription like after a local failure, and recover at once.
                            intake.fail(scope, consumer, Status::aborted("local storage is rebuilding"));
                            intake.retries.remove(scope);
                            changed(progress, changes, |state| {
                                state.scopes.get_mut(scope).unwrap().2 = false;
                            });
                            send(&requests, consumer.request()).await?;
                        }
                    },
                    frame = &mut reading => break Some(frame?),
                }
            }
        };
        let Some(frame) = frame else {
            send(
                &requests,
                pb::RelayRequest {
                    kind: Some(pb::relay_request::Kind::Member(member.unwrap().get())),
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
            Some(pb::relay_frame::Kind::Ranges(ranges)) => {
                pushed.pending.insert(ranges.dataset.clone(), ranges);
                settle(&requests, consumers, pushed, progress, changes).await?;
                intake.forget(consumers);
                continue;
            }
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
            && let Some(engine) = pushed.groups.get(&(dataset.clone(), *range))
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
        let Some(consumer) = consumers.get_mut(&scope) else {
            if let (dataset, _, Some(range)) = &scope
                && pushed.retired.contains(&(dataset.clone(), *range))
            {
                continue;
            }
            return Err(Status::data_loss("unsolicited relay scope"));
        };
        let effect = intake.apply(&scope, consumer, action)?;
        changed(progress, changes, |state| {
            let entry = state.scopes.get_mut(&scope).unwrap();
            entry.0 = consumer.applied;
            entry.2 = consumer.validated();
            if entry.2 {
                state.rebuilt.insert(scope.clone(), consumer.rebuild);
            }
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
        // The membership view may have reached a pushed set's revision.
        if !pushed.pending.is_empty() {
            settle(&requests, consumers, pushed, progress, changes).await?;
            intake.forget(consumers);
        }
    }
}

/// Applies each pushed range set whose revision the member's own directory view has reached: follows new ranges,
/// narrows split ones, drops revoked ones the view confirms and refuses the rest, then acknowledges the set. The
/// relay counts acknowledgements toward readiness only after that (`docs/spec/handoff.tla`, `ReadyFor`).
async fn settle(
    requests: &mpsc::Sender<pb::RelayRequest>,
    consumers: &mut BTreeMap<Scope, Consumer>,
    pushed: &mut Pushed,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
) -> Result<(), Status> {
    let Some((node, view)) = pushed.me.clone() else {
        pushed.pending.clear();
        return Ok(());
    };
    let Some(membership) = consumers.get(&view).filter(|c| c.validated()) else {
        return Ok(());
    };
    let due: Vec<String> = pushed
        .pending
        .iter()
        .filter(|(_, frame)| frame.revision <= membership.applied)
        .map(|(dataset, _)| dataset.clone())
        .collect();
    if due.is_empty() {
        return Ok(());
    }
    let (members, directories) =
        crate::membership::read(membership.target.engine.as_ref(), &membership.target.prefix)?;
    for dataset in due {
        let frame = pushed.pending.remove(&dataset).unwrap();
        let mut refused = Vec::new();
        let mut reasons = Vec::new();
        if let Some(engine) = pushed.ranged.get(&dataset).cloned() {
            let held: Vec<Range> = pushed
                .groups
                .keys()
                .filter(|(d, _)| *d == dataset)
                .map(|(_, range)| *range)
                .collect();
            // A range unreadable when the session started has no group, and only this can still drop it. A failure here
            // reconnects for a new push; a rebuild empties storage that stays unreadable.
            let stored: Vec<Range> = self::held(engine.as_ref(), &dataset)?.into_iter().collect();
            let plan = ranges::plan(&held, &stored, &frame.spans)?;
            for change in &plan.changed {
                let unhealthy = members
                    .values()
                    .filter(|member| member.unhealthy.contains(&dataset))
                    .map(|member| member.node_id.clone())
                    .collect();
                let confirmed =
                    ranges::confirm(directories.get(&dataset), &node, change, &unhealthy);
                let confirmed = match confirmed {
                    Ok(confirmed) => confirmed,
                    Err(reason) => {
                        tracing::warn!(%dataset, held = ?change.held, %reason, "refused a revoke the directory view does not confirm");
                        refused.extend(change.dropped.iter().filter_map(|d| span(Some(*d))));
                        reasons.push(reason);
                        continue;
                    }
                };
                for kind in narrow(
                    &dataset, &engine, confirmed, consumers, pushed, progress, changes,
                )? {
                    send(requests, pb::RelayRequest { kind: Some(kind) }).await?;
                }
            }
            for range in plan.added {
                pushed
                    .groups
                    .insert((dataset.clone(), range), engine.clone());
                let follow = pb::Follow {
                    dataset: dataset.clone(),
                    namespace: String::new(),
                    from: 0,
                    span: span(Some(range)),
                    from_namespaces: BTreeMap::new(),
                    ..Default::default()
                };
                send(
                    requests,
                    pb::RelayRequest {
                        kind: Some(pb::relay_request::Kind::Follow(follow)),
                    },
                )
                .await?;
            }
        }
        let ack = pb::RangesAck {
            dataset,
            revision: frame.revision,
            refused,
            reason: reasons.join("; "),
        };
        send(
            requests,
            pb::RelayRequest {
                kind: Some(pb::relay_request::Kind::Ranges(ack)),
            },
        )
        .await?;
    }
    Ok(())
}

/// Carries a held range's coverage to the parts a split kept and deletes the dropped parts in one atomic batch, then
/// attempts to compact them. Returns the requests that move the relay's streams to the kept parts. Write and consumer
/// initialization errors propagate; compaction errors are logged and do not fail the operation.
fn narrow(
    dataset: &str,
    engine: &Arc<dyn StorageEngine>,
    confirmed: ranges::Confirmed<'_>,
    consumers: &mut BTreeMap<Scope, Consumer>,
    pushed: &mut Pushed,
    progress: &Mutex<Progress>,
    changes: &Watched<u64>,
) -> Result<Vec<pb::relay_request::Kind>, Status> {
    let change = confirmed.change();
    let scopes: Vec<Scope> = consumers
        .keys()
        .filter(|(d, _, range)| d == dataset && *range == Some(change.held))
        .cloned()
        .collect();
    let group = Target {
        selection: Selection {
            dataset: dataset.to_owned(),
            namespace: String::new(),
        },
        engine: engine.clone(),
        prefix: Vec::new(),
        span: None,
    };
    let mut batch = WriteBatch::default();
    for &(start, end) in &change.dropped {
        batch.delete_range(l2_span(start, end));
    }
    // Left behind, stored progress would resume over deleted or replaced data. It may have no consumer yet, and an earlier
    // attempt that committed but reported failure can have left it on parts of the range, which no group follows.
    let stored = held(engine.as_ref(), dataset)?;
    let followed = |range: &Range| pushed.groups.contains_key(&(dataset.to_owned(), *range));
    for range in stored.into_iter().filter(|range| {
        *range == change.held || (ranges::overlaps(*range, change.held) && !followed(range))
    }) {
        for consumer in Consumer::resumed(&group, range)? {
            consumer.retire(&mut batch);
            for &kept in &change.kept {
                consumer.mark(&mut batch, kept);
            }
        }
    }
    for scope in &scopes {
        let consumer = &consumers[scope];
        consumer.retire(&mut batch);
        for &kept in &change.kept {
            consumer.carry(&mut batch, kept);
        }
    }
    if let Err(error) = engine.write(batch) {
        // The batch may have committed, so these cursors may now describe deleted data.
        for scope in &scopes {
            consumers.get_mut(scope).unwrap().distrust();
        }
        return Err(storage(error));
    }
    for &(start, end) in &change.dropped {
        // ponytail: compacts inline on the session task; move it to a blocking task if it stalls deliveries.
        if let Err(error) = engine.compact(&l2_span(start, end)) {
            tracing::warn!(%error, "compacting a dropped range failed; its space is reclaimed by later compaction");
        }
    }
    pushed.groups.remove(&(dataset.to_owned(), change.held));
    pushed.retired.insert((dataset.to_owned(), change.held));
    for scope in &scopes {
        consumers.remove(scope);
    }
    changed(progress, changes, |state| {
        for scope in &scopes {
            state.scopes.remove(scope);
        }
    });
    let mut requests = vec![pb::relay_request::Kind::Unfollow(pb::Unfollow {
        dataset: dataset.to_owned(),
        namespace: String::new(),
        span: span(Some(change.held)),
    })];
    for &kept in &change.kept {
        // A stored range can surround a part already followed.
        if pushed.groups.contains_key(&(dataset.to_owned(), kept)) {
            continue;
        }
        pushed
            .groups
            .insert((dataset.to_owned(), kept), engine.clone());
        let mut from_namespaces = BTreeMap::new();
        for (_, namespace, _) in &scopes {
            let consumer = Consumer::in_range(&group, namespace, kept)?;
            from_namespaces.insert(namespace.clone(), consumer.resume());
            let scope = consumer.scope();
            changed(progress, changes, |state| {
                state.scopes.insert(
                    scope.clone(),
                    (consumer.applied, None, consumer.validated()),
                );
            });
            consumers.insert(scope, consumer);
        }
        requests.push(pb::relay_request::Kind::Follow(pb::Follow {
            dataset: dataset.to_owned(),
            namespace: String::new(),
            from: 0,
            span: span(Some(kept)),
            from_namespaces,
            ..Default::default()
        }));
    }
    Ok(requests)
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
    /// Drops the failures and retries of scopes a revoke or split removed while they were pending.
    fn forget(&mut self, consumers: &BTreeMap<Scope, Consumer>) {
        self.failed.retain(|scope| consumers.contains_key(scope));
        self.retries.retain(|scope| consumers.contains_key(scope));
    }

    /// Takes the retries to send now. A quarantined engine refuses every install until its rebuild starts, and starting
    /// it requests again, so those wait: each request would hold one of the relay's few recovery slots for nothing,
    /// starving other recoveries, including this node's rebuild of another dataset.
    fn due(&mut self, consumers: &BTreeMap<Scope, Consumer>) -> BTreeSet<Scope> {
        let (due, held) = std::mem::take(&mut self.retries)
            .into_iter()
            .partition(|scope| consumers[scope].target.engine.health().is_ok());
        self.retries = held;
        due
    }

    fn fail(&mut self, scope: &Scope, consumer: &mut Consumer, error: Status) -> Effect {
        consumer.distrust();
        let mut batch = WriteBatch::default();
        batch.set_revision(&consumer.pending_confirmation, 1);
        // A failed engine may reject this marker too; the in-process gate still isolates that scope.
        let _ = consumer.target.engine.write(batch);
        self.failed.insert(scope.clone());
        self.retries.insert(scope.clone());
        assert!(!consumer.validated(), "a failed scope still counts");
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
            Err(error) if local(&error) => Ok(self.fail(scope, consumer, error)),
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
