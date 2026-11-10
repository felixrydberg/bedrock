//! Cell membership from relay subscriber streams (#15 / B-12, `docs/spec/membership.md`).
//!
//! A `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process announces a [`pb::Member`] on its leaf relay session. Cell relays watch
//! each other's connected lists over the sibling stream ([`crate::relays::siblings`]). The cell's embedded Raft leader
//! writes the union to dataset [`DATASET`], namespace `{service}.{cell}`, on a durable local quorum. Nodes subscribe to that namespace
//! like any other ([`View`]). A disconnected node stays a member for the grace period, so failover to another cell
//! relay changes nothing; a drained node leaves at once. Without a majority, or before the relay has caught up with
//! the published view, nothing is written and the view freezes.
//!
//! The publisher also writes each ranged service's L2 placement directory into the same namespace (#19 / B-16, §4
//! decision 13), so membership and placement share one revision order. It plans placement only once membership has
//! nothing left to write.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use tokio::time::Instant;
use tonic::Status;

use crate::datasets::capabilities::ranged_datasets;
use crate::node::{MembershipSettings, ServiceConfig};
use crate::placement::{self, Directory, Owner, State};
use crate::reconciliation::drain::{self, Drain};
use crate::relays::client::{Selection, Session, Target, applied_at};
pub use crate::relays::siblings::Members;
use crate::relays::siblings::{self, Siblings};
use crate::relays::{Hub, IDLE, MemberKey};
use crate::replication::pb;
use crate::storage::memory::MemoryEngine;
use crate::storage::{KeyRange, Snapshot, StorageEngine};
use crate::transport::Peers;

pub mod control;

pub const DATASET: &str = crate::datasets::capabilities::MEMBERSHIP;
/// Each service has its own namespace per cell; members of different services never mix.
pub use crate::datasets::capabilities::SERVICES;
const TIMEOUT: Duration = Duration::from_secs(5);
/// Re-plans at least this often, which is what expires grace periods.
const RECONCILE: Duration = Duration::from_secs(1);
/// Directory keys are `range/{dataset}/{start:016x}`; node IDs cannot contain `/`.
const RANGE: &str = "range/";
/// Dataset → its placement directory.
pub type Directories = BTreeMap<String, Directory>;
// ponytail: fixed until B-34 makes them per-service settings read at relay startup (CDN artifacts want 2 replicas).
const REPLICAS: usize = 3;
const MAX_MOVING: usize = 16;

pub fn namespace(service: &str, cell: &str) -> String {
    format!("{service}.{cell}")
}

pub fn key(member: &pb::Member) -> MemberKey {
    (member.service.clone(), member.node_id.clone())
}

/// Identities travel in namespaces and write URLs unescaped.
fn token(identity: &str) -> bool {
    !identity.is_empty()
        && identity
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

/// An entry a node may announce: a known service, plain identities, `ready` or `draining`, and unhealthy datasets
/// that are its own, in order and without repeats (so equal health compares equal).
pub fn validate(member: &pb::Member) -> Result<(), Status> {
    let status = pb::member::Status::try_from(member.status);
    if !SERVICES.contains(&member.service.as_str())
        || !token(&member.node_id)
        || !token(&member.cell)
        || !matches!(
            status,
            Ok(pb::member::Status::Ready | pb::member::Status::Draining)
        )
        || !member.unhealthy.is_sorted_by(|a, b| a < b)
        || !member.unhealthy.iter().all(|d| member.datasets.contains(d))
    {
        return Err(Status::invalid_argument("invalid membership entry"));
    }
    Ok(())
}

/// The entry a `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process announces.
pub fn member(config: &ServiceConfig, service: &str) -> pb::Member {
    let node = &config.node;
    pb::Member {
        node_id: node.node_id.clone(),
        service: service.into(),
        region: node.location.region.clone(),
        dc: node.location.dc.clone(),
        cell: node.location.cell.clone(),
        roles: config
            .roles
            .iter()
            .map(|role| format!("{role:?}").to_lowercase())
            .collect(),
        datasets: config.datasets.clone(),
        cpus: node.budget.cpus.unwrap_or(0.0),
        memory_mb: node.budget.memory_mb.unwrap_or(0),
        disk_mb: node.budget.disk_mb.unwrap_or(0),
        failure_domain: node.location.failure_domain.clone(),
        status: pb::member::Status::Ready as i32,
        address: node.advertised(),
        unhealthy: Vec::new(),
    }
}

/// One service's published members in a cell, held in memory; a restarted node bootstraps it again.
#[derive(Clone)]
struct Published {
    engine: Arc<MemoryEngine>,
    prefix: Vec<u8>,
}

type ServiceSnapshot = (
    BTreeMap<String, pb::Member>,
    Directories,
    Vec<(String, Drain)>,
    Option<u64>,
);

impl Published {
    fn new(service: &str, cell: &str) -> (Self, Target) {
        let engine = Arc::new(MemoryEngine::new(DATASET));
        let namespace = namespace(service, cell);
        let mut prefix = namespace.as_bytes().to_vec();
        prefix.push(0);
        let target = Target {
            selection: Selection {
                dataset: DATASET.into(),
                namespace,
            },
            engine: engine.clone(),
            prefix: prefix.clone(),
            span: None,
        };
        (Self { engine, prefix }, target)
    }

    fn read(&self) -> Result<(BTreeMap<String, pb::Member>, Directories), Status> {
        read(&*self.engine, &self.prefix)
    }

    fn validated(
        &self,
        service: &str,
        cell: &str,
        hooks: &[Arc<dyn drain::Hook>],
    ) -> Result<ServiceSnapshot, Status> {
        let snapshot = self
            .engine
            .snapshot()
            .map_err(|error| Status::internal(error.to_string()))?;
        let (members, directory) = read_snapshot(&*snapshot, &self.prefix)?;
        let records = read_drains(&*snapshot, &self.prefix)?;
        for (id, record) in &records {
            control::validate_drain(cell, service, id, record)?;
            drain::validate(hooks, record).map_err(|error| Status::data_loss(error.to_string()))?;
        }
        let revision = applied_at(
            &*snapshot,
            DATASET,
            &namespace(service, cell),
            &self.prefix,
            None,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        Ok((members, directory, records, revision))
    }

    fn directory(&self, dataset: &str) -> Result<Directory, String> {
        self.read()
            .map(|(_, mut directories)| directories.remove(dataset).unwrap_or_default())
            .map_err(|error| error.to_string())
    }

    /// The drain records written to this namespace. A record that does not decode is corruption, never a missing drain.
    #[cfg(test)]
    fn drains(&self) -> Result<Vec<(String, Drain)>, Status> {
        let snapshot = self
            .engine
            .snapshot()
            .map_err(|error| Status::internal(error.to_string()))?;
        read_drains(&*snapshot, &self.prefix)
    }

    fn members(&self) -> Result<BTreeMap<String, pb::Member>, Status> {
        let scan = self
            .engine
            .scan(&KeyRange::prefix(self.prefix.clone()))
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut members = BTreeMap::new();
        for entry in scan {
            let (key, value) = entry.map_err(|e| Status::internal(e.to_string()))?;
            let key = &key[self.prefix.len()..];
            if key.starts_with(RANGE.as_bytes()) || key.starts_with(drain::PREFIX.as_bytes()) {
                continue;
            }
            let key = String::from_utf8(key.to_vec())
                .map_err(|_| Status::data_loss("membership key is not UTF-8"))?;
            let member = pb::Member::decode(value.as_slice())
                .map_err(|e| Status::data_loss(e.to_string()))?;
            members.insert(key, member);
        }
        Ok(members)
    }
}

fn read_drains(
    snapshot: &dyn Snapshot,
    namespace_prefix: &[u8],
) -> Result<Vec<(String, Drain)>, Status> {
    let mut prefix = namespace_prefix.to_vec();
    prefix.extend_from_slice(drain::PREFIX.as_bytes());
    let scan = snapshot
        .scan(&KeyRange::prefix(prefix))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut drains = Vec::new();
    for entry in scan {
        let (key, value) = entry.map_err(|e| Status::internal(e.to_string()))?;
        let id = std::str::from_utf8(&key[namespace_prefix.len() + drain::PREFIX.len()..])
            .map_err(|_| Status::data_loss("drain key is not UTF-8"))?
            .to_owned();
        drains.push((
            id,
            Drain::decode(&value).map_err(|e| Status::data_loss(e.to_string()))?,
        ));
    }
    Ok(drains)
}

/// Node ID → entry, and the placement directories, as stored under `prefix` by a subscription to the membership
/// namespace. An undecodable entry is corruption, never a missing one.
/// Storage scan failures return an internal status; malformed keys or entries return a data-loss status.
pub(crate) fn read(
    engine: &dyn StorageEngine,
    prefix: &[u8],
) -> Result<(BTreeMap<String, pb::Member>, Directories), Status> {
    let snapshot = engine
        .snapshot()
        .map_err(|error| Status::internal(error.to_string()))?;
    read_snapshot(&*snapshot, prefix)
}

fn read_snapshot(
    snapshot: &dyn Snapshot,
    prefix: &[u8],
) -> Result<(BTreeMap<String, pb::Member>, Directories), Status> {
    let mut members = BTreeMap::new();
    let mut directories = Directories::new();
    let scan = snapshot
        .scan(&KeyRange::prefix(prefix.to_vec()))
        .map_err(|e| Status::internal(e.to_string()))?;
    for entry in scan {
        let (key, value) = entry.map_err(|e| Status::internal(e.to_string()))?;
        let key = String::from_utf8(key[prefix.len()..].to_vec())
            .map_err(|_| Status::data_loss("membership key is not UTF-8"))?;
        if key.starts_with(drain::PREFIX) {
            continue;
        }
        if let Some(range) = key.strip_prefix(RANGE) {
            let (dataset, start) = range
                .split_once('/')
                .filter(|(dataset, start)| {
                    token(dataset)
                        && start.len() == 16
                        && start
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                .ok_or_else(|| Status::data_loss("invalid range key"))?;
            let start = u64::from_str_radix(start, 16)
                .map_err(|_| Status::data_loss("invalid range key"))?;
            directories
                .entry(dataset.to_owned())
                .or_default()
                .insert(start, decode(&value)?);
            continue;
        }
        let member =
            pb::Member::decode(value.as_slice()).map_err(|e| Status::data_loss(e.to_string()))?;
        members.insert(key, member);
    }
    Ok((members, directories))
}

fn decode(value: &[u8]) -> Result<Vec<Owner>, Status> {
    use pb::range::owner::State as Pb;
    let range = pb::Range::decode(value).map_err(|e| Status::data_loss(e.to_string()))?;
    range
        .owners
        .into_iter()
        .map(|owner| {
            let state = match Pb::try_from(owner.state) {
                Ok(Pb::Populating) => State::Populating,
                Ok(Pb::Ready) => State::Ready,
                Ok(Pb::Leaving) => State::Leaving,
                Err(_) => return Err(Status::data_loss("unknown range owner state")),
            };
            Ok(Owner {
                node: owner.node_id,
                state,
            })
        })
        .collect()
}

fn encode(owners: &[Owner]) -> Vec<u8> {
    use pb::range::owner::State as Pb;
    let owners = owners.iter().map(|owner| pb::range::Owner {
        node_id: owner.node.clone(),
        state: match owner.state {
            State::Populating => Pb::Populating,
            State::Ready => Pb::Ready,
            State::Leaving => Pb::Leaving,
        } as i32,
    });
    pb::Range {
        owners: owners.collect(),
    }
    .encode_to_vec()
}

/// A node's replicated view of its own service's members in its cell.
pub struct View {
    published: Published,
    namespace: String,
    seen: Mutex<(Option<u64>, BTreeMap<String, pb::Member>)>,
}

impl View {
    /// The view and the relay subscription that fills it; pass the target to the process's one session.
    pub fn new(service: &str, cell: &str) -> (Self, Target) {
        let (published, target) = Published::new(service, cell);
        let view = Self {
            published,
            namespace: namespace(service, cell),
            seen: Mutex::default(),
        };
        (view, target)
    }

    /// Node ID → entry, as of the last applied revision. Without relays the last view stays.
    pub fn members(&self) -> Result<BTreeMap<String, pb::Member>, Status> {
        self.published.members()
    }

    /// One dataset's L2 placement directory in this cell: route with [`placement::owners`], and serve only what
    /// [`placement::serves`] allows. Empty when the dataset has no directory, including for customer KV and before
    /// the first write. Scan and decoding errors from the membership view are returned to the caller.
    pub fn directory(&self, dataset: &str) -> Result<Directory, Status> {
        Ok(self.published.read()?.1.remove(dataset).unwrap_or_default())
    }

    /// When the view has advanced, drops connections and breakers of members that left.
    pub fn refresh(&self, session: &Session, peers: &Peers) -> Result<(), Status> {
        let applied = session.applied(DATASET, &self.namespace);
        let mut seen = self.seen.lock().unwrap();
        if applied.is_none() || applied == seen.0 {
            return Ok(());
        }
        let members = self.members()?;
        for (id, member) in &seen.1 {
            if !members.contains_key(id) {
                peers.forget(&member.address);
            }
        }
        *seen = (applied, members);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
// ponytail: a pass plans a handful of changes, so boxing `Set` would save nothing measurable.
#[allow(clippy::large_enum_variant)]
pub enum Change {
    Set(pb::Member),
    Delete(MemberKey),
    Range {
        service: String,
        dataset: String,
        start: u64,
        owners: Vec<Owner>,
    },
    /// Deletes the range entry at `start`, joining its range to the one before (#66 / B-61, [`placement::merges`]).
    Merge {
        service: String,
        dataset: String,
        start: u64,
    },
    Drain {
        service: String,
        id: String,
        record: Drain,
    },
}

impl Change {
    fn service(&self) -> &str {
        match self {
            Change::Set(member) => &member.service,
            Change::Delete((service, _))
            | Change::Range { service, .. }
            | Change::Merge { service, .. }
            | Change::Drain { service, .. } => service,
        }
    }
}

/// Writes a plan in order from the revisions in `based`, each write building on the last, and returns the latest revision
/// written per service. Only a prefix of a plan may land: a later write alone can leave part of the hash space unowned. A
/// failed write may never have reached the API, so its base would still pass; it ends its service's plan, and the next pass
/// replans from the view.
async fn commit<F: Future<Output = Option<u64>>>(
    changes: Vec<Change>,
    mut based: BTreeMap<String, u64>,
    mut write: impl FnMut(Change, u64) -> F,
) -> BTreeMap<String, u64> {
    let mut written = BTreeMap::new();
    for change in changes {
        let service = change.service().to_owned();
        let Some(&base) = based.get(&service) else {
            continue;
        };
        match write(change, base).await {
            Some(revision) => {
                based.insert(service.clone(), revision);
                written.insert(service, revision);
            }
            None => {
                based.remove(&service);
            }
        }
    }
    written
}

/// What one cell relay sees: its own leaf members and each sibling's synced list, `None` while unreachable.
pub struct Observed<'a> {
    pub me: &'a str,
    /// Every relay in the cell: `me` and its siblings.
    pub relays: &'a [String],
    pub cell: &'a str,
    pub local: &'a Members,
    pub remote: &'a BTreeMap<String, Option<Members>>,
}

/// The desired-vs-published diff of `docs/spec/membership.tla`. Advances this relay's absence clocks, then returns the
/// writes only with a live majority of the cell's relays. The caller must hold Raft leadership before committing it.
pub fn plan(
    seen: &Observed,
    actual: &Members,
    absent: &mut BTreeMap<MemberKey, Instant>,
    now: Instant,
    grace: Duration,
) -> Option<Vec<Change>> {
    let mut held = Members::new();
    let mut left = BTreeSet::new();
    // Local first, then siblings by address: a duplicate identity resolves the same way on every pass.
    for list in std::iter::once(seen.local).chain(seen.remote.values().flatten()) {
        for (key, member) in list {
            if member.cell != seen.cell || !SERVICES.contains(&member.service.as_str()) {
                continue;
            }
            if member.status == pb::member::Status::Left as i32 {
                left.insert(key.clone());
            } else {
                held.entry(key.clone()).or_insert_with(|| member.clone());
            }
        }
    }
    absent.retain(|key, _| actual.contains_key(key) && !held.contains_key(key));
    for key in actual.keys().filter(|key| !held.contains_key(*key)) {
        absent.entry(key.clone()).or_insert(now);
    }
    if !majority(seen) {
        return None;
    }
    let mut changes: Vec<_> = held
        .iter()
        .filter(|(key, member)| actual.get(*key) != Some(*member))
        .map(|(_, member)| Change::Set(member.clone()))
        .collect();
    changes.extend(
        actual
            .keys()
            .filter(|key| {
                !held.contains_key(*key)
                    && (left.contains(*key) || now.duration_since(absent[*key]) >= grace)
            })
            .map(|key| Change::Delete(key.clone())),
    );
    Some(changes)
}

/// Followers still observe reconnects and restart absence clocks before they can publish again.
fn publication_plan(
    seen: &Observed,
    actual: &Members,
    absent: &mut BTreeMap<MemberKey, Instant>,
    now: Instant,
    grace: Duration,
    leader: bool,
) -> Option<Vec<Change>> {
    let changes = plan(seen, actual, absent, now, grace);
    if leader { changes } else { None }
}

/// This relay and its synced siblings.
fn live<'a>(seen: &Observed<'a>) -> impl Iterator<Item = &'a str> {
    std::iter::once(seen.me).chain(
        seen.remote
            .iter()
            .filter(|(_, list)| list.is_some())
            .map(|(relay, _)| relay.as_str()),
    )
}

/// A failed service view must not create changes from live observations or retain stale absence clocks.
fn retain_readable(
    readable: &BTreeSet<&str>,
    local: &mut Members,
    remote: &mut BTreeMap<String, Option<Members>>,
    absent: &mut BTreeMap<MemberKey, Instant>,
) {
    local.retain(|(service, _), _| readable.contains(service.as_str()));
    for members in remote.values_mut().flatten() {
        members.retain(|(service, _), _| readable.contains(service.as_str()));
    }
    absent.retain(|(service, _), _| readable.contains(service.as_str()));
}

fn validated_revision(snapshot: Option<u64>, applied: Option<u64>, empty: bool) -> Option<u64> {
    match (snapshot, applied) {
        (Some(snapshot), Some(applied)) if snapshot <= applied => Some(snapshot),
        // A fresh validated empty stream has no persisted cursor until its first event or bootstrap.
        (None, Some(0)) if empty => Some(0),
        _ => None,
    }
}

/// Whether this relay's live side is a strict majority of the cell's relays; without one, nothing is written.
pub fn majority(seen: &Observed) -> bool {
    live(seen).count() * 2 > seen.relays.len()
}

/// One service's directory writes from the published `actual` members, per dataset it places, over the members
/// carrying that dataset: complete the handoffs `ready` reports, otherwise assign (`docs/spec/membership.tla` and
/// `handoff.tla`, `Write`).
pub fn place(
    service: &str,
    actual: &Members,
    directories: &Directories,
    ready: &siblings::Ready,
    drains: &[Drain],
) -> Vec<Change> {
    let mut changes = Vec::new();
    for dataset in ranged_datasets(service) {
        let members: BTreeMap<_, _> = actual
            .iter()
            .filter(|((s, _), member)| s == service && member.datasets.iter().any(|d| d == dataset))
            .map(|((_, id), member)| (id.clone(), member.clone()))
            .collect();
        let empty = Directory::new();
        let directory = directories.get(dataset).unwrap_or(&empty);
        let ready: BTreeSet<(String, u64)> = ready
            .iter()
            .filter(|(((s, _), d), _)| s == service && d == dataset)
            .flat_map(|(((_, node), _), spans)| {
                spans.iter().map(|(start, _)| (node.clone(), *start))
            })
            .collect();
        // Storage trouble in a dataset takes no new ranges of it (B-14). Such members keep what they hold for B-22 to
        // rebuild; sources skip them meanwhile. Their copy may be damaged, so it also cannot release a healthy one.
        let unhealthy: BTreeSet<String> = members
            .iter()
            .filter(|(_, m)| m.unhealthy.iter().any(|d| d == dataset))
            .map(|(id, _)| id.clone())
            .collect();
        let healthy = members
            .iter()
            .filter(|(id, _)| !unhealthy.contains(*id))
            .map(|(id, member)| (id.clone(), member.clone()))
            .collect();
        let mut entries = placement::handoff(directory, &healthy, &ready);
        if entries.is_empty() {
            // A drain's members are `draining`, and its rate bounds the handoffs (#68 / B-21b).
            let drained = drain::apply(&members, drains, dataset);
            let moving = drain::moving(drains, MAX_MOVING);
            entries = placement::assign(directory, &drained, &unhealthy, REPLICAS, moving);
        }
        // Merges wait for a pass with nothing else to write, so they never share a plan with a split.
        let merges = if entries.is_empty() {
            placement::merges(directory)
        } else {
            Vec::new()
        };
        changes.extend(entries.into_iter().map(|(start, owners)| Change::Range {
            service: service.into(),
            dataset: dataset.into(),
            start,
            owners,
        }));
        changes.extend(merges.into_iter().map(|start| Change::Merge {
            service: service.into(),
            dataset: dataset.into(),
            start,
        }));
    }
    changes
}

/// The ranges `node` is listed in, in any state.
fn owned(directory: &Directory, node: &str) -> Vec<pb::Span> {
    let mut entries = directory.iter().peekable();
    let mut spans = Vec::new();
    while let Some((&start, owners)) = entries.next() {
        if owners.iter().any(|o| o.node == node) {
            let end = entries.peek().map_or(0, |(next, _)| **next);
            spans.push(pb::Span { start, end });
        }
    }
    spans
}

/// A placement view and the exact membership revision captured with it.
struct DirectoryAt {
    revision: u64,
    directories: Directories,
}

impl DirectoryAt {
    fn ranges(&self, dataset: &str, node: &str) -> pb::Ranges {
        let spans = self
            .directories
            .get(dataset)
            .map(|directory| owned(directory, node))
            .unwrap_or_default();
        pb::Ranges {
            dataset: dataset.into(),
            revision: self.revision,
            spans,
        }
    }
}

/// What a cell relay's publisher sees and does, for relay introspection (#17 / B-18b).
pub struct Publisher {
    published: Vec<(&'static str, Published)>,
    frozen_since: Mutex<Option<Instant>>,
    directory_writes: Mutex<BTreeMap<String, u64>>,
}

impl Publisher {
    /// Each service's published members and per-dataset placement directories, as this relay's subscription applied
    /// them. Returns scan or decoding errors from the membership views.
    #[allow(clippy::type_complexity)]
    pub fn views(
        &self,
    ) -> Result<Vec<(&'static str, BTreeMap<String, pb::Member>, Directories)>, Status> {
        self.published
            .iter()
            .map(|(service, view)| {
                view.read()
                    .map(|(members, directory)| (*service, members, directory))
            })
            .collect()
    }

    /// How long this relay has been without a live majority, so membership and placement are frozen.
    pub fn frozen(&self) -> Option<Duration> {
        self.frozen_since
            .lock()
            .unwrap()
            .map(|since| crate::monotonic_now().duration_since(since))
    }

    /// Successful placement-directory writes by service.
    pub fn directory_writes(&self) -> BTreeMap<String, u64> {
        self.directory_writes.lock().unwrap().clone()
    }
}

/// A cell relay's membership publisher.
pub struct Cell {
    publisher: Arc<Publisher>,
    hub: Hub,
    siblings: Arc<Siblings>,
    me: String,
    relays: Vec<String>,
    cell: String,
    grace: Duration,
    control: Arc<control::Control>,
    drain_hooks: BTreeMap<String, Vec<Arc<dyn drain::Hook>>>,
}

impl Cell {
    /// Publishes from this relay's members and its watched siblings; abort the returned task on shutdown. `me` is this
    /// relay's advertised address, as its siblings list it. The [`Publisher`] serves introspection.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        hub: Hub,
        peers: Arc<Peers>,
        siblings: Arc<Siblings>,
        settings: &MembershipSettings,
        me: String,
        cell: String,
        control: Arc<control::Control>,
    ) -> Result<(tokio::task::JoinHandle<()>, Arc<Publisher>), Status> {
        let mut published = Vec::new();
        let mut targets = Vec::new();
        let mut drain_hooks = BTreeMap::new();
        for service in SERVICES {
            let (view, target) = Published::new(service, &cell);
            let hooks = drain::ranged_with(service, {
                let view = view.clone();
                move |dataset| view.directory(dataset)
            });
            drain_hooks.insert(service.to_owned(), hooks);
            published.push((service, view));
            targets.push(target);
        }
        // Bootstrap and delivery come from this relay's durable cell authority, never an upstream service.
        let session = Session::start(peers, vec![me.clone()], targets)?;
        let relays = std::iter::once(me.clone())
            .chain(siblings.state().into_keys())
            .collect();
        let publisher = Arc::new(Publisher {
            published,
            frozen_since: Mutex::default(),
            directory_writes: Mutex::default(),
        });
        let cell = Self {
            publisher: publisher.clone(),
            hub,
            siblings,
            me,
            relays,
            cell,
            grace: Duration::from_millis(settings.grace_ms),
            control,
            drain_hooks,
        };
        Ok((
            tokio::spawn(async move { cell.publish(session).await }),
            publisher,
        ))
    }

    async fn publish(&self, session: Session) {
        let published = &self.publisher.published;
        let version = self.hub.member_version();
        let mut absent = BTreeMap::new();
        // After a successful write, plan again at once: placement takes a pass per step.
        let mut again = false;
        loop {
            let (members_at, siblings_at) = (version.get(), self.siblings.version());
            if !std::mem::take(&mut again) {
                tokio::select! {
                    _ = version.wait_until(|v| *v != members_at) => {},
                    _ = self.siblings.changed(siblings_at) => {},
                    _ = tokio::time::sleep(RECONCILE) => {},
                }
            }
            // Writing from a stale view could resurrect or remove the wrong entries; freeze instead.
            if !session.ready() {
                // Reconnects may pass unseen while the local view is recovering; restart grace conservatively.
                absent.clear();
                continue;
            }
            // Read each directory, its drains and its cursor from one applied snapshot.
            let mut based = BTreeMap::new();
            let mut actual = Members::new();
            let mut directories = Vec::new();
            let mut drains = BTreeMap::new();
            let mut drain_records = BTreeMap::new();
            let mut readable = BTreeSet::new();
            for (service, view) in published {
                let hooks = self
                    .drain_hooks
                    .get(*service)
                    .map_or(&[][..], Vec::as_slice);
                let (members, directory, records, revision) = match view
                    .validated(service, &self.cell, hooks)
                {
                    Ok(read) => read,
                    Err(error) => {
                        tracing::warn!(%error, service, "published service unreadable; not publishing it");
                        continue;
                    }
                };
                let applied = session.applied(DATASET, &namespace(service, &self.cell));
                let empty = members.is_empty() && directory.is_empty() && records.is_empty();
                let Some(revision) = validated_revision(revision, applied, empty) else {
                    continue;
                };
                based.insert((*service).to_owned(), revision);
                readable.insert(*service);
                drains.insert(
                    *service,
                    records.iter().map(|(_, record)| record.clone()).collect(),
                );
                drain_records.insert(*service, records);
                for (id, member) in members {
                    actual.insert(((*service).into(), id), member);
                }
                directories.push((
                    *service,
                    DirectoryAt {
                        revision,
                        directories: directory,
                    },
                ));
            }
            let mut local = self.hub.members();
            let mut remote = self.siblings.state();
            retain_readable(&readable, &mut local, &mut remote, &mut absent);
            self.push(&local, &directories);
            let seen = Observed {
                me: &self.me,
                relays: &self.relays,
                cell: &self.cell,
                local: &local,
                remote: &remote,
            };
            {
                let mut frozen = self.publisher.frozen_since.lock().unwrap();
                match (majority(&seen), *frozen) {
                    (true, _) => *frozen = None,
                    (false, None) => *frozen = Some(crate::monotonic_now()),
                    (false, Some(_)) => {}
                }
            }
            // Every relay observes membership and pushes its committed view; only Raft's leader writes.
            let term = self.control.term();
            let Some(mut changes) = publication_plan(
                &seen,
                &actual,
                &mut absent,
                crate::monotonic_now(),
                self.grace,
                self.control.is_leader(),
            ) else {
                continue;
            };
            // Placement plans only from applied membership.
            if changes.is_empty() {
                // Readiness comes from each member's holding relay: this one, or a live sibling.
                let mut ready = self.hub.ready();
                ready.extend(self.siblings.ready());
                for (service, snapshot) in &directories {
                    let drains = drains.get(service).map_or(&[][..], Vec::as_slice);
                    changes.extend(place(
                        service,
                        &actual,
                        &snapshot.directories,
                        &ready,
                        drains,
                    ));
                }
            }
            if changes.is_empty() {
                for (service, records) in &drain_records {
                    let hooks = self
                        .drain_hooks
                        .get(*service)
                        .map_or(&[][..], Vec::as_slice);
                    for (id, record) in records {
                        match drain::completed(hooks, record) {
                            Ok(Some(record)) => changes.push(Change::Drain {
                                service: (*service).to_owned(),
                                id: id.clone(),
                                record,
                            }),
                            Ok(None) => {}
                            Err(error) => {
                                tracing::warn!(%error, service, id, "drain progress unavailable");
                            }
                        }
                    }
                }
            }
            let written = commit(changes, based, |change, base| async move {
                tracing::debug!(relay = %self.me, base, ?change, "membership write");
                match tokio::time::timeout(TIMEOUT, self.write(&change, base, term)).await {
                    Ok(Ok(revision)) => {
                        if matches!(change, Change::Range { .. } | Change::Merge { .. }) {
                            *self
                                .publisher
                                .directory_writes
                                .lock()
                                .unwrap()
                                .entry(change.service().to_owned())
                                .or_default() += 1;
                        }
                        Some(revision)
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(?change, %error, "membership write failed");
                        None
                    }
                    Err(_) => {
                        tracing::warn!(?change, "membership write timed out");
                        None
                    }
                }
            })
            .await;
            // The next plan must see these writes, or it would repeat them.
            again = !written.is_empty();
            for (service, revision) in written {
                let namespace = namespace(&service, &self.cell);
                let _ = tokio::time::timeout(IDLE, session.wait_for(DATASET, &namespace, revision))
                    .await;
            }
        }
    }

    /// Pushes each of this relay's leaf members in the cell the ranges it owns of each dataset its service places,
    /// from the published directory at the revision applied (#22 / B-21).
    fn push(&self, local: &Members, directories: &[(&str, DirectoryAt)]) {
        for ((service, node), member) in local {
            let Some((_, published)) = directories.iter().find(|(s, _)| s == service) else {
                continue;
            };
            if member.cell != self.cell {
                continue;
            }
            for dataset in ranged_datasets(service) {
                let frame = published.ranges(dataset, node);
                self.hub
                    .push_ranges(&(service.clone(), node.clone()), frame);
            }
        }
    }

    /// Commits a plan through the cell quorum, fenced by its original service revision and Raft leadership term.
    async fn write(&self, change: &Change, base: u64, term: u64) -> Result<u64, String> {
        let ((service, key), value) = match change {
            Change::Set(member) => (key(member), Some(member.encode_to_vec())),
            Change::Delete(key) => (key.clone(), None),
            Change::Range {
                service,
                dataset,
                start,
                owners,
            } => (
                (service.clone(), format!("{RANGE}{dataset}/{start:016x}")),
                Some(encode(owners)),
            ),
            Change::Merge {
                service,
                dataset,
                start,
            } => (
                (service.clone(), format!("{RANGE}{dataset}/{start:016x}")),
                None,
            ),
            Change::Drain {
                service,
                id,
                record,
            } => ((service.clone(), Drain::key(id)), Some(record.encode())),
        };
        self.control
            .write(pb::ControlWrite {
                namespace: namespace(&service, &self.cell),
                key: key.into_bytes(),
                value,
                expected: Some(base),
                idempotency_key: String::new(),
                expected_term: term,
            })
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
#[path = "../tests/unit/membership.rs"]
mod tests;
