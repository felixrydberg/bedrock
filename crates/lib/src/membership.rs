//! Cell membership from relay subscriber streams (#15 / B-12, `docs/spec/membership.md`).
//!
//! A `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process announces a [`pb::Member`] on its leaf relay session. Cell relays watch
//! each other's connected lists over the sibling stream ([`crate::relays::siblings`]). The cell's embedded Raft leader
//! writes the union to dataset [`DATASET`], namespace `{service}.{cell}`, on a durable local quorum. Nodes subscribe to that namespace
//! like any other ([`View`]). A disconnected node stays a member for the grace period, so failover to another cell
//! relay changes nothing; a drained node leaves at once. Without a majority, or before the relay has caught up with
//! the published view, nothing is written and the view freezes.
//!
//! This is the shared half: the announced entry, the published namespace's layout and the [`View`] every node reads. The
//! publisher that writes it, with each ranged service's L2 placement directory (#19 / B-16, §4 decision 13), runs only in
//! `bedrock-relay`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use tokio::time::Instant;
use tonic::Status;

use crate::node::ServiceConfig;
use crate::placement::{Directory, Owner, State};
use crate::reconciliation::drain::{self, Drain, Scope};
use crate::relays::client::{Selection, Session, Target, applied_at};
use crate::replication::pb;
use crate::storage::memory::MemoryEngine;
use crate::storage::{KeyRange, Snapshot, StorageEngine};
use crate::transport::Peers;

pub const DATASET: &str = crate::datasets::capabilities::MEMBERSHIP;
/// Each service has its own namespace per cell; members of different services never mix.
pub use crate::datasets::capabilities::SERVICES;
/// Directory keys are `range/{dataset}/{start:016x}`; node IDs cannot contain `/`.
pub const RANGE: &str = "range/";
/// Dataset → its placement directory.
pub type Directories = BTreeMap<String, Directory>;

pub fn namespace(service: &str, cell: &str) -> String {
    format!("{service}.{cell}")
}

pub fn key(member: &pb::Member) -> crate::relays::MemberKey {
    (member.service.clone(), member.node_id.clone())
}

/// Identities travel in namespaces and write URLs unescaped.
pub fn token(identity: &str) -> bool {
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
pub struct Published {
    engine: Arc<MemoryEngine>,
    prefix: Vec<u8>,
}

pub type ServiceSnapshot = (
    BTreeMap<String, pb::Member>,
    Directories,
    Vec<(String, Drain)>,
    Option<u64>,
);

impl Published {
    pub fn new(service: &str, cell: &str) -> (Self, Target) {
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

    pub fn read(&self) -> Result<(BTreeMap<String, pb::Member>, Directories), Status> {
        read(&*self.engine, &self.prefix)
    }

    pub fn validated(
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
            validate_drain(cell, service, id, record)?;
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

    pub fn directory(&self, dataset: &str) -> Result<Directory, String> {
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
    let scan = snapshot
        .scan(&KeyRange::prefix(prefix.to_vec()))
        .map_err(|e| Status::internal(e.to_string()))?;
    parse(scan.map(|entry| {
        let (key, value) = entry.map_err(|e| Status::internal(e.to_string()))?;
        Ok((key[prefix.len()..].to_vec(), value))
    }))
}

/// Node ID → entry, and the placement directories, from a membership namespace's `(key, value)` rows, as a
/// subscription stores them or as a relay's `Routes` returns them (#56 / B-25).
pub fn parse(
    entries: impl IntoIterator<Item = Result<(Vec<u8>, Vec<u8>), Status>>,
) -> Result<(BTreeMap<String, pb::Member>, Directories), Status> {
    let mut members = BTreeMap::new();
    let mut directories = Directories::new();
    for entry in entries {
        let (key, value) = entry?;
        let key =
            String::from_utf8(key).map_err(|_| Status::data_loss("membership key is not UTF-8"))?;
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

pub fn encode(owners: &[Owner]) -> Vec<u8> {
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

    /// One dataset's L2 placement directory in this cell: route with [`crate::placement::owners`], and serve only what
    /// [`crate::placement::serves`] allows. Empty when the dataset has no directory, including for customer KV and before
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

/// Whether the control voters configured for a cell carry one distinct node identity per configured address.
pub fn validate_voter_ids(
    voters: &[String],
    identities: &BTreeMap<String, String>,
) -> Result<(), String> {
    let unique: std::collections::BTreeSet<_> = identities.values().collect();
    if identities.len() != voters.len()
        || unique.len() != voters.len()
        || identities.values().any(|id| !token(id))
        || voters
            .iter()
            .any(|address| !identities.contains_key(address))
    {
        return Err(
            "control voters require one distinct node identity per configured address".into(),
        );
    }
    Ok(())
}

/// A drain record the control authority may hold: a plain identity, a scope inside this cell and a supported selection.
pub fn validate_drain(cell: &str, service: &str, id: &str, drain: &Drain) -> Result<(), Status> {
    if id.len() > 128 || !token(id) {
        return Err(Status::invalid_argument("invalid drain identity"));
    }
    if match &drain.scope {
        Scope::Node(node) => !token(node),
        Scope::Cell(target) => target != cell,
    } || drain::validate_ranged_selection(service, drain).is_err()
    {
        return Err(Status::invalid_argument(
            "unsupported drain scope, namespace, dataset, or state",
        ));
    }
    Ok(())
}

/// What a cell relay's publisher sees and does, for relay introspection (#17 / B-18b). The publisher in `bedrock-relay`
/// builds and updates it.
pub struct Publisher {
    published: Vec<(&'static str, Published)>,
    frozen_since: Mutex<Option<Instant>>,
    directory_writes: Mutex<BTreeMap<String, u64>>,
}

impl Publisher {
    pub fn new(published: Vec<(&'static str, Published)>) -> Self {
        Self {
            published,
            frozen_since: Mutex::default(),
            directory_writes: Mutex::default(),
        }
    }

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

    /// Records whether this pass saw a live majority; the freeze runs from the first pass without one.
    pub fn set_majority(&self, live: bool) {
        let mut frozen = self.frozen_since.lock().unwrap();
        match (live, *frozen) {
            (true, _) => *frozen = None,
            (false, None) => *frozen = Some(crate::monotonic_now()),
            (false, Some(_)) => {}
        }
    }

    /// Counts one successful placement-directory write for `service`.
    pub fn wrote_directory(&self, service: &str) {
        *self
            .directory_writes
            .lock()
            .unwrap()
            .entry(service.to_owned())
            .or_default() += 1;
    }
}

#[cfg(test)]
#[path = "../tests/unit/membership.rs"]
mod tests;
