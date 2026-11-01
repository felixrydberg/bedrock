//! Cell membership from relay subscriber streams (#15 / B-12, `docs/spec/membership.md`).
//!
//! A `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process announces a [`pb::Member`] on its leaf relay session. Cell relays watch
//! each other's connected lists over the sibling stream ([`crate::relays::siblings`]), and the lowest relay of a
//! majority writes the union to dataset [`DATASET`], namespace `{service}.{cell}`, through `bedrock-api`. Nodes subscribe to that namespace
//! like any other ([`View`]). A disconnected node stays a member for the grace period, so failover to another cell
//! relay changes nothing; a drained node leaves at once. Without a majority, or before the relay has caught up with
//! the published view, nothing is written and the view freezes.
//!
//! The publisher also writes each ranged service's L2 placement directory into the same namespace (#19 / B-16, §4
//! decision 13), so membership and placement share one revision order. It plans placement only once membership has
//! nothing left to write.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{AUTHORIZATION, HOST};
use hyper_util::rt::TokioIo;
use prost::Message;
use tokio::time::Instant;
use tonic::Status;

use crate::node::{MembershipSettings, ServiceConfig};
use crate::placement::{self, Directory, Owner, State};
use crate::relays::client::{Selection, Session, Target};
pub use crate::relays::siblings::Members;
use crate::relays::siblings::Siblings;
use crate::relays::{Hub, IDLE, MemberKey};
use crate::replication::pb;
use crate::storage::memory::MemoryEngine;
use crate::storage::{KeyRange, StorageEngine};
use crate::transport::Peers;

pub const DATASET: &str = "membership";
/// Each service has its own namespace per cell; members of different services never mix.
pub const SERVICES: [&str; 3] = ["quicksilver", "kv", "cdn"];
const TIMEOUT: Duration = Duration::from_secs(5);
/// Re-plans at least this often, which is what expires grace periods.
const RECONCILE: Duration = Duration::from_secs(1);
/// Services whose L2 is placed in hash ranges; customer KV ranks its readers instead (#18 / B-15).
const RANGED: [&str; 2] = ["quicksilver", "cdn"];
/// Directory keys are `range/{start:016x}`; node IDs cannot contain `/`.
const RANGE: &str = "range/";
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

/// An entry a node may announce: a known service, plain identities and `ready` or `draining`.
pub fn validate(member: &pb::Member) -> Result<(), Status> {
    let status = pb::member::Status::try_from(member.status);
    if !SERVICES.contains(&member.service.as_str())
        || !token(&member.node_id)
        || !token(&member.cell)
        || !matches!(
            status,
            Ok(pb::member::Status::Ready | pb::member::Status::Draining)
        )
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
    }
}

/// One service's published members in a cell, held in memory; a restarted node bootstraps it again.
struct Published {
    engine: Arc<MemoryEngine>,
    prefix: Vec<u8>,
}

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
                buckets: Vec::new(),
            },
            engine: engine.clone(),
            prefix: prefix.clone(),
        };
        (Self { engine, prefix }, target)
    }

    /// Node ID → entry, and the placement directory. An undecodable entry is corruption, never a missing one.
    fn read(&self) -> Result<(BTreeMap<String, pb::Member>, Directory), Status> {
        let mut members = BTreeMap::new();
        let mut directory = Directory::new();
        let scan = self
            .engine
            .scan(&KeyRange::prefix(self.prefix.clone()))
            .map_err(|e| Status::internal(e.to_string()))?;
        for entry in scan {
            let (key, value) = entry.map_err(|e| Status::internal(e.to_string()))?;
            let key = String::from_utf8(key[self.prefix.len()..].to_vec())
                .map_err(|_| Status::data_loss("membership key is not UTF-8"))?;
            if let Some(start) = key.strip_prefix(RANGE) {
                let start = u64::from_str_radix(start, 16)
                    .map_err(|_| Status::data_loss("invalid range key"))?;
                directory.insert(start, decode(&value)?);
                continue;
            }
            let member = pb::Member::decode(value.as_slice())
                .map_err(|e| Status::data_loss(e.to_string()))?;
            members.insert(key, member);
        }
        Ok((members, directory))
    }
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
        Ok(self.published.read()?.0)
    }

    /// The service's L2 placement directory in this cell: route with [`placement::owners`], and serve only what
    /// [`placement::serves`] allows. Empty for customer KV.
    pub fn directory(&self) -> Result<Directory, Status> {
        Ok(self.published.read()?.1)
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
pub enum Change {
    Set(pb::Member),
    Delete(MemberKey),
    Range {
        service: String,
        start: u64,
        owners: Vec<Owner>,
    },
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
/// writes only if this relay is the lowest of a live majority of the cell's relays.
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
    let live: Vec<&str> = std::iter::once(seen.me)
        .chain(
            seen.remote
                .iter()
                .filter(|(_, list)| list.is_some())
                .map(|(relay, _)| relay.as_str()),
        )
        .collect();
    if live.len() * 2 <= seen.relays.len() || live.iter().any(|relay| *relay < seen.me) {
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

/// One ranged service's directory writes from the published `actual` members: promote what the last pass assigned,
/// otherwise assign (`docs/spec/membership.tla`, `Write`).
pub fn place(service: &str, actual: &Members, directory: &Directory) -> Vec<Change> {
    let mut entries = placement::promote(directory);
    if entries.is_empty() {
        let members = actual
            .iter()
            .filter(|((s, _), _)| s == service)
            .map(|((_, id), member)| (id.clone(), member.clone()))
            .collect();
        // ponytail: no health input until #13 / B-14 reports it in member updates.
        entries = placement::assign(directory, &members, &BTreeSet::new(), REPLICAS, MAX_MOVING);
    }
    entries
        .into_iter()
        .map(|(start, owners)| Change::Range {
            service: service.into(),
            start,
            owners,
        })
        .collect()
}

/// A cell relay's membership publisher.
pub struct Cell {
    hub: Hub,
    siblings: Arc<Siblings>,
    me: String,
    relays: Vec<String>,
    cell: String,
    grace: Duration,
    api: String,
    write_token: String,
}

impl Cell {
    /// Publishes from this relay's members and its watched siblings; abort the returned task on shutdown. `me` is this
    /// relay's advertised address, as its siblings list it.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        hub: Hub,
        peers: Arc<Peers>,
        upstreams: Vec<String>,
        siblings: Arc<Siblings>,
        settings: &MembershipSettings,
        me: String,
        cell: String,
        write_token: String,
    ) -> Result<tokio::task::JoinHandle<()>, Status> {
        let mut published = Vec::new();
        let mut targets = Vec::new();
        for service in SERVICES {
            let (view, target) = Published::new(service, &cell);
            published.push((service, view));
            targets.push(target);
        }
        // The relay reads the published view through its own upstreams, like any subscriber.
        let session = Session::start(peers, upstreams, targets)?;
        let relays = std::iter::once(me.clone())
            .chain(siblings.state().into_keys())
            .collect();
        let cell = Self {
            hub,
            siblings,
            me,
            relays,
            cell,
            grace: Duration::from_millis(settings.grace_ms),
            api: settings.api.clone(),
            write_token,
        };
        Ok(tokio::spawn(async move {
            cell.publish(session, published).await
        }))
    }

    async fn publish(&self, session: Session, published: Vec<(&'static str, Published)>) {
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
                continue;
            }
            let mut actual = Members::new();
            let mut directories = Vec::new();
            let read: Result<(), Status> = published.iter().try_for_each(|(service, view)| {
                let (members, directory) = view.read()?;
                for (id, member) in members {
                    actual.insert(((*service).into(), id), member);
                }
                directories.push((*service, directory));
                Ok(())
            });
            if let Err(error) = read {
                tracing::warn!(%error, "published membership unreadable; not publishing");
                continue;
            }
            let local: Members = self
                .hub
                .members()
                .into_iter()
                .map(|(key, connected)| (key, connected.member))
                .collect();
            let remote = self.siblings.state();
            let seen = Observed {
                me: &self.me,
                relays: &self.relays,
                cell: &self.cell,
                local: &local,
                remote: &remote,
            };
            let Some(mut changes) = plan(
                &seen,
                &actual,
                &mut absent,
                crate::monotonic_now(),
                self.grace,
            ) else {
                continue;
            };
            // Placement plans only from applied membership.
            if changes.is_empty() {
                for (service, directory) in &directories {
                    if RANGED.contains(service) {
                        changes.extend(place(service, &actual, directory));
                    }
                }
            }
            let mut written = BTreeMap::new();
            for change in changes {
                let service = match &change {
                    Change::Set(member) => member.service.clone(),
                    Change::Delete((service, _)) | Change::Range { service, .. } => service.clone(),
                };
                match tokio::time::timeout(TIMEOUT, self.write(&change)).await {
                    Ok(Ok(revision)) => {
                        let latest = written.entry(service).or_insert(revision);
                        *latest = revision.max(*latest);
                    }
                    Ok(Err(error)) => tracing::warn!(?change, %error, "membership write failed"),
                    Err(_) => tracing::warn!(?change, "membership write timed out"),
                }
            }
            // The next plan must see these writes, or it would repeat them.
            again = !written.is_empty();
            for (service, revision) in written {
                let namespace = namespace(&service, &self.cell);
                let _ = tokio::time::timeout(IDLE, session.wait_for(DATASET, &namespace, revision))
                    .await;
            }
        }
    }

    /// One write through `bedrock-api`; returns the committed revision.
    async fn write(&self, change: &Change) -> Result<u64, String> {
        let (method, (service, key), body) = match change {
            Change::Set(member) => (hyper::Method::PUT, key(member), member.encode_to_vec()),
            Change::Delete(key) => (hyper::Method::DELETE, key.clone(), Vec::new()),
            Change::Range {
                service,
                start,
                owners,
            } => (
                hyper::Method::PUT,
                (service.clone(), format!("{RANGE}{start:016x}")),
                encode(owners),
            ),
        };
        let uri = format!("/v1/{DATASET}/{}/{key}", namespace(&service, &self.cell));
        let stream = tokio::net::TcpStream::connect(self.api.as_str())
            .await
            .map_err(|e| e.to_string())?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, &self.api)
            .header(AUTHORIZATION, format!("Bearer {}", self.write_token))
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| e.to_string())?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        let body = String::from_utf8_lossy(&body);
        if !status.is_success() {
            return Err(format!("{status}: {body}"));
        }
        body.trim()
            .parse()
            .map_err(|_| format!("unexpected response {body:?}"))
    }
}

#[cfg(test)]
#[path = "../tests/unit/membership.rs"]
mod tests;
