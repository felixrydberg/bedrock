//! The cell membership publisher (#15 / B-12, `docs/spec/membership.md`): what a cell relay writes into the membership
//! dataset, and the placement directory beside it (#19 / B-16, §4 decision 13).
//!
//! Each cell relay watches its leaf members and its siblings' lists ([`bedrock_lib::relays::siblings`]); the cell's
//! embedded Raft leader ([`control`]) writes the union to `bedrock_lib::membership::DATASET`, namespace `{service}.{cell}`,
//! on a durable local quorum. A disconnected node stays a member for the grace period, so failover to another cell relay
//! changes nothing; a drained node leaves at once. Without a majority, or before the relay has caught up with the
//! published view, nothing is written and the view freezes. The publisher plans placement only once membership has
//! nothing left to write. Readers of the published namespace live in `bedrock_lib::membership`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::datasets::capabilities::{VOLUME, full_replica, pushed, ranged_datasets};
use bedrock_lib::membership::{
    DATASET, Directories, Generations, Published, Publisher, RANGE, SERVICES, encode_generations,
    key, namespace,
};
use bedrock_lib::node::MembershipSettings;
use bedrock_lib::placement::coverage::{self, Report};
use bedrock_lib::placement::{Directory, Owner, State};
use bedrock_lib::reconciliation::drain::{self, Drain};
use bedrock_lib::relays::client::Session;
use bedrock_lib::relays::siblings::{self, Members, Siblings};
use bedrock_lib::relays::{Hub, IDLE, MemberKey};
use bedrock_lib::replication::pb;
use bedrock_lib::transport::Peers;
use prost::Message;
use tokio::time::Instant;
use tonic::Status;

use crate::placement;

pub mod control;

const TIMEOUT: Duration = Duration::from_secs(5);
/// Re-plans at least this often, which is what expires grace periods.
const RECONCILE: Duration = Duration::from_secs(1);
// ponytail: fixed until B-34 makes them per-service settings read at relay startup (CDN artifacts want 2 replicas).
// `volume` has its own setting, `volume_replicas`.
const REPLICAS: usize = 3;
const MAX_MOVING: usize = 16;

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
        /// Owner → the generation of its assignment, for a dataset no relay streams (#114 / S-06).
        generations: BTreeMap<String, u64>,
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

/// The coverage of a `full_replica` dataset's directory (#87 / B-40). An owner holds a range only while it is connected
/// to a cell relay (this one or a synced sibling), still carries the dataset and its storage for it is healthy, so a
/// killed node is degraded at once, not after the grace period; members behind an unreachable sibling count as gone
/// until they reconnect. Each member is judged by one observation, local first as in [`plan`]. Redundancy wants as many
/// holders as placement can give each range: [`REPLICAS`], or fewer when fewer published members are `ready` `l2`
/// members carrying the dataset. Unlike placement, an unhealthy member still counts there: it keeps its ranges, so the
/// cell stays degraded until it recovers.
pub fn completeness(
    seen: &Observed,
    actual: &Members,
    service: &str,
    dataset: &str,
    directory: &Directory,
) -> coverage::Coverage {
    let carries = |m: &pb::Member| m.datasets.iter().any(|d| d == dataset);
    let mut observed: BTreeMap<&str, &pb::Member> = BTreeMap::new();
    for list in std::iter::once(seen.local).chain(seen.remote.values().flatten()) {
        for member in list.values() {
            if member.cell == seen.cell && member.service == service {
                observed.entry(member.node_id.as_str()).or_insert(member);
            }
        }
    }
    // ponytail: B-62's per-range `damaged` spans would refine this; a partly quarantined dataset is already `unhealthy`,
    // so until then such a member counts as holding none of its ranges.
    let holders: BTreeSet<&str> = observed
        .into_iter()
        .filter(|(_, m)| {
            m.status != pb::member::Status::Left as i32
                && carries(m)
                && !m.unhealthy.iter().any(|d| d == dataset)
        })
        .map(|(id, _)| id)
        .collect();
    let placeable = actual
        .iter()
        .filter(|((s, _), m)| {
            s == service
                && m.status == pb::member::Status::Ready as i32
                && m.roles.iter().any(|r| r == "l2")
                && carries(m)
        })
        .count();
    coverage::coverage(directory, REPLICAS.min(placeable), |node| {
        holders.contains(node)
    })
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
/// For a dataset no relay streams (`volume`, #114 / S-06), each owner's assignment has a generation: an owner keeps
/// the one it has, and one new to a range gets `revision + 1`, above the revision this plan read and below any later
/// plan's. A populating owner counts as ready only for its current generation.
#[allow(clippy::too_many_arguments)]
pub fn place(
    service: &str,
    actual: &Members,
    directories: &Directories,
    generations: &Generations,
    revision: u64,
    ready: &siblings::Ready,
    drains: &[Drain],
    replicas: usize,
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
        let owned = generations.get(dataset);
        let generation = |start: u64, node: &str| {
            owned
                .and_then(|g| g.get(&start))
                .and_then(|g| g.get(node))
                .copied()
                .unwrap_or(0)
        };
        let ready: BTreeSet<(String, u64)> = ready
            .iter()
            .filter(|(((s, _), d), _)| s == service && d == dataset)
            .flat_map(|(((_, node), _), spans)| {
                spans
                    .iter()
                    .filter(|(start, _, copied)| *copied == generation(*start, node))
                    .map(|(start, _, _)| (node.clone(), *start))
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
            entries = placement::assign(directory, &drained, &unhealthy, replicas, moving);
            // A cell's first owners of a dataset no relay streams (`volume`) have nothing anywhere to copy, so they start
            // `ready` (#114 / S-06). Every later owner copies and reports.
            if directory.is_empty() && !pushed(dataset) {
                for owner in entries.values_mut().flatten() {
                    owner.state = State::Ready;
                }
            }
        }
        // Merges wait for a pass with nothing else to write, so they never share a plan with a split. The merged range
        // keeps the earlier entry's generations, so only ranges whose owners have the same generation in both merge:
        // otherwise an owner would forget its copy of the later part and copy it again.
        let merges = if entries.is_empty() {
            placement::merges(directory)
                .into_iter()
                .filter(|start| {
                    let before = bedrock_lib::placement::start_of(directory, start - 1);
                    directory[start]
                        .iter()
                        .all(|o| generation(before, &o.node) == generation(*start, &o.node))
                })
                .collect()
        } else {
            Vec::new()
        };
        let assigned = |start: u64, owners: &[Owner]| -> BTreeMap<String, u64> {
            if pushed(dataset) {
                return BTreeMap::new();
            }
            let before = bedrock_lib::placement::owners(directory, start);
            let cover = bedrock_lib::placement::start_of(directory, start);
            owners
                .iter()
                .map(|o| {
                    let kept = before.iter().any(|b| b.node == o.node);
                    let g = if kept {
                        generation(cover, &o.node)
                    } else {
                        revision + 1
                    };
                    (o.node.clone(), g)
                })
                .filter(|(_, g)| *g != 0)
                .collect()
        };
        changes.extend(entries.into_iter().map(|(start, owners)| Change::Range {
            service: service.into(),
            dataset: dataset.into(),
            start,
            generations: assigned(start, &owners),
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

/// A placement view, its assignment generations and the exact membership revision captured with them.
struct DirectoryAt {
    revision: u64,
    directories: Directories,
    generations: Generations,
}

impl DirectoryAt {
    fn ranges(&self, dataset: &str, node: &str) -> pb::Ranges {
        let spans = self
            .directories
            .get(dataset)
            .map(|directory| owned(directory, node))
            .unwrap_or_default();
        let generations = if pushed(dataset) {
            Vec::new()
        } else {
            let owned = self.generations.get(dataset);
            spans
                .iter()
                .map(|s| {
                    owned
                        .and_then(|g| g.get(&s.start))
                        .and_then(|g| g.get(node))
                        .copied()
                        .unwrap_or(0)
                })
                .collect()
        };
        pb::Ranges {
            dataset: dataset.into(),
            revision: self.revision,
            spans,
            generations,
        }
    }
}

/// A cell relay's membership publisher.
pub struct Cell {
    publisher: Arc<Publisher>,
    published: Vec<(&'static str, Published)>,
    hub: Hub,
    siblings: Arc<Siblings>,
    me: String,
    relays: Vec<String>,
    cell: String,
    grace: Duration,
    volume_replicas: usize,
    control: Arc<control::Control>,
    drain_hooks: BTreeMap<String, Vec<Arc<dyn drain::Hook>>>,
    report: Report,
}

impl Cell {
    /// Publishes from this relay's members and its watched siblings; abort the returned task on shutdown. `me` is this
    /// relay's advertised address, as its siblings list it. The [`Publisher`] serves introspection.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        admin: &Admin,
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
        let publisher = Arc::new(Publisher::new(published.clone()));
        let report = Report::register(admin, &cell);
        let cell = Self {
            report,
            publisher: publisher.clone(),
            published,
            hub,
            siblings,
            me,
            relays,
            cell,
            grace: Duration::from_millis(settings.grace_ms),
            volume_replicas: settings.volume_replicas,
            control,
            drain_hooks,
        };
        Ok((
            tokio::spawn(async move { cell.publish(session).await }),
            publisher,
        ))
    }

    async fn publish(&self, session: Session) {
        let published = &self.published;
        let version = self.hub.member_version();
        let mut absent = BTreeMap::new();
        // After a successful write, plan again at once: placement takes a pass per step.
        let mut again = false;
        loop {
            let (members_at, siblings_at) = (version.get(), self.siblings.version());
            if !std::mem::take(&mut again) {
                tokio::select! {
                    biased;
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
                let (members, directory, records, revision, generations) = match view
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
                        generations,
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
            self.report(&seen, &actual, &directories);
            self.publisher.set_majority(majority(&seen));
            // Every relay observes membership and pushes its committed view; only Raft's leader writes.
            let term = self.control.term();
            let Some(mut changes) = publication_plan(
                &seen,
                &actual,
                &mut absent,
                bedrock_lib::monotonic_now(),
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
                        &snapshot.generations,
                        snapshot.revision,
                        &ready,
                        drains,
                        if *service == VOLUME {
                            self.volume_replicas
                        } else {
                            REPLICAS
                        },
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
                            self.publisher.wrote_directory(change.service());
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

    /// Reports each `full_replica` dataset's completeness (#87 / B-40, [`completeness`]). Every relay reports from its
    /// own view; none writes anything.
    fn report(&self, seen: &Observed, actual: &Members, directories: &[(&str, DirectoryAt)]) {
        for (service, published) in directories {
            for dataset in ranged_datasets(service).filter(|d| full_replica(d)) {
                let empty = Directory::new();
                let directory = published.directories.get(dataset).unwrap_or(&empty);
                self.report.set(
                    dataset,
                    completeness(seen, actual, service, dataset, directory),
                );
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
                generations,
            } => (
                (service.clone(), format!("{RANGE}{dataset}/{start:016x}")),
                Some(encode_generations(owners, generations)),
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
