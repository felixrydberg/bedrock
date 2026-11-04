//! Sibling relays, the `replay_peers` of one tier, watch each other over one stream: a snapshot completed by `synced`,
//! then changes, with progress ticks and the [`IDLE`] deadline. A relay shares only what its own sessions hold, never
//! what it learned from a sibling. Cell relays share leaf members for membership (#15 / B-12) and their range readiness
//! (#22 / B-21). Relays of every tier share their subscribers' interests, so a sibling keeps the same streams live and
//! in its replay log, and a dead relay's subscribers arrive warm (#41 / B-12b).
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{Hub, IDLE, Key, MemberKey, PROGRESS, StreamKey, WHOLE, range, span};
use crate::membership::key;
use crate::replication::log::Watched;
use crate::replication::pb;
use crate::transport::{Caller, Class, Peers};

const TIMEOUT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_secs(1);

pub type Members = BTreeMap<MemberKey, pb::Member>;
/// Leaf member and dataset → the ranges its holding relay reports it ready for (#22 / B-21).
pub type Ready = BTreeMap<(MemberKey, String), Vec<(u64, u64)>>;

/// The follows a relay's own subscribers need from upstream, by selector (#41 / B-12b).
pub type Interests = BTreeMap<StreamKey, pb::Follow>;

/// How often a relay rechecks what it shares with its siblings and expires what a lost sibling left.
const READINESS: Duration = Duration::from_secs(1);
/// Sibling interests a relay follows beyond its own. The rest stay cold, are reported, and fail over as before. A cell
/// shares at most six, so this holds about 1,300 cells below one upper-tier relay.
pub const MAX_INTERESTS: usize = 4096;

/// What one sibling shares.
#[derive(Default)]
pub(super) struct Shared {
    /// Its synced interests, `None` while its stream is down.
    pub(super) live: Option<Interests>,
    /// What it shared when its stream dropped, kept until `until` while its subscribers fail over, even if it returns
    /// without them.
    pub(super) lingering: Interests,
    until: Option<tokio::time::Instant>,
}

impl Shared {
    fn interests(&self) -> impl Iterator<Item = (&StreamKey, &pb::Follow)> {
        self.live.iter().flatten().chain(&self.lingering)
    }
}

fn selector(follow: &pb::Follow) -> StreamKey {
    (
        (follow.dataset.clone(), follow.namespace.clone()),
        range(follow.span.as_ref()),
    )
}

/// Lowers `held` to the cursors of `other`, which has the same selector, so the stream covers every sibling's
/// subscribers. A namespace missing from a whole-dataset follow starts at 1. Partial ranges keep the first follow.
fn lowest(held: &mut pb::Follow, other: &pb::Follow) {
    if held.span.is_none() {
        held.from = held.from.min(other.from);
    } else if held.namespace.is_empty() && range(held.span.as_ref()) == Some(WHOLE) {
        for (namespace, from) in &mut held.from_namespaces {
            let theirs = other.from_namespaces.get(namespace).copied();
            *from = (*from).min(theirs.unwrap_or(1));
        }
        for (namespace, from) in &other.from_namespaces {
            let ours = held.from_namespaces.entry(namespace.clone());
            ours.or_insert((*from).min(1));
        }
    }
}

impl Hub {
    /// The follows this relay would send upstream for its own subscribers alone. Whatever hash ranges they follow, that
    /// is the whole dataset, so one interest stands for all of them. Never includes what siblings share.
    pub fn interests(&self) -> Interests {
        let (namespaces, datasets) = self.own();
        let ranged = datasets.iter().map(|dataset| pb::Follow {
            dataset: dataset.clone(),
            span: span(Some(WHOLE)),
            from_namespaces: self
                .namespaces(dataset)
                .into_iter()
                .filter_map(|namespace| {
                    let from = *namespaces.get(&(dataset.clone(), namespace.clone()))?;
                    Some((namespace, from))
                })
                .collect(),
            ..Default::default()
        });
        let whole = namespaces
            .iter()
            .filter(|((dataset, _), _)| !datasets.contains(dataset))
            .map(|((dataset, namespace), from)| pb::Follow {
                dataset: dataset.clone(),
                namespace: namespace.clone(),
                from: *from,
                ..Default::default()
            });
        ranged
            .chain(whole)
            .map(|follow| (selector(&follow), follow))
            .collect()
    }

    /// Sibling interests this relay does not hold itself, as `(followed, cold)`: range follows first, then by selector,
    /// split at [`MAX_INTERESTS`].
    pub(super) fn derived(
        &self,
        namespaces: &BTreeMap<Key, u64>,
        datasets: &BTreeSet<String>,
    ) -> (Vec<pb::Follow>, Vec<pb::Follow>) {
        let shared = self.0.shared.lock().unwrap();
        let mut wanted = BTreeMap::new();
        for (key, follow) in shared.values().flat_map(Shared::interests) {
            if !datasets.contains(&key.0.0) && !namespaces.contains_key(&key.0) {
                wanted
                    .entry((!follow.namespace.is_empty(), key))
                    .and_modify(|held| lowest(held, follow))
                    .or_insert_with(|| follow.clone());
            }
        }
        let mut followed: Vec<_> = wanted.into_values().collect();
        let cold = followed.split_off(followed.len().min(MAX_INTERESTS));
        (followed, cold)
    }

    /// [`Hub::derived`] for what this relay's subscribers want right now.
    pub(super) fn sibling_interests(&self) -> (Vec<pb::Follow>, Vec<pb::Follow>) {
        let (namespaces, datasets) = self.own();
        self.derived(&namespaces, &datasets)
    }

    /// Records what `sibling` shares, or with `None` that its stream dropped: what it held stays for `grace`.
    fn shared(&self, sibling: &str, interests: Option<Interests>, grace: Duration) {
        let mut all = self.0.shared.lock().unwrap();
        let shared = all.entry(sibling.to_owned()).or_default();
        match interests {
            Some(interests) if shared.live.as_ref() == Some(&interests) => return,
            Some(interests) => shared.live = Some(interests),
            None => {
                let Some(live) = shared.live.take() else {
                    return;
                };
                shared.lingering.extend(live);
                shared.until = Some(crate::monotonic_now() + grace);
            }
        }
        drop(all);
        self.changed();
    }

    /// Drops what lost siblings left once their grace ran out.
    fn expire_shared(&self) {
        let now = crate::monotonic_now();
        let mut expired = false;
        for shared in self.0.shared.lock().unwrap().values_mut() {
            if shared.until.is_some_and(|until| now >= until) {
                shared.lingering.clear();
                shared.until = None;
                expired = true;
            }
        }
        if expired {
            self.changed();
        }
    }
}

/// Serves this relay's own state to its siblings. Every relay serves it.
pub struct Server(pub Hub);

#[tonic::async_trait]
impl pb::siblings_server::Siblings for Server {
    type WatchStream = ReceiverStream<Result<pb::SiblingUpdate, Status>>;

    async fn watch(
        &self,
        request: Request<pb::SiblingWatch>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // Only a cell tier shares members, and only its relays have one cell.
        let cell_tier = self.0.0.tier == super::tier_rank(crate::node::Tier::Cell);
        if let Some(caller) = Caller::of(&request) {
            caller.require(&["relay"], cell_tier.then(|| self.0.cell()))?;
        }
        let (tx, rx) = mpsc::channel(16);
        let hub = self.0.clone();
        tokio::spawn(async move {
            // A watcher that drains nothing for IDLE reconnects and gets a fresh snapshot.
            let tx = &tx;
            let send = |update| async move {
                matches!(
                    tokio::time::timeout(IDLE, tx.send(Ok(update))).await,
                    Ok(Ok(()))
                )
            };
            let version = hub.member_version();
            let mut tick = tokio::time::interval(PROGRESS);
            tick.tick().await;
            // ponytail: polls readiness, which acknowledgements change; wake on them if handoffs need lower latency.
            let mut readiness = tokio::time::interval(READINESS);
            let mut sent = Members::new();
            let mut sent_ready = Ready::new();
            let mut sent_interests = Interests::new();
            let mut synced = false;
            loop {
                let at = version.get();
                let current = if cell_tier {
                    hub.members()
                } else {
                    Members::new()
                };
                let ready = hub.ready();
                let interests = hub.interests();
                let mut changed_ready: Vec<pb::Ready> = ready
                    .iter()
                    .filter(|(key, spans)| sent_ready.get(*key) != Some(*spans))
                    .map(|(key, spans)| wire(key, spans))
                    .collect();
                changed_ready.extend(
                    sent_ready
                        .keys()
                        .filter(|key| !ready.contains_key(*key))
                        .map(|key| wire(key, &[])),
                );
                let update = pb::SiblingUpdate {
                    members: current
                        .iter()
                        .filter(|(key, member)| sent.get(*key) != Some(*member))
                        .map(|(_, member)| member.clone())
                        .collect(),
                    members_removed: sent
                        .keys()
                        .filter(|key| !current.contains_key(*key))
                        .map(|(service, node_id)| pb::Member {
                            service: service.clone(),
                            node_id: node_id.clone(),
                            ..Default::default()
                        })
                        .collect(),
                    synced: !synced,
                    ready: changed_ready,
                    interests: interests
                        .iter()
                        .filter(|(key, _)| !sent_interests.contains_key(*key))
                        .map(|(_, follow)| follow.clone())
                        .collect(),
                    interests_removed: sent_interests
                        .iter()
                        .filter(|(key, _)| !interests.contains_key(*key))
                        .map(|(_, follow)| pb::Unfollow {
                            dataset: follow.dataset.clone(),
                            namespace: follow.namespace.clone(),
                            span: follow.span,
                        })
                        .collect(),
                };
                if update.synced || !empty(&update) {
                    if !send(update).await {
                        return;
                    }
                    sent = current;
                    sent_ready = ready;
                    sent_interests = interests;
                    synced = true;
                }
                tokio::select! {
                    _ = version.wait_until(|v| *v != at) => {},
                    _ = readiness.tick() => {},
                    _ = tick.tick() => if !send(pb::SiblingUpdate::default()).await { return },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Whether an update changes nothing: a progress tick, or the `synced` mark of an empty snapshot.
fn empty(update: &pb::SiblingUpdate) -> bool {
    update.members.is_empty()
        && update.members_removed.is_empty()
        && update.ready.is_empty()
        && update.interests.is_empty()
        && update.interests_removed.is_empty()
}

fn wire(((service, node_id), dataset): &(MemberKey, String), spans: &[(u64, u64)]) -> pb::Ready {
    pb::Ready {
        service: service.clone(),
        node_id: node_id.clone(),
        dataset: dataset.clone(),
        spans: spans
            .iter()
            .map(|&(start, end)| pb::Span { start, end })
            .collect(),
    }
}

/// Aborts every task it holds when dropped, so aborting the owner stops them all.
struct Tasks(Vec<tokio::task::JoinHandle<()>>);

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// The latest synced state of each sibling, `None` while its stream is down.
pub struct Siblings {
    hub: Hub,
    peers: Arc<Peers>,
    grace: Duration,
    state: Mutex<BTreeMap<String, Option<(Members, Ready)>>>,
    changed: Watched<u64>,
}

impl Siblings {
    /// Watches `siblings` (this relay excluded), feeding `hub` their interests, which outlive a lost stream by `grace`.
    /// Abort the returned task on shutdown.
    pub fn start(
        hub: Hub,
        peers: Arc<Peers>,
        siblings: Vec<String>,
        grace: Duration,
    ) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
        hub.0
            .shared
            .lock()
            .unwrap()
            .extend(siblings.iter().map(|s| (s.clone(), Shared::default())));
        let this = Arc::new(Self {
            hub,
            peers,
            grace,
            state: Mutex::new(siblings.iter().map(|s| (s.clone(), None)).collect()),
            changed: Watched::new(0),
        });
        let watching = this.clone();
        let task = tokio::spawn(async move {
            let _watchers = Tasks(
                siblings
                    .into_iter()
                    .map(|sibling| tokio::spawn(watching.clone().watch(sibling)))
                    .collect(),
            );
            let mut sweep = tokio::time::interval(READINESS);
            loop {
                sweep.tick().await;
                watching.hub.expire_shared();
            }
        });
        (this, task)
    }

    /// The latest synced members per sibling, or `None` before its first sync or while its stream is down.
    pub fn state(&self) -> BTreeMap<String, Option<Members>> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .map(|(sibling, state)| (sibling.clone(), state.as_ref().map(|(m, _)| m.clone())))
            .collect()
    }

    /// The readiness live siblings report for their own leaf members.
    pub fn ready(&self) -> Ready {
        self.state
            .lock()
            .unwrap()
            .values()
            .flatten()
            .flat_map(|(_, ready)| ready.clone())
            .collect()
    }

    /// Bumped on every change to [`Siblings::state`].
    pub fn version(&self) -> u64 {
        self.changed.get()
    }

    pub async fn changed(&self, since: u64) {
        self.changed.wait_until(|v| *v != since).await;
    }

    fn set(&self, sibling: &str, state: Option<(Members, Ready)>) {
        self.state.lock().unwrap().insert(sibling.to_owned(), state);
        self.changed.update(|v| *v = v.wrapping_add(1));
    }

    async fn watch(self: Arc<Self>, sibling: String) {
        loop {
            let error = self.watched(&sibling).await;
            tracing::debug!(%sibling, %error, "sibling relay watch ended");
            self.set(&sibling, None);
            self.hub.shared(&sibling, None, self.grace);
            tokio::time::sleep(RETRY).await;
        }
    }

    async fn watched(&self, sibling: &str) -> Status {
        let result: Result<(), Status> = async {
            let channel = self
                .peers
                .channel(sibling, Class::Control)
                .map_err(|e| Status::unavailable(e.to_string()))?;
            let mut client = pb::siblings_client::SiblingsClient::new(channel);
            let mut stream = self
                .peers
                .call(
                    sibling,
                    Class::Control,
                    TIMEOUT,
                    client.watch(pb::SiblingWatch {}),
                )
                .await?
                .into_inner();
            let mut members = Members::new();
            let mut ready = Ready::new();
            let mut interests = Interests::new();
            let mut synced = false;
            loop {
                let update = self
                    .peers
                    .call(sibling, Class::Control, IDLE, stream.message())
                    .await?
                    .ok_or_else(|| Status::unavailable("sibling relay closed"))?;
                if synced && empty(&update) {
                    continue;
                }
                for member in update.members_removed {
                    let gone = key(&member);
                    ready.retain(|(member, _), _| *member != gone);
                    members.remove(&gone);
                }
                for member in update.members {
                    members.insert(key(&member), member);
                }
                for entry in update.ready {
                    let at = ((entry.service, entry.node_id), entry.dataset);
                    let spans: Vec<(u64, u64)> =
                        entry.spans.iter().map(|s| (s.start, s.end)).collect();
                    if spans.is_empty() {
                        ready.remove(&at);
                    } else {
                        ready.insert(at, spans);
                    }
                }
                for removed in update.interests_removed {
                    interests.remove(&(
                        (removed.dataset, removed.namespace),
                        range(removed.span.as_ref()),
                    ));
                }
                for follow in update.interests {
                    if super::selectable(&follow) {
                        interests.insert(selector(&follow), follow);
                    } else {
                        tracing::warn!(%sibling, ?follow, "ignoring an invalid sibling interest");
                    }
                }
                synced |= update.synced;
                if synced {
                    self.set(sibling, Some((members.clone(), ready.clone())));
                    self.hub
                        .shared(sibling, Some(interests.clone()), self.grace);
                }
            }
        }
        .await;
        result
            .err()
            .unwrap_or_else(|| Status::unavailable("sibling relay closed"))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/relays/siblings.rs"]
mod tests;
