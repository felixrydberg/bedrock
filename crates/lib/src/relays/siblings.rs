//! Sibling relays, the `replay_peers` of one tier, watch each other over one stream: a snapshot completed by `synced`,
//! then changes, with progress ticks and the [`IDLE`] deadline. A relay shares only what its own sessions hold, never
//! what it learned from a sibling. Today that is leaf members for cell membership (#15 / B-12) and their range
//! readiness (#22 / B-21); #41 / B-12b adds subscriber interests to the same update.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{Hub, IDLE, MemberKey, PROGRESS};
use crate::membership::key;
use crate::replication::log::Watched;
use crate::replication::pb;
use crate::transport::{Class, Peers};

const TIMEOUT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_secs(1);

pub type Members = BTreeMap<MemberKey, pb::Member>;
/// Leaf member and dataset → the ranges its holding relay reports it ready for (#22 / B-21).
pub type Ready = BTreeMap<(MemberKey, String), Vec<(u64, u64)>>;

/// How often a relay rechecks its leaf members' readiness for its siblings.
const READINESS: Duration = Duration::from_secs(1);

/// Serves this relay's own state to its siblings. Every relay serves it.
pub struct Server(pub Hub);

#[tonic::async_trait]
impl pb::siblings_server::Siblings for Server {
    type WatchStream = ReceiverStream<Result<pb::SiblingUpdate, Status>>;

    async fn watch(
        &self,
        _: Request<pb::SiblingWatch>,
    ) -> Result<Response<Self::WatchStream>, Status> {
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
            let mut synced = false;
            loop {
                let at = version.get();
                let current = hub.members();
                let ready = hub.ready();
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
                };
                if update.synced
                    || !update.members.is_empty()
                    || !update.members_removed.is_empty()
                    || !update.ready.is_empty()
                {
                    if !send(update).await {
                        return;
                    }
                    sent = current;
                    sent_ready = ready;
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
    peers: Arc<Peers>,
    state: Mutex<BTreeMap<String, Option<(Members, Ready)>>>,
    changed: Watched<u64>,
}

impl Siblings {
    /// Watches `siblings` (this relay excluded); abort the returned task on shutdown.
    pub fn start(
        peers: Arc<Peers>,
        siblings: Vec<String>,
    ) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
        let this = Arc::new(Self {
            peers,
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
            std::future::pending::<()>().await;
        });
        (this, task)
    }

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
            let mut synced = false;
            loop {
                let update = self
                    .peers
                    .call(sibling, Class::Control, IDLE, stream.message())
                    .await?
                    .ok_or_else(|| Status::unavailable("sibling relay closed"))?;
                if synced
                    && update.members.is_empty()
                    && update.members_removed.is_empty()
                    && update.ready.is_empty()
                {
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
                synced |= update.synced;
                if synced {
                    self.set(sibling, Some((members.clone(), ready.clone())));
                }
            }
        }
        .await;
        result
            .err()
            .unwrap_or_else(|| Status::unavailable("sibling relay closed"))
    }
}
