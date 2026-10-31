//! Sibling relays, the `replay_peers` of one tier, watch each other over one stream: a snapshot completed by `synced`,
//! then changes, with progress ticks and the [`IDLE`] deadline. A relay shares only what its own sessions hold, never
//! what it learned from a sibling. Today that is leaf members for cell membership (#15 / B-12); #41 / B-12b adds
//! subscriber interests to the same update.
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

fn own(hub: &Hub) -> Members {
    hub.members()
        .into_iter()
        .map(|(key, connected)| (key, connected.member))
        .collect()
}

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
            let mut sent = Members::new();
            let mut synced = false;
            loop {
                let at = version.get();
                let current = own(&hub);
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
                };
                if update.synced || !update.members.is_empty() || !update.members_removed.is_empty()
                {
                    if !send(update).await {
                        return;
                    }
                    sent = current;
                    synced = true;
                }
                tokio::select! {
                    _ = version.wait_until(|v| *v != at) => {},
                    _ = tick.tick() => if !send(pb::SiblingUpdate::default()).await { return },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
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
    state: Mutex<BTreeMap<String, Option<Members>>>,
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
        self.state.lock().unwrap().clone()
    }

    /// Bumped on every change to [`Siblings::state`].
    pub fn version(&self) -> u64 {
        self.changed.get()
    }

    pub async fn changed(&self, since: u64) {
        self.changed.wait_until(|v| *v != since).await;
    }

    fn set(&self, sibling: &str, state: Option<Members>) {
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
            let mut synced = false;
            loop {
                let update = self
                    .peers
                    .call(sibling, Class::Control, IDLE, stream.message())
                    .await?
                    .ok_or_else(|| Status::unavailable("sibling relay closed"))?;
                if synced && update.members.is_empty() && update.members_removed.is_empty() {
                    continue;
                }
                for member in update.members_removed {
                    members.remove(&key(&member));
                }
                for member in update.members {
                    members.insert(key(&member), member);
                }
                synced |= update.synced;
                if synced {
                    self.set(sibling, Some(members.clone()));
                }
            }
        }
        .await;
        result
            .err()
            .unwrap_or_else(|| Status::unavailable("sibling relay closed"))
    }
}
