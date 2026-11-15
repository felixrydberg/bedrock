//! [`Placement`] over the region's topology record and the `volume` routes of this cell and its siblings (#113 / S-05).
//!
//! An actor's slots are in the cells [`actors::slots`] names; in each, the range holding its hash has the owners. A
//! truncation goes to every owner of those ranges that is not populating: an owner that holds no slot of the actor
//! answers with an empty slot, which needs nothing truncated, so no slot index rule is needed here.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_lib::actors::{self, ActorId, Topology};
use bedrock_lib::client::{Client, Table};
use bedrock_lib::datasets::capabilities::VOLUME;
use bedrock_lib::placement::{self, State};
use bedrock_lib::transport::{Class, Peers, tls};
use bedrock_lib::volume as pb;
use tokio::time::Instant;
use tonic::Status;

use super::Placement;

/// The topology record is static config; a node rereads it this often.
const TOPOLOGY_TTL: Duration = Duration::from_secs(10 * 60);
const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Cells {
    client: Arc<Client>,
    peers: Arc<Peers>,
    cell: String,
    node: String,
    topology: Mutex<BTreeMap<String, (Arc<Topology>, Instant)>>,
    /// Route revisions a failed truncation saw, by cell: the next fetch must be newer.
    stale: Mutex<BTreeMap<String, u64>>,
}

impl Cells {
    pub fn new(client: Arc<Client>, peers: Arc<Peers>, cell: &str, node: &str) -> Self {
        Cells {
            client,
            peers,
            cell: cell.to_owned(),
            node: node.to_owned(),
            topology: Mutex::default(),
            stale: Mutex::default(),
        }
    }

    async fn topology(&self, region: &str) -> Result<Arc<Topology>, Status> {
        let now = bedrock_lib::monotonic_now();
        if let Some((topology, read)) = self
            .topology
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(region)
            && now.duration_since(*read) < TOPOLOGY_TTL
        {
            return Ok(topology.clone());
        }
        let (topology, _) = Topology::read(&self.client, region, 0)
            .await
            .map_err(|e| Status::unavailable(format!("topology of {region}: {e:?}")))?;
        let topology = Arc::new(topology);
        self.topology
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(region.to_owned(), (topology.clone(), now));
        Ok(topology)
    }

    /// The actor's three slot cells.
    async fn slots(&self, actor: &pb::Actor) -> Result<[String; actors::SLOTS], Status> {
        let region = actor
            .id
            .rsplit_once('_')
            .map(|(region, _)| region)
            .ok_or_else(|| Status::invalid_argument("an actor ID names its region"))?;
        let topology = self.topology(region).await?;
        let regions = BTreeMap::from([(region.to_owned(), (*topology).clone())]);
        let id = ActorId::parse(&regions, &actor.id)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;
        let slots = actors::slots(&id, &topology)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;
        Ok(slots.map(str::to_owned))
    }

    async fn routes(&self, cell: &str) -> Result<Arc<Table>, Status> {
        let stale = self
            .stale
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(cell);
        if cell == self.cell {
            self.client.routes(VOLUME, stale).await
        } else {
            self.client.routes_in(cell, VOLUME, stale).await
        }
    }
}

fn hash(actor: &pb::Actor) -> Result<u64, Status> {
    ActorId::hash_of(&actor.id).map_err(|e| Status::invalid_argument(format!("{e:?}")))
}

#[async_trait]
impl Placement for Cells {
    async fn first_here(&self, actor: &pb::Actor) -> Result<bool, Status> {
        Ok(self.slots(actor).await?[0] == self.cell)
    }

    async fn truncate_others(&self, actor: &pb::Actor, through: u64) -> Result<bool, Status> {
        let hash = hash(actor)?;
        let mut cells = self.slots(actor).await?.to_vec();
        cells.sort();
        cells.dedup();
        let mut done = true;
        for cell in cells {
            let table = self.routes(&cell).await?;
            let directory = table.directories.get(VOLUME).cloned().unwrap_or_default();
            for owner in placement::owners(&directory, hash) {
                if owner.state == State::Populating || owner.node == self.node {
                    continue;
                }
                let Some(member) = table.members.get(&owner.node) else {
                    done = false;
                    continue;
                };
                let identity = tls::Identity {
                    service: member.service.clone(),
                    cell: member.cell.clone(),
                    node_id: member.node_id.clone(),
                    host: bedrock_lib::transport::host_of(&member.address).into(),
                };
                let request = pb::TruncateRequest {
                    actor: Some(actor.clone()),
                    through,
                };
                let answered = async {
                    let channel = self
                        .peers
                        .pinned(&member.address, Class::Control, identity)
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                    let call = async move {
                        pb::volume_client::VolumeClient::new(channel)
                            .truncate(request)
                            .await
                    };
                    self.peers
                        .call(&member.address, Class::Control, TIMEOUT, call)
                        .await
                }
                .await;
                let finished = match answered {
                    Ok(answer) => answer.into_inner().slot.is_some_and(|slot| {
                        // A node holding no slot of the actor has nothing to truncate.
                        slot.flushed >= through || (slot.epoch == 0 && slot.end.is_none())
                    }),
                    Err(error) => {
                        tracing::debug!(%error, node = %owner.node, "a slot truncation failed");
                        self.stale
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(cell.clone(), table.revision);
                        false
                    }
                };
                done &= finished;
            }
        }
        Ok(done)
    }

    async fn first_cell(&self, actor: &pb::Actor) -> Result<String, Status> {
        Ok(self.slots(actor).await?[0].clone())
    }
}
