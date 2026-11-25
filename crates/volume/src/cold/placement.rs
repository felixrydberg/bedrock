//! [`Placement`] over the region's topology record and the `volume` routes of this cell and its siblings (#113 / S-05).
//!
//! An actor's slots are in the cells [`actors::slots`] names; in each, the range holding its hash has the owners. A
//! truncation goes to every owner of those ranges that is not populating: an owner that holds no slot of the actor
//! answers with an empty slot, which needs nothing truncated, so no slot index rule is needed here.
//!
//! Sibling cells are reached through the relays configured for them (`sibling_relays`). Each read of this node's own
//! region's topology checks that configuration against this DC's cells; while it does not match, calls to other cells
//! fail closed.

use std::collections::{BTreeMap, BTreeSet};
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
use crate::repair::Source;

/// The topology record is static config; a node rereads it this often.
const TOPOLOGY_TTL: Duration = Duration::from_secs(10 * 60);
/// How long one slot truncation call may take.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Cells {
    client: Arc<Client>,
    peers: Arc<Peers>,
    region: String,
    dc: String,
    cell: String,
    node: String,
    /// The sibling cells relays are configured for.
    siblings: BTreeSet<String>,
    /// Why the configured siblings do not match this DC's cells, as of the last topology read.
    mismatch: Mutex<Option<String>>,
    topology: Mutex<BTreeMap<String, (Arc<Topology>, Instant)>>,
    /// Route revisions a failed truncation saw, by cell: the next fetch must be newer.
    stale: Mutex<BTreeMap<String, u64>>,
}

impl Cells {
    pub fn new(
        client: Arc<Client>,
        peers: Arc<Peers>,
        location: &bedrock_lib::node::Location,
        node: &str,
        siblings: BTreeSet<String>,
    ) -> Self {
        Cells {
            client,
            peers,
            region: location.region.clone(),
            dc: location.dc.clone(),
            cell: location.cell.clone(),
            node: node.to_owned(),
            siblings,
            mismatch: Mutex::default(),
            topology: Mutex::default(),
            stale: Mutex::default(),
        }
    }

    /// The region's topology, read again when the cached one is older than [`TOPOLOGY_TTL`] or below `version`. A read
    /// still below `version` is a retryable failure: the record's storage revision says nothing of its version.
    async fn topology(&self, region: &str, version: u64) -> Result<Arc<Topology>, Status> {
        let now = bedrock_lib::monotonic_now();
        if let Some((topology, read)) = self
            .topology
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(region)
            && now.duration_since(*read) < TOPOLOGY_TTL
            && topology.version >= version
        {
            return Ok(topology.clone());
        }
        let (topology, _) = Topology::read(&self.client, region, 0)
            .await
            .map_err(|e| Status::unavailable(format!("topology of {region}: {e:?}")))?;
        let topology = Arc::new(topology);
        if region == self.region {
            let mismatch = sibling_mismatch(&topology, &self.dc, &self.cell, &self.siblings);
            if let Some(mismatch) = &mismatch {
                tracing::error!(%mismatch, "sibling cell relays do not match the topology");
            }
            *self.mismatch.lock().unwrap_or_else(PoisonError::into_inner) = mismatch;
        }
        self.topology
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(region.to_owned(), (topology.clone(), now));
        if topology.version < version {
            return Err(Status::unavailable(format!(
                "topology of {region} is at version {}, below {version}",
                topology.version
            )));
        }
        Ok(topology)
    }

    /// The actor's ID and its region's topology, at `version` or newer.
    async fn placed(
        &self,
        actor: &pb::Actor,
        version: u64,
    ) -> Result<(ActorId, Arc<Topology>), Status> {
        let region = actor
            .id
            .rsplit_once('_')
            .map(|(region, _)| region)
            .ok_or_else(|| Status::invalid_argument("an actor ID names its region"))?;
        let topology = self.topology(region, version).await?;
        let regions = BTreeMap::from([(region.to_owned(), (*topology).clone())]);
        let id = ActorId::parse(&regions, &actor.id)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;
        Ok((id, topology))
    }

    /// The actor's three slot cells.
    async fn slots(&self, actor: &pb::Actor) -> Result<[String; actors::SLOTS], Status> {
        let (id, topology) = self.placed(actor, 0).await?;
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
            return self.client.routes(VOLUME, stale).await;
        }
        let mismatch = self
            .mismatch
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(mismatch) = mismatch {
            return Err(Status::failed_precondition(mismatch));
        }
        self.client.routes_in(cell, VOLUME, stale).await
    }
}

/// Why `configured` sibling cells differ from the other cells of `dc` in `topology`, or `None` when they match.
pub fn sibling_mismatch(
    topology: &Topology,
    dc: &str,
    cell: &str,
    configured: &BTreeSet<String>,
) -> Option<String> {
    let Some(found) = topology.dcs.iter().find(|d| d.name == dc) else {
        return Some(format!("the topology has no DC {dc}"));
    };
    let expected: BTreeSet<&String> = found.cells.iter().filter(|c| *c != cell).collect();
    let configured: BTreeSet<&String> = configured.iter().collect();
    let unknown: Vec<_> = configured.difference(&expected).collect();
    let missing: Vec<_> = expected.difference(&configured).collect();
    (!unknown.is_empty() || !missing.is_empty()).then(|| {
        format!("sibling_relays of DC {dc}: not in the DC {unknown:?}, without relays {missing:?}")
    })
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
        let (mut reached, mut failure) = (false, None);
        for cell in cells {
            let table = match self.routes(&cell).await {
                Ok(table) => table,
                Err(error) => {
                    tracing::debug!(%error, %cell, "no routes for a slot cell");
                    done = false;
                    failure = Some(error);
                    continue;
                }
            };
            reached = true;
            // A slot cell always has an owner for the actor's range: a missing directory or owner is a table that is not
            // there yet, never a slot with nothing to truncate.
            let directory = table.directories.get(VOLUME).cloned().unwrap_or_default();
            let owners = placement::owners(&directory, hash);
            if !owners.iter().any(|o| o.state != State::Populating) {
                self.stale
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(cell.clone(), table.revision);
                done = false;
                continue;
            }
            for owner in owners {
                if owner.state == State::Populating || owner.node == self.node {
                    continue;
                }
                let Some(member) = table.members.get(&owner.node) else {
                    self.stale
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(cell.clone(), table.revision);
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
                            .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                            .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
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
        match failure {
            Some(error) if !reached => Err(error),
            _ => Ok(done),
        }
    }

    async fn first_cell(&self, actor: &pb::Actor) -> Result<String, Status> {
        Ok(self.slots(actor).await?[0].clone())
    }

    async fn moving_in(&self, actor: &pb::Actor, version: u64) -> Result<bool, Status> {
        let (id, topology) = self.placed(actor, version).await?;
        let placed = |e: actors::Error| Status::invalid_argument(format!("{e:?}"));
        let Some(before) = actors::dc_before(&id, &topology).map_err(placed)? else {
            return Ok(false);
        };
        let now = actors::dc_of(&id, &topology).map_err(placed)?;
        Ok(before.name != now.name && now.name == self.dc)
    }
}

/// `[start, end)` cut to `[from, to)`, `end` and `to` 0 meaning 2⁶⁴.
fn clip(start: u64, end: u64, from: u64, to: u64) -> (u64, u64) {
    let wide = |e: u64| if e == 0 { 1u128 << 64 } else { u128::from(e) };
    let end = wide(end).min(wide(to));
    (start.max(from), if end == 1 << 64 { 0 } else { end as u64 })
}

#[async_trait]
impl crate::repair::Peers for Cells {
    async fn sources(&self, start: u64, end: u64) -> Result<Vec<Source>, Status> {
        let topology = self.topology(&self.region, 0).await?;
        let dc = topology
            .dcs
            .iter()
            .find(|d| d.name == self.dc)
            .ok_or_else(|| {
                Status::failed_precondition(format!("the topology has no DC {}", self.dc))
            })?;
        let mut sources = Vec::new();
        for cell in &dc.cells {
            let table = self.routes(cell).await?;
            let directory = table.directories.get(VOLUME).cloned().unwrap_or_default();
            let first = placement::start_of(&directory, start);
            let stop = if end == 0 { u128::MAX } else { u128::from(end) };
            for (&from, owners) in directory
                .range(first..)
                .take_while(|(s, _)| u128::from(**s) < stop)
            {
                let (part_start, part_end) =
                    clip(start, end, from, placement::end_of(&directory, from));
                let found: Vec<_> = owners
                    .iter()
                    // Node names are unique within a cell only: a sibling cell's owner may share this node's.
                    .filter(|o| {
                        o.state != State::Populating && !(o.node == self.node && *cell == self.cell)
                    })
                    .map(|o| Source {
                        cell: cell.clone(),
                        node: o.node.clone(),
                        start: part_start,
                        end: part_end,
                    })
                    .collect();
                // This cell's other owners matter only where the DC has fewer than three cells.
                if found.is_empty() && *cell != self.cell {
                    self.stale
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(cell.clone(), table.revision);
                    return Err(Status::unavailable(format!(
                        "cell {cell} has no ready owner of {part_start:016x}"
                    )));
                }
                sources.extend(found);
            }
            if directory.is_empty() && *cell != self.cell {
                return Err(Status::unavailable(format!(
                    "cell {cell} has no volume directory yet"
                )));
            }
        }
        Ok(sources)
    }

    async fn slots(&self, actor: &pb::Actor) -> Result<[String; actors::SLOTS], Status> {
        Cells::slots(self, actor).await
    }

    async fn page(
        &self,
        source: &Source,
        request: pb::RepairRequest,
    ) -> Result<pb::RepairReply, Status> {
        let table = self.routes(&source.cell).await?;
        let Some(member) = table.members.get(&source.node) else {
            self.stale
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(source.cell.clone(), table.revision);
            return Err(Status::unavailable(format!(
                "{} is not in its cell's routes",
                source.node
            )));
        };
        let identity = tls::Identity {
            service: member.service.clone(),
            cell: member.cell.clone(),
            node_id: member.node_id.clone(),
            host: bedrock_lib::transport::host_of(&member.address).into(),
        };
        let channel = self
            .peers
            .pinned(&member.address, Class::Control, identity)
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let call = async move {
            pb::volume_client::VolumeClient::new(channel)
                .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                .repair(request)
                .await
        };
        let reply = self
            .peers
            .call(&member.address, Class::Control, TIMEOUT, call)
            .await;
        // The source no longer serves the span as this view says: the next fetch must be newer.
        if let Err(status) = &reply
            && status.code() == tonic::Code::FailedPrecondition
        {
            self.stale
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(source.cell.clone(), table.revision);
        }
        Ok(reply?.into_inner())
    }
}
