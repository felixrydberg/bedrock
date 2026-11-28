//! `bedrock-compute`: Compute hosts (`statefull/v2.md` §3, §9). It depends only on `bedrock-lib`. It holds the actor
//! host and the volume client of stateful actors (#112 / S-04, #115 / S-07) and the one execution seam ([`runtime`]).
//! [`bindings`] sends a Worker's actor calls to the cell Gateway, and [`actors::serve`] takes them from it (#116 / S-08).
//!
//! [`run`] is the `bedrock-compute` binary (#147 / S-20): it joins its cell relay as a `compute` member with no datasets,
//! so the cell's Gateways route actor events to it, and serves `Actors` on the internal mTLS listener. It places an
//! actor's slots from its own cell's `volume` table only; sibling cells' tables come with #148 / S-20b. The sandbox is
//! S-10.

pub mod actors;
pub mod bindings;
pub mod builtin;
pub mod runtime;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_lib::actors::{ActorId, SLOTS, Topology};
use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::{COMPUTE, VOLUME};
use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig};
use bedrock_lib::replication::pb as rpb;
use bedrock_lib::volume as pb;
use serde::Deserialize;

use actors::host::{ActorHost, Placement};
use actors::volume::{self, Slot};
use runtime::{Error, Executor, RustExecutor};

#[derive(Debug, Clone, Deserialize)]
pub struct ComputeConfig {
    /// `listen` is the internal mTLS listener.
    #[serde(flatten)]
    pub node: NodeConfig,
    /// This cell's relays, tried in order.
    pub relays: Vec<String>,
    #[serde(default)]
    pub sockets: bedrock_lib::websockets::Limits,
}

impl AsRef<NodeConfig> for ComputeConfig {
    fn as_ref(&self) -> &NodeConfig {
        &self.node
    }
}

/// Places an actor's slots through the region's topology record and this cell's `volume` routes. A slot in a sibling
/// cell fails closed in [`volume::slot_nodes`] until #148 / S-20b.
// ponytail: every wake reads the topology from Quicksilver and asks for a newer `volume` table, which in steady state asks
// each cell relay in turn (and counts a "stale" refresh); cache both with S-20b's shared lookup if wake latency matters.
struct OwnCell {
    client: Arc<Client>,
    cell: String,
    /// The last `volume` table revision placed from: each wake asks for a newer one, so a handoff or a directory that
    /// was empty at boot is never placed from twice.
    seen: Mutex<Option<u64>>,
}

#[async_trait]
impl Placement for OwnCell {
    async fn slots(&self, actor: &pb::Actor) -> Result<([Slot; SLOTS], u64), Error> {
        let unavailable = |e: String| Error::Volume(volume::Error::Unavailable(e));
        let region = actor
            .id
            .rsplit_once('_')
            .map(|(region, _)| region)
            .ok_or_else(|| Error::Unsupported("an actor ID names its region".into()))?;
        let (topology, _) = Topology::read(&self.client, region, 0)
            .await
            .map_err(|e| unavailable(format!("topology of {region}: {e}")))?;
        let regions = BTreeMap::from([(region.to_owned(), topology.clone())]);
        let id = ActorId::parse(&regions, &actor.id)
            .map_err(|e| Error::Unsupported(format!("actor ID: {e}")))?;
        let seen = *self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let table = self
            .client
            .routes(VOLUME, seen)
            .await
            .map_err(|e| unavailable(format!("volume routes: {}", e.message())))?;
        *self.seen.lock().unwrap_or_else(PoisonError::into_inner) = Some(table.revision);
        let tables = BTreeMap::from([(self.cell.clone(), table)]);
        Ok((
            volume::slot_nodes(&id, &topology, &tables)?,
            topology.version,
        ))
    }
}

/// This host as a member of its cell: no datasets, role `compute`, which Gateways pick wake hosts from.
fn member(node: &NodeConfig) -> rpb::Member {
    rpb::Member {
        node_id: node.node_id.clone(),
        service: COMPUTE.into(),
        region: node.location.region.clone(),
        dc: node.location.dc.clone(),
        cell: node.location.cell.clone(),
        roles: vec![COMPUTE.into()],
        cpus: node.budget.cpus.unwrap_or(0.0),
        memory_mb: node.budget.memory_mb.unwrap_or(0),
        disk_mb: node.budget.disk_mb.unwrap_or(0),
        failure_domain: node.location.failure_domain.clone(),
        status: rpb::member::Status::Ready as i32,
        address: node.advertised(),
        ..Default::default()
    }
}

/// Runs the host until Ctrl-C or SIGTERM; built-in actor kinds run in `builtin`. `main` and the simulation both start
/// here.
// ponytail: awake actors are not released on shutdown; their slots' 5 s lease lets another host claim them. Events
// accepted before shutdown get [`bedrock_lib::transport::DRAIN`] to finish.
pub async fn run(config: ComputeConfig, builtin: Arc<dyn Executor>) -> Result<(), ConfigError> {
    if config.relays.is_empty() {
        return Err(ConfigError("relays must not be empty".into()));
    }
    let node = &config.node;
    let (cell, dc) = (&node.location.cell, &node.location.dc);
    tracing::info!(node_id = %node.node_id, relays = ?config.relays, "starting bedrock-compute");
    let admin = node::start_admin(node).await?;
    let tls = bedrock_lib::transport::tls::start(node, COMPUTE, &admin).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(tls.clone()));
    let client = Arc::new(Client::new(
        &admin,
        peers.clone(),
        config.relays.clone(),
        node.location.failure_domain.clone(),
    ));
    let directory = Arc::new(bedrock_lib::kv::DirectoryView::new(client.clone()));
    // Volume slots accept a holder only as the node its certificate names.
    let holder = pb::Holder {
        cell: cell.clone(),
        host: node.node_id.clone(),
    };
    let placement = OwnCell {
        client: client.clone(),
        cell: cell.clone(),
        seen: Mutex::default(),
    };
    let host = ActorHost::new(
        &admin,
        cell,
        dc,
        volume::Host::start(peers.clone(), holder.clone()),
        Arc::new(placement),
        Arc::new(builtin::KvExecutor::new(&admin, directory.clone(), builtin)),
        // The sandbox executor is S-10; until then customer code is refused as unsupported.
        Arc::new(RustExecutor::new([])),
    )
    .with_sockets(&admin, client.clone(), config.sockets.clone())
    .map_err(|e| ConfigError(e.to_string()))?;
    let (members, target) = membership::View::new(COMPUTE, cell);
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        member(node),
        BTreeMap::new(),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let router = bedrock_lib::transport::server(tls.as_ref()).add_service(
        pb::actors_server::ActorsServer::new(
            actors::serve::Actors::new(host, holder).with_kv(directory),
        )
        .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES),
    );
    let mut server = bedrock_lib::transport::serve(router, node.listen, tls);
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    let mut readiness = tokio::time::interval(Duration::from_millis(100));
    let result = loop {
        tokio::select! {biased;
            () = &mut shutdown => break Ok(()),
            result = &mut server => break result.map_err(|e| ConfigError(format!("listen {}: {e}", node.listen))),
            _ = readiness.tick() => {
                admin.set_ready(session.ready());
                if let Err(error) = members.refresh(&session, &peers) {
                    tracing::warn!(%error, "membership view unreadable");
                }
            },
        }
    };
    admin.set_ready(false);
    session.drain().await;
    server.close().await;
    tracing::info!("shutting down");
    result
}
