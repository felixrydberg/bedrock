//! `bedrock-kv`: customer KV. It hosts no relay-fed datasets; it only joins the cell membership as service `kv`.
//! It reads the namespace directory from Quicksilver through the shared client. Each namespace's owner in its home cell
//! writes and reads it through an embedded owner writer ([`storage::Owners`], #58 / B-26); every node takes Cloudflare's
//! value writes ([`operations::routes`]) on its `listen` address and forwards them to the owner. With `manage_listen`
//! set, it also serves namespace management ([`namespaces::routes`]) and the value routes there.

pub mod namespaces;
pub mod operations;
pub mod storage;

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig, ServiceConfig};
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Writer};
use bedrock_lib::s3::S3Config;
use serde::Deserialize;

/// How often idle namespace handles are closed.
const SWEEP: Duration = Duration::from_secs(60);
/// How often handles of namespaces that membership moved away are closed.
const RELEASE: Duration = Duration::from_secs(1);
/// Customer KV keeps retained events only briefly: no relay replays them, and a retry is answered from its retry record,
/// which keeps the default day.
const KV_EVENT_RETENTION_MS: u64 = 60_000;

/// Another cell, for forwarding writes to namespaces whose home it is.
#[derive(Clone, Debug, Deserialize)]
pub struct CellConfig {
    pub region: String,
    /// A `bedrock-kv` `listen` address in that cell, or its Gateway once B-57 exists.
    // ponytail: static entries until Gateways route between cells (B-57).
    pub address: String,
}

fn kv_retention() -> Retention {
    Retention {
        event_retention_ms: KV_EVENT_RETENTION_MS,
        ..Retention::default()
    }
}

#[derive(Clone, Deserialize)]
pub struct KvConfig {
    #[serde(flatten)]
    pub service: ServiceConfig,
    /// Serves namespace management and value writes when set. Callers send `Authorization: Bearer <write_token>`.
    #[serde(default)]
    pub manage_listen: Option<SocketAddr>,
    /// Required. The credential for value writes and forwards between `bedrock-kv` nodes, and `bedrock-api`'s, which
    /// writes the namespace directory.
    #[serde(default)]
    pub write_token: Option<String>,
    /// The bucket namespace databases live in, at `kv/{id}/`. Required outside simulation.
    #[serde(default)]
    pub s3: Option<S3Config>,
    /// The other cells, by name: namespaces are created in a cell of their jurisdiction, and writes for another cell's
    /// namespaces are forwarded there.
    #[serde(default)]
    pub cells: BTreeMap<String, CellConfig>,
    /// How long the owner writer keeps events and retry results.
    #[serde(default = "kv_retention")]
    pub retention: Retention,
}

impl From<ServiceConfig> for KvConfig {
    fn from(service: ServiceConfig) -> Self {
        Self {
            service,
            manage_listen: None,
            write_token: None,
            s3: None,
            cells: BTreeMap::new(),
            retention: kv_retention(),
        }
    }
}

impl AsRef<NodeConfig> for KvConfig {
    fn as_ref(&self) -> &NodeConfig {
        &self.service.node
    }
}

impl fmt::Debug for KvConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvConfig")
            .field("service", &self.service)
            .field("manage_listen", &self.manage_listen)
            .field(
                "write_token",
                &self.write_token.as_ref().map(|_| "<redacted>"),
            )
            .field("s3", &self.s3)
            .field("cells", &self.cells)
            .field("retention", &self.retention)
            .finish()
    }
}

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// Returns configuration, admin startup, or relay session initialization errors.
pub async fn run(config: KvConfig) -> Result<(), ConfigError> {
    let KvConfig {
        service: config,
        manage_listen,
        write_token,
        s3,
        cells,
        retention,
    } = config;
    config.validate()?;
    check_hosts_nothing_pushed(&config)?;
    let token = write_token.ok_or_else(|| ConfigError("bedrock-kv needs write_token".into()))?;
    retention
        .validate()
        .map_err(|error| ConfigError(error.to_string()))?;
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-kv");
    // ponytail: roles are only logged; the read path (B-27) decides what l1 and l2 serve.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let location = &config.node.location;
    let memory_mb = config.node.budget.memory_mb.unwrap_or(1024);
    let factory = namespace_factory(s3.as_ref(), memory_mb)?;
    let admin = node::start_admin(&config.node).await?;
    let writer = Writer::owner(
        now,
        Persistence {
            factory,
            retention,
            metrics: PersistenceMetrics::new(&admin),
        },
        storage::max_open(memory_mb),
    )
    .map_err(|error| ConfigError(error.to_string()))?;
    let (members, target) = membership::View::new("kv", &location.cell);
    let members = Arc::new(members);
    let owners = storage::Owners::new(
        &admin,
        writer.clone(),
        config.node.node_id.clone(),
        location.region.clone(),
        location.cell.clone(),
        Arc::new({
            let members = members.clone();
            move || members.members().unwrap_or_default()
        }),
    );
    let tls = bedrock_lib::transport::tls::start(&config.node, "kv", &admin).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(tls.clone()));
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        membership::member(&config, "kv"),
        // Customer KV has no ranges.
        Default::default(),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    // The directory is read from Quicksilver holders found through the cell relays, not from a local copy.
    let client = Arc::new(bedrock_lib::client::Client::new(
        &admin,
        peers.clone(),
        config.relays.clone(),
        location.failure_domain.clone(),
    ));
    let mut known_cells: Vec<(String, String)> = cells
        .iter()
        .map(|(cell, entry)| (entry.region.clone(), cell.clone()))
        .collect();
    known_cells.push((location.region.clone(), location.cell.clone()));
    let directory = Arc::new(namespaces::Directory::new(
        &admin,
        client,
        namespaces::Api {
            address: config.api.clone(),
            token: token.clone(),
            tls: tls.clone(),
        },
        owners.clone(),
        location.region.clone(),
        known_cells,
    ));
    let values = Arc::new(operations::Values {
        directory: directory.clone(),
        owners: owners.clone(),
        token: token.clone(),
        tls: tls.clone(),
        cells: cells
            .into_iter()
            .map(|(cell, entry)| (cell, entry.address))
            .collect(),
    });
    // Other nodes forward writes to the address this node announces in membership.
    let internal = bedrock_lib::http::bearer(operations::routes(values.clone()), &token)?;
    let listener = tokio::net::TcpListener::bind(config.node.listen)
        .await
        .map_err(|e| ConfigError(format!("listen {}: {e}", config.node.listen)))?;
    let internal = tokio::spawn(bedrock_lib::http::serve(listener, internal, tls.clone()));
    let manage = match manage_listen {
        Some(listen) => {
            let app = namespaces::routes(directory).merge(operations::routes(values));
            let app = bedrock_lib::http::bearer(app, &token)?;
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(|e| ConfigError(format!("manage_listen {listen}: {e}")))?;
            Some(tokio::spawn(bedrock_lib::http::serve(listener, app, tls)))
        }
        None => None,
    };
    let mut readiness = tokio::time::interval(Duration::from_millis(100));
    // Its own task, so a slow handle close never delays readiness or shutdown.
    let sweeping = tokio::spawn({
        let owners = owners.clone();
        async move {
            let mut sweep = tokio::time::interval(SWEEP);
            let mut release = tokio::time::interval(RELEASE);
            loop {
                tokio::select! {
                    _ = sweep.tick() => owners.sweep().await,
                    _ = release.tick() => owners.release_moved().await,
                }
            }
        }
    });
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            _=readiness.tick()=>{
                admin.set_ready(session.ready());
                if let Err(error)=members.refresh(&session,&peers){
                    tracing::warn!(%error,"membership view unreadable");
                }
            },
        }
    }
    admin.set_ready(false);
    sweeping.abort();
    internal.abort();
    if let Some(manage) = manage {
        manage.abort();
    }
    session.drain().await;
    // Flushes what is buffered; a write still waiting for its flush was never acknowledged.
    if let Err(error) = writer.close().await {
        tracing::warn!(%error, "closing namespace handles failed");
    }
    tracing::info!("shutting down");
    Ok(())
}

/// Milliseconds since the Unix epoch, for `committed_at`. Wall time is the point here; madsim simulates it.
#[allow(clippy::disallowed_methods)]
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The namespace databases, opened through one block cache of a quarter of the memory budget.
#[cfg(not(madsim))]
fn namespace_factory(
    s3: Option<&S3Config>,
    memory_mb: u64,
) -> Result<Arc<dyn bedrock_lib::storage::namespace::NamespaceFactory>, ConfigError> {
    let s3 =
        s3.ok_or_else(|| ConfigError("bedrock-kv needs [s3] for namespace databases".into()))?;
    let store =
        bedrock_lib::s3::object_store(s3).map_err(|error| ConfigError(error.to_string()))?;
    Ok(Arc::new(
        bedrock_lib::storage::slate::SlateFactory::new(store)
            .with_cache_bytes(memory_mb * 1024 * 1024 / 4),
    ))
}

#[cfg(madsim)]
fn namespace_factory(
    _: Option<&S3Config>,
    _: u64,
) -> Result<Arc<dyn bedrock_lib::storage::namespace::NamespaceFactory>, ConfigError> {
    Ok(bedrock_lib::sim::namespaces())
}

/// Customer KV has no pushed data; Quicksilver and CDN have separate binaries.
fn check_hosts_nothing_pushed(config: &ServiceConfig) -> Result<(), ConfigError> {
    if config.datasets.is_empty() {
        return Ok(());
    }
    Err(ConfigError(
        "bedrock-kv hosts no relay-fed datasets; datasets must be empty".into(),
    ))
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
