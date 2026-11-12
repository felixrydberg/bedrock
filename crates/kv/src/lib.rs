//! `bedrock-kv`: customer KV. It hosts no relay-fed datasets; it only joins the cell membership as service `kv`.
//! It reads the namespace directory from Quicksilver through the shared client and, with `manage_listen` set, serves
//! namespace management ([`namespaces::routes`]). Home-region namespaces are read from S3 through [`storage::Readers`].

pub mod namespaces;
pub mod storage;

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig, ServiceConfig};
use bedrock_lib::s3::S3Config;
use serde::Deserialize;

/// How often idle readers are closed and retired ones released.
const SWEEP: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone, Deserialize)]
pub struct KvConfig {
    #[serde(flatten)]
    pub service: ServiceConfig,
    /// Serves namespace management when set. Callers send `Authorization: Bearer <write_token>`: the credential that
    /// already lets them write the directory through `bedrock-api`.
    #[serde(default)]
    pub manage_listen: Option<SocketAddr>,
    /// `bedrock-api`'s write credential, required with `manage_listen`.
    #[serde(default)]
    pub write_token: Option<String>,
    /// The bucket `bedrock-api` writes namespace databases to; readers only read it, except for their managed
    /// checkpoints and purges. Required outside simulation.
    #[serde(default)]
    pub s3: Option<S3Config>,
}

impl From<ServiceConfig> for KvConfig {
    fn from(service: ServiceConfig) -> Self {
        Self {
            service,
            manage_listen: None,
            write_token: None,
            s3: None,
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
    } = config;
    config.validate()?;
    check_hosts_nothing_pushed(&config)?;
    if manage_listen.is_some() && write_token.is_none() {
        return Err(ConfigError("manage_listen requires write_token".into()));
    }
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-kv");
    // ponytail: roles are only logged; the read path (B-27) decides what l1 and l2 serve.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let memory_mb = config.node.budget.memory_mb.unwrap_or(1024);
    let factory = namespace_factory(s3.as_ref(), memory_mb)?;
    let admin = node::start_admin(&config.node).await?;
    let readers = storage::Readers::new(
        &admin,
        factory,
        config.node.location.region.clone(),
        config.node.location.cell.clone(),
        storage::max_open(memory_mb),
    );
    let (members, target) = membership::View::new("kv", &config.node.location.cell);
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
        config.node.location.failure_domain.clone(),
    ));
    let manage = match (manage_listen, write_token) {
        (Some(listen), Some(token)) => {
            let directory = Arc::new(namespaces::Directory::new(
                &admin,
                client,
                namespaces::Api {
                    address: config.api.clone(),
                    token: token.clone(),
                    tls: tls.clone(),
                },
                readers.clone(),
                config.node.location.region.clone(),
            ));
            let app = bedrock_lib::http::bearer(namespaces::routes(directory), &token)?;
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(|e| ConfigError(format!("manage_listen {listen}: {e}")))?;
            Some(tokio::spawn(bedrock_lib::http::serve(listener, app, tls)))
        }
        _ => None,
    };
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut sweep = tokio::time::interval(SWEEP);
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
            _=sweep.tick()=>readers.sweep().await,
        }
    }
    admin.set_ready(false);
    if let Some(manage) = manage {
        manage.abort();
    }
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
}

/// The namespace databases `bedrock-api` writes, read through one block cache of a quarter of the memory budget.
#[cfg(not(madsim))]
fn namespace_factory(
    s3: Option<&S3Config>,
    memory_mb: u64,
) -> Result<Arc<dyn bedrock_lib::storage::namespace::NamespaceFactory>, ConfigError> {
    let s3 = s3.ok_or_else(|| ConfigError("bedrock-kv needs [s3] to read namespaces".into()))?;
    let store =
        bedrock_lib::s3::object_store(s3).map_err(|error| ConfigError(error.to_string()))?;
    Ok(Arc::new(
        bedrock_lib::storage::slate::SlateFactory::new(store)
            .with_reader_cache_bytes(memory_mb * 1024 * 1024 / 4),
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
