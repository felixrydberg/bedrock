//! `bedrock-kv`: customer KV. It hosts no relay-fed datasets; it only joins the cell membership as service `kv`.
//! It reads the namespace directory from Quicksilver through the shared client and, with `manage_listen` set, serves
//! namespace management ([`namespaces::routes`]).

pub mod namespaces;

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig, ServiceConfig};
use serde::Deserialize;

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
}

impl From<ServiceConfig> for KvConfig {
    fn from(service: ServiceConfig) -> Self {
        Self {
            service,
            manage_listen: None,
            write_token: None,
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
    } = config;
    config.validate()?;
    check_hosts_nothing_pushed(&config)?;
    if manage_listen.is_some() && write_token.is_none() {
        return Err(ConfigError("manage_listen requires write_token".into()));
    }
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-kv");
    // ponytail: customer-KV readers (per-namespace S3 databases behind read-only caches) arrive with B-39; roles
    // are only logged until then.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
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
    if let Some(manage) = manage {
        manage.abort();
    }
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
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
