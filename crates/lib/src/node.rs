//! Node startup shared by every binary: configuration, logging and shutdown.
//!
//! Configuration is read from the TOML file named by `BEDROCK_CONFIG` (default `bedrock.toml`, optional),
//! then overridden by `BEDROCK_*` environment variables. Nested keys use `__`, e.g.
//! `BEDROCK_LOCATION__CELL=eu-1a`; lists use `[a, b]`, e.g. `BEDROCK_ROLES=[l1, l2]`.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::replication::materialized::Retention;
use crate::s3::S3Config;
use crate::transport::admin::Admin;

/// Settings every binary has.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub location: Location,
    pub listen: SocketAddr,
    /// Serves `/metrics`, `/health` and `/ready`.
    pub admin_listen: SocketAddr,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub budget: Budget,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Location {
    pub region: String,
    pub dc: String,
    pub cell: String,
}

/// Resource budgets for the process; unset means no limit.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Budget {
    pub cpus: Option<f64>,
    pub memory_mb: Option<u64>,
    pub disk_mb: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    L1,
    L2,
}

/// `bedrock-kv` and `bedrock-cdn`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub roles: Vec<Role>,
    pub datasets: Vec<String>,
    /// Relay endpoints to subscribe through, cell relays first.
    pub relays: Vec<String>,
    /// `bedrock-api` endpoint for writes, its `write_listen`.
    pub api: String,
    pub s3_endpoint: String,
}

impl ServiceConfig {
    /// Rejects configs that name no roles, datasets or relays. `datasets::Registry::open` rejects datasets this
    /// binary does not host.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.roles.is_empty() {
            return Err(ConfigError("roles must name at least one of l1, l2".into()));
        }
        if self.datasets.is_empty() {
            return Err(ConfigError("datasets must not be empty".into()));
        }
        if self.relays.is_empty() {
            return Err(ConfigError("relays must not be empty".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Region,
    Dc,
    Cell,
}

/// `bedrock-relay`. Relays have no roles.
#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub tier: Tier,
    /// Upstream relays, or `bedrock-api` for the top tier, in preference order.
    pub upstreams: Vec<String>,
}

impl RelayConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.upstreams.is_empty() {
            return Err(ConfigError("upstreams must not be empty".into()));
        }
        Ok(())
    }
}

/// `bedrock-api`. Relays subscribe on `listen`; writes arrive over HTTP on `write_listen`.
#[derive(Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub write_listen: SocketAddr,
    /// Required bearer credential granting write access to every dataset and namespace.
    pub write_token: String,
    /// SlateDB's durable namespace databases.
    pub s3: S3Config,
    #[serde(default)]
    pub retention: Retention,
}

impl fmt::Debug for ApiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiConfig")
            .field("node", &self.node)
            .field("write_listen", &self.write_listen)
            .field("write_token", &"<redacted>")
            .field("s3", &self.s3)
            .field("retention", &self.retention)
            .finish()
    }
}

macro_rules! as_node {
    ($($t:ty),*) => {$(
        impl AsRef<NodeConfig> for $t {
            fn as_ref(&self) -> &NodeConfig {
                &self.node
            }
        }
    )*};
}
as_node!(ServiceConfig, RelayConfig, ApiConfig);

pub struct ConfigError(pub String);

impl fmt::Debug for ConfigError {
    // `main` returning `Err` prints `Debug`; keep it readable.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Loads config from `BEDROCK_CONFIG` plus `BEDROCK_*` overrides and creates the data directory.
// Startup adapter: reads the environment and creates the data directory before any simulated I/O runs.
#[allow(clippy::disallowed_methods)]
pub fn load<T: DeserializeOwned + AsRef<NodeConfig>>() -> Result<T, ConfigError> {
    let path = std::env::var("BEDROCK_CONFIG").unwrap_or_else(|_| "bedrock.toml".into());
    let mut source = Figment::new().merge(Toml::file(&path)).merge(
        Env::prefixed("BEDROCK_")
            .split("__")
            .ignore(&["config", "write_token"]),
    );
    // Credentials must stay literal: Env's value parser interprets numbers, booleans and quoted strings.
    let token_env = Env::prefixed("BEDROCK_").only(&["write_token"]);
    let mut tokens = token_env.iter();
    if let Some((_, token)) = tokens.next() {
        if tokens.next().is_some() {
            return Err(ConfigError("multiple BEDROCK_WRITE_TOKEN variables".into()));
        }
        source = source.merge(("write_token", token));
    }
    let config: T = source.extract().map_err(|e| {
        if e.path
            .first()
            .is_some_and(|key| key.eq_ignore_ascii_case("write_token"))
        {
            ConfigError(format!("config ({path}): write_token must be a string"))
        } else {
            ConfigError(format!("config ({path}): {e}"))
        }
    })?;
    let dir = &config.as_ref().data_dir;
    std::fs::create_dir_all(dir)
        .map_err(|e| ConfigError(format!("data_dir {}: {e}", dir.display())))?;
    Ok(config)
}

/// Logs to stderr, filtered by `RUST_LOG` (default `info`).
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
}

/// Binds `admin_listen` and serves the admin endpoint in the background; the node is not ready until it says so.
pub async fn start_admin(node: &NodeConfig) -> Result<Arc<Admin>, ConfigError> {
    let listener = tokio::net::TcpListener::bind(node.admin_listen)
        .await
        .map_err(|e| ConfigError(format!("admin_listen {}: {e}", node.admin_listen)))?;
    let admin = Admin::new();
    tokio::spawn(admin.clone().serve(listener));
    Ok(admin)
}

/// Resolves on Ctrl-C or SIGTERM (what `docker stop` sends).
pub async fn shutdown_signal() {
    // madsim simulates only Ctrl-C.
    #[cfg(all(unix, not(madsim)))]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(any(not(unix), madsim))]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
// `Jail::expect_with` closures must return `figment::Result`; newer clippy flags `figment::Error` as large.
#[allow(clippy::result_large_err)]
#[path = "../tests/unit/node.rs"]
mod tests;
