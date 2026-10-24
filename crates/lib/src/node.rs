//! Node startup shared by every binary: configuration, logging and shutdown.
//!
//! Configuration is read from the TOML file named by `BEDROCK_CONFIG` (default `bedrock.toml`, optional),
//! then overridden by `BEDROCK_*` environment variables. Nested keys use `__`, e.g.
//! `BEDROCK_LOCATION__CELL=eu-1a`; lists use `[a, b]`, e.g. `BEDROCK_ROLES=[l1, l2]`.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// Settings every binary has.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub location: Location,
    pub listen: SocketAddr,
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
#[derive(Debug, Clone, Default, Deserialize)]
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
    /// `bedrock-api` endpoint for writes.
    pub api: String,
    pub s3_endpoint: String,
}

impl ServiceConfig {
    /// Rejects configs that name no roles or relays, or datasets this binary does not host.
    pub fn validate(&self, hosted_datasets: &[&str]) -> Result<(), ConfigError> {
        if self.roles.is_empty() {
            return Err(ConfigError("roles must name at least one of l1, l2".into()));
        }
        if self.datasets.is_empty() {
            return Err(ConfigError("datasets must not be empty".into()));
        }
        if let Some(d) = self
            .datasets
            .iter()
            .find(|d| !hosted_datasets.contains(&d.as_str()))
        {
            return Err(ConfigError(format!(
                "dataset {d:?} is not hosted by this binary (hosted: {hosted_datasets:?})"
            )));
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

/// `bedrock-api`.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub s3_endpoint: String,
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
    let config: T = Figment::new()
        .merge(Toml::file(&path))
        .merge(Env::prefixed("BEDROCK_").split("__").ignore(&["config"]))
        .extract()
        .map_err(|e| ConfigError(format!("config ({path}): {e}")))?;
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
mod tests {
    use super::*;
    use figment::Jail;

    const KV_TOML: &str = r#"
        node_id = "kv-1"
        listen = "0.0.0.0:7100"
        data_dir = "data"
        roles = ["l1"]
        datasets = ["quicksilver", "kv"]
        relays = ["relay-1:7300"]
        api = "api:7000"
        s3_endpoint = "http://minio:9000"

        [location]
        region = "eu"
        dc = "eu-1"
        cell = "eu-1a"
    "#;

    #[test]
    #[allow(clippy::disallowed_methods)] // checks that `load` created the data directory
    fn file_then_env_overrides() {
        Jail::expect_with(|jail| {
            jail.create_file("kv.toml", KV_TOML)?;
            jail.set_env("BEDROCK_CONFIG", "kv.toml");
            jail.set_env("BEDROCK_ROLES", "[l1, l2]");
            jail.set_env("BEDROCK_LOCATION__CELL", "eu-1b");
            jail.set_env("BEDROCK_BUDGET__MEMORY_MB", "512");

            let c: ServiceConfig = load().map_err(|e| e.0)?;
            assert_eq!(c.roles, [Role::L1, Role::L2]);
            assert_eq!(c.node.location.cell, "eu-1b");
            assert_eq!(c.node.budget.memory_mb, Some(512));
            assert!(c.node.data_dir.is_dir());
            c.validate(&["quicksilver", "kv", "auth"])
                .map_err(|e| e.0)?;
            Ok(())
        });
    }

    #[test]
    fn rejects_foreign_dataset_and_empty_roles() {
        Jail::expect_with(|jail| {
            jail.create_file("bedrock.toml", KV_TOML)?;
            let mut c: ServiceConfig = load().map_err(|e| e.0)?;
            assert!(c.validate(&["cdn"]).is_err());
            c.roles.clear();
            assert!(c.validate(&["quicksilver", "kv"]).is_err());
            Ok(())
        });
    }

    #[test]
    fn relay_tier_and_upstreams() {
        Jail::expect_with(|jail| {
            jail.set_env("BEDROCK_NODE_ID", "relay-1");
            jail.set_env("BEDROCK_LISTEN", "0.0.0.0:7300");
            jail.set_env("BEDROCK_DATA_DIR", "data");
            jail.set_env("BEDROCK_LOCATION__REGION", "eu");
            jail.set_env("BEDROCK_LOCATION__DC", "eu-1");
            jail.set_env("BEDROCK_LOCATION__CELL", "eu-1a");
            jail.set_env("BEDROCK_TIER", "cell");
            jail.set_env("BEDROCK_UPSTREAMS", "[]");

            let c: RelayConfig = load().map_err(|e| e.0)?;
            assert_eq!(c.tier, Tier::Cell);
            assert!(c.validate().is_err());
            Ok(())
        });
    }
}
