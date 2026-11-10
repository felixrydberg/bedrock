//! Node startup shared by every binary: configuration, logging and shutdown.
//!
//! Configuration is read from the TOML file named by `BEDROCK_CONFIG` (default `bedrock.toml`, optional),
//! then overridden by `BEDROCK_*` environment variables. Nested keys use `__`, e.g.
//! `BEDROCK_LOCATION__CELL=eu-1a`; lists use `[a, b]`, e.g. `BEDROCK_ROLES=[l1, l2]`.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::admin::Admin;
use crate::replication::materialized::Retention;
use crate::s3::S3Config;

/// Settings every binary has.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub location: Location,
    pub listen: SocketAddr,
    /// `host:port` other nodes dial; defaults to `listen` (B-12 membership entries carry it).
    #[serde(default)]
    pub advertise: Option<String>,
    /// Serves `/metrics`, `/health` and `/ready`.
    pub admin_listen: SocketAddr,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub budget: Budget,
    /// Mutual TLS for node-to-node traffic (B-55); unset means plaintext.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// PEM files from any CA; the certificate contract is in [`crate::transport::tls`].
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TlsConfig {
    /// This node's certificate chain, leaf first.
    pub cert: PathBuf,
    pub key: PathBuf,
    /// The CA certificates that issue node certificates.
    pub ca: PathBuf,
}

impl NodeConfig {
    pub fn advertised(&self) -> String {
        self.advertise
            .clone()
            .unwrap_or_else(|| self.listen.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Location {
    pub region: String,
    pub dc: String,
    pub cell: String,
    /// Placement keeps replicas in different failure domains (e.g. a rack); empty means none.
    #[serde(default)]
    pub failure_domain: String,
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

/// `bedrock-quicksilver`, `bedrock-kv` and `bedrock-cdn`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub roles: Vec<Role>,
    /// Relay-fed datasets this process hosts; `bedrock-kv` hosts none and leaves it empty.
    #[serde(default)]
    pub datasets: Vec<String>,
    /// Relay endpoints to subscribe through, cell relays first.
    pub relays: Vec<String>,
    /// `bedrock-api` endpoint for writes, its `write_listen`.
    pub api: String,
    pub s3_endpoint: String,
}

impl ServiceConfig {
    /// Rejects configs that name no roles or relays. `datasets::Registry::open` rejects empty dataset lists and
    /// datasets this binary does not host.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.roles.is_empty() {
            return Err(ConfigError("roles must name at least one of l1, l2".into()));
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
#[derive(Clone, Deserialize)]
pub struct RelayConfig {
    #[serde(flatten)]
    pub node: NodeConfig,
    pub tier: Tier,
    /// Upstream relays, or `bedrock-api` for the top tier, in preference order.
    pub upstreams: Vec<String>,
    /// Same-tier sibling relays: recovery sources and the sibling stream (B-12), by advertised address.
    #[serde(default)]
    pub replay_peers: Vec<String>,
    #[serde(default)]
    pub replay: ReplaySettings,
    /// Cell-tier membership publication (B-12); absent disables it.
    #[serde(default)]
    pub membership: Option<MembershipSettings>,
    /// Optional cell-local operator credential for drain writes. Membership publication uses Raft, not this token.
    #[serde(default)]
    pub write_token: Option<String>,
}

impl fmt::Debug for RelayConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayConfig")
            .field("node", &self.node)
            .field("tier", &self.tier)
            .field("upstreams", &self.upstreams)
            .field("replay_peers", &self.replay_peers)
            .field("replay", &self.replay)
            .field("membership", &self.membership)
            .field(
                "write_token",
                &self.write_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// The cell's relays are this relay and its `replay_peers`; publishing needs a majority of them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipSettings {
    /// Every configured voter address, including this relay, mapped to its unique certificate node ID.
    pub voter_ids: BTreeMap<String, String>,
    /// Explicit first initialization of an empty fixed-voter control store. Disable after initial cluster creation;
    /// a replacement disk must recover the existing cluster, never silently form a new voting replica.
    #[serde(default)]
    pub bootstrap: bool,
    /// How long a disconnected node stays a member; it may reconnect to any cell relay meanwhile.
    #[serde(default = "default_grace_ms")]
    pub grace_ms: u64,
}

/// Longer than a silent relay's detection ([`crate::relays::IDLE`]) minus its progress interval, so failover fits.
fn default_grace_ms() -> u64 {
    20_000
}

/// Optional overrides preserve tier defaults without overriding an explicit window.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplaySettings {
    pub max_bytes: Option<u64>,
    pub window_ms: Option<u64>,
    pub priorities: std::collections::BTreeMap<String, u32>,
}

impl RelayConfig {
    /// How long a lost sibling's interests stay: the membership grace, whose default upper tiers use (#41 / B-12b).
    pub fn grace(&self) -> std::time::Duration {
        let grace = self.membership.as_ref().map(|m| m.grace_ms);
        std::time::Duration::from_millis(grace.unwrap_or_else(default_grace_ms))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.upstreams.is_empty() && self.membership.is_none() {
            return Err(ConfigError(
                "upstreams must not be empty without cell membership".into(),
            ));
        }
        if let Some(membership) = &self.membership {
            #[cfg(not(madsim))]
            if self.node.tls.is_none() {
                return Err(ConfigError("cell membership requires mutual TLS".into()));
            }
            if self.tier != Tier::Cell {
                return Err(ConfigError("membership requires a cell-tier relay".into()));
            }
            let advertised = self.node.advertised();
            // Siblings elect the publisher by address, so it must be the one they dial.
            if advertised
                .parse::<SocketAddr>()
                .is_ok_and(|address| address.ip().is_unspecified())
            {
                return Err(ConfigError(
                    "membership requires advertise when listen is an unspecified address".into(),
                ));
            }
            if self.replay_peers.contains(&advertised) {
                return Err(ConfigError(
                    "replay_peers must not list this relay's advertised address".into(),
                ));
            }
            let location = &self.node.location;
            if ![&location.region, &location.dc, &location.cell]
                .into_iter()
                .all(|part| {
                    !part.is_empty()
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                })
            {
                return Err(ConfigError(
                    "cell control requires plain, nonempty region, dc and cell identifiers".into(),
                ));
            }
            let mut voters = self.replay_peers.clone();
            voters.sort();
            if voters.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(ConfigError("replay_peers contains duplicate voters".into()));
            }
            voters.push(advertised.clone());
            crate::membership::control::validate_voter_ids(&voters, &membership.voter_ids)
                .map_err(ConfigError)?;
            if membership.voter_ids.get(&advertised) != Some(&self.node.node_id) {
                return Err(ConfigError(
                    "local control voter identity differs from node_id".into(),
                ));
            }
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
