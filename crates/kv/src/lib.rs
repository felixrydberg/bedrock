//! `bedrock-kv`: hosts the `quicksilver`, `kv` and `auth` datasets.

mod namespaces {}
mod operations {}
mod storage {}

use bedrock_lib::node::{self, ConfigError, ServiceConfig};

pub const DATASETS: &[&str] = &["quicksilver", "kv", "auth"];

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate(DATASETS)?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-kv");
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
    admin.set_ready(true);
    node::shutdown_signal().await;
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}
