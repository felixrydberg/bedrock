//! `bedrock-kv`: hosts the `quicksilver`, `kv` and `auth` datasets.

mod namespaces {}
mod operations {}
mod storage {}

use bedrock_lib::node::{self, ConfigError, ServiceConfig};

const DATASETS: &[&str] = &["quicksilver", "kv", "auth"];

#[tokio::main]
async fn main() -> Result<(), ConfigError> {
    node::init_tracing();
    let config: ServiceConfig = node::load()?;
    config.validate(DATASETS)?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-kv");
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    node::shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
}
