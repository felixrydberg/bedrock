//! `bedrock-cdn`: hosts the `cdn` dataset (retention metadata) and artifact bytes.

mod artifact_storage {}
mod artifact_transfer {}
mod manifests {}
mod retention {}

use bedrock_lib::node::{self, ConfigError, ServiceConfig};

const DATASETS: &[&str] = &["cdn"];

#[tokio::main]
async fn main() -> Result<(), ConfigError> {
    node::init_tracing();
    let config: ServiceConfig = node::load()?;
    config.validate(DATASETS)?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-cdn");
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    node::shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
}
