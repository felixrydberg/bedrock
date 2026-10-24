//! `bedrock-api`: authoritative writes and revision ordering (B-09).

use bedrock_lib::node::{self, ApiConfig, ConfigError};

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ApiConfig) -> Result<(), ConfigError> {
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-api");
    node::shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
}
