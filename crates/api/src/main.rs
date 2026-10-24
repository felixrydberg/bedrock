//! `bedrock-api`: authoritative writes and revision ordering (B-09).

use bedrock_lib::node::{self, ApiConfig, ConfigError};

#[tokio::main]
async fn main() -> Result<(), ConfigError> {
    node::init_tracing();
    let config: ApiConfig = node::load()?;
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-api");
    node::shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
}
