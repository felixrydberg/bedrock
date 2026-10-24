//! `bedrock-relay`: relay tree, fan-out, replay log and cell membership; product logic stays in `bedrock-lib` (B-17).

use bedrock_lib::node::{self, ConfigError, RelayConfig};

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: RelayConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(
        node_id = %config.node.node_id,
        tier = ?config.tier,
        upstreams = ?config.upstreams,
        "starting bedrock-relay"
    );
    node::shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
}
