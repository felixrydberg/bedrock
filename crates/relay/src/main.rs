//! `bedrock-relay`: relay tree, fan-out, replay log and cell membership; product logic stays in `bedrock-lib` (B-17).

use bedrock_lib::node::{self, ConfigError, RelayConfig};

#[tokio::main]
async fn main() -> Result<(), ConfigError> {
    node::init_tracing();
    let config: RelayConfig = node::load()?;
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
