//! `bedrock-relay`: relay tree, fan-out, replay log and cell membership; product logic stays in `bedrock-lib` (B-17).

use bedrock_lib::node::{self, ConfigError, RelayConfig};
use std::sync::Arc;

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: RelayConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(
        node_id = %config.node.node_id,
        tier = ?config.tier,
        upstreams = ?config.upstreams,
        "starting bedrock-relay"
    );
    let admin = node::start_admin(&config.node).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin));
    let mut replay_config = bedrock_lib::relays::replay::ReplayConfig {
        max_bytes: config.replay.max_bytes.unwrap_or(1024 * 1024 * 1024),
        window_ms: config.replay.window_ms.unwrap_or(match config.tier {
            node::Tier::Cell => 300_000,
            node::Tier::Dc => 900_000,
            node::Tier::Region => 3_600_000,
        }),
        priorities: config.replay.priorities.clone(),
    };
    if let Some(mb) = config.node.budget.disk_mb {
        replay_config.max_bytes = replay_config.max_bytes.min(mb.saturating_mul(1024 * 1024));
    }
    #[cfg(not(madsim))]
    let engine: Result<Arc<dyn bedrock_lib::storage::StorageEngine>, _> =
        bedrock_lib::storage::rocksdb::RocksDbAdapter::open(
            &config.node.data_dir.join("replay"),
            "replay",
            &Default::default(),
        )
        .map(|engine| Arc::new(engine) as Arc<dyn bedrock_lib::storage::StorageEngine>);
    #[cfg(madsim)]
    let engine: Result<
        Arc<dyn bedrock_lib::storage::StorageEngine>,
        bedrock_lib::storage::StorageError,
    > = Ok(Arc::new(bedrock_lib::storage::memory::MemoryEngine::new(
        "replay",
    )));
    let replay =
        engine.and_then(|engine| bedrock_lib::relays::replay::Replay::open(engine, replay_config));
    let replay = match replay {
        Ok(replay) => Some(Arc::new(replay)),
        Err(error) => {
            tracing::warn!(%error,"replay unavailable; forwarding without local replay");
            None
        }
    };
    let hub = bedrock_lib::relays::Hub::relay(
        config.node.node_id.clone(),
        bedrock_lib::relays::tier_rank(config.tier),
        peers,
        config.upstreams.clone(),
        config.replay_peers.clone(),
        replay,
    )
    .with_clock(now);
    let upstream = hub.start();
    let server = tonic::transport::Server::builder()
        .add_service(bedrock_lib::replication::pb::relay_server::RelayServer::new(hub))
        .serve(config.node.listen);
    admin.set_ready(true);
    tokio::select! {biased;
        () = node::shutdown_signal() => {},
        result = server => {result.map_err(|e|ConfigError(format!("relay listen: {e}")))?;},
    }
    upstream.abort();
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}

// Adapter boundary: shared relay code receives its time source explicitly.
#[allow(clippy::disallowed_types, clippy::disallowed_methods)]
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
