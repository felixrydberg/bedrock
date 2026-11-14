//! `bedrock-relay`: relay tree, fan-out, replay log, and the cell membership publisher with its control authority and L2
//! placement writes (B-17, B-68); the relay server core and everything other binaries read stay in `bedrock-lib`.

use bedrock_lib::node::{self, ConfigError, RelayConfig};
use std::sync::Arc;

pub mod membership;
pub mod placement;
mod raft_runtime;

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
    let tls = bedrock_lib::transport::tls::start(&config.node, "relay", &admin).await?;
    let identities = config
        .membership
        .as_ref()
        .map(|settings| {
            settings
                .voter_ids
                .iter()
                .map(|(address, node_id)| {
                    (
                        address.clone(),
                        bedrock_lib::transport::tls::Identity {
                            service: "relay".into(),
                            cell: config.node.location.cell.clone(),
                            node_id: node_id.clone(),
                            host: bedrock_lib::transport::host_of(address).into(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let peers = Arc::new(
        bedrock_lib::transport::Peers::new(&admin)
            .with_tls(tls.clone())
            .with_identities(identities),
    );
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
    let replay = storage(&config.node, "replay")
        .and_then(|engine| bedrock_lib::relays::replay::Replay::open(engine, replay_config));
    let replay = match replay {
        Ok(replay) => Some(Arc::new(replay)),
        Err(error) => {
            tracing::warn!(%error,"replay unavailable; forwarding without local replay");
            None
        }
    };
    let mut tasks = Vec::new();
    let mut control_task = None;
    let control = if let Some(settings) = &config.membership {
        let mut voters = config.replay_peers.clone();
        voters.push(config.node.advertised());
        let engine = storage(&config.node, "control")
            .map_err(|error| ConfigError(format!("cell control: {error}")))?;
        let (control, running) = membership::control::Control::start(
            engine,
            peers.clone(),
            membership::control::Config {
                region: config.node.location.region.clone(),
                dc: config.node.location.dc.clone(),
                cell: config.node.location.cell.clone(),
                me: config.node.advertised(),
                voters,
                voter_ids: settings.voter_ids.clone(),
                clock: now,
                operator_token: config.write_token.clone(),
                bootstrap: settings.bootstrap,
            },
        )
        .map_err(|error| ConfigError(format!("cell control: {error}")))?;
        control_task = Some(running);
        Some(control)
    } else {
        None
    };
    let mut hub = bedrock_lib::relays::Hub::relay(
        config.node.node_id.clone(),
        bedrock_lib::relays::tier_rank(config.tier),
        peers.clone(),
        config.upstreams.clone(),
        config.replay_peers.clone(),
        replay,
    )
    .with_clock(now)
    .with_cell(config.node.location.cell.clone());
    if let Some(control) = &control {
        hub = hub.with_control(control.clone());
    }
    tasks.push(hub.start());
    let mut publisher = None;
    // Every relay serves and watches its siblings; only cell relays with membership also publish.
    let (siblings, watching) = bedrock_lib::relays::siblings::Siblings::start(
        hub.clone(),
        peers.clone(),
        config.replay_peers.clone(),
        config.grace(),
    );
    tasks.push(watching);
    if let Some(settings) = &config.membership {
        let (publishing, published) = membership::Cell::start(
            hub.clone(),
            peers,
            siblings,
            settings,
            config.node.advertised(),
            config.node.location.cell.clone(),
            control.as_ref().unwrap().clone(),
        )
        .map_err(|e| ConfigError(format!("membership: {e}")))?;
        tasks.push(publishing);
        publisher = Some(published);
    }
    let tier = match config.tier {
        node::Tier::Cell => "cell",
        node::Tier::Dc => "dc",
        node::Tier::Region => "region",
    };
    bedrock_lib::relays::introspection::register(
        &admin,
        &hub,
        tier,
        &config.node.location.cell,
        publisher,
    );
    let router = bedrock_lib::transport::server(tls.as_ref())
        .add_service(bedrock_lib::replication::pb::relay_server::RelayServer::new(hub.clone()))
        .add_service(
            bedrock_lib::replication::pb::siblings_server::SiblingsServer::new(
                bedrock_lib::relays::siblings::Server(hub),
            ),
        )
        .add_service(
            bedrock_lib::replication::pb::cell_control_server::CellControlServer::new(
                membership::control::Server(control),
            )
            .max_decoding_message_size(membership::control::MAX_PACKET)
            .max_encoding_message_size(membership::control::MAX_PACKET),
        );
    let server = bedrock_lib::transport::serve(router, config.node.listen, tls);
    admin.set_ready(true);
    let result = tokio::select! {biased;
        () = node::shutdown_signal() => Ok(()),
        _ = async {
            match &mut control_task {
                Some(task) => { let _ = task.await; },
                None => std::future::pending::<()>().await,
            }
        } => Err(ConfigError("cell control authority stopped; relay must recover before rejoining".into())),
        result = server => result.map_err(|error| ConfigError(format!("relay listen: {error}"))),
    };
    for task in tasks {
        task.abort();
    }
    if let Some(task) = control_task {
        task.abort();
    }
    admin.set_ready(false);
    tracing::info!("shutting down");
    result
}

// Adapter boundary: control is durable authority; replay remains independently disposable.
fn storage(
    config: &node::NodeConfig,
    name: &str,
) -> bedrock_lib::storage::StorageResult<Arc<dyn bedrock_lib::storage::StorageEngine>> {
    #[cfg(not(madsim))]
    {
        bedrock_lib::storage::rocksdb::RocksDbAdapter::open(
            &config.data_dir.join(name),
            name,
            &Default::default(),
        )
        .map(|engine| Arc::new(engine) as Arc<dyn bedrock_lib::storage::StorageEngine>)
    }
    #[cfg(madsim)]
    {
        Ok(bedrock_lib::sim::engine(&config.data_dir, name))
    }
}

// Adapter boundary: shared relay code receives its time source explicitly.
#[allow(clippy::disallowed_types, clippy::disallowed_methods)]
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
