//! `bedrock-cdn`: hosts the `cdn` dataset (retention metadata) and artifact bytes.

mod artifact_storage {}
mod artifact_transfer {}
mod manifests {}
mod retention {}

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::membership;
use bedrock_lib::node::{self, Budget, ConfigError, ServiceConfig};

pub const DATASETS: &[Dataset] = &[Dataset {
    name: "cdn",
    // Global retention namespaces (`workers`, `pages`; B-32). Every `bedrock-cdn` needs every record, so they are never
    // evicted; idle artifact bytes are, through `CacheStorage` (B-44).
    defaults: Policy {
        replication: ReplicationPolicy::Global,
        cache: CachePolicy {
            l1_recent_window_seconds: 0,
            l2_eviction: false,
            pinned_prefixes: Vec::new(),
        },
        budget: Budget {
            cpus: None,
            memory_mb: None,
            disk_mb: None,
        },
    },
}];

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// If the enabled CDN engine cannot open or is unhealthy at startup, its subscription is skipped and the node stays
/// unready until restart. Configuration, admin startup, registry and relay session initialization errors are returned.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-cdn");
    let admin = node::start_admin(&config.node).await?;
    let storage = StorageHealth::new(&admin);
    let datasets = Registry::open(DATASETS, &config.datasets, |d| {
        let engine = datasets::open_engine(&config.node.data_dir, d)?;
        Ok(storage.wrap(d.name, "engine", engine))
    })?;
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    // Every CDN node needs all retention metadata, so it follows `cdn` over the whole hash range, in every namespace.
    // Artifact bytes are what is ranged (B-34).
    let mut targets = Vec::new();
    let mut skipped = false;
    if let Some(instance) = datasets.get("cdn") {
        match &instance.engine {
            Ok(engine) if engine.health().is_ok() => {
                targets.push(bedrock_lib::relays::client::Target {
                    selection: bedrock_lib::relays::client::Selection {
                        dataset: "cdn".into(),
                        namespace: String::new(),
                    },
                    engine: engine.clone(),
                    prefix: Vec::new(),
                    span: Some((0, 0)),
                })
            }
            // ponytail: an unavailable dataset keeps the node unready until restart; rebuild the Session if hot
            // recovery matters.
            _ => {
                tracing::warn!(
                    dataset = "cdn",
                    "dataset unavailable; retention metadata is not followed"
                );
                skipped = true;
            }
        }
    }
    let (members, target) = membership::View::new("cdn", &config.node.location.cell);
    targets.push(target);
    let peers = std::sync::Arc::new(bedrock_lib::transport::Peers::new(&admin));
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        targets,
        membership::member(&config, "cdn"),
        // Artifact ranges carry no dataset to store yet (B-34).
        Default::default(),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            _=readiness.tick()=>{
                // Quarantined datasets are routed around (B-14); they do not hold back the others.
                let unhealthy = storage.unhealthy();
                admin.set_ready(!skipped && datasets.is_healthy(&unhealthy) && session.ready_except(&unhealthy));
                session.set_unhealthy(&unhealthy);
                if let Err(error)=members.refresh(&session,&peers){
                    tracing::warn!(%error,"membership view unreadable");
                }
            },
        }
    }
    admin.set_ready(false);
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
