//! `bedrock-cdn`: hosts the `cdn` dataset (retention metadata) and artifact bytes.

mod artifact_storage {}
mod artifact_transfer {}
mod manifests {}
mod retention {}

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
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
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-cdn");
    let datasets = Registry::open(DATASETS, &config.datasets, |d| {
        datasets::open_engine(&config.node.data_dir, d)
    })?;
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
    let targets = config
        .subscriptions
        .iter()
        .map(|selection| {
            if selection.dataset != "cdn" {
                return Err(ConfigError(
                    "CDN relay subscriptions permit only cdn".into(),
                ));
            }
            let instance = datasets
                .get(&selection.dataset)
                .ok_or_else(|| ConfigError("subscribed dataset is not enabled".into()))?;
            let Ok(engine)=&instance.engine else{return Ok(None)};
            if let Err(error)=engine.health(){
                tracing::warn!(dataset=%selection.dataset,%error,"dataset unavailable; other subscriptions continue");
                return Ok(None);
            }
            let mut prefix = selection.namespace.as_bytes().to_vec();
            prefix.push(0);
            Ok(Some(bedrock_lib::relays::client::Target {
                selection: selection.clone(),
                engine:engine.clone(),
                prefix,
            }))
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    // ponytail: a skipped subscription keeps the node unready until restart; rebuild the Session if hot recovery matters.
    let skipped = targets.iter().any(Option::is_none);
    let targets = targets.into_iter().flatten().collect();
    let session = bedrock_lib::relays::client::Session::start(
        std::sync::Arc::new(bedrock_lib::transport::Peers::new(&admin)),
        config.relays.clone(),
        targets,
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            _=readiness.tick()=>admin.set_ready(!skipped && datasets.is_healthy() && session.ready()),
        }
    }
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
