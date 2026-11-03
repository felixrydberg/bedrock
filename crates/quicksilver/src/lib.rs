//! `bedrock-quicksilver`: hosts the `quicksilver` and `auth` datasets (control-plane data in relay-assigned L2 hash ranges).

mod namespaces {}
mod operations {}
pub mod storage;

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::membership;
use bedrock_lib::node::{self, Budget, ConfigError, ServiceConfig};

pub const DATASETS: &[Dataset] = &[
    // Routing and deployment metadata, complete in every datacenter (`kv.md` §4).
    dataset("quicksilver", ReplicationPolicy::FullReplica, 60),
    dataset("auth", ReplicationPolicy::OnDemand, 0),
];

const fn dataset(
    name: &'static str,
    replication: ReplicationPolicy,
    l1_recent_window_seconds: u64,
) -> Dataset {
    Dataset {
        name,
        defaults: Policy {
            replication,
            cache: CachePolicy {
                l1_recent_window_seconds,
                // `full_replica` keeps the complete namespace in L2.
                l2_eviction: !matches!(replication, ReplicationPolicy::FullReplica),
                pinned_prefixes: Vec::new(),
            },
            budget: Budget {
                cpus: None,
                memory_mb: None,
                disk_mb: None,
            },
        },
    }
}

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-quicksilver");
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
    // Relays push the ranges this node owns of each dataset it carries.
    let mut ranged = std::collections::BTreeMap::new();
    for instance in datasets.iter() {
        let name = instance.dataset.name;
        match &instance.engine {
            Ok(engine) if engine.health().is_ok() => {
                ranged.insert(name.to_owned(), engine.clone());
            }
            _ => tracing::warn!(
                dataset = name,
                "dataset unavailable; its ranges are not followed"
            ),
        }
    }
    // ponytail: a dataset unavailable at startup keeps the node unready until restart; rebuild the Session if hot
    // recovery matters.
    let skipped = ranged.len() != datasets.iter().count();
    let (members, target) = membership::View::new("quicksilver", &config.node.location.cell);
    let peers = std::sync::Arc::new(bedrock_lib::transport::Peers::new(&admin));
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        membership::member(&config, "quicksilver"),
        ranged,
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
