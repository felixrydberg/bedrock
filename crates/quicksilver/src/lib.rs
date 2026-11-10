//! `bedrock-quicksilver`: hosts the `quicksilver` dataset (control-plane data in relay-assigned L2 hash ranges).

mod namespaces {}
mod operations {}
pub mod storage;

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::membership;
use bedrock_lib::node::{self, Budget, ConfigError, ServiceConfig};

pub const DATASETS: &[Dataset] = &[
    // Routing and deployment metadata, complete in every datacenter (`kv.md` §4).
    Dataset {
        name: datasets::capabilities::QUICKSILVER,
        defaults: Policy {
            replication: ReplicationPolicy::FullReplica,
            cache: CachePolicy {
                l1_recent_window_seconds: 60,
                l2_eviction: false,
                pinned_prefixes: Vec::new(),
            },
            budget: Budget {
                cpus: None,
                memory_mb: None,
                disk_mb: None,
            },
        },
    },
];

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// Enabled datasets whose engines cannot open or are unhealthy at startup are skipped and keep the node unready until
/// restart. Configuration, admin startup, registry and relay session initialization errors are returned.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-quicksilver");
    let admin = node::start_admin(&config.node).await?;
    let storage = StorageHealth::new(&admin);
    // Pushed ranges apply through the replication handles, which keep working while B-22 rebuilds a dataset.
    let mut replication = std::collections::BTreeMap::new();
    let datasets = Registry::open(DATASETS, &config.datasets, |d| {
        let engine = datasets::open_engine(&config.node.data_dir, d)?;
        replication.insert(
            d.name,
            storage.wrap_replication(d.name, "engine", engine.clone()),
        );
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
            // A quarantined dataset still joins: its rebuild refills it through this session.
            Ok(engine) if engine.health().is_ok() || storage.unhealthy().contains(name) => {
                ranged.insert(name.to_owned(), replication[name].clone());
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
    let peers =
        std::sync::Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(
            bedrock_lib::transport::tls::start(&config.node, "quicksilver", &admin).await?,
        ));
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        membership::member(&config, "quicksilver"),
        ranged,
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let rebuilding = bedrock_lib::reconciliation::rebuild::start(
        &admin,
        &storage,
        &session,
        "engine",
        &replication,
        bedrock_lib::placement::hash_of("rebuild", &config.node.node_id, b""),
    );
    let scrubbing = bedrock_lib::health::scrub::start(
        datasets
            .iter()
            .filter_map(|instance| {
                let engine = instance.engine.as_ref().ok()?.clone();
                Some((instance.dataset.name.to_owned(), engine))
            })
            .collect(),
    );
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
    rebuilding.abort();
    scrubbing.abort();
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
