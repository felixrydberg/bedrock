//! `bedrock-kv`: hosts the `quicksilver`, `kv` and `auth` datasets.

mod namespaces {}
mod operations {}
pub mod storage;

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::node::{self, Budget, ConfigError, ServiceConfig};

pub const DATASETS: &[Dataset] = &[
    // Routing and deployment metadata, complete in every datacenter (`kv.md` §4).
    dataset("quicksilver", ReplicationPolicy::FullReplica, 60),
    dataset("kv", ReplicationPolicy::OnDemand, 0),
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
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-kv");
    let datasets = Registry::open(DATASETS, &config.datasets, |d| {
        datasets::open_engine(&config.node.data_dir, d)
    })?;
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
    admin.set_ready(datasets.is_healthy());
    node::shutdown_signal().await;
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bedrock_lib::storage::memory::MemoryEngine;
    use std::sync::Arc;

    #[test]
    fn opens_only_kv_datasets() {
        let all = ["quicksilver", "kv", "auth"].map(String::from);
        let r =
            Registry::open(DATASETS, &all, |d| Ok(Arc::new(MemoryEngine::new(d.name)))).unwrap();
        assert_eq!(r.iter().count(), 3);
        let qs = &r.get("quicksilver").unwrap().dataset.defaults;
        assert_eq!(qs.replication, ReplicationPolicy::FullReplica);
        assert!(!qs.cache.l2_eviction);
        assert!(r.get("kv").unwrap().dataset.defaults.cache.l2_eviction);
        assert!(
            Registry::open(DATASETS, &["kv".into(), "cdn".into()], |_| unreachable!()).is_err()
        );
    }
}
