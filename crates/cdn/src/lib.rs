//! `bedrock-cdn`: hosts the `cdn` dataset (retention metadata) and artifact bytes.

mod artifact_storage {}
mod artifact_transfer {}
mod manifests {}
mod retention {}

use std::sync::Arc;

use bedrock_lib::datasets::{CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::node::{self, Budget, ConfigError, ServiceConfig};
use bedrock_lib::storage::memory::MemoryEngine;

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
    // ponytail: in-memory until the RocksDB adapter (B-05) lands.
    let _datasets = Registry::open(DATASETS, &config.datasets, |d| {
        Ok(Arc::new(MemoryEngine::new(d.name)))
    })?;
    // ponytail: role subsystems don't exist yet; each starts here as its issue lands.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
    admin.set_ready(true);
    node::shutdown_signal().await;
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_only_cdn() {
        let r = Registry::open(DATASETS, &["cdn".into()], |d| {
            Ok(Arc::new(MemoryEngine::new(d.name)))
        })
        .unwrap();
        assert!(r.get("cdn").unwrap().engine.is_ok());
        for kv in ["quicksilver", "kv", "auth"] {
            assert!(Registry::open(DATASETS, &[kv.into()], |_| unreachable!()).is_err());
        }
    }
}
