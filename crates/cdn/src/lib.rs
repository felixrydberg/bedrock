//! `bedrock-cdn`: hosts the `cdn` dataset (retention metadata) and artifact bytes.

pub mod artifact_storage;
pub mod artifact_transfer;
pub mod paths;
pub mod retention;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::datasets::{self, CachePolicy, Dataset, Policy, Registry, ReplicationPolicy};
use bedrock_lib::health::protection;
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::membership;
use bedrock_lib::node::{self, Budget, ConfigError, NodeConfig, ServiceConfig};
use bedrock_lib::reconciliation::{self, Reconciler};
use bedrock_lib::s3::S3Config;
use serde::Deserialize;

use crate::artifact_storage::{Artifacts, CacheStorage, Origin};
use crate::artifact_transfer::{ArtifactTransferServer, Transfer};

pub const DATASETS: &[Dataset] = &[Dataset {
    name: datasets::capabilities::CDN,
    // Path rows and ref keys (#80 / B-32) in the global `deployments` retention namespace (decision 17). A range's L2
    // owners never evict its records; idle artifact bytes are evicted through `CacheStorage` (B-44).
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

#[derive(Clone, Deserialize)]
pub struct CdnConfig {
    #[serde(flatten)]
    pub service: ServiceConfig,
    /// The bucket artifacts live in, under `artifacts/{sha256}`. Required outside simulation.
    #[serde(default)]
    pub s3: Option<S3Config>,
    /// The local artifact cache on disk, in MiB, in `data_dir/cdn-artifacts`.
    #[serde(default = "default_cache_mb")]
    pub cache_mb: u64,
}

fn default_cache_mb() -> u64 {
    10 * 1024
}

impl From<ServiceConfig> for CdnConfig {
    fn from(service: ServiceConfig) -> Self {
        CdnConfig {
            service,
            s3: None,
            cache_mb: default_cache_mb(),
        }
    }
}

impl AsRef<NodeConfig> for CdnConfig {
    fn as_ref(&self) -> &NodeConfig {
        &self.service.node
    }
}

impl fmt::Debug for CdnConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdnConfig")
            .field("service", &self.service)
            .field("s3", &self.s3)
            .field("cache_mb", &self.cache_mb)
            .finish()
    }
}

/// How often the holders' prewarming and repair pass runs (#82 / B-34).
const PREWARM: Duration = Duration::from_secs(1);
/// The artifact cache's memory tier, in MiB.
// ponytail: fixed; make it a setting when a deployment needs another size.
#[cfg(not(madsim))]
const CACHE_MEMORY_MB: u64 = 256;

/// Artifacts in the configured bucket through the Foyer cache, or in the shared in-memory model in simulation builds.
async fn artifacts(
    node: &NodeConfig,
    s3: Option<&S3Config>,
    cache_mb: u64,
    health: Arc<StorageHealth>,
) -> Result<Artifacts, ConfigError> {
    #[cfg(not(madsim))]
    let (origin, cache): (Arc<dyn Origin>, Arc<dyn CacheStorage>) = {
        let s3 = s3.ok_or_else(|| ConfigError("bedrock-cdn needs [s3] for artifacts".into()))?;
        let store = bedrock_lib::s3::object_store(s3).map_err(|e| ConfigError(e.to_string()))?;
        let cache = artifact_storage::Foyer::open(
            &node.data_dir.join("cdn-artifacts"),
            CACHE_MEMORY_MB,
            cache_mb,
        )
        .await
        .map_err(|e| ConfigError(format!("opening the artifact cache: {e}")))?;
        (Arc::new(artifact_storage::S3(store)), Arc::new(cache))
    };
    #[cfg(madsim)]
    let (origin, cache): (Arc<dyn Origin>, Arc<dyn CacheStorage>) = {
        let _ = (node, s3, cache_mb);
        (
            artifact_storage::Memory::shared(),
            Arc::new(artifact_storage::Memory::default()),
        )
    };
    Ok(Artifacts::new(origin, cache, health))
}

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// If the enabled CDN engine cannot open or is unhealthy at startup, its ranges are not followed and the node stays
/// unready until restart; one that damage stopped opening starts empty and quarantined instead, and rebuilds. Configuration, admin startup, registry and relay session initialization errors are returned.
pub async fn run(config: impl Into<CdnConfig>) -> Result<(), ConfigError> {
    let CdnConfig {
        service: config,
        s3,
        cache_mb,
    } = config.into();
    config.validate()?;
    tracing::info!(node_id = %config.node.node_id, datasets = ?config.datasets, "starting bedrock-cdn");
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
    // Relays push the `cdn` ranges this node owns, like Quicksilver's (§4 decision 14).
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
    let (members, target) = membership::View::new("cdn", &config.node.location.cell);
    let members = Arc::new(members);
    let tls = bedrock_lib::transport::tls::start(&config.node, "cdn", &admin).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(tls.clone()));
    let artifacts =
        Arc::new(artifacts(&config.node, s3.as_ref(), cache_mb, storage.clone()).await?);
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        membership::member(&config, "cdn"),
        ranged,
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let metrics = reconciliation::Metrics::new(&admin);
    let rebuilding = bedrock_lib::reconciliation::rebuild::start(
        &metrics,
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
    // Peers fetch artifacts on the bulk class; holders prewarm and repair what their ranges retain (#82 / B-34).
    let transfer = Arc::new(Transfer::new(
        artifacts.clone(),
        peers.clone(),
        &protection::Metrics::new(&admin),
    ));
    let prewarming = datasets
        .get(datasets::capabilities::CDN)
        .and_then(|instance| instance.engine.clone().ok())
        .map(|engine| {
            let view = members.clone();
            let prewarm = retention::Prewarm {
                engine,
                placement: Box::new(move || {
                    let members = view.members().map_err(|e| e.to_string())?;
                    let directory = view
                        .directory(datasets::capabilities::CDN)
                        .map_err(|e| e.to_string())?;
                    Ok((members, directory))
                }),
                me: config.node.node_id.clone(),
                lane: format!("{}/warm", config.node.location.cell),
                artifacts: artifacts.clone(),
                transfer: transfer.clone(),
            };
            let settings = reconciliation::Config {
                burst: 64,
                refill: Duration::from_millis(10),
                // Above B-31's whole-transfer deadline, so a slow peer falls back to S3 within one repair.
                repair_timeout: artifact_transfer::DEADLINE * 2,
                base_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
                seed: bedrock_lib::placement::hash_of("prewarm", &config.node.node_id, b""),
            };
            tokio::spawn(
                Reconciler::new("prewarm", Arc::new(prewarm), settings, &metrics).run(PREWARM),
            )
        });
    let router = bedrock_lib::transport::server(tls.as_ref())
        .add_service(ArtifactTransferServer::from_arc(transfer));
    let mut serving = bedrock_lib::transport::serve(router, config.node.listen, tls);
    let mut readiness = tokio::time::interval(Duration::from_millis(100));
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            served=&mut serving=>{
                served.map_err(|e| ConfigError(format!("listen {}: {e}", config.node.listen)))?;
                break;
            },
            _=readiness.tick()=>{
                // Quarantined datasets are routed around (B-14); they do not hold back the others.
                let unhealthy = storage.unhealthy();
                admin.set_ready(!skipped && datasets.is_healthy(&unhealthy) && session.ready_except(&unhealthy));
                session.set_unhealthy(&unhealthy, &storage.damaged());
                if let Err(error)=members.refresh(&session,&peers){
                    tracing::warn!(%error,"membership view unreadable");
                }
            },
        }
    }
    admin.set_ready(false);
    rebuilding.abort();
    scrubbing.abort();
    if let Some(prewarming) = prewarming {
        prewarming.abort();
    }
    session.drain().await;
    serving.close().await;
    tracing::info!("shutting down");
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
