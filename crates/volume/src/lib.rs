//! `bedrock-volume`: actor storage (#111 / S-03, `statefull/v2.md` §5). Each node holds the slots of the actors whose
//! hash lies in the `volume` ranges its cell relays assign it, and serves the `Volume` gRPC protocol to their holders.
//! It joins its cell as service `volume` and announces the `volume` dataset, which relays place in ranges but never
//! stream.

pub mod cold;
pub mod serve;
pub mod store;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, PoisonError};

use bedrock_lib::datasets::capabilities::VOLUME;
use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig, Role, ServiceConfig};
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::replication::pb::RangeSize;
use bedrock_lib::s3::S3Config;
use bedrock_lib::storage::StorageEngine;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct VolumeConfig {
    #[serde(flatten)]
    pub service: ServiceConfig,
    /// The bucket range databases live in (S-05). Required outside simulation.
    #[serde(default)]
    pub s3: Option<S3Config>,
    /// Relays of the other cells in this DC, by cell: an actor's other slots are there.
    #[serde(default)]
    pub sibling_relays: BTreeMap<String, Vec<String>>,
    /// The SSD cache of blocks read from S3, in MiB, in `data_dir/volume-cache`.
    #[serde(default = "default_cache_mb")]
    pub cache_mb: u64,
}

fn default_cache_mb() -> u64 {
    10 * 1024
}

impl From<ServiceConfig> for VolumeConfig {
    fn from(service: ServiceConfig) -> Self {
        VolumeConfig {
            service,
            s3: None,
            sibling_relays: BTreeMap::new(),
            cache_mb: default_cache_mb(),
        }
    }
}

impl AsRef<NodeConfig> for VolumeConfig {
    fn as_ref(&self) -> &NodeConfig {
        &self.service.node
    }
}

impl fmt::Debug for VolumeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VolumeConfig")
            .field("service", &self.service)
            .field("s3", &self.s3)
            .field("sibling_relays", &self.sibling_relays)
            .field("cache_mb", &self.cache_mb)
            .finish()
    }
}

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// Returns configuration, storage, admin startup, or relay session initialization errors.
pub async fn run(config: impl Into<VolumeConfig>) -> Result<(), ConfigError> {
    let VolumeConfig {
        service: config,
        s3,
        sibling_relays,
        cache_mb,
    } = config.into();
    config.validate()?;
    if !config.roles.contains(&Role::L2) || !config.datasets.is_empty() {
        return Err(ConfigError(
            "bedrock-volume takes ranges as role l2 and hosts no relay-fed datasets".into(),
        ));
    }
    let node = &config.node;
    tracing::info!(node_id = %node.node_id, "starting bedrock-volume");
    let admin = node::start_admin(node).await?;
    let databases = databases(node, s3.as_ref(), cache_mb).await?;
    let store = Arc::new(store::Store::new(open(node)?));
    let service = serve::Service::new(
        &admin,
        store.clone(),
        &node.node_id,
        &node.location.cell,
        &node.location.dc,
    )
    .map_err(|e| ConfigError(format!("reading the store: {e}")))?;
    let directory = service.directory.clone();
    let (members, target) = membership::View::new(VOLUME, &node.location.cell);
    let tls = bedrock_lib::transport::tls::start(node, VOLUME, &admin).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(tls.clone()));
    let client = Arc::new(
        bedrock_lib::client::Client::new(
            &admin,
            peers.clone(),
            config.relays.clone(),
            node.location.failure_domain.clone(),
        )
        .with_siblings(sibling_relays),
    );
    let placement = Arc::new(cold::placement::Cells::new(
        client,
        peers.clone(),
        &node.location.cell,
        &node.node_id,
    ));
    let metrics = bedrock_lib::health::protection::Metrics::new(&admin);
    let readers = cold::readers::Readers::new(
        databases.clone(),
        max_readers(node.budget.memory_mb.unwrap_or(1024)),
        &metrics,
    );
    let cold = Arc::new(cold::Cold::new(
        store.clone(),
        databases,
        placement,
        readers,
        &node.location.cell,
        &node.node_id,
    ));
    let service = service.with_cold(cold.clone());
    let mut member = membership::member(&config, VOLUME);
    member.datasets = vec![VOLUME.into()];
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        member,
        Default::default(),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let router = bedrock_lib::transport::server(tls.as_ref()).add_service(
        bedrock_lib::volume::volume_server::VolumeServer::new(service),
    );
    let mut serving = std::pin::pin!(bedrock_lib::transport::serve(router, node.listen, tls));
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut measure = tokio::time::interval(SIZES);
    let mut flushing = std::pin::pin!(flush(cold.clone(), directory.clone()));
    let mut published = Vec::new();
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            ()=&mut flushing=>unreachable!("the flush loop runs until shutdown"),
            served=&mut serving=>{
                served.map_err(|e| ConfigError(format!("listen {}: {e}", node.listen)))?;
                break;
            },
            _=readiness.tick()=>{
                admin.set_ready(session.ready());
                if let Err(error)=members.refresh(&session,&peers){
                    tracing::warn!(%error,"membership view unreadable");
                }
                // A directory that cannot be read keeps the last one.
                match members.directory(VOLUME) {
                    Ok(current)=>*directory.write().unwrap_or_else(PoisonError::into_inner)=current,
                    Err(error)=>tracing::warn!(%error,"volume directory unreadable"),
                }
            },
            _=measure.tick()=>{
                let current=directory.read().unwrap_or_else(PoisonError::into_inner).clone();
                match sizes(&store,&cold,&current,&node.node_id) {
                    Ok(sizes) if store::worth_publishing(&published,&sizes)=>{
                        session.set_sizes(sizes.clone());
                        published=sizes;
                    }
                    Ok(_)=>{}
                    Err(error)=>tracing::warn!(%error,"volume range sizes unreadable"),
                }
            },
        }
    }
    admin.set_ready(false);
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
}

/// How often a node measures the ranges it holds for placement (#111 / S-03).
const SIZES: std::time::Duration = std::time::Duration::from_secs(30);

/// Flushes the ranges this node maintains every [`cold::FLUSH_INTERVAL`], or sooner once [`cold::FLUSH_BYTES`] wait.
async fn flush(cold: Arc<cold::Cold>, directory: Arc<std::sync::RwLock<Directory>>) {
    let mut last = bedrock_lib::monotonic_now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !cold.due() && last.elapsed() < cold::FLUSH_INTERVAL {
            continue;
        }
        last = bedrock_lib::monotonic_now();
        let current = directory
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        cold.tick(&current).await;
    }
}

/// How many range database readers a node with `memory_mb` keeps open: each holds up to one memtable of unflushed
/// SlateDB state, at most the 64 MiB L0 size, and half the budget is for readers.
// ponytail: sized for every reader full at once; measure (S-05, v2 section 12 question 2) and raise if readers stay small.
fn max_readers(memory_mb: u64) -> usize {
    (memory_mb / 2 / 64).max(1) as usize
}

/// The range databases: SlateDB in the configured bucket with the SSD block cache, or the shared in-memory model in
/// simulation builds.
async fn databases(
    node: &NodeConfig,
    s3: Option<&S3Config>,
    cache_mb: u64,
) -> Result<Arc<dyn cold::db::Databases>, ConfigError> {
    #[cfg(not(madsim))]
    {
        let s3 =
            s3.ok_or_else(|| ConfigError("bedrock-volume needs [s3] for range databases".into()))?;
        let store = bedrock_lib::s3::object_store(s3).map_err(|e| ConfigError(e.to_string()))?;
        let cache = cold::db::ssd_cache(&node.data_dir.join("volume-cache"), cache_mb)
            .await
            .map_err(|e| ConfigError(format!("opening the SSD cache: {e}")))?;
        Ok(Arc::new(cold::db::Slate::new(store, Some(cache))))
    }
    #[cfg(madsim)]
    {
        let _ = (node, s3, cache_mb);
        Ok(Arc::new(cold::db::Memory::shared()))
    }
}

/// The sizes of the ranges `node` holds ready or leaving, in order; a populating copy is still incomplete. A range
/// database this node writes adds its manifest bytes; the middle is the slot store one, so sizing makes no S3 request.
/// It never waits for a flush, which runs in the same loop.
fn sizes(
    store: &store::Store,
    cold: &cold::Cold,
    directory: &Directory,
    node: &str,
) -> Result<Vec<RangeSize>, bedrock_lib::storage::StorageError> {
    let mut entries = directory.iter().peekable();
    let mut sizes = Vec::new();
    while let Some((&start, owners)) = entries.next() {
        let end = entries.peek().map_or(0, |(next, _)| **next);
        if owners
            .iter()
            .any(|o| o.node == node && o.state != State::Populating)
        {
            let mut size = store.size(start, end)?;
            size.bytes += cold.size(start, end);
            sizes.push(size);
        }
    }
    Ok(sizes)
}

/// The slot store: RocksDB in `data_dir/volume`, or in memory in simulation builds (B-23a). Unlike a relay-fed dataset,
/// a store that cannot open is not deleted: a slot that lost its data must repair before it votes (S-06).
fn open(node: &NodeConfig) -> Result<Arc<dyn StorageEngine>, ConfigError> {
    #[cfg(not(madsim))]
    {
        bedrock_lib::storage::rocksdb::RocksDbAdapter::open(
            &node.data_dir.join(VOLUME),
            VOLUME,
            &Default::default(),
        )
        .map(|engine| Arc::new(engine) as Arc<dyn StorageEngine>)
        .map_err(|e| ConfigError(format!("opening the volume store: {e}")))
    }
    #[cfg(madsim)]
    {
        Ok(bedrock_lib::sim::engine(&node.data_dir, VOLUME))
    }
}
