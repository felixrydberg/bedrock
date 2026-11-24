//! `bedrock-volume`: actor storage (#111 / S-03, `statefull/v2.md` §5). Each node holds the slots of the actors whose
//! hash lies in the `volume` ranges its cell relays assign it, and serves the `Volume` gRPC protocol to their holders.
//! It joins its cell as service `volume` and announces the `volume` dataset, which relays place in ranges but never
//! stream.

pub mod cold;
pub mod repair;
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
    // A store's start is decided from a majority of these relays ([`newest_directory`]): fewer than a cell's three
    // would let one relay that lost its state decide it.
    if config.relays.len() < 3 {
        return Err(ConfigError(
            "bedrock-volume needs at least its cell's three relays".into(),
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
    let siblings = sibling_relays.keys().cloned().collect();
    let client = Arc::new(
        bedrock_lib::client::Client::new(
            &admin,
            peers.clone(),
            config.relays.clone(),
            node.location.failure_domain.clone(),
        )
        .with_siblings(sibling_relays),
    );
    // Whether an empty store is a new node's or a lost disk is decided before joining, while no range can be given to
    // this node yet (S-06).
    while store.fresh().is_none() {
        match newest_directory(&peers, &config.relays, &node.location.failure_domain).await {
            Some(current) => repair::decide(&store, &current, &node.node_id)
                .map_err(|e| ConfigError(format!("recording the store's start: {e:?}")))?,
            None => {
                tracing::warn!(
                    "volume routes unavailable from a majority of relays; retrying before joining"
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
    let placement = Arc::new(cold::placement::Cells::new(
        client,
        peers.clone(),
        &node.location,
        &node.node_id,
        siblings,
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
        placement.clone(),
        readers,
        &node.location.cell,
        &node.node_id,
    ));
    let service = service.with_cold(cold.clone()).with_repair();
    let repair = repair::Repair::new(store.clone(), placement, &node.node_id, &node.location.cell);
    let mut member = membership::member(&config, VOLUME);
    member.datasets = vec![VOLUME.into()];
    let held: Arc<dyn bedrock_lib::relays::client::Holdings> =
        Arc::new(repair::Held(store.clone()));
    let session = bedrock_lib::relays::client::Session::join_reporting(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        member,
        BTreeMap::from([(VOLUME.to_owned(), held)]),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let router = bedrock_lib::transport::server(tls.as_ref()).add_service(
        bedrock_lib::volume::volume_server::VolumeServer::new(service)
            .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES),
    );
    let mut serving = bedrock_lib::transport::serve(router, node.listen, tls);
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut measure = tokio::time::interval(SIZES);
    let mut flushing = std::pin::pin!(flush(cold.clone(), directory.clone()));
    // The repair loop copies only from a validated directory view, with its generations; it says when a range became
    // held.
    let assignments = Arc::new(std::sync::RwLock::new(None));
    let (held_tx, mut held_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut repairing = std::pin::pin!(repairs(repair, assignments.clone(), held_tx));
    let mut published = Vec::new();
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            ()=&mut flushing=>unreachable!("the flush loop runs until shutdown"),
            ()=&mut repairing=>unreachable!("the repair loop runs until shutdown"),
            Some(())=held_rx.recv()=>session.report(),
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
                match members.assignments(VOLUME) {
                    Ok(current)=>{
                        *directory.write().unwrap_or_else(PoisonError::into_inner)=current.0.clone();
                        *assignments.write().unwrap_or_else(PoisonError::into_inner)=
                            session.ready().then_some(current);
                    }
                    // Repair waits for the next good read; serving keeps the last directory.
                    Err(error)=>{
                        *assignments.write().unwrap_or_else(PoisonError::into_inner)=None;
                        tracing::warn!(%error,"volume directory unreadable");
                    }
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
    serving.close().await;
    tracing::info!("shutting down");
    Ok(())
}

/// The newest `volume` directory among the answers of a majority of the cell's `relays`, or `None` without a majority.
/// A directory write commits on a majority of them, so one relay that lags it, or lost its state, cannot pass for a
/// cell where this node owns nothing.
// ponytail: newest of a majority's applied views; read from the Raft leader if a follower may apply long after commit.
async fn newest_directory(
    peers: &Arc<bedrock_lib::transport::Peers>,
    relays: &[String],
    failure_domain: &str,
) -> Option<Directory> {
    let mut answers = 0;
    let mut newest: Option<Arc<bedrock_lib::client::Table>> = None;
    for relay in relays {
        // Its own registry: these one-off clients' refresh counts are not the node's.
        let client = bedrock_lib::client::Client::new(
            &bedrock_lib::admin::Admin::new(),
            peers.clone(),
            vec![relay.clone()],
            failure_domain.to_owned(),
        );
        if let Ok(table) = client.routes(VOLUME, None).await {
            answers += 1;
            if newest.as_ref().is_none_or(|n| table.revision > n.revision) {
                newest = Some(table);
            }
        }
    }
    (answers * 2 > relays.len()).then(|| {
        newest
            .and_then(|t| t.directories.get(VOLUME).cloned())
            .unwrap_or_default()
    })
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

/// Copies and repairs ranges (S-06) from the last validated directory view and its generations, and sends a note when
/// a range became held.
async fn repairs(
    repair: repair::Repair,
    assignments: Arc<std::sync::RwLock<Option<membership::Assignments>>>,
    held: tokio::sync::mpsc::UnboundedSender<()>,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let current = assignments
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some((directory, generations)) = current else {
            continue;
        };
        match repair.tick(&directory, &generations).await {
            Ok(true) => {
                let _ = held.send(());
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(%error, "volume range repair failed"),
        }
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
/// a store that cannot open is not deleted: a slot that lost its data repairs before it votes ([`repair`]).
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
