//! `bedrock-volume`: actor storage (#111 / S-03, `statefull/v2.md` §5). Each node holds the slots of the actors whose
//! hash lies in the `volume` ranges its cell relays assign it, and serves the `Volume` gRPC protocol to their holders.
//! It joins its cell as service `volume` and announces the `volume` dataset, which relays place in ranges but never
//! stream.

pub mod serve;
pub mod store;

use std::sync::{Arc, PoisonError};

use bedrock_lib::datasets::capabilities::VOLUME;
use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig, Role, ServiceConfig};
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::replication::pb::RangeSize;
use bedrock_lib::storage::StorageEngine;

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
/// Returns configuration, storage, admin startup, or relay session initialization errors.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    if !config.roles.contains(&Role::L2) || !config.datasets.is_empty() {
        return Err(ConfigError(
            "bedrock-volume takes ranges as role l2 and hosts no relay-fed datasets".into(),
        ));
    }
    let node = &config.node;
    tracing::info!(node_id = %node.node_id, "starting bedrock-volume");
    let admin = node::start_admin(node).await?;
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
    let mut published = Vec::new();
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
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
                match sizes(&store,&current,&node.node_id) {
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

/// The sizes of the ranges `node` holds ready or leaving, in order; a populating copy is still incomplete.
fn sizes(
    store: &store::Store,
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
            sizes.push(store.size(start, end)?);
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
