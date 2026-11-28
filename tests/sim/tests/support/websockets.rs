//! Real volume, actor, callback and HTTP transports; only control-plane hints are fixed.
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, Placement};
use bedrock_compute::actors::serve::Actors;
use bedrock_compute::actors::volume::{Host, Slot};
use bedrock_compute::runtime::{Error, Executor, RustExecutor};
use bedrock_gateway::actors::Router;
use bedrock_gateway::sockets::Sockets;
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, Location};
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::replication::pb as rp;
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume as pb;
use bedrock_lib::websockets::Limits;
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;
use hyper_util::rt::TokioIo;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

pub fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "sockets".into(),
        id: format!("eu_{}a", "b".repeat(25)),
        kind: 0,
    }
}
fn member(name: &str, service: &str, address: String) -> rp::Member {
    rp::Member {
        node_id: name.into(),
        service: service.into(),
        cell: "cell".into(),
        dc: "dc".into(),
        region: "eu".into(),
        address,
        roles: vec![service.into()],
        status: rp::member::Status::Ready as i32,
        ..Default::default()
    }
}

pub async fn listen(port: u16) -> tokio::net::TcpListener {
    #[cfg(not(madsim))]
    let address = {
        let _ = port;
        "127.0.0.1:0".to_owned()
    };
    #[cfg(madsim)]
    let address = format!("10.8.0.1:{port}");
    tokio::net::TcpListener::bind(address).await.unwrap()
}
async fn serve(router: tonic::transport::server::Router, port: u16) -> String {
    #[cfg(not(madsim))]
    {
        let listener = listen(port).await;
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(
            router.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        address
    }
    #[cfg(madsim)]
    {
        let address = format!("10.8.0.1:{port}");
        let bind = address.parse().unwrap();
        tokio::spawn(async move {
            router.serve(bind).await.unwrap();
        });
        address
    }
}

struct Fixed([Slot; 3]);
#[async_trait]
impl Placement for Fixed {
    async fn slots(&self, _: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        Ok((self.0.clone(), 0))
    }
}

/// Fixed authoritative topology/membership HINTS; no actor or socket outcomes are fabricated.
struct Control {
    computes: Mutex<Vec<rp::Member>>,
    revision: AtomicU64,
    quicksilver: Mutex<Option<rp::Member>>,
}
type Frames = Pin<Box<dyn tokio_stream::Stream<Item = Result<rp::RelayFrame, Status>> + Send>>;
#[tonic::async_trait]
impl rp::relay_server::Relay for Control {
    type SessionStream = Frames;
    async fn session(
        &self,
        _: Request<Streaming<rp::RelayRequest>>,
    ) -> Result<Response<Frames>, Status> {
        Err(Status::unimplemented("fixed hints"))
    }
    async fn cells(
        &self,
        _: Request<rp::CellsRequest>,
    ) -> Result<Response<rp::CellsReply>, Status> {
        Err(Status::unimplemented("one cell"))
    }
    async fn routes(
        &self,
        request: Request<rp::RoutesRequest>,
    ) -> Result<Response<rp::RoutesReply>, Status> {
        let mut entries = Vec::new();
        if request.get_ref().service == "compute" {
            for member in self.computes.lock().unwrap().iter() {
                entries.push(rp::StateEntry {
                    key: member.node_id.as_bytes().to_vec(),
                    value: member.encode_to_vec(),
                });
            }
        } else {
            let member = self.quicksilver.lock().unwrap().clone().unwrap();
            entries.push(rp::StateEntry {
                key: member.node_id.as_bytes().to_vec(),
                value: member.encode_to_vec(),
            });
            let range = rp::Range {
                owners: vec![rp::range::Owner {
                    node_id: member.node_id,
                    state: rp::range::owner::State::Ready as i32,
                    ..Default::default()
                }],
            };
            entries.push(rp::StateEntry {
                key: b"range/quicksilver/0000000000000000".to_vec(),
                value: range.encode_to_vec(),
            });
        }
        Ok(Response::new(rp::RoutesReply {
            revision: self.revision.load(Ordering::SeqCst),
            entries,
            cell: "cell".into(),
        }))
    }
}
#[tonic::async_trait]
impl rp::reads_server::Reads for Control {
    async fn get(
        &self,
        request: Request<rp::GetRequest>,
    ) -> Result<Response<rp::GetReply>, Status> {
        assert_eq!(
            request.get_ref().key,
            bedrock_lib::actors::topology_key("eu")
        );
        let topology = bedrock_lib::actors::Topology {
            format_version: 1,
            region: "eu".into(),
            version: 0,
            dcs: vec![bedrock_lib::actors::Dc {
                name: "dc".into(),
                cells: vec!["cell".into()],
            }],
            previous_dcs: Vec::new(),
        };
        Ok(Response::new(rp::GetReply {
            revision: 1,
            value: Some(topology.encode_to_vec()),
        }))
    }
    async fn list(&self, _: Request<rp::ListRequest>) -> Result<Response<rp::ListReply>, Status> {
        Err(Status::unimplemented("topology only"))
    }
}

/// A transport fault returns no owner receipt; successful calls always run the real owner.
pub struct OwnerEndpoint {
    pub current: Mutex<Arc<Sockets>>,
    pub unavailable: AtomicBool,
    pub stall_apply: AtomicBool,
}
#[tonic::async_trait]
impl pb::sockets_server::Sockets for OwnerEndpoint {
    async fn lookup(
        &self,
        request: Request<pb::SocketLookup>,
    ) -> Result<Response<pb::SocketSnapshots>, Status> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(Status::unavailable("owner partition"));
        }
        let current = self.current.lock().unwrap().clone();
        pb::sockets_server::Sockets::lookup(current.as_ref(), request).await
    }
    async fn apply(
        &self,
        request: Request<pb::SocketApply>,
    ) -> Result<Response<pb::SocketReport>, Status> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(Status::unavailable("owner partition"));
        }
        if self.stall_apply.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let current = self.current.lock().unwrap().clone();
        pb::sockets_server::Sockets::apply(current.as_ref(), request).await
    }
}
struct ComputeEndpoint {
    inner: Actors,
    online: Arc<AtomicBool>,
    refused: Arc<AtomicUsize>,
    lose_message_reply: Arc<AtomicBool>,
}
#[tonic::async_trait]
impl pb::actors_server::Actors for ComputeEndpoint {
    async fn invoke(
        &self,
        request: Request<pb::InvokeRequest>,
    ) -> Result<Response<pb::InvokeReply>, Status> {
        if !self.online.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(Status::unavailable("compute transport stopped"));
        }
        let message = request
            .get_ref()
            .socket
            .as_ref()
            .is_some_and(|event| event.kind == pb::socket_event::Kind::Message as i32);
        let lose_reply = message && self.lose_message_reply.swap(false, Ordering::SeqCst);
        let response = pb::actors_server::Actors::invoke(&self.inner, request).await?;
        if lose_reply {
            Err(Status::unavailable("executed socket message; reply lost"))
        } else {
            Ok(response)
        }
    }
}

pub struct Gateway {
    pub address: String,
    pub endpoint: Arc<OwnerEndpoint>,
    owner: rp::Member,
    router: Arc<Router>,
    limits: Limits,
}
impl Gateway {
    pub fn restart_registry(&self) {
        *self.endpoint.current.lock().unwrap() = Arc::new(
            Sockets::new(self.owner.clone(), self.router.clone(), self.limits.clone()).unwrap(),
        );
    }
}
pub struct Fixture {
    pub hosts: Vec<ActorHost>,
    pub computes: Vec<String>,
    pub stores: Vec<Arc<Store>>,
    pub volumes_online: Vec<Arc<AtomicBool>>,
    pub gateways: Vec<Gateway>,
    pub compute_online: Arc<AtomicBool>,
    pub compute_refused: Arc<AtomicUsize>,
    pub lose_message_reply: Arc<AtomicBool>,
    control: Arc<Control>,
    second: rp::Member,
}
impl Fixture {
    pub fn publish_second_holder(&self) {
        self.control
            .computes
            .lock()
            .unwrap()
            .push(self.second.clone());
        self.control.revision.fetch_add(1, Ordering::SeqCst);
    }
    pub async fn new(executor: Arc<dyn Executor>, limits: Limits) -> Self {
        Self::new_with_gateways(executor, limits, 2).await
    }
    pub async fn new_with_gateways(
        executor: Arc<dyn Executor>,
        limits: Limits,
        count: u16,
    ) -> Self {
        let mut slots = Vec::new();
        let mut stores = Vec::new();
        let mut volumes_online = Vec::new();
        for i in 0..3 {
            let name = format!("volume-{i}");
            #[cfg(not(madsim))]
            let engine = Arc::new(bedrock_lib::storage::memory::MemoryEngine::new("volume"));
            #[cfg(madsim)]
            let engine = Arc::new(bedrock_lib::storage::sim::SimEngine::new(
                &name,
                Arc::new(|_| None),
                None,
                false,
            ));
            let store = Arc::new(Store::new(engine));
            store.hold(0, 0, 0).unwrap();
            let service = Service::new(&Admin::new(), store.clone(), &name, "cell", "dc").unwrap();
            *service.directory.write().unwrap() = BTreeMap::from([(
                0,
                vec![Owner {
                    node: name.clone(),
                    state: State::Ready,
                }],
            )]);
            let online = Arc::new(AtomicBool::new(true));
            let serving = online.clone();
            let volume = pb::volume_server::VolumeServer::with_interceptor(
                service,
                move |request: Request<()>| {
                    if serving.load(Ordering::SeqCst) {
                        Ok(request)
                    } else {
                        Err(Status::unavailable("slot transport partition"))
                    }
                },
            );
            let address = serve(
                tonic::transport::Server::builder().add_service(volume),
                8100 + i,
            )
            .await;
            slots.push(Slot {
                cell: "cell".into(),
                address,
                identity: tls::Identity {
                    service: "volume".into(),
                    cell: "cell".into(),
                    node_id: name,
                    host: "127.0.0.1".into(),
                },
            });
            stores.push(store);
            volumes_online.push(online);
        }
        let control = Arc::new(Control {
            computes: Mutex::new(Vec::new()),
            revision: AtomicU64::new(1),
            quicksilver: Mutex::new(None),
        });
        let relay = serve(
            tonic::transport::Server::builder()
                .add_service(rp::relay_server::RelayServer::from_arc(control.clone()))
                .add_service(rp::reads_server::ReadsServer::from_arc(control.clone())),
            8200,
        )
        .await;
        let mut quicksilver = member("quicksilver", "quicksilver", relay.clone());
        quicksilver.datasets = vec!["quicksilver".into()];
        *control.quicksilver.lock().unwrap() = Some(quicksilver);
        let compute_online = Arc::new(AtomicBool::new(true));
        let compute_refused = Arc::new(AtomicUsize::new(0));
        let lose_message_reply = Arc::new(AtomicBool::new(false));
        let mut hosts = Vec::new();
        let mut computes = Vec::new();
        for i in 0..2 {
            let admin = Admin::new();
            let peers = Arc::new(Peers::new(&admin));
            let client = Arc::new(Client::new(
                &admin,
                peers.clone(),
                vec![relay.clone()],
                String::new(),
            ));
            let holder = pb::Holder {
                cell: "cell".into(),
                host: format!("compute-{i}"),
            };
            let volume = Host::start(peers, holder.clone());
            let host = ActorHost::new(
                &admin,
                "cell",
                "dc",
                volume,
                Arc::new(Fixed(slots.clone().try_into().unwrap())),
                Arc::new(RustExecutor::new([])),
                executor.clone(),
            )
            .with_sockets(&admin, client, limits.clone())
            .unwrap();
            let service = ComputeEndpoint {
                inner: Actors::new(host.clone(), holder),
                online: compute_online.clone(),
                refused: compute_refused.clone(),
                lose_message_reply: lose_message_reply.clone(),
            };
            computes.push(
                serve(
                    tonic::transport::Server::builder()
                        .add_service(pb::actors_server::ActorsServer::new(service)),
                    8300 + i,
                )
                .await,
            );
            hosts.push(host);
        }
        let first = member("compute-0", "compute", computes[0].clone());
        let second = member("compute-1", "compute", computes[1].clone());
        control.computes.lock().unwrap().push(first);
        let mut gateways = Vec::new();
        for i in 0..count {
            let admin = Admin::new();
            let peers = Arc::new(Peers::new(&admin));
            let client = Arc::new(Client::new(
                &admin,
                peers.clone(),
                vec![relay.clone()],
                String::new(),
            ));
            let router = Arc::new(Router::new(
                &admin,
                client,
                peers,
                Location {
                    region: "eu".into(),
                    dc: "dc".into(),
                    cell: "cell".into(),
                },
                16,
            ));
            // Reserve the callback listener before publishing this owner's endpoint.
            #[cfg(not(madsim))]
            let callback_listener = listen(8400 + i).await;
            #[cfg(not(madsim))]
            let callback = callback_listener.local_addr().unwrap().to_string();
            #[cfg(madsim)]
            let callback = format!("10.8.0.1:{}", 8400 + i);
            let owner = member(&format!("gateway-{i}"), "gateway", callback);
            let sockets =
                Arc::new(Sockets::new(owner.clone(), router.clone(), limits.clone()).unwrap());
            let endpoint = Arc::new(OwnerEndpoint {
                current: Mutex::new(sockets),
                unavailable: AtomicBool::new(false),
                stall_apply: AtomicBool::new(false),
            });
            let callback_server = tonic::transport::Server::builder().add_service(
                pb::sockets_server::SocketsServer::from_arc(endpoint.clone()),
            );
            #[cfg(not(madsim))]
            tokio::spawn(callback_server.serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(callback_listener),
            ));
            #[cfg(madsim)]
            {
                let address = owner.address.parse().unwrap();
                tokio::spawn(async move {
                    callback_server.serve(address).await.unwrap();
                });
            }
            let listener = listen(8500 + i).await;
            let address = listener.local_addr().unwrap().to_string();
            let public = endpoint.clone();
            tokio::spawn(async move {
                loop {
                    let (io, _) = listener.accept().await.unwrap();
                    let endpoint = public.clone();
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(move |request| {
                            let sockets = endpoint.current.lock().unwrap().clone();
                            async move {
                                sockets
                                    .upgrade(actor(), request)
                                    .await
                                    .map_err(|e| std::io::Error::other(e.to_string()))
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(io), service)
                            .with_upgrades()
                            .await;
                    });
                }
            });
            gateways.push(Gateway {
                address,
                endpoint,
                owner,
                router,
                limits: limits.clone(),
            });
        }
        Self {
            hosts,
            computes,
            stores,
            volumes_online,
            gateways,
            compute_online,
            compute_refused,
            lose_message_reply,
            control,
            second,
        }
    }
}
