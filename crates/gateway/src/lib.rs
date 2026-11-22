//! `bedrock-gateway` (#89 / B-57 part 1, §4 decision 15): routing between cells on an internal mTLS listener.
//!
//! The Gateway subscribes to a relay like any other process, and its role follows the tier in that relay's `Welcome`:
//! - **Cell tier:** a member of its cell (`gateway.{cell}`). It serves forwarded reads for its own cell only, from the
//!   cell's holders through the shared client, and turns away requests for any other cell (`docs/spec/cells.tla`).
//! - **DC or region tier:** it picks the cell. It learns the cells below it from `Relay.Cells` ([`cells::Cells`]),
//!   resolves the cell's Gateways with `Relay.Routes("gateway")` at that cell's relays and forwards to one of them.
//!
//! Reads inside a cell never pass a Gateway: the shared client reads holders directly, and sends a read of another
//! cell to the target DC's or region's Gateways ([`bedrock_lib::client::Client::get_in`]). The public listener and API
//! keys are B-57 part 2.
//!
//! Actor requests (`Actors.Invoke`, #116 / S-08) are routed by [`actors::Router`] at the cell Gateway they enter and
//! forwarded through the upper tiers like reads.

pub mod actors;
pub mod cells;

use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::client::{CELL_HEADER, Client, Location, target_of, targeted};
use bedrock_lib::datasets::capabilities::{COMPUTE, GATEWAY, SERVICES};
use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, NodeConfig};
use bedrock_lib::replication::pb;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

/// Requests the internal listener serves at once by default.
pub const INTERNAL_LIMIT: usize = 1024;
const RETRY: Duration = Duration::from_secs(1);
const WELCOME: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    /// `listen` is the internal mTLS listener.
    #[serde(flatten)]
    pub node: NodeConfig,
    /// Relays of this Gateway's tier, tried in order: cell relays for a cell Gateway, DC or region relays above.
    pub relays: Vec<String>,
    /// Requests the internal listener serves at once; more are refused with `RESOURCE_EXHAUSTED`. Its own limit, so the
    /// public listener (part 2) cannot starve internal reads.
    #[serde(default = "internal_limit")]
    pub internal_limit: usize,
    /// Actor locations a cell Gateway caches.
    #[serde(default = "actor_cache")]
    pub actor_cache: usize,
}

fn internal_limit() -> usize {
    INTERNAL_LIMIT
}

fn actor_cache() -> usize {
    actors::CACHE
}

impl AsRef<NodeConfig> for GatewayConfig {
    fn as_ref(&self) -> &NodeConfig {
        &self.node
    }
}

/// What the Gateway does, from its relay's tier.
pub enum Role {
    /// Serves its own cell.
    Cell(String),
    /// Picks a cell below a DC or region relay.
    Upper(cells::Cells),
}

impl Role {
    fn label(&self) -> &'static str {
        match self {
            Role::Cell(_) => "cell",
            Role::Upper(cells) if cells.tier() == 2 => "dc",
            Role::Upper(_) => "region",
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Labels {
    role: &'static str,
    /// One of [`SERVICES`], or "invalid".
    service: &'static str,
    outcome: &'static str,
}

/// The internal listener's `Reads` and `Actors` services.
pub struct Gateway {
    role: Role,
    client: Arc<Client>,
    limit: Semaphore,
    requests: Family<Labels, Counter>,
    /// A cell Gateway's actor routing.
    router: Option<actors::Router>,
}

impl Gateway {
    pub fn new(
        admin: &bedrock_lib::admin::Admin,
        role: Role,
        client: Arc<Client>,
        limit: usize,
    ) -> Self {
        let requests = Family::default();
        admin.register(
            "gateway_requests",
            "Forwarded reads by role, service and outcome",
            requests.clone(),
        );
        Self {
            role,
            client,
            limit: Semaphore::new(limit),
            requests,
            router: None,
        }
    }

    /// Routes actor requests that enter or reach this cell Gateway.
    pub fn with_actors(mut self, router: actors::Router) -> Self {
        self.router = Some(router);
        self
    }

    fn count(&self, service: &str, outcome: &'static str) {
        let service = SERVICES
            .iter()
            .copied()
            .find(|s| *s == service)
            .unwrap_or("invalid");
        self.requests
            .get_or_create(&Labels {
                role: self.role.label(),
                service,
                outcome,
            })
            .inc();
    }

    /// Admits a forwarded request: under the limit, for a known service, and at a cell Gateway for its own cell.
    async fn serve<M, T, Local, LocalFut, Forward, ForwardFut>(
        &self,
        request: Request<M>,
        local: Local,
        forward: Forward,
    ) -> Result<Response<T>, Status>
    where
        M: Clone,
        Local: FnOnce(String, M) -> LocalFut,
        LocalFut: Future<Output = Result<T, Status>>,
        Forward: Fn(tonic::transport::Channel, Request<M>) -> ForwardFut,
        ForwardFut: Future<Output = Result<Response<T>, Status>>,
    {
        let (service, cell) = target_of(&request)?;
        let Ok(_permit) = self.limit.try_acquire() else {
            self.count(&service, "limited");
            return Err(Status::resource_exhausted("internal listener at its limit"));
        };
        if !SERVICES.contains(&service.as_str()) {
            self.count(&service, "rejected");
            return Err(Status::invalid_argument("unknown service"));
        }
        let message = request.into_inner();
        let result = match &self.role {
            Role::Cell(own) if *own != cell => {
                self.count(&service, "rejected");
                return Err(Status::failed_precondition(format!(
                    "this Gateway serves cell {own}, not {cell}"
                )));
            }
            Role::Cell(_) => local(service.clone(), message).await,
            Role::Upper(cells) => {
                cells
                    .forward(&cell, |channel| {
                        let request = targeted(message.clone(), &service, &cell);
                        let forward = &forward;
                        async move { forward(channel, request?).await }
                    })
                    .await
            }
        };
        self.count(&service, if result.is_ok() { "served" } else { "failed" });
        result.map(Response::new)
    }
}

#[tonic::async_trait]
impl pb::reads_server::Reads for Gateway {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        let client = self.client.clone();
        self.serve(
            request,
            |service, r: pb::GetRequest| async move {
                let found = client
                    .get(&service, &r.dataset, &r.namespace, &r.key, r.min_revision)
                    .await?;
                Ok(pb::GetReply {
                    revision: found.revision,
                    value: found.value,
                })
            },
            |channel, request| async move {
                pb::reads_client::ReadsClient::new(channel)
                    .get(request)
                    .await
            },
        )
        .await
    }

    /// The whole cell's page: the cell Gateway merges its ranges, so `range_start` and `range_end` are ignored.
    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListReply>, Status> {
        let client = self.client.clone();
        self.serve(
            request,
            |service, r: pb::ListRequest| async move {
                let page = client
                    .list(
                        &service,
                        &r.dataset,
                        &r.namespace,
                        &r.prefix,
                        &r.start_after,
                        r.limit,
                        r.min_revision,
                    )
                    .await?;
                Ok(pb::ListReply {
                    revision: page.revision,
                    more: page.more,
                    entries: page
                        .entries
                        .into_iter()
                        .map(|(key, value)| pb::StateEntry { key, value })
                        .collect(),
                })
            },
            |channel, request| async move {
                pb::reads_client::ReadsClient::new(channel)
                    .list(request)
                    .await
            },
        )
        .await
    }
}

#[tonic::async_trait]
impl bedrock_lib::volume::actors_server::Actors for Gateway {
    /// Without a target cell the request enters here and is routed; with one it was forwarded, and is delivered in that
    /// cell or forwarded further down like a read.
    async fn invoke(
        &self,
        request: Request<bedrock_lib::volume::InvokeRequest>,
    ) -> Result<Response<bedrock_lib::volume::InvokeReply>, Status> {
        use bedrock_lib::volume::actors_client::ActorsClient;
        let router = self.router.as_ref();
        if request.metadata().get(CELL_HEADER).is_some() {
            let dispatch = actors::Dispatch::default();
            let dispatch = &dispatch;
            let result = self
                .serve(
                    request,
                    |service, r| async move {
                        if service != COMPUTE {
                            return Err(Status::invalid_argument("actor requests are for compute"));
                        }
                        let router =
                            router.ok_or_else(|| Status::unimplemented("no actor routing"))?;
                        router.deliver(r).await
                    },
                    |channel, request| {
                        dispatch
                            .send(async move { ActorsClient::new(channel).invoke(request).await })
                    },
                )
                .await;
            return dispatch.finish(result);
        }
        let Some(router) = router else {
            return Err(Status::invalid_argument(format!("missing {CELL_HEADER}")));
        };
        let Ok(_permit) = self.limit.try_acquire() else {
            self.count(COMPUTE, "limited");
            return Err(Status::resource_exhausted("internal listener at its limit"));
        };
        let request = request.into_inner();
        let actor = request
            .actor
            .ok_or_else(|| Status::invalid_argument("missing actor"))?;
        let result = router.route(actor, request.payload).await;
        self.count(COMPUTE, if result.is_ok() { "served" } else { "failed" });
        Ok(Response::new(bedrock_lib::volume::InvokeReply {
            payload: result?,
            ..Default::default()
        }))
    }
}

/// The tier of the first of `relays` that welcomes a leaf session without a member.
pub async fn tier_of(client: &Client, relays: &[String]) -> Result<u32, Status> {
    let mut last = Status::unavailable("no relays configured");
    for relay in relays {
        let hello = pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                ..Default::default()
            })),
        };
        let welcome = async {
            let mut frames = client
                .call_relay(relay, |channel| async move {
                    pb::relay_client::RelayClient::new(channel)
                        .session(tokio_stream::iter([hello]))
                        .await
                })
                .await?;
            match tokio::time::timeout(WELCOME, frames.message()).await {
                Ok(Ok(Some(pb::RelayFrame {
                    kind: Some(pb::relay_frame::Kind::Welcome(welcome)),
                }))) if welcome.version == 1 => Ok(welcome.tier),
                Ok(Err(status)) => Err(status),
                _ => Err(Status::unavailable("relay sent no welcome")),
            }
        };
        match welcome.await {
            Ok(tier) => return Ok(tier),
            Err(status) => last = status,
        }
    }
    Err(last)
}

/// This Gateway as a member of its cell.
fn member(node: &NodeConfig) -> pb::Member {
    pb::Member {
        node_id: node.node_id.clone(),
        service: GATEWAY.into(),
        region: node.location.region.clone(),
        dc: node.location.dc.clone(),
        cell: node.location.cell.clone(),
        failure_domain: node.location.failure_domain.clone(),
        status: pb::member::Status::Ready as i32,
        address: node.advertised(),
        ..Default::default()
    }
}

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: GatewayConfig) -> Result<(), ConfigError> {
    if config.relays.is_empty() {
        return Err(ConfigError("relays must not be empty".into()));
    }
    if config.internal_limit == 0 {
        return Err(ConfigError("internal_limit must be positive".into()));
    }
    tracing::info!(node_id = %config.node.node_id, relays = ?config.relays, "starting bedrock-gateway");
    let admin = node::start_admin(&config.node).await?;
    let tls = bedrock_lib::transport::tls::start(&config.node, GATEWAY, &admin).await?;
    let peers = Arc::new(bedrock_lib::transport::Peers::new(&admin).with_tls(tls.clone()));
    let client = Arc::new(Client::new(
        &admin,
        peers.clone(),
        config.relays.clone(),
        config.node.location.failure_domain.clone(),
    ));
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    // The role never changes, so it is learned once; relays may start after the Gateway.
    let tier = loop {
        tokio::select! {biased;
            () = &mut shutdown => return Ok(()),
            tier = tier_of(&client, &config.relays) => match tier {
                Ok(tier) => break tier,
                Err(status) => {
                    tracing::warn!(%status, "no relay welcomed the gateway; retrying");
                    tokio::time::sleep(RETRY).await;
                }
            },
        }
    };
    let (role, session, members) = match tier {
        1 => {
            let cell = config.node.location.cell.clone();
            let (members, target) = membership::View::new(GATEWAY, &cell);
            let session = bedrock_lib::relays::client::Session::join(
                peers.clone(),
                config.relays.clone(),
                vec![target],
                member(&config.node),
                Default::default(),
            )
            .map_err(|e| ConfigError(e.to_string()))?;
            (Role::Cell(cell), Some(session), Some(members))
        }
        2 | 3 => (
            Role::Upper(cells::Cells::new(
                client.clone(),
                config.relays.clone(),
                tier,
            )),
            None,
            None,
        ),
        tier => {
            return Err(ConfigError(format!(
                "a gateway cannot follow a tier {tier} relay"
            )));
        }
    };
    tracing::info!(role = role.label(), "gateway role");
    let mut gateway = Gateway::new(&admin, role, client.clone(), config.internal_limit);
    if let Role::Cell(cell) = &gateway.role {
        let home = Location {
            region: config.node.location.region.clone(),
            dc: config.node.location.dc.clone(),
            cell: cell.clone(),
        };
        let router = actors::Router::new(&admin, client, peers.clone(), home, config.actor_cache);
        gateway = gateway.with_actors(router);
    }
    let gateway = Arc::new(gateway);
    let router = bedrock_lib::transport::server(tls.as_ref())
        .add_service(pb::reads_server::ReadsServer::from_arc(gateway.clone()))
        .add_service(bedrock_lib::volume::actors_server::ActorsServer::from_arc(
            gateway,
        ));
    let mut server = std::pin::pin!(bedrock_lib::transport::serve(
        router,
        config.node.listen,
        tls
    ));
    let mut readiness = tokio::time::interval(Duration::from_millis(100));
    let result = loop {
        tokio::select! {biased;
            () = &mut shutdown => break Ok(()),
            result = &mut server => break result.map_err(|e| ConfigError(format!("listen: {e}"))),
            _ = readiness.tick() => {
                admin.set_ready(session.as_ref().is_none_or(|s| s.ready()));
                if let (Some(session), Some(members)) = (&session, &members)
                    && let Err(error) = members.refresh(session, &peers)
                {
                    tracing::warn!(%error, "membership view unreadable");
                }
            },
        }
    };
    admin.set_ready(false);
    if let Some(session) = session {
        session.drain().await;
    }
    tracing::info!("shutting down");
    result
}
