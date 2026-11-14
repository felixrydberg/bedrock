//! The `Volume` gRPC service: owner checks, leases and metrics around [`Store`] (#111 / S-03).
//!
//! - **Owner check.** A node serves an actor only while its cell's `volume` directory lists it as a `ready` or
//!   `leaving` owner of the range holding the actor's hash. A `populating` owner may lack the data and refuses (S-06
//!   makes it ready).
//! - **Leases.** Each Compute host heartbeats once per second; an actor's lease is live while its holder's host was seen
//!   within [`LEASE`]. A restarted node has seen no one yet, so for its first [`LEASE`] it treats every holder as live.
//! - **Callers.** Over mutual TLS, holder calls must come from that holder's Compute host, and `Truncate` and `Repair`
//!   from a volume node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use bedrock_lib::actors::Kind;
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{self, Directory};
use bedrock_lib::transport::Caller;
use bedrock_lib::volume as pb;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use tokio::time::Instant;
use tonic::{Request, Response, Status};

use crate::store::{self, Actor, Store};

/// How long a holder's host stays live after its last heartbeat (§5 Lease).
pub const LEASE: Duration = Duration::from_secs(5);
const COMPUTE: &str = "compute";
const VOLUME: &str = bedrock_lib::datasets::capabilities::VOLUME;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KindLabel {
    kind: &'static str,
}

fn label(kind: i32) -> KindLabel {
    KindLabel {
        kind: Kind::try_from(kind).map_or("unknown", Kind::as_str),
    }
}

type Latency = Family<KindLabel, Histogram, fn() -> Histogram>;

/// Labelled only by `cell`, `dc` (on the sub-registry) and actor kind; per-actor detail is for introspection.
struct Metrics {
    fsync: Latency,
    overlay: Family<KindLabel, Gauge>,
    expiries: Family<KindLabel, Counter>,
}

impl Metrics {
    fn new(admin: &Admin, cell: &str, dc: &str) -> Self {
        let metrics = Metrics {
            fsync: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.000_05, 2.0, 16))
            }),
            overlay: Family::default(),
            expiries: Family::default(),
        };
        admin.register_labelled(
            vec![("cell", cell.to_owned()), ("dc", dc.to_owned())],
            |registry| {
                registry.register(
                    "volume_commit_fsync_seconds",
                    "Synced batch time of appends and truncations",
                    metrics.fsync.clone(),
                );
                registry.register(
                    "volume_overlay_bytes",
                    "Keys and records in the overlay, not yet truncated after a flush",
                    metrics.overlay.clone(),
                );
                registry.register(
                    "volume_lease_expiries",
                    "Claims that took an actor over from a holder whose lease had expired",
                    metrics.expiries.clone(),
                );
            },
        );
        metrics
    }
}

pub struct Service {
    store: Arc<Store>,
    node: String,
    /// This cell's `volume` placement directory, refreshed by [`crate::run`].
    pub directory: Arc<RwLock<Directory>>,
    leases: Mutex<BTreeMap<(String, String), Instant>>,
    started: Instant,
    metrics: Metrics,
}

fn status(error: store::Error) -> Status {
    match error {
        store::Error::Invalid(detail) => Status::invalid_argument(detail),
        store::Error::Cold => {
            Status::unimplemented("reads from S3 arrive with the cold tier (S-05)")
        }
        store::Error::Storage(error) => match error {
            bedrock_lib::storage::StorageError::Corruption(_) => {
                Status::data_loss(error.to_string())
            }
            _ => Status::unavailable(error.to_string()),
        },
    }
}

fn holder(holder: Option<&pb::Holder>) -> Result<&pb::Holder, Status> {
    holder
        .filter(|h| !h.cell.is_empty() && !h.host.is_empty())
        .ok_or_else(|| Status::invalid_argument("a request names its holder's cell and host"))
}

impl Service {
    /// Opens the service over `store`, as node `node`, with metrics labelled by `cell` and `dc`.
    pub fn new(
        admin: &Admin,
        store: Arc<Store>,
        node: &str,
        cell: &str,
        dc: &str,
    ) -> Result<Self, Status> {
        let metrics = Metrics::new(admin, cell, dc);
        for (kind, bytes) in store.overlay_bytes().map_err(status)? {
            metrics.overlay.get_or_create(&label(kind)).set(bytes);
        }
        Ok(Service {
            store,
            node: node.to_owned(),
            directory: Arc::default(),
            leases: Mutex::default(),
            started: bedrock_lib::monotonic_now(),
            metrics,
        })
    }

    /// The actor, once this node serves the range holding it.
    fn actor(&self, actor: Option<&pb::Actor>) -> Result<Actor, Status> {
        let actor = actor.ok_or_else(|| Status::invalid_argument("a request names its actor"))?;
        let actor = Actor::new(actor).map_err(status)?;
        self.owns(actor.hash())?;
        Ok(actor)
    }

    fn owns(&self, hash: u64) -> Result<(), Status> {
        let directory = self
            .directory
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        if placement::serves(&directory, &self.node, hash) {
            return Ok(());
        }
        Err(Status::failed_precondition(format!(
            "{} is not a ready or leaving owner of the range holding {hash:016x}",
            self.node
        )))
    }

    fn live(&self, holder: &pb::Holder) -> bool {
        let now = bedrock_lib::monotonic_now();
        now.duration_since(self.started) < LEASE
            || self
                .leases
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&(holder.cell.clone(), holder.host.clone()))
                .is_some_and(|seen| now.duration_since(*seen) < LEASE)
    }

    fn record(&self, kind: i32, outcome: &store::Outcome) {
        let label = label(kind);
        if let Some(fsync) = outcome.fsync {
            self.metrics
                .fsync
                .get_or_create(&label)
                .observe(fsync.as_secs_f64());
        }
        if outcome.overlay != 0 {
            self.metrics
                .overlay
                .get_or_create(&label)
                .inc_by(outcome.overlay);
        }
    }
}

/// Over mutual TLS, fails unless the caller runs `service`, and, for a holder's call, is that holder's host.
fn check<T>(
    request: &Request<T>,
    service: &str,
    holder: Option<&pb::Holder>,
) -> Result<(), Status> {
    let Some(caller) = Caller::of(request) else {
        return Ok(());
    };
    caller.require(&[service], holder.map(|h| h.cell.as_str()))?;
    if holder.is_some_and(|h| h.host != caller.node_id) {
        return Err(Status::permission_denied(format!(
            "{} may not act for another holder",
            caller.node_id
        )));
    }
    Ok(())
}

#[tonic::async_trait]
impl pb::volume_server::Volume for Service {
    async fn reserve(
        &self,
        request: Request<pb::ReserveRequest>,
    ) -> Result<Response<pb::Answer>, Status> {
        check(&request, COMPUTE, None)?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let answer = self
            .store
            .reserve(&actor, request.epoch, request.version)
            .map_err(status)?;
        Ok(Response::new(answer))
    }

    async fn claim(
        &self,
        request: Request<pb::ClaimRequest>,
    ) -> Result<Response<pb::Answer>, Status> {
        let claimant = holder(request.get_ref().holder.as_ref())?.clone();
        check(&request, COMPUTE, Some(&claimant))?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let (answer, expired) = self
            .store
            .claim(&actor, &claimant, request.epoch, request.version, |h| {
                self.live(h)
            })
            .map_err(status)?;
        if expired {
            self.metrics
                .expiries
                .get_or_create(&label(actor.kind()))
                .inc();
        }
        Ok(Response::new(answer))
    }

    async fn append(
        &self,
        request: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::Answer>, Status> {
        check(
            &request,
            COMPUTE,
            Some(holder(request.get_ref().holder.as_ref())?),
        )?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let outcome = self.store.append(&actor, &request).map_err(status)?;
        self.record(actor.kind(), &outcome);
        Ok(Response::new(outcome.answer))
    }

    async fn release(
        &self,
        request: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::Answer>, Status> {
        let release = pb::entry::Kind::Release as i32;
        if request
            .get_ref()
            .entries
            .last()
            .is_none_or(|e| e.kind != release)
        {
            return Err(Status::invalid_argument(
                "a release ends with a RELEASE entry",
            ));
        }
        self.append(request).await
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatReply>, Status> {
        let host = holder(request.get_ref().holder.as_ref())?.clone();
        check(&request, COMPUTE, Some(&host))?;
        let now = bedrock_lib::monotonic_now();
        let mut leases = self.leases.lock().unwrap_or_else(PoisonError::into_inner);
        // Hosts seen long ago no longer hold a lease here.
        leases.retain(|_, seen| now.duration_since(*seen) < LEASE);
        leases.insert((host.cell, host.host), now);
        Ok(Response::new(pb::HeartbeatReply {}))
    }

    async fn read(
        &self,
        request: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadReply>, Status> {
        check(&request, COMPUTE, None)?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let (answer, values) = self
            .store
            .read(&actor, &request.keys, request.min_seq)
            .map_err(status)?;
        Ok(Response::new(pb::ReadReply {
            answer: Some(answer),
            values,
        }))
    }

    async fn scan(
        &self,
        request: Request<pb::ScanRequest>,
    ) -> Result<Response<pb::ScanReply>, Status> {
        check(&request, COMPUTE, None)?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let (answer, values, more) = self
            .store
            .scan(&actor, &request.start, request.limit, request.min_seq)
            .map_err(status)?;
        Ok(Response::new(pb::ScanReply {
            answer: Some(answer),
            values,
            more,
        }))
    }

    async fn truncate(
        &self,
        request: Request<pb::TruncateRequest>,
    ) -> Result<Response<pb::Answer>, Status> {
        check(&request, VOLUME, None)?;
        let request = request.into_inner();
        let actor = self.actor(request.actor.as_ref())?;
        let outcome = self
            .store
            .truncate(&actor, request.through)
            .map_err(status)?;
        self.record(actor.kind(), &outcome);
        Ok(Response::new(outcome.answer))
    }

    async fn repair(
        &self,
        request: Request<pb::RepairRequest>,
    ) -> Result<Response<pb::RepairReply>, Status> {
        check(&request, VOLUME, None)?;
        let request = request.into_inner();
        if !bedrock_lib::placement::valid_span(&bedrock_lib::replication::pb::Span {
            start: request.start,
            end: request.end,
        }) {
            return Err(Status::invalid_argument("an empty span"));
        }
        // The whole span must lie in one range this node serves.
        self.owns(request.start)?;
        let directory = self
            .directory
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let start = placement::start_of(&directory, request.start);
        if !bedrock_lib::relays::ranges::within(
            (request.start, request.end),
            (start, placement::end_of(&directory, start)),
        ) {
            return Err(Status::failed_precondition(
                "the span crosses a range boundary",
            ));
        }
        let (records, more) = self
            .store
            .records(request.start, request.end, &request.after, request.limit)
            .map_err(|e| status(e.into()))?;
        Ok(Response::new(pb::RepairReply { records, more }))
    }
}

#[cfg(test)]
#[path = "../tests/unit/serve.rs"]
mod tests;
