//! Actor routing (#116 / S-08, `statefull/v2.md` §6). Every location here is a hint: the volume's epochs and leases keep
//! one writer whatever a cache says (B-66), so a stale entry costs a redirect or a retry, never a second writer.
//!
//! - **Entry.** A request enters a cell Gateway: a Worker binding from a Compute host of the cell, or (B-57 part 2) a
//!   public request. That Gateway reads `topology/region/{code}` (decision 15 keeps upper tiers free of Quicksilver),
//!   computes `dc_of(id)` and `cells_of(id)` and keeps the [`Lru`] location cache (actor → DC, cell, host), learned from
//!   answers and redirects and dropped on failure.
//! - **Forwarding.** A request for another cell goes through the target DC's Gateways, then the region's, with the
//!   target cell in `bedrock-cell` metadata ([`Client::through_gateways`]); they pick the cell as for reads.
//! - **Delivery.** The target cell's Gateway hands it to the named host, or on a miss to the wake host among its own
//!   cell's healthy `compute` members ([`actors::host_in`]). A miss tries `cells_of(id)` in order, skipping a cell with
//!   no healthy host.
//! - **Redirects.** The host's `Acquire` serves, wakes, or redirects to the live holder. The entry Gateway follows at most
//!   [`REDIRECTS`], then answers `UNAVAILABLE` (a retryable 503).
//! - **At most once per request.** A host that took the event answers with its outcome, failures included. A forwarding
//!   hop tries another Gateway only after a failure that proves the event was not dispatched ([`Dispatch`]); after any
//!   other failure, a timeout included, it stops. The entry Gateway then drops the cached place and returns the error
//!   rather than send the event again. The caller retries.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use bedrock_lib::actors::{self, ActorId, Kind, Regions, Topology};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, Location, targeted};
use bedrock_lib::datasets::capabilities::COMPUTE;
use bedrock_lib::health::State;
use bedrock_lib::transport::Peers;
use bedrock_lib::volume as pb;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tonic::{Code, Status};

/// Locations the cache keeps by default.
pub const CACHE: usize = 100_000;
/// Redirects one request follows before it is answered with a retryable 503.
pub const REDIRECTS: usize = 2;

/// A bounded least-recently-used map.
pub struct Lru<K, V> {
    capacity: usize,
    tick: u64,
    entries: BTreeMap<K, (V, u64)>,
    /// Use tick → key, oldest first.
    order: BTreeMap<u64, K>,
}

impl<K: Ord + Clone, V: Clone> Lru<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            tick: 0,
            entries: BTreeMap::new(),
            order: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn touch(&mut self, key: &K) -> Option<&mut (V, u64)> {
        self.tick += 1;
        let entry = self.entries.get_mut(key)?;
        self.order.remove(&entry.1);
        entry.1 = self.tick;
        self.order.insert(self.tick, key.clone());
        Some(entry)
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        self.touch(key).map(|(value, _)| value.clone())
    }

    pub fn insert(&mut self, key: K, value: V) {
        if let Some(entry) = self.touch(&key) {
            entry.0 = value;
            return;
        }
        if self.entries.len() >= self.capacity
            && let Some((_, oldest)) = self.order.pop_first()
        {
            self.entries.remove(&oldest);
        }
        if self.capacity > 0 {
            self.order.insert(self.tick, key.clone());
            self.entries.insert(key, (value, self.tick));
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some((_, tick)) = self.entries.remove(key) {
            self.order.remove(&tick);
        }
    }
}

/// One actor event's sends through a forwarding loop built for reads (`Cells::forward`, `Client::through_gateways`),
/// which tries the next Gateway after any failure. Once a send fails in a way that may have reached a host (a timeout,
/// a broken connection, or a send cancelled mid-flight), every later send is refused without being made, so the event
/// runs at most once, and [`Dispatch::finish`] keeps that failure as the result.
#[derive(Default)]
pub struct Dispatch {
    failed: Mutex<Option<Status>>,
}

impl Dispatch {
    /// Failures a Gateway answers before it hands the event on: a cell it does not serve or has no healthy host for
    /// (`FAILED_PRECONDITION`), and its own limit (`RESOURCE_EXHAUSTED`). Host failures travel inside the reply.
    pub fn undispatched(status: &Status) -> bool {
        matches!(
            status.code(),
            Code::FailedPrecondition | Code::ResourceExhausted
        )
    }

    /// Sends with `send` unless an earlier send may have dispatched the event.
    pub async fn send<T>(
        &self,
        send: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        {
            let mut failed = self.lock();
            if let Some(status) = failed.clone() {
                return Err(status);
            }
            // Marked before the send, so one cancelled mid-flight (an outer timeout) still counts as dispatched.
            *failed = Some(Status::unavailable(
                "an earlier send of this actor event may have reached a host",
            ));
        }
        let result = send.await;
        let mut failed = self.lock();
        match &result {
            Err(status) if !Self::undispatched(status) => *failed = Some(status.clone()),
            _ => *failed = None,
        }
        result
    }

    /// The forwarding loop's result, or the failure that may have dispatched the event: a later routing error must not
    /// replace it, since a refusal would let the caller send the event elsewhere.
    pub fn finish<T>(&self, result: Result<T, Status>) -> Result<T, Status> {
        match (result, self.lock().clone()) {
            (Err(_), Some(status)) => Err(status),
            (result, _) => result,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Status>> {
        self.failed.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where an actor was last seen.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Place {
    dc: String,
    /// The holder; `host` is empty for a wake on a miss.
    holder: pb::Holder,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KindLabel {
    kind: &'static str,
}

/// A cell Gateway's actor routing.
pub struct Router {
    client: Arc<Client>,
    peers: Arc<Peers>,
    home: Location,
    /// (namespace, ID) → place.
    cache: Mutex<Lru<(String, String), Place>>,
    redirects: Family<KindLabel, Counter>,
}

/// The ID's region: its prefix, checked by [`ActorId::parse`] against the topology on a miss.
fn region(id: &str) -> Result<&str, Status> {
    id.rsplit_once('_')
        .map(|(region, _)| region)
        .ok_or_else(|| Status::invalid_argument("malformed actor ID"))
}

fn placement(error: actors::Error) -> Status {
    match error {
        actors::Error::Read(code, detail) => Status::new(code, detail),
        other => Status::invalid_argument(other.to_string()),
    }
}

impl Router {
    /// Routing for the cell Gateway at `home`, caching up to `capacity` locations.
    pub fn new(
        admin: &Admin,
        client: Arc<Client>,
        peers: Arc<Peers>,
        home: Location,
        capacity: usize,
    ) -> Self {
        let redirects = Family::default();
        admin.register_labelled(
            vec![("cell", home.cell.clone()), ("dc", home.dc.clone())],
            |registry| {
                registry.register(
                    "gateway_actor_redirects",
                    "Redirect answers to actor requests routed from this Gateway, by actor kind",
                    redirects.clone(),
                )
            },
        );
        Self {
            client,
            peers,
            home,
            cache: Mutex::new(Lru::new(capacity)),
            redirects,
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Lru<(String, String), Place>> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Routes an entering request to the actor's holder and returns its answer.
    pub async fn route(&self, actor: pb::Actor, payload: Vec<u8>) -> Result<Vec<u8>, Status> {
        let key = (actor.namespace.clone(), actor.id.clone());
        let mut place = self.cache().get(&key);
        let mut redirects = 0;
        loop {
            let result = match &place {
                Some(place) => self
                    .send(&actor, &payload, place)
                    .await
                    .map(|reply| (place.dc.clone(), reply)),
                None => self.wake(&actor, &payload).await,
            };
            let (dc, reply) = match result {
                Ok(answer) => answer,
                Err(status) => {
                    self.cache().remove(&key);
                    return Err(status);
                }
            };
            let holder = reply
                .holder
                .ok_or_else(|| Status::internal("compute answered without a holder"))?;
            let seen = Place { dc, holder };
            self.cache().insert(key.clone(), seen.clone());
            if reply.code != 0 {
                return Err(Status::new(reply.code.into(), reply.message));
            }
            if !reply.redirect {
                return Ok(reply.payload);
            }
            self.redirects
                .get_or_create(&KindLabel {
                    kind: Kind::try_from(actor.kind).map_or("unknown", Kind::as_str),
                })
                .inc();
            if redirects == REDIRECTS {
                return Err(Status::unavailable(
                    "the actor moved while it was routed; retry",
                ));
            }
            redirects += 1;
            place = Some(seen);
        }
    }

    /// A miss: the wake host in the first of `cells_of(id)` that has a healthy host.
    async fn wake(
        &self,
        actor: &pb::Actor,
        payload: &[u8],
    ) -> Result<(String, pb::InvokeReply), Status> {
        // ponytail: one Quicksilver read in this cell per miss; cache the topology by revision if misses get hot.
        let code = region(&actor.id)?;
        let (topology, _) = Topology::read(&self.client, code, 0)
            .await
            .map_err(placement)?;
        let regions: Regions = BTreeMap::from([(code.to_owned(), topology)]);
        let topology = &regions[code];
        let id = ActorId::parse(&regions, &actor.id).map_err(placement)?;
        let dc = actors::dc_of(&id, topology)
            .map_err(placement)?
            .name
            .clone();
        let mut last = Status::unavailable("no cell to wake the actor in");
        for cell in actors::cells_of(&id, topology).map_err(placement)? {
            let place = Place {
                dc: dc.clone(),
                holder: pb::Holder {
                    cell: cell.to_owned(),
                    host: String::new(),
                },
            };
            match self.send(actor, payload, &place).await {
                Ok(reply) => return Ok((dc, reply)),
                Err(status) if status.code() == Code::FailedPrecondition => last = status,
                Err(status) => return Err(status),
            }
        }
        Err(last)
    }

    /// Sends to `place`: delivered here in this cell, otherwise through the target DC's Gateways.
    async fn send(
        &self,
        actor: &pb::Actor,
        payload: &[u8],
        place: &Place,
    ) -> Result<pb::InvokeReply, Status> {
        let target = Location {
            region: region(&actor.id)?.to_owned(),
            dc: place.dc.clone(),
            cell: place.holder.cell.clone(),
        };
        let request = pb::InvokeRequest {
            actor: Some(actor.clone()),
            payload: payload.to_vec(),
            host: place.holder.host.clone(),
        };
        if target == self.home {
            return self.deliver(request).await;
        }
        let dispatch = Dispatch::default();
        let result = self
            .client
            .through_gateways(&target, |channel| {
                let request = targeted(request.clone(), COMPUTE, &target.cell);
                dispatch.send(async move {
                    pb::actors_client::ActorsClient::new(channel)
                        .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .invoke(request?)
                        .await
                })
            })
            .await;
        dispatch.finish(result)
    }

    /// Delivers to a Compute host of this cell: the named one while it is a member, otherwise the wake host.
    /// `FAILED_PRECONDITION` when the cell has no healthy host.
    pub async fn deliver(&self, request: pb::InvokeRequest) -> Result<pb::InvokeReply, Status> {
        let actor = request
            .actor
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing actor"))?;
        let hash = ActorId::hash_of(&actor.id).map_err(placement)?;
        // A cached table may predate a host that joined or name one that left: a miss fetches a newer one once.
        let mut stale = None;
        let member = loop {
            let table = self.client.routes(COMPUTE, stale).await?;
            let members: Vec<_> = table.members.values().cloned().collect();
            let healthy = |node: &str| {
                members.iter().any(|m| {
                    m.node_id == node && self.peers.health().state(&m.address) != State::Open
                })
            };
            let named = members.iter().find(|m| {
                !request.host.is_empty() && m.node_id == request.host && m.cell == self.home.cell
            });
            let found = match named {
                Some(member) => Some(member),
                None if stale.is_none() && !request.host.is_empty() => None,
                None => actors::host_in(hash, &self.home.cell, &members, healthy),
            };
            match found {
                Some(member) => break member.clone(),
                None if stale.is_none() => stale = Some(table.revision),
                None => {
                    return Err(Status::failed_precondition(format!(
                        "no healthy compute host in cell {}",
                        self.home.cell
                    )));
                }
            }
        };
        let request = pb::InvokeRequest {
            host: String::new(),
            ..request
        };
        self.client
            .call_member(&member, |channel| async move {
                pb::actors_client::ActorsClient::new(channel)
                    .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                    .invoke(request)
                    .await
            })
            .await
    }
}
