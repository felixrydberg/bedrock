//! Shared cluster client (#56 / B-25, the first slice of B-57's client). A process asks for a key; the client finds its
//! holders in a routing table fetched on demand from a cell relay (`Relay.Routes`) and reads from a holder directly.
//! Relays answer only table fetches, never reads (§4 decision 13), and a process holds tables only for services it uses:
//! one idle for [`IDLE_TTL`] is dropped and fetched again on next use.
//!
//! A table can be stale. A holder that no longer serves the range rejects the read and the client tries the next one;
//! once every candidate failed it fetches a newer table, a bounded number of times. Freshness is proven by the holder,
//! which answers with the applied revision of its range (`docs/implementation-prototype/kv.md` §9).
//!
//! A read of another cell ([`Client::get_in`]) goes through a Gateway (#89 / B-57, §4 decision 15): the target DC's
//! Gateways, then the target region's, listed in Quicksilver at [`gateways_key`]. The caller holds no table of the
//! target cell; a read in its own cell goes to the holder directly, as before.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tokio::time::Instant;
use tonic::Status;
use tonic::transport::Channel;

use crate::admin::Admin;
use crate::datasets::capabilities::{GATEWAY, QUICKSILVER};
use crate::membership::{self, Directories};
use crate::placement;
use crate::relays::sources::Sources;
use crate::replication::pb;
use crate::transport::{Class, Peers, tls};

const INFRASTRUCTURE: &str = "infrastructure";
/// A routing table not used for this long is dropped; the next read fetches a fresh one.
pub const IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const TIMEOUT: Duration = Duration::from_secs(5);
/// Newer tables fetched by one read after every candidate failed, before it gives up.
const REFRESHES: usize = 2;
/// Keys per range and page when a list sets no limit.
pub const LIST_LIMIT: u32 = 1000;
/// Encoded bytes of one range's list reply: tonic's default decode limit.
pub const LIST_BYTES: usize = 4 * 1024 * 1024;

/// A value, or `None` for a key proven absent, at the answering range's applied revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub value: Option<Vec<u8>>,
    pub revision: u64,
}

/// Keys in order. `revision` is the lowest applied revision among the ranges that answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Page {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub revision: u64,
    pub more: bool,
}

/// The service a request forwarded through a Gateway is for, as gRPC metadata.
pub const SERVICE_HEADER: &str = "bedrock-service";
/// The cell a request forwarded through a Gateway is for. A cell Gateway serves only its own (`docs/spec/cells.tla`).
pub const CELL_HEADER: &str = "bedrock-cell";

/// Where a cell is: a read of it from elsewhere enters its DC's or region's Gateways.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub region: String,
    pub dc: String,
    pub cell: String,
}

/// The `quicksilver/infrastructure` key listing a DC's upper-tier Gateways, or a region's with `dc` empty. Operators
/// write these ([`pb::Gateways`]); nothing expires an entry, and a dead address only costs a retry.
pub fn gateways_key(region: &str, dc: &str) -> Vec<u8> {
    if dc.is_empty() {
        format!("gateways/{region}").into_bytes()
    } else {
        format!("gateways/{region}/{dc}").into_bytes()
    }
}

/// `message` for `service` in `cell`, as a Gateway forwards it.
pub fn targeted<T>(message: T, service: &str, cell: &str) -> Result<tonic::Request<T>, Status> {
    let mut request = tonic::Request::new(message);
    for (name, value) in [(SERVICE_HEADER, service), (CELL_HEADER, cell)] {
        let value = value
            .parse()
            .map_err(|_| Status::invalid_argument(format!("invalid {name}")))?;
        request.metadata_mut().insert(name, value);
    }
    Ok(request)
}

/// The service and cell a forwarded request names.
pub fn target_of<T>(request: &tonic::Request<T>) -> Result<(String, String), Status> {
    let header = |name| {
        request
            .metadata()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| Status::invalid_argument(format!("missing {name}")))
    };
    Ok((header(SERVICE_HEADER)?, header(CELL_HEADER)?))
}

/// A service's published membership namespace in this cell, as one relay returned it.
pub struct Table {
    pub revision: u64,
    pub members: BTreeMap<String, pb::Member>,
    pub directories: Directories,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RefreshLabels {
    service: String,
    reason: &'static str,
}

pub struct Client {
    peers: Arc<Peers>,
    /// Cell relays, tried in order for table fetches.
    relays: Vec<String>,
    /// Relays of sibling cells in the same DC, by cell, for `volume` routes (§10 decision 15, #113 / S-05).
    siblings: BTreeMap<String, Vec<String>>,
    /// The caller's failure domain; holders in it are preferred.
    domain: String,
    /// Where the caller is; reads of other cells go through Gateways. Unset, [`Client::get_in`] refuses them.
    home: Option<Location>,
    tables: Mutex<BTreeMap<String, (Arc<Table>, Instant)>>,
    sources: Sources,
    refreshes: Family<RefreshLabels, Counter>,
}

impl Client {
    pub fn new(admin: &Admin, peers: Arc<Peers>, relays: Vec<String>, domain: String) -> Self {
        let refreshes = Family::default();
        admin.register(
            "client_route_refreshes",
            "Routing tables fetched from cell relays by service and reason",
            refreshes.clone(),
        );
        Self {
            peers,
            relays,
            siblings: BTreeMap::new(),
            domain,
            home: None,
            tables: Mutex::default(),
            sources: Sources::default(),
            refreshes,
        }
    }

    /// Adds sibling cells' relays, by cell, for [`Client::routes_in`].
    pub fn with_siblings(mut self, siblings: BTreeMap<String, Vec<String>>) -> Self {
        self.siblings = siblings;
        self
    }

    /// Sets where the caller is, for [`Client::get_in`].
    pub fn with_home(mut self, home: Location) -> Self {
        self.home = Some(home);
        self
    }

    /// `service`'s routing table: the cached one, or a fetched one when none is cached, it went idle, or the caller
    /// saw `stale` fail and needs one newer than that revision.
    pub async fn routes(&self, service: &str, stale: Option<u64>) -> Result<Arc<Table>, Status> {
        self.routes_from(service.to_owned(), &self.relays, service, None, stale)
            .await
    }

    /// [`Client::routes`] of sibling cell `cell`, from its relays.
    pub async fn routes_in(
        &self,
        cell: &str,
        service: &str,
        stale: Option<u64>,
    ) -> Result<Arc<Table>, Status> {
        let relays = self
            .siblings
            .get(cell)
            .ok_or_else(|| Status::failed_precondition(format!("no relays for cell {cell}")))?;
        self.routes_at(cell, relays, service, stale).await
    }

    /// [`Client::routes`] of `cell`, from `relays` the caller learned itself (an upper-tier Gateway, #89 / B-57).
    pub async fn routes_at(
        &self,
        cell: &str,
        relays: &[String],
        service: &str,
        stale: Option<u64>,
    ) -> Result<Arc<Table>, Status> {
        self.routes_from(
            format!("{service}.{cell}"),
            relays,
            service,
            Some(cell),
            stale,
        )
        .await
    }

    async fn routes_from(
        &self,
        key: String,
        relays: &[String],
        service: &str,
        cell: Option<&str>,
        stale: Option<u64>,
    ) -> Result<Arc<Table>, Status> {
        let now = crate::monotonic_now();
        let reason = {
            let mut tables = self.tables.lock().unwrap();
            // ponytail: swept on use, no background task; a process calls a handful of services.
            tables.retain(|_, (_, used)| now.duration_since(*used) < IDLE_TTL);
            match tables.get_mut(&key) {
                Some((table, used)) if stale.is_none_or(|stale| table.revision > stale) => {
                    *used = now;
                    return Ok(table.clone());
                }
                Some(_) => "stale",
                None => "missing",
            }
        };
        self.refreshes
            .get_or_create(&RefreshLabels {
                service: service.into(),
                reason,
            })
            .inc();
        let fetched = self.fetch(relays, service, cell, stale).await?;
        let mut tables = self.tables.lock().unwrap();
        let entry = tables.entry(key).or_insert_with(|| (fetched.clone(), now));
        // A lagging relay must not replace a newer table another caller fetched meanwhile.
        if fetched.revision >= entry.0.revision {
            entry.0 = fetched;
        }
        entry.1 = now;
        Ok(entry.0.clone())
    }

    /// The first relay table newer than `stale`. Relays apply the cell's view separately, so one may lag another; when
    /// none is newer, the newest answer. With `cell`, an answer naming a member of another cell is refused: a relay
    /// reprovisioned for another cell must not replace this cell's table, whose revisions it does not share.
    async fn fetch(
        &self,
        relays: &[String],
        service: &str,
        cell: Option<&str>,
        stale: Option<u64>,
    ) -> Result<Arc<Table>, Status> {
        let mut last = Status::unavailable("no cell relays configured");
        let mut best: Option<Arc<Table>> = None;
        for relay in relays {
            let request = pb::RoutesRequest {
                service: service.into(),
            };
            let reply = self
                .call(relay, None, |channel| async move {
                    pb::relay_client::RelayClient::new(channel)
                        .routes(request)
                        .await
                })
                .await;
            match reply {
                Ok(reply) => {
                    let (members, directories) =
                        membership::parse(reply.entries.into_iter().map(|e| Ok((e.key, e.value))))?;
                    if let Some(cell) = cell
                        && members.values().any(|member| member.cell != cell)
                    {
                        last = Status::failed_precondition(format!(
                            "relay {relay} answered with members of a cell other than {cell}"
                        ));
                        continue;
                    }
                    let table = Arc::new(Table {
                        revision: reply.revision,
                        members,
                        directories,
                    });
                    if stale.is_none_or(|stale| table.revision > stale) {
                        return Ok(table);
                    }
                    if best
                        .as_ref()
                        .is_none_or(|best| table.revision > best.revision)
                    {
                        best = Some(table);
                    }
                }
                Err(status) => last = status,
            }
        }
        best.ok_or(last)
    }

    /// `key` of a ranged `dataset` served by `service`, from a holder at least at `min_revision`.
    pub async fn get(
        &self,
        service: &str,
        dataset: &str,
        namespace: &str,
        key: &[u8],
        min_revision: u64,
    ) -> Result<Found, Status> {
        let hash = placement::hash_of(dataset, namespace, key);
        let mut stale = None;
        let mut last = Status::unavailable("no holder answered");
        for _ in 0..=REFRESHES {
            let table = self.routes(service, stale).await?;
            let request = pb::GetRequest {
                dataset: dataset.into(),
                namespace: namespace.into(),
                key: key.to_vec(),
                min_revision,
            };
            let reply = self
                .holders(&table, dataset, namespace, hash, min_revision, |channel| {
                    let request = request.clone();
                    async move {
                        pb::reads_client::ReadsClient::new(channel)
                            .get(request)
                            .await
                    }
                })
                .await;
            match reply {
                Ok(reply) => {
                    return Ok(Found {
                        value: reply.value,
                        revision: reply.revision,
                    });
                }
                Err(status) => last = status,
            }
            stale = Some(table.revision);
        }
        Err(last)
    }

    /// [`Client::get`] in `target`'s cell: directly in the caller's own cell, otherwise through a Gateway.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_in(
        &self,
        target: &Location,
        service: &str,
        dataset: &str,
        namespace: &str,
        key: &[u8],
        min_revision: u64,
    ) -> Result<Found, Status> {
        if self.at_home(target)? {
            return self
                .get(service, dataset, namespace, key, min_revision)
                .await;
        }
        let request = pb::GetRequest {
            dataset: dataset.into(),
            namespace: namespace.into(),
            key: key.to_vec(),
            min_revision,
        };
        let reply = self
            .through_gateways(target, |channel| {
                let request = targeted(request.clone(), service, &target.cell);
                async move {
                    pb::reads_client::ReadsClient::new(channel)
                        .get(request?)
                        .await
                }
            })
            .await?;
        Ok(Found {
            value: reply.value,
            revision: reply.revision,
        })
    }

    /// [`Client::list`] in `target`'s cell: directly in the caller's own cell, otherwise through a Gateway, which
    /// merges the cell's ranges.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_in(
        &self,
        target: &Location,
        service: &str,
        dataset: &str,
        namespace: &str,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        min_revision: u64,
    ) -> Result<Page, Status> {
        if self.at_home(target)? {
            return self
                .list(
                    service,
                    dataset,
                    namespace,
                    prefix,
                    start_after,
                    limit,
                    min_revision,
                )
                .await;
        }
        let request = pb::ListRequest {
            dataset: dataset.into(),
            namespace: namespace.into(),
            prefix: prefix.to_vec(),
            start_after: start_after.to_vec(),
            limit,
            min_revision,
            ..Default::default()
        };
        let reply = self
            .through_gateways(target, |channel| {
                let request = targeted(request.clone(), service, &target.cell);
                async move {
                    pb::reads_client::ReadsClient::new(channel)
                        .list(request?)
                        .await
                }
            })
            .await?;
        // ponytail: the cell Gateway merges the whole page into one reply, so a page over tonic's 4 MiB decode limit
        // fails; lower `limit` or page by range if cross-cell lists grow.
        Ok(Page {
            entries: reply
                .entries
                .into_iter()
                .map(|e| (e.key, e.value))
                .collect(),
            revision: reply.revision,
            more: reply.more,
        })
    }

    /// Tries the target DC's Gateways, then the target region's (the tier fallback), each listed in Quicksilver.
    /// ponytail: reads the lists from Quicksilver on every call, one round trip inside the caller's cell; cache them
    /// like routing tables if cross-cell reads get hot.
    async fn through_gateways<T, F, Fut>(&self, target: &Location, call: F) -> Result<T, Status>
    where
        F: Fn(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let mut last = Status::unavailable("no gateway listed for the target");
        for dc in [target.dc.as_str(), ""] {
            let key = gateways_key(&target.region, dc);
            let listed = match self
                .get(QUICKSILVER, QUICKSILVER, INFRASTRUCTURE, &key, 0)
                .await
            {
                Ok(found) => found.value,
                Err(status) => {
                    last = status;
                    continue;
                }
            };
            let Some(listed) = listed else { continue };
            let gateways = <pb::Gateways as prost::Message>::decode(listed.as_slice())
                .map_err(|e| Status::data_loss(format!("gateway list: {e}")))?;
            let addresses: Vec<String> =
                gateways.members.iter().map(|m| m.address.clone()).collect();
            for address in self.route(&addresses) {
                let Some(member) = gateways.members.iter().find(|m| m.address == *address) else {
                    continue;
                };
                // The record names only where a Gateway is; what it must present is a Gateway's certificate.
                let member = pb::Member {
                    service: GATEWAY.into(),
                    ..member.clone()
                };
                match self.call_member(&member, &call).await {
                    Ok(reply) => return Ok(reply),
                    Err(status) => last = status,
                }
            }
        }
        Err(last)
    }

    /// Whether `target` is the caller's own cell. Without a home, a read of another cell has nowhere to start.
    fn at_home(&self, target: &Location) -> Result<bool, Status> {
        match &self.home {
            Some(home) => Ok(home == target),
            None => Err(Status::failed_precondition(
                "this client has no home location for reads of other cells",
            )),
        }
    }

    /// `addresses` usable by real-traffic reachability first; all of them when none is.
    pub fn route<'a>(&self, addresses: &'a [String]) -> Vec<&'a String> {
        self.peers.health().route(addresses)
    }

    /// Calls the process `member` names, which must present its identity: a holder or Gateway a table names.
    pub async fn call_member<T, F, Fut>(&self, member: &pb::Member, call: F) -> Result<T, Status>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let identity = tls::Identity {
            service: member.service.clone(),
            cell: member.cell.clone(),
            node_id: member.node_id.clone(),
            host: crate::transport::host_of(&member.address).into(),
        };
        self.call(&member.address, Some(identity), call).await
    }

    /// Calls a relay at `address`; the server must present a relay's (or the writer's) certificate.
    pub async fn call_relay<T, F, Fut>(&self, address: &str, call: F) -> Result<T, Status>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        self.call(address, None, call).await
    }

    /// Up to `limit` keys of `namespace` with `prefix` after `start_after`, in key order, merged across every range of
    /// `dataset`. Page with the last returned key while `more` is set.
    #[allow(clippy::too_many_arguments)]
    pub async fn list(
        &self,
        service: &str,
        dataset: &str,
        namespace: &str,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        min_revision: u64,
    ) -> Result<Page, Status> {
        let limit = if limit == 0 { LIST_LIMIT } else { limit };
        let mut stale = None;
        let mut last = Status::unavailable("no holder answered");
        'tables: for _ in 0..=REFRESHES {
            let table = self.routes(service, stale).await?;
            stale = Some(table.revision);
            let Some(directory) = table.directories.get(dataset) else {
                last = Status::unavailable("dataset has no placement directory");
                continue;
            };
            let mut page = Page {
                revision: u64::MAX,
                ..Page::default()
            };
            // A range that stopped early has unsent keys after its last one, so the merged page ends there.
            let mut cut: Option<Vec<u8>> = None;
            // ponytail: asks every range for a full page and merges; a key-ordered index would ask fewer.
            for &start in directory.keys() {
                let request = pb::ListRequest {
                    dataset: dataset.into(),
                    namespace: namespace.into(),
                    prefix: prefix.to_vec(),
                    start_after: start_after.to_vec(),
                    limit,
                    range_start: start,
                    range_end: placement::end_of(directory, start),
                    min_revision,
                };
                let reply = self
                    .holders(&table, dataset, namespace, start, min_revision, |channel| {
                        let request = request.clone();
                        async move {
                            pb::reads_client::ReadsClient::new(channel)
                                .list(request)
                                .await
                        }
                    })
                    .await;
                match reply {
                    Ok(reply) => {
                        page.revision = page.revision.min(reply.revision);
                        if reply.more
                            && let Some(last) = reply.entries.last()
                            && cut.as_ref().is_none_or(|cut| last.key < *cut)
                        {
                            cut = Some(last.key.clone());
                        }
                        page.more |= reply.more;
                        page.entries
                            .extend(reply.entries.into_iter().map(|e| (e.key, e.value)));
                    }
                    Err(status) => {
                        last = status;
                        continue 'tables;
                    }
                }
            }
            page.entries.sort();
            if let Some(cut) = cut {
                page.entries.retain(|(key, _)| *key <= cut);
            }
            if page.entries.len() > limit as usize {
                page.entries.truncate(limit as usize);
                page.more = true;
            }
            if page.revision == u64::MAX {
                page.revision = 0;
            }
            return Ok(page);
        }
        Err(last)
    }

    /// Tries the holders of `hash` best first. A holder behind `min_revision` is reported to [`Sources`]; one that does
    /// not serve the range or cannot be reached is skipped.
    async fn holders<T, F, Fut>(
        &self,
        table: &Table,
        dataset: &str,
        namespace: &str,
        hash: u64,
        min_revision: u64,
        call: F,
    ) -> Result<T, Status>
    where
        F: Fn(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let directory = table.directories.get(dataset).cloned().unwrap_or_default();
        let candidates: Vec<pb::Member> = self
            .sources
            .select(
                &table.members,
                &directory,
                self.peers.health(),
                &self.domain,
                dataset,
                namespace,
                hash,
                min_revision,
            )
            .into_iter()
            .cloned()
            .collect();
        let mut last = Status::unavailable("no holder for the range");
        for member in &candidates {
            let node = member.node_id.clone();
            match self.call_member(member, &call).await {
                Ok(reply) => return Ok(reply),
                Err(status) => {
                    if status.code() == tonic::Code::Aborted {
                        let applied = min_revision.saturating_sub(1);
                        self.sources
                            .behind(&directory, &node, dataset, namespace, hash, applied);
                    }
                    last = status;
                }
            }
        }
        Err(last)
    }

    /// Calls `address`, which must present `identity` when given: a holder is the member its table names.
    async fn call<T, F, Fut>(
        &self,
        address: &str,
        identity: Option<tls::Identity>,
        call: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let channel = match identity {
            Some(identity) => self.peers.pinned(address, Class::Control, identity),
            None => self.peers.channel(address, Class::Control),
        }
        .map_err(|e| Status::unavailable(e.to_string()))?;
        let reply = self
            .peers
            .call(address, Class::Control, TIMEOUT, call(channel))
            .await?;
        Ok(reply.into_inner())
    }
}

#[cfg(test)]
#[path = "../tests/unit/client.rs"]
mod tests;
