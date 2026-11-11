//! Shared cluster client (#56 / B-25, the first slice of B-57's client). A process asks for a key; the client finds its
//! holders in a routing table fetched on demand from a cell relay (`Relay.Routes`) and reads from a holder directly.
//! Relays answer only table fetches, never reads (§4 decision 13), and a process holds tables only for services it uses:
//! one idle for [`IDLE_TTL`] is dropped and fetched again on next use.
//!
//! A table can be stale. A holder that no longer serves the range rejects the read and the client tries the next one;
//! once every candidate failed it fetches a newer table, a bounded number of times. Freshness is proven by the holder,
//! which answers with the applied revision of its range (`docs/implementation-prototype/kv.md` §9).

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
use crate::membership::{self, Directories};
use crate::placement;
use crate::relays::sources::Sources;
use crate::replication::pb;
use crate::transport::{Class, Peers, tls};

/// A routing table not used for this long is dropped; the next read fetches a fresh one.
pub const IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const TIMEOUT: Duration = Duration::from_secs(5);
/// Newer tables fetched by one read after every candidate failed, before it gives up.
const REFRESHES: usize = 2;
/// Keys per range and page when a list sets no limit.
pub const LIST_LIMIT: u32 = 1000;

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
    /// The caller's failure domain; holders in it are preferred.
    domain: String,
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
            domain,
            tables: Mutex::default(),
            sources: Sources::default(),
            refreshes,
        }
    }

    /// `service`'s routing table: the cached one, or a fetched one when none is cached, it went idle, or the caller
    /// saw `stale` fail and needs one newer than that revision.
    pub async fn routes(&self, service: &str, stale: Option<u64>) -> Result<Arc<Table>, Status> {
        let now = crate::monotonic_now();
        let reason = {
            let mut tables = self.tables.lock().unwrap();
            // ponytail: swept on use, no background task; a process calls a handful of services.
            tables.retain(|_, (_, used)| now.duration_since(*used) < IDLE_TTL);
            match tables.get_mut(service) {
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
        let fetched = self.fetch(service, stale).await?;
        let mut tables = self.tables.lock().unwrap();
        let entry = tables
            .entry(service.into())
            .or_insert_with(|| (fetched.clone(), now));
        // A lagging relay must not replace a newer table another caller fetched meanwhile.
        if fetched.revision >= entry.0.revision {
            entry.0 = fetched;
        }
        entry.1 = now;
        Ok(entry.0.clone())
    }

    /// The first relay table newer than `stale`. Relays apply the cell's view separately, so one may lag another; when
    /// none is newer, the newest answer.
    async fn fetch(&self, service: &str, stale: Option<u64>) -> Result<Arc<Table>, Status> {
        let mut last = Status::unavailable("no cell relays configured");
        let mut best: Option<Arc<Table>> = None;
        for relay in &self.relays {
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
            // ponytail: asks every range for a full page and merges; a key-ordered index would ask fewer.
            for &start in directory.keys() {
                let request = pb::ListRequest {
                    dataset: dataset.into(),
                    namespace: namespace.into(),
                    prefix: prefix.to_vec(),
                    start_after: start_after.to_vec(),
                    limit,
                    range_start: start,
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
        let candidates: Vec<(String, tls::Identity)> = self
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
            .map(|m| {
                let identity = tls::Identity {
                    service: m.service.clone(),
                    cell: m.cell.clone(),
                    node_id: m.node_id.clone(),
                    host: crate::transport::host_of(&m.address).into(),
                };
                (m.address.clone(), identity)
            })
            .collect();
        let mut last = Status::unavailable("no holder for the range");
        for (address, identity) in candidates {
            let node = identity.node_id.clone();
            match self.call(&address, Some(identity), &call).await {
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
