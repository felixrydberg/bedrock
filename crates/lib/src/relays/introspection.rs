//! Relay introspection API and low-cardinality metrics (#17 / B-18b).
//!
//! Metrics carry only `dataset` or `service`, plus the process's fixed `tier` and `cell` labels. Per-namespace and
//! per-subscriber detail is served as paginated JSON on
//! `/introspect/{upstream,namespaces,subscribers,membership,siblings}`, filtered by `dataset`, namespace `prefix` and `service`; the only per-namespace series are the top-[`TOP`] gauges.
//! End-to-end lag is `now - committed_at` on the hub's runtime clock as each event enters a relay, at every tier.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use serde_json::{Value, json};
use tokio::time::Instant;

use super::{Hub, Key, MemberKey, StreamKey};
use crate::membership::Publisher;
use crate::placement::State;
use crate::replication::log::Log;
use crate::replication::pb;
use crate::transport::admin::Admin;

/// Namespaces in each top-N gauge.
pub const TOP: usize = 20;
const RATE_WINDOW: Duration = Duration::from_secs(10);
const PAGE: usize = 100;
const MAX_PAGE: usize = 1000;
/// The service of sessions without a member: downstream relays, recovery sessions and member-less clients.
const NONE: &str = "none";

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Dataset {
    dataset: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Service {
    service: String,
}

type Lag = Family<Dataset, Histogram, fn() -> Histogram>;

/// What the hot path records; everything else is read from the hub when asked.
pub(super) struct Stats {
    // ponytail: never pruned, bounded by the namespaces this relay has carried, like the hub's head map.
    namespaces: Mutex<BTreeMap<Key, Namespace>>,
    upstream: Mutex<Option<String>>,
    disconnects: Mutex<BTreeMap<MemberKey, u64>>,
    events_in: Family<Dataset, Counter>,
    bytes_in: Family<Dataset, Counter>,
    events_out: Family<Dataset, Counter>,
    bytes_out: Family<Dataset, Counter>,
    lag: Lag,
    full: Family<Service, Counter>,
    follows: Family<Dataset, Counter>,
    recoveries: Family<Dataset, Counter>,
    catch_up: Lag,
}

struct Namespace {
    /// Greatest sequence received from upstream.
    latest: u64,
    committed_at: u64,
    /// End-to-end lag of the latest event when it arrived, without a clock none.
    lag_ms: Option<u64>,
    /// The head the upstream last announced in a `Range` frame (at follow and recovery).
    upstream_head: u64,
    window: Instant,
    counted: (u64, u64),
    rate: (f64, f64),
}

impl Namespace {
    fn new() -> Self {
        Namespace {
            latest: 0,
            committed_at: 0,
            lag_ms: None,
            upstream_head: 0,
            window: crate::monotonic_now(),
            counted: (0, 0),
            rate: (0.0, 0.0),
        }
    }

    /// Events and bytes per second over the last complete window; an idle namespace decays towards zero.
    fn rates(&self, now: Instant) -> (f64, f64) {
        let elapsed = now.duration_since(self.window);
        if elapsed < RATE_WINDOW {
            return self.rate;
        }
        let secs = elapsed.as_secs_f64();
        (self.counted.0 as f64 / secs, self.counted.1 as f64 / secs)
    }

    fn behind(&self) -> u64 {
        self.upstream_head.saturating_sub(self.latest)
    }
}

fn lag_histogram() -> Histogram {
    // 1 ms to about 131 s.
    Histogram::new(exponential_buckets(0.001, 2.0, 18))
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            namespaces: Mutex::default(),
            upstream: Mutex::default(),
            disconnects: Mutex::default(),
            events_in: Family::default(),
            bytes_in: Family::default(),
            events_out: Family::default(),
            bytes_out: Family::default(),
            lag: Family::new_with_constructor(lag_histogram),
            full: Family::default(),
            follows: Family::default(),
            recoveries: Family::default(),
            catch_up: Family::new_with_constructor(lag_histogram),
        }
    }
}

impl Stats {
    fn entry<R>(&self, key: &Key, f: impl FnOnce(&mut Namespace) -> R) -> R {
        let mut namespaces = self.namespaces.lock().unwrap();
        if !namespaces.contains_key(key) {
            namespaces.insert(key.clone(), Namespace::new());
        }
        f(namespaces.get_mut(key).unwrap())
    }

    /// An event arrived from upstream; `now_ms` is the hub's clock.
    pub(super) fn received(
        &self,
        key: &Key,
        sequence: u64,
        committed_at: u64,
        bytes: usize,
        now_ms: Option<u64>,
    ) {
        let labels = Dataset {
            dataset: key.0.clone(),
        };
        self.events_in.get_or_create(&labels).inc();
        self.bytes_in.get_or_create(&labels).inc_by(bytes as u64);
        let lag = now_ms.map(|now| now.saturating_sub(committed_at));
        if let Some(lag) = lag {
            self.lag.get_or_create(&labels).observe(lag as f64 / 1000.0);
        }
        let now = crate::monotonic_now();
        self.entry(key, |ns| {
            if now.duration_since(ns.window) >= RATE_WINDOW {
                ns.rate = ns.rates(now);
                ns.window = now;
                ns.counted = (0, 0);
            }
            ns.counted.0 += 1;
            ns.counted.1 += bytes as u64;
            if sequence >= ns.latest {
                ns.latest = sequence;
                ns.committed_at = committed_at;
                ns.lag_ms = lag;
            }
        });
    }

    pub(super) fn upstream_head(&self, key: &Key, head: u64) {
        self.entry(key, |ns| ns.upstream_head = ns.upstream_head.max(head));
    }

    pub(super) fn connected(&self, upstream: Option<&str>) {
        *self.upstream.lock().unwrap() = upstream.map(str::to_owned);
    }

    /// A delivery frame of `events` and `bytes` was queued for a subscriber.
    pub(super) fn sent(&self, dataset: String, events: usize, bytes: usize) {
        let labels = Dataset { dataset };
        self.events_out.get_or_create(&labels).inc_by(events as u64);
        self.bytes_out.get_or_create(&labels).inc_by(bytes as u64);
    }

    /// A subscriber asked for a live follow.
    pub(super) fn followed(&self, dataset: &str) {
        let dataset = dataset.to_owned();
        self.follows.get_or_create(&Dataset { dataset }).inc();
    }

    /// A subscriber or a proxying relay asked for recovery.
    pub(super) fn recovering(&self, dataset: &str) {
        let dataset = dataset.to_owned();
        self.recoveries.get_or_create(&Dataset { dataset }).inc();
    }

    /// A stream of a session that `opened` after leaving another relay reached the head.
    pub(super) fn caught_up(&self, dataset: &str, opened: Instant) {
        let dataset = dataset.to_owned();
        let took = crate::monotonic_now().duration_since(opened);
        self.catch_up
            .get_or_create(&Dataset { dataset })
            .observe(took.as_secs_f64());
    }

    pub(super) fn disconnected(&self, member: &pb::Member) {
        *self
            .disconnects
            .lock()
            .unwrap()
            .entry(crate::membership::key(member))
            .or_default() += 1;
    }
}

impl Hub {
    /// The live follow and recovery requests subscribers have made of this hub for `dataset`.
    pub fn requests(&self, dataset: &str) -> (u64, u64) {
        let labels = Dataset {
            dataset: dataset.to_owned(),
        };
        let stats = &self.0.stats;
        (
            stats.follows.get_or_create(&labels).get(),
            stats.recoveries.get_or_create(&labels).get(),
        )
    }

    /// Disconnects a subscriber whose queue stayed full, counting it by service.
    pub(super) fn drop_full(&self, id: u64) {
        let service = self.0.subscribers.lock().unwrap().get(&id).map(|sub| {
            sub.member
                .as_ref()
                .map_or(NONE.into(), |m| m.service.clone())
        });
        if let Some(service) = service {
            self.0.stats.full.get_or_create(&Service { service }).inc();
        }
        self.remove(id);
    }

    fn snapshot(&self) -> Vec<Subscription> {
        self.0
            .subscribers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, sub)| Subscription {
                id: *id,
                member: sub.member.clone(),
                follows: sub
                    .follows
                    .iter()
                    .map(|(key, interest)| (key.clone(), interest.acked))
                    .collect(),
                events: sub.events.load(std::sync::atomic::Ordering::Relaxed),
                bytes: sub.bytes.load(std::sync::atomic::Ordering::Relaxed),
            })
            .collect()
    }

    /// The greatest revision this hub knows of, without seeding the head map.
    fn known_head(&self, key: &Key) -> Option<u64> {
        if let Some(writer) = &self.0.writer {
            return Some(writer.log().head(&key.0, &key.1).get());
        }
        self.0.heads.lock().unwrap().get(key).and_then(|h| h.get())
    }

    fn now_ms(&self) -> Option<u64> {
        self.0.clock.lock().unwrap().map(|clock| clock())
    }
}

struct Subscription {
    id: u64,
    member: Option<pb::Member>,
    follows: Vec<(StreamKey, u64)>,
    events: usize,
    bytes: usize,
}

impl Subscription {
    fn service(&self) -> &str {
        self.member.as_ref().map_or(NONE, |m| m.service.as_str())
    }
}

/// Registers the relay's metrics, labelled with its `tier` and `cell`, and serves `/introspect/` on `admin`.
pub fn register(
    admin: &Admin,
    hub: &Hub,
    tier: &str,
    cell: &str,
    publisher: Option<Arc<Publisher>>,
) {
    let stats = &hub.0.stats;
    admin.register_labelled(
        vec![("tier", tier.into()), ("cell", cell.into())],
        |registry: &mut Registry| {
            registry.register(
                "relay_events_in",
                "Events received from upstream",
                stats.events_in.clone(),
            );
            registry.register(
                "relay_bytes_in",
                "Encoded event bytes received from upstream",
                stats.bytes_in.clone(),
            );
            registry.register(
                "relay_events_out",
                "Events queued to subscribers",
                stats.events_out.clone(),
            );
            registry.register(
                "relay_bytes_out",
                "Delivery frame bytes queued to subscribers",
                stats.bytes_out.clone(),
            );
            registry.register(
                "relay_lag_seconds",
                "End-to-end lag, now - committed_at, as events reach this relay",
                stats.lag.clone(),
            );
            registry.register(
                "relay_full_queue_disconnects",
                "Subscribers disconnected because their queue stayed full",
                stats.full.clone(),
            );
            registry.register(
                "relay_follows",
                "Live follows subscribers asked of this relay",
                stats.follows.clone(),
            );
            registry.register(
                "relay_recoveries",
                "Recovery requests subscribers and proxying relays made of this relay",
                stats.recoveries.clone(),
            );
            registry.register(
                "relay_failover_catch_up_seconds",
                "From a session arriving from another relay to each of its streams reaching the head",
                stats.catch_up.clone(),
            );
            registry.register_collector(Box::new(Collector {
                hub: hub.clone(),
                publisher: publisher.clone(),
            }));
        },
    );
    let hub = hub.clone();
    admin.introspect(move |view, query| {
        let query = Query::parse(query);
        Some(match view {
            "upstream" => upstream(&hub, &query),
            "namespaces" => namespaces(&hub, &query),
            "subscribers" => subscribers(&hub, &query),
            "membership" => membership(publisher.as_deref(), &query),
            "siblings" => siblings(&hub, &query),
            _ => return None,
        })
        .map(|value| value.to_string())
    });
}

/// Gauges computed from the hub at scrape time, so they are current within one scrape interval.
struct Collector {
    hub: Hub,
    publisher: Option<Arc<Publisher>>,
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelayCollector").finish_non_exhaustive()
    }
}

fn gauges<L: EncodeLabelSet>(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    values: impl IntoIterator<Item = (L, f64)>,
) -> fmt::Result {
    let mut metric = encoder.encode_descriptor(name, help, None, MetricType::Gauge)?;
    for (labels, value) in values {
        ConstGauge::new(value).encode(metric.encode_family(&labels)?)?;
    }
    Ok(())
}

fn counters<L: EncodeLabelSet>(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    values: impl IntoIterator<Item = (L, u64)>,
) -> fmt::Result {
    let mut metric = encoder.encode_descriptor(name, help, None, MetricType::Counter)?;
    for (labels, value) in values {
        ConstCounter::new(value).encode(metric.encode_family(&labels)?)?;
    }
    Ok(())
}

/// The value at quantile `q` of sorted `values`.
fn quantile(values: &[u64], q: f64) -> u64 {
    let index = ((values.len() as f64 * q).ceil() as usize).clamp(1, values.len()) - 1;
    values[index]
}

impl prometheus_client::collector::Collector for Collector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> fmt::Result {
        let hub = &self.hub;
        let subs = hub.snapshot();
        let mut by_service = BTreeMap::<&str, u64>::new();
        for sub in &subs {
            *by_service.entry(sub.service()).or_default() += 1;
        }
        gauges(
            &mut encoder,
            "relay_subscribers",
            "Connected subscriber sessions by service; sessions without a member count as none",
            by_service
                .iter()
                .map(|(service, n)| ([("service", *service)], *n as f64)),
        )?;

        let shared = hub.0.shared.lock().unwrap();
        let streams = shared.values().filter(|s| s.live.is_some()).count();
        // `sibling_interests` below takes this lock.
        drop(shared);
        let labels: [(&str, &str); 0] = [];
        gauges(
            &mut encoder,
            "relay_sibling_streams",
            "Sibling streams currently synced",
            [(labels, streams as f64)],
        )?;
        let (followed, cold) = hub.sibling_interests();
        for (name, help, interests) in [
            (
                "relay_sibling_interests",
                "Sibling interests followed beyond this relay's own",
                &followed,
            ),
            (
                "relay_sibling_interests_cold",
                "Sibling interests left cold by the cap",
                &cold,
            ),
        ] {
            let mut counts = BTreeMap::<&str, u64>::new();
            for follow in interests {
                *counts.entry(follow.dataset.as_str()).or_default() += 1;
            }
            gauges(
                &mut encoder,
                name,
                help,
                counts
                    .iter()
                    .map(|(dataset, n)| ([("dataset", *dataset)], *n as f64)),
            )?;
        }

        let now = crate::monotonic_now();
        let (mut behind, mut rate, mut lagging) =
            (BTreeMap::<&str, Vec<u64>>::new(), Vec::new(), Vec::new());
        // ponytail: one O(namespaces) pass per scrape under the stats lock; keep running aggregates if scrapes
        // ever show up in hot-path lock contention.
        let namespaces = hub.0.stats.namespaces.lock().unwrap();
        for (key, ns) in namespaces.iter() {
            behind.entry(key.0.as_str()).or_default().push(ns.behind());
            let events = ns.rates(now).0;
            if events > 0.0 {
                rate.push((events, key));
            }
            if ns.behind() > 0 {
                lagging.push((ns.behind(), key));
            }
        }
        for values in behind.values_mut() {
            values.sort_unstable();
        }
        gauges(
            &mut encoder,
            "relay_upstream_lag_revisions_max",
            "Greatest revision lag behind the upstream's announced head across namespaces",
            behind
                .iter()
                .map(|(dataset, v)| ([("dataset", *dataset)], *v.last().unwrap() as f64)),
        )?;
        gauges(
            &mut encoder,
            "relay_upstream_lag_revisions_p99",
            "p99 revision lag behind the upstream's announced head across namespaces",
            behind
                .iter()
                .map(|(dataset, v)| ([("dataset", *dataset)], quantile(v, 0.99) as f64)),
        )?;
        rate.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        gauges(
            &mut encoder,
            "relay_top_namespace_events_per_second",
            "The busiest namespaces by received events per second; the only per-namespace series",
            rate.iter().take(TOP).map(|(events, key)| {
                (
                    [("dataset", key.0.as_str()), ("namespace", key.1.as_str())],
                    *events,
                )
            }),
        )?;
        lagging.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        gauges(
            &mut encoder,
            "relay_top_namespace_lag_revisions",
            "The namespaces furthest behind their upstream, in revisions; the only per-namespace series",
            lagging.iter().take(TOP).map(|(lag, key)| {
                (
                    [("dataset", key.0.as_str()), ("namespace", key.1.as_str())],
                    *lag as f64,
                )
            }),
        )?;
        drop(namespaces);

        // Without a clock, no capacity eviction counts as recent.
        let now_ms = hub.now_ms().unwrap_or(u64::MAX);
        let followed: BTreeSet<&Key> = subs
            .iter()
            .flat_map(|s| s.follows.iter().map(|((k, _), _)| k))
            .collect();
        let mut coverage = BTreeMap::<&str, (u64, u64)>::new();
        for key in followed {
            let covered = hub
                .0
                .replay
                .as_ref()
                .is_some_and(|replay| replay.covers_window(&key.0, &key.1, now_ms));
            let entry = coverage.entry(key.0.as_str()).or_default();
            entry.0 += covered as u64;
            entry.1 += 1;
        }
        gauges(
            &mut encoder,
            "relay_replay_coverage_ratio",
            "Share of subscribed namespaces whose replay holds everything that arrived within the window",
            coverage.iter().map(|(dataset, (covered, n))| {
                ([("dataset", *dataset)], *covered as f64 / *n as f64)
            }),
        )?;

        let Some(publisher) = &self.publisher else {
            return Ok(());
        };
        let mut ranges = Vec::new();
        if let Ok(views) = publisher.views() {
            for (service, _, directories) in &views {
                let mut counts = [0u64; 3];
                for owners in directories.values().flat_map(|d| d.values()) {
                    counts[range_state(owners) as usize] += 1;
                }
                for (state, n) in ["populating", "ready", "leaving"].into_iter().zip(counts) {
                    if !directories.is_empty() {
                        ranges.push(([("service", *service), ("state", state)], n as f64));
                    }
                }
            }
        }
        gauges(
            &mut encoder,
            "relay_placement_ranges",
            "Placement directory ranges by state: leaving if any owner is, else populating if any owner is",
            ranges,
        )?;
        gauges(
            &mut encoder,
            "relay_placement_frozen_seconds",
            "How long membership and placement have been frozen without a live majority",
            [(
                Vec::<(&str, &str)>::new(),
                publisher.frozen().map_or(0.0, |d| d.as_secs_f64()),
            )],
        )?;
        let writes = publisher.directory_writes();
        counters(
            &mut encoder,
            "relay_placement_directory_writes",
            "Placement directory entries this relay wrote",
            writes
                .iter()
                .map(|(service, n)| ([("service", service.as_str())], *n)),
        )
    }
}

/// 0 populating, 1 ready, 2 leaving, as in `relay_placement_ranges`.
fn range_state(owners: &[crate::placement::Owner]) -> u8 {
    if owners.iter().any(|o| o.state == State::Leaving) {
        2
    } else if owners.iter().any(|o| o.state == State::Populating) {
        0
    } else {
        1
    }
}

fn state_name(state: &State) -> &'static str {
    match state {
        State::Populating => "populating",
        State::Ready => "ready",
        State::Leaving => "leaving",
    }
}

#[derive(Debug, Default, PartialEq)]
struct Query {
    dataset: Option<String>,
    prefix: String,
    service: Option<String>,
    after: Option<String>,
    limit: usize,
}

impl Query {
    fn parse(query: &str) -> Self {
        let mut parsed = Query {
            limit: PAGE,
            ..Query::default()
        };
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = decode(value);
            match name {
                "dataset" => parsed.dataset = Some(value),
                "prefix" => parsed.prefix = value,
                "service" => parsed.service = Some(value),
                "after" => parsed.after = Some(value),
                "limit" => parsed.limit = value.parse().unwrap_or(PAGE).clamp(1, MAX_PAGE),
                _ => {}
            }
        }
        parsed
    }

    fn matches(&self, key: &Key) -> bool {
        self.dataset.as_ref().is_none_or(|d| *d == key.0) && key.1.starts_with(&self.prefix)
    }

    /// `after` names a namespace as `dataset/namespace`; datasets contain no `/`.
    fn after_key(&self) -> Option<Key> {
        let (dataset, namespace) = self.after.as_ref()?.split_once('/')?;
        Some((dataset.into(), namespace.into()))
    }
}

/// Percent-decodes a query value; `+` is a space.
fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| value.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (escaped, bytes[i]) {
            (Some(byte), _) => {
                out.push(byte);
                i += 2;
            }
            (None, b'+') => out.push(b' '),
            (None, byte) => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One page of `items`, sorted by cursor, after `after`; `next` resumes after the last item.
fn page<K: Ord>(
    items: impl Iterator<Item = (K, Value)>,
    after: Option<K>,
    limit: usize,
    cursor: impl Fn(&K) -> String,
) -> Value {
    let mut items = items
        .filter(|(k, _)| after.as_ref().is_none_or(|a| k > a))
        .peekable();
    let mut out = Vec::new();
    let mut last = None;
    while out.len() < limit
        && let Some((key, value)) = items.next()
    {
        out.push(value);
        last = Some(key);
    }
    let next = items.peek().and(last).map(|k| cursor(&k));
    json!({ "items": out, "next": next })
}

fn namespace_cursor(key: &Key) -> String {
    format!("{}/{}", key.0, key.1)
}

/// Up to `limit` keys of `map` after the query's cursor that match its dataset and prefix. It seeks past
/// non-matching ranges instead of scanning, so a page costs O((datasets + limit) log n) however many namespaces exist.
fn seek<V>(map: &BTreeMap<Key, V>, query: &Query, limit: usize) -> Vec<Key> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let mut from = query.after_key().map_or(Unbounded, Excluded);
    let mut out = Vec::new();
    while out.len() < limit
        && let Some((key, _)) = map.range((from.clone(), Unbounded)).next()
    {
        from = match &query.dataset {
            Some(dataset) if key.0 < *dataset => Included((dataset.clone(), query.prefix.clone())),
            Some(dataset) if key.0 > *dataset => break,
            _ if key.1 < query.prefix => Included((key.0.clone(), query.prefix.clone())),
            // The smallest dataset after `key.0`: nothing sorts between a string and itself plus NUL.
            _ if !key.1.starts_with(&query.prefix) => {
                Included((format!("{} ", key.0), String::new()))
            }
            _ => {
                out.push(key.clone());
                Excluded(key.clone())
            }
        };
    }
    out
}

/// The namespaces of one page plus one, to tell whether another follows. With a service filter they come from
/// that service's follows; otherwise from the namespaces this relay carried and `followed`.
fn candidates(
    hub: &Hub,
    query: &Query,
    served: Option<BTreeSet<Key>>,
    followed: &BTreeMap<Key, u64>,
) -> Vec<Key> {
    let limit = query.limit + 1;
    let namespaces = hub.0.stats.namespaces.lock().unwrap();
    let after = query.after_key();
    if let Some(served) = served {
        return served
            .into_iter()
            .filter(|k| {
                query.matches(k)
                    && after.as_ref().is_none_or(|a| k > a)
                    && (namespaces.contains_key(k) || followed.contains_key(k))
            })
            .take(limit)
            .collect();
    }
    let keys: BTreeSet<Key> = seek(&namespaces, query, limit)
        .into_iter()
        .chain(seek(followed, query, limit))
        .collect();
    keys.into_iter().take(limit).collect()
}

/// Namespaces followed by a subscriber of `service`, when filtered by service.
fn served(subs: &[Subscription], query: &Query) -> Option<BTreeSet<Key>> {
    let service = query.service.as_ref()?;
    Some(
        subs.iter()
            .filter(|s| s.service() == service)
            .flat_map(|s| s.follows.iter().map(|((k, _), _)| k.clone()))
            .collect(),
    )
}

/// The connected upstream, and per namespace the cursor received through, the upstream's announced head and lag.
fn upstream(hub: &Hub, query: &Query) -> Value {
    let served = served(&hub.snapshot(), query);
    let keys = candidates(hub, query, served, &BTreeMap::new());
    let namespaces = hub.0.stats.namespaces.lock().unwrap();
    let items = keys.into_iter().filter_map(|key| {
        let ns = namespaces.get(&key)?;
        let value = json!({
            "dataset": key.0,
            "namespace": key.1,
            "cursor": ns.latest,
            "upstream_head": ns.upstream_head,
            "behind": ns.behind(),
            "committed_at": ns.committed_at,
            "lag_ms": ns.lag_ms,
        });
        Some((key, value))
    });
    let mut value = page(items, query.after_key(), query.limit, namespace_cursor);
    drop(namespaces);
    value["connected"] = json!(*hub.0.stats.upstream.lock().unwrap());
    value
}

/// The interests siblings share beyond this relay's own, each `followed` or `cold`, and every sibling's stream.
fn siblings(hub: &Hub, query: &Query) -> Value {
    let (followed, cold) = hub.sibling_interests();
    let states = followed
        .iter()
        .map(|follow| (follow, "followed"))
        .chain(cold.iter().map(|follow| (follow, "cold")));
    let mut interests = BTreeMap::new();
    for (follow, state) in states {
        let key = (follow.dataset.clone(), follow.namespace.clone());
        if query.matches(&key) {
            let value = json!({
                "dataset": follow.dataset,
                "namespace": follow.namespace,
                "range": follow.span.map(|s| [format!("{:016x}", s.start), format!("{:016x}", s.end)]),
                "state": state,
            });
            interests.insert(key, value);
        }
    }
    let mut value = page(
        interests.into_iter(),
        query.after_key(),
        query.limit,
        namespace_cursor,
    );
    let shared = hub.0.shared.lock().unwrap();
    let streams: BTreeMap<&String, Value> = shared
        .iter()
        .map(|(sibling, shared)| {
            let stream = json!({
                "connected": shared.live.is_some(),
                "interests": shared.live.as_ref().map_or(0, BTreeMap::len),
                "lingering": shared.lingering.len(),
            });
            (sibling, stream)
        })
        .collect();
    value["streams"] = json!(streams);
    value
}

/// Per namespace: subscribers, latest sequence received and head, replay ranges and rates. A subscriber following
/// multiple ranges of a namespace is counted once.
fn namespaces(hub: &Hub, query: &Query) -> Value {
    let subs = hub.snapshot();
    let served = served(&subs, query);
    let mut subscribers = BTreeMap::<Key, u64>::new();
    for sub in &subs {
        // A subscriber following a namespace in several ranges counts once.
        let keys: BTreeSet<&Key> = sub.follows.iter().map(|((key, _), _)| key).collect();
        for key in keys {
            *subscribers.entry(key.clone()).or_default() += 1;
        }
    }
    let keys = candidates(hub, query, served, &subscribers);
    let stats: BTreeMap<Key, _> = {
        let now = crate::monotonic_now();
        let namespaces = hub.0.stats.namespaces.lock().unwrap();
        keys.iter()
            .filter_map(|k| {
                let ns = namespaces.get(k)?;
                Some((k.clone(), (ns.latest, ns.rates(now), ns.lag_ms)))
            })
            .collect()
    };
    let items = keys.into_iter().map(|key| {
        let (latest, (events, bytes), lag_ms) = stats.get(&key).copied().unwrap_or_default();
        let replay = hub
            .0
            .replay
            .as_ref()
            .map(|r| r.ranges(&key.0, &key.1))
            .unwrap_or_default();
        let value = json!({
            "dataset": key.0,
            "namespace": key.1,
            "subscribers": subscribers.get(&key).copied().unwrap_or(0),
            "latest": latest,
            "head": hub.known_head(&key),
            "replay": replay,
            "events_per_second": events,
            "bytes_per_second": bytes,
            "lag_ms": lag_ms,
        });
        (key, value)
    });
    page(items, query.after_key(), query.limit, namespace_cursor)
}

/// Per session: identity, follows with range, acknowledged revision and lag, queue depth and the member's disconnects.
fn subscribers(hub: &Hub, query: &Query) -> Value {
    let disconnects = hub.0.stats.disconnects.lock().unwrap().clone();
    let filtered = query.dataset.is_some() || !query.prefix.is_empty();
    let items = hub.snapshot().into_iter().filter_map(|sub| {
        if query.service.as_ref().is_some_and(|s| s != sub.service()) {
            return None;
        }
        let follows: Vec<Value> = sub
            .follows
            .iter()
            .filter(|((key, _), _)| query.matches(key))
            .map(|((key, range), acked)| {
                json!({
                    "dataset": key.0,
                    "namespace": key.1,
                    "range": range.map(|(start, end)| [format!("{start:016x}"), format!("{end:016x}")]),
                    "acked": acked,
                    "lag": hub.known_head(key).unwrap_or(0).saturating_sub(*acked),
                })
            })
            .collect();
        if filtered && follows.is_empty() {
            return None;
        }
        let member = sub.member.as_ref();
        let value = json!({
            "id": sub.id,
            "service": sub.service(),
            "node": member.map(|m| &m.node_id),
            "address": member.map(|m| &m.address),
            "follows": follows,
            "queued_events": sub.events,
            "queued_bytes": sub.bytes,
            "disconnects": member.map_or(0, |m| disconnects.get(&crate::membership::key(m)).copied().unwrap_or(0)),
        });
        Some((sub.id, value))
    });
    let after = query.after.as_ref().and_then(|a| a.parse().ok());
    page(items, after, query.limit, u64::to_string)
}

/// The cell view this relay publishes, with each member's status and per-dataset placement directories. A failed view
/// read is returned in the JSON error field.
fn membership(publisher: Option<&Publisher>, query: &Query) -> Value {
    let Some(publisher) = publisher else {
        return json!({ "publishing": false, "services": [] });
    };
    let services: Vec<Value> = match publisher.views() {
        Ok(views) => views
            .into_iter()
            .filter(|(service, ..)| query.service.as_ref().is_none_or(|s| s == service))
            .map(|(service, members, directories)| {
                let members: Vec<Value> = members
                    .values()
                    .map(|m| {
                        let status = pb::member::Status::try_from(m.status)
                            .map_or("unknown", |s| s.as_str_name());
                        json!({ "node": m.node_id, "status": status, "address": m.address })
                    })
                    .collect();
                let ranges: Vec<Value> = directories
                    .iter()
                    .flat_map(|(dataset, directory)| {
                        directory.iter().map(move |(start, owners)| {
                            let owners: Vec<Value> = owners
                                .iter()
                                .map(|o| json!({ "node": o.node, "state": state_name(&o.state) }))
                                .collect();
                            json!({ "dataset": dataset, "start": format!("{start:016x}"), "owners": owners })
                        })
                    })
                    .collect();
                json!({ "service": service, "members": members, "ranges": ranges })
            })
            .collect(),
        Err(error) => return json!({ "publishing": true, "error": error.message() }),
    };
    json!({
        "publishing": true,
        "frozen_seconds": publisher.frozen().map(|d| d.as_secs_f64()),
        "services": services,
    })
}

#[cfg(test)]
#[path = "../../tests/unit/relays/introspection.rs"]
mod tests;
