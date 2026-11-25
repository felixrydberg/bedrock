//! Local storage health (#13 / B-14, `kv.md` §15.1–15.2). Each dataset's engine is [`StorageHealth::wrap`]ped, so every
//! I/O, corruption and unavailability error reaches the process's one manager.
//!
//! - **Quarantine.** `Corruption` marks the engine instance `Unhealthy` at once, as do [`CONSECUTIVE_FAILURES`] I/O or
//!   unavailability errors in a row, since the extent of the damage is unknown. `Fenced` and `Unsupported` are not
//!   storage failures.
//! - **Narrowing** (#103 / B-62). A rebuild first verifies the whole instance and discards the damage it finds
//!   ([`crate::reconciliation::rebuild`]). When every damaged span lies in L2 hash ranges, [`StorageHealth::narrow`] keeps
//!   only the ranges overlapping them quarantined, and the rest serve again. Calls are then refused by key: those touching
//!   a quarantined range, and those that cannot be confined to one, such as `restore`. Any new failure quarantines the
//!   whole instance again.
//!   The exception is damage confined to one content-addressed item, which [`StorageHealth::report_contained`] counts
//!   like an I/O error (the CDN artifact cache, B-30).
//! - **Isolation.** A quarantined instance answers every call with `Unavailable`, never a miss, so callers fall back to
//!   other sources. Other datasets keep running, and readiness ignores quarantined ones. The process announces
//!   [`StorageHealth::unhealthy`] datasets in its member updates, so relays assign it no new ranges and peers skip it as a
//!   source for those datasets (`docs/spec/membership.md`), or only for its [`StorageHealth::damaged`] ranges.
//! - **Recovery** is [`crate::reconciliation::rebuild`] (#23 / B-22). A `Rebuilding` source still refuses every
//!   [`StorageHealth::wrap`]ped call, so nothing is served from it, while its [`StorageHealth::wrap_replication`] handle
//!   restores it. Only a validated rebuild makes it [`StorageHealth::recovered`].
//! - **Detection.** Reads find damage, and [`super::scrub`] reads what nothing else does.
//! - **Restarts.** Quarantine is recorded in the engine itself, so a process that stops before its rebuild finishes starts
//!   quarantined rather than serving damage nothing would notice: the whole instance, or the ranges a narrowed record
//!   lists. A record that cannot be read as ranges means the whole instance. A rebuild removes it once it validated.
//!   Recording is best effort, since the failing disk may refuse it.
//!
//! Processes are per service, so a broken CDN cache disk never marks the same machine's KV node unhealthy.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

use super::CONSECUTIVE_FAILURES;
use crate::admin::Admin;
use crate::relays::ranges::Range;
use crate::storage::{
    ErrorContext, KeyRange, Scan, Snapshot, StorageEngine, StorageError, StorageResult,
    StorageStats, WriteBatch, WriteOp, l2_span, prefixed,
};

/// The revision that records a quarantine in the quarantined engine: [`WHOLE`], or [`NARROWED`] to the ranges listed
/// under [`LISTED`].
const QUARANTINED: &str = "health:quarantined";
const WHOLE: u64 = 1;
const NARROWED: u64 = 2;
/// `health:quarantined:{start:016x}-{end:016x}`, one per range holding discarded damage.
const LISTED: &str = "health:quarantined:";

/// `kv.md` §16: `Unhealthy` data cannot be trusted; `Rebuilding` is being restored and validated. Neither is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
pub enum State {
    Healthy,
    Unhealthy,
    Rebuilding,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct ErrorLabels {
    dataset: String,
    kind: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct StateLabels {
    dataset: String,
    state: State,
}

/// What the manager knows about one source, for introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub state: State,
    /// Storage failures reported since startup.
    pub errors: u64,
    /// Rebuilds started.
    pub recoveries: u32,
    /// The latest failure: engine, instance, file and partition when the adapter knows them.
    pub last: Option<StorageError>,
    /// The L2 hash ranges still quarantined after narrowing, in order; empty means the whole instance.
    pub ranges: Vec<Range>,
}

/// One dataset's storage in one cache layer, e.g. its whole engine instance.
struct Source {
    dataset: String,
    healthy: AtomicBool,
    rebuilding: AtomicBool,
    consecutive: AtomicU32,
    report: Mutex<Report>,
    /// The keys of [`Report::ranges`], read on every call while the source is not healthy.
    refused: RwLock<Vec<KeyRange>>,
}

/// The process's storage-health manager.
pub struct StorageHealth {
    sources: Mutex<BTreeMap<(String, &'static str), Arc<Source>>>,
    errors: Family<ErrorLabels, Counter>,
    states: Family<StateLabels, Gauge>,
}

impl StorageHealth {
    pub fn new(admin: &Admin) -> Arc<Self> {
        let errors = Family::default();
        let states = Family::default();
        admin.register(
            "storage_errors",
            "Local storage failures by dataset and kind (io, corruption, unavailable)",
            errors.clone(),
        );
        admin.register(
            "storage_health",
            "Local storage sources by dataset and health state",
            states.clone(),
        );
        Arc::new(StorageHealth {
            sources: Mutex::default(),
            errors,
            states,
        })
    }

    /// `engine` reporting into this manager as `dataset`'s `layer`; once quarantined, it refuses every call.
    pub fn wrap(
        self: &Arc<Self>,
        dataset: &str,
        layer: &'static str,
        engine: Arc<dyn StorageEngine>,
    ) -> Arc<dyn StorageEngine> {
        self.monitored(dataset, layer, engine, false)
    }

    /// Like [`StorageHealth::wrap`], but it also works while the source is `Rebuilding`. Replication applies and
    /// rebuilds write through it; serving reads never do.
    pub fn wrap_replication(
        self: &Arc<Self>,
        dataset: &str,
        layer: &'static str,
        engine: Arc<dyn StorageEngine>,
    ) -> Arc<dyn StorageEngine> {
        self.monitored(dataset, layer, engine, true)
    }

    fn monitored(
        self: &Arc<Self>,
        dataset: &str,
        layer: &'static str,
        engine: Arc<dyn StorageEngine>,
        rebuilds: bool,
    ) -> Arc<dyn StorageEngine> {
        let monitored = Monitored {
            health: self.clone(),
            source: self.source(dataset, layer),
            layer,
            rebuilds,
            inner: engine,
        };
        match monitored.inner.revision(QUARANTINED) {
            Ok(Some(marker)) => {
                // A narrowed record whose ranges cannot be read means the whole instance.
                let ranges = (marker == NARROWED)
                    .then(|| listed(monitored.inner.as_ref()).ok())
                    .flatten()
                    .unwrap_or_default();
                let mut report = monitored.source.report.lock().unwrap();
                if report.state == State::Healthy {
                    tracing::error!(%dataset, ?ranges, "local storage was quarantined before this restart");
                    self.set(&monitored.source, &mut report, State::Unhealthy);
                    self.refuse(&monitored.source, &mut report, ranges);
                }
            }
            Ok(None) => {}
            // This read counts like any other: it may be the one that finds the damage.
            Err(error) => monitored.failed(&error),
        }
        Arc::new(monitored)
    }

    /// Reports a failure of storage that is not a wrapped engine; for one item the caller evicts and refetches, see
    /// [`StorageHealth::report_contained`].
    pub fn report(&self, dataset: &str, layer: &'static str, error: &StorageError) {
        self.failed(&self.source(dataset, layer), error, false);
    }

    /// Reports a failure confined to one item that the caller evicts and refetches, such as one CDN artifact whose hash
    /// mismatched (B-30). Corruption then counts toward [`CONSECUTIVE_FAILURES`] like I/O errors, instead of
    /// quarantining the whole source at once; [`StorageHealth::verified`] resets the count.
    pub fn report_contained(&self, dataset: &str, layer: &'static str, error: &StorageError) {
        self.failed(&self.source(dataset, layer), error, true);
    }

    /// A non-engine source served a verified read, so its earlier failures are no longer consecutive.
    pub fn verified(&self, dataset: &str, layer: &'static str) {
        self.succeeded(&self.source(dataset, layer));
    }

    /// The quarantined ranges of each unhealthy dataset whose unhealthy sources were all narrowed, for the member update.
    /// [`StorageHealth::unhealthy`] datasets missing here are quarantined whole.
    pub fn damaged(&self) -> BTreeMap<String, Vec<Range>> {
        let mut damaged: BTreeMap<String, Option<Vec<Range>>> = BTreeMap::new();
        for source in self.sources.lock().unwrap().values() {
            let report = source.report.lock().unwrap();
            if report.state == State::Healthy {
                continue;
            }
            let entry = damaged
                .entry(source.dataset.clone())
                .or_insert_with(|| Some(Vec::new()));
            match entry {
                Some(ranges) if !report.ranges.is_empty() => ranges.extend(&report.ranges),
                _ => *entry = None,
            }
        }
        damaged
            .into_iter()
            .filter_map(|(dataset, ranges)| Some((dataset, merged(ranges?))))
            .collect()
    }

    /// Datasets with a source that is not healthy, for the member update.
    pub fn unhealthy(&self) -> BTreeSet<String> {
        self.sources
            .lock()
            .unwrap()
            .values()
            .filter(|s| !s.healthy.load(Ordering::Relaxed))
            .map(|s| s.dataset.clone())
            .collect()
    }

    pub fn get(&self, dataset: &str, layer: &'static str) -> Option<Report> {
        let sources = self.sources.lock().unwrap();
        let source = sources.get(&(dataset.to_owned(), layer))?;
        Some(source.report.lock().unwrap().clone())
    }

    /// Every source's state, for the rebuild coordinator.
    pub fn states(&self) -> BTreeMap<(String, &'static str), State> {
        self.sources
            .lock()
            .unwrap()
            .iter()
            .map(|(key, source)| (key.clone(), source.report.lock().unwrap().state))
            .collect()
    }

    /// B-22 starts rebuilding an unhealthy source. Returns whether it was unhealthy.
    pub fn rebuilding(&self, dataset: &str, layer: &'static str) -> bool {
        self.transition(dataset, layer, State::Unhealthy, State::Rebuilding)
    }

    /// A rebuild verified the whole instance and discarded its damage, which lies in `ranges`: only those stay
    /// quarantined, and the rest serve again. The caller recorded them in the engine first. Returns whether the source was
    /// rebuilding the whole instance.
    pub fn narrow(&self, dataset: &str, layer: &'static str, ranges: Vec<Range>) -> bool {
        let source = self.source(dataset, layer);
        let mut report = source.report.lock().unwrap();
        if report.state != State::Rebuilding || !report.ranges.is_empty() || ranges.is_empty() {
            return false;
        }
        tracing::info!(%dataset, layer, ?ranges, "local storage quarantine narrowed");
        self.refuse(&source, &mut report, ranges);
        true
    }

    /// B-22 restored and validated a rebuilding source; it serves again. Returns whether it was rebuilding.
    pub fn recovered(&self, dataset: &str, layer: &'static str) -> bool {
        self.transition(dataset, layer, State::Rebuilding, State::Healthy)
    }

    /// B-22 could not rebuild the source; it is quarantined until the next attempt. Returns whether it was rebuilding.
    pub fn rebuild_failed(&self, dataset: &str, layer: &'static str) -> bool {
        self.transition(dataset, layer, State::Rebuilding, State::Unhealthy)
    }

    fn source(&self, dataset: &str, layer: &'static str) -> Arc<Source> {
        self.sources
            .lock()
            .unwrap()
            .entry((dataset.to_owned(), layer))
            .or_insert_with(|| {
                self.gauge(dataset, State::Healthy).inc();
                Arc::new(Source {
                    dataset: dataset.to_owned(),
                    healthy: AtomicBool::new(true),
                    rebuilding: AtomicBool::new(false),
                    consecutive: AtomicU32::new(0),
                    report: Mutex::new(Report {
                        state: State::Healthy,
                        errors: 0,
                        recoveries: 0,
                        last: None,
                        ranges: Vec::new(),
                    }),
                    refused: RwLock::default(),
                })
            })
            .clone()
    }

    fn transition(&self, dataset: &str, layer: &'static str, from: State, to: State) -> bool {
        let source = self.source(dataset, layer);
        let mut report = source.report.lock().unwrap();
        if report.state != from {
            return false;
        }
        if to == State::Rebuilding {
            report.recoveries += 1;
        }
        source.consecutive.store(0, Ordering::Relaxed);
        self.set(&source, &mut report, to);
        true
    }

    fn succeeded(&self, source: &Source) {
        if source.consecutive.load(Ordering::Relaxed) != 0 {
            source.consecutive.store(0, Ordering::Relaxed);
        }
    }

    /// Returns whether this failure quarantined the source.
    fn failed(&self, source: &Source, error: &StorageError, contained: bool) -> bool {
        let (kind, quarantine) = match error {
            StorageError::Corruption(_) => ("corruption", !contained),
            StorageError::IoError(_) => ("io", false),
            StorageError::Unavailable(_) => ("unavailable", false),
            StorageError::Fenced(_)
            | StorageError::Unsupported(_)
            | StorageError::Overloaded(_) => {
                return false;
            }
        };
        let quarantine = quarantine
            || source.consecutive.fetch_add(1, Ordering::Relaxed) + 1 >= CONSECUTIVE_FAILURES;
        self.errors
            .get_or_create(&ErrorLabels {
                dataset: source.dataset.clone(),
                kind,
            })
            .inc();
        let mut report = source.report.lock().unwrap();
        report.errors += 1;
        report.last = Some(error.clone());
        // A narrowed source is quarantined whole again: the new failure may lie anywhere.
        let newly = quarantine && (report.state != State::Unhealthy || !report.ranges.is_empty());
        if newly {
            tracing::error!(dataset = %source.dataset, %error, "local storage quarantined");
            if report.state != State::Unhealthy {
                self.set(source, &mut report, State::Unhealthy);
            }
            self.refuse(source, &mut report, Vec::new());
        }
        newly
    }

    /// Quarantines `ranges` only, or the whole source when empty.
    fn refuse(&self, source: &Source, report: &mut Report, ranges: Vec<Range>) {
        let ranges = merged(ranges);
        *source.refused.write().unwrap() = ranges.iter().map(|r| l2_span(r.0, r.1)).collect();
        report.ranges = ranges;
    }

    fn set(&self, source: &Source, report: &mut Report, to: State) {
        assert!(
            matches!(
                (report.state, to),
                (State::Healthy, State::Unhealthy)
                    | (State::Unhealthy, State::Rebuilding)
                    | (State::Rebuilding, State::Healthy | State::Unhealthy)
            ),
            "storage health cannot go from {:?} to {to:?}",
            report.state
        );
        self.gauge(&source.dataset, report.state).dec();
        self.gauge(&source.dataset, to).inc();
        report.state = to;
        source
            .healthy
            .store(to == State::Healthy, Ordering::Relaxed);
        source
            .rebuilding
            .store(to == State::Rebuilding, Ordering::Relaxed);
        if to == State::Healthy {
            self.refuse(source, report, Vec::new());
        }
    }

    fn gauge(&self, dataset: &str, state: State) -> Gauge {
        self.states
            .get_or_create(&StateLabels {
                dataset: dataset.to_owned(),
                state,
            })
            .clone()
    }
}

/// Records a quarantine in `engine` itself, so wrapping it quarantines it until a rebuild. Ranges listed by an earlier
/// narrowing stay listed: their damage was discarded, so they still need rebuilding.
pub fn quarantine(engine: &dyn StorageEngine) -> StorageResult<()> {
    let mut mark = WriteBatch::default();
    mark.set_revision(QUARANTINED, WHOLE);
    engine.write(mark)
}

/// Records in `engine` that `ranges` hold discarded damage, while the whole instance stays quarantined; with `narrowed`,
/// that only the listed ranges stay quarantined.
pub fn list(engine: &dyn StorageEngine, ranges: &[Range], narrowed: bool) -> StorageResult<()> {
    let mut mark = WriteBatch::default();
    mark.set_revision(QUARANTINED, if narrowed { NARROWED } else { WHOLE });
    for (start, end) in ranges {
        mark.set_revision(format!("{LISTED}{start:016x}-{end:016x}"), 1);
    }
    engine.write(mark)
}

/// The ranges `engine`'s quarantine record lists; an error if a listing cannot be read as a range.
pub fn listed(engine: &dyn StorageEngine) -> StorageResult<Vec<Range>> {
    let names = engine.snapshot()?.revisions_in(&[prefixed(LISTED)])?;
    names
        .keys()
        .map(|name| {
            let rest = &name[LISTED.len()..];
            let hex = |at: std::ops::Range<usize>| {
                rest.get(at).and_then(|h| u64::from_str_radix(h, 16).ok())
            };
            match (hex(0..16), rest.get(16..17), hex(17..33)) {
                (Some(start), Some("-"), Some(end)) if rest.len() == 33 => Ok((start, end)),
                _ => Err(StorageError::Corruption(ErrorContext {
                    engine: "health",
                    instance: String::new(),
                    file: None,
                    partition: Some(name.clone()),
                    detail: "unreadable quarantine listing".into(),
                })),
            }
        })
        .collect()
}

/// Removes `engine`'s quarantine record, once a rebuild validated everything it covered.
pub fn release(engine: &dyn StorageEngine) -> StorageResult<()> {
    let names = engine.snapshot()?.revisions_in(&[prefixed(LISTED)])?;
    let mut batch = WriteBatch::default();
    batch.delete_revision(QUARANTINED);
    for name in names.into_keys() {
        batch.delete_revision(name);
    }
    engine.write(batch)
}

/// `ranges` sorted, with overlapping and adjacent ones joined.
pub fn merged(mut ranges: Vec<Range>) -> Vec<Range> {
    ranges.sort_unstable();
    let mut out: Vec<Range> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match out.last_mut() {
            // A range ending at 2⁶⁴ covers everything after it.
            Some(last) if last.1 == 0 => {}
            Some(last) if range.0 <= last.1 => {
                if range.1 == 0 || range.1 > last.1 {
                    last.1 = range.1;
                }
            }
            _ => out.push(range),
        }
    }
    out
}

/// Whether `a` and `b` share a key.
fn overlap(a: &KeyRange, b: &KeyRange) -> bool {
    let ends_by = |end: &Option<Vec<u8>>, at: &[u8]| end.as_deref().is_some_and(|e| e <= at);
    a.bounds().is_some()
        && b.bounds().is_some()
        && !ends_by(&a.end, &b.start)
        && !ends_by(&b.end, &a.start)
}

/// A wrapped engine: reports every outcome and refuses calls while its source is quarantined.
struct Monitored {
    health: Arc<StorageHealth>,
    source: Arc<Source>,
    layer: &'static str,
    /// A [`StorageHealth::wrap_replication`] handle, which also works while the source is rebuilding.
    rebuilds: bool,
    inner: Arc<dyn StorageEngine>,
}

impl Monitored {
    /// Refuses a call touching `keys` (`None`: no records, such as a revision read) always while the whole source is
    /// quarantined, and once narrowed only over a quarantined range.
    fn gate(&self, keys: Option<&KeyRange>) -> StorageResult<()> {
        if self.source.healthy.load(Ordering::Relaxed)
            || (self.rebuilds && self.source.rebuilding.load(Ordering::Relaxed))
        {
            return Ok(());
        }
        let refused = self.source.refused.read().unwrap();
        if !refused.is_empty() && !keys.is_some_and(|keys| refused.iter().any(|r| overlap(r, keys)))
        {
            return Ok(());
        }
        Err(StorageError::Unavailable(ErrorContext {
            engine: "health",
            instance: self.source.dataset.clone(),
            file: None,
            partition: None,
            detail: format!("{} storage is quarantined", self.layer),
        }))
    }

    fn call<T>(&self, result: impl FnOnce() -> StorageResult<T>) -> StorageResult<T> {
        self.call_on(None, result)
    }

    fn call_on<T>(
        &self,
        keys: Option<&KeyRange>,
        result: impl FnOnce() -> StorageResult<T>,
    ) -> StorageResult<T> {
        self.gate(keys)?;
        let result = result();
        match &result {
            Ok(_) => self.health.succeeded(&self.source),
            Err(error) => self.failed(error),
        }
        result
    }

    fn failed(&self, error: &StorageError) {
        if self.health.failed(&self.source, error, false)
            && let Err(error) = quarantine(self.inner.as_ref())
        {
            tracing::warn!(%error, "could not record the quarantine; a restart will forget it");
        }
    }

    /// Reports errors met while iterating; entries already returned stay returned.
    fn reporting<'a>(&'a self, scan: Scan<'a>) -> Scan<'a> {
        Box::new(scan.inspect(move |entry| {
            if let Err(error) = entry {
                self.failed(error);
            }
        }))
    }
}

/// The single key `key`.
fn point(key: &[u8]) -> KeyRange {
    let mut end = key.to_vec();
    end.push(0);
    KeyRange::new(key, end)
}

impl StorageEngine for Monitored {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.call_on(Some(&point(key)), || self.inner.get(key))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let scan = self.call_on(Some(range), || self.inner.scan(range))?;
        Ok(self.reporting(scan))
    }

    /// Revisions alone pass: they hold no records, and a quarantined range's are replaced when it rebuilds.
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        for op in &batch.ops {
            match op {
                WriteOp::Put { key, .. } | WriteOp::Delete { key } => {
                    self.gate(Some(&point(key)))?;
                }
                WriteOp::DeleteRange(range) => self.gate(Some(range))?,
            }
        }
        self.call(|| self.inner.write(batch))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.call(|| self.inner.revision(name))
    }

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        let inner = self.call(|| self.inner.snapshot())?;
        Ok(Box::new(MonitoredSnapshot { owner: self, inner }))
    }

    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        self.call_on(Some(&KeyRange::all()), || self.inner.restore(from))
    }

    fn stats(&self) -> StorageResult<StorageStats> {
        self.call(|| self.inner.stats())
    }

    fn disk_usage(&self) -> StorageResult<u64> {
        self.call(|| self.inner.disk_usage())
    }

    fn health(&self) -> StorageResult<()> {
        self.call(|| self.inner.health())
    }

    fn compact(&self, range: &KeyRange) -> StorageResult<()> {
        self.call_on(Some(range), || self.inner.compact(range))
    }

    fn approximate_size(&self, range: &KeyRange) -> StorageResult<u64> {
        self.call_on(Some(range), || self.inner.approximate_size(range))
    }

    /// Not reported: the rebuild verifying a quarantined instance expects to find its damage.
    fn verify(&self) -> StorageResult<()> {
        self.gate(Some(&KeyRange::all()))?;
        self.inner.verify()
    }

    fn damaged(&self, failure: &StorageError) -> Option<KeyRange> {
        self.inner.damaged(failure)
    }

    fn discard(&self, range: &KeyRange) -> StorageResult<()> {
        self.call_on(Some(range), || self.inner.discard(range))
    }
}

struct MonitoredSnapshot<'a> {
    owner: &'a Monitored,
    inner: Box<dyn Snapshot + 'a>,
}

impl Snapshot for MonitoredSnapshot<'_> {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.owner
            .call_on(Some(&point(key)), || self.inner.get(key))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let scan = self.owner.call_on(Some(range), || self.inner.scan(range))?;
        Ok(self.owner.reporting(scan))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.owner.call(|| self.inner.revision(name))
    }

    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>> {
        self.owner.call(|| self.inner.revisions())
    }

    fn revisions_in(
        &self,
        ranges: &[crate::storage::Names],
    ) -> StorageResult<BTreeMap<String, u64>> {
        self.owner.call(|| self.inner.revisions_in(ranges))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/health/storage.rs"]
mod tests;
