//! Local storage health (#13 / B-14, `kv.md` §15.1–15.2). Each dataset's engine is [`StorageHealth::wrap`]ped, so every
//! I/O, corruption and unavailability error reaches the process's one manager.
//!
//! - **Quarantine.** `Corruption` marks the engine instance `Unhealthy` at once, as do [`CONSECUTIVE_FAILURES`] I/O or
//!   unavailability errors in a row. The error's partition (hash range or key) is recorded, but the whole instance is
//!   quarantined: no adapter can yet prove damage is limited to it. `Fenced` and `Unsupported` are not storage failures.
//! - **Isolation.** A quarantined instance answers every call with `Unavailable`, never a miss, so callers fall back to
//!   other sources. Other datasets keep running, and readiness ignores quarantined ones. The process announces
//!   [`StorageHealth::unhealthy`] datasets in its member updates, so relays assign it no new ranges and peers skip it as a
//!   source for those datasets (`docs/spec/membership.md`).
//! - **Recovery** is [`crate::reconciliation::rebuild`] (#23 / B-22). A `Rebuilding` source still refuses every
//!   [`StorageHealth::wrap`]ped call, so nothing is served from it, while its [`StorageHealth::wrap_replication`] handle
//!   restores it. Only a validated rebuild makes it [`StorageHealth::recovered`].
//!
//! Processes are per service, so a broken CDN cache disk never marks the same machine's KV node unhealthy.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

use super::CONSECUTIVE_FAILURES;
use crate::storage::{
    ErrorContext, KeyRange, Scan, Snapshot, StorageEngine, StorageError, StorageResult,
    StorageStats, WriteBatch,
};
use crate::transport::admin::Admin;

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
}

/// One dataset's storage in one cache layer, e.g. its whole engine instance.
struct Source {
    dataset: String,
    healthy: AtomicBool,
    rebuilding: AtomicBool,
    consecutive: AtomicU32,
    report: Mutex<Report>,
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
        Arc::new(Monitored {
            health: self.clone(),
            source: self.source(dataset, layer),
            layer,
            rebuilds,
            inner: engine,
        })
    }

    /// Reports a failure of storage that is not a wrapped engine, such as the CDN's `CacheStorage` (B-30).
    pub fn report(&self, dataset: &str, layer: &'static str, error: &StorageError) {
        self.failed(&self.source(dataset, layer), error);
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
                    }),
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

    fn failed(&self, source: &Source, error: &StorageError) {
        let (kind, quarantine) = match error {
            StorageError::Corruption(_) => ("corruption", true),
            StorageError::IoError(_) => ("io", false),
            StorageError::Unavailable(_) => ("unavailable", false),
            StorageError::Fenced(_) | StorageError::Unsupported(_) => return,
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
        if quarantine && report.state != State::Unhealthy {
            tracing::error!(dataset = %source.dataset, %error, "local storage quarantined");
            self.set(source, &mut report, State::Unhealthy);
        }
    }

    fn set(&self, source: &Source, report: &mut Report, to: State) {
        self.gauge(&source.dataset, report.state).dec();
        self.gauge(&source.dataset, to).inc();
        report.state = to;
        source
            .healthy
            .store(to == State::Healthy, Ordering::Relaxed);
        source
            .rebuilding
            .store(to == State::Rebuilding, Ordering::Relaxed);
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
    fn call<T>(&self, result: impl FnOnce() -> StorageResult<T>) -> StorageResult<T> {
        if !self.source.healthy.load(Ordering::Relaxed)
            && !(self.rebuilds && self.source.rebuilding.load(Ordering::Relaxed))
        {
            return Err(StorageError::Unavailable(ErrorContext {
                engine: "health",
                instance: self.source.dataset.clone(),
                file: None,
                partition: None,
                detail: format!("{} storage is quarantined", self.layer),
            }));
        }
        let result = result();
        match &result {
            Ok(_) => self.health.succeeded(&self.source),
            Err(error) => self.health.failed(&self.source, error),
        }
        result
    }

    /// Reports errors met while iterating; entries already returned stay returned.
    fn reporting<'a>(&'a self, scan: Scan<'a>) -> Scan<'a> {
        Box::new(scan.inspect(move |entry| {
            if let Err(error) = entry {
                self.health.failed(&self.source, error);
            }
        }))
    }
}

impl StorageEngine for Monitored {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.call(|| self.inner.get(key))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let scan = self.call(|| self.inner.scan(range))?;
        Ok(self.reporting(scan))
    }

    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
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
        self.call(|| self.inner.restore(from))
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
}

struct MonitoredSnapshot<'a> {
    owner: &'a Monitored,
    inner: Box<dyn Snapshot + 'a>,
}

impl Snapshot for MonitoredSnapshot<'_> {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.owner.call(|| self.inner.get(key))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let scan = self.owner.call(|| self.inner.scan(range))?;
        Ok(self.owner.reporting(scan))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.owner.call(|| self.inner.revision(name))
    }

    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>> {
        self.owner.call(|| self.inner.revisions())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/health/storage.rs"]
mod tests;
