//! Validated rebuilding of quarantined local storage (#23 / B-22, `kv.md` §15.6–15.8).
//!
//! [`Rebuild`] is a [`Reconcile`] user: every registered storage source should be healthy, and each one that is not gets
//! a rebuild, retried with the reconciler's backoff and rate limit. A rebuild moves the source to `Rebuilding`, where it
//! still serves nothing, runs its [`Restore`], and marks it recovered only after the restore validated the result.
//!
//! Recovery takes the smallest scope the failure allows:
//! - **Key.** A missing or stale cached key is refetched by the read itself ([`crate::health::fallback`]).
//! - **Range** (#103 / B-62). Corruption quarantines a whole engine instance (B-14), since its extent is unknown. A
//!   rebuild then verifies the instance ([`StorageEngine::verify`]), has the adapter discard the span each failure names,
//!   and verifies again until it is clean. When every span lies in L2 hash ranges and they are few, only the ranges
//!   overlapping them stay quarantined ([`StorageHealth::narrow`]); each re-bootstraps through its own scope and validates.
//!   The owner stays `ready` in the directory throughout, as a rebuilding owner always has.
//! - **Instance.** Otherwise [`Reinitialise`] recreates the instance empty and, for relay-fed datasets, waits until every
//!   subscription installed a verified bootstrap again: when a failure names no file or key, a span is outside the L2
//!   ranges (a revision, say), the adapter cannot discard it, verification fails, or too much is damaged.
//!
//! Bootstraps come from B-11 materialized state at a checksummed boundary plus the ordered tail, so deletes stay deleted
//! and nothing older replaces newer state, including after retained history expired.
//! - **Disk.** A failed disk is replaced outside the process; the instance then rebuilds as above.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::{Config, Metrics, Reconcile, Reconciler};
use crate::admin::Admin;
use crate::health::storage::{self, State, StorageHealth};
use crate::relays::client::{Rebuilder, Session};
use crate::relays::ranges::Range;
use crate::storage::memory::MemoryEngine;
use crate::storage::{ErrorContext, KeyRange, StorageEngine, StorageError, StorageResult, l2_span};

/// How often the coordinator looks for quarantined sources.
pub const INTERVAL: Duration = Duration::from_secs(1);
/// The most damaged spans one rebuild discards before rebuilding the whole instance instead.
// ponytail: fixed limits; weigh a range rebuild's cost against a whole one's if either rebuilds too much.
const MAX_SPANS: usize = 8;
/// The largest share of the hash space, in 1/2⁶⁴ units, rebuilt range by range.
const MAX_WIDTH: u128 = 1 << 63;

/// `(dataset, layer)`, as [`StorageHealth`] keys its sources.
pub type Source = (String, &'static str);

/// Recreates one source through a [`StorageHealth::wrap_replication`] handle.
#[async_trait::async_trait]
pub trait Restore: Send + Sync {
    /// Verifies the whole source and discards the damage it finds. Returns the L2 hash ranges that held it, recorded in
    /// the source, or `None` when the whole source must be restored.
    async fn narrow(&self) -> StorageResult<Option<Vec<Range>>> {
        Ok(None)
    }

    /// Restores `ranges`, or the whole source when empty. Returns `Ok` only once the restored state passed integrity and
    /// freshness validation.
    async fn restore(&self, ranges: &[Range]) -> StorageResult<()>;
}

pub struct Rebuild {
    health: Arc<StorageHealth>,
    sources: BTreeMap<Source, Arc<dyn Restore>>,
}

impl Rebuild {
    pub fn new(health: Arc<StorageHealth>, sources: BTreeMap<Source, Arc<dyn Restore>>) -> Self {
        Rebuild { health, sources }
    }

    /// Returns once the source stops `Rebuilding`.
    async fn quarantined_again(&self, dataset: &str, layer: &'static str) {
        while self.health.get(dataset, layer).map(|report| report.state) == Some(State::Rebuilding)
        {
            tokio::time::sleep(INTERVAL).await;
        }
    }
}

#[async_trait::async_trait]
impl Reconcile for Rebuild {
    type Key = Source;
    type Desired = ();
    type Actual = State;
    type Repair = ();
    type Error = StorageError;

    async fn desired(&self) -> Result<BTreeMap<Source, ()>, StorageError> {
        Ok(self
            .sources
            .keys()
            .map(|source| (source.clone(), ()))
            .collect())
    }

    async fn actual(&self) -> Result<BTreeMap<Source, State>, StorageError> {
        Ok(self.health.states())
    }

    fn plan(&self, _: &Source, desired: Option<&()>, actual: Option<&State>) -> Option<(u8, ())> {
        // A source left `Rebuilding` by an abandoned attempt (timeout, restart of the pass) is rebuilt again.
        (desired.is_some() && matches!(actual, Some(State::Unhealthy | State::Rebuilding)))
            .then_some((0, ()))
    }

    async fn repair(&self, source: &Source, (): &()) -> Result<(), StorageError> {
        let (dataset, layer) = (source.0.as_str(), source.1);
        self.health.rebuilding(dataset, layer);
        let failed = |detail: &str| {
            StorageError::Unavailable(ErrorContext {
                engine: "rebuild",
                instance: dataset.to_owned(),
                file: None,
                partition: None,
                detail: format!("{layer} {detail}"),
            })
        };
        if self.health.get(dataset, layer).map(|r| r.state) != Some(State::Rebuilding) {
            return Err(failed("is not rebuilding"));
        }
        let restore = &self.sources[source];
        let rebuild = async {
            // Ranges a narrowed quarantine recorded before a restart need no new verification.
            let mut ranges = self
                .health
                .get(dataset, layer)
                .map(|r| r.ranges)
                .unwrap_or_default();
            if ranges.is_empty()
                && let Some(found) = restore.narrow().await?
                && self.health.narrow(dataset, layer, found.clone())
            {
                ranges = found;
            }
            restore.restore(&ranges).await
        };
        let outcome = tokio::select! {
            biased;
            outcome = rebuild => outcome,
            // A new failure quarantines the source, which then refuses the installs a resubscribe waits for, so waiting
            // on could only end at the repair timeout. Start again instead.
            () = self.quarantined_again(dataset, layer) => Err(failed("failed again while rebuilding")),
        };
        match outcome {
            Ok(()) if self.health.recovered(dataset, layer) => {
                tracing::info!(dataset, layer, "local storage rebuilt");
                Ok(())
            }
            Ok(()) => Err(failed("failed again while rebuilding")),
            Err(error) => {
                self.health.rebuild_failed(dataset, layer);
                Err(error)
            }
        }
    }
}

/// Recreates an engine instance empty, or only some of its L2 ranges, then, for a relay-fed dataset, waits until those
/// subscriptions installed verified bootstraps again. Finally every restored record must read back cleanly.
pub struct Reinitialise {
    engine: Arc<dyn StorageEngine>,
    resubscribe: Option<Rebuilder>,
    /// Held by a verification pass until it ends, even after a timed-out repair stopped waiting for it, so a retry
    /// cannot verify, discard or restore the same engine alongside it.
    busy: Arc<tokio::sync::Mutex<()>>,
}

impl Reinitialise {
    pub fn new(engine: Arc<dyn StorageEngine>, resubscribe: Option<Rebuilder>) -> Self {
        Reinitialise {
            engine,
            resubscribe,
            busy: Arc::default(),
        }
    }
}

#[async_trait::async_trait]
impl Restore for Reinitialise {
    async fn narrow(&self) -> StorageResult<Option<Vec<Range>>> {
        let busy = self.busy.clone().lock_owned().await;
        let (engine, rebuilder) = (self.engine.clone(), self.resubscribe.clone());
        let pass = move || {
            let _busy = busy;
            verify(engine.as_ref(), &|ranges: &[Range]| match &rebuilder {
                Some(rebuilder) => rebuilder.widen(engine.as_ref(), ranges),
                None => Ok(ranges.to_vec()),
            })
        };
        #[cfg(not(madsim))]
        {
            tokio::task::spawn_blocking(pass)
                .await
                .expect("verification does not panic")
        }
        #[cfg(madsim)]
        pass()
    }

    async fn restore(&self, ranges: &[Range]) -> StorageResult<()> {
        let _busy = self.busy.lock().await;
        if ranges.is_empty() {
            let empty = MemoryEngine::new("empty");
            self.engine.restore(&*empty.snapshot()?)?;
        } else {
            // Again, for a quarantine narrowed before a restart: the discard may not have finished.
            for range in ranges {
                self.engine.discard(&l2_span(range.0, range.1))?;
            }
        }
        if let Some(rebuilder) = &self.resubscribe {
            if !ranges.is_empty() {
                rebuilder.forget(self.engine.as_ref(), ranges)?;
            }
            rebuilder.rebuild(ranges).await;
        }
        // ponytail: a full scan; verify only the subscribed prefixes if instances grow too large for it.
        let restored = match ranges {
            [] => vec![KeyRange::all()],
            ranges => ranges.iter().map(|r| l2_span(r.0, r.1)).collect(),
        };
        for range in &restored {
            for entry in self.engine.scan(range)? {
                entry?;
            }
        }
        self.engine.health()?;
        if !ranges.is_empty() {
            storage::release(self.engine.as_ref())?;
        }
        Ok(())
    }
}

/// Verifies `engine` and discards each damaged span it finds, until it is clean. Returns the L2 hash ranges holding them,
/// `widen`ed to the subscriptions' ranges around them and listed with the quarantine, or `None` to rebuild the whole
/// instance. Each span is listed before it is discarded, so a restart before the narrowing still rebuilds it; the whole
/// instance stays quarantined until the last listing.
fn verify(
    engine: &dyn StorageEngine,
    widen: &dyn Fn(&[Range]) -> StorageResult<Vec<Range>>,
) -> StorageResult<Option<Vec<Range>>> {
    let Ok(mut ranges) = storage::listed(engine) else {
        return Ok(None);
    };
    let mut discarded = Vec::new();
    loop {
        let failure = match engine.verify() {
            Ok(()) => break,
            Err(failure @ StorageError::Corruption(_)) => failure,
            // Verification itself failed, so the extent is unknown.
            Err(_) => return Ok(None),
        };
        let Some(range) = engine.damaged(&failure).as_ref().and_then(hashes) else {
            return Ok(None);
        };
        // The adapter could not make the damage leave, or there is too much of it.
        if discarded.contains(&range) || discarded.len() == MAX_SPANS {
            return Ok(None);
        }
        storage::list(engine, &[range], false)?;
        match engine.discard(&l2_span(range.0, range.1)) {
            Ok(()) => {}
            Err(StorageError::Unsupported(_)) => return Ok(None),
            Err(error) => return Err(error),
        }
        discarded.push(range);
        ranges.push(range);
    }
    // Merged, so a damaged span inside a held range counts once towards the width.
    let Ok(ranges) = widen(&ranges).map(storage::merged) else {
        return Ok(None);
    };
    let width: u128 = ranges
        .iter()
        .map(|&(start, end)| (if end == 0 { 1 << 64 } else { u128::from(end) }) - u128::from(start))
        .sum();
    // A failure that named nothing to discard leaves the extent unknown.
    if ranges.is_empty() || width > MAX_WIDTH {
        return Ok(None);
    }
    storage::list(engine, &ranges, true)?;
    Ok(Some(ranges))
}

/// The L2 hash range holding every key of `range`, or `None` if it reaches outside the L2 keys.
fn hashes(range: &KeyRange) -> Option<Range> {
    let hash = |key: &[u8]| -> Option<u64> {
        Some(u64::from_be_bytes(
            key.strip_prefix(b"l2/")?.get(..8)?.try_into().ok()?,
        ))
    };
    let start = hash(&range.start)?;
    // The last key lies before the exclusive end, so the end's hash is the last one the range can hold.
    let last = hash(range.end.as_deref()?)?;
    (start <= last).then_some((start, last.wrapping_add(1)))
}

/// Rebuilds a binary's quarantined dataset engines in the background: each of `engines` (its
/// [`StorageHealth::wrap_replication`] handle, wrapped as `layer`) is reinitialised and re-bootstrapped through `session`.
/// `seed` seeds retry jitter (B-23a).
pub fn start(
    admin: &Admin,
    health: &Arc<StorageHealth>,
    session: &Session,
    layer: &'static str,
    engines: &BTreeMap<&'static str, Arc<dyn StorageEngine>>,
    seed: u64,
) -> tokio::task::JoinHandle<()> {
    let sources = engines
        .iter()
        .map(|(dataset, engine)| {
            let restore: Arc<dyn Restore> = Arc::new(Reinitialise::new(
                engine.clone(),
                Some(session.rebuilder(dataset)),
            ));
            (((*dataset).to_owned(), layer), restore)
        })
        .collect();
    let config = Config {
        burst: 1,
        refill: Duration::from_secs(1),
        // A re-bootstrap of a large `full_replica` dataset can take a while.
        repair_timeout: Duration::from_secs(600),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(60),
        seed,
    };
    let reconciler = Reconciler::new(
        "rebuild",
        Arc::new(Rebuild::new(health.clone(), sources)),
        config,
        &Metrics::new(admin),
    );
    tokio::spawn(reconciler.run(INTERVAL))
}

#[cfg(test)]
#[path = "../../tests/unit/reconciliation/rebuild.rs"]
mod tests;
