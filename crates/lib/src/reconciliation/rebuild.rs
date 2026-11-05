//! Validated rebuilding of quarantined local storage (#23 / B-22, `kv.md` §15.6–15.8).
//!
//! [`Rebuild`] is a [`Reconcile`] user: every registered storage source should be healthy, and each one that is not gets
//! a rebuild, retried with the reconciler's backoff and rate limit. A rebuild moves the source to `Rebuilding`, where it
//! still serves nothing, runs its [`Restore`], and marks it recovered only after the restore validated the result.
//!
//! Recovery takes the smallest scope the failure allows:
//! - **Key.** A missing or stale cached key is refetched by the read itself ([`crate::health::fallback`]).
//! - **Range.** Rebuilding one L2 hash range needs the relay handoff's `populating` → `ready` readiness (#22 / B-21).
//! - **Instance.** Corruption quarantines a whole engine instance (B-14), so [`Reinitialise`] recreates it empty and, for
//!   relay-fed datasets, waits until every subscription installed a verified bootstrap again. Bootstraps come from B-11
//!   materialized state at a checksummed boundary plus the ordered tail, so deletes stay deleted and nothing older
//!   replaces newer state, including after retained history expired.
//! - **Disk.** A failed disk is replaced outside the process; the instance then rebuilds as above.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::{Config, Metrics, Reconcile, Reconciler};
use crate::health::storage::{State, StorageHealth};
use crate::relays::client::{Rebuilder, Session};
use crate::storage::memory::MemoryEngine;
use crate::storage::{ErrorContext, KeyRange, StorageEngine, StorageError, StorageResult};
use crate::transport::admin::Admin;

/// How often the coordinator looks for quarantined sources.
pub const INTERVAL: Duration = Duration::from_secs(1);

/// `(dataset, layer)`, as [`StorageHealth`] keys its sources.
pub type Source = (String, &'static str);

/// Recreates one source through a [`StorageHealth::wrap_replication`] handle.
#[async_trait::async_trait]
pub trait Restore: Send + Sync {
    /// Returns `Ok` only once the restored state passed integrity and freshness validation.
    async fn restore(&self) -> StorageResult<()>;
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
        let outcome = tokio::select! {
            outcome = self.sources[source].restore() => outcome,
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

/// Recreates an engine instance empty, then, for a relay-fed dataset, waits until its subscriptions installed verified
/// bootstraps again. Finally every record must read back cleanly.
pub struct Reinitialise {
    pub engine: Arc<dyn StorageEngine>,
    pub resubscribe: Option<Rebuilder>,
}

#[async_trait::async_trait]
impl Restore for Reinitialise {
    async fn restore(&self) -> StorageResult<()> {
        let empty = MemoryEngine::new("empty");
        self.engine.restore(&*empty.snapshot()?)?;
        if let Some(rebuilder) = &self.resubscribe {
            rebuilder.rebuild().await;
        }
        // ponytail: a full scan; verify only the subscribed prefixes if instances grow too large for it.
        for entry in self.engine.scan(&KeyRange::all())? {
            entry?;
        }
        self.engine.health()
    }
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
            let restore: Arc<dyn Restore> = Arc::new(Reinitialise {
                engine: engine.clone(),
                resubscribe: Some(session.rebuilder(dataset)),
            });
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
