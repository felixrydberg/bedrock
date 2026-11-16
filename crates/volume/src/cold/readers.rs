//! Read-only handles on range databases (#73's residency, moved here by #113 / S-05 and keyed by range database).
//!
//! A handle is opened on demand, at most [`Readers::new`]'s `max_open` stay open, the least recently used goes first,
//! and one unused for [`IDLE`] is closed by [`Readers::sweep`]. Concurrent opens of one database share one open, and
//! opens are bounded per caller cell. Closing a handle never touches database data.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bedrock_lib::health::protection::{Coalescer, Limiter, Metrics, NAMESPACE_OPENS};
use bedrock_lib::storage::{StorageError, StorageResult};
use tokio::time::Instant;

use super::db::{Database, Databases};

/// A handle unused this long is closed; the next read reopens it.
pub const IDLE: Duration = Duration::from_secs(15 * 60);

type Opened = (String, StorageResult<Option<Arc<dyn Database>>>);
/// An open handle and when a read last used it.
type Open = (Arc<dyn Database>, Instant);

pub struct Readers {
    databases: Arc<dyn Databases>,
    max_open: usize,
    open: Mutex<BTreeMap<String, Open>>,
    /// Handles taken out of service; each closes once no read still holds it.
    retired: Mutex<Vec<Arc<dyn Database>>>,
    opens: Coalescer<String, Opened>,
    limiter: Limiter,
}

impl Readers {
    pub fn new(databases: Arc<dyn Databases>, max_open: usize, metrics: &Metrics) -> Self {
        Readers {
            databases,
            max_open: max_open.max(1),
            open: Mutex::default(),
            retired: Mutex::default(),
            opens: Coalescer::new("volume_open", metrics),
            limiter: Limiter::new("volume_open", NAMESPACE_OPENS, metrics),
        }
    }

    /// Open handles, for tests and introspection.
    pub fn len(&self) -> usize {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `name`'s handle, opening it for a caller in `cell` if needed; `None` while no writer has created the database.
    pub async fn get(&self, cell: &str, name: &str) -> StorageResult<Option<Arc<dyn Database>>> {
        if let Some((db, used)) = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(name)
        {
            *used = bedrock_lib::monotonic_now();
            return Ok(Some(db.clone()));
        }
        let accept = |(leader, opened): &Opened| {
            leader == cell || !matches!(opened, Err(StorageError::Overloaded(_)))
        };
        let open = || async {
            let opened = match self.limiter.acquire(cell).await {
                Ok(_permit) => self.databases.open_reader(name).await,
                Err(overloaded) => Err(overloaded),
            };
            (cell.to_owned(), opened)
        };
        let Some(db) = self.opens.run(name.to_owned(), accept, open).await.1? else {
            // Not kept: the database may be created any moment.
            return Ok(None);
        };
        let (db, evicted) = self.install(name, db);
        if evicted {
            self.release().await;
        }
        Ok(Some(db))
    }

    /// Keeps `db` for `name` unless another handle already is, retiring the least recently used beyond `max_open`.
    pub(crate) fn install(&self, name: &str, db: Arc<dyn Database>) -> (Arc<dyn Database>, bool) {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let mut retired = self.retired.lock().unwrap_or_else(PoisonError::into_inner);
        // A coalesced caller that finished after its handle was retired reads through it once more, but never
        // reinstalls it: it closes once no read holds it.
        if retired.iter().any(|r| Arc::ptr_eq(r, &db)) {
            return (db, false);
        }
        let now = bedrock_lib::monotonic_now();
        let installed = open
            .entry(name.to_owned())
            .or_insert_with(|| (db.clone(), now));
        installed.1 = now;
        let installed = installed.0.clone();
        // Coalesced callers share one handle; a second open that lost the install is retired once.
        if !Arc::ptr_eq(&installed, &db) && !retired.iter().any(|r| Arc::ptr_eq(r, &db)) {
            retired.push(db);
        }
        let mut evicted = false;
        // ponytail: least recently used by a scan over at most `max_open` handles; an ordered index if that grows.
        while open.len() > self.max_open {
            evicted = true;
            let oldest = open
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(name, _)| name.clone())
                .expect("over the bound means not empty");
            retired.push(open.remove(&oldest).expect("just found").0);
        }
        (installed, evicted)
    }

    /// Takes `name`'s handle out of service after a read through it failed, so the next read opens a fresh one.
    pub fn discard(&self, name: &str, db: &Arc<dyn Database>) {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if open
            .get(name)
            .is_some_and(|(held, _)| Arc::ptr_eq(held, db))
        {
            let (removed, _) = open.remove(name).expect("just checked");
            self.retired
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(removed);
        }
    }

    /// Retires handles idle for [`IDLE`] and closes every retired handle no read still holds. Runs on a node-wide timer.
    pub async fn sweep(&self) {
        let now = bedrock_lib::monotonic_now();
        {
            let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
            let idle: Vec<String> = open
                .iter()
                .filter(|(_, (_, used))| now.duration_since(*used) >= IDLE)
                .map(|(name, _)| name.clone())
                .collect();
            let mut retired = self.retired.lock().unwrap_or_else(PoisonError::into_inner);
            for name in idle {
                retired.push(open.remove(&name).expect("just listed").0);
            }
        }
        self.release().await;
    }

    async fn release(&self) {
        let closable: Vec<_> = {
            let mut retired = self.retired.lock().unwrap_or_else(PoisonError::into_inner);
            let (closable, held) = std::mem::take(&mut *retired)
                .into_iter()
                .partition(|db| Arc::strong_count(db) == 1);
            *retired = held;
            closable
        };
        for db in closable {
            if let Err(error) = db.close().await {
                tracing::debug!(%error, "closing a range database reader failed");
            }
        }
    }
}
