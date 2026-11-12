//! Asynchronous storage for a materialized namespace (#11 / B-11).
//! One atomic batch contains original events, resolved values, retries and progress.
//! Standalone writers acknowledge only after remote flush; future Raft can commit first.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::{Entry, StorageResult};

pub(crate) fn valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity != "."
        && identity != ".."
        && !identity.contains(['/', '\\', '\0'])
}

pub type Changes = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Sorted reads from one pinned point in time, released when the scan is dropped.
#[async_trait]
pub trait NamespaceScan: Send {
    async fn next(&mut self) -> StorageResult<Option<Entry>>;
}

#[async_trait]
pub trait NamespaceDatabase: Send + Sync {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>>;
    /// Capture a stable prefix scan without materializing its rows. Later writes must
    /// not change its results. Protect the backing files for `lifetime`; callers
    /// must finish sooner. Read errors must propagate to the caller.
    async fn snapshot(
        &self,
        prefix: &[u8],
        lifetime: Duration,
    ) -> StorageResult<Box<dyn NamespaceScan>>;
    /// Atomically buffer the whole batch. The caller serializes mutations and
    /// records values, retry identities and represented revision together.
    async fn write(&self, changes: Changes) -> StorageResult<()>;
    async fn flush(&self) -> StorageResult<()>;
    async fn close(&self) -> StorageResult<()>;
}

/// A read-only handle on a namespace database (#57 / B-39). It never writes namespace data, so opening one never fences
/// the writer. It sees only what the writer made durable, and follows later publications on its own, a little behind.
#[async_trait]
pub trait NamespaceReader: Send + Sync {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    /// Up to `limit` entries under `prefix` after `after` (exclusive), in key order, from one consistent view.
    async fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StorageResult<Vec<Entry>>;
    async fn close(&self) -> StorageResult<()>;
}

#[async_trait]
pub trait NamespaceFactory: Send + Sync {
    /// Existing namespace databases, in deterministic dataset/namespace order.
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>>;
    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>>;

    /// A read-only handle, or `None` while no writer has created the database.
    async fn open_reader(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Option<Arc<dyn NamespaceReader>>> {
        let _ = (dataset, namespace);
        Err(unsupported("read-only namespace handles"))
    }

    /// Deletes every object of the database, and nothing outside `{dataset}/{namespace}/`. Open handles fail afterwards;
    /// the caller retires them first. Deleting an absent database succeeds.
    async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        let _ = (dataset, namespace);
        Err(unsupported("namespace destruction"))
    }
}

fn unsupported(what: &str) -> super::StorageError {
    super::StorageError::Unsupported(super::ErrorContext {
        engine: "namespace",
        instance: "namespace".into(),
        file: None,
        partition: None,
        detail: format!("this factory has no {what}"),
    })
}
