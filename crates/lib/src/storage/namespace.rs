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
    /// Reads of this handle that see only what a flush made durable, never a buffered batch (#58 / B-26). Customer KV
    /// serves reads from its writer's handle, so no separate reader polls the store. The view lives as long as the
    /// handle: once the handle is closed or fenced, its reads fail.
    fn durable(&self) -> Arc<dyn NamespaceReader>;
}

/// Reads of a namespace as its writer made it durable (#58 / B-26).
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
