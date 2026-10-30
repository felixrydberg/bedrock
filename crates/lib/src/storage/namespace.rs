//! Asynchronous storage for a materialized namespace (#11 / B-11).
//! One atomic batch contains original events, resolved values, retries and progress.
//! Standalone writers acknowledge only after remote flush; future Raft can commit first.

use std::sync::Arc;

use async_trait::async_trait;

use super::{Entry, StorageResult};

pub mod memory;

pub(crate) fn valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity != "."
        && identity != ".."
        && !identity.contains(['/', '\\', '\0'])
}

pub type Changes = Vec<(Vec<u8>, Option<Vec<u8>>)>;

#[async_trait]
pub trait NamespaceDatabase: Send + Sync {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>>;
    /// Atomically buffer the whole batch. The caller serializes mutations and
    /// records values, retry identities and represented revision together.
    async fn write(&self, changes: Changes) -> StorageResult<()>;
    async fn flush(&self) -> StorageResult<()>;
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
}
