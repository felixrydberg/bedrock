//! Real SlateDB materialized namespace storage (#11 / B-11).
//!
//! Original events, resolved values, progress and retry records share one atomic batch.
//! A successful explicit remote flush is the standalone acknowledgement boundary.
//!
//! Open handles retain upstream manifest, compaction and GC timers. Automatic WAL flushing is
//! disabled; closing inactive handles ends their tasks. Removing all idle engine timers requires
//! a scoped upstream change rather than disabling compaction or pretending these tasks are absent.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use slatedb::{CloseReason, Db, ErrorKind, Settings, WriteBatch};

use super::namespace::{Changes, NamespaceDatabase, NamespaceFactory};
use super::{Entry, ErrorContext, KeyRange, StorageError, StorageResult};

pub struct SlateFactory {
    store: Arc<dyn ObjectStore>,
}

impl SlateFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

struct SlateDatabase {
    db: Db,
    instance: String,
}

fn context(instance: &str, detail: String) -> ErrorContext {
    ErrorContext {
        engine: "slatedb",
        instance: instance.into(),
        file: None,
        partition: None,
        detail,
    }
}

fn engine_error(instance: &str, operation: &str, error: slatedb::Error) -> StorageError {
    let kind = error.kind();
    // Public classifications retain the actionable cause without leaking backend URLs or secrets.
    let context = context(instance, format!("{operation}: {kind:?}"));
    match kind {
        ErrorKind::Closed(CloseReason::Fenced) => StorageError::Fenced(context),
        ErrorKind::Closed(_) => StorageError::Unavailable(ErrorContext {
            detail: format!("{operation}: engine closed; restart writer to recover ({kind:?})"),
            ..context
        }),
        ErrorKind::Data => StorageError::Corruption(context),
        ErrorKind::Unavailable => StorageError::Unavailable(context),
        _ => StorageError::IoError(context),
    }
}

fn valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity != "."
        && identity != ".."
        && !identity.contains(['/', '\\', '\0'])
}

#[async_trait]
impl NamespaceFactory for SlateFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        let mut scopes = BTreeSet::new();
        let listing_error =
            |_| StorageError::Unavailable(context("namespace", "namespace listing failed".into()));
        let mut datasets = self
            .store
            .list_with_delimiter(None)
            .await
            .map_err(listing_error)?
            .common_prefixes;
        datasets.sort();
        for dataset in datasets {
            let mut namespaces = self
                .store
                .list_with_delimiter(Some(&dataset))
                .await
                .map_err(listing_error)?
                .common_prefixes;
            namespaces.sort();
            for namespace in namespaces {
                let children = self
                    .store
                    .list_with_delimiter(Some(&namespace))
                    .await
                    .map_err(listing_error)?;
                if !children
                    .common_prefixes
                    .contains(&namespace.clone().join("state"))
                {
                    continue;
                }
                let dataset_name = dataset.as_ref();
                let namespace_name = namespace
                    .as_ref()
                    .strip_prefix(dataset_name)
                    .and_then(|suffix| suffix.strip_prefix('/'))
                    .unwrap_or_default();
                if !valid_identity(dataset_name) || !valid_identity(namespace_name) {
                    return Err(StorageError::Corruption(context(
                        "namespace",
                        "invalid stored namespace identity".into(),
                    )));
                }
                scopes.insert((dataset_name.to_owned(), namespace_name.to_owned()));
            }
        }
        Ok(scopes.into_iter().collect())
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(StorageError::IoError(context(
                "namespace",
                "dataset and namespace must be nonempty single path components".into(),
            )));
        }
        let instance: String = format!("{dataset}/{namespace}");
        let settings = Settings {
            flush_interval: None,
            object_store_max_retries: Some(0),
            ..Settings::default()
        };
        let db = Db::builder(format!("{instance}/state"), self.store.clone())
            .with_settings(settings)
            .build()
            .await
            .map_err(|error| engine_error(&instance, "open", error))?;
        Ok(Arc::new(SlateDatabase { db, instance }))
    }
}

#[async_trait]
impl NamespaceDatabase for SlateDatabase {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.db
            .get(key)
            .await
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|error| engine_error(&self.instance, "get", error))
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        let mut iter = self
            .db
            .scan(range.bounds().expect("prefix range is never empty"))
            .await
            .map_err(|error| engine_error(&self.instance, "scan", error))?;
        let mut entries = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|error| engine_error(&self.instance, "scan next", error))?
        {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(entries)
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        if changes.is_empty() {
            return Ok(());
        }
        // SlateDB's batch builder panics on invalid lengths; reject before constructing any rows.
        if changes.iter().any(|(key, value)| {
            key.is_empty()
                || u32::try_from(key.len()).is_err()
                || value
                    .as_ref()
                    .is_some_and(|value| u32::try_from(value.len()).is_err())
        }) {
            return Err(StorageError::Unsupported(context(
                &self.instance,
                "SlateDB keys must be nonempty and keys/values must fit u32 lengths".into(),
            )));
        }
        let mut batch = WriteBatch::new();
        for (key, value) in changes {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        self.db
            .write(batch)
            .await
            .map(|_| ())
            .map_err(|error| engine_error(&self.instance, "write", error))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db
            .flush()
            .await
            .map_err(|error| engine_error(&self.instance, "flush", error))
    }

    async fn close(&self) -> StorageResult<()> {
        self.db
            .close()
            .await
            .map_err(|error| engine_error(&self.instance, "close", error))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/storage/slate.rs"]
mod tests;
