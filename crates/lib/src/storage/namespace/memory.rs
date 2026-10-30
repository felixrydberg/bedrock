//! Deterministic namespace-storage model with separate buffered and durable state (#11 / B-11).
//! Opening a new writer or crashing invalidates old handles; only explicit flush/clean close persists.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{Changes, NamespaceDatabase, NamespaceFactory};
use crate::storage::{Entry, ErrorContext, KeyRange, StorageError, StorageResult};

type Identity = (String, String);
type Entries = BTreeMap<Vec<u8>, Vec<u8>>;
type State = BTreeMap<Identity, DurableNamespace>;

#[derive(Default)]
struct DurableNamespace {
    entries: Entries,
    // Identity is used only for equality, never message ordering or output.
    generation: Arc<()>,
}

/// Clones share durable namespace state, including across modeled process crashes.
#[derive(Clone, Default)]
pub struct MemoryFactory {
    state: Arc<Mutex<State>>,
}

struct View {
    entries: Entries,
    closed: bool,
}

struct MemoryDatabase {
    state: Arc<Mutex<State>>,
    identity: Identity,
    instance: String,
    generation: Arc<()>,
    view: Mutex<View>,
}

fn context(instance: &str, detail: &str) -> ErrorContext {
    ErrorContext {
        engine: "namespace-memory",
        instance: instance.into(),
        file: None,
        partition: None,
        detail: detail.into(),
    }
}

fn unavailable(instance: &str, detail: &str) -> StorageError {
    StorageError::Unavailable(context(instance, detail))
}

fn valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity != "."
        && identity != ".."
        && !identity.contains(['/', '\\', '\0'])
}

impl MemoryFactory {
    /// Discards every handle's unflushed state without changing any durable namespace.
    pub fn crash(&self) -> StorageResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?;
        for namespace in state.values_mut() {
            namespace.generation = Arc::new(());
        }
        Ok(())
    }
}

impl MemoryDatabase {
    fn with_active<T>(
        &self,
        operation: impl FnOnce(&mut DurableNamespace, &mut View) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable(&self.instance, "state lock poisoned"))?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| unavailable(&self.instance, "view lock poisoned"))?;
        let namespace = state
            .get_mut(&self.identity)
            .ok_or_else(|| unavailable(&self.instance, "namespace is unavailable"))?;
        if !Arc::ptr_eq(&namespace.generation, &self.generation) {
            return Err(StorageError::Fenced(context(
                &self.instance,
                "newer writer or crash",
            )));
        }
        if view.closed {
            return Err(unavailable(&self.instance, "database closed"));
        }
        operation(namespace, &mut view)
    }
}

#[async_trait]
impl NamespaceFactory for MemoryFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(StorageError::IoError(context(
                "namespace",
                "dataset and namespace must be nonempty single path components",
            )));
        }
        let identity = (dataset.to_owned(), namespace.to_owned());
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?;
        let durable = state.entry(identity.clone()).or_default();
        durable.generation = Arc::new(());
        Ok(Arc::new(MemoryDatabase {
            state: self.state.clone(),
            identity,
            instance: format!("{dataset}/{namespace}"),
            generation: durable.generation.clone(),
            view: Mutex::new(View {
                entries: durable.entries.clone(),
                closed: false,
            }),
        }))
    }
}

#[async_trait]
impl NamespaceDatabase for MemoryDatabase {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.with_active(|_, view| Ok(view.entries.get(key).cloned()))
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        self.with_active(|_, view| {
            Ok(view
                .entries
                .range::<[u8], _>(range.bounds().expect("prefix range is never empty"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        })
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        self.with_active(|_, view| {
            if changes.iter().any(|(key, value)| {
                key.is_empty()
                    || u32::try_from(key.len()).is_err()
                    || value
                        .as_ref()
                        .is_some_and(|value| u32::try_from(value.len()).is_err())
            }) {
                return Err(StorageError::Unsupported(context(
                    &self.instance,
                    "SlateDB keys must be nonempty and keys/values must fit u32 lengths",
                )));
            }
            for (key, value) in changes {
                match value {
                    Some(value) => {
                        view.entries.insert(key, value);
                    }
                    None => {
                        view.entries.remove(&key);
                    }
                }
            }
            Ok(())
        })
    }

    async fn flush(&self) -> StorageResult<()> {
        self.with_active(|namespace, view| {
            namespace.entries = view.entries.clone();
            Ok(())
        })
    }

    async fn close(&self) -> StorageResult<()> {
        self.with_active(|namespace, view| {
            namespace.entries = view.entries.clone();
            view.closed = true;
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/storage/namespace_memory.rs"]
mod tests;
