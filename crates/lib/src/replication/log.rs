//! Ordered namespace history and deterministic change notifications shared by writers and relays.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tonic::Status;

use super::Event;

/// A source's committed history: the writer's durable log (B-09) or a relay's replay log (B-18).
pub trait Log: Send + Sync + 'static {
    /// Up to `limit` committed events of the namespace from sequence `from`, consecutive and in order; empty when
    /// `from` is past the head.
    fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Event>, Status>> + Send;

    /// The namespace's head sequence (0 before its first commit), updated after every commit.
    fn head(&self, dataset: &str, namespace: &str) -> Arc<Watched<u64>>;
}

/// A value whose changes can be awaited. `tokio::sync::watch` spreads its waiters over shards picked by an RNG the
/// simulation does not seed, so a seed would not replay; this is a lock and a [`Notify`].
pub struct Watched<T> {
    value: Mutex<T>,
    changed: Notify,
}

impl<T: Clone> Watched<T> {
    pub fn new(value: T) -> Self {
        Watched {
            value: Mutex::new(value),
            changed: Notify::new(),
        }
    }

    pub fn get(&self) -> T {
        self.value.lock().unwrap().clone()
    }

    pub fn update(&self, f: impl FnOnce(&mut T)) {
        f(&mut self.value.lock().unwrap());
        self.changed.notify_waiters();
    }

    /// Waits until `ready` holds for the value, and returns it.
    pub async fn wait_until(&self, mut ready: impl FnMut(&T) -> bool) -> T {
        loop {
            let mut notified = std::pin::pin!(self.changed.notified());
            // Registered before the check, so an update right after it still wakes this waiter.
            notified.as_mut().enable();
            {
                let value = self.value.lock().unwrap();
                if ready(&value) {
                    return value.clone();
                }
            }
            notified.await;
        }
    }
}

/// A [`Log`] held in memory, e.g. the writer's recent tail.
#[derive(Default)]
pub struct MemoryLog {
    namespaces: Mutex<BTreeMap<(String, String), NamespaceLog>>,
    retention: Option<(fn() -> u64, u64)>,
}

struct NamespaceLog {
    events: Vec<Event>,
    base_sequence: u64,
    head: Arc<Watched<u64>>,
}

impl NamespaceLog {
    fn new() -> Self {
        NamespaceLog {
            events: Vec::new(),
            base_sequence: 0,
            head: Arc::new(Watched::new(0)),
        }
    }
}

impl MemoryLog {
    /// Enforces the writer's event window on reads, independently of physical GC.
    pub fn with_retention(clock: fn() -> u64, retention_ms: u64) -> Self {
        Self {
            namespaces: Mutex::default(),
            retention: Some((clock, retention_ms)),
        }
    }

    /// Initializes an empty log at an already-installed state boundary.
    pub fn reset(&self, dataset: &str, namespace: &str, boundary: u64) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let log = namespaces
            .entry((dataset.to_owned(), namespace.to_owned()))
            .or_insert_with(NamespaceLog::new);
        if !log.events.is_empty() || log.head.get() != 0 {
            return Err("cannot reset a non-empty log".into());
        }
        log.base_sequence = boundary;
        log.head.update(|head| *head = boundary);
        Ok(())
    }

    /// Drops expired durable events while preserving the namespace head.
    pub fn prune(&self, dataset: &str, namespace: &str, through: u64) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let Some(log) = namespaces.get_mut(&(dataset.to_owned(), namespace.to_owned())) else {
            return if through == 0 {
                Ok(())
            } else {
                Err("unknown namespace log".into())
            };
        };
        let head = log.head.get();
        if through > head {
            return Err(format!("cannot prune through {through} past head {head}"));
        }
        let drop_count = usize::try_from(through.saturating_sub(log.base_sequence))
            .unwrap_or(usize::MAX)
            .min(log.events.len());
        log.events.drain(..drop_count);
        log.base_sequence = log.base_sequence.max(through);
        Ok(())
    }

    /// Commits `event`, which must carry its namespace's next sequence.
    pub fn append(&self, event: Event) -> Result<(), String> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let log = namespaces
            .entry((event.dataset.clone(), event.namespace.clone()))
            .or_insert_with(NamespaceLog::new);
        let next = log
            .head
            .get()
            .checked_add(1)
            .ok_or_else(|| "sequence overflow".to_owned())?;
        if event.sequence != next {
            return Err(format!("expected sequence {next}, got {}", event.sequence));
        }
        log.events.push(event);
        log.head.update(|head| *head = next);
        Ok(())
    }
}

impl Log for MemoryLog {
    async fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>, Status> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let Some(log) = namespaces.get_mut(&(dataset.to_owned(), namespace.to_owned())) else {
            return Ok(Vec::new());
        };
        if let Some((clock, window)) = self.retention {
            let now = clock();
            let expired = log
                .events
                .partition_point(|event| now.saturating_sub(event.committed_at) >= window);
            if expired > 0 {
                log.base_sequence = log.events[expired - 1].sequence;
                log.events.drain(..expired);
            }
        }
        let first = log.base_sequence.saturating_add(1);
        if from.max(1) <= log.base_sequence {
            return Err(Status::out_of_range(format!(
                "sequence {} is outside retained history; bootstrap at or after {}",
                from.max(1),
                log.base_sequence
            )));
        }
        let start = usize::try_from(from.max(first) - first).unwrap_or(usize::MAX);
        let events = log.events.get(start..).unwrap_or_default();
        Ok(events.iter().take(limit).cloned().collect())
    }

    fn head(&self, dataset: &str, namespace: &str) -> Arc<Watched<u64>> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let key = (dataset.to_owned(), namespace.to_owned());
        let log = namespaces.entry(key).or_insert_with(NamespaceLog::new);
        log.head.clone()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replication/log.rs"]
mod tests;
