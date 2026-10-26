//! The single-node commit path behind `bedrock-api` (B-09, `kv.md` §8, §12, §14).
//!
//! The writer assigns each namespace's sequences in memory, in order and without gaps, and commits them in batches:
//! one write-once object per batch at `{dataset}/{namespace}/log/{first:020}`, acknowledged once the put succeeds.
//! Writes that arrive while a namespace's batch is in flight wait and go out together in the next one. One batch per
//! namespace is in flight at a time, so a failed batch never leaves a gap behind a later one.
//!
//! Committed events go to [`Writer::log`], which [`super::subscriptions::Source`] serves to subscribers (B-10). On start
//! the writer reads every batch back from the bucket, so acknowledged writes, heads and idempotency keys survive a
//! restart.
//!
//! Create-if-absent guards the sequences: a second instance on the same bucket assigns sequences that already exist, and
//! its put fails instead of overwriting. That namespace then refuses writes on the second instance until it restarts.
//! B-09b replaces this writer with a Raft quorum behind the same `commit` and [`Log`] interface.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use prost::Message;
use tokio::sync::oneshot;

use super::subscriptions::{MemoryLog, pb};
use super::{Event, Operation, stream};
use crate::s3::{ObjectStore, Put, S3Error, S3Result};

/// Writer epoch on every event. Fixed until B-09b fences writers by epoch.
pub const EPOCH: u64 = 1;
/// Writes that may wait per namespace for the next batch; more are refused as [`CommitError::Busy`] (backpressure).
pub const MAX_QUEUED: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub dataset: String,
    pub namespace: String,
    pub key: Vec<u8>,
    pub operation: Operation,
    /// A retry with the same key, in the same namespace, returns the first commit's sequence and applies nothing.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// The write is malformed. Nothing was committed.
    Invalid(String),
    /// [`MAX_QUEUED`] writes already wait in the namespace. Nothing was committed.
    Busy,
    /// Another writer committed to the namespace's log. This one takes no writes there until it restarts.
    Fenced,
    /// The batch's put failed and may or may not have landed. Retry with the same idempotency key.
    Unavailable(String),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::Invalid(reason) => write!(f, "invalid write: {reason}"),
            CommitError::Busy => f.write_str("too many writes waiting in this namespace"),
            CommitError::Fenced => f.write_str("another writer committed to this namespace"),
            CommitError::Unavailable(detail) => write!(f, "commit outcome unknown: {detail}"),
        }
    }
}

impl std::error::Error for CommitError {}

pub struct Writer<S> {
    store: S,
    /// Milliseconds since the Unix epoch, for `committed_at`. Wall time comes from the binary (B-23a).
    clock: fn() -> u64,
    log: Arc<MemoryLog>,
    namespaces: Mutex<BTreeMap<(String, String), Arc<Namespace>>>,
}

#[derive(Default)]
struct Namespace {
    queue: Mutex<Vec<Queued>>,
    /// Held while a batch commits.
    state: tokio::sync::Mutex<State>,
}

struct Queued {
    write: Write,
    done: oneshot::Sender<Result<u64, CommitError>>,
}

#[derive(Default)]
struct State {
    head: u64,
    // ponytail: every key since the first write, like the log itself; B-11 bounds both to the tail after a snapshot.
    idempotency: BTreeMap<String, u64>,
    /// A batch whose put failed with an unknown outcome. It is put again, unchanged, before the next one.
    unresolved: Option<Batch>,
    fenced: bool,
}

struct Batch {
    object: String,
    body: Vec<u8>,
    entries: Vec<(Event, Option<String>)>,
}

impl Batch {
    fn new(entries: Vec<(Event, Option<String>)>) -> Self {
        let first = &entries[0].0;
        let object = log_object(&first.dataset, &first.namespace, first.sequence);
        let entries_pb = entries.iter().map(|(event, key)| pb::LogEntry {
            event: Some(event.clone().into()),
            idempotency_key: key.clone().unwrap_or_default(),
        });
        let body = pb::LogBatch {
            entries: entries_pb.collect(),
        };
        Batch {
            object,
            body: body.encode_to_vec(),
            entries,
        }
    }
}

impl<S: ObjectStore + 'static> Writer<S> {
    /// Loads every committed batch from `store`. Fails on a batch that does not continue its namespace's log.
    // ponytail: reads the whole log at start; B-11 starts from the latest manifest instead.
    pub async fn open(store: S, clock: fn() -> u64) -> S3Result<Arc<Self>> {
        let writer = Writer {
            store,
            clock,
            log: Arc::default(),
            namespaces: Mutex::default(),
        };
        for object in writer.store.list("").await? {
            let Some((dataset, namespace, first)) = parse_log_object(&object) else {
                continue;
            };
            let corrupt = |detail: String| S3Error::Corruption {
                key: object.clone(),
                detail,
            };
            let body = writer
                .store
                .get(&object)
                .await?
                .ok_or_else(|| S3Error::Unavailable {
                    detail: format!("{object} was deleted while loading"),
                })?;
            let batch =
                pb::LogBatch::decode(body.as_slice()).map_err(|e| corrupt(e.to_string()))?;
            let ns = writer.namespace(dataset, namespace);
            let mut state = ns.state.lock().await;
            let mut entries = Vec::new();
            for (i, entry) in batch.entries.into_iter().enumerate() {
                let event = Event::from(
                    entry
                        .event
                        .ok_or_else(|| corrupt("entry without event".into()))?,
                );
                let expected = state.head + 1 + i as u64;
                if event.dataset != dataset
                    || event.namespace != namespace
                    || event.sequence != expected
                {
                    return Err(corrupt(format!(
                        "expected {dataset}/{namespace} sequence {expected}, found {}/{} sequence {}",
                        event.dataset, event.namespace, event.sequence
                    )));
                }
                let key = Some(entry.idempotency_key).filter(|k| !k.is_empty());
                entries.push((event, key));
            }
            if first != state.head + 1 || entries.is_empty() {
                return Err(corrupt(format!(
                    "batch does not follow head {}",
                    state.head
                )));
            }
            writer.finish(&mut state, entries);
        }
        Ok(Arc::new(writer))
    }

    /// Every committed event, for [`super::subscriptions::Source`].
    pub fn log(&self) -> Arc<MemoryLog> {
        self.log.clone()
    }

    /// Commits `write` with the next sequence in its namespace, or returns the earlier sequence of a retry.
    pub async fn commit(self: &Arc<Self>, write: Write) -> Result<u64, CommitError> {
        validate(&write)?;
        let namespace = self.namespace(&write.dataset, &write.namespace);
        let (done, committed) = oneshot::channel();
        {
            let mut queue = namespace.queue.lock().unwrap();
            if queue.len() >= MAX_QUEUED {
                return Err(CommitError::Busy);
            }
            queue.push(Queued { write, done });
        }
        // Spawned, so a caller that gives up cannot stop a batch between its put and the log.
        tokio::spawn(self.clone().drain(namespace));
        committed
            .await
            .unwrap_or_else(|_| Err(CommitError::Unavailable("writer stopped".into())))
    }

    fn namespace(&self, dataset: &str, namespace: &str) -> Arc<Namespace> {
        let mut namespaces = self.namespaces.lock().unwrap();
        let key = (dataset.to_owned(), namespace.to_owned());
        namespaces.entry(key).or_default().clone()
    }

    /// Commits whatever is queued in the namespace as one batch, once the batch in flight is done.
    async fn drain(self: Arc<Self>, namespace: Arc<Namespace>) {
        let mut state = namespace.state.lock().await;
        let queued = std::mem::take(&mut *namespace.queue.lock().unwrap());
        if queued.is_empty() {
            return;
        }
        if let Some(batch) = state.unresolved.take()
            && let Err(error) = self.put(&mut state, batch).await
        {
            for Queued { done, .. } in queued {
                let _ = done.send(Err(error.clone()));
            }
            return;
        }
        if state.fenced {
            for Queued { done, .. } in queued {
                let _ = done.send(Err(CommitError::Fenced));
            }
            return;
        }

        let committed_at = (self.clock)();
        let mut entries = Vec::new();
        let mut in_batch = BTreeMap::new();
        let mut waiting = Vec::new();
        for Queued { write, done } in queued {
            let key = write.idempotency_key;
            if let Some(&sequence) = key.as_ref().and_then(|k| state.idempotency.get(k)) {
                let _ = done.send(Ok(sequence));
                continue;
            }
            if let Some(&sequence) = key.as_ref().and_then(|k| in_batch.get(k)) {
                waiting.push((done, sequence));
                continue;
            }
            let sequence = state.head + entries.len() as u64 + 1;
            if let Some(key) = &key {
                in_batch.insert(key.clone(), sequence);
            }
            let event = Event {
                stream: stream(&write.dataset, 0),
                dataset: write.dataset,
                namespace: write.namespace,
                sequence,
                epoch: EPOCH,
                committed_at,
                key: write.key,
                operation: write.operation,
            };
            entries.push((event, key));
            waiting.push((done, sequence));
        }
        let result = if entries.is_empty() {
            Ok(())
        } else {
            self.put(&mut state, Batch::new(entries)).await
        };
        for (done, sequence) in waiting {
            let _ = done.send(result.clone().map(|()| sequence));
        }
    }

    async fn put(&self, state: &mut State, batch: Batch) -> Result<(), CommitError> {
        match self
            .store
            .put(&batch.object, batch.body.clone(), Put::Create)
            .await
        {
            Ok(()) => {
                self.finish(state, batch.entries);
                Ok(())
            }
            Err(S3Error::AlreadyExists { key }) => {
                tracing::error!(
                    key,
                    "another writer committed these sequences; refusing writes to the namespace"
                );
                state.fenced = true;
                Err(CommitError::Fenced)
            }
            Err(error) => {
                state.unresolved = Some(batch);
                Err(CommitError::Unavailable(error.to_string()))
            }
        }
    }

    /// Records a batch that is in the bucket.
    fn finish(&self, state: &mut State, entries: Vec<(Event, Option<String>)>) {
        for (event, key) in entries {
            state.head = event.sequence;
            if let Some(key) = key {
                state.idempotency.insert(key, event.sequence);
            }
            // The namespace lock orders every append, so the sequence is always the log's next.
            self.log.append(event).expect("batch continues the log");
        }
    }
}

fn validate(write: &Write) -> Result<(), CommitError> {
    for (what, name) in [("dataset", &write.dataset), ("namespace", &write.namespace)] {
        if name.is_empty() || name.contains(['/', '\0']) {
            return Err(CommitError::Invalid(format!(
                "{what} must be non-empty, without '/' or NUL"
            )));
        }
    }
    if write.key.is_empty() {
        return Err(CommitError::Invalid("key must not be empty".into()));
    }
    Ok(())
}

/// Zero-padded to u64's 20 digits, so listing order is sequence order.
fn log_object(dataset: &str, namespace: &str, first: u64) -> String {
    format!("{dataset}/{namespace}/log/{first:020}")
}

fn parse_log_object(object: &str) -> Option<(&str, &str, u64)> {
    let mut parts = object.split('/');
    let (Some(dataset), Some(namespace), Some("log"), Some(first), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    Some((dataset, namespace, first.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::replication::subscriptions::Log;
    use crate::s3::memory::MemoryStore;

    /// A [`MemoryStore`] whose puts take a moment, so writes queue behind a batch in flight, and whose next put can be
    /// made to land but report failure, as when the response is lost.
    #[derive(Clone, Default)]
    struct Store {
        objects: MemoryStore,
        lose_reply: Arc<AtomicBool>,
    }

    impl ObjectStore for Store {
        async fn get(&self, key: &str) -> S3Result<Option<Vec<u8>>> {
            self.objects.get(key).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, mode: Put) -> S3Result<()> {
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.objects.put(key, body, mode).await?;
            if self.lose_reply.swap(false, Ordering::SeqCst) {
                return Err(S3Error::Unavailable {
                    detail: "response lost".into(),
                });
            }
            Ok(())
        }

        async fn list(&self, prefix: &str) -> S3Result<Vec<String>> {
            self.objects.list(prefix).await
        }

        async fn delete(&self, key: &str) -> S3Result<()> {
            self.objects.delete(key).await
        }
    }

    fn clock() -> u64 {
        1_700_000_000_000
    }

    fn set(namespace: &str, key: &str, value: &str, idempotency_key: Option<&str>) -> Write {
        Write {
            dataset: "kv".into(),
            namespace: namespace.into(),
            key: key.into(),
            operation: Operation::Set(value.into()),
            idempotency_key: idempotency_key.map(Into::into),
        }
    }

    async fn events(writer: &Writer<Store>, namespace: &str) -> Vec<Event> {
        writer
            .log
            .read("kv", namespace, 1, usize::MAX)
            .await
            .unwrap()
    }

    fn sequences(events: &[Event]) -> Vec<u64> {
        events.iter().map(|e| e.sequence).collect()
    }

    #[tokio::test]
    async fn concurrent_writes_get_gap_free_sequences_in_batches() {
        let store = Store::default();
        let writer = Writer::open(store.clone(), clock).await.unwrap();
        let tasks: Vec<_> = (0..300)
            .map(|i| {
                let writer = writer.clone();
                let namespace = ["a", "b", "c"][i % 3];
                tokio::spawn(async move {
                    let sequence = writer
                        .commit(set(namespace, "k", &i.to_string(), None))
                        .await;
                    (namespace, i, sequence.unwrap())
                })
            })
            .collect();
        let mut committed = BTreeMap::<&str, Vec<(u64, usize)>>::new();
        for task in tasks {
            let (namespace, i, sequence) = task.await.unwrap();
            committed.entry(namespace).or_default().push((sequence, i));
        }

        for (namespace, mut acked) in committed {
            acked.sort();
            let logged = events(&writer, namespace).await;
            assert_eq!(
                sequences(&logged),
                (1..=100).collect::<Vec<_>>(),
                "{namespace}"
            );
            for ((sequence, i), event) in acked.iter().zip(&logged) {
                assert_eq!(event.sequence, *sequence);
                assert_eq!(event.operation, Operation::Set(i.to_string().into()));
            }
        }
        let objects = store.list("kv/").await.unwrap();
        assert!(
            objects.len() < 300,
            "{} batches for 300 writes",
            objects.len()
        );
    }

    #[tokio::test]
    async fn acknowledged_writes_and_keys_survive_a_restart() {
        let store = Store::default();
        let writer = Writer::open(store.clone(), clock).await.unwrap();
        writer.commit(set("a", "x", "1", Some("w1"))).await.unwrap();
        writer.commit(set("b", "x", "1", None)).await.unwrap();
        writer.commit(set("a", "y", "2", None)).await.unwrap();
        let before = events(&writer, "a").await;
        drop(writer);

        let writer = Writer::open(store, clock).await.unwrap();
        assert_eq!(events(&writer, "a").await, before);
        assert_eq!(writer.commit(set("a", "x", "1", Some("w1"))).await, Ok(1));
        assert_eq!(writer.commit(set("a", "z", "3", None)).await, Ok(3));
        assert_eq!(writer.commit(set("b", "z", "3", None)).await, Ok(2));
    }

    #[tokio::test]
    async fn a_second_instance_cannot_commit_existing_sequences() {
        let store = Store::default();
        let first = Writer::open(store.clone(), clock).await.unwrap();
        let second = Writer::open(store.clone(), clock).await.unwrap();
        assert_eq!(first.commit(set("a", "x", "first", None)).await, Ok(1));
        assert_eq!(
            second.commit(set("a", "x", "second", None)).await,
            Err(CommitError::Fenced)
        );
        assert_eq!(
            second.commit(set("a", "y", "second", None)).await,
            Err(CommitError::Fenced)
        );
        assert_eq!(first.commit(set("a", "y", "first", None)).await, Ok(2));

        let reloaded = Writer::open(store, clock).await.unwrap();
        let logged = events(&reloaded, "a").await;
        assert_eq!(sequences(&logged), [1, 2]);
        assert!(
            logged
                .iter()
                .all(|e| e.operation == Operation::Set("first".into()))
        );
    }

    #[tokio::test]
    async fn retries_with_the_same_key_apply_once() {
        let writer = Writer::open(Store::default(), clock).await.unwrap();
        let (a, b) = tokio::join!(
            writer.commit(set("a", "x", "1", Some("k"))),
            writer.commit(set("a", "x", "1", Some("k"))),
        );
        assert_eq!((a, b), (Ok(1), Ok(1)));
        assert_eq!(writer.commit(set("a", "x", "1", Some("k"))).await, Ok(1));
        // Keys are per namespace.
        assert_eq!(writer.commit(set("b", "x", "1", Some("k"))).await, Ok(1));
        assert_eq!(sequences(&events(&writer, "a").await), [1]);
    }

    #[tokio::test]
    async fn a_lost_put_response_is_resolved_before_the_next_batch() {
        let store = Store::default();
        let writer = Writer::open(store.clone(), clock).await.unwrap();
        store.lose_reply.store(true, Ordering::SeqCst);
        let lost = writer.commit(set("a", "x", "1", Some("k"))).await;
        assert!(matches!(lost, Err(CommitError::Unavailable(_))), "{lost:?}");
        assert!(events(&writer, "a").await.is_empty());

        // The retry puts the same batch again, finds it landed, and returns its sequence.
        assert_eq!(writer.commit(set("a", "x", "1", Some("k"))).await, Ok(1));
        assert_eq!(writer.commit(set("a", "y", "2", None)).await, Ok(2));
        assert_eq!(sequences(&events(&writer, "a").await), [1, 2]);
    }

    #[tokio::test]
    async fn full_queues_and_malformed_writes_are_refused() {
        let writer = Writer::open(Store::default(), clock).await.unwrap();
        // Holding the namespace as a batch in flight would, so the commits below queue.
        let namespace = writer.namespace("kv", "a");
        let in_flight = namespace.state.lock().await;
        let queued: Vec<_> = (0..MAX_QUEUED)
            .map(|i| {
                let writer = writer.clone();
                tokio::spawn(
                    async move { writer.commit(set("a", "k", &i.to_string(), None)).await },
                )
            })
            .collect();
        while namespace.queue.lock().unwrap().len() < MAX_QUEUED {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            writer.commit(set("a", "k", "v", None)).await,
            Err(CommitError::Busy)
        );
        drop(in_flight);
        for commit in queued {
            commit.await.unwrap().unwrap();
        }

        for write in [
            set("a/b", "k", "v", None),
            set("", "k", "v", None),
            set("a", "", "v", None),
        ] {
            assert!(matches!(
                writer.commit(write).await,
                Err(CommitError::Invalid(_))
            ));
        }
    }
}
