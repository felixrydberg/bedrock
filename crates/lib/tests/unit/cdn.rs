use super::*;

fn row() -> PathRow {
    PathRow {
        sha256: Hash::of(b"<html>"),
        content_type: "text/html; charset=utf-8".into(),
        size: 6,
    }
}

#[test]
fn a_path_row_round_trips_and_every_malformed_value_is_an_error() {
    let encoded = row().encode().unwrap();
    assert_eq!(PathRow::decode(&encoded), Ok(row()));
    let empty = PathRow {
        content_type: String::new(),
        ..row()
    };
    assert_eq!(PathRow::decode(&empty.encode().unwrap()), Ok(empty));

    let mut version = encoded.clone();
    version[0] = 2;
    let mut control = encoded.clone();
    control.push(b'\n');
    let mut utf8 = encoded.clone();
    utf8.push(0xff);
    for value in [&[][..], &encoded[..HEADER - 1], &version, &control, &utf8] {
        assert!(PathRow::decode(value).is_err(), "{value:?}");
    }
    let header = PathRow {
        content_type: "text/html\r\nx: y".into(),
        ..row()
    };
    assert!(header.encode().is_err());
}

#[test]
fn keys_have_one_parse_and_refuse_ambiguous_deployment_ids() {
    let hash = row().sha256;
    assert_eq!(
        path_key("d1", "/assets/a.css").unwrap(),
        b"path/d1//assets/a.css"
    );
    assert_eq!(
        ref_key(&hash, "d1").unwrap(),
        format!("ref/{hash}/d1").into_bytes()
    );
    assert!(
        ref_key(&hash, "d1")
            .unwrap()
            .starts_with(ref_prefix(&hash).as_bytes())
    );
    for id in ["", "a/b"] {
        assert!(path_key(id, "/").is_err());
        assert!(ref_key(&hash, id).is_err());
    }
}

// Publish client (#81 / B-33).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::replication::materialized::Retention;
use crate::replication::writer::{Persistence, PersistenceMetrics};
use crate::storage::sim::MemoryFactory;
use crate::storage::{ErrorContext, StorageError};

const NS: &str = "deployments";

fn down() -> StorageError {
    StorageError::Unavailable(ErrorContext {
        engine: "test",
        instance: "origin".into(),
        file: None,
        partition: None,
        detail: "injected".into(),
    })
}

/// Uploads and bulks count as steps from 1; `failing` steps fail from step `fail_at` on.
#[derive(Default)]
struct Faults {
    steps: AtomicUsize,
    fail_at: AtomicUsize,
    failing: AtomicUsize,
}

impl Faults {
    fn fail(&self) -> bool {
        let step = self.steps.fetch_add(1, Ordering::SeqCst) + 1;
        let failing = self.failing.load(Ordering::SeqCst);
        let fail = step >= self.fail_at.load(Ordering::SeqCst) && failing > 0;
        if fail {
            self.failing.store(failing - 1, Ordering::SeqCst);
        }
        fail
    }
}

#[derive(Default)]
struct Store {
    objects: Mutex<BTreeMap<Hash, Bytes>>,
    puts: AtomicUsize,
}

struct TestOrigin(Arc<Store>, Arc<Faults>);

#[async_trait]
impl Origin for TestOrigin {
    async fn put(&self, hash: &Hash, bytes: Bytes) -> StorageResult<()> {
        if self.1.fail() {
            return Err(down());
        }
        self.0.puts.fetch_add(1, Ordering::SeqCst);
        self.0.objects.lock().unwrap().insert(*hash, bytes);
        Ok(())
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
        Ok(self.0.objects.lock().unwrap().get(hash).cloned())
    }
}

/// The real writer, checked after every commit: each path row's artifact is stored and has its deployment's ref key.
struct Checked(Arc<Writer>, Arc<Faults>, Arc<Store>);

#[async_trait]
impl Bulks for Checked {
    async fn commit(&self, namespace: &str, changes: Changes) -> Result<u64, Error> {
        if self.1.fail() {
            return Err(Error::Unavailable("injected".into()));
        }
        let revision = Bulks::commit(&self.0, namespace, changes).await?;
        let values = self.0.bootstrap(CDN, namespace).await.unwrap().values;
        for (key, value) in &values {
            let Some(rest) = key.strip_prefix(b"path/") else {
                continue;
            };
            let rest = std::str::from_utf8(rest).unwrap();
            let deployment = rest.split('/').next().unwrap();
            let sha256 = PathRow::decode(value).unwrap().sha256;
            assert!(values.contains_key(&ref_key(&sha256, deployment).unwrap()));
            assert!(self.2.objects.lock().unwrap().contains_key(&sha256));
        }
        Ok(revision)
    }
}

struct World {
    store: Arc<Store>,
    writer: Arc<Writer>,
    faults: Arc<Faults>,
}

impl World {
    async fn new() -> Self {
        let writer = Writer::open(
            || 1_700_000_000_000,
            Persistence {
                factory: Arc::new(MemoryFactory::default()),
                retention: Retention::default(),
                metrics: PersistenceMetrics::default(),
            },
        )
        .await
        .unwrap();
        World {
            store: Arc::default(),
            writer,
            faults: Arc::default(),
        }
    }

    fn publisher(&self) -> Publisher {
        Publisher {
            origin: Arc::new(TestOrigin(self.store.clone(), self.faults.clone())),
            bulks: Arc::new(Checked(
                self.writer.clone(),
                self.faults.clone(),
                self.store.clone(),
            )),
            bulk_max_bytes: CAP,
        }
    }

    /// Fails `count` steps, counted afresh, from step `at` on.
    fn fail(&self, at: usize, count: usize) {
        self.faults.steps.store(0, Ordering::SeqCst);
        self.faults.fail_at.store(at, Ordering::SeqCst);
        self.faults.failing.store(count, Ordering::SeqCst);
    }

    async fn state(&self) -> (BTreeMap<Vec<u8>, Vec<u8>>, BTreeSet<Hash>) {
        let values = self.writer.bootstrap(CDN, NS).await.unwrap().values;
        let objects = self.store.objects.lock().unwrap().keys().copied().collect();
        (values, objects)
    }
}

fn file(path: Option<&str>, body: &str) -> File {
    File {
        path: path.map(Into::into),
        content_type: "text/plain".into(),
        bytes: Bytes::from(body.to_owned()),
    }
}

/// Twelve assets over ten artifacts, and a Worker bundle that is also one of the assets.
fn site() -> Vec<File> {
    let mut files: Vec<File> = (0..12)
        .map(|i| file(Some(&format!("/f{i}.txt")), &format!("body {}", i % 10)))
        .collect();
    files.push(file(None, "body 3"));
    files
}

/// Small enough that the site needs several bulks.
const CAP: usize = 400;

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn publish_uploads_every_artifact_then_writes_refs_before_rows_in_bulks_under_the_cap() {
    let world = World::new().await;
    // Another tenant already stored one artifact: it is uploaded again anyway.
    let existing = Bytes::from_static(b"body 0");
    world
        .store
        .objects
        .lock()
        .unwrap()
        .insert(Hash::of(&existing), existing);
    let revision = world.publisher().publish(NS, "d1", &site()).await.unwrap();
    assert_eq!(world.store.puts.load(Ordering::SeqCst), 10);

    let view = world.writer.bootstrap(CDN, NS).await.unwrap();
    assert_eq!(view.sequence, revision);
    assert!(revision > 2, "the site took {revision} bulks");
    let count = |prefix: &[u8]| view.values.keys().filter(|k| k.starts_with(prefix)).count();
    assert_eq!((count(b"path/"), count(b"ref/")), (12, 10));
    let row = &view.values[&path_key("d1", "/f3.txt").unwrap()];
    assert_eq!(
        PathRow::decode(row),
        Ok(PathRow {
            sha256: Hash::of(b"body 3"),
            content_type: "text/plain".into(),
            size: 6,
        })
    );

    // Rerunning writes the same keys.
    world.publisher().publish(NS, "d1", &site()).await.unwrap();
    assert_eq!(world.state().await.0, view.values);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_publish_killed_after_any_step_returns_no_revision_and_a_rerun_reaches_the_same_state() {
    let clean = World::new().await;
    clean.publisher().publish(NS, "d1", &site()).await.unwrap();
    let expected = clean.state().await;
    let steps = clean.faults.steps.load(Ordering::SeqCst);
    assert!(steps > 12, "{steps} steps");

    for at in 1..=steps {
        let world = World::new().await;
        // Every attempt of step `at` fails, as if the client died there.
        world.fail(at, ATTEMPTS as usize);
        let killed = world.publisher().publish(NS, "d1", &site()).await;
        assert!(matches!(killed, Err(Error::Unavailable(_))), "step {at}");
        world.publisher().publish(NS, "d1", &site()).await.unwrap();
        assert_eq!(world.state().await, expected, "step {at}");
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn release_deletes_rows_before_refs_retries_a_failed_bulk_and_keeps_shared_artifacts() {
    let world = World::new().await;
    let publisher = world.publisher();
    publisher.publish(NS, "d1", &site()).await.unwrap();
    let shared = [file(Some("/"), "body 0")];
    publisher.publish(NS, "d2", &shared).await.unwrap();

    let paths: Vec<String> = (0..12).map(|i| format!("/f{i}.txt")).collect();
    let artifacts: Vec<Hash> = (0..10)
        .map(|i| Hash::of(format!("body {i}").as_bytes()))
        .collect();
    // The second bulk fails once and is retried.
    world.fail(2, 1);
    let revision = publisher
        .release(NS, "d1", &paths, &artifacts)
        .await
        .unwrap();
    assert_eq!(world.faults.failing.load(Ordering::SeqCst), 0);
    assert!(
        publisher
            .release(NS, "d1", &paths, &artifacts)
            .await
            .unwrap()
            > revision
    );

    let keys: Vec<Vec<u8>> = world.state().await.0.into_keys().collect();
    let sha256 = Hash::of(b"body 0");
    assert_eq!(
        keys,
        [
            path_key("d2", "/").unwrap(),
            ref_key(&sha256, "d2").unwrap()
        ]
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_publish_that_can_never_succeed_uploads_and_writes_nothing() {
    let world = World::new().await;
    let twice = [file(Some("/a"), "x"), file(Some("/a"), "y")];
    let big = [file(Some(&"p".repeat(CAP)), "x")];
    for files in [&twice[..], &big, &[]] {
        let result = world.publisher().publish(NS, "d1", files).await;
        assert!(matches!(result, Err(Error::Invalid(_))), "{result:?}");
    }
    let ambiguous = world.publisher().publish(NS, "a/b", &site()).await;
    assert!(matches!(ambiguous, Err(Error::Invalid(_))));
    assert_eq!(world.store.puts.load(Ordering::SeqCst), 0);
    assert_eq!(world.writer.bootstrap(CDN, NS).await.unwrap().sequence, 0);
}

#[cfg(not(madsim))]
#[tokio::test]
async fn the_write_api_client_posts_one_bulk_and_maps_rejections() {
    use axum::routing::post;
    let ok = |body: Bytes| async move {
        let bulk = pb::Bulk::decode(body).unwrap();
        (StatusCode::OK, bulk.changes.len().to_string())
    };
    let app = axum::Router::new()
        .route("/v1/cdn/ok", post(ok))
        .route(
            "/v1/cdn/big",
            post(|| async { StatusCode::PAYLOAD_TOO_LARGE }),
        )
        .route(
            "/v1/cdn/busy",
            post(|| async { StatusCode::TOO_MANY_REQUESTS }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(crate::http::serve(listener, app, None));
    let api = WriteApi {
        address,
        token: "t".into(),
        tls: None,
    };
    let changes: Changes = vec![(b"a".to_vec(), Some(Vec::new())), (b"b".to_vec(), None)];
    assert_eq!(api.commit("ok", changes.clone()).await, Ok(2));
    let big = api.commit("big", changes.clone()).await;
    assert!(matches!(big, Err(Error::Invalid(_))));
    let busy = api.commit("busy", changes).await;
    assert!(matches!(busy, Err(Error::Unavailable(_))));
}
