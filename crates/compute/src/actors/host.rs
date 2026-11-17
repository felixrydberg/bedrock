//! The actor host (#115 / S-07, `statefull/v2.md` §5 "Inside one host", §6, §8): one instance per actor on this host.
//!
//! - **Acquire.** [`ActorHost::acquire`] returns the awake actor, or claims it through the volume client and starts its
//!   instance, or fails with [`Error::Redirect`] naming the live holder. Each actor has one wake lock, so an `Acquire`
//!   during a wake joins it and a second wake never starts.
//! - **Execution.** A built-in kind starts in the Rust executor and customer code in the sandbox executor, through the
//!   one [`Executor`]/[`Instance`] seam; a built-in kind never calls the sandbox.
//! - **Gates.** [`Awake::invoke`] runs one event at a time across storage awaits (the input gate). Storage calls return
//!   only after their commit's quorum, so a response never precedes its commits (the output gate); a response is also
//!   withheld once the holder stops serving, so an answer from memory never outlives the lease.
//! - **Leases.** The volume client's lease fences a holder cut off from its slots within 4 s; the host then drops the
//!   instance and counts a lease expiry. A volume error drops the instance too, so pages of a commit that failed are
//!   never read again.
//! - **Sleep.** An actor with no event for [`IDLE`] appends `Release` and stops. Neither sleep nor wake touches S3.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_lib::actors::{Kind, SLOTS};
use bedrock_lib::admin::Admin;
use bedrock_lib::volume as pb;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use tokio::time::{Instant, sleep};

use super::storage::Storage;
use super::volume::{self, HEARTBEAT, Slot, Writer};
use crate::runtime::{Error, ExecutionSpec, Executor, Instance};

/// An actor with no event for this long sleeps.
pub const IDLE: Duration = Duration::from_secs(10);

/// Where an actor's slots are.
#[async_trait]
pub trait Placement: Send + Sync {
    /// The actor's three slot nodes and the topology version they come from.
    async fn slots(&self, actor: &pb::Actor) -> Result<([Slot; SLOTS], u64), Error>;
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KindLabel {
    kind: &'static str,
}

fn label(kind: i32) -> KindLabel {
    KindLabel {
        kind: Kind::try_from(kind).map_or("unknown", Kind::as_str),
    }
}

type Latency = Family<KindLabel, Histogram, fn() -> Histogram>;

/// Labelled only by `cell`, `dc` (on the sub-registry) and actor kind.
struct Metrics {
    claim: Latency,
    wake: Latency,
    expiries: Family<KindLabel, Counter>,
}

impl Metrics {
    fn new(admin: &Admin, cell: &str, dc: &str) -> Self {
        let latency = || Histogram::new(exponential_buckets(0.000_5, 2.0, 16));
        let metrics = Metrics {
            claim: Family::new_with_constructor(latency),
            wake: Family::new_with_constructor(latency),
            expiries: Family::default(),
        };
        admin.register_labelled(
            vec![("cell", cell.to_owned()), ("dc", dc.to_owned())],
            |registry| {
                registry.register(
                    "compute_actor_claim_seconds",
                    "Time to reserve, claim and recover an actor's slots",
                    metrics.claim.clone(),
                );
                registry.register(
                    "compute_actor_wake_seconds",
                    "Time from a wake's start until its instance runs",
                    metrics.wake.clone(),
                );
                registry.register(
                    "compute_actor_lease_expiries",
                    "Awake actors dropped because their holder stopped serving or their storage failed",
                    metrics.expiries.clone(),
                );
            },
        );
        metrics
    }
}

/// An actor's wake lock and its awake instance, if any.
type Cell = Arc<tokio::sync::Mutex<Option<Arc<Awake>>>>;
type Key = (String, String);

/// A Compute host's actors.
#[derive(Clone)]
pub struct ActorHost(Arc<Inner>);

struct Inner {
    volume: Arc<volume::Host>,
    placement: Arc<dyn Placement>,
    rust: Arc<dyn Executor>,
    sandbox: Arc<dyn Executor>,
    actors: Mutex<BTreeMap<Key, Cell>>,
    metrics: Metrics,
}

impl ActorHost {
    /// A host heartbeating through `volume`; built-in kinds run in `rust`, customer code in `sandbox`.
    pub fn new(
        admin: &Admin,
        cell: &str,
        dc: &str,
        volume: Arc<volume::Host>,
        placement: Arc<dyn Placement>,
        rust: Arc<dyn Executor>,
        sandbox: Arc<dyn Executor>,
    ) -> Self {
        ActorHost(Arc::new(Inner {
            volume,
            placement,
            rust,
            sandbox,
            actors: Mutex::default(),
            metrics: Metrics::new(admin, cell, dc),
        }))
    }

    /// The awake actor: the one already running here, or one claimed and woken now.
    pub async fn acquire(&self, actor: &pb::Actor) -> Result<Arc<Awake>, Error> {
        let key = (actor.namespace.clone(), actor.id.clone());
        let cell = self.0.lock().entry(key.clone()).or_default().clone();
        let result = {
            let mut awake = cell.lock().await;
            match awake.as_ref() {
                Some(running) if running.serving() => {
                    // Under the wake lock, so the watcher sees it before it can sleep the actor.
                    running.touch();
                    Ok(running.clone())
                }
                _ => {
                    if let Some(stopped) = awake.take() {
                        self.0.expired(&stopped).await;
                    }
                    let woken = self.0.wake(actor).await;
                    if let Ok(woken) = &woken {
                        *awake = Some(woken.clone());
                        tokio::spawn(watch(Arc::downgrade(&self.0), key.clone(), woken.clone()));
                    }
                    woken
                }
            }
        };
        drop(cell);
        self.0.forget(&key);
        result
    }

    /// How many actors are awake here.
    pub async fn awake(&self) -> usize {
        let cells: Vec<Cell> = self.0.lock().values().cloned().collect();
        let mut n = 0;
        for cell in cells {
            n += usize::from(cell.lock().await.is_some());
        }
        n
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<Key, Cell>> {
        self.actors.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Drops `key`'s wake lock when nobody holds or waits for it and no actor is awake.
    fn forget(&self, key: &Key) {
        let mut actors = self.lock();
        if actors.get(key).is_some_and(|cell| {
            Arc::strong_count(cell) == 1 && cell.try_lock().is_ok_and(|awake| awake.is_none())
        }) {
            actors.remove(key);
        }
    }

    async fn wake(&self, actor: &pb::Actor) -> Result<Arc<Awake>, Error> {
        let started = bedrock_lib::monotonic_now();
        let (slots, version) = self.placement.slots(actor).await?;
        let writer = Writer::open(self.volume.clone(), actor.clone(), slots, version).await?;
        let kind = label(actor.kind);
        self.metrics
            .claim
            .get_or_create(&kind)
            .observe(started.elapsed().as_secs_f64());
        let storage = Storage::new(writer);
        let spec = ExecutionSpec::Actor {
            actor: actor.clone(),
            storage: storage.clone(),
        };
        let executor = if spec.builtin().is_some() {
            &self.rust
        } else {
            &self.sandbox
        };
        let instance = match executor.start(spec).await {
            Ok(instance) => instance,
            Err(error) => {
                release(storage).await;
                return Err(error);
            }
        };
        self.metrics
            .wake
            .get_or_create(&kind)
            .observe(started.elapsed().as_secs_f64());
        Ok(Arc::new(Awake {
            kind: actor.kind,
            storage: Mutex::new(Some(storage)),
            instance: tokio::sync::Mutex::new(Some(instance)),
            last: Mutex::new(bedrock_lib::monotonic_now()),
            busy: AtomicUsize::new(0),
        }))
    }

    async fn expired(&self, awake: &Awake) {
        self.metrics
            .expiries
            .get_or_create(&label(awake.kind))
            .inc();
        awake.stop().await;
    }
}

/// Appends `Release` once no other clone of `storage` is left; a fenced writer has nothing to release.
async fn release(storage: Storage) {
    if let Some(writer) = storage.into_writer()
        && let Err(error) = writer.release(super::storage::now_ms()).await
    {
        tracing::debug!(%error, "releasing an actor failed");
    }
}

/// Sleeps `awake` once idle, and drops it once its holder stops serving.
async fn watch(host: Weak<Inner>, key: Key, awake: Arc<Awake>) {
    loop {
        sleep(HEARTBEAT).await;
        let Some(host) = host.upgrade() else { return };
        let serving = awake.serving();
        if serving && !awake.idle() {
            continue;
        }
        let Some(cell) = host.lock().get(&key).cloned() else {
            return;
        };
        {
            let mut current = cell.lock().await;
            if !current.as_ref().is_some_and(|a| Arc::ptr_eq(a, &awake)) {
                return;
            }
            if !awake.serving() {
                *current = None;
                host.expired(&awake).await;
            } else if awake.idle() {
                *current = None;
                // Under the wake lock, so a new wake waits for the release and then needs no lease wait.
                if let Some(storage) = awake.stop().await {
                    release(storage).await;
                }
            } else {
                continue;
            }
        }
        drop(cell);
        host.forget(&key);
        return;
    }
}

/// An awake actor on this host.
pub struct Awake {
    kind: i32,
    /// `None` once stopped.
    storage: Mutex<Option<Storage>>,
    /// The input gate: one event at a time. `None` once stopped.
    instance: tokio::sync::Mutex<Option<Box<dyn Instance>>>,
    last: Mutex<Instant>,
    busy: AtomicUsize,
}

impl Awake {
    /// Whether this holder may still answer: not stopped, and its writer still serving.
    pub fn serving(&self) -> bool {
        self.storage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(Storage::serving)
    }

    fn touch(&self) {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = bedrock_lib::monotonic_now();
    }

    fn idle(&self) -> bool {
        self.busy.load(Ordering::Relaxed) == 0
            && self
                .last
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .elapsed()
                >= IDLE
    }

    /// Runs one event; its response leaves only after its commits and while this holder serves. An event whose caller
    /// is cancelled, or whose handler panics, may leave the instance's memory ahead of or behind its commits, so the
    /// actor stops and the next `Acquire` wakes it again from its slots.
    pub async fn invoke(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.busy.fetch_add(1, Ordering::Relaxed);
        let mut busy = Busy {
            awake: self,
            armed: false,
        };
        let result = {
            let mut instance = self.instance.lock().await;
            let result = match instance.as_mut() {
                Some(instance) if self.serving() => {
                    // Only once the event runs: a caller cancelled while queued at the gate leaves the actor alone.
                    busy.armed = true;
                    instance.invoke(request).await
                }
                _ => Err(Error::Volume(volume::Error::Fenced)),
            };
            match result {
                Ok(_) if !self.serving() => Err(Error::Volume(volume::Error::Fenced)),
                other => other,
            }
        };
        if matches!(result, Err(Error::Volume(_))) {
            self.stop().await;
        }
        busy.armed = false;
        drop(busy);
        result
    }

    /// Drops the instance and returns the storage, once no event runs.
    async fn stop(&self) -> Option<Storage> {
        let instance = self.instance.lock().await.take();
        drop(instance);
        self.storage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// One event: on every exit it leaves the busy count and marks the actor used. An event dropped while armed (started
/// but not finished and cleaned up) takes the storage, so the actor stops serving.
struct Busy<'a> {
    awake: &'a Awake,
    armed: bool,
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        if self.armed {
            drop(
                self.awake
                    .storage
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take(),
            );
        }
        self.awake.touch();
        self.awake.busy.fetch_sub(1, Ordering::Relaxed);
    }
}
