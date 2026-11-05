use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::time::{sleep, timeout};

use super::*;
use crate::health::fallback::{self, Found, Limited, Read, ReadKey, SourceChain, Tier};
use crate::monotonic_now;
use crate::reconciliation::{self, Reconcile, Reconciler};
use crate::storage::sim::MemoryFactory;

const OP: Duration = Duration::from_millis(80);
/// Timer and scheduling noise. Waiting for capacity would cost a whole [`OP`].
const TICK: Duration = Duration::from_millis(2);

#[derive(Clone)]
struct Get {
    cell: String,
    key: String,
}

fn get(cell: &str, key: &str) -> Get {
    Get {
        cell: cell.to_owned(),
        key: key.to_owned(),
    }
}

impl ReadKey for Get {
    fn cell(&self) -> &str {
        &self.cell
    }

    fn identity(&self) -> (String, Vec<u8>) {
        ("ns".into(), self.key.clone().into_bytes())
    }
}

fn unavailable() -> StorageError {
    StorageError::Unavailable(ErrorContext {
        engine: "test",
        instance: "s3".into(),
        file: None,
        partition: None,
        detail: "down".into(),
    })
}

/// A shared upstream that takes [`OP`] per read and records how hard it was hit.
#[derive(Default)]
struct Source {
    revision: AtomicU64,
    down: AtomicBool,
    reads: AtomicUsize,
    running: Mutex<BTreeMap<String, usize>>,
    peak: Mutex<BTreeMap<String, usize>>,
    peak_total: AtomicUsize,
}

impl Source {
    fn at(revision: u64) -> Arc<Self> {
        let source = Source::default();
        source.revision.store(revision, Ordering::Relaxed);
        Arc::new(source)
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    fn peak(&self, cell: &str) -> usize {
        self.peak.lock().unwrap().get(cell).copied().unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Tier<Get> for Source {
    fn name(&self) -> &'static str {
        "s3"
    }

    async fn read(&self, key: &Get, _: u64) -> StorageResult<Read> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        {
            let mut running = self.running.lock().unwrap();
            let cell = running.entry(key.cell.clone()).or_default();
            *cell += 1;
            let mut peak = self.peak.lock().unwrap();
            let highest = peak.entry(key.cell.clone()).or_default();
            *highest = (*highest).max(*cell);
            self.peak_total
                .fetch_max(running.values().sum(), Ordering::Relaxed);
        }
        // The view the read saw when it started, however long the answer takes to arrive.
        let revision = self.revision.load(Ordering::Relaxed);
        sleep(OP).await;
        *self.running.lock().unwrap().get_mut(&key.cell).unwrap() -= 1;
        if self.down.load(Ordering::Relaxed) {
            return Err(unavailable());
        }
        Ok(Read::Found(Found {
            value: Some(key.key.clone().into_bytes()),
            revision,
        }))
    }
}

/// A local cache. `delay` keeps a flight open long enough for a test to join it.
#[derive(Default)]
struct Cache {
    delay: Duration,
    entries: Mutex<BTreeMap<String, Found>>,
    fills: AtomicUsize,
}

#[async_trait::async_trait]
impl Tier<Get> for Cache {
    fn name(&self) -> &'static str {
        "l1"
    }

    async fn read(&self, key: &Get, _: u64) -> StorageResult<Read> {
        sleep(self.delay).await;
        Ok(match self.entries.lock().unwrap().get(&key.key) {
            Some(found) => Read::Found(found.clone()),
            None => Read::Miss,
        })
    }

    async fn fill(&self, key: &Get, found: &Found) -> StorageResult<()> {
        self.fills.fetch_add(1, Ordering::Relaxed);
        self.entries
            .lock()
            .unwrap()
            .insert(key.key.clone(), found.clone());
        Ok(())
    }
}

/// One node's read path: `cache` → limited `source`.
fn node(
    admin: &Admin,
    cache: &Arc<Cache>,
    source: &Arc<Source>,
    limits: Limits,
) -> (Arc<SourceChain<Get>>, Arc<Limiter>, Metrics) {
    let metrics = Metrics::new(admin);
    let limiter = Arc::new(Limiter::new("s3", limits, &metrics));
    let s3: Arc<dyn Tier<Get>> = source.clone();
    let tiers: Vec<Arc<dyn Tier<Get>>> =
        vec![cache.clone(), Arc::new(Limited::new(s3, limiter.clone()))];
    let chain = SourceChain::new("cdn", tiers, &fallback::Metrics::new(admin), &metrics);
    (Arc::new(chain), limiter, metrics)
}

fn spawn_read(
    chain: &Arc<SourceChain<Get>>,
    key: Get,
    min_revision: u64,
) -> tokio::task::JoinHandle<StorageResult<Found>> {
    let chain = chain.clone();
    tokio::spawn(async move { chain.read(&key, min_revision).await })
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_thousand_concurrent_misses_share_one_fetch_and_one_fill() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    let (chain, _, metrics) = node(&admin, &cache, &source, S3_FETCHES);

    // Spread over cells: identity, not the caller, decides what is the same read.
    let reads: Vec<_> = (0..1000)
        .map(|i| spawn_read(&chain, get(&format!("cell-{}", i % 4), "hot"), 0))
        .collect();
    for read in reads {
        let found = read.await.unwrap().unwrap();
        assert_eq!(found.value.as_deref(), Some(&b"hot"[..]));
    }
    assert_eq!(source.reads(), 1);
    assert_eq!(cache.fills.load(Ordering::Relaxed), 1);
    let labels = SourceLabels {
        source: "cdn".into(),
    };
    assert_eq!(metrics.coalesced.get_or_create(&labels).get(), 999);

    // The flight is over and the cache is filled, so the next read costs the source nothing.
    chain.read(&get("cell-0", "hot"), 0).await.unwrap();
    assert_eq!(source.reads(), 1);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_failed_fetch_is_shared_rather_than_repeated_by_every_waiter() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    source.down.store(true, Ordering::Relaxed);
    let (chain, ..) = node(&admin, &cache, &source, S3_FETCHES);

    let reads: Vec<_> = (0..1000)
        .map(|_| spawn_read(&chain, get("cell-a", "k"), 0))
        .collect();
    for read in reads {
        assert!(matches!(
            read.await.unwrap(),
            Err(StorageError::Unavailable(_))
        ));
    }
    assert_eq!(source.reads(), 1);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cancelled_leader_hands_over_to_a_waiter() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    let (chain, _, metrics) = node(&admin, &cache, &source, S3_FETCHES);

    let leader = {
        let chain = chain.clone();
        tokio::spawn(async move { timeout(OP / 2, chain.read(&get("cell-a", "k"), 0)).await })
    };
    sleep(OP / 4).await;
    let waiter = spawn_read(&chain, get("cell-a", "k"), 0);

    assert!(leader.await.unwrap().is_err(), "the leader timed out");
    assert_eq!(waiter.await.unwrap().unwrap().revision, 7);
    assert_eq!(source.reads(), 2);
    let labels = SourceLabels {
        source: "s3".into(),
    };
    assert_eq!(metrics.running.get_or_create(&labels).get(), 0);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_waiter_never_takes_an_answer_older_than_its_minimum_revision() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(5));
    let (chain, ..) = node(&admin, &cache, &source, S3_FETCHES);

    let old = spawn_read(&chain, get("cell-a", "k"), 0);
    sleep(OP / 4).await;
    let fresh = spawn_read(&chain, get("cell-a", "k"), 7);
    sleep(OP / 4).await;
    // A write lands while the first flight is still answering at revision 5.
    source.revision.store(9, Ordering::Relaxed);
    assert_eq!(old.await.unwrap().unwrap().revision, 5);
    assert_eq!(fresh.await.unwrap().unwrap().revision, 9);
    assert_eq!(source.reads(), 2);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_stricter_read_failing_does_not_fail_a_waiter_that_needs_less() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(5));
    let (chain, ..) = node(&admin, &cache, &source, S3_FETCHES);

    let strict = spawn_read(&chain, get("cell-a", "k"), 7);
    sleep(OP / 4).await;
    let relaxed = spawn_read(&chain, get("cell-a", "k"), 0);
    assert!(matches!(
        strict.await.unwrap(),
        Err(StorageError::Unavailable(_))
    ));
    assert_eq!(relaxed.await.unwrap().unwrap().revision, 5);
    assert_eq!(source.reads(), 2);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn an_overload_stays_with_the_cell_that_caused_it() {
    let admin = Admin::new();
    let cache = Arc::new(Cache {
        delay: OP,
        ..Cache::default()
    });
    let source = Source::at(7);
    let limits = Limits {
        total: 4,
        per_cell: 1,
        queue: 0,
    };
    let (chain, limiter, _) = node(&admin, &cache, &source, limits);
    let _full = limiter.acquire("noisy").await.unwrap();

    let leader = spawn_read(&chain, get("noisy", "hot"), 0);
    sleep(OP / 4).await;
    let same_cell = spawn_read(&chain, get("noisy", "hot"), 0);
    let other_cell = spawn_read(&chain, get("quiet", "hot"), 0);

    for read in [leader, same_cell] {
        assert!(matches!(
            read.await.unwrap(),
            Err(StorageError::Overloaded(_))
        ));
    }
    assert_eq!(other_cell.await.unwrap().unwrap().revision, 7);
    assert_eq!(source.reads(), 1);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn separate_coalescers_each_fetch_once_and_stay_bounded() {
    let source = Source::at(7);
    // No global deduplication: each node has its own coalescer and limiter in front of the same S3.
    let nodes: Vec<_> = (0..4)
        .map(|_| {
            let admin = Admin::new();
            node(&admin, &Arc::default(), &source, S3_FETCHES).0
        })
        .collect();

    let reads: Vec<_> = nodes
        .iter()
        .flat_map(|chain| (0..1000).map(|_| spawn_read(chain, get("cell-a", "hot"), 0)))
        .collect();
    for read in reads {
        read.await.unwrap().unwrap();
    }
    assert_eq!(source.reads(), 4, "one fetch per coalescer");

    let reads: Vec<_> = nodes
        .iter()
        .flat_map(|chain| {
            (0..200).map(|i| spawn_read(chain, get("cell-a", &format!("cold-{i}")), 0))
        })
        .collect();
    for read in reads {
        let _ = read.await.unwrap();
    }
    assert!(source.peak_total.load(Ordering::Relaxed) <= 4 * S3_FETCHES.per_cell);
}

#[derive(Debug, Default)]
struct Outcome {
    answered: usize,
    rejected: usize,
    worst: Duration,
}

/// Each quiet cell sends one miss every 10 ms for 400 ms, and each noisy cell ten times that, all for distinct keys.
async fn surge(noisy: usize) -> (BTreeMap<String, Outcome>, Arc<Source>) {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    let (chain, ..) = node(&admin, &cache, &source, S3_FETCHES);
    let outcomes: Arc<Mutex<BTreeMap<String, Outcome>>> = Arc::default();

    let quiet = (0..2).map(|i| (format!("quiet-{i}"), 10, 40));
    let noisy = (0..noisy).map(|i| (format!("noisy-{i}"), 1, 400));
    let cells: Vec<_> = quiet
        .chain(noisy)
        .map(|(cell, every_ms, count)| {
            let (chain, outcomes) = (chain.clone(), outcomes.clone());
            tokio::spawn(async move {
                let mut requests = Vec::new();
                for i in 0..count {
                    let (chain, outcomes) = (chain.clone(), outcomes.clone());
                    let key = get(&cell, &format!("{cell}-{i}"));
                    requests.push(tokio::spawn(async move {
                        let started = monotonic_now();
                        let answer = chain.read(&key, 0).await;
                        let mut outcomes = outcomes.lock().unwrap();
                        let outcome = outcomes.entry(key.cell).or_default();
                        match answer {
                            Ok(_) => outcome.answered += 1,
                            Err(StorageError::Overloaded(_)) => outcome.rejected += 1,
                            Err(error) => panic!("overload must stay retryable: {error}"),
                        }
                        outcome.worst = outcome.worst.max(started.elapsed());
                    }));
                    sleep(Duration::from_millis(every_ms)).await;
                }
                for request in requests {
                    request.await.unwrap();
                }
            })
        })
        .collect();
    for cell in cells {
        cell.await.unwrap();
    }
    let outcomes = std::mem::take(&mut *outcomes.lock().unwrap());
    (outcomes, source)
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cell_at_ten_times_its_miss_rate_is_throttled_and_others_keep_their_latency() {
    let (normal, _) = surge(0).await;
    let (outcomes, source) = surge(1).await;

    let noisy = &outcomes["noisy-0"];
    assert!(noisy.rejected > 0, "{noisy:?}");
    assert!(noisy.answered > 0, "throttled, not cut off: {noisy:?}");
    assert_eq!(source.peak("noisy-0"), S3_FETCHES.per_cell);
    for cell in ["quiet-0", "quiet-1"] {
        assert_eq!(outcomes[cell].rejected, 0);
        assert_eq!(outcomes[cell].answered, 40);
        assert!(outcomes[cell].worst <= normal[cell].worst + TICK, "{cell}");
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn many_surging_cells_filling_the_source_still_leave_quiet_cells_their_turn() {
    // Five cells can ask for 80 operations, more than the source's 64.
    let (outcomes, source) = surge(5).await;

    assert_eq!(
        source.peak_total.load(Ordering::Relaxed),
        S3_FETCHES.total,
        "the source was full"
    );
    for cell in ["quiet-0", "quiet-1"] {
        assert_eq!(outcomes[cell].rejected, 0);
        assert_eq!(outcomes[cell].answered, 40);
        assert!(outcomes[cell].worst < 3 * OP, "{:?}", outcomes[cell]);
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cancelled_queued_request_gives_up_its_place() {
    let admin = Admin::new();
    let metrics = Metrics::new(&admin);
    let limits = Limits {
        total: 1,
        per_cell: 1,
        queue: 1,
    };
    let limiter = Arc::new(Limiter::new("s3", limits, &metrics));
    let labels = SourceLabels {
        source: "s3".into(),
    };
    let running = limiter.acquire("cell-a").await.unwrap();

    let queued = {
        let limiter = limiter.clone();
        tokio::spawn(async move { limiter.acquire("cell-a").await.map(drop) })
    };
    sleep(Duration::from_millis(1)).await;
    assert_eq!(metrics.queued.get_or_create(&labels).get(), 1);
    assert!(matches!(
        limiter.acquire("cell-a").await,
        Err(StorageError::Overloaded(_))
    ));

    queued.abort();
    let _ = queued.await;
    assert_eq!(metrics.queued.get_or_create(&labels).get(), 0);
    let waiting = {
        let limiter = limiter.clone();
        tokio::spawn(async move { limiter.acquire("cell-a").await.map(drop) })
    };
    sleep(Duration::from_millis(1)).await;
    drop(running);
    waiting.await.unwrap().unwrap();
    assert_eq!(metrics.running.get_or_create(&labels).get(), 0);
}

/// Answers `read` until it stops being overloaded, the way a caller backs off.
async fn read_with_backoff(chain: &SourceChain<Get>, key: &Get) -> Found {
    loop {
        match chain.read(key, 0).await {
            Err(StorageError::Overloaded(_)) => sleep(OP).await,
            answer => return answer.unwrap(),
        }
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_range_move_warming_a_new_owner_stays_bounded_and_shares_fetches_with_readers() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    let (chain, ..) = node(&admin, &cache, &source, S3_FETCHES);

    // The new owner warms every artifact in the range while readers ask for the same ones.
    let mut tasks = Vec::new();
    for artifact in 0..300 {
        for cell in ["cell-a/warm", "cell-a"] {
            let chain = chain.clone();
            let key = get(cell, &format!("artifact-{artifact}"));
            tasks.push(tokio::spawn(async move {
                read_with_backoff(&chain, &key).await
            }));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(source.reads(), 300, "each artifact was fetched once");
    assert!(source.peak("cell-a/warm") <= S3_FETCHES.per_cell);
    assert!(source.peak("cell-a") <= S3_FETCHES.per_cell);
}

/// Replica repair as B-34 will run it: desired artifacts against the local cache, fetching through the read path.
struct Warm {
    artifacts: Vec<String>,
    cache: Arc<Cache>,
    chain: Arc<SourceChain<Get>>,
}

#[async_trait::async_trait]
impl Reconcile for Warm {
    type Key = String;
    type Desired = ();
    type Actual = Found;
    type Repair = ();
    type Error = StorageError;

    async fn desired(&self) -> StorageResult<BTreeMap<String, ()>> {
        Ok(self.artifacts.iter().map(|a| (a.clone(), ())).collect())
    }

    async fn actual(&self) -> StorageResult<BTreeMap<String, Found>> {
        Ok(self.cache.entries.lock().unwrap().clone())
    }

    fn plan(&self, _: &String, _: Option<&()>, actual: Option<&Found>) -> Option<(u8, ())> {
        actual.is_none().then_some((0, ()))
    }

    async fn repair(&self, key: &String, _: &()) -> StorageResult<()> {
        let key = get("cell-a/warm", key);
        self.chain.read(&key, 0).await.map(drop)
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn warming_backs_off_when_overloaded_and_never_takes_the_readers_lane() {
    let admin = Admin::new();
    let (cache, source) = (Arc::new(Cache::default()), Source::at(7));
    let limits = Limits {
        total: 4,
        per_cell: 1,
        queue: 0,
    };
    let (chain, limiter, _) = node(&admin, &cache, &source, limits);
    let warm = Warm {
        artifacts: vec!["a".into(), "b".into()],
        cache: cache.clone(),
        chain: chain.clone(),
    };
    let config = reconciliation::Config {
        burst: 100,
        refill: Duration::from_millis(1),
        repair_timeout: Duration::from_secs(1),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
        seed: 1,
    };
    let metrics = reconciliation::Metrics::new(&admin);
    let mut warming = Reconciler::new("warm", Arc::new(warm), config, &metrics);

    let full = limiter.acquire("cell-a/warm").await.unwrap();
    let pass = warming.pass().await.unwrap();
    assert_eq!((pass.repaired, pass.failed), (0, 2));
    // Readers in the same cell are on their own lane and unaffected.
    chain.read(&get("cell-a", "other"), 0).await.unwrap();
    drop(full);

    let pass = warming.pass().await.unwrap();
    assert_eq!((pass.repaired, pass.failed), (0, 0), "still backing off");
    sleep(Duration::from_secs(1)).await;
    let pass = warming.pass().await.unwrap();
    assert_eq!((pass.repaired, pass.failed), (2, 0));
}

/// Counts opens and takes [`OP`] each, like a cold manifest read.
#[derive(Default)]
struct Factory {
    memory: MemoryFactory,
    opens: AtomicUsize,
    running: AtomicUsize,
    peak: AtomicUsize,
    down: AtomicBool,
}

#[async_trait::async_trait]
impl NamespaceFactory for Factory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        self.memory.namespaces().await
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        let running = self.running.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(running, Ordering::Relaxed);
        sleep(OP).await;
        self.running.fetch_sub(1, Ordering::Relaxed);
        if self.down.load(Ordering::Relaxed) {
            return Err(unavailable());
        }
        self.memory.open(dataset, namespace).await
    }
}

fn spawn_open(
    opens: &Arc<NamespaceOpens>,
    cell: &str,
    namespace: &str,
) -> tokio::task::JoinHandle<StorageResult<Arc<dyn NamespaceDatabase>>> {
    let (opens, cell, namespace) = (opens.clone(), cell.to_owned(), namespace.to_owned());
    tokio::spawn(async move { opens.open(&cell, "kv", &namespace).await })
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn concurrent_opens_of_one_namespace_share_one_initialization() {
    let admin = Admin::new();
    let factory = Arc::new(Factory::default());
    let opens = Arc::new(NamespaceOpens::new(factory.clone(), &Metrics::new(&admin)));

    let handles: Vec<_> = (0..1000)
        .map(|i| spawn_open(&opens, &format!("cell-{}", i % 4), "ns"))
        .collect();
    let mut readers = Vec::new();
    for handle in handles {
        readers.push(handle.await.unwrap().unwrap());
    }
    assert_eq!(factory.opens.load(Ordering::Relaxed), 1);
    assert!(readers.iter().all(|r| Arc::ptr_eq(r, &readers[0])));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_failed_open_is_shared_and_the_next_open_tries_again() {
    let admin = Admin::new();
    let factory = Arc::new(Factory::default());
    factory.down.store(true, Ordering::Relaxed);
    let opens = Arc::new(NamespaceOpens::new(factory.clone(), &Metrics::new(&admin)));

    let handles: Vec<_> = (0..10)
        .map(|_| spawn_open(&opens, "cell-a", "ns"))
        .collect();
    for handle in handles {
        assert!(matches!(
            handle.await.unwrap(),
            Err(StorageError::Unavailable(_))
        ));
    }
    assert_eq!(factory.opens.load(Ordering::Relaxed), 1);

    factory.down.store(false, Ordering::Relaxed);
    opens.open("cell-a", "kv", "ns").await.unwrap();
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cold_open_storm_is_bounded_and_leaves_other_cells_able_to_open() {
    let admin = Admin::new();
    let factory = Arc::new(Factory::default());
    let opens = Arc::new(NamespaceOpens::new(factory.clone(), &Metrics::new(&admin)));

    let storm: Vec<_> = (0..100)
        .map(|i| spawn_open(&opens, "noisy", &format!("ns-{i}")))
        .collect();
    sleep(Duration::from_millis(1)).await;
    let started = monotonic_now();
    opens.open("quiet", "kv", "mine").await.unwrap();
    assert!(
        started.elapsed() <= OP + TICK,
        "the quiet cell did not wait"
    );

    let mut rejected = 0;
    for open in storm {
        match open.await.unwrap() {
            Ok(_) => {}
            Err(StorageError::Overloaded(_)) => rejected += 1,
            Err(error) => panic!("overload must stay retryable: {error}"),
        }
    }
    let admitted = NAMESPACE_OPENS.per_cell + NAMESPACE_OPENS.queue;
    assert_eq!(rejected, 100 - admitted);
    assert!(factory.peak.load(Ordering::Relaxed) <= NAMESPACE_OPENS.total);
}
