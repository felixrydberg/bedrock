use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bedrock_lib::placement::Owner;
use bedrock_lib::storage::memory::MemoryEngine;

use super::*;

struct Here(bool);

#[async_trait]
impl cold::Placement for Here {
    async fn first_here(&self, _: &pb::Actor) -> Result<bool, Status> {
        Ok(self.0)
    }
    async fn truncate_others(&self, _: &pb::Actor, _: u64) -> Result<bool, Status> {
        Ok(true)
    }
    async fn first_cell(&self, _: &pb::Actor) -> Result<String, Status> {
        Ok("cell".into())
    }
}

#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<(String, u64)>>,
    hang: AtomicBool,
}

#[async_trait]
impl Wake for Recorder {
    async fn wake(&self, actor: pb::Actor, at_ms: u64) -> Result<(), Status> {
        self.calls.lock().unwrap().push((actor.id, at_ms));
        if self.hang.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

fn actor(i: usize) -> pb::Actor {
    const DIGITS: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let head: String = (0..4)
        .map(|d| DIGITS[(i >> (5 * (3 - d))) & 31] as char)
        .collect();
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{head}{}a", "c".repeat(21)),
        kind: 0,
    }
}

fn setup(first: bool, count: usize) -> (Scanner, Arc<Recorder>, Directory) {
    let store = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    store.hold(0, 0, 0).unwrap();
    for i in 0..count {
        store
            .import(&Actor::new(&actor(i)).unwrap(), 1, 1, Some(1000), 0)
            .unwrap();
    }
    let wake = Arc::new(Recorder::default());
    let scanner = Scanner::new(
        store,
        Arc::new(Here(first)),
        wake.clone(),
        "node",
        Arc::new(|_| false),
    );
    (
        scanner,
        wake,
        BTreeMap::from([(
            0,
            vec![Owner {
                node: "node".into(),
                state: State::Ready,
            }],
        )]),
    )
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn primary_retries_success_while_pending_and_fallback_waits_five_seconds() {
    let (mut primary, wake, directory) = setup(true, 1);
    primary.tick_at(&directory, 999).await;
    settle().await;
    assert!(wake.calls.lock().unwrap().is_empty());
    primary.tick_at(&directory, 1000).await;
    settle().await;
    assert_eq!(wake.calls.lock().unwrap().len(), 1);
    // An RPC success is a hint acknowledgement, not a committed handler result.
    primary.tick_at(&directory, 1000).await;
    tokio::time::sleep(RETRY).await;
    primary.tick_at(&directory, 1100).await;
    settle().await;
    assert_eq!(wake.calls.lock().unwrap().len(), 2);

    let (mut fallback, wake, directory) = setup(false, 1);
    fallback.tick_at(&directory, 5999).await;
    settle().await;
    assert!(wake.calls.lock().unwrap().is_empty());
    fallback.tick_at(&directory, 5999).await;
    tokio::time::sleep(RETRY).await;
    fallback.tick_at(&directory, 6000).await;
    settle().await;
    assert_eq!(wake.calls.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn expired_held_alarm_is_delivered_but_live_host_and_incomplete_ranges_are_not() {
    let (mut scanner, wake, directory) = setup(true, 1);
    let wire = actor(0);
    let actor = Actor::new(&wire).unwrap();
    scanner
        .store
        .claim(
            &actor,
            &pb::Holder {
                cell: "cell".into(),
                host: "host".into(),
            },
            2,
            0,
            |_| false,
        )
        .unwrap();
    scanner.lease_live = Arc::new(|_| true);
    scanner.tick_at(&directory, 1000).await;
    settle().await;
    assert!(wake.calls.lock().unwrap().is_empty());
    scanner.lease_live = Arc::new(|_| false);
    let populating = BTreeMap::from([(
        0,
        vec![Owner {
            node: "node".into(),
            state: State::Populating,
        }],
    )]);
    scanner.tick_at(&populating, 1000).await;
    settle().await;
    assert!(wake.calls.lock().unwrap().is_empty());
    scanner.tick_at(&directory, 1000).await;
    settle().await;
    assert_eq!(wake.calls.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn hanging_earlier_alarms_do_not_starve_later_candidates() {
    let (mut scanner, wake, directory) = setup(true, CONCURRENCY + 1);
    wake.hang.store(true, Ordering::Relaxed);
    scanner.tick_at(&directory, 1000).await;
    settle().await;
    assert_eq!(wake.calls.lock().unwrap().len(), CONCURRENCY);
    tokio::time::sleep(TIMEOUT).await;
    settle().await;
    scanner.tick_at(&directory, 6000).await;
    settle().await;
    let calls = wake.calls.lock().unwrap();
    let unique: BTreeSet<_> = calls.iter().map(|(id, _)| id).collect();
    assert_eq!(unique.len(), CONCURRENCY + 1);
}

struct ClaimingWake {
    store: Arc<Store>,
    started: tokio::sync::Notify,
    finish: tokio::sync::Notify,
    completed: AtomicBool,
    calls: AtomicUsize,
}

#[async_trait]
impl Wake for ClaimingWake {
    async fn wake(&self, actor: pb::Actor, _: u64) -> Result<(), Status> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            self.store
                .claim(
                    &Actor::new(&actor).unwrap(),
                    &pb::Holder {
                        cell: "cell".into(),
                        host: "host".into(),
                    },
                    2,
                    0,
                    |_| false,
                )
                .unwrap();
            self.started.notify_one();
            self.finish.notified().await;
            self.completed.store(true, Ordering::Relaxed);
            return Err(Status::unavailable("failed after claiming"));
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn claiming_does_not_cancel_wake_and_failure_retries_despite_the_live_host_lease() {
    let (mut scanner, _, directory) = setup(true, 1);
    scanner.lease_live = Arc::new(|_| true);
    let wake = Arc::new(ClaimingWake {
        store: scanner.store.clone(),
        started: tokio::sync::Notify::new(),
        finish: tokio::sync::Notify::new(),
        completed: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    scanner.wake = wake.clone();
    scanner.tick_at(&directory, 1000).await;
    wake.started.notified().await;
    // Claiming removes the released index entry; this tick must keep the in-flight delivery alive.
    scanner.tick_at(&directory, 1000).await;
    wake.finish.notify_one();
    settle().await;
    assert!(wake.completed.load(Ordering::Relaxed));
    scanner.tick_at(&directory, 1000).await;
    tokio::time::sleep(RETRY).await;
    scanner.tick_at(&directory, 1100).await;
    settle().await;
    assert_eq!(wake.calls.load(Ordering::Relaxed), 2);
    // Successful delivery gives this live holder's timer responsibility again.
    scanner.tick_at(&directory, 1100).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    scanner.tick_at(&directory, 2100).await;
    settle().await;
    assert_eq!(wake.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_wake_retries_after_claiming_on_a_live_host() {
    let (mut scanner, _, directory) = setup(true, 1);
    scanner.lease_live = Arc::new(|_| true);
    let wake = Arc::new(ClaimingWake {
        store: scanner.store.clone(),
        started: tokio::sync::Notify::new(),
        finish: tokio::sync::Notify::new(),
        completed: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    scanner.wake = wake.clone();
    scanner.tick_at(&directory, 1000).await;
    wake.started.notified().await;
    tokio::time::sleep(TIMEOUT).await;
    settle().await;
    scanner.tick_at(&directory, 6000).await;
    tokio::time::sleep(RETRY).await;
    scanner.tick_at(&directory, 6100).await;
    settle().await;
    assert_eq!(wake.calls.load(Ordering::Relaxed), 2);
}
