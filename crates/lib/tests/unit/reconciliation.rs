use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// What an interrupted repair leaves behind before finishing.
const PARTIAL: u32 = u32::MAX;

/// Desired and actual are key → value maps; a repair sets the desired value or deletes an extra key.
#[derive(Default)]
struct Fake {
    desired: Mutex<BTreeMap<u32, u32>>,
    actual: Mutex<BTreeMap<u32, u32>>,
    /// Keys repaired, in order, including failed attempts.
    repaired: Mutex<Vec<u32>>,
    /// Key → failures left before its repair succeeds.
    fail: Mutex<BTreeMap<u32, u32>>,
    /// Repairs write [`PARTIAL`] and never finish, as if the process died mid-repair.
    hang: AtomicBool,
}

impl Fake {
    fn new(desired: &[(u32, u32)], actual: &[(u32, u32)]) -> Arc<Self> {
        Arc::new(Fake {
            desired: Mutex::new(desired.iter().copied().collect()),
            actual: Mutex::new(actual.iter().copied().collect()),
            ..Fake::default()
        })
    }

    fn converged(&self) -> bool {
        *self.desired.lock().unwrap() == *self.actual.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl Reconcile for Fake {
    type Key = u32;
    type Desired = u32;
    type Actual = u32;
    type Repair = Option<u32>;
    type Error = String;

    async fn desired(&self) -> Result<BTreeMap<u32, u32>, String> {
        Ok(self.desired.lock().unwrap().clone())
    }

    async fn actual(&self) -> Result<BTreeMap<u32, u32>, String> {
        Ok(self.actual.lock().unwrap().clone())
    }

    fn plan(
        &self,
        _: &u32,
        desired: Option<&u32>,
        actual: Option<&u32>,
    ) -> Option<(u8, Option<u32>)> {
        // Deletes run after sets.
        (desired != actual).then(|| (u8::from(desired.is_none()), desired.copied()))
    }

    async fn repair(&self, key: &u32, repair: &Option<u32>) -> Result<(), String> {
        self.repaired.lock().unwrap().push(*key);
        if let Some(left) = self
            .fail
            .lock()
            .unwrap()
            .get_mut(key)
            .filter(|left| **left > 0)
        {
            *left -= 1;
            return Err("injected".into());
        }
        let Some(value) = repair else {
            self.actual.lock().unwrap().remove(key);
            return Ok(());
        };
        if self.hang.load(Ordering::SeqCst) {
            self.actual.lock().unwrap().insert(*key, PARTIAL);
            std::future::pending::<()>().await;
        }
        self.actual.lock().unwrap().insert(*key, *value);
        Ok(())
    }
}

fn config(burst: u32) -> Config {
    Config {
        burst,
        refill: Duration::from_secs(1),
        repair_timeout: Duration::from_secs(5),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(4),
        seed: 7,
    }
}

fn reconciler(fake: &Arc<Fake>, burst: u32) -> (Arc<Admin>, Reconciler<Fake>) {
    let admin = Admin::new();
    let reconciler = Reconciler::new("test", fake.clone(), config(burst), &Metrics::new(&admin));
    (admin, reconciler)
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn converges_by_priority_then_does_nothing() {
    let fake = Fake::new(&[(1, 10), (2, 20), (3, 30)], &[(0, 1), (2, 99)]);
    let (admin, mut reconciler) = reconciler(&fake, 10);

    let pass = reconciler.pass().await.unwrap();
    assert_eq!((pass.pending, pass.repaired, pass.failed), (4, 4, 0));
    assert!(fake.converged());
    assert_eq!(*fake.repaired.lock().unwrap(), [1, 2, 3, 0]);

    let pass = reconciler.pass().await.unwrap();
    assert_eq!(pass, Pass::default());
    assert_eq!(fake.repaired.lock().unwrap().len(), 4);
    let metrics = admin.render();
    assert!(
        metrics.contains(r#"reconcile_pending{reconciler="test"} 0"#),
        "{metrics}"
    );
    assert!(
        metrics.contains(r#"reconcile_repairs_total{reconciler="test",outcome="ok"} 4"#),
        "{metrics}"
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn repairs_are_rate_limited() {
    let desired: Vec<_> = (0..5).map(|k| (k, k)).collect();
    let fake = Fake::new(&desired, &[]);
    let (_admin, mut reconciler) = reconciler(&fake, 2);

    assert_eq!(reconciler.pass().await.unwrap().repaired, 2);
    assert_eq!(reconciler.pass().await.unwrap().repaired, 0);
    sleep(Duration::from_secs(1)).await;
    assert_eq!(reconciler.pass().await.unwrap().repaired, 1);
    // Idle time refills only up to the burst.
    sleep(Duration::from_secs(10)).await;
    let pass = reconciler.pass().await.unwrap();
    assert_eq!((pass.pending, pass.repaired), (2, 2));
    assert!(fake.converged());
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn failed_repairs_back_off_and_retry() {
    let fake = Fake::new(&[(1, 1), (2, 2)], &[]);
    fake.fail.lock().unwrap().insert(1, 2);
    let (admin, mut reconciler) = reconciler(&fake, 10);

    let pass = reconciler.pass().await.unwrap();
    assert_eq!((pass.repaired, pass.failed), (1, 1));
    assert!(
        admin
            .render()
            .contains(r#"reconcile_failing{reconciler="test"} 1"#)
    );

    // Jitter keeps the first retry within [½, 1) of the base backoff.
    sleep(Duration::from_millis(400)).await;
    assert_eq!(
        reconciler.pass().await.unwrap(),
        Pass {
            pending: 1,
            ..Pass::default()
        }
    );
    sleep(Duration::from_millis(600)).await;
    assert_eq!(reconciler.pass().await.unwrap().failed, 1);
    sleep(Duration::from_secs(2)).await;
    assert_eq!(reconciler.pass().await.unwrap().repaired, 1);
    assert!(fake.converged());
    assert_eq!(*fake.repaired.lock().unwrap(), [1, 2, 1, 1]);
    assert!(
        admin
            .render()
            .contains(r#"reconcile_failing{reconciler="test"} 0"#)
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn timed_out_repairs_fail_and_retry_after_backoff() {
    let fake = Fake::new(&[(1, 10)], &[]);
    fake.hang.store(true, Ordering::SeqCst);
    let (admin, mut reconciler) = reconciler(&fake, 10);
    reconciler.config.repair_timeout = Duration::from_millis(100);

    let pass = reconciler.pass().await.unwrap();
    assert_eq!((pass.pending, pass.repaired, pass.failed), (1, 0, 1));
    assert!(
        admin
            .render()
            .contains(r#"reconcile_repairs_total{reconciler="test",outcome="failed"} 1"#)
    );
    assert!(
        admin
            .render()
            .contains(r#"reconcile_failing{reconciler="test"} 1"#)
    );

    fake.hang.store(false, Ordering::SeqCst);
    sleep(Duration::from_secs(1)).await;
    assert_eq!(reconciler.pass().await.unwrap().repaired, 1);
    assert!(fake.converged());
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn backoff_is_seeded_exponential_and_capped() {
    let fake = Fake::new(&[], &[]);
    let (_, mut a) = reconciler(&fake, 1);
    let (_, mut b) = reconciler(&fake, 1);
    for attempts in 1..=5 {
        let delay = a.backoff(attempts);
        assert_eq!(delay, b.backoff(attempts));
        let full = Duration::from_secs(1 << (attempts - 1).min(2));
        assert!(delay >= full / 2 && delay < full, "{attempts}: {delay:?}");
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn repair_interrupted_by_restart_completes_on_next_run() {
    let fake = Fake::new(&[(1, 10)], &[]);
    fake.hang.store(true, Ordering::SeqCst);
    let (_, mut first) = reconciler(&fake, 10);
    let task = tokio::spawn(async move { first.pass().await });
    while fake.actual.lock().unwrap().get(&1) != Some(&PARTIAL) {
        tokio::task::yield_now().await;
    }
    task.abort();

    fake.hang.store(false, Ordering::SeqCst);
    let (_, mut restarted) = reconciler(&fake, 10);
    assert_eq!(restarted.pass().await.unwrap().repaired, 1);
    assert!(fake.converged());
    assert_eq!(restarted.pass().await.unwrap(), Pass::default());
}
