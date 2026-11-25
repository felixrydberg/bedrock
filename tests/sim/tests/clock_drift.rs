//! #124 / S-16: the vendored simulator's per-node clock rate (`TimeHandle::set_clock_rate`, marked `bedrock:` in
//! `vendor/madsim`). A node's `Instant`, sleeps, intervals and timeouts follow its clock; wall time and the network keep
//! simulated time.
#![cfg(madsim)]

use std::time::{Duration, SystemTime};

use madsim::runtime::{Handle, Runtime};
use madsim::time::TimeHandle;

#[test]
#[allow(clippy::disallowed_methods)] // The simulator's own clocks are what this checks.
fn a_slow_nodes_timers_follow_its_clock_and_wall_time_does_not() {
    let runtime = Runtime::with_seed_and_config(1, madsim::Config::default());
    runtime.block_on(async {
        let node = Handle::current().create_node().name("slow").build();
        TimeHandle::current().set_clock_rate(node.id(), 0.8);
        let start = tokio::time::Instant::now();
        let wall = SystemTime::now();
        let local = node
            .spawn(async {
                let t0 = tokio::time::Instant::now();
                tokio::time::sleep(Duration::from_secs(4)).await;
                let slept = t0.elapsed();
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                for _ in 0..3 {
                    interval.tick().await;
                }
                let timed_out =
                    tokio::time::timeout(Duration::from_secs(1), std::future::pending::<()>())
                        .await
                        .is_err();
                (slept, t0.elapsed(), timed_out)
            })
            .await
            .unwrap();
        let (slept, local_total, timed_out) = local;
        let simulated = start.elapsed();
        assert!(timed_out);
        // 4 s on the node's clock, then two more interval ticks and a 1 s timeout: 7 s it reads, 8.75 s simulated.
        assert!(slept >= Duration::from_secs(4) && slept < Duration::from_millis(4010));
        assert!(local_total >= Duration::from_secs(7) && local_total < Duration::from_millis(7020));
        assert!(
            simulated >= Duration::from_millis(8750) && simulated < Duration::from_millis(8800),
            "{simulated:?}"
        );
        let walled = SystemTime::now().duration_since(wall).unwrap();
        assert!(
            walled.abs_diff(simulated) < Duration::from_millis(1),
            "wall time drifted"
        );
    });
}
