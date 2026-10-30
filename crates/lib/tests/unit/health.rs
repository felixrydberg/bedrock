use tokio::time::sleep;

use super::*;

const PROBE_TIMEOUT: Duration = Duration::from_millis(100);

fn fail(health: &Health, peer: &str, times: u32) {
    for _ in 0..times {
        let probe = health.begin(peer, PROBE_TIMEOUT);
        health.record(peer, probe, false);
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn failing_peer_is_skipped_within_n_calls() {
    let health = Health::new(&Admin::new());
    fail(&health, "a", CONSECUTIVE_FAILURES - 1);
    assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
    fail(&health, "a", 1);
    assert_eq!(health.state("a"), State::Open);
    assert_eq!(health.route(&["a", "b"]), [&"b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn high_error_rate_trips_without_consecutive_failures() {
    let health = Health::new(&Admin::new());
    // Two failures for every success: never CONSECUTIVE_FAILURES in a row, but well over half.
    for i in 0..WINDOW_MIN_CALLS {
        assert_eq!(
            health.state("a"),
            State::Closed,
            "tripped early at call {i}"
        );
        let probe = health.begin("a", PROBE_TIMEOUT);
        health.record("a", probe, i % 3 == 0);
    }
    assert_eq!(health.state("a"), State::Open);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn recovered_peer_is_used_after_one_probe() {
    let health = Health::new(&Admin::new());
    health.trip("a");
    sleep(BASE_BACKOFF).await;
    assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);

    let probe = health.begin("a", PROBE_TIMEOUT);
    assert_eq!(health.state("a"), State::HalfOpen);
    // Only one probe at a time.
    assert_eq!(health.route(&["a", "b"]), [&"b"]);

    health.record("a", probe, true);
    assert_eq!(health.state("a"), State::Closed);
    assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn failing_peer_is_probed_at_decreasing_rate() {
    let health = Health::new(&Admin::new());
    health.trip("a");
    let tick = Duration::from_millis(1);
    for secs in [1, 2, 4, 8, 16, 32, 60, 60] {
        sleep(Duration::from_secs(secs) - tick).await;
        assert_eq!(health.route(&["a", "b"]), [&"b"], "probed before {secs}s");
        sleep(tick).await;
        assert_eq!(
            health.route(&["a", "b"]),
            [&"a", &"b"],
            "not probed at {secs}s"
        );
        fail(&health, "a", 1);
        assert_eq!(health.state("a"), State::Open);
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn abandoned_probe_is_retried() {
    let health = Health::new(&Admin::new());
    health.trip("a");
    sleep(BASE_BACKOFF).await;
    let _ = health.begin("a", PROBE_TIMEOUT);
    assert_eq!(health.route(&["a", "b"]), [&"b"]);
    sleep(PROBE_TIMEOUT).await;
    assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn only_the_active_probe_can_resolve_half_open() {
    for ok in [true, false] {
        for interrupted in [true, false] {
            let health = Health::new(&Admin::new());
            let closed_call = health.begin("a", PROBE_TIMEOUT);
            health.trip("a");
            let open_call = health.begin("a", PROBE_TIMEOUT);
            sleep(BASE_BACKOFF).await;
            let old_probe = health.begin("a", PROBE_TIMEOUT);
            assert!(old_probe.is_some());
            let concurrent_call = health.begin("a", PROBE_TIMEOUT);

            for call in [closed_call, open_call, concurrent_call] {
                assert!(call.is_none());
                health.record("a", call, ok);
                assert_eq!(health.state("a"), State::HalfOpen);
            }

            if interrupted {
                health.trip("a");
                sleep(BASE_BACKOFF * 2).await;
            } else {
                sleep(PROBE_TIMEOUT).await;
            }
            let active_probe = health.begin("a", PROBE_TIMEOUT);
            assert!(active_probe.is_some());
            health.record("a", old_probe, ok);
            assert_eq!(health.state("a"), State::HalfOpen);
            assert_eq!(health.route(&["a", "b"]), [&"b"]);

            health.record("a", active_probe, ok);
            assert_eq!(
                health.state("a"),
                if ok { State::Closed } else { State::Open }
            );
        }
    }
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn every_candidate_open_fails_open() {
    let admin = Admin::new();
    let health = Health::new(&admin);
    health.trip("a");
    health.trip("b");
    assert_eq!(health.route(&["a", "b"]), [&"a", &"b"]);
    assert!(health.route::<&str>(&[]).is_empty());

    let metrics = admin.render();
    assert!(
        metrics.contains("bedrock_health_fail_open_total 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains(r#"bedrock_health_breaker_transitions_total{to="Open"} 2"#),
        "{metrics}"
    );
}
