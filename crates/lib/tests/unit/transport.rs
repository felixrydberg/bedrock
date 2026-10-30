use super::*;
use crate::health::{CONSECUTIVE_FAILURES, State};

#[tokio::test]
async fn peer_failures_feed_the_breaker() {
    let peers = Peers::new(&Admin::new());
    let timeout = Duration::from_secs(1);
    for _ in 0..CONSECUTIVE_FAILURES {
        let _ = peers
            .call::<()>("a", Class::Control, timeout, async {
                Err(Status::unavailable("down"))
            })
            .await;
        // An answer that is an error is not a peer failure.
        let _ = peers
            .call::<()>("b", Class::Control, timeout, async {
                Err(Status::not_found("no key"))
            })
            .await;
    }
    assert_eq!(peers.health().state("a"), State::Open);
    assert_eq!(peers.health().state("b"), State::Closed);
    assert_eq!(peers.health().route(&["a", "b"]), [&"b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn stale_call_cannot_complete_another_calls_probe() {
    let peers = Peers::new(&Admin::new());
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let (finish, wait_finish) = tokio::sync::oneshot::channel();
    let (finished, wait_finished) = tokio::sync::oneshot::channel();
    let stale = async {
        let result = peers
            .call("a", Class::Control, Duration::from_secs(10), async {
                started.send(()).unwrap();
                wait_finish.await.unwrap();
                Ok(())
            })
            .await;
        finished.send(()).unwrap();
        result
    };
    let probe = async {
        wait_started.await.unwrap();
        peers.health().trip("a");
        tokio::time::sleep(crate::health::BASE_BACKOFF).await;
        let result = peers
            .call("a", Class::Control, Duration::from_secs(1), async {
                assert_eq!(peers.health().state("a"), State::HalfOpen);
                finish.send(()).unwrap();
                wait_finished.await.unwrap();
                assert_eq!(peers.health().state("a"), State::HalfOpen);
                Ok(())
            })
            .await;
        assert_eq!(peers.health().state("a"), State::Closed);
        result
    };
    let (stale, probe) = tokio::join!(stale, probe);
    stale.unwrap();
    probe.unwrap();
}
