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

#[test]
fn identities_parse_only_the_exact_form() {
    assert_eq!(
        parse_identity("bedrock://kv/eu-1a/node-7"),
        Some(("kv", "eu-1a", "node-7"))
    );
    for bad in [
        "https://kv/eu-1a/node-7",
        "bedrock://kv/eu-1a",
        "bedrock://kv/eu-1a/node-7/extra",
        "bedrock://kv//node-7",
        "bedrock://kv/eu 1a/node-7",
    ] {
        assert_eq!(parse_identity(bad), None, "{bad}");
    }
}

#[test]
fn hosts_match_by_address_or_name() {
    assert_eq!(host_of("10.0.0.1:7000"), "10.0.0.1");
    assert_eq!(host_of("[::1]:7000"), "::1");
    assert_eq!(host_of("relay.example:7000"), "relay.example");
    let caller = Caller {
        service: "kv".into(),
        cell: "c1".into(),
        node_id: "n1".into(),
        hosts: vec!["::1".into(), "node.example".into()],
    };
    assert!(caller.has_host("0:0:0:0:0:0:0:1"));
    assert!(caller.has_host("NODE.example"));
    assert!(!caller.has_host("10.0.0.1"));
    assert!(caller.require(&["kv", "cdn"], Some("c1")).is_ok());
    assert_eq!(
        caller.require(&["relay"], None).unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        caller.require(&["kv"], Some("c2")).unwrap_err().code(),
        Code::PermissionDenied
    );
}

/// Found by the simulation (#25 / B-23): waits on an idle stream are cancelled by keepalive ticks. A cancelled wait kept the
/// probe, and the next one claimed it again, so a healthy idle upstream stayed out of routing and recovery never reached it.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_cancelled_call_gives_back_its_probe() {
    let peers = Peers::new(&Admin::new());
    peers.health().trip("a");
    tokio::time::sleep(crate::health::BASE_BACKOFF).await;
    let wait = peers.call(
        "a",
        Class::Control,
        Duration::from_secs(30),
        std::future::pending::<Result<(), Status>>(),
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .is_err()
    );
    assert_eq!(peers.health().route(&["a", "b"]), [&"a", &"b"]);

    let answered = peers.call("a", Class::Control, Duration::from_secs(30), async {
        Ok::<_, Status>(())
    });
    answered.await.unwrap();
    assert_eq!(peers.health().state("a"), crate::health::State::Closed);
}
