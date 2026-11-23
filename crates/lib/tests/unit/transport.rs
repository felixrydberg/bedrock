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

/// Answers `Get` after `min_revision` milliseconds; says when a call entered and records when one finished.
#[cfg(not(madsim))]
struct Slow {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    finished: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(not(madsim))]
#[tonic::async_trait]
impl crate::replication::pb::reads_server::Reads for Slow {
    async fn get(
        &self,
        request: tonic::Request<crate::replication::pb::GetRequest>,
    ) -> Result<tonic::Response<crate::replication::pb::GetReply>, Status> {
        let _ = self.entered.send(());
        let millis = request.into_inner().min_revision;
        tokio::time::sleep(Duration::from_millis(millis)).await;
        self.finished
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(tonic::Response::new(crate::replication::pb::GetReply {
            revision: millis,
            value: None,
        }))
    }

    async fn list(
        &self,
        _: tonic::Request<crate::replication::pb::ListRequest>,
    ) -> Result<tonic::Response<crate::replication::pb::ListReply>, Status> {
        Err(Status::unimplemented("list"))
    }
}

/// A listener serving [`Slow`] on a free local port, a `Get` to it that takes `millis`, and whether its handler finished.
/// Returns once the handler has the call.
#[cfg(not(madsim))]
async fn slow_call(
    millis: u64,
) -> (
    Listener,
    std::net::SocketAddr,
    tokio::task::JoinHandle<Result<u64, Status>>,
    Arc<std::sync::atomic::AtomicBool>,
) {
    let addr = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap()
    };
    let (entered, mut handling) = tokio::sync::mpsc::unbounded_channel();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slow = Slow {
        entered,
        finished: finished.clone(),
    };
    let mut listener = serve(
        server(None).add_service(crate::replication::pb::reads_server::ReadsServer::new(slow)),
        addr,
        None,
    );
    let call = tokio::spawn(async move {
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let mut attempts = 0;
        let channel = loop {
            match endpoint.connect().await {
                Ok(channel) => break channel,
                Err(error) if attempts == 100 => {
                    return Err(Status::unavailable(error.to_string()));
                }
                Err(_) => attempts += 1,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let reply = crate::replication::pb::reads_client::ReadsClient::new(channel)
            .get(crate::replication::pb::GetRequest {
                min_revision: millis,
                ..Default::default()
            })
            .await?;
        Ok(reply.into_inner().revision)
    });
    tokio::select! {
        served = &mut listener => panic!("the listener stopped: {served:?}"),
        entered = handling.recv() => entered.expect("the call reached the handler"),
    }
    (listener, addr, call, finished)
}

#[cfg(not(madsim))]
#[tokio::test]
async fn close_lets_accepted_rpcs_finish_and_refuses_new_connections() {
    let (listener, addr, call, finished) = slow_call(700).await;
    let start = crate::monotonic_now();
    listener.close().await;
    assert!(
        finished.load(std::sync::atomic::Ordering::SeqCst),
        "close waited for the accepted call's handler"
    );
    assert!(start.elapsed() < DRAIN, "closed once the RPC finished");
    assert_eq!(call.await.unwrap().unwrap(), 700);
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn close_cuts_off_rpcs_still_running_after_the_drain() {
    let (listener, _, call, finished) = slow_call(60_000).await;
    let start = crate::monotonic_now();
    listener.close().await;
    let waited = start.elapsed();
    assert!(
        waited >= DRAIN && waited < DRAIN + Duration::from_secs(2),
        "{waited:?}"
    );
    assert!(!finished.load(std::sync::atomic::Ordering::SeqCst));
    // The handler's task ends with the runtime; the caller's call has not succeeded.
    assert!(!call.is_finished() || call.await.unwrap().is_err());
}
