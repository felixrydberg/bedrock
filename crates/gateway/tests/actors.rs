//! #116 / S-08: the location cache stays bounded and evicts the least recently used actor, and the `Actors` listener
//! admits only actor requests it can route. The routing itself, with redirects, runs in `tests/sim/tests/actor_routing.rs`.
#![cfg(not(madsim))]

use std::sync::Arc;

use bedrock_gateway::actors::{Lru, Router};
use bedrock_gateway::{Gateway, Role};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, Location, targeted};
use bedrock_lib::transport::Peers;
use bedrock_lib::volume::InvokeRequest;
use bedrock_lib::volume::actors_server::Actors;
use tonic::{Code, Request};

#[test]
fn the_location_cache_evicts_the_least_recently_used_actor() {
    let mut cache = Lru::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    assert_eq!(cache.get(&"a"), Some(1));
    cache.insert("c", 3);
    assert_eq!(cache.get(&"b"), None, "b was used least recently");
    assert_eq!((cache.get(&"a"), cache.get(&"c")), (Some(1), Some(3)));
    cache.insert("a", 4);
    assert_eq!((cache.len(), cache.get(&"a")), (2, Some(4)));
    cache.remove(&"a");
    assert_eq!(cache.get(&"a"), None);
    let mut none = Lru::new(0);
    none.insert("a", 1);
    assert!(none.is_empty());
}

fn gateway(admin: &Arc<Admin>, limit: usize) -> Gateway {
    // No relays: nothing in these tests may reach a host.
    let peers = Arc::new(Peers::new(admin));
    let client = Arc::new(Client::new(admin, peers.clone(), Vec::new(), String::new()));
    let home = Location {
        region: "eu".into(),
        dc: "arn1".into(),
        cell: "c1".into(),
    };
    let router = Router::new(admin, client.clone(), peers, home, 16);
    Gateway::new(admin, Role::Cell("c1".into()), client, limit).with_actors(router)
}

fn invoke(id: &str) -> InvokeRequest {
    InvokeRequest {
        actor: Some(bedrock_lib::volume::Actor {
            namespace: "ns".into(),
            id: id.into(),
            kind: 0,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_actors_listener_refuses_requests_it_cannot_route() {
    let admin = Admin::new();
    let gateway = gateway(&admin, 4);
    let code = |result: Result<_, tonic::Status>| result.unwrap_err().code();
    // Forwarded: only compute requests, only for this cell.
    let forwarded = |service, cell| targeted(invoke("eu_x"), service, cell).unwrap();
    assert_eq!(
        code(gateway.invoke(forwarded("kv", "c1")).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(gateway.invoke(forwarded("compute", "c2")).await),
        Code::FailedPrecondition
    );
    // Entering: an actor with a parseable ID.
    assert_eq!(
        code(gateway.invoke(Request::new(InvokeRequest::default())).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(gateway.invoke(Request::new(invoke("no-region"))).await),
        Code::InvalidArgument
    );
    let limited = self::gateway(&Admin::new(), 0);
    assert_eq!(
        code(limited.invoke(Request::new(invoke("eu_x"))).await),
        Code::ResourceExhausted
    );
}

#[tokio::test]
async fn an_actor_event_is_not_sent_again_after_a_failure_that_may_have_dispatched_it() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bedrock_gateway::actors::Dispatch;
    use tonic::Status;

    let sent = AtomicUsize::new(0);
    let send = |status: Option<Status>| {
        let sent = &sent;
        async move {
            sent.fetch_add(1, Ordering::SeqCst);
            status.map_or(Ok(()), Err)
        }
    };
    let dispatch = Dispatch::default();
    // A Gateway that refused before handing the event on: the next Gateway is tried.
    let refused = Status::failed_precondition("this Gateway serves another cell");
    assert!(dispatch.send(send(Some(refused))).await.is_err());
    // A timeout may have reached a host: later sends are refused without being made.
    let timeout = Status::deadline_exceeded("timed out");
    assert!(dispatch.send(send(Some(timeout))).await.is_err());
    let after = dispatch.send(send(None)).await.unwrap_err();
    assert_eq!(after.code(), Code::DeadlineExceeded);
    assert_eq!(sent.load(Ordering::SeqCst), 2);
}
