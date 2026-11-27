//! Registry contract tests and the real transport boundary, included by the owner to access private lifecycle state.
#![cfg(not(madsim))]

use super::*;
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, Location};
use bedrock_lib::transport::Peers;
use pb::sockets_server::Sockets as _;

fn owner(limits: Limits) -> Arc<Sockets> {
    let admin = Admin::new();
    let peers = Arc::new(Peers::new(&admin));
    let client = Arc::new(Client::new(
        &admin,
        peers.clone(),
        Vec::new(),
        String::new(),
    ));
    let home = Location {
        region: "eu".into(),
        dc: "arn".into(),
        cell: "c1".into(),
    };
    let router = Arc::new(Router::new(&admin, client, peers, home, 16));
    Arc::new(
        Sockets::new(
            Member {
                node_id: "gateway".into(),
                service: "gateway".into(),
                address: "127.0.0.1:1".into(),
                region: "eu".into(),
                dc: "arn".into(),
                cell: "c1".into(),
                ..Default::default()
            },
            router,
            limits,
        )
        .unwrap(),
    )
}

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: "eu_actor".into(),
        kind: 0,
    }
}

fn register(owner: &Sockets, digit: char) -> (String, Arc<Notify>, watch::Receiver<bool>) {
    let id = digit.to_string().repeat(32);
    let (stop, stopped) = watch::channel(false);
    let notify = Arc::new(Notify::new());
    owner.state().connections.insert(
        id.clone(),
        Connection {
            actor: (actor().namespace, actor().id),
            attachment: Vec::new(),
            last: (0, 0),
            closing: false,
            outgoing: VecDeque::new(),
            bytes: 0,
            messages: 0,
            notify: notify.clone(),
            stop,
        },
    );
    (id, notify, stopped)
}

fn effect(id: &str, action: pb::socket_effect::Action) -> pb::SocketEffect {
    pb::SocketEffect {
        id: id.into(),
        action: Some(action),
    }
}

async fn apply(
    owner: &Sockets,
    epoch: u64,
    event: u64,
    effects: Vec<pb::SocketEffect>,
) -> Vec<pb::SocketResult> {
    owner
        .apply(Request::new(pb::SocketApply {
            actor: Some(actor()),
            epoch,
            event,
            effects,
        }))
        .await
        .unwrap()
        .into_inner()
        .results
}

async fn lookup(owner: &Sockets, id: &str) -> pb::SocketSnapshot {
    owner
        .lookup(Request::new(pb::SocketLookup {
            actor: Some(actor()),
            ids: vec![id.into()],
        }))
        .await
        .unwrap()
        .into_inner()
        .sockets
        .remove(0)
}

#[tokio::test]
async fn callbacks_are_actor_bound_ordered_and_attachments_end_with_the_connection() {
    let owner = owner(Limits::default());
    let (id, _, _) = register(&owner, 'a');
    let attachment =
        |bytes: &[u8]| effect(&id, pb::socket_effect::Action::Attachment(bytes.to_vec()));
    assert_eq!(
        apply(&owner, 2, 3, vec![attachment(b"current")]).await[0].outcome,
        pb::SocketOutcome::Accepted as i32
    );
    for (epoch, event) in [(2, 3), (1, 99), (2, 2)] {
        assert_eq!(
            apply(&owner, epoch, event, vec![attachment(b"stale")]).await[0].outcome,
            pb::SocketOutcome::Stale as i32
        );
    }
    let mut wrong_actor = actor();
    wrong_actor.namespace = "other".into();
    let answer = owner
        .apply(Request::new(pb::SocketApply {
            actor: Some(wrong_actor),
            epoch: 3,
            event: 1,
            effects: vec![attachment(b"wrong")],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(answer.results[0].outcome, pb::SocketOutcome::Missing as i32);
    assert_eq!(lookup(&owner, &id).await.attachment, b"current");
    owner.remove(&id);
    let absent = lookup(&owner, &id).await;
    assert_eq!(absent.outcome, pb::SocketOutcome::Missing as i32);
    assert!(absent.attachment.is_empty());
}

#[tokio::test]
async fn effects_validate_before_mutation_and_overflow_only_closes_the_full_connection() {
    let limits = Limits {
        queue_messages: 1,
        ..Limits::default()
    };
    let owner = owner(limits);
    let (full, _, mut stopped) = register(&owner, 'a');
    let (healthy, _, _) = register(&owner, 'b');
    let send = |id: &str| {
        effect(
            id,
            pb::socket_effect::Action::Send(pb::SocketFrame {
                data: b"hello".to_vec(),
                text: true,
            }),
        )
    };
    let invalid = owner
        .apply(Request::new(pb::SocketApply {
            actor: Some(actor()),
            epoch: 1,
            event: 1,
            effects: vec![
                send(&full),
                effect(
                    &healthy,
                    pb::socket_effect::Action::Close(pb::SocketClose {
                        code: 1005,
                        reason: String::new(),
                    }),
                ),
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
    let after_close = owner
        .apply(Request::new(pb::SocketApply {
            actor: Some(actor()),
            epoch: 1,
            event: 1,
            effects: vec![
                effect(
                    &full,
                    pb::socket_effect::Action::Close(pb::SocketClose {
                        code: 1000,
                        reason: String::new(),
                    }),
                ),
                send(&full),
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(after_close.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        apply(&owner, 1, 1, vec![send(&full)]).await[0].outcome,
        pb::SocketOutcome::Accepted as i32
    );
    let results = apply(&owner, 1, 2, vec![send(&full), send(&healthy)]).await;
    assert_eq!(results[0].outcome, pb::SocketOutcome::Overloaded as i32);
    assert_eq!(results[1].outcome, pb::SocketOutcome::Accepted as i32);
    stopped.changed().await.unwrap();
    assert!(*stopped.borrow());
    assert_eq!(
        lookup(&owner, &full).await.outcome,
        pb::SocketOutcome::Missing as i32
    );
    assert_eq!(
        lookup(&owner, &healthy).await.outcome,
        pb::SocketOutcome::Accepted as i32
    );
}

#[tokio::test]
async fn authenticated_callbacks_reject_other_services_and_requests_are_bounded() {
    let owner = owner(Limits {
        roster: 4096,
        attachment_bytes: 64 * 1024,
        ..Limits::default()
    });
    let mut request = Request::new(pb::SocketLookup {
        actor: Some(actor()),
        ids: vec![],
    });
    request.extensions_mut().insert(Caller {
        service: "gateway".into(),
        cell: "c1".into(),
        node_id: "other".into(),
        hosts: vec![],
    });
    assert_eq!(
        owner.lookup(request).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let excessive = Request::new(pb::SocketLookup {
        actor: Some(actor()),
        ids: vec!["a".repeat(32); owner.limits.roster + 1],
    });
    assert_eq!(
        owner.lookup(excessive).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    let (id, _, _) = register(&owner, 'd');
    let _ = apply(
        &owner,
        1,
        1,
        vec![effect(
            &id,
            pb::socket_effect::Action::Attachment(vec![0; 64 * 1024]),
        )],
    )
    .await;
    let amplified = Request::new(pb::SocketLookup {
        actor: Some(actor()),
        ids: vec![id; owner.limits.roster],
    });
    assert_eq!(
        owner.lookup(amplified).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
}

#[tokio::test]
async fn real_socket_receives_queued_frames_and_close_releases_the_transport() {
    let owner = owner(Limits::default());
    let (id, notify, stopped) = register(&owner, 'c');
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connecting = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
    let (accepted, connected) = tokio::join!(listener.accept(), connecting);
    let (server, _) = accepted.unwrap();
    let mut client = WebSocketStream::from_raw_socket(connected.unwrap(), Role::Client, None).await;
    let connection = pb::SocketConnection {
        id: id.clone(),
        gateway: Some(owner.owner.clone()),
    };
    let running_owner = owner.clone();
    let task = tokio::spawn(async move {
        let _ = running_owner
            .run(actor(), connection, server, notify, stopped)
            .await;
        running_owner.remove(&id);
    });
    let id = "c".repeat(32);
    let results = apply(
        &owner,
        1,
        1,
        vec![
            effect(
                &id,
                pb::socket_effect::Action::Send(pb::SocketFrame {
                    data: b"awake output".to_vec(),
                    text: true,
                }),
            ),
            effect(
                &id,
                pb::socket_effect::Action::Close(pb::SocketClose {
                    code: 1000,
                    reason: "done".into(),
                }),
            ),
        ],
    )
    .await;
    assert!(
        results
            .iter()
            .all(|r| r.outcome == pb::SocketOutcome::Accepted as i32)
    );
    let received = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(received, Message::Text("awake output".into()));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_close()
    );
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        lookup(&owner, &id).await.outcome,
        pb::SocketOutcome::Missing as i32
    );
}
