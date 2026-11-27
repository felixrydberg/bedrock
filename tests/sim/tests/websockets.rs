//! #123 / S-15: real sockets, Gateway routing/callbacks and quorum-backed actor recovery.
//! The test executor exercises the production Instance seam while S-10 supplies customer code.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::actors::sockets::Context;
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::runtime::{Error, Event, ExecutionSpec, Executor, Instance};
use bedrock_lib::volume as pb;
use bedrock_lib::websockets::Limits;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::Notify;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

#[path = "support/websockets.rs"]
mod support;
use support::{Fixture, actor};

type Socket = WebSocketStream<tokio::net::TcpStream>;
#[derive(Default)]
struct Backend {
    gate_open: AtomicBool,
    entered: Notify,
    resume: Notify,
    hangs: AtomicUsize,
    messages: AtomicUsize,
    hold_message: AtomicBool,
    opened: Mutex<Vec<String>>,
    trace: Mutex<Vec<String>>,
}
struct Object {
    storage: Storage,
    backend: Arc<Backend>,
}
struct ExecutorAdapter(Arc<Backend>);
#[async_trait]
impl Executor for ExecutorAdapter {
    async fn start(&self, spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error> {
        let ExecutionSpec::Actor {
            storage,
            actor: target,
        } = spec
        else {
            panic!("socket created a stateless Worker")
        };
        assert_eq!(target, actor());
        self.0
            .trace
            .lock()
            .unwrap()
            .push(format!("start:{}", target.id));
        Ok(Box::new(Object {
            storage,
            backend: self.0.clone(),
        }))
    }
}
#[async_trait]
impl Instance for Object {
    async fn invoke(&mut self, _: Vec<u8>) -> Result<Vec<u8>, Error> {
        unreachable!("typed event seam")
    }
    async fn event(&mut self, event: Event, sockets: &mut Context<'_>) -> Result<Vec<u8>, Error> {
        let observed = match &event {
            Event::Socket(event) => format!(
                "socket:{}:{}:{:?}",
                event.kind,
                event.connection.as_ref().unwrap().id,
                event.frame.as_ref().map(|frame| &frame.data)
            ),
            Event::Alarm(_) => "alarm".to_owned(),
            Event::Request(command) if command.starts_with(b"alarm:") => {
                "schedule-alarm".to_owned()
            }
            Event::Request(command) => format!("request:{command:?}"),
        };
        self.backend.trace.lock().unwrap().push(observed);
        if let Event::Socket(event) = &event {
            let connection = event.connection.as_ref().unwrap();
            match pb::socket_event::Kind::try_from(event.kind).unwrap() {
                pb::socket_event::Kind::Open => {
                    self.backend
                        .opened
                        .lock()
                        .unwrap()
                        .push(connection.id.clone());
                    if self.backend.gate_open.swap(false, Ordering::SeqCst) {
                        self.backend.entered.notify_one();
                        self.backend.resume.notified().await;
                    }
                    return Ok(Vec::new());
                }
                pb::socket_event::Kind::Message => {
                    let data = &event.frame.as_ref().unwrap().data;
                    self.backend.messages.fetch_add(1, Ordering::SeqCst);
                    if self.backend.hold_message.swap(false, Ordering::SeqCst) {
                        self.backend.entered.notify_one();
                        self.backend.resume.notified().await;
                        return Ok(Vec::new());
                    }
                    sockets.attachment(&connection.id, data.clone())?;
                    sockets.send(&connection.id, [b"set:".as_slice(), data].concat(), true)?;
                    return Ok(Vec::new());
                }
                _ => return Ok(Vec::new()),
            }
        }
        let connections = sockets.connections().await?;
        self.backend.trace.lock().unwrap().push(format!(
            "owners:{:?}",
            connections
                .iter()
                .map(|connection| (
                    &connection.reference.id,
                    connection.outcome as i32,
                    &connection.attachment
                ))
                .collect::<Vec<_>>()
        ));
        match event {
            Event::Alarm(_) => {
                self.storage.commit_with_alarm(Vec::new(), None).await?;
                for connection in connections {
                    sockets.send(
                        &connection.reference.id,
                        [b"alarm:".as_slice(), &connection.attachment].concat(),
                        true,
                    )?;
                }
            }
            Event::Request(command) if command == b"snapshot" => {
                return Ok(pb::SocketSnapshots {
                    sockets: connections
                        .into_iter()
                        .map(|connection| pb::SocketSnapshot {
                            id: connection.reference.id,
                            outcome: connection.outcome as i32,
                            attachment: connection.attachment,
                        })
                        .collect(),
                }
                .encode_to_vec());
            }
            Event::Request(command) if command.starts_with(b"alarm:") => {
                let at = u64::from_be_bytes(command[6..].try_into().unwrap());
                self.storage.commit_with_alarm(Vec::new(), Some(at)).await?;
            }
            Event::Request(command) => {
                for connection in &connections {
                    let data = if command == b"fail"
                        || command == b"hang"
                        || command == b"fence"
                        || command == b"stop"
                    {
                        b"poison".as_slice()
                    } else {
                        &command
                    };
                    if command == b"overflow" && connection.attachment == b"B" {
                        // One actual callback batch exceeds B's queue before its writer can drain it.
                        for _ in 0..3 {
                            sockets.send(&connection.reference.id, b"slow".to_vec(), true)?;
                        }
                    } else {
                        sockets.send(&connection.reference.id, data.to_vec(), true)?;
                    }
                }
                if command == b"fail" {
                    return Err(Error::Handler("rejected after staging".into()));
                }
                if command == b"hang" {
                    self.backend.hangs.fetch_add(1, Ordering::SeqCst);
                    self.backend.entered.notify_one();
                    std::future::pending::<()>().await;
                }
                if command == b"fence" {
                    self.backend.entered.notify_one();
                    self.backend.resume.notified().await;
                    self.storage
                        .commit(vec![pb::Change {
                            key: b"fenced-commit".to_vec(),
                            value: Some(vec![1]),
                        }])
                        .await?;
                }
                if command == b"stop" {
                    self.backend.entered.notify_one();
                    self.backend.resume.notified().await;
                }
            }
            Event::Socket(_) => unreachable!(),
        }
        Ok(Vec::new())
    }
}

async fn connect(address: &str) -> Socket {
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    // A literal RFC 6455 example nonce keeps the upgrade transcript seed-independent.
    let request = hyper::Request::builder()
        .uri(format!("ws://{address}/object"))
        .header("Host", address)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    assert_eq!(response.status(), 101);
    socket
}
async fn receive(socket: &mut Socket, expected: &str) {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .expect("Gateway did not deliver a frame")
        .expect("socket closed")
        .unwrap();
    assert_eq!(message, Message::Text(expected.into()));
}
async fn set(socket: &mut Socket, value: &str) {
    socket.send(Message::Text(value.into())).await.unwrap();
    receive(socket, &format!("set:{value}")).await;
}
async fn snapshot(fixture: &Fixture, holder: usize) -> Vec<pb::SocketSnapshot> {
    let payload = fixture.hosts[holder]
        .acquire(&actor())
        .await
        .unwrap()
        .invoke(b"snapshot".to_vec())
        .await
        .unwrap();
    pb::SocketSnapshots::decode(payload.as_slice())
        .unwrap()
        .sockets
}
async fn invoke(fixture: &Fixture, holder: usize, command: &[u8]) -> Result<Vec<u8>, Error> {
    fixture.hosts[holder]
        .acquire(&actor())
        .await
        .unwrap()
        .invoke(command.to_vec())
        .await
}
async fn wait_asleep(fixture: &Fixture, holder: usize) {
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(12);
    while fixture.hosts[holder].awake().await != 0 && bedrock_lib::monotonic_now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        fixture.hosts[holder].awake().await,
        0,
        "hibernatable sockets pinned the actor"
    );
}
fn accepted(sockets: &[pb::SocketSnapshot]) -> usize {
    sockets
        .iter()
        .filter(|s| s.outcome == pb::SocketOutcome::Accepted as i32)
        .count()
}

async fn scenario() -> Vec<String> {
    let backend = Arc::new(Backend::default());
    backend.gate_open.store(true, Ordering::SeqCst);
    let limits = Limits {
        queue_messages: 2,
        callback_ms: 200,
        ..Limits::default()
    };
    let fixture = Fixture::new(Arc::new(ExecutorAdapter(backend.clone())), limits).await;
    let first_address = fixture.gateways[0].address.clone();
    let opening = tokio::spawn(async move { connect(&first_address).await });
    backend.entered.notified().await;
    assert!(
        !opening.is_finished(),
        "HTTP 101 preceded the Open handler/output gate"
    );
    backend.resume.notify_one();
    let mut a = opening.await.unwrap();
    let first_id = backend.opened.lock().unwrap()[0].clone();
    // An independent volume-log oracle: at least two fsynced slots hold the roster value at acceptance.
    let target = bedrock_volume::store::Actor::new(&actor()).unwrap();
    let copies = fixture
        .stores
        .iter()
        .filter(|store| {
            let (answer, _, _) = store.read(&target, &[], 0).unwrap();
            let slot = answer.slot.unwrap();
            let Some(holder) = slot.holder.as_ref() else {
                return false;
            };
            let (_, entries, _) = store.log(&target, holder, slot.epoch, 1, 128).unwrap();
            entries
                .iter()
                .flat_map(|entry| &entry.changes)
                .filter_map(|change| change.value.as_deref())
                .filter_map(|bytes| pb::SocketConnection::decode(bytes).ok())
                .any(|reference| reference.id == first_id)
        })
        .count();
    assert!(
        copies >= 2,
        "HTTP 101 preceded a quorum's durable connection reference"
    );
    let mut b = connect(&fixture.gateways[1].address).await;
    set(&mut a, "A").await;
    set(&mut b, "B").await;

    // No input frames arrive after this schedule. The alarm Wake acquires a NEW holder from volume.
    let at = bedrock_lib::wall_ms() + 12_500;
    invoke(
        &fixture,
        0,
        &[b"alarm:".as_slice(), &at.to_be_bytes()].concat(),
    )
    .await
    .unwrap();
    wait_asleep(&fixture, 0).await;
    fixture.publish_second_holder();
    tokio::time::sleep(Duration::from_millis(
        at.saturating_sub(bedrock_lib::wall_ms()),
    ))
    .await;
    let reply = pb::actors_client::ActorsClient::connect(format!("http://{}", fixture.computes[1]))
        .await
        .unwrap()
        .invoke(pb::InvokeRequest {
            actor: Some(actor()),
            alarm_ms: Some(at),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, 0, "alarm Wake failed: {}", reply.message);
    receive(&mut a, "alarm:A").await;
    receive(&mut b, "alarm:B").await;
    assert_eq!(accepted(&snapshot(&fixture, 1).await), 2);
    // Its cached old holder must redirect the next real socket input to the new holder.
    set(&mut a, "A").await;

    assert!(invoke(&fixture, 1, b"fail").await.is_err());
    invoke(&fixture, 1, b"after-failure").await.unwrap();
    receive(&mut a, "after-failure").await;
    receive(&mut b, "after-failure").await;
    let awake = fixture.hosts[1].acquire(&actor()).await.unwrap();
    let hanging = tokio::spawn(async move { awake.invoke(b"hang".to_vec()).await });
    backend.entered.notified().await;
    hanging.abort();
    assert!(hanging.await.unwrap_err().is_cancelled());
    invoke(&fixture, 1, b"after-cancel").await.unwrap();
    receive(&mut a, "after-cancel").await;
    receive(&mut b, "after-cancel").await;
    assert_eq!(
        backend.hangs.load(Ordering::SeqCst),
        1,
        "cancelled input was replayed"
    );
    let awake = fixture.hosts[1].acquire(&actor()).await.unwrap();
    let fenced = tokio::spawn(async move { awake.invoke(b"fence".to_vec()).await });
    backend.entered.notified().await;
    for slot in &fixture.volumes_online[..2] {
        slot.store(false, Ordering::SeqCst);
    }
    backend.resume.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(7), fenced)
        .await
        .expect("holder kept serving without a slot quorum")
        .unwrap();
    assert!(
        matches!(outcome, Err(Error::Volume(_))),
        "fenced handler answered successfully"
    );
    for slot in &fixture.volumes_online[..2] {
        slot.store(true, Ordering::SeqCst);
    }
    invoke(&fixture, 1, b"after-fence").await.unwrap();
    receive(&mut a, "after-fence").await;
    receive(&mut b, "after-fence").await;

    // Host stop precedes the gate drain: the retained Storage writer can still have a valid lease.
    let awake = fixture.hosts[1].acquire(&actor()).await.unwrap();
    let stopped = tokio::spawn(async move { awake.invoke(b"stop".to_vec()).await });
    backend.entered.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), fixture.hosts[1].delete(&actor()))
            .await
            .is_err(),
        "deletion bypassed the active request gate"
    );
    backend.resume.notify_one();
    assert!(
        stopped.await.unwrap().is_err(),
        "stopped host returned a successful invocation"
    );
    invoke(&fixture, 1, b"after-stop").await.unwrap();
    receive(&mut a, "after-stop").await;
    receive(&mut b, "after-stop").await;

    fixture.gateways[1]
        .endpoint
        .unavailable
        .store(true, Ordering::SeqCst);
    let candidates = snapshot(&fixture, 1).await;
    assert_eq!(candidates.len(), 2, "unknown owner was pruned");
    assert_eq!(accepted(&candidates), 1);
    assert!(
        candidates
            .iter()
            .any(|s| s.outcome == pb::SocketOutcome::Unavailable as i32)
    );
    invoke(&fixture, 1, b"lost").await.unwrap();
    receive(&mut a, "lost").await;
    fixture.gateways[1]
        .endpoint
        .unavailable
        .store(false, Ordering::SeqCst);
    invoke(&fixture, 1, b"recovered").await.unwrap();
    receive(&mut a, "recovered").await;
    receive(&mut b, "recovered").await; // A replay would deliver "lost" first.
    assert_eq!(accepted(&snapshot(&fixture, 1).await), 2);

    invoke(&fixture, 1, b"overflow").await.unwrap();
    receive(&mut a, "overflow").await;
    let ended = tokio::time::timeout(Duration::from_secs(3), b.next())
        .await
        .expect("slow socket remained open");
    assert!(
        !matches!(ended, Some(Ok(Message::Text(_)))),
        "overflow leaked queued slow frames"
    );
    invoke(&fixture, 1, b"healthy").await.unwrap();
    receive(&mut a, "healthy").await;

    // Lose A's Close notification during a Compute transport outage, then replace its owning Gateway registry.
    fixture.compute_online.store(false, Ordering::SeqCst);
    let refused = fixture.compute_refused.load(Ordering::SeqCst);
    drop(a);
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(3);
    loop {
        use pb::sockets_server::Sockets as _;
        let owner = fixture.gateways[0].endpoint.current.lock().unwrap().clone();
        let answer = owner
            .lookup(tonic::Request::new(pb::SocketLookup {
                actor: Some(actor()),
                ids: vec![first_id.clone()],
            }))
            .await
            .unwrap()
            .into_inner();
        if answer.sockets[0].outcome == pb::SocketOutcome::Missing as i32
            && fixture.compute_refused.load(Ordering::SeqCst) > refused
        {
            break;
        }
        assert!(
            bedrock_lib::monotonic_now() < deadline,
            "disconnected socket was retained at Gateway"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fixture.gateways[0].restart_registry();
    fixture.compute_online.store(true, Ordering::SeqCst);
    let missing = snapshot(&fixture, 1).await;
    assert!(
        missing
            .iter()
            .any(|socket| socket.id == first_id
                && socket.outcome == pb::SocketOutcome::Missing as i32),
        "lost Close did not leave a recoverable candidate"
    );
    assert!(
        missing
            .iter()
            .all(|socket| socket.outcome == pb::SocketOutcome::Missing as i32)
    );
    assert!(
        snapshot(&fixture, 1).await.is_empty(),
        "definitive Missing refs survived cleanup"
    );
    let mut fresh = connect(&fixture.gateways[0].address).await;
    set(&mut fresh, "fresh").await;
    assert_ne!(backend.opened.lock().unwrap().last().unwrap(), &first_id);
    let remaining = snapshot(&fixture, 1).await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].attachment, b"fresh");
    // The handler really executes; only its reply is lost at the Compute transport boundary.
    let before = backend.messages.load(Ordering::SeqCst);
    fixture.lose_message_reply.store(true, Ordering::SeqCst);
    fresh
        .send(Message::Text("lost-reply".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(message) = fresh.next().await {
            match message {
                Ok(Message::Text(value)) => assert_eq!(value.as_str(), "set:lost-reply"),
                Ok(Message::Close(_)) | Err(_) => break,
                other => panic!("unexpected message after lost Invoke reply: {other:?}"),
            }
        }
    })
    .await
    .expect("ambiguous actor dispatch did not close its socket");
    assert_eq!(
        backend.messages.load(Ordering::SeqCst),
        before + 1,
        "socket input replayed after its reply was lost"
    );
    backend.trace.lock().unwrap().clone()
}

async fn in_flight_input_budget() {
    let backend = Arc::new(Backend::default());
    let limits = Limits {
        frame_bytes: 64,
        queue_bytes: 64,
        total_queue_bytes: 64,
        callback_ms: 200,
        ..Limits::default()
    };
    let fixture = Fixture::new(Arc::new(ExecutorAdapter(backend.clone())), limits).await;
    // Each frame fits its own connection's budget, but together they exceed this Gateway's total.
    let mut held = connect(&fixture.gateways[0].address).await;
    let mut overflow = connect(&fixture.gateways[0].address).await;
    backend.hold_message.store(true, Ordering::SeqCst);
    held.send(Message::Binary(vec![b'H'; 48].into()))
        .await
        .unwrap();
    backend.entered.notified().await;
    overflow
        .send(Message::Binary(vec![b'S'; 32].into()))
        .await
        .unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(3), overflow.next())
        .await
        .expect("in-flight input escaped the global queue budget");
    assert!(
        matches!(terminal, None | Some(Err(_)) | Some(Ok(Message::Close(_)))),
        "over-budget socket delivered application data"
    );
    assert_eq!(
        backend.messages.load(Ordering::SeqCst),
        1,
        "over-budget frame reached the blocked actor"
    );
    // Release only after closure: timing out the first handler cannot explain the observed close.
    backend.resume.notify_one();
    set(&mut held, "ok").await;
    assert_eq!(
        backend.messages.load(Ordering::SeqCst),
        2,
        "cancelled socket reservation leaked or replayed"
    );
}

async fn stalled_owner_does_not_block_other_gateways() {
    let backend = Arc::new(Backend::default());
    let limits = Limits {
        callback_concurrency: 2,
        callback_ms: 1000,
        ..Limits::default()
    };
    let fixture = Fixture::new_with_gateways(Arc::new(ExecutorAdapter(backend)), limits, 3).await;
    let _slow = connect(&fixture.gateways[0].address).await;
    let mut fast = connect(&fixture.gateways[1].address).await;
    let mut later = connect(&fixture.gateways[2].address).await;
    // Owners are visited in Gateway name order: slow first, fast second, later third.
    // Lookup stays real and responsive; only the first owner's Apply has no result.
    fixture.gateways[0]
        .endpoint
        .stall_apply
        .store(true, Ordering::SeqCst);
    let holder = fixture.hosts[0].clone();
    let deadline = bedrock_lib::monotonic_now() + Duration::from_secs(1);
    let broadcast = tokio::spawn(async move {
        holder
            .acquire(&actor())
            .await
            .unwrap()
            .invoke(b"isolated".to_vec())
            .await
    });
    tokio::time::timeout(
        deadline.saturating_duration_since(bedrock_lib::monotonic_now()),
        async {
            receive(&mut fast, "isolated").await;
            receive(&mut later, "isolated").await;
        },
    )
    .await
    .expect("a stalled first owner blocked a later healthy Gateway past the callback deadline");
    assert!(
        !broadcast.is_finished(),
        "healthy deliveries waited for the stalled owner's timeout"
    );
    broadcast.await.unwrap().unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn a_stalled_callback_owner_does_not_block_other_gateways() {
    stalled_owner_does_not_block_other_gateways().await;
}

#[cfg(madsim)]
#[madsim::test]
async fn a_stalled_callback_owner_does_not_block_other_gateways() {
    madsim::runtime::Handle::current()
        .create_node()
        .ip("10.8.0.1".parse().unwrap())
        .build()
        .spawn(stalled_owner_does_not_block_other_gateways())
        .await
        .unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn in_flight_input_remains_charged_to_the_gateway_budget() {
    in_flight_input_budget().await;
}

#[cfg(madsim)]
#[madsim::test]
async fn in_flight_input_remains_charged_to_the_gateway_budget() {
    madsim::runtime::Handle::current()
        .create_node()
        .ip("10.8.0.1".parse().unwrap())
        .build()
        .spawn(in_flight_input_budget())
        .await
        .unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn sockets_survive_hibernation_move_and_owner_faults_without_replay() {
    scenario().await;
}

#[cfg(madsim)]
#[test]
#[allow(clippy::disallowed_methods)]
fn sockets_survive_hibernation_move_and_owner_faults_and_replay() {
    use std::hash::{Hash, Hasher};

    let run = |seed| {
        std::thread::spawn(move || {
            let mut runtime =
                madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
            runtime.set_time_limit(Duration::from_secs(90));
            runtime.block_on(async {
                madsim::runtime::Handle::current()
                    .create_node()
                    .ip("10.8.0.1".parse().unwrap())
                    .build()
                    .spawn(scenario())
                    .await
                    .unwrap()
            })
        })
        .join()
        .unwrap_or_else(|failure| {
            eprintln!("Replay: BEDROCK_WEBSOCKETS_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test websockets -- --nocapture");
            std::panic::resume_unwind(failure)
        })
    };
    let seeds = std::env::var("BEDROCK_WEBSOCKETS_SEED")
        .map(|s| vec![s.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2]);
    for seed in seeds {
        let trace = run(seed);
        assert_eq!(trace, run(seed), "seed {seed} changed the socket trace");
        let mut fingerprint = std::hash::DefaultHasher::new();
        trace.hash(&mut fingerprint);
        println!(
            "websockets seed {seed}: {} observed trace entries matched; fingerprint {:016x}",
            trace.len(),
            fingerprint.finish()
        );
    }
}
