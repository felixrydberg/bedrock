//! Live WebSocket ownership (#123 / S-15). Actor references are durable; this registry is deliberately volatile.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bedrock_lib::datasets::capabilities::COMPUTE;
use bedrock_lib::replication::pb::Member;
use bedrock_lib::transport::Caller;
use bedrock_lib::{
    volume as pb,
    websockets::{self, Limits},
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message,
        protocol::{Role, WebSocketConfig},
    },
};
use tonic::{Request, Response, Status};

use crate::actors::{Dispatch, Router};

type ActorKey = (String, String);
struct Connection {
    actor: ActorKey,
    attachment: Vec<u8>,
    last: (u64, u64),
    closing: bool,
    outgoing: VecDeque<Message>,
    bytes: usize,
    messages: usize,
    notify: Arc<Notify>,
    stop: watch::Sender<bool>,
}
#[derive(Default)]
struct State {
    connections: BTreeMap<String, Connection>,
    bytes: usize,
}

/// Gateway-owned transports and bounded queues. No socket, attachment or callback is recovered or replayed.
pub struct Sockets {
    owner: Member,
    router: Arc<Router>,
    limits: Limits,
    state: Mutex<State>,
    callbacks: Semaphore,
}

impl Sockets {
    pub fn new(owner: Member, router: Arc<Router>, limits: Limits) -> Result<Self, Status> {
        limits.validate()?;
        websockets::connection(&pb::SocketConnection {
            id: "0".repeat(32),
            gateway: Some(owner.clone()),
        })?;
        Ok(Self {
            owner,
            router,
            callbacks: Semaphore::new(limits.callback_concurrency),
            limits,
            state: Mutex::default(),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if let Some(caller) = Caller::of(request) {
            caller.require(&[COMPUTE], None)?;
        }
        Ok(())
    }

    fn remove(&self, id: &str) {
        let mut state = self.state();
        if let Some(connection) = state.connections.remove(id) {
            state.bytes -= connection.bytes;
            let _ = connection.stop.send(true);
            connection.notify.notify_one();
        }
    }

    fn reserve(&self, id: &str, bytes: usize) -> bool {
        let mut state = self.state();
        let total = state.bytes;
        let Some(connection) = state.connections.get_mut(id) else {
            return false;
        };
        if connection.messages >= self.limits.queue_messages
            || connection.bytes + bytes > self.limits.queue_bytes
            || total + bytes > self.limits.total_queue_bytes
        {
            return false;
        }
        connection.bytes += bytes;
        connection.messages += 1;
        state.bytes += bytes;
        true
    }

    fn consumed(&self, id: &str, bytes: usize) {
        let mut state = self.state();
        if let Some(connection) = state.connections.get_mut(id) {
            connection.bytes -= bytes;
            connection.messages -= 1;
            state.bytes -= bytes;
        }
    }

    fn enqueue(&self, id: &str, message: Message) -> bool {
        if !self.reserve(id, message.len()) {
            self.remove(id);
            return false;
        }
        let mut state = self.state();
        if let Some(connection) = state.connections.get_mut(id) {
            connection.outgoing.push_back(message);
            connection.notify.notify_one();
            true
        } else {
            false
        }
    }

    fn event(
        &self,
        actor: pb::Actor,
        event: pb::SocketEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send + '_>> {
        Box::pin(async move {
            let dispatch = async {
                loop {
                    match self.router.socket(actor.clone(), event.clone()).await {
                        Err(status) if Dispatch::undispatched(&status) => {
                            tokio::time::sleep(Duration::from_millis(50)).await
                        }
                        result => return result.map(|_| ()),
                    }
                }
            };
            tokio::time::timeout(Duration::from_millis(self.limits.recovery_ms), dispatch)
                .await
                .map_err(|_| Status::deadline_exceeded("socket actor recovery deadline"))?
        })
    }

    /// Mount on the public listener. Open is committed before the HTTP 101 response is returned.
    pub async fn upgrade<B: Send + 'static>(
        self: &Arc<Self>,
        actor: pb::Actor,
        mut request: hyper::Request<B>,
    ) -> Result<hyper::Response<Full<Bytes>>, Status> {
        let mut head = hyper::Request::new(());
        *head.method_mut() = request.method().clone();
        *head.uri_mut() = request.uri().clone();
        *head.version_mut() = request.version();
        *head.headers_mut() = request.headers().clone();
        let response = tungstenite::handshake::server::create_response(&head)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let id = bedrock_lib::uuid();
        let (stop, stopped) = watch::channel(false);
        let notify = Arc::new(Notify::new());
        {
            let mut state = self.state();
            if state.connections.len() >= self.limits.connections {
                return Err(Status::resource_exhausted("Gateway socket limit"));
            }
            state.connections.insert(
                id.clone(),
                Connection {
                    actor: (actor.namespace.clone(), actor.id.clone()),
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
        }
        let connection = pb::SocketConnection {
            id: id.clone(),
            gateway: Some(self.owner.clone()),
        };
        let open = pb::SocketEvent {
            connection: Some(connection.clone()),
            kind: pb::socket_event::Kind::Open as i32,
            ..Default::default()
        };
        let upgrade = hyper::upgrade::on(&mut request);
        let owner = self.clone();
        let (accepted, answer) = tokio::sync::oneshot::channel();
        // The task owns cleanup even if the request is cancelled during Open or the upgrade handshake.
        tokio::spawn(async move {
            let result = owner.event(actor.clone(), open).await;
            let opened = result.is_ok();
            let mut terminal = (pb::socket_event::Kind::Error, None);
            if accepted.send(result).is_ok() && opened {
                let transport =
                    tokio::time::timeout(Duration::from_millis(owner.limits.recovery_ms), upgrade)
                        .await;
                if let Ok(Ok(transport)) = transport {
                    terminal = owner
                        .run(
                            actor.clone(),
                            connection.clone(),
                            TokioIo::new(transport),
                            notify,
                            stopped,
                        )
                        .await;
                }
            }
            owner.remove(&id);
            let _ = owner
                .event(
                    actor,
                    pb::SocketEvent {
                        connection: Some(connection),
                        kind: terminal.0 as i32,
                        close: terminal.1,
                        frame: None,
                    },
                )
                .await;
        });
        answer
            .await
            .map_err(|_| Status::unavailable("socket Open task stopped"))??;
        Ok(response.map(|_| Full::new(Bytes::new())))
    }

    async fn run<T>(
        &self,
        actor: pb::Actor,
        connection: pb::SocketConnection,
        io: T,
        notify: Arc<Notify>,
        mut stopped: watch::Receiver<bool>,
    ) -> (pb::socket_event::Kind, Option<pb::SocketClose>)
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let config = WebSocketConfig::default()
            .max_message_size(Some(self.limits.frame_bytes))
            .max_frame_size(Some(self.limits.frame_bytes));
        let socket = WebSocketStream::from_raw_socket(io, Role::Server, Some(config)).await;
        let (mut sink, mut stream) = socket.split();
        let id = &connection.id;
        let (inputs, mut events) = mpsc::channel(self.limits.queue_messages);
        let reader = async {
            while let Some(message) = stream.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        let frame = pb::SocketFrame {
                            data: text.as_bytes().to_vec(),
                            text: true,
                        };
                        if !self.reserve(id, frame.data.len()) {
                            break;
                        }
                        let bytes = frame.data.len();
                        if inputs.try_send(frame).is_err() {
                            self.consumed(id, bytes);
                            break;
                        }
                    }
                    Ok(Message::Binary(data)) => {
                        let frame = pb::SocketFrame {
                            data: data.to_vec(),
                            text: false,
                        };
                        if !self.reserve(id, frame.data.len()) {
                            break;
                        }
                        let bytes = frame.data.len();
                        if inputs.try_send(frame).is_err() {
                            self.consumed(id, bytes);
                            break;
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        if !self.enqueue(id, Message::Pong(data)) {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(close)) => {
                        let close = close
                            .map(|value| pb::SocketClose {
                                code: u16::from(value.code) as u32,
                                reason: value.reason.to_string(),
                            })
                            .filter(|value| websockets::close(value).is_ok());
                        return (pb::socket_event::Kind::Close, close);
                    }
                    Err(_) => return (pb::socket_event::Kind::Error, None),
                    Ok(Message::Frame(_)) => {}
                }
            }
            (pb::socket_event::Kind::Error, None)
        };
        let execute = async {
            while let Some(frame) = events.recv().await {
                // Keep in-flight input charged while routing/actor execution still retains its bytes.
                let bytes = frame.data.len();
                let event = pb::SocketEvent {
                    connection: Some(connection.clone()),
                    kind: pb::socket_event::Kind::Message as i32,
                    frame: Some(frame),
                    close: None,
                };
                let result = self.event(actor.clone(), event).await;
                self.consumed(id, bytes);
                if result.is_err() {
                    break;
                }
            }
        };
        let writer = async {
            loop {
                let message = self
                    .state()
                    .connections
                    .get_mut(id)
                    .and_then(|entry| entry.outgoing.pop_front());
                if let Some(message) = message {
                    let bytes = message.len();
                    let close = message.is_close();
                    let reason = if let Message::Close(value) = &message {
                        value.as_ref().map(|value| pb::SocketClose {
                            code: u16::from(value.code) as u32,
                            reason: value.reason.to_string(),
                        })
                    } else {
                        None
                    };
                    let sent = tokio::time::timeout(
                        Duration::from_millis(self.limits.callback_ms),
                        sink.send(message),
                    )
                    .await;
                    self.consumed(id, bytes);
                    if !matches!(sent, Ok(Ok(()))) {
                        return (pb::socket_event::Kind::Error, None);
                    }
                    if close {
                        return (pb::socket_event::Kind::Close, reason);
                    }
                } else {
                    notify.notified().await;
                }
            }
        };
        let terminal = tokio::select! {
            biased;
            _ = stopped.changed() => (pb::socket_event::Kind::Error, None),
            terminal = writer => terminal,
            () = execute => (pb::socket_event::Kind::Error, None),
            terminal = reader => terminal,
        };
        // Tungstenite queued the protocol reply when the peer sent Close. Bound its final flush too.
        let _ = tokio::time::timeout(Duration::from_millis(self.limits.callback_ms), sink.close())
            .await;
        terminal
    }
}

#[tonic::async_trait]
impl pb::sockets_server::Sockets for Sockets {
    async fn lookup(
        &self,
        request: Request<pb::SocketLookup>,
    ) -> Result<Response<pb::SocketSnapshots>, Status> {
        self.authorize(&request)?;
        let _permit = self
            .callbacks
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("socket callback limit"))?;
        let request = request.into_inner();
        let actor = request
            .actor
            .ok_or_else(|| Status::invalid_argument("missing actor"))?;
        if request.ids.len() > self.limits.roster {
            return Err(Status::resource_exhausted("socket lookup limit"));
        }
        for id in &request.ids {
            websockets::id(id)?;
        }
        let state = self.state();
        let key = (actor.namespace, actor.id);
        // Bound cloning before constructing a reply, including repeated IDs and large configured attachments.
        let bytes: usize = request
            .ids
            .iter()
            .map(|id| {
                id.len()
                    + 16
                    + state
                        .connections
                        .get(id)
                        .filter(|entry| entry.actor == key && !entry.closing)
                        .map_or(0, |entry| entry.attachment.len())
            })
            .sum();
        if bytes > bedrock_lib::kv::MAX_MESSAGE_BYTES / 2 {
            return Err(Status::resource_exhausted("socket snapshot byte limit"));
        }
        let sockets = request
            .ids
            .into_iter()
            .map(|id| {
                match state
                    .connections
                    .get(&id)
                    .filter(|entry| entry.actor == key && !entry.closing)
                {
                    Some(entry) => pb::SocketSnapshot {
                        id,
                        outcome: pb::SocketOutcome::Accepted as i32,
                        attachment: entry.attachment.clone(),
                    },
                    None => pb::SocketSnapshot {
                        id,
                        outcome: pb::SocketOutcome::Missing as i32,
                        attachment: Vec::new(),
                    },
                }
            })
            .collect();
        Ok(Response::new(pb::SocketSnapshots { sockets }))
    }

    async fn apply(
        &self,
        request: Request<pb::SocketApply>,
    ) -> Result<Response<pb::SocketReport>, Status> {
        self.authorize(&request)?;
        let _permit = self
            .callbacks
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("socket callback limit"))?;
        let request = request.into_inner();
        let actor = request
            .actor
            .ok_or_else(|| Status::invalid_argument("missing actor"))?;
        if request.epoch == 0 || request.event == 0 {
            return Err(Status::invalid_argument("missing callback order"));
        }
        if request.effects.len() > self.limits.effects {
            return Err(Status::resource_exhausted("socket effects limit"));
        }
        let mut bytes = 0;
        let mut closing = BTreeSet::new();
        for effect in &request.effects {
            self.limits.effect(effect)?;
            if closing.contains(&effect.id) {
                return Err(Status::invalid_argument("effect follows socket Close"));
            }
            if matches!(effect.action, Some(pb::socket_effect::Action::Close(_))) {
                closing.insert(effect.id.clone());
            }
            bytes += match effect.action.as_ref().unwrap() {
                pb::socket_effect::Action::Send(frame) => frame.data.len(),
                pb::socket_effect::Action::Attachment(value) => value.len(),
                pb::socket_effect::Action::Close(close) => close.reason.len(),
            };
        }
        if bytes > self.limits.effect_bytes {
            return Err(Status::resource_exhausted("socket effects byte limit"));
        }
        let key = (actor.namespace, actor.id);
        let order = (request.epoch, request.event);
        let mut state = self.state();
        // Check order once per connection: an event may stage multiple effects for the same socket.
        let mut outcomes = BTreeMap::new();
        for effect in &request.effects {
            outcomes.entry(effect.id.clone()).or_insert_with(|| {
                match state
                    .connections
                    .get_mut(&effect.id)
                    .filter(|entry| entry.actor == key && !entry.closing)
                {
                    None => pb::SocketOutcome::Missing,
                    Some(entry) if order <= entry.last => pb::SocketOutcome::Stale,
                    Some(entry) => {
                        entry.last = order;
                        pb::SocketOutcome::Accepted
                    }
                }
            });
        }
        let mut results = Vec::new();
        for effect in request.effects {
            let mut outcome = outcomes[&effect.id];
            if outcome == pb::SocketOutcome::Accepted {
                let message = match effect.action.unwrap() {
                    pb::socket_effect::Action::Attachment(value) => {
                        if let Some(entry) = state.connections.get_mut(&effect.id) {
                            entry.attachment = value;
                        }
                        None
                    }
                    pb::socket_effect::Action::Send(frame) => Some(if frame.text {
                        Message::Text(String::from_utf8(frame.data).unwrap().into())
                    } else {
                        Message::Binary(frame.data.into())
                    }),
                    pb::socket_effect::Action::Close(close) => {
                        Some(Message::Close(Some(tungstenite::protocol::CloseFrame {
                            code: (close.code as u16).into(),
                            reason: close.reason.into(),
                        })))
                    }
                };
                if let Some(message) = message {
                    let total = state.bytes;
                    if let Some(entry) = state.connections.get_mut(&effect.id) {
                        let bytes = message.len();
                        if entry.messages >= self.limits.queue_messages
                            || entry.bytes + bytes > self.limits.queue_bytes
                            || total + bytes > self.limits.total_queue_bytes
                        {
                            outcome = pb::SocketOutcome::Overloaded;
                            outcomes.insert(effect.id.clone(), outcome);
                            let entry = state.connections.remove(&effect.id).unwrap();
                            state.bytes -= entry.bytes;
                            let _ = entry.stop.send(true);
                            entry.notify.notify_one();
                        } else {
                            entry.closing = message.is_close();
                            entry.bytes += bytes;
                            entry.messages += 1;
                            entry.outgoing.push_back(message);
                            entry.notify.notify_one();
                            state.bytes += bytes;
                        }
                    }
                }
            }
            results.push(pb::SocketResult {
                id: effect.id,
                outcome: outcome as i32,
                detail: String::new(),
            });
        }
        Ok(Response::new(pb::SocketReport { results }))
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "../tests/unit/sockets.rs"]
mod tests;
