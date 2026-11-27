//! S-15: recoverable references and event-scoped outputs; transport/attachments remain at the Gateway.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::replication::pb::Member;
use bedrock_lib::volume as pb;
use bedrock_lib::websockets::{self, Limits};
use futures_util::{StreamExt, stream};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family};
use prost::Message;
use tonic::Status;

use super::storage::Storage;
use crate::runtime::{Error, Event};

// SQLite uses four-byte page keys and sqlite/size; DO customer KV is a hidden SQLite table.
const PREFIX: &[u8] = b"bedrock/socket/";
type Owner = (String, String, String);

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Outcome {
    outcome: &'static str,
}

pub(crate) struct Settings {
    client: Arc<Client>,
    pub limits: Limits,
    results: Family<Outcome, Counter>,
}

impl Settings {
    pub fn new(admin: &Admin, client: Arc<Client>, limits: Limits) -> Result<Self, Status> {
        limits.validate()?;
        let results = Family::default();
        admin.register(
            "compute_socket_outputs",
            "Socket output results (accepted means queued, not received)",
            results.clone(),
        );
        Ok(Self {
            client,
            limits,
            results,
        })
    }

    fn record(&self, report: &pb::SocketReport) {
        for result in &report.results {
            let outcome = match pb::SocketOutcome::try_from(result.outcome) {
                Ok(pb::SocketOutcome::Accepted) => "accepted",
                Ok(pb::SocketOutcome::Missing) => "missing",
                Ok(pb::SocketOutcome::Overloaded) => "overloaded",
                Ok(pb::SocketOutcome::Unavailable) => "unavailable",
                Ok(pb::SocketOutcome::Stale) => "stale",
                _ => "uncertain",
            };
            self.results.get_or_create(&Outcome { outcome }).inc();
            if outcome != "accepted" {
                tracing::warn!(connection = %result.id, outcome, detail = %result.detail, "socket output was not confirmed queued");
            }
        }
    }
}

/// A candidate connection's current owner result; unavailable is not absence.
#[derive(Clone, Debug)]
pub struct Connection {
    pub reference: pb::SocketConnection,
    pub outcome: pb::SocketOutcome,
    pub attachment: Vec<u8>,
}

/// A borrowed event capability: it cannot be cloned or retained by a detached task.
pub struct Context<'a> {
    actor: &'a pb::Actor,
    storage: &'a Storage,
    settings: Option<&'a Settings>,
    references: Option<BTreeMap<String, pb::SocketConnection>>,
    snapshots: Option<Vec<Connection>>,
    opening: Option<pb::SocketConnection>,
    retiring: BTreeSet<String>,
    effects: Vec<pb::SocketEffect>,
    bytes: usize,
    event: u64,
    serving: &'a (dyn Fn() -> bool + Send + Sync),
}

fn key(id: &str) -> Vec<u8> {
    [PREFIX, id.as_bytes()].concat()
}
fn owner(member: &Member) -> Owner {
    (
        member.cell.clone(),
        member.node_id.clone(),
        member.address.clone(),
    )
}
fn failure(detail: impl Into<String>) -> Error {
    Error::Handler(detail.into())
}

impl<'a> Context<'a> {
    pub(crate) fn new(
        actor: &'a pb::Actor,
        storage: &'a Storage,
        settings: Option<&'a Settings>,
        event: u64,
        serving: &'a (dyn Fn() -> bool + Send + Sync),
    ) -> Self {
        Self {
            actor,
            storage,
            settings,
            references: None,
            snapshots: None,
            opening: None,
            retiring: BTreeSet::new(),
            effects: Vec::new(),
            bytes: 0,
            event,
            serving,
        }
    }

    fn settings(&self) -> Result<&Settings, Error> {
        self.settings
            .ok_or_else(|| Error::Unsupported("WebSocket host is not configured".into()))
    }

    async fn load(&mut self) -> Result<(), Error> {
        if self.references.is_some() {
            return Ok(());
        }
        let limit = self.settings()?.limits.roster;
        let mut references = BTreeMap::new();
        let mut start = PREFIX.to_vec();
        'pages: loop {
            let (values, more) = self
                .storage
                .scan(
                    start.clone(),
                    ((limit + 1 - references.len()).min(128)) as u32,
                )
                .await?;
            if more && values.is_empty() {
                return Err(Error::Volume(super::volume::Error::Rpc(Status::data_loss(
                    "empty socket roster page with continuation",
                ))));
            }
            for value in values {
                if !value.key.starts_with(PREFIX) {
                    break 'pages;
                }
                start = value.key.clone();
                start.push(0);
                let bytes = value
                    .value
                    .ok_or_else(|| failure("socket roster scan returned a tombstone"))?;
                let reference = pb::SocketConnection::decode(bytes.as_slice()).map_err(|e| {
                    Error::Volume(super::volume::Error::Rpc(Status::data_loss(format!(
                        "socket roster: {e}"
                    ))))
                })?;
                websockets::connection(&reference).map_err(|e| {
                    Error::Volume(super::volume::Error::Rpc(Status::data_loss(e.message())))
                })?;
                if key(&reference.id) != value.key
                    || references.insert(reference.id.clone(), reference).is_some()
                {
                    return Err(Error::Volume(super::volume::Error::Rpc(Status::data_loss(
                        "invalid socket roster key",
                    ))));
                }
                if references.len() > limit {
                    return Err(failure("socket roster exceeds configured cap"));
                }
            }
            if !more {
                break;
            }
        }
        self.references = Some(references);
        Ok(())
    }

    pub(crate) async fn prepare(&mut self, event: &Event) -> Result<(), Error> {
        let Event::Socket(event) = event else {
            return Ok(());
        };
        websockets::event(event, &self.settings()?.limits).map_err(|e| failure(e.to_string()))?;
        self.load().await?;
        let reference = event.connection.as_ref().expect("validated connection");
        let references = self.references.as_mut().expect("loaded roster");
        match pb::socket_event::Kind::try_from(event.kind).expect("validated kind") {
            pb::socket_event::Kind::Open => {
                if references.contains_key(&reference.id) {
                    return Err(failure("connection is already registered"));
                }
                if references.len() >= self.settings.expect("configured").limits.roster {
                    return Err(failure("actor connection limit"));
                }
                self.opening = Some(reference.clone());
                references.insert(reference.id.clone(), reference.clone());
            }
            kind => {
                match references.get(&reference.id) {
                    Some(saved) if saved != reference => {
                        return Err(failure("connection owner does not match its roster"));
                    }
                    None if kind == pb::socket_event::Kind::Message => {
                        return Err(failure("connection was retired"));
                    }
                    _ => {}
                }
                if matches!(
                    kind,
                    pb::socket_event::Kind::Close | pb::socket_event::Kind::Error
                ) {
                    self.retiring.insert(reference.id.clone());
                }
            }
        }
        Ok(())
    }

    /// Enumerates all durable candidates, preserving unavailable owner results explicitly.
    pub async fn connections(&mut self) -> Result<Vec<Connection>, Error> {
        self.load().await?;
        if let Some(snapshots) = &self.snapshots {
            return Ok(snapshots.clone());
        }
        let settings = self.settings()?;
        let mut groups: BTreeMap<Owner, (Member, Vec<pb::SocketConnection>)> = BTreeMap::new();
        for reference in self.references.as_ref().expect("loaded roster").values() {
            let member = reference.gateway.as_ref().expect("validated owner");
            groups
                .entry(owner(member))
                .or_insert_with(|| (member.clone(), Vec::new()))
                .1
                .push(reference.clone());
        }
        let deadline =
            bedrock_lib::monotonic_now() + Duration::from_millis(settings.limits.callback_ms);
        let actor = self.actor;
        let client = &settings.client;
        let max_attachment = settings.limits.attachment_bytes;
        let mut pending = stream::iter(groups.into_values().map(|(member, refs)| async move {
            let request = pb::SocketLookup {
                actor: Some(actor.clone()),
                ids: refs.iter().map(|r| r.id.clone()).collect(),
            };
            let expired = bedrock_lib::monotonic_now() >= deadline;
            let call: futures_util::future::BoxFuture<'_, Result<pb::SocketSnapshots, Status>> =
                Box::pin(client.call_member(&member, |channel| async move {
                    pb::sockets_client::SocketsClient::new(channel)
                        .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .lookup(request)
                        .await
                }));
            let result = if expired {
                None
            } else {
                Some(
                    tokio::time::timeout(
                        deadline.saturating_duration_since(bedrock_lib::monotonic_now()),
                        call,
                    )
                    .await,
                )
            };
            let answer = match result {
                Some(Ok(Ok(answer))) => Some(answer),
                _ => None,
            };
            let mut received: BTreeMap<String, pb::SocketSnapshot> = answer
                .map(|a| a.sockets.into_iter().map(|s| (s.id.clone(), s)).collect())
                .unwrap_or_default();
            refs.into_iter()
                .map(|reference| {
                    let snapshot = received.remove(&reference.id);
                    let (outcome, attachment) = match snapshot {
                        Some(s)
                            if s.outcome == pb::SocketOutcome::Accepted as i32
                                && s.attachment.len() <= max_attachment =>
                        {
                            (pb::SocketOutcome::Accepted, s.attachment)
                        }
                        Some(s)
                            if s.outcome == pb::SocketOutcome::Missing as i32
                                && s.attachment.is_empty() =>
                        {
                            (pb::SocketOutcome::Missing, Vec::new())
                        }
                        _ => (pb::SocketOutcome::Unavailable, Vec::new()),
                    };
                    Connection {
                        reference,
                        outcome,
                        attachment,
                    }
                })
                .collect::<Vec<_>>()
        }))
        // An unavailable first owner must not prevent later healthy owners from starting.
        .buffer_unordered(settings.limits.callback_concurrency);
        let mut snapshots = Vec::new();
        while let Some(group) = pending.next().await {
            snapshots.extend(group);
        }
        drop(pending);
        for snapshot in &snapshots {
            if snapshot.outcome == pb::SocketOutcome::Missing {
                self.retiring.insert(snapshot.reference.id.clone());
            }
        }
        snapshots.sort_by(|a, b| a.reference.id.cmp(&b.reference.id));
        self.snapshots = Some(snapshots.clone());
        Ok(snapshots)
    }

    fn push(&mut self, effect: pb::SocketEffect) -> Result<(), Error> {
        let limits = &self.settings()?.limits;
        limits.effect(&effect).map_err(|e| failure(e.to_string()))?;
        if self
            .references
            .as_ref()
            .is_none_or(|r| !r.contains_key(&effect.id))
        {
            return Err(failure(
                "enumerate connections before sending to an unknown socket",
            ));
        }
        if self.effects.iter().any(|prior| {
            prior.id == effect.id
                && matches!(prior.action, Some(pb::socket_effect::Action::Close(_)))
        }) {
            return Err(failure("connection was already closed in this event"));
        }
        let bytes = self
            .bytes
            .checked_add(effect.encoded_len())
            .ok_or_else(|| failure("socket effects overflow"))?;
        if self.effects.len() >= limits.effects || bytes > limits.effect_bytes {
            return Err(failure("socket event output limit"));
        }
        self.bytes = bytes;
        self.effects.push(effect);
        Ok(())
    }

    pub fn send(&mut self, id: &str, data: Vec<u8>, text: bool) -> Result<(), Error> {
        self.push(pb::SocketEffect {
            id: id.into(),
            action: Some(pb::socket_effect::Action::Send(pb::SocketFrame {
                data,
                text,
            })),
        })
    }
    pub fn attachment(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), Error> {
        self.push(pb::SocketEffect {
            id: id.into(),
            action: Some(pb::socket_effect::Action::Attachment(bytes)),
        })
    }
    /// Retires the durable ID even if its Gateway cannot currently be reached.
    pub fn close(&mut self, id: &str, code: u32, reason: String) -> Result<(), Error> {
        self.push(pb::SocketEffect {
            id: id.into(),
            action: Some(pb::socket_effect::Action::Close(pb::SocketClose {
                code,
                reason,
            })),
        })
    }

    pub(crate) async fn finish(
        mut self,
        result: Result<Vec<u8>, Error>,
        socket_event: bool,
    ) -> Result<Vec<u8>, Error> {
        if !(self.serving)() {
            return Err(super::volume::Error::Fenced(
                "the host stopped the actor before socket roster commit".into(),
            )
            .into());
        }
        if result.is_err() {
            self.effects.clear();
            self.opening = None;
        }
        for effect in &self.effects {
            if matches!(effect.action, Some(pb::socket_effect::Action::Close(_))) {
                self.retiring.insert(effect.id.clone());
            }
        }
        // Missing/closed references are cleanup even when the application rejects its close callback.
        let mut changes: Vec<_> = self
            .retiring
            .iter()
            .map(|id| pb::Change {
                key: key(id),
                value: None,
            })
            .collect();
        if let Some(reference) = &self.opening
            && !self.retiring.contains(&reference.id)
        {
            changes.push(pb::Change {
                key: key(&reference.id),
                value: Some(reference.encode_to_vec()),
            });
        }
        if !changes.is_empty() {
            self.storage.commit(changes).await?;
        }
        let payload = result?;
        if !(self.serving)() {
            return Err(super::volume::Error::Fenced(
                "the host stopped the actor before socket output".into(),
            )
            .into());
        }
        let report = self.flush().await?;
        if socket_event {
            Ok(report.encode_to_vec())
        } else {
            Ok(payload)
        }
    }

    async fn flush(&mut self) -> Result<pb::SocketReport, Error> {
        if self.effects.is_empty() {
            return Ok(pb::SocketReport::default());
        }
        let settings = self.settings()?;
        let mut groups: BTreeMap<Owner, (Member, Vec<pb::SocketEffect>)> = BTreeMap::new();
        for effect in &self.effects {
            let member = self
                .references
                .as_ref()
                .and_then(|r| r.get(&effect.id))
                .and_then(|r| r.gateway.as_ref())
                .expect("validated effect destination");
            groups
                .entry(owner(member))
                .or_insert_with(|| (member.clone(), Vec::new()))
                .1
                .push(effect.clone());
        }
        let deadline =
            bedrock_lib::monotonic_now() + Duration::from_millis(settings.limits.callback_ms);
        let actor = self.actor;
        let epoch = self.storage.epoch();
        let event = self.event;
        let serving = self.serving;
        let mut pending = stream::iter(groups.into_values().map(|(member, effects)| async move {
            let ids: BTreeSet<_> = effects.iter().map(|effect| effect.id.clone()).collect();
            if !serving() || bedrock_lib::monotonic_now() >= deadline {
                return pb::SocketReport {
                    results: ids
                        .into_iter()
                        .map(|id| pb::SocketResult {
                            id,
                            outcome: pb::SocketOutcome::Unavailable as i32,
                            detail: "holder fenced or callback budget expired before emission"
                                .into(),
                        })
                        .collect(),
                };
            }
            let request = pb::SocketApply {
                actor: Some(actor.clone()),
                epoch,
                event,
                effects,
            };
            let call: futures_util::future::BoxFuture<'_, Result<pb::SocketReport, Status>> =
                Box::pin(settings.client.call_member(&member, |channel| async move {
                    pb::sockets_client::SocketsClient::new(channel)
                        .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                        .apply(request)
                        .await
                }));
            let result = tokio::time::timeout(
                deadline.saturating_duration_since(bedrock_lib::monotonic_now()),
                call,
            )
            .await;
            match result {
                Ok(Ok(report)) => {
                    let mut results: BTreeMap<_, _> = report
                        .results
                        .into_iter()
                        .map(|r| (r.id.clone(), r))
                        .collect();
                    pb::SocketReport {
                        results: ids
                            .into_iter()
                            .map(|id| {
                                results.remove(&id).unwrap_or(pb::SocketResult {
                                    id,
                                    outcome: pb::SocketOutcome::Uncertain as i32,
                                    detail: "incomplete Gateway callback result".into(),
                                })
                            })
                            .collect(),
                    }
                }
                other => {
                    let detail = match other {
                        Ok(Err(status)) => format!("{}: {}", status.code(), status.message()),
                        _ => "Gateway callback deadline; outcome unknown".into(),
                    };
                    pb::SocketReport {
                        results: ids
                            .into_iter()
                            .map(|id| pb::SocketResult {
                                id,
                                outcome: pb::SocketOutcome::Uncertain as i32,
                                detail: detail.clone(),
                            })
                            .collect(),
                    }
                }
            }
        }))
        .buffer_unordered(settings.limits.callback_concurrency);
        let mut report = pb::SocketReport::default();
        while let Some(answer) = pending.next().await {
            settings.record(&answer);
            report.results.extend(answer.results);
        }
        report.results.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(report)
    }
}
