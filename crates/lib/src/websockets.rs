//! Gateway-owned hibernatable sockets (#123 / S-15). Protocol: `docs/spec/websockets.md`.

use serde::Deserialize;
use tonic::Status;

use crate::datasets::capabilities::GATEWAY;
use crate::volume as pb;

/// Resource limits shared by the Gateway transport and the actor's event scope.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub frame_bytes: usize,
    pub attachment_bytes: usize,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub total_queue_bytes: usize,
    pub connections: usize,
    pub roster: usize,
    pub effects: usize,
    pub effect_bytes: usize,
    pub callback_concurrency: usize,
    pub callback_ms: u64,
    pub recovery_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            frame_bytes: 64 * 1024,
            attachment_bytes: 2048,
            queue_messages: 64,
            queue_bytes: 1024 * 1024,
            total_queue_bytes: 64 * 1024 * 1024,
            connections: 4096,
            roster: 1024,
            effects: 128,
            effect_bytes: 1024 * 1024,
            callback_concurrency: 4,
            callback_ms: 1000,
            recovery_ms: 15_000,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<(), Status> {
        if [
            self.frame_bytes,
            self.attachment_bytes,
            self.queue_messages,
            self.queue_bytes,
            self.total_queue_bytes,
            self.connections,
            self.roster,
            self.effects,
            self.effect_bytes,
            self.callback_concurrency,
        ]
        .contains(&0)
            || self.callback_ms == 0
            || self.recovery_ms == 0
            || self.callback_ms > 60_000
            || self.recovery_ms > 300_000
            || self.queue_messages > tokio::sync::Semaphore::MAX_PERMITS
            || self.frame_bytes > self.effect_bytes
            || self.attachment_bytes > self.effect_bytes
            || self.effect_bytes > crate::kv::MAX_MESSAGE_BYTES / 2
            || self.queue_bytes > self.total_queue_bytes
            || self.total_queue_bytes > u32::MAX as usize
            || self.roster > 4096
            || self.effects > 4096
            || self.callback_concurrency > 16
        {
            return Err(Status::invalid_argument(
                "invalid WebSocket resource limits",
            ));
        }
        Ok(())
    }

    pub fn frame(&self, frame: &pb::SocketFrame) -> Result<(), Status> {
        if frame.data.len() > self.frame_bytes {
            return Err(Status::resource_exhausted(
                "WebSocket message exceeds frame_bytes",
            ));
        }
        if frame.text && std::str::from_utf8(&frame.data).is_err() {
            return Err(Status::invalid_argument("WebSocket text is not UTF-8"));
        }
        Ok(())
    }

    pub fn effect(&self, effect: &pb::SocketEffect) -> Result<(), Status> {
        id(&effect.id)?;
        match &effect.action {
            Some(pb::socket_effect::Action::Send(frame)) => self.frame(frame),
            Some(pb::socket_effect::Action::Attachment(bytes))
                if bytes.len() <= self.attachment_bytes =>
            {
                Ok(())
            }
            Some(pb::socket_effect::Action::Attachment(_)) => Err(Status::resource_exhausted(
                "WebSocket attachment exceeds attachment_bytes",
            )),
            Some(pb::socket_effect::Action::Close(value)) => close(value),
            None => Err(Status::invalid_argument("missing WebSocket effect")),
        }
    }
}

pub fn id(value: &str) -> Result<(), Status> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Status::invalid_argument("invalid WebSocket connection ID"));
    }
    Ok(())
}

pub fn connection(value: &pb::SocketConnection) -> Result<(), Status> {
    id(&value.id)?;
    let gateway = value
        .gateway
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing socket owner"))?;
    if gateway.service != GATEWAY
        || gateway.node_id.is_empty()
        || gateway.cell.is_empty()
        || gateway.address.is_empty()
        || gateway.region.is_empty()
        || gateway.dc.is_empty()
    {
        return Err(Status::invalid_argument("invalid socket owner"));
    }
    Ok(())
}

pub fn close(value: &pb::SocketClose) -> Result<(), Status> {
    if !matches!(value.code, 1000..=1003 | 1007..=1014 | 3000..=4999) || value.reason.len() > 123 {
        return Err(Status::invalid_argument(
            "invalid WebSocket close code or reason",
        ));
    }
    Ok(())
}

pub fn event(value: &pb::SocketEvent, limits: &Limits) -> Result<(), Status> {
    connection(
        value
            .connection
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing socket connection"))?,
    )?;
    match pb::socket_event::Kind::try_from(value.kind) {
        Ok(pb::socket_event::Kind::Message) if value.close.is_none() => limits.frame(
            value
                .frame
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing socket message"))?,
        ),
        Ok(pb::socket_event::Kind::Open) if value.frame.is_none() && value.close.is_none() => {
            Ok(())
        }
        Ok(pb::socket_event::Kind::Close | pb::socket_event::Kind::Error)
            if value.frame.is_none() =>
        {
            value.close.as_ref().map_or(Ok(()), close)
        }
        _ => Err(Status::invalid_argument("invalid WebSocket event fields")),
    }
}

#[cfg(test)]
#[path = "../tests/unit/websockets.rs"]
mod tests;
