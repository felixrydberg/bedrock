//! The one execution seam (`statefull/v2.md` §8, §9, §10 item 8; #115 / S-07): every event reaches a Worker or an actor
//! through an [`Executor`] that starts an [`Instance`] for an [`ExecutionSpec`]. Built-in actor kinds run in the
//! [`RustExecutor`]; customer code (Workers, Durable Objects) runs in S-10's sandbox executor. There is no second
//! invoke path.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bedrock_lib::actors::Kind;
use bedrock_lib::volume as pb;

use crate::actors::storage::Storage;
use crate::actors::volume;

#[derive(Debug)]
pub enum Error {
    /// Another host holds the actor; send the request there.
    Redirect(pb::Holder),
    /// The actor's storage failed, or this holder stopped serving. A request that fails this way may be retried; a
    /// commit it made may still have committed.
    Volume(volume::Error),
    /// No executor here runs this spec (sandboxed code before S-10), or the actor ID does not place.
    Unsupported(String),
    /// The handler failed; nothing it did is lost or sent.
    Handler(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redirect(h) => write!(f, "held by {} in {}", h.host, h.cell),
            Self::Volume(error) => write!(f, "volume: {error}"),
            Self::Unsupported(detail) => write!(f, "unsupported: {detail}"),
            Self::Handler(detail) => write!(f, "handler failed: {detail}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<volume::Error> for Error {
    fn from(error: volume::Error) -> Self {
        match error {
            volume::Error::Held(holder) => Self::Redirect(holder),
            other => Self::Volume(other),
        }
    }
}

/// What to run.
pub enum ExecutionSpec {
    /// A stateless Worker: one execution of a CDN artifact (B-35, B-51) on any host, with no claim, lease, actor ID or
    /// storage. Runs in the sandbox (S-10).
    Worker { artifact: String },
    /// An awake actor of `actor.kind`, with its volume-backed storage.
    Actor { actor: pb::Actor, storage: Storage },
}

impl ExecutionSpec {
    /// The built-in actor kind a Rust handler runs, or `None` for customer code, which runs in the sandbox. An unknown
    /// kind is customer code, so it never reaches a built-in handler.
    pub fn builtin(&self) -> Option<Kind> {
        match self {
            Self::Worker { .. } => None,
            Self::Actor { actor, .. } => match Kind::try_from(actor.kind) {
                Ok(kind @ (Kind::D1 | Kind::Queue | Kind::Cron | Kind::Kv)) => Some(kind),
                _ => None,
            },
        }
    }
}

/// Starts instances for specs.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn start(&self, spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error>;
}

/// A started Worker or actor. The actor host calls [`Instance::invoke`] one event at a time (the input gate).
#[async_trait]
pub trait Instance: Send {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error>;
}

/// Builds a built-in kind's instance over the actor's storage.
pub type Handler = Arc<dyn Fn(pb::Actor, Storage) -> Box<dyn Instance> + Send + Sync>;

/// Runs built-in actor kinds as Rust handlers in the host, without a sandbox.
pub struct RustExecutor {
    handlers: BTreeMap<i32, Handler>,
}

impl RustExecutor {
    /// `handlers` by built-in kind; S-11 … S-18 add theirs.
    pub fn new(handlers: impl IntoIterator<Item = (Kind, Handler)>) -> Self {
        RustExecutor {
            handlers: handlers
                .into_iter()
                .map(|(kind, h)| (kind as i32, h))
                .collect(),
        }
    }
}

#[async_trait]
impl Executor for RustExecutor {
    async fn start(&self, spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error> {
        let kind = spec.builtin();
        let ExecutionSpec::Actor { actor, storage } = spec else {
            return Err(Error::Unsupported("a Worker runs in the sandbox".into()));
        };
        let handler = kind
            .and_then(|kind| self.handlers.get(&(kind as i32)))
            .ok_or_else(|| {
                Error::Unsupported(format!("no built-in handler for kind {}", actor.kind))
            })?;
        Ok(handler(actor, storage))
    }
}
