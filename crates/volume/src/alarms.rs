//! Alarm delivery hints (#117 / S-09). Only the handler's quorum commit consumes an alarm. Released actors use the
//! durable index; held actors whose host lease expired are scanned too, so a Compute crash cannot strand an alarm.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::placement::{self, Directory, State};
use bedrock_lib::volume as pb;
use tokio::time::Instant;
use tonic::Status;

use crate::{
    cold, serve,
    store::{Actor, Store},
};

const CONCURRENCY: usize = 64;
const TIMEOUT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_millis(100);
const LATE_MS: u64 = 5000;

/// A Wake follows the same Gateway route as an actor request. Repeated and delayed hints are allowed.
#[async_trait]
pub trait Wake: Send + Sync {
    async fn wake(&self, actor: pb::Actor, at_ms: u64) -> Result<(), Status>;
}

pub struct Gateway(pub Arc<Client>);

#[async_trait]
impl Wake for Gateway {
    async fn wake(&self, actor: pb::Actor, at_ms: u64) -> Result<(), Status> {
        let mut table = self.0.routes(GATEWAY, None).await?;
        if table.members.is_empty() {
            table = self.0.routes(GATEWAY, Some(table.revision)).await?;
        }
        let addresses: Vec<_> = table.members.values().map(|m| m.address.clone()).collect();
        let routes = self.0.route(&addresses);
        let address = routes
            .first()
            .ok_or_else(|| Status::unavailable("no gateway in this cell"))?;
        let member = table
            .members
            .values()
            .find(|m| m.address == **address)
            .expect("routed from table");
        let request = pb::InvokeRequest {
            actor: Some(actor),
            payload: Vec::new(),
            host: String::new(),
            alarm_ms: Some(at_ms),
        };
        let reply = self
            .0
            .call_member(member, |channel| async move {
                pb::actors_client::ActorsClient::new(channel)
                    .invoke(request)
                    .await
            })
            .await;
        if reply.is_err() {
            let _ = self.0.routes(GATEWAY, Some(table.revision)).await;
        }
        let reply = reply?;
        if reply.code != 0 {
            return Err(Status::new(
                tonic::Code::from_i32(reply.code),
                reply.message,
            ));
        }
        Ok(())
    }
}

pub type LeaseLive = Arc<dyn Fn(&pb::Holder) -> bool + Send + Sync>;
type Key = (String, String, u64);

struct Attempt {
    serial: u64,
    next: Instant,
    delay: Duration,
    running: Option<tokio::task::JoinHandle<()>>,
    /// A timed-out/failed Wake may already have claimed on a host that is still heartbeating other actors.
    needs_retry: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Completion {
    Skipped,
    Delivered,
    Failed,
}

pub struct Scanner {
    store: Arc<Store>,
    placement: Arc<dyn cold::Placement>,
    wake: Arc<dyn Wake>,
    node: String,
    lease_live: LeaseLive,
    attempts: BTreeMap<Key, Attempt>,
    cursor: Option<Key>,
    serial: u64,
    finished: tokio::sync::mpsc::UnboundedReceiver<(Key, u64, Completion)>,
    complete: tokio::sync::mpsc::UnboundedSender<(Key, u64, Completion)>,
}

impl Scanner {
    pub fn new(
        store: Arc<Store>,
        placement: Arc<dyn cold::Placement>,
        wake: Arc<dyn Wake>,
        node: &str,
        lease_live: LeaseLive,
    ) -> Self {
        let (complete, finished) = tokio::sync::mpsc::unbounded_channel();
        Self {
            store,
            placement,
            wake,
            node: node.into(),
            lease_live,
            attempts: BTreeMap::new(),
            cursor: None,
            serial: 0,
            finished,
            complete,
        }
    }

    /// Stops outstanding hints when membership is unreadable or the session is not ready.
    pub fn pause(&mut self) {
        for attempt in self.attempts.values_mut() {
            if let Some(task) = attempt.running.take() {
                task.abort();
            }
        }
        self.attempts.clear();
        while self.finished.try_recv().is_ok() {}
    }

    pub async fn tick(&mut self, directory: &Directory) {
        self.tick_at(directory, wall_ms()).await;
    }

    async fn tick_at(&mut self, directory: &Directory, now_ms: u64) {
        let now = bedrock_lib::monotonic_now();
        while let Ok((key, serial, completion)) = self.finished.try_recv() {
            if let Some(attempt) = self.attempts.get_mut(&key).filter(|a| a.serial == serial) {
                drop(attempt.running.take());
                let attempted = completion != Completion::Skipped;
                if attempted {
                    attempt.needs_retry = completion == Completion::Failed;
                }
                attempt.next = now + if attempted { attempt.delay } else { RETRY };
                if attempted {
                    attempt.delay = (attempt.delay * 2).min(TIMEOUT);
                }
            }
        }
        let store = self.store.clone();
        let candidates = match serve::blocking(move || store.due_alarms(now_ms)).await {
            Ok(Ok(candidates)) => candidates,
            Ok(Err(error)) => {
                tracing::warn!(?error, "alarm candidates unreadable");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "alarm scan failed");
                return;
            }
        };
        let held = self.store.held();
        let sealed = self.store.sealed();
        let owns = |key: &Key| {
            let wire = pb::Actor {
                namespace: key.0.clone(),
                id: key.1.clone(),
                kind: 0,
            };
            Actor::new(&wire).is_ok_and(|actor| {
                held.contains(actor.hash())
                    && !sealed.contains(actor.hash())
                    && placement::owners(directory, actor.hash())
                        .iter()
                        .any(|o| o.node == self.node && o.state != State::Populating)
            })
        };
        let mut pending = BTreeMap::new();
        let mut current = BTreeSet::new();
        for candidate in candidates {
            let Ok(actor) = Actor::new(&candidate.actor) else {
                continue;
            };
            let owners = placement::owners(directory, actor.hash());
            if !held.contains(actor.hash())
                || sealed.contains(actor.hash())
                || !owners
                    .iter()
                    .any(|o| o.node == self.node && o.state != State::Populating)
            {
                continue;
            }
            let key = (
                candidate.actor.namespace.clone(),
                candidate.actor.id.clone(),
                candidate.at_ms,
            );
            current.insert(key.clone());
            if !candidate.released
                && candidate
                    .holder
                    .as_ref()
                    .is_some_and(|h| (self.lease_live)(h))
                && !self.attempts.get(&key).is_some_and(|a| a.needs_retry)
            {
                continue;
            }
            let primary_owner = cold::maintainer(owners) == Some(self.node.as_str());
            pending.insert(key, (candidate, primary_owner));
        }
        self.attempts.retain(|key, attempt| {
            // Acquiring the actor removes it from the released index. Keep that Wake alive through its response or
            // timeout, even if the handler already cleared/rescheduled its alarm. Revoked ownership still stops it.
            if owns(key) && (current.contains(key) || attempt.running.is_some()) {
                true
            } else {
                if let Some(task) = attempt.running.take() {
                    task.abort();
                }
                false
            }
        });
        let mut keys: Vec<_> = pending.keys().cloned().collect();
        if let Some(cursor) = &self.cursor {
            let split = keys.partition_point(|key| key <= cursor);
            keys.rotate_left(split);
        }
        let mut running = self
            .attempts
            .values()
            .filter(|a| a.running.is_some())
            .count();
        for key in keys {
            if running >= CONCURRENCY {
                break;
            }
            let (candidate, primary_owner) = pending.remove(&key).expect("pending key");
            let attempt = self.attempts.entry(key.clone()).or_insert(Attempt {
                serial: 0,
                next: now,
                delay: RETRY,
                running: None,
                needs_retry: false,
            });
            if attempt.running.is_some() || now < attempt.next {
                continue;
            }
            let (placement, wake, complete) = (
                self.placement.clone(),
                self.wake.clone(),
                self.complete.clone(),
            );
            let done = key.clone();
            self.serial += 1;
            let serial = self.serial;
            attempt.serial = serial;
            attempt.running = Some(tokio::spawn(async move {
                let call = async {
                    let late = now_ms.saturating_sub(candidate.at_ms) >= LATE_MS;
                    if late || (primary_owner && placement.first_here(&candidate.actor).await?) {
                        wake.wake(candidate.actor, candidate.at_ms).await?;
                        return Ok(Completion::Delivered);
                    }
                    Ok::<_, Status>(Completion::Skipped)
                };
                let completion = match tokio::time::timeout(TIMEOUT, call).await {
                    Ok(Ok(completion)) => completion,
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "alarm Wake failed; retrying");
                        Completion::Failed
                    }
                    Err(_) => {
                        tracing::debug!("alarm Wake timed out; retrying");
                        Completion::Failed
                    }
                };
                let _ = complete.send((done, serial, completion));
            }));
            self.cursor = Some(key);
            running += 1;
        }
    }
}

impl Drop for Scanner {
    fn drop(&mut self) {
        self.pause();
    }
}

/// Persisted alarm timestamps use wall time; madsim supplies the simulated wall clock.
#[allow(clippy::disallowed_methods)]
fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
#[path = "../tests/unit/alarms.rs"]
mod tests;
