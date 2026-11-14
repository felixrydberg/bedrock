//! Raft storage and durable Ready processing, independent of the membership state machine.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use protobuf::Message as _;
use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType, Snapshot};
use raft::{GetEntriesContext, RawNode, Storage};
use tokio::sync::{mpsc, oneshot};

use bedrock_lib::storage::StorageEngine;

/// Commands and admission policy belong to the owner; Raft traffic uses the same actor queue.
pub(crate) enum Input<C> {
    Step(Message, oneshot::Sender<Result<(), String>>),
    Command(C),
    Sent { to: u64, snapshot: bool, ok: bool },
}

/// The owner publishes durable outcomes and leadership changes in `ready`.
pub(crate) trait Policy {
    type Machine: Machine;
    type Command;

    fn raw(&mut self) -> &mut RawNode<Store<Self::Machine>>;
    fn command(&mut self, command: Self::Command);
    fn ready(&mut self) -> Result<(), String>;
}

/// Drive a fixed-voter actor, acknowledging inbound steps only after Ready processing.
pub(crate) async fn run<P: Policy>(
    policy: &mut P,
    mut receiver: mpsc::Receiver<Input<P::Command>>,
    tick_period: Duration,
) -> Result<(), String> {
    let mut tick = tokio::time::interval(tick_period);
    policy.ready()?;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                policy.raw().tick();
                policy.ready()?;
            },
            input = receiver.recv() => match input {
                None => return Ok(()),
                Some(Input::Step(message, reply)) => {
                    let stepped = policy.raw().step(message).map_err(|error| error.to_string());
                    policy.ready()?;
                    let _ = reply.send(stepped);
                },
                Some(Input::Command(command)) => {
                    policy.command(command);
                    policy.ready()?;
                },
                Some(Input::Sent { to, snapshot, ok }) => {
                    if snapshot {
                        policy.raw().report_snapshot(to, if ok {
                            raft::SnapshotStatus::Finish
                        } else {
                            raft::SnapshotStatus::Failure
                        });
                    } else if !ok {
                        policy.raw().report_unreachable(to);
                    }
                    policy.ready()?;
                }
            }
        }
    }
}

/// One bounded, ordered queue per peer. Dropping the owner cancels every worker.
#[derive(Default)]
pub(crate) struct Outbound {
    pub(crate) queues: BTreeMap<u64, mpsc::Sender<Message>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Outbound {
    pub(crate) fn start<C, F, Fut>(
        peers: impl IntoIterator<Item = u64>,
        input: mpsc::Sender<Input<C>>,
        send: F,
    ) -> Self
    where
        C: Send + 'static,
        F: Fn(u64, Message) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let mut outbound = Self::default();
        for to in peers {
            let (tx, mut rx) = mpsc::channel::<Message>(256);
            outbound.queues.insert(to, tx);
            let (input, send) = (input.clone(), send.clone());
            outbound.tasks.push(tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                    let ok = send(to, message).await;
                    if (snapshot || !ok)
                        && input.send(Input::Sent { to, snapshot, ok }).await.is_err()
                    {
                        break;
                    }
                }
            }));
        }
        outbound
    }

    pub(crate) fn send(&self, mut messages: Vec<Message>) -> Vec<(u64, bool)> {
        // Stable sorting preserves each peer's order and removes map iteration from delivery order.
        messages.sort_by_key(|message| message.to);
        let mut failed = Vec::new();
        for message in messages {
            let to = message.to;
            let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
            if self
                .queues
                .get(&to)
                .is_none_or(|tx| tx.try_send(message).is_err())
            {
                failed.push((to, snapshot));
            }
        }
        failed
    }
}

impl Drop for Outbound {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The owner validates/applies commands and encodes its durable envelope. `persist` must
/// atomically save the machine, its applied position, and all supplied Raft state.
pub(crate) trait Machine: Clone {
    type Applied;

    fn conf(&self) -> ConfState;
    fn position(&self) -> (u64, u64);
    fn truncated(&self) -> (u64, u64);
    fn truncate(&mut self, index: u64, term: u64);
    fn restore(&mut self, snapshot: &Snapshot) -> Result<(), String>;
    fn apply(&mut self, entry: &Entry, results: &mut Vec<Self::Applied>) -> Result<(), String>;
    fn snapshot(&self) -> Result<Vec<u8>, String>;
    fn persist(
        &mut self,
        hard: &HardState,
        entries: &[Entry],
        snapshot: &Snapshot,
        engine: &dyn StorageEngine,
    ) -> Result<(), String>;
}

#[derive(Clone, Debug)]
pub(crate) struct Saved<M> {
    pub(crate) disk: M,
    pub(crate) hard: HardState,
    pub(crate) entries: Vec<Entry>,
    pub(crate) snapshot: Snapshot,
}

impl<M: Machine> Saved<M> {
    pub(crate) fn persist(&mut self, engine: &dyn StorageEngine) -> Result<(), String> {
        self.disk
            .persist(&self.hard, &self.entries, &self.snapshot, engine)
    }

    /// Validate the fixed-voter log and durable positions before exposing them to Raft.
    pub(crate) fn validate_raft(&self) -> Result<(), String> {
        let (applied, term) = self.disk.position();
        let (truncated, truncated_term) = self.disk.truncated();
        let voters = self.disk.conf();
        let meta = self.snapshot.get_metadata();
        if meta.get_conf_state() != &voters || meta.index != applied || meta.term != term {
            return Err("control snapshot metadata mismatch".into());
        }
        if truncated > applied
            || (truncated == 0) != (truncated_term == 0)
            || truncated_term > term
            || self.hard.commit < applied
            || self.hard.term < term
            || (self.hard.vote != 0 && !voters.get_voters().contains(&self.hard.vote))
        {
            return Err("invalid control durable progress".into());
        }
        for (offset, entry) in self.entries.iter().enumerate() {
            if entry.index != truncated + offset as u64 + 1
                || entry.term == 0
                || entry.term > self.hard.term
            {
                return Err("invalid control log prefix".into());
            }
        }
        if !self.entries.is_sorted_by(|a, b| a.term <= b.term) {
            return Err("control log terms regressed".into());
        }
        let last = self.entries.last().map_or(truncated, |entry| entry.index);
        if self.hard.commit > last {
            return Err("control commit exceeds durable log".into());
        }
        Ok(())
    }

    fn refresh_snapshot(&mut self) -> Result<(), String> {
        let (index, term) = self.disk.position();
        if index == 0 {
            return Ok(());
        }
        let payload = self.disk.snapshot()?;
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(self.disk.conf());
        snapshot.set_data(payload.into());
        self.snapshot = snapshot;
        Ok(())
    }

    fn compact(&mut self) -> Result<(), String> {
        let through = self.disk.position().0 - 64;
        let term = self
            .entries
            .iter()
            .find(|e| e.index == through)
            .ok_or("compaction boundary missing from log")?
            .term;
        self.entries.retain(|e| e.index > through);
        self.disk.truncate(through, term);
        Ok(())
    }
}

/// Raft reads only the last synchronously persisted state, including a real state-machine snapshot.
#[derive(Clone)]
pub(crate) struct Store<M: Machine>(pub(crate) Arc<RwLock<Saved<M>>>);

impl<M: Machine> Storage for Store<M> {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        let s = self.0.read().unwrap();
        Ok(raft::RaftState::new(s.hard.clone(), s.disk.conf()))
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let s = self.0.read().unwrap();
        if high < low {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        if low <= s.disk.truncated().0 {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        let start = (low - s.disk.truncated().0 - 1) as usize;
        let end = (high - s.disk.truncated().0 - 1) as usize;
        let Some(entries) = s.entries.get(start..end) else {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        };
        let mut bytes = 0_u64;
        let limit = max_size.into().unwrap_or(u64::MAX);
        Ok(entries
            .iter()
            .enumerate()
            .take_while(|(i, e)| {
                bytes = bytes.saturating_add(e.compute_size() as u64);
                *i == 0 || bytes <= limit
            })
            .map(|(_, e)| e.clone())
            .collect())
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        let s = self.0.read().unwrap();
        if index == s.disk.truncated().0 {
            return Ok(s.disk.truncated().1);
        }
        if index < s.disk.truncated().0 {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        s.entries
            .get((index - s.disk.truncated().0 - 1) as usize)
            .map(|e| e.term)
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.0.read().unwrap().disk.truncated().0 + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        let s = self.0.read().unwrap();
        Ok(s.entries.last().map_or(s.disk.truncated().0, |e| e.index))
    }
    fn snapshot(&self, requested: u64, _: u64) -> raft::Result<Snapshot> {
        let s = self.0.read().unwrap();
        if s.snapshot.get_metadata().index < requested {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(s.snapshot.clone())
    }
}

/// Process one Ready without publishing apply outcomes until all persistence succeeds.
/// Sends retain their original position relative to Ready/LightReady persistence.
pub(crate) fn advance<M: Machine>(
    raw: &mut RawNode<Store<M>>,
    store: &Store<M>,
    engine: &dyn StorageEngine,
    mut send: impl FnMut(Vec<Message>) -> Vec<(u64, bool)>,
) -> Result<(u64, u64, Vec<M::Applied>), String> {
    let mut ready = raw.ready();
    let before_applied = store.0.read().unwrap().disk.position().0;
    let mut applied = Vec::new();
    let durable = ready.snapshot().get_metadata().index != 0
        || ready.hs().is_some()
        || !ready.entries().is_empty()
        || !ready.committed_entries().is_empty();
    if durable {
        let mut next = store.0.read().unwrap().clone();
        let protected_commit = next.hard.commit.max(ready.snapshot().get_metadata().index);
        let snapshot = ready.snapshot();
        if snapshot.get_metadata().index != 0 {
            if snapshot.get_metadata().get_conf_state() != &next.disk.conf() {
                return Err("snapshot voter configuration mismatch".into());
            }
            next.disk.restore(snapshot)?;
            next.disk
                .truncate(snapshot.get_metadata().index, snapshot.get_metadata().term);
            next.entries.clear();
            next.snapshot = snapshot.clone();
        }
        if let Some(hard) = ready.hs() {
            next.hard = hard.clone();
        }
        for entry in ready.entries() {
            if entry.index <= next.disk.truncated().0 {
                continue;
            }
            let offset = (entry.index - next.disk.truncated().0 - 1) as usize;
            if offset > next.entries.len() {
                return Err("Raft append leaves a log gap".into());
            }
            if entry.index <= protected_commit
                && next.entries.get(offset).is_some_and(|old| old != entry)
            {
                return Err("Raft overwrote committed history".into());
            }
            next.entries.truncate(offset);
            next.entries.push(entry.clone());
        }
        for entry in ready.take_committed_entries() {
            next.disk.apply(&entry, &mut applied)?;
        }
        if next.disk.position().0 != before_applied {
            next.refresh_snapshot()?;
        }
        next.persist(engine)?;
        *store.0.write().unwrap() = next;
    }
    let mut messages = ready.take_messages();
    messages.extend(ready.take_persisted_messages());
    let mut failed_sends = send(messages);
    let mut light = raw.advance(ready);
    let entries = light.take_committed_entries();
    if light.commit_index().is_some() || !entries.is_empty() {
        let mut next = store.0.read().unwrap().clone();
        if let Some(commit) = light.commit_index() {
            next.hard.commit = commit;
        }
        for entry in entries {
            next.disk.apply(&entry, &mut applied)?;
        }
        next.refresh_snapshot()?;
        next.persist(engine)?;
        *store.0.write().unwrap() = next;
    }
    failed_sends.extend(send(light.take_messages()));
    let index = store.0.read().unwrap().disk.position().0;
    raw.advance_apply_to(index);
    // RawNode cannot be mutated while a Ready is outstanding, even to report a full outbound queue.
    for (to, snapshot) in failed_sends {
        if snapshot {
            raw.report_snapshot(to, raft::SnapshotStatus::Failure);
        } else {
            raw.report_unreachable(to);
        }
    }
    let compacted = {
        let saved = store.0.read().unwrap();
        (saved
            .disk
            .position()
            .0
            .saturating_sub(saved.disk.truncated().0)
            >= 256)
            .then(|| saved.clone())
    };
    if let Some(mut compacted) = compacted {
        compacted.compact()?;
        compacted.persist(engine)?;
        *store.0.write().unwrap() = compacted;
    }
    Ok((before_applied, index, applied))
}
