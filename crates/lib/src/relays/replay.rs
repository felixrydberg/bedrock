//! Bounded, disposable replay history. Only validated committed records are advertised.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use prost::Message;
use sha2::{Digest, Sha256};

use crate::replication::Event;
use crate::replication::pb;
use crate::storage::{
    ErrorContext, KeyRange, StorageEngine, StorageError, StorageResult, WriteBatch,
};

const PREFIX: &[u8] = b"bedrock/replay/v1/";
/// Physical usage carries WAL/SST overhead, so the disk limit leaves headroom over the logical budget.
const DISK_HEADROOM: u64 = 2;
type Identity = (String, String, u64);
type Candidates = BTreeMap<Identity, (Record, Vec<u8>)>;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct ReplayConfig {
    pub max_bytes: u64,
    pub window_ms: u64,
    /// Dataset priorities; larger numbers retain data longer under capacity pressure.
    pub priorities: BTreeMap<String, u32>,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024 * 1024,
            window_ms: 300_000,
            priorities: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
struct Record {
    arrived_at: u64,
    bytes: u64,
}

#[derive(Default)]
struct State {
    records: BTreeMap<Identity, Record>,
    age: BTreeSet<(u64, Identity)>,
    eviction: BTreeSet<(u32, u64, Identity)>,
    bytes: u64,
    failure: Option<StorageError>,
}

pub struct Replay {
    engine: Arc<dyn StorageEngine>,
    config: ReplayConfig,
    state: Mutex<State>,
}

impl Replay {
    /// Validate the complete committed cache before publishing any coverage.
    pub fn open(engine: Arc<dyn StorageEngine>, config: ReplayConfig) -> StorageResult<Self> {
        engine.health()?;
        let mut state = State::default();
        for entry in engine.scan(&KeyRange::prefix(PREFIX))? {
            let (key, value) = entry?;
            let (event, arrived_at) = decode(&key, &value)?;
            state.insert(
                &config,
                identity(&event),
                Record {
                    arrived_at,
                    bytes: (key.len() + value.len()) as u64,
                },
            );
        }
        let replay = Self {
            engine,
            config,
            state: Mutex::new(state),
        };
        {
            let mut state = replay.lock()?;
            replay.commit(&mut state, BTreeMap::new(), None)?;
        }
        Ok(replay)
    }

    /// Commit the event and capacity/window evictions atomically. Time comes from the runtime caller.
    pub fn append(&self, event: &Event, now_ms: u64) -> StorageResult<()> {
        self.append_batch(std::slice::from_ref(event), now_ms)
    }

    /// Persist a worker's bounded batch with one atomic write and no intermediate advertised revisions.
    pub fn append_batch(&self, events: &[Event], now_ms: u64) -> StorageResult<()> {
        let mut state = self.lock()?;
        let result = (|| {
            self.check(&state)?;
            let mut seen = BTreeMap::<Identity, &Event>::new();
            let mut candidates = Candidates::new();
            for event in events {
                validate(event)?;
                let id = identity(event);
                if let Some(previous) = seen.get(&id) {
                    if *previous != event {
                        return Err(corruption("conflicting events within replay batch"));
                    }
                    continue;
                }
                seen.insert(id.clone(), event);
                let record_key = key(&id);
                match self.engine.get(&record_key)? {
                    Some(value) => {
                        if decode(&record_key, &value)?.0 != *event {
                            return Err(corruption("conflicting event at an existing revision"));
                        }
                    }
                    None if state.records.contains_key(&id) => {
                        return Err(corruption("indexed replay record is missing"));
                    }
                    None => {}
                }
                if !state.records.contains_key(&id) {
                    let value = encode(event, now_ms);
                    let record = Record {
                        arrived_at: now_ms,
                        bytes: (record_key.len() + value.len()) as u64,
                    };
                    candidates.insert(id, (record, value));
                }
            }
            self.commit(&mut state, candidates, Some(now_ms))
        })();
        if let Err(error) = &result {
            state.failure = Some(error.clone());
        }
        result
    }

    /// Return contiguous events beginning at `from`; a hole is a cache miss, never skipped.
    pub fn read(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        limit: usize,
    ) -> StorageResult<Vec<Event>> {
        let mut state = self.lock()?;
        let result: StorageResult<Vec<Event>> = (|| {
            self.check(&state)?;
            let mut events = Vec::new();
            let mut sequence = from;
            for _ in 0..limit {
                let id = (dataset.to_owned(), namespace.to_owned(), sequence);
                if !state.records.contains_key(&id) {
                    break;
                }
                let key = key(&id);
                let value = self
                    .engine
                    .get(&key)?
                    .ok_or_else(|| corruption("indexed replay record is missing"))?;
                events.push(decode(&key, &value)?.0);
                let Some(next) = sequence.checked_add(1) else {
                    break;
                };
                sequence = next;
            }
            Ok(events)
        })();
        if let Err(error) = &result {
            state.failure = Some(error.clone());
        }
        result
    }

    /// Inclusive revision ranges. A disabled cache advertises no coverage.
    pub fn ranges(&self, dataset: &str, namespace: &str) -> Vec<(u64, u64)> {
        let Ok(mut state) = self.lock() else {
            return Vec::new();
        };
        if let Err(error) = self.check(&state) {
            state.failure = Some(error);
            return Vec::new();
        }
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for ((stored_dataset, stored_namespace, revision), _) in state
            .records
            .range((dataset.to_owned(), namespace.to_owned(), 0)..)
        {
            if stored_dataset != dataset || stored_namespace != namespace {
                break;
            }
            if let Some((_, end)) = ranges.last_mut()
                && end.checked_add(1) == Some(*revision)
            {
                *end = *revision;
            } else {
                ranges.push((*revision, *revision));
            }
        }
        ranges
    }

    pub fn healthy(&self) -> bool {
        let Ok(mut state) = self.lock() else {
            return false;
        };
        if let Err(error) = self.check(&state) {
            state.failure = Some(error);
        }
        state.failure.is_none()
    }

    /// Logical key and checksummed-value bytes retained by this cache.
    pub fn bytes(&self) -> u64 {
        self.lock().map(|state| state.bytes).unwrap_or(0)
    }

    /// Discard only disposable replay data after a fault. The caller schedules retries on real traffic.
    pub fn rebuild(&self) -> StorageResult<()> {
        let mut state = self.lock()?;
        let result: StorageResult<()> = (|| {
            // Deletion may repair corrupt records, so do not require a healthy engine first.
            self.engine.delete_range(KeyRange::prefix(PREFIX))?;
            self.engine.health()?;
            self.check_disk()?;
            Ok(())
        })();
        match &result {
            Ok(()) => *state = State::default(),
            Err(error) => state.failure = Some(error.clone()),
        }
        result
    }

    /// Remove aged records even while the source is idle. The caller supplies runtime time.
    pub fn prune(&self, now_ms: u64) -> StorageResult<()> {
        let mut state = self.lock()?;
        let result = self
            .check(&state)
            .and_then(|()| self.commit(&mut state, BTreeMap::new(), Some(now_ms)));
        if let Err(error) = &result {
            state.failure = Some(error.clone());
        }
        result
    }

    fn commit(
        &self,
        state: &mut State,
        candidates: Candidates,
        now_ms: Option<u64>,
    ) -> StorageResult<()> {
        let mut removed = BTreeSet::new();
        let mut bytes = state.bytes
            + candidates
                .values()
                .map(|(record, _)| record.bytes)
                .sum::<u64>();
        if let Some(now_ms) = now_ms {
            for (arrived_at, id) in &state.age {
                if now_ms.saturating_sub(*arrived_at) <= self.config.window_ms {
                    break;
                }
                removed.insert(id.clone());
                bytes -= state.records[id].bytes;
            }
        }
        let candidate_order: BTreeSet<_> = candidates
            .iter()
            .map(|(id, (record, _))| (self.priority(id), record.arrived_at, id.clone()))
            .collect();
        let mut candidate_pending = candidate_order.iter().peekable();
        let mut existing = state
            .eviction
            .iter()
            .filter(|(_, _, id)| !removed.contains(id))
            .peekable();
        let mut capacity_removed = Vec::new();
        while bytes > self.config.max_bytes {
            let use_candidate = candidate_pending
                .peek()
                .is_some_and(|order| existing.peek().is_none_or(|next| *order <= *next));
            if use_candidate {
                let (_, _, id) = candidate_pending.next().unwrap();
                bytes -= candidates[id].0.bytes;
                capacity_removed.push(id.clone());
            } else if let Some((_, _, id)) = existing.next() {
                bytes -= state.records[id].bytes;
                capacity_removed.push(id.clone());
            } else {
                break;
            }
        }
        removed.extend(capacity_removed);
        let mut batch = WriteBatch::default();
        let mut retained = Vec::new();
        for (id, (record, value)) in candidates {
            if !removed.contains(&id) {
                batch.put(key(&id), value);
                retained.push((id, record));
            }
        }
        for id in &removed {
            batch.delete(key(id));
        }
        if !batch.ops.is_empty() {
            self.engine.write(batch)?;
        }
        for id in removed {
            state.remove(&self.config, &id);
        }
        for (id, record) in retained {
            state.insert(&self.config, id, record);
        }
        self.check_disk()
    }

    fn priority(&self, id: &Identity) -> u32 {
        self.config.priorities.get(&id.0).copied().unwrap_or(0)
    }

    fn check_disk(&self) -> StorageResult<()> {
        match self.engine.disk_usage() {
            Ok(bytes) if bytes > self.config.max_bytes.saturating_mul(DISK_HEADROOM) => Err(
                StorageError::Unavailable(context("physical replay storage exceeds disk budget")),
            ),
            Ok(_) | Err(StorageError::Unsupported(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn lock(&self) -> StorageResult<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| StorageError::Unavailable(context("replay lock poisoned")))
    }

    fn check(&self, state: &State) -> StorageResult<()> {
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        self.engine.health()?;
        self.check_disk()
    }
}

impl State {
    fn insert(&mut self, config: &ReplayConfig, id: Identity, record: Record) {
        self.age.insert((record.arrived_at, id.clone()));
        self.eviction.insert((
            config.priorities.get(&id.0).copied().unwrap_or(0),
            record.arrived_at,
            id.clone(),
        ));
        self.bytes += record.bytes;
        self.records.insert(id, record);
    }

    fn remove(&mut self, config: &ReplayConfig, id: &Identity) {
        if let Some(record) = self.records.remove(id) {
            self.age.remove(&(record.arrived_at, id.clone()));
            self.eviction.remove(&(
                config.priorities.get(&id.0).copied().unwrap_or(0),
                record.arrived_at,
                id.clone(),
            ));
            self.bytes -= record.bytes;
        }
    }
}

fn identity(event: &Event) -> Identity {
    (
        event.dataset.clone(),
        event.namespace.clone(),
        event.sequence,
    )
}

fn key(id: &Identity) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    for component in [&id.0, &id.1] {
        key.extend_from_slice(&(component.len() as u64).to_be_bytes());
        key.extend_from_slice(component.as_bytes());
    }
    key.extend_from_slice(&id.2.to_be_bytes());
    key
}

fn encode(event: &Event, now_ms: u64) -> Vec<u8> {
    let mut value = now_ms.to_be_bytes().to_vec();
    value.extend_from_slice(&pb::Event::from(event.clone()).encode_to_vec());
    let digest = Sha256::digest(&value);
    value.extend_from_slice(&digest);
    value
}

fn decode(stored_key: &[u8], value: &[u8]) -> StorageResult<(Event, u64)> {
    if value.len() < 40 {
        return Err(corruption("truncated replay record"));
    }
    let (body, expected) = value.split_at(value.len() - 32);
    if Sha256::digest(body).as_slice() != expected {
        return Err(corruption("replay checksum mismatch"));
    }
    let event: Event = pb::Event::decode(&body[8..])
        .map_err(|_| corruption("invalid replay event encoding"))?
        .into();
    validate(&event)?;
    if key(&identity(&event)) != stored_key {
        return Err(corruption("replay key and payload disagree"));
    }
    let mut time = [0; 8];
    time.copy_from_slice(&body[..8]);
    Ok((event, u64::from_be_bytes(time)))
}

fn validate(event: &Event) -> StorageResult<()> {
    if event.dataset.is_empty() || event.namespace.is_empty() || event.sequence == 0 {
        return Err(corruption("invalid replay event identity"));
    }
    Ok(())
}

fn context(detail: &str) -> ErrorContext {
    ErrorContext {
        engine: "replay",
        instance: "replay".into(),
        file: None,
        partition: None,
        detail: detail.into(),
    }
}

fn corruption(detail: &str) -> StorageError {
    StorageError::Corruption(context(detail))
}

#[cfg(test)]
#[path = "../../tests/unit/relays/replay.rs"]
mod tests;
