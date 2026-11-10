//! Cell-local, fixed-voter Raft authority. Only durable committed entries reach subscribers.
//!
//! Operator retry identities are `<issued_unix_ms>:<unique>`, valid for 24 hours. Expired
//! identities are rejected rather than becoming fresh mutations after retry-record collection.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use prost::Message as ProstMessage;
use protobuf::Message as RaftMessage;
use raft::eraftpb::{ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot};
use raft::{RawNode, StateRole};

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::{DATASET, SERVICES, namespace};
use crate::reconciliation::drain::{Drain, Scope};
use crate::replication::log::Watched;
use crate::replication::materialized::{RaftPosition, View};
use crate::replication::{Event, Operation, pb};
use crate::storage::{KeyRange, StorageEngine, WriteBatch};
use crate::transport::{Caller, Class, Peers, host_of};

use crate::raft_runtime::{self as raft_core, Machine, Store};
type Saved = raft_core::Saved<Disk>;

const KEY: &[u8] = b"cell-control-v1";
const RETRY_MS: u64 = 86_400_000;
const CLOCK_SKEW_MS: u64 = 30_000;
const MAX_RETRIES: usize = 65_536;
const MAX_PENDING: usize = 128;
const MAX_WRITE: usize = 64 * 1024;
// Explicit cell control budgets, independent of the disposable product replay cache.
const MAX_VALUES: usize = 16 * 1024 * 1024;
const MAX_TAIL_BYTES: usize = 512 * 1024;
const MAX_TAIL: usize = 256;
pub const MAX_PACKET: usize = 64 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Config {
    pub region: String,
    pub dc: String,
    pub cell: String,
    pub me: String,
    pub voters: Vec<String>,
    pub voter_ids: BTreeMap<String, String>,
    pub clock: fn() -> u64,
    pub operator_token: Option<String>,
    pub bootstrap: bool,
}

impl crate::relays::LocalControlSource for Control {
    fn validate_namespace(&self, cell: &str, namespace: &str) -> Result<(), Status> {
        if cell.is_empty()
            || cell != self.config.cell
            || !SERVICES
                .iter()
                .any(|service| namespace == super::namespace(service, cell))
        {
            return Err(Status::invalid_argument(
                "membership namespace is outside this cell",
            ));
        }
        Ok(())
    }

    fn identity(&self) -> pb::ControlAck {
        Control::identity(self)
    }

    fn validate_peer(&self, peer: &pb::ControlAck) -> Result<(), Status> {
        Control::validate_peer(self, peer)
    }

    fn changed(&self) -> Arc<Watched<u64>> {
        Control::changed(self)
    }

    fn head(&self, namespace: &str) -> Result<u64, Status> {
        Control::head(self, namespace)
    }

    fn read(&self, namespace: &str, from: u64, limit: usize) -> Result<Vec<Event>, Status> {
        Control::read(self, namespace, from, limit)
    }

    fn view(&self, namespace: &str) -> Result<View, Status> {
        Control::view(self, namespace)
    }
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Checked {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    digest: Vec<u8>,
}

fn checked(payload: Vec<u8>) -> Vec<u8> {
    Checked {
        digest: Sha256::digest(&payload).to_vec(),
        payload,
    }
    .encode_to_vec()
}

fn unchecked(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let record = Checked::decode(bytes).map_err(|e| e.to_string())?;
    if record.digest != Sha256::digest(&record.payload).as_slice() {
        return Err("cell control checksum mismatch".into());
    }
    Ok(record.payload)
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Pair {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Namespace {
    #[prost(uint64, tag = "1")]
    head: u64,
    #[prost(uint64, tag = "2")]
    at: u64,
    #[prost(uint64, tag = "3")]
    pruned: u64,
    #[prost(message, repeated, tag = "4")]
    values: Vec<Pair>,
    #[prost(message, repeated, tag = "5")]
    events: Vec<pb::Event>,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Retry {
    #[prost(bytes = "vec", tag = "1")]
    digest: Vec<u8>,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(uint64, tag = "3")]
    expires: u64,
    #[prost(uint32, tag = "4")]
    failure: u32,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct State {
    #[prost(uint64, tag = "1")]
    applied: u64,
    #[prost(uint64, tag = "2")]
    term: u64,
    #[prost(uint64, tag = "3")]
    time: u64,
    #[prost(btree_map = "string, message", tag = "4")]
    namespaces: BTreeMap<String, Namespace>,
    #[prost(btree_map = "string, message", tag = "5")]
    retries: BTreeMap<String, Retry>,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Disk {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    cell: String,
    #[prost(string, repeated, tag = "3")]
    voters: Vec<String>,
    #[prost(bytes = "vec", tag = "4")]
    hard: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    entries: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "6")]
    snapshot: Vec<u8>,
    #[prost(uint64, tag = "7")]
    truncated: u64,
    #[prost(uint64, tag = "8")]
    truncated_term: u64,
    #[prost(message, optional, tag = "9")]
    state: Option<State>,
    #[prost(string, tag = "10")]
    region: String,
    #[prost(string, tag = "11")]
    dc: String,
    #[prost(string, tag = "12")]
    me: String,
    #[prost(btree_map = "string, string", tag = "13")]
    voter_ids: BTreeMap<String, String>,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Command {
    #[prost(message, optional, tag = "1")]
    request: Option<pb::ControlWrite>,
    #[prost(uint64, tag = "2")]
    at: u64,
}

impl Saved {
    fn state(&self) -> &State {
        self.disk.state.as_ref().expect("validated control state")
    }
    fn open(engine: &dyn StorageEngine, config: &Config) -> Result<Self, String> {
        engine.health().map_err(|e| e.to_string())?;
        let bytes = engine.get(KEY).map_err(|e| e.to_string())?;
        let Some(bytes) = bytes else {
            if !config.bootstrap {
                return Err("empty control disk requires explicit cell bootstrap".into());
            }
            if engine
                .scan(&KeyRange::prefix(Vec::new()))
                .map_err(|e| e.to_string())?
                .next()
                .is_some()
            {
                return Err("control disk contains data without its identity record".into());
            }
            let mut snapshot = Snapshot::default();
            snapshot
                .mut_metadata()
                .set_conf_state(conf(config.voters.len()));
            let mut saved = Self {
                disk: Disk {
                    version: 1,
                    region: config.region.clone(),
                    dc: config.dc.clone(),
                    me: config.me.clone(),
                    cell: config.cell.clone(),
                    voters: config.voters.clone(),
                    voter_ids: config.voter_ids.clone(),
                    state: Some(State {
                        namespaces: SERVICES
                            .iter()
                            .map(|service| (namespace(service, &config.cell), Namespace::default()))
                            .collect(),
                        ..State::default()
                    }),
                    ..Disk::default()
                },
                hard: HardState::default(),
                entries: Vec::new(),
                snapshot,
            };
            saved.persist(engine)?;
            return Ok(saved);
        };
        let disk = Disk::decode(unchecked(&bytes)?.as_slice()).map_err(|e| e.to_string())?;
        if disk.version != 1
            || disk.region != config.region
            || disk.dc != config.dc
            || disk.me != config.me
            || disk.cell != config.cell
            || disk.voters != config.voters
            || disk.voter_ids != config.voter_ids
        {
            return Err("control disk format/cell/voter configuration mismatch".into());
        }
        let saved = Self {
            hard: HardState::parse_from_bytes(&disk.hard).map_err(|e| e.to_string())?,
            entries: disk
                .entries
                .iter()
                .map(|e| Entry::parse_from_bytes(e).map_err(|e| e.to_string()))
                .collect::<Result<_, _>>()?,
            snapshot: Snapshot::parse_from_bytes(&disk.snapshot).map_err(|e| e.to_string())?,
            disk,
        };
        saved.validate()?;
        Ok(saved)
    }

    fn validate(&self) -> Result<(), String> {
        let state = self.disk.state.as_ref().ok_or("missing control state")?;
        validate_state(state, &self.disk.cell)?;
        self.validate_raft()?;
        if state.applied > 0
            && State::decode(unchecked(&self.snapshot.data)?.as_slice())
                .map_err(|e| e.to_string())?
                != *state
        {
            return Err("control snapshot differs from applied state".into());
        }
        Ok(())
    }
}

fn conf(count: usize) -> ConfState {
    let mut state = ConfState::default();
    state.set_voters((1..=count as u64).collect());
    state
}

pub(crate) fn validate_voter_ids(
    voters: &[String],
    identities: &BTreeMap<String, String>,
) -> Result<(), String> {
    let unique: std::collections::BTreeSet<_> = identities.values().collect();
    if identities.len() != voters.len()
        || unique.len() != voters.len()
        || identities.values().any(|id| !super::token(id))
        || voters
            .iter()
            .any(|address| !identities.contains_key(address))
    {
        return Err(
            "control voters require one distinct node identity per configured address".into(),
        );
    }
    Ok(())
}

fn validate_state(state: &State, cell: &str) -> Result<(), String> {
    if state.namespaces.len() != SERVICES.len() || state.retries.len() > MAX_RETRIES {
        return Err("invalid control state cardinality".into());
    }
    for service in SERVICES {
        let name = namespace(service, cell);
        let ns = state
            .namespaces
            .get(&name)
            .ok_or("missing control namespace")?;
        if ns.pruned > ns.head
            || ns.at > state.time
            || !ns.values.is_sorted_by(|a, b| a.key < b.key)
            || ns.events.len() > MAX_TAIL
        {
            return Err("invalid control namespace state".into());
        }
        if ns.events.len() as u64 != ns.head - ns.pruned {
            return Err("control event tail gap".into());
        }
        for (offset, event) in ns.events.iter().enumerate() {
            if event.sequence != ns.pruned + offset as u64 + 1
                || event.dataset != DATASET
                || event.namespace != name
                || event.committed_at > ns.at
            {
                return Err("invalid control event tail".into());
            }
        }
    }
    if state
        .namespaces
        .values()
        .flat_map(|n| &n.values)
        .map(|p| p.key.len() + p.value.len())
        .sum::<usize>()
        > MAX_VALUES
        || state.retries.iter().any(|(identity, retry)| {
            expiry(identity).ok() != Some(retry.expires)
                || retry.digest.len() != 32
                || !matches!(retry.failure, 0 | 8 | 10)
        })
    {
        return Err("invalid cell control value/retry state".into());
    }
    Ok(())
}

type Input = raft_core::Input<(pb::ControlWrite, Waiter)>;

struct Leadership {
    leader: u64,
    term: u64,
    ready: bool,
    failed: Option<String>,
}

pub struct Control {
    config: Config,
    id: u64,
    store: Store<Disk>,
    peers: Arc<Peers>,
    input: mpsc::Sender<Input>,
    leadership: RwLock<Leadership>,
    changed: Arc<Watched<u64>>,
}

impl Control {
    pub fn start(
        engine: Arc<dyn StorageEngine>,
        peers: Arc<Peers>,
        mut config: Config,
    ) -> Result<(Arc<Self>, tokio::task::JoinHandle<()>), String> {
        config.voters.sort();
        validate_voter_ids(&config.voters, &config.voter_ids)?;
        if !super::token(&config.region)
            || !super::token(&config.dc)
            || !super::token(&config.cell)
            || config.voters.len() < 3
            || config.voters.windows(2).any(|p| p[0] == p[1])
            || config.voters.iter().any(|v| v.is_empty())
        {
            return Err(
                "cell control requires a cell and at least three distinct configured voters".into(),
            );
        }
        let id = config
            .voters
            .iter()
            .position(|v| v == &config.me)
            .ok_or("local relay is not a configured control voter")? as u64
            + 1;
        let saved = Saved::open(&*engine, &config)?;
        let applied = saved.state().applied;
        let term = saved.hard.term;
        let store = Store(Arc::new(RwLock::new(saved)));
        // A singleton election range removes raft-rs's unseeded random timeout choice.
        let election = 10 + id as usize * 3;
        let raft_config = raft::Config {
            id,
            election_tick: 10,
            heartbeat_tick: 2,
            min_election_tick: election,
            max_election_tick: election + 1,
            applied,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 128 * 1024,
            max_uncommitted_size: 1024 * 1024,
            max_inflight_msgs: 16,
            max_committed_size_per_ready: 256 * 1024,
            ..raft::Config::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&raft_config, store.clone(), &logger).map_err(|e| e.to_string())?;
        let (input, receiver) = mpsc::channel(256);
        let control = Arc::new(Self {
            config,
            id,
            store,
            peers,
            input,
            leadership: RwLock::new(Leadership {
                leader: 0,
                term,
                ready: false,
                failed: None,
            }),
            changed: Arc::new(Watched::new(0)),
        });
        let actor = Actor {
            control: control.clone(),
            engine,
            raw,
            pending: BTreeMap::new(),
            serial: 0,
            proposal_time: 0,
            outbound: raft_core::Outbound::default(),
        };
        let task = tokio::spawn(actor.run(receiver));
        Ok((control, task))
    }

    pub fn changed(&self) -> Arc<Watched<u64>> {
        self.changed.clone()
    }
    pub fn identity(&self) -> pb::ControlAck {
        pb::ControlAck {
            cell: self.config.cell.clone(),
            region: self.config.region.clone(),
            dc: self.config.dc.clone(),
            voters: self.config.voters.clone(),
            voter_ids: self.config.voter_ids.clone(),
        }
    }
    pub fn validate_peer(&self, peer: &pb::ControlAck) -> Result<(), Status> {
        if peer != &self.identity() {
            return Err(Status::permission_denied(
                "cell peer location/voter configuration mismatch",
            ));
        }
        Ok(())
    }
    async fn send(&self, to: u64, message: Message) -> Result<(), Status> {
        let address = &self.config.voters[to as usize - 1];
        let channel = self
            .peers
            .channel(address, Class::Control)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut client = pb::cell_control_client::CellControlClient::new(channel)
            .max_decoding_message_size(MAX_PACKET)
            .max_encoding_message_size(MAX_PACKET);
        let payload = message
            .write_to_bytes()
            .map_err(|error| Status::internal(error.to_string()))?;
        let ack = self
            .peers
            .call(
                address,
                Class::Control,
                RPC_TIMEOUT,
                client.step(pb::ControlPacket {
                    cell: self.config.cell.clone(),
                    region: self.config.region.clone(),
                    dc: self.config.dc.clone(),
                    voters: self.config.voters.clone(),
                    voter_ids: self.config.voter_ids.clone(),
                    payload,
                }),
            )
            .await?
            .into_inner();
        self.validate_peer(&ack)
    }
    pub fn is_leader(&self) -> bool {
        let l = self.leadership.read().unwrap();
        l.failed.is_none() && l.ready && l.leader == self.id
    }
    pub fn term(&self) -> u64 {
        self.leadership.read().unwrap().term
    }
    fn healthy(&self) -> Result<(), Status> {
        if let Some(error) = &self.leadership.read().unwrap().failed {
            return Err(Status::unavailable(error.clone()));
        }
        Ok(())
    }
    pub fn head(&self, namespace: &str) -> Result<u64, Status> {
        self.healthy()?;
        self.store
            .0
            .read()
            .unwrap()
            .state()
            .namespaces
            .get(namespace)
            .map(|n| n.head)
            .ok_or_else(|| Status::invalid_argument("unknown cell control namespace"))
    }
    pub fn view(&self, namespace: &str) -> Result<View, Status> {
        self.healthy()?;
        let saved = self.store.0.read().unwrap();
        let state = saved.state();
        let ns = state
            .namespaces
            .get(namespace)
            .ok_or_else(|| Status::invalid_argument("unknown cell control namespace"))?;
        Ok(View {
            dataset: DATASET.into(),
            namespace: namespace.into(),
            sequence: ns.head,
            committed_at: ns.at,
            retries: BTreeMap::new(),
            values: ns
                .values
                .iter()
                .map(|p| (p.key.clone(), p.value.clone()))
                .collect(),
            pruned_through: ns.pruned,
            raft: (state.applied > 0).then_some(RaftPosition {
                index: state.applied,
                term: state.term,
            }),
        })
    }
    pub fn read(&self, namespace: &str, from: u64, limit: usize) -> Result<Vec<Event>, Status> {
        self.healthy()?;
        let saved = self.store.0.read().unwrap();
        let ns = saved
            .state()
            .namespaces
            .get(namespace)
            .ok_or_else(|| Status::invalid_argument("unknown cell control namespace"))?;
        if from <= ns.pruned {
            return Err(Status::out_of_range(
                "cell control cursor predates retained history",
            ));
        }
        Ok(ns
            .events
            .iter()
            .filter(|e| e.sequence >= from)
            .take(limit)
            .cloned()
            .map(Event::from)
            .collect())
    }
    pub async fn write(&self, request: pb::ControlWrite) -> Result<u64, Status> {
        self.healthy()?;
        if !self.is_leader() {
            if request.expected_term != 0 {
                return Err(Status::failed_precondition(
                    "cell planner is not the current leader",
                ));
            }
            validate_operator(&self.config, &request)?;
            let leader = self.leadership.read().unwrap().leader;
            let address = self
                .config
                .voters
                .get(leader.wrapping_sub(1) as usize)
                .ok_or_else(|| Status::unavailable("cell has no known leader"))?;
            if leader == self.id {
                return Err(Status::unavailable("cell leadership is changing"));
            }
            let channel = self
                .peers
                .channel(address, Class::Control)
                .map_err(|e| Status::unavailable(e.to_string()))?;
            let mut client = pb::cell_control_client::CellControlClient::new(channel)
                .max_decoding_message_size(MAX_PACKET)
                .max_encoding_message_size(MAX_PACKET);
            let mut rpc = Request::new(request);
            let token = self
                .config
                .operator_token
                .as_deref()
                .ok_or_else(|| Status::permission_denied("operator writes disabled"))?;
            rpc.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .map_err(|_| Status::internal("invalid configured operator token"))?,
            );
            rpc.metadata_mut().insert(
                "bedrock-cell-forwarded",
                "1".parse().expect("static metadata"),
            );
            return self
                .peers
                .call(address, Class::Control, WRITE_TIMEOUT, client.write(rpc))
                .await
                .map(|r| r.into_inner().revision);
        }
        let (tx, rx) = oneshot::channel();
        tokio::time::timeout(WRITE_TIMEOUT, async {
            self.input
                .send(Input::Command((request, tx)))
                .await
                .map_err(|_| Status::unavailable("cell control stopped"))?;
            rx.await
                .map_err(|_| Status::unavailable("cell control stopped"))?
        })
        .await
        .map_err(|_| Status::deadline_exceeded("cell control write did not commit"))?
    }
}

type Waiter = oneshot::Sender<Result<u64, Status>>;
struct Actor {
    control: Arc<Control>,
    engine: Arc<dyn StorageEngine>,
    raw: RawNode<Store<Disk>>,
    pending: BTreeMap<String, (Vec<u8>, Vec<Waiter>)>,
    serial: u64,
    proposal_time: u64,
    outbound: raft_core::Outbound,
}

impl Actor {
    async fn run(mut self, receiver: mpsc::Receiver<Input>) {
        let control = self.control.clone();
        self.outbound = raft_core::Outbound::start(
            (1..=control.config.voters.len() as u64).filter(|to| *to != control.id),
            control.input.clone(),
            {
                let control = control.clone();
                move |to, message| {
                    let control = control.clone();
                    async move { control.send(to, message).await.is_ok() }
                }
            },
        );
        let result = raft_core::run(&mut self, receiver, Duration::from_millis(100)).await;
        let error = result
            .err()
            .unwrap_or_else(|| "cell control stopped".into());
        tracing::error!(%error, "cell control authority stopped");
        {
            let mut l = self.control.leadership.write().unwrap();
            l.failed = Some(error.clone());
            l.leader = 0;
        }
        self.control.changed.update(|v| *v += 1);
        for (_, (_, waiting)) in self.pending {
            for reply in waiting {
                let _ = reply.send(Err(Status::unavailable(error.clone())));
            }
        }
    }

    fn propose(&mut self, mut request: pb::ControlWrite, reply: Waiter) {
        let result = (|| {
            if self.raw.raft.state != StateRole::Leader {
                return Err(Status::failed_precondition(
                    "cell planner is not the current leader",
                ));
            }
            if request.expected_term != 0 && request.expected_term != self.raw.raft.term {
                return Err(Status::aborted("cell planner leadership term changed"));
            }
            if request.expected_term != 0 && request.expected.is_none() {
                return Err(Status::invalid_argument(
                    "cell planner requires an expected namespace revision",
                ));
            }
            let saved = self.control.store.0.read().unwrap();
            let state = saved.state();
            if state.term != self.raw.raft.term {
                return Err(Status::unavailable(
                    "cell leader is recovering its committed prefix",
                ));
            }
            let now = (self.control.config.clock)()
                .max(state.time)
                .max(self.proposal_time);
            if request.idempotency_key.is_empty() {
                if request.expected_term == 0 {
                    return Err(Status::invalid_argument(
                        "operator write requires an idempotency key",
                    ));
                }
                self.serial = self
                    .serial
                    .checked_add(1)
                    .ok_or_else(|| Status::internal("cell request counter exhausted"))?;
                request.idempotency_key = format!(
                    "{now}:{}-{}-{}",
                    self.control.id, self.raw.raft.term, self.serial
                );
            }
            validate_request(&self.control.config, state, &request, now)?;
            let digest = request_digest(&request);
            if request.expected_term == 0
                && let Some(old) = state.retries.get(&request.idempotency_key)
            {
                return Ok(Some(retry_result(old, &digest)));
            }
            if let Some((prior, _)) = self.pending.get(&request.idempotency_key) {
                if *prior != digest {
                    return Err(Status::already_exists(
                        "idempotency identity reused with different mutation",
                    ));
                }
                if self.pending[&request.idempotency_key].1.len() >= MAX_PENDING {
                    return Err(Status::resource_exhausted(
                        "cell duplicate request waiters full",
                    ));
                }
                return Ok(None);
            }
            if self.pending.len() >= MAX_PENDING {
                return Err(Status::resource_exhausted("cell proposal queue full"));
            }
            if request.expected_term == 0 {
                validate_drain_target(&self.control.config, state, &request)?;
            }
            if request.expected_term == 0
                && state.retries.values().filter(|r| r.expires > now).count() + self.pending.len()
                    >= MAX_RETRIES
            {
                return Err(Status::resource_exhausted("cell retry window full"));
            }
            drop(saved);
            self.raw
                .propose(
                    Vec::new(),
                    Command {
                        request: Some(request.clone()),
                        at: now,
                    }
                    .encode_to_vec(),
                )
                .map_err(|e| Status::unavailable(e.to_string()))?;
            self.proposal_time = now;
            Ok(None)
        })();
        match result {
            Err(error) => {
                let _ = reply.send(Err(error));
            }
            Ok(Some(result)) => {
                let _ = reply.send(result);
            }
            Ok(None) => {
                self.pending
                    .entry(request.idempotency_key.clone())
                    .or_insert_with(|| (request_digest(&request), Vec::new()))
                    .1
                    .push(reply);
            }
        }
    }

    fn ready(&mut self) -> Result<(), String> {
        while self.raw.has_ready() {
            let (before_applied, index, applied) = raft_core::advance(
                &mut self.raw,
                &self.control.store,
                &*self.engine,
                |messages| self.outbound.send(messages),
            )?;
            for (id, result) in applied {
                if let Some((_, waiting)) = self.pending.remove(&id) {
                    for reply in waiting {
                        let _ = reply.send(result.clone());
                    }
                }
            }
            let mut leadership = self.control.leadership.write().unwrap();
            let changed = leadership.leader != self.raw.raft.leader_id
                || leadership.term != self.raw.raft.term;
            leadership.leader = self.raw.raft.leader_id;
            leadership.term = self.raw.raft.term;
            leadership.ready =
                self.control.store.0.read().unwrap().state().term == self.raw.raft.term;
            drop(leadership);
            if changed || index != before_applied {
                self.control.changed.update(|v| *v += 1);
            }
            if self.raw.raft.state != StateRole::Leader {
                let pending = std::mem::take(&mut self.pending);
                for (_, (_, waiting)) in pending {
                    for reply in waiting {
                        let _ = reply.send(Err(Status::aborted(
                            "cell leadership changed before write acknowledgement",
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl raft_core::Policy for Actor {
    type Machine = Disk;
    type Command = (pb::ControlWrite, Waiter);

    fn raw(&mut self) -> &mut RawNode<Store<Disk>> {
        &mut self.raw
    }

    fn command(&mut self, (request, reply): Self::Command) {
        self.propose(request, reply);
    }

    fn ready(&mut self) -> Result<(), String> {
        Actor::ready(self)
    }
}

fn request_digest(request: &pb::ControlWrite) -> Vec<u8> {
    let mut request = request.clone();
    request.expected_term = 0;
    Sha256::digest(request.encode_to_vec()).to_vec()
}

fn expiry(identity: &str) -> Result<u64, Status> {
    let (issued, unique) = identity.split_once(':').ok_or_else(|| {
        Status::invalid_argument("idempotency key must be <issued_unix_ms>:<unique>")
    })?;
    if unique.is_empty() || identity.len() > 256 {
        return Err(Status::invalid_argument("invalid idempotency identity"));
    }
    issued
        .parse::<u64>()
        .ok()
        .and_then(|at| at.checked_add(RETRY_MS))
        .ok_or_else(|| Status::invalid_argument("invalid idempotency timestamp"))
}

fn validate_request(
    config: &Config,
    state: &State,
    request: &pb::ControlWrite,
    now: u64,
) -> Result<(), Status> {
    if !state.namespaces.contains_key(&request.namespace)
        || request.key.is_empty()
        || request.key.len() > 4096
        || request.value.as_ref().is_some_and(|v| v.len() > MAX_WRITE)
    {
        return Err(Status::invalid_argument("invalid cell control mutation"));
    }
    let expires = expiry(&request.idempotency_key)?;
    if expires <= now {
        return Err(Status::failed_precondition("idempotency identity expired"));
    }
    if expires - RETRY_MS > now.saturating_add(CLOCK_SKEW_MS) {
        return Err(Status::invalid_argument(
            "idempotency timestamp is in the future",
        ));
    }
    if request.expected_term == 0 {
        validate_operator(config, request)?;
    }
    let live: usize = state
        .namespaces
        .values()
        .flat_map(|n| &n.values)
        .map(|p| p.key.len() + p.value.len())
        .sum();
    let old = state.namespaces[&request.namespace]
        .values
        .iter()
        .find(|p| p.key == request.key)
        .map_or(0, |p| p.key.len() + p.value.len());
    if !state.retries.contains_key(&request.idempotency_key)
        && live - old
            + request
                .value
                .as_ref()
                .map_or(0, |v| v.len() + request.key.len())
            > MAX_VALUES
    {
        return Err(Status::resource_exhausted(
            "cell control live-state budget exceeded",
        ));
    }
    Ok(())
}

fn retry_result(retry: &Retry, digest: &[u8]) -> Result<u64, Status> {
    if retry.digest != digest {
        return Err(Status::already_exists(
            "idempotency identity reused with different mutation",
        ));
    }
    match retry.failure {
        0 => Ok(retry.revision),
        8 => Err(Status::resource_exhausted(
            "cell control live-state budget exceeded",
        )),
        _ => Err(Status::aborted(
            "cell namespace moved past the expected revision",
        )),
    }
}

fn apply_entry(
    state: &mut State,
    entry: &Entry,
    results: &mut Vec<(String, Result<u64, Status>)>,
) -> Result<(), String> {
    if entry.index <= state.applied {
        return Ok(());
    }
    if entry.index != state.applied + 1 || entry.get_entry_type() != EntryType::EntryNormal {
        return Err("invalid committed cell log entry".into());
    }
    if !entry.data.is_empty() {
        let command = Command::decode(entry.data.as_ref()).map_err(|e| e.to_string())?;
        let request = command.request.ok_or("cell command missing mutation")?;
        if command.at < state.time {
            return Err("cell command time regressed".into());
        }
        state.time = command.at;
        state.retries.retain(|_, retry| retry.expires > command.at);
        let digest = request_digest(&request);
        let result = if request.expected_term == 0
            && let Some(retry) = state.retries.get(&request.idempotency_key)
        {
            retry_result(retry, &digest)
        } else {
            let expires = expiry(&request.idempotency_key).map_err(|e| e.to_string())?;
            if expires <= command.at {
                return Err("committed cell request already expired".into());
            }
            let live: usize = state
                .namespaces
                .values()
                .flat_map(|n| &n.values)
                .map(|p| p.key.len() + p.value.len())
                .sum();
            let ns = state
                .namespaces
                .get_mut(&request.namespace)
                .ok_or("committed cell request has unknown namespace")?;
            let conflict = request.expected.is_some_and(|base| base != ns.head);
            let old = ns
                .values
                .iter()
                .find(|p| p.key == request.key)
                .map_or(0, |p| p.key.len() + p.value.len());
            let full = live - old
                + request
                    .value
                    .as_ref()
                    .map_or(0, |v| v.len() + request.key.len())
                > MAX_VALUES;
            if !conflict && !full {
                ns.head = ns
                    .head
                    .checked_add(1)
                    .ok_or("cell namespace revision exhausted")?;
                ns.at = command.at;
                match ns
                    .values
                    .binary_search_by(|pair| pair.key.cmp(&request.key))
                {
                    Ok(index) => {
                        if let Some(value) = &request.value {
                            ns.values[index].value = value.clone();
                        } else {
                            ns.values.remove(index);
                        }
                    }
                    Err(index) => {
                        if let Some(value) = &request.value {
                            ns.values.insert(
                                index,
                                Pair {
                                    key: request.key.clone(),
                                    value: value.clone(),
                                },
                            );
                        }
                    }
                }
                let event = Event {
                    dataset: DATASET.into(),
                    namespace: request.namespace.clone(),
                    stream: crate::replication::stream(DATASET, 0),
                    sequence: ns.head,
                    epoch: 0,
                    committed_at: command.at,
                    key: request.key.clone(),
                    operation: request
                        .value
                        .clone()
                        .map_or(Operation::Delete, Operation::Set),
                };
                ns.events.push(event.into());
                let mut bytes: usize = ns.events.iter().map(ProstMessage::encoded_len).sum();
                while ns.events.len() > MAX_TAIL || bytes > MAX_TAIL_BYTES {
                    let removed = ns.events.remove(0);
                    bytes -= removed.encoded_len();
                    ns.pruned = removed.sequence;
                }
            }
            let retry = Retry {
                digest: digest.clone(),
                revision: ns.head,
                expires,
                failure: if conflict {
                    10
                } else if full {
                    8
                } else {
                    0
                },
            };
            let result = retry_result(&retry, &digest);
            if request.expected_term == 0 {
                state.retries.insert(request.idempotency_key.clone(), retry);
            }
            result
        };
        results.push((request.idempotency_key, result));
    }
    state.applied = entry.index;
    state.term = entry.term;
    Ok(())
}

impl Machine for Disk {
    type Applied = (String, Result<u64, Status>);

    fn conf(&self) -> ConfState {
        conf(self.voters.len())
    }

    fn position(&self) -> (u64, u64) {
        let state = self.state.as_ref().expect("validated control state");
        (state.applied, state.term)
    }

    fn truncated(&self) -> (u64, u64) {
        (self.truncated, self.truncated_term)
    }

    fn truncate(&mut self, index: u64, term: u64) {
        self.truncated = index;
        self.truncated_term = term;
    }

    fn restore(&mut self, snapshot: &Snapshot) -> Result<(), String> {
        let state =
            State::decode(unchecked(&snapshot.data)?.as_slice()).map_err(|e| e.to_string())?;
        validate_state(&state, &self.cell)?;
        if state.applied != snapshot.get_metadata().index
            || state.term != snapshot.get_metadata().term
            || state.applied < self.position().0
        {
            return Err("snapshot applied position mismatch".into());
        }
        self.state = Some(state);
        Ok(())
    }

    fn apply(&mut self, entry: &Entry, results: &mut Vec<Self::Applied>) -> Result<(), String> {
        apply_entry(
            self.state.as_mut().expect("validated control state"),
            entry,
            results,
        )
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        let payload = checked(
            self.state
                .as_ref()
                .expect("validated control state")
                .encode_to_vec(),
        );
        if payload.len() > MAX_PACKET - 1024 {
            return Err("cell snapshot exceeds transport budget".into());
        }
        Ok(payload)
    }

    fn persist(
        &mut self,
        hard: &HardState,
        entries: &[Entry],
        snapshot: &Snapshot,
        engine: &dyn StorageEngine,
    ) -> Result<(), String> {
        self.hard = hard.write_to_bytes().map_err(|e| e.to_string())?;
        self.entries = entries
            .iter()
            .map(|e| e.write_to_bytes().map_err(|e| e.to_string()))
            .collect::<Result<_, _>>()?;
        self.snapshot = snapshot.write_to_bytes().map_err(|e| e.to_string())?;
        let mut batch = WriteBatch::default();
        batch.put(KEY, checked(self.encode_to_vec()));
        engine.write(batch).map_err(|e| e.to_string())
    }
}

fn validate_operator(config: &Config, request: &pb::ControlWrite) -> Result<(), Status> {
    if request.expected_term != 0 {
        return Err(Status::permission_denied(
            "operator may write only this cell's drain records",
        ));
    }
    let Some(service) = SERVICES
        .iter()
        .copied()
        .find(|service| request.namespace == namespace(service, &config.cell))
    else {
        return Err(Status::permission_denied(
            "operator may write only this cell's drain records",
        ));
    };
    let key = std::str::from_utf8(&request.key)
        .map_err(|_| Status::invalid_argument("drain key is not UTF-8"))?;
    let id = key
        .strip_prefix("drain/")
        .filter(|id| super::token(id))
        .ok_or_else(|| Status::permission_denied("operator may write only drain/<id>"))?;
    if id.len() > 128 {
        return Err(Status::invalid_argument("drain identity too long"));
    }
    if let Some(bytes) = &request.value {
        let drain = Drain::decode(bytes).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if drain.state != crate::reconciliation::drain::State::Draining {
            return Err(Status::invalid_argument(
                "operators may create only draining records",
            ));
        }
        validate_drain(&config.cell, service, id, &drain)?;
    }
    Ok(())
}

pub(super) fn validate_drain(
    cell: &str,
    service: &str,
    id: &str,
    drain: &Drain,
) -> Result<(), Status> {
    if id.len() > 128 || !super::token(id) {
        return Err(Status::invalid_argument("invalid drain identity"));
    }
    if match &drain.scope {
        Scope::Node(node) => !super::token(node),
        Scope::Cell(target) => target != cell,
    } || crate::reconciliation::drain::validate_ranged_selection(service, drain).is_err()
    {
        return Err(Status::invalid_argument(
            "unsupported drain scope, namespace, dataset, or state",
        ));
    }
    Ok(())
}

fn validate_drain_target(
    config: &Config,
    state: &State,
    request: &pb::ControlWrite,
) -> Result<(), Status> {
    let Some(bytes) = &request.value else {
        return Ok(());
    };
    let drain = Drain::decode(bytes).map_err(|e| Status::invalid_argument(e.to_string()))?;
    let Scope::Node(node) = drain.scope else {
        return Ok(());
    };
    let service = SERVICES
        .iter()
        .copied()
        .find(|service| request.namespace == namespace(service, &config.cell))
        .ok_or_else(|| {
            Status::permission_denied("operator may write only this cell's drain records")
        })?;
    let ns = state
        .namespaces
        .get(&request.namespace)
        .ok_or_else(|| Status::invalid_argument("unknown cell control namespace"))?;
    let pair = ns
        .values
        .iter()
        .find(|pair| pair.key.as_slice() == node.as_bytes())
        .ok_or_else(|| Status::failed_precondition("drain target is not a member in this cell"))?;
    let member = pb::Member::decode(pair.value.as_slice())
        .map_err(|_| Status::data_loss("drain target membership is corrupt"))?;
    if member.node_id != node || member.service != service || member.cell != config.cell {
        return Err(Status::failed_precondition(
            "drain target is not a member in this cell",
        ));
    }
    Ok(())
}

fn validate_step(
    control: &Control,
    request: &Request<pb::ControlPacket>,
) -> Result<Message, Status> {
    control.healthy()?;
    #[cfg(not(madsim))]
    let caller = Some(
        Caller::of(request)
            .ok_or_else(|| Status::unauthenticated("cell Raft traffic requires mutual TLS"))?,
    );
    #[cfg(madsim)]
    let caller = Caller::of(request);
    if let Some(caller) = caller {
        caller.require(&["relay"], Some(&control.config.cell))?;
    }
    let packet = request.get_ref();
    if packet.cell != control.config.cell
        || packet.region != control.config.region
        || packet.dc != control.config.dc
        || packet.voters != control.config.voters
        || packet.voter_ids != control.config.voter_ids
    {
        return Err(Status::permission_denied(
            "cell peer location/voter configuration mismatch",
        ));
    }
    if packet.payload.len() > MAX_PACKET {
        return Err(Status::invalid_argument("invalid cell Raft packet"));
    }
    let message = Message::parse_from_bytes(&packet.payload)
        .map_err(|_| Status::invalid_argument("malformed Raft packet"))?;
    if message.to != control.id
        || message.from == control.id
        || message.from == 0
        || message.from > control.config.voters.len() as u64
        || raft::raw_node::is_local_msg(message.get_msg_type())
        || message.get_msg_type() == MessageType::MsgPropose
    {
        return Err(Status::permission_denied(
            "Raft packet is not between configured voters",
        ));
    }
    if let Some(caller) = caller {
        let address = &control.config.voters[message.from as usize - 1];
        if !caller.has_host(host_of(address))
            || control.config.voter_ids.get(address) != Some(&caller.node_id)
        {
            return Err(Status::permission_denied(
                "Raft sender differs from verified relay",
            ));
        }
    }
    Ok(message)
}

pub struct Server(pub Option<Arc<Control>>);

#[tonic::async_trait]
impl pb::cell_control_server::CellControl for Server {
    async fn step(
        &self,
        request: Request<pb::ControlPacket>,
    ) -> Result<Response<pb::ControlAck>, Status> {
        let control = self
            .0
            .as_ref()
            .ok_or_else(|| Status::unavailable("cell control disabled"))?;
        let message = validate_step(control, &request)?;
        let (tx, rx) = oneshot::channel();
        tokio::time::timeout(RPC_TIMEOUT, async {
            control
                .input
                .send(Input::Step(message, tx))
                .await
                .map_err(|_| Status::unavailable("cell control stopped"))?;
            rx.await
                .map_err(|_| Status::unavailable("cell control stopped"))?
                .map_err(Status::invalid_argument)
        })
        .await
        .map_err(|_| Status::deadline_exceeded("cell Raft step timed out"))??;
        Ok(Response::new(control.identity()))
    }

    async fn write(
        &self,
        request: Request<pb::ControlWrite>,
    ) -> Result<Response<pb::ControlWritten>, Status> {
        #[cfg(not(madsim))]
        if Caller::of(&request).is_none() {
            return Err(Status::unauthenticated(
                "cell operator traffic requires mutual TLS",
            ));
        }
        let control = self
            .0
            .as_ref()
            .ok_or_else(|| Status::unavailable("cell control disabled"))?;
        let token = control
            .config
            .operator_token
            .as_deref()
            .ok_or_else(|| Status::permission_denied("operator writes disabled"))?;
        let authorized = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .is_some_and(|(scheme, supplied)| {
                scheme.eq_ignore_ascii_case("Bearer")
                    && !token.is_empty()
                    && bool::from(supplied.as_bytes().ct_eq(token.as_bytes()))
            });
        if !authorized {
            return Err(Status::unauthenticated("invalid cell operator credential"));
        }
        validate_operator(&control.config, request.get_ref())?;
        if request.metadata().contains_key("bedrock-cell-forwarded") && !control.is_leader() {
            return Err(Status::unavailable(
                "forwarded cell write reached a relay without current leadership",
            ));
        }
        let revision = control.write(request.into_inner()).await?;
        Ok(Response::new(pb::ControlWritten { revision }))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/membership/control.rs"]
mod tests;
