//! #25 / B-23 Layer 2, after TigerBeetle's VOPR: one seed drives the real `bedrock-api`, `bedrock-relay`,
//! `bedrock-quicksilver`, `bedrock-cdn` and `bedrock-kv` run functions through storage, network and process faults while
//! clients write, and the history checker tries to catch an invariant breaking.
//!
//! Each run has two phases. Safety: faults run continuously. Liveness: faults stop and everything must converge within
//! [`LIVENESS_BOUND`]. The seed picks the fault classes and rates, leaving whole classes out so rare combinations come up
//! (swarm testing), and how the load arrives. The load itself is the same for every seed, because the point is to find
//! what breaks under real traffic, and long runs (`scripts/vopr.sh soak`) find what only appears over time. Adding to
//! either changes what every seed does: a seed reproduces a failure only at the commit and length that found it, so each
//! bug also gets a directed regression test.
//!
//! Replay one seed with `scripts/vopr.sh replay 42`, or sweep with `scripts/vopr.sh sweep 500`. Any seed that fails goes in
//! [`OPEN`]. `BEDROCK_VOPR_FAULTS=storage,rpc` restricts the classes a seed may pick.
#![cfg(madsim)]

// B-61: allocation is a large share of a run. Memory figures from long runs are therefore mimalloc's, not glibc's.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::membership::View;
use bedrock_lib::node::{
    ApiConfig, Budget, ConfigError, Location, MembershipSettings, NodeConfig, RelayConfig,
    ReplaySettings, Role, ServiceConfig, Tier,
};
use bedrock_lib::placement::{Directory, Owner, State, hash_of};
use bedrock_lib::relays::client::{Selection, Session, Target, applied_at, applied_ranges};
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::s3::S3Config;
use bedrock_lib::sim::{Hooks, install};
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::sim::{
    Access, Bug, Disks, Fault, Faults, Fingerprint, Frozen, SimEngine, SimFactory, StoreAccess,
    StoreFault, StoreFaults, StoreObserver,
};
use bedrock_lib::storage::{KeyRange, Snapshot, StorageEngine, l2_parts, l2_span};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use bedrock_sim::history::{
    History, Key, OwnerState, Placement, Range, Stream, Value, Violation, check_coverage,
    check_handoff, check_held, check_routing, within,
};
use bedrock_sim::rpc;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use madsim::task::NodeId;
use prost::Message;
use tokio::time::{Instant, sleep};

/// Seeds that still fail: the bugs left to fix. The check fails until this is empty. A failing seed goes here first, with what
/// it found and where the fix belongs, then moves to [`REGRESSIONS`] with its fix. A failure that turns out to be a harness
/// mistake is fixed in the harness, and its seed leaves the list.
const OPEN: &[(u64, &str)] = &[];

/// Seeds that found a product bug, now fixed, under the bug and the test that holds the fix without the seed. Harness changes
/// move what a seed does, so that test is the lasting proof. "Traced" seeds were followed to the bug; "grouped" ones stopped
/// failing in the same sweep as that fix, without being traced themselves.
const REGRESSIONS: &[(u64, &str)] = &[
    // health/scrub.rs: a crash took back a quarantine record and its rebuild, and the damage returned where nothing read it.
    // Traced, on 3 nodes per service with BEDROCK_VOPR_SECONDS=30; it no longer reaches that path as it stands here.
    // `damage_in_a_cold_key_is_found`, storage.rs `a_crash_can_undo_a_quarantine_and_its_rebuild_but_the_record_still_fails_a_read`.
    (21641, ALL),
    // reconciliation/rebuild.rs: a second corruption during a rebuild stalled it until the repair timeout. Traced.
    // `storage_failing_again_abandons_a_blocked_rebuild_at_once`.
    (43, "corruption"),
    // replication/writer.rs: a failed flush nobody retried blocked bootstraps, deadlocking the membership publisher. Traced.
    // `bootstrap_settles_a_failed_flush_without_a_later_commit`.
    (317, ALL),
    // relays/client.rs: a damaged record stopped the session starting, so the process crash-looped before any rebuild. Traced.
    // `a_damaged_engine_does_not_stop_the_session_starting`.
    (389, ALL),
    // Downstream of the last two: a dataset never placed, and a node that never caught up. Grouped.
    (161, ALL),
    (475, ALL),
    // relays/client.rs: a revoke's write committed but reported failure, leaving a trusted cursor over deleted data. Traced.
    // `a_revoke_with_an_unknown_outcome_stops_trusting_the_consumer`.
    (1593, ALL),
    (1687, ALL),
    // membership.rs: a failed directory write let the plan's later ones land, leaving no range at 0. 132 traced, the rest
    // grouped. `a_failed_write_abandons_the_rest_of_its_services_plan`.
    (132, ALL),
    (228, ALL),
    (442, ALL),
    // relays/client.rs: a split that committed but reported failure left progress on the parts, and a later revoke missed it.
    // Traced. `a_revoke_retires_progress_stored_for_part_of_the_range`.
    (182, ALL),
    // relays/client.rs: a range unreadable when the session started had no consumer, so nothing dropped it after a revoke. 146
    // traced, the rest grouped. `a_stored_range_without_a_consumer_is_dropped_where_the_set_no_longer_has_it`.
    (146, ALL),
    (84, ALL),
    (85, ALL),
    (193, ALL),
    (207, ALL),
    (208, ALL),
    (264, ALL),
    (265, ALL),
    (283, ALL),
    (345, ALL),
    // relays/client.rs: a split by an unvalidated consumer kept the data without a marker, so a restart forgot it was held. 186
    // traced, the rest grouped. `splitting_unconfirmed_coverage_requires_the_kept_range_to_recover`.
    (186, ALL),
    (251, ALL),
    (279, ALL),
    (336, ALL),
    (351, ALL),
    (388, ALL),
    (401, ALL),
    // relays/ranges.rs: a refused drop left a stored whole beside a followed part, hiding the rest from later revokes. Traced.
    // `a_stored_range_around_a_held_part_still_drops_the_rest`.
    (823, ALL),
    // relays/client.rs: a read failure creating a split part's consumer left a group with none, and its next split left the
    // parts it kept unmarked. Traced. `a_split_without_a_consumer_still_marks_what_it_keeps`.
    (158, ALL),
    // relays.rs: a relay followed from a sibling's cursor past its own head and kept reporting the old head, so a subscriber
    // behind that cursor never fetched what the stream skipped. Traced.
    // `a_stream_started_past_this_relays_history_moves_its_head_to_the_cursor`.
    (2718, ALL),
    // relays/client.rs: a split that committed but reported failure left a trusted cursor on a part, active over replaced data.
    // Traced. `a_retried_split_retires_progress_an_earlier_attempt_left_on_a_part`.
    (7209, ALL),
    // relays/client.rs: the same cursor, overtaken by the whole range's consumer before any retry. Traced.
    // `a_commit_retires_cursors_an_unknown_split_left_on_parts_of_its_range`.
    (9471, ALL),
    // relays/ranges.rs: the parts of a stored range were followed as new, with no marker until their first commit, so a crash
    // left their data unknown to later revokes. Traced. `a_stored_range_without_a_consumer_is_dropped_where_the_set_no_longer_has_it`.
    (33291, ALL),
    // relays.rs: a recovery source was lost after a bootstrap but before its confirmation, and the next source was asked for a
    // plain catch-up, so the subscriber waited forever. Traced. `a_bootstrap_still_unconfirmed_makes_the_next_source_a_full_recovery`.
    (36029, ALL),
    // transport.rs: a cancelled wait on an idle stream kept the breaker's probe, so the healthy API stayed out of routing and
    // relays could not recover their views. Traced. `a_cancelled_call_gives_back_its_probe`.
    (10825, ALL),
    // relays.rs: a recovery took a slot on every relay it passed through, so sibling relays waited on each other's slots and a
    // long run could not recover its ranges in time. Traced, at BEDROCK_VOPR_SECONDS=86400; passes at 43200 with the fix, and
    // the full day has not been rerun. `only_the_relay_a_recovery_entered_needs_a_slot`.
    (90003, ALL),
];

/// Seeds that never failed: a quick sample of every fault class.
const SAMPLE: &[(u64, &str)] = &[(1, ALL), (2, ALL), (3, ALL)];

/// What the check always runs.
const SEEDS: [&[(u64, &str)]; 3] = [OPEN, REGRESSIONS, SAMPLE];
const ALL: &str = "all";

const TOKEN: &str = "simulation-writer-token";
const CELL: &str = "eu-1a";
const API: &str = "10.0.5.1";
const STALE_API: &str = "10.0.5.2";
/// The relays above the cell, between it and the API.
const REGION_RELAYS: [&str; 2] = ["10.0.5.61", "10.0.5.62"];
const RELAYS: [&str; 3] = ["10.0.5.11", "10.0.5.12", "10.0.5.13"];
/// One more of each than replicas, as a real cell has: nodes then differ in what they own, and ranges split and move.
const QUICKSILVERS: [&str; 4] = ["10.0.5.21", "10.0.5.22", "10.0.5.23", "10.0.5.24"];
const CDNS: [&str; 4] = ["10.0.5.31", "10.0.5.32", "10.0.5.33", "10.0.5.34"];
const KVS: [&str; 2] = ["10.0.5.41", "10.0.5.42"];
const SUBSCRIBER: &str = "10.0.5.50";
const OBSERVER: &str = "10.0.5.51";
const CLIENT: &str = "10.0.5.52";

/// The workload's streams: every pushed dataset's namespaces.
const STREAMS: [(&str, &str); 4] = [
    ("quicksilver", "infrastructure"),
    ("auth", "keys"),
    ("cdn", "pages"),
    ("cdn", "workers"),
];
/// Streams the external subscriber follows whole.
const FOLLOWED: [(&str, &str); 2] = [("auth", "keys"), ("cdn", "pages")];
const GRACE: Duration = Duration::from_secs(5);
/// How long the safety phase runs unless `BEDROCK_VOPR_SECONDS` says otherwise.
const SAFETY: Duration = Duration::from_secs(60);
/// How often a long run says how far it has got.
const PROGRESS: Duration = Duration::from_secs(600);
/// The load every seed sends: all of [`STREAMS`], from this many writers, over this many keys.
const WRITERS: usize = 4;
const KEYS: u32 = 64;
const DELETES: f64 = 0.2;
const PADDING: usize = 256;
const WRITE_SPACING_MS: std::ops::Range<u64> = 10..200;
/// How long everything has to converge after faults stop.
const LIVENESS_BOUND: Duration = Duration::from_secs(300);
// Damage is only found by the scrubber's walk, so a full pass of the largest process has to fit with room for the rebuild.
const _: () = assert!(bedrock_lib::health::scrub::pass(2).as_secs() * 2 < LIVENESS_BOUND.as_secs());
/// Events expire quickly, so cleanup and obsolete-cursor bootstraps happen during a run.
const EVENT_RETENTION_MS: u64 = 20_000;
/// Retry records expire within a soak, but only after a client's retries of one write have finished, or a retry would rightly
/// apply again.
const RETRY_RETENTION_MS: u64 = 120_000;
/// Replay history ages out during a run. The default windows outlast any run.
const REPLAY_WINDOW_MS: u64 = 30_000;
/// A process unready this long is restarted, as a supervisor would. A dataset that fails to open stays unready until then.
const SUPERVISOR_PATIENCE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    Storage,
    Corruption,
    Namespace,
    Rpc,
    Crash,
    LostTail,
    Partition,
    Pause,
    Fencing,
}

const CLASSES: [(&str, Class); 9] = [
    ("storage", Class::Storage),
    ("corruption", Class::Corruption),
    ("namespace", Class::Namespace),
    ("rpc", Class::Rpc),
    ("crash", Class::Crash),
    ("lost-tail", Class::LostTail),
    ("partition", Class::Partition),
    ("pause", Class::Pause),
    ("fencing", Class::Fencing),
];

/// A deliberate bug: its invariant must fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deliberate {
    Engine(Bug),
    LostFlush,
    /// Retry records never reach the store.
    LostRetries,
    /// Directory writes around the publisher, as a bug in it would make.
    Rogue(Rogue),
    /// A process's disks stop answering for good.
    Wedged(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rogue {
    /// Removes the range at 0.
    Gap,
    /// Marks every owner of a range as still populating.
    Unready,
    /// Gives a range to a member with a dead disk.
    Unhealthy,
}

/// What a run may do; the seed picks within it.
#[derive(Debug, Clone, Default)]
struct Options {
    /// Fault classes a seed may pick from.
    allowed: BTreeSet<Class>,
    bug: Option<Deliberate>,
    /// Kill and restart every process once, mid-run.
    restart_all: bool,
    /// Damage a record once writes have stopped, so no write or delivery ever touches it again.
    cold_damage: bool,
}

impl Options {
    /// `names` is `all`, `none`, or a comma-separated list of [`CLASSES`].
    fn with_faults(names: &str) -> Self {
        let allowed = match names {
            ALL => CLASSES.iter().map(|(_, class)| *class).collect(),
            "none" => BTreeSet::new(),
            names => names
                .split(',')
                .map(|name| {
                    let found = CLASSES.iter().find(|(known, _)| *known == name);
                    found
                        .unwrap_or_else(|| panic!("unknown fault class {name}"))
                        .1
                })
                .collect(),
        };
        Options {
            allowed,
            ..Options::default()
        }
    }
}

/// How the fixed load arrives, which is all a seed decides about traffic: it never changes how much is sent or what kind.
/// New request types join the load for every seed, each with its own check in the history.
#[derive(Debug)]
struct Arrival {
    /// Chance that a write goes to the first two keys, concentrating overwrites and deletes.
    hot: f64,
    /// Writes sent back to back before a pause as long as they would otherwise have spaced out; 1 is steady.
    burst: u64,
}

impl Arrival {
    fn draw(rng: &mut impl Rng) -> Self {
        Arrival {
            hot: [0.0, 0.0, 0.5, 0.9][rng.gen_range(0..4)],
            burst: [1, 1, 5, 20][rng.gen_range(0..4)],
        }
    }

    fn stream(&self) -> Stream {
        let (dataset, namespace) = STREAMS[thread_rng().gen_range(0..STREAMS.len())];
        (dataset.to_owned(), namespace.to_owned())
    }

    fn key(&self) -> Key {
        let mut rng = thread_rng();
        let hot = rng.gen_bool(self.hot);
        key_of(rng.gen_range(0..if hot { 2 } else { KEYS }))
    }

    /// A value that names `operation`, so the checker can tell any two apart.
    fn value(&self, operation: &str) -> Value {
        let mut value = operation.as_bytes().to_vec();
        value.resize(value.len() + thread_rng().gen_range(0..=PADDING), b'.');
        value
    }

    /// The pause after write number `n`. Bursts keep the same average rate as steady traffic.
    fn pause(&self, n: u64) -> Option<Duration> {
        n.is_multiple_of(self.burst).then(|| {
            let spacing = thread_rng().gen_range(WRITE_SPACING_MS);
            Duration::from_millis(spacing * self.burst)
        })
    }
}

/// What this seed does: its fault classes, the rates of the per-call ones, how traffic arrives, and how long it runs.
#[derive(Debug)]
struct Profile {
    classes: BTreeSet<Class>,
    /// Chance that one storage, namespace or RPC call fails.
    rate: f64,
    arrival: Arrival,
    safety: Duration,
    /// Concurrent fault drivers.
    drivers: usize,
    replay_max_bytes: Option<u64>,
    /// What `bedrock-kv` does through liveness, proving nothing else waits for it.
    kv: Kv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kv {
    Serving,
    Dead,
    /// Overloaded: it holds its connections and answers nothing.
    Stalled,
}

/// The safety phase's length.
fn safety() -> Duration {
    std::env::var("BEDROCK_VOPR_SECONDS").map_or(SAFETY, |seconds| {
        Duration::from_secs(seconds.parse().expect("BEDROCK_VOPR_SECONDS is a number"))
    })
}

impl Profile {
    fn draw(options: &Options) -> Self {
        let mut rng = thread_rng();
        Profile {
            classes: options
                .allowed
                .iter()
                .copied()
                .filter(|_| rng.gen_bool(0.6))
                .collect(),
            rate: [0.001, 0.01, 0.05][rng.gen_range(0..3)],
            arrival: Arrival::draw(&mut rng),
            safety: safety(),
            drivers: rng.gen_range(1..=3),
            replay_max_bytes: rng.gen_bool(0.5).then_some(16 * 1024),
            kv: [Kv::Serving, Kv::Dead, Kv::Stalled][rng.gen_range(0..3)],
        }
    }
}

/// The seed's fault policy, shared with the storage and RPC hooks.
struct Chaos {
    profile: Profile,
    /// Faults are on.
    active: AtomicBool,
    /// Faults injected per class, so a sweep shows it exercised what it claims to.
    injected: Mutex<BTreeMap<Class, u64>>,
    /// Every fault decision and write, which is what makes a replay comparison mean something.
    fingerprint: Fingerprint,
    /// The writer was told it is fenced and stays out of service until replaced.
    fenced: AtomicBool,
    cell: Mutex<Cell>,
}

/// The directory invariants, checked at the store: there every durable member and directory write arrives in commit order,
/// and a write planned from an older view is refused before it gets here. A sampled view can stall across a member's whole
/// departure or recovery, and every allowance for that hid real failures or missed some.
#[derive(Default)]
struct Cell {
    members: BTreeMap<String, pb::Member>,
    directories: BTreeMap<String, Directory>,
    violation: Option<Violation>,
}

impl Cell {
    fn range(&mut self, dataset: &str, start: u64, owners: Option<Vec<Owner>>) {
        let directory = self.directories.entry(dataset.to_owned()).or_default();
        let before = placement(directory);
        match owners {
            Some(owners) => directory.insert(start, owners),
            None => directory.remove(&start),
        };
        let after = placement(directory);
        let sick = |member: &&pb::Member| member.unhealthy.iter().any(|d| d == dataset);
        let carries = |member: &&pb::Member| member.datasets.iter().any(|d| d == dataset);
        let members = || self.members.values();
        let id = |member: &pb::Member| member.node_id.clone();
        let unhealthy = members().filter(sick).map(id).collect();
        let healthy = members()
            .filter(carries)
            .filter(|member| !sick(member))
            .map(id)
            .collect();
        let checked = check_coverage(&after)
            .and_then(|()| check_handoff(&before, &after, &healthy))
            .and_then(|()| check_routing(&before, &after, &unhealthy));
        if let Err(violation) = checked {
            self.violation.get_or_insert(violation);
        }
    }
}

impl Chaos {
    fn on(&self, class: Class) -> bool {
        self.active.load(Ordering::SeqCst) && self.profile.classes.contains(&class)
    }

    /// Whether to inject a `class` fault into this call, drawn at the seed's rate.
    fn strikes(&self, class: Class) -> bool {
        let struck = self.on(class) && thread_rng().gen_bool(self.profile.rate);
        if struck {
            self.count(class);
        }
        struck
    }

    fn count(&self, class: Class) {
        *self.injected.lock().unwrap().entry(class).or_default() += 1;
    }
}

fn storage_faults(chaos: &Arc<Chaos>) -> Faults {
    let chaos = chaos.clone();
    Arc::new(move |access| {
        let fault = chaos.strikes(Class::Storage).then(|| {
            let mut rng = thread_rng();
            match (access, rng.gen_range(0..3)) {
                (Access::Read, 0) => Fault::PartialScan(rng.gen_range(0..4)),
                (Access::Write, 0) => Fault::Ambiguous,
                (_, 1) => Fault::Unavailable,
                _ => Fault::Io,
            }
        });
        chaos.fingerprint.mix((access, fault));
        fault
    })
}

fn store_faults(chaos: &Arc<Chaos>, bug: Option<Deliberate>) -> StoreFaults {
    let chaos = chaos.clone();
    Arc::new(move |access| {
        match (bug, access) {
            (Some(Deliberate::LostFlush), StoreAccess::Flush) => {
                return Some(StoreFault::LostFlush);
            }
            (Some(Deliberate::LostRetries), StoreAccess::Write) => {
                return Some(StoreFault::DropRetries);
            }
            _ => {}
        }
        let fault = chaos.strikes(Class::Namespace).then(|| {
            let mut rng = thread_rng();
            let conditional = matches!(access, StoreAccess::Write | StoreAccess::Flush);
            match rng.gen_range(0..if conditional { 4 } else { 3 }) {
                0 => StoreFault::Ambiguous,
                1 => StoreFault::Unavailable,
                2 => StoreFault::Delay(Duration::from_millis(rng.gen_range(10..3_000))),
                _ => {
                    chaos.fenced.store(true, Ordering::SeqCst);
                    StoreFault::Fenced {
                        landed: rng.gen_bool(0.5),
                    }
                }
            }
        });
        chaos.fingerprint.mix((access, fault));
        fault
    })
}

/// Feeds durable membership writes to [`Cell`].
fn watch_membership(chaos: &Arc<Chaos>) -> StoreObserver {
    use pb::range::owner::State as Pb;
    let chaos = chaos.clone();
    Arc::new(move |instance, changes| {
        if !instance.starts_with("membership/") {
            return;
        }
        let mut cell = chaos.cell.lock().unwrap();
        for (key, value) in changes {
            let Some(id) = key.strip_prefix(b"v") else {
                continue;
            };
            let id = String::from_utf8_lossy(id).into_owned();
            if let Some(range) = id.strip_prefix("range/") {
                let (dataset, start) = range.rsplit_once('/').expect("a directory key");
                let start = u64::from_str_radix(start, 16).expect("a directory key");
                let owners = value.as_ref().map(|value| {
                    let range = pb::Range::decode(value.as_slice()).expect("a directory record");
                    let owner = |owner: pb::range::Owner| Owner {
                        state: match Pb::try_from(owner.state) {
                            Ok(Pb::Ready) => State::Ready,
                            Ok(Pb::Leaving) => State::Leaving,
                            _ => State::Populating,
                        },
                        node: owner.node_id,
                    };
                    range.owners.into_iter().map(owner).collect()
                });
                cell.range(dataset, start, owners);
            } else if let Some(value) = value {
                let member = pb::Member::decode(value.as_slice()).expect("a member record");
                cell.members.insert(id, member);
            } else {
                cell.members.remove(&id);
            }
        }
    })
}

/// A copy of one stream's selected keys, sampled during the run and checked against the finished history.
struct Sample {
    node: String,
    stream: Stream,
    revision: u64,
    span: Option<Range>,
    held: BTreeMap<Key, Value>,
}

impl Sample {
    fn check(&self, history: &History) -> Result<(), Violation> {
        let (dataset, namespace) = &self.stream;
        let selected = |key: &[u8]| {
            self.span
                .is_none_or(|span| within(span, hash_of(dataset, namespace, key)))
        };
        let located = |mut violation: Violation| {
            violation.detail = format!("{}: {}", self.node, violation.detail);
            violation
        };
        history
            .check_copy(&self.stream, self.revision, selected, &self.held)
            .map_err(located)?;
        // A read at this copy's revision proves values and absence alike.
        for n in 0..KEYS {
            let key = key_of(n);
            if selected(&key) {
                let observed = self.held.get(&key).map(Vec::as_slice);
                history
                    .check_read(&self.stream, self.revision, self.revision, &key, observed)
                    .map_err(located)?;
            }
        }
        Ok(())
    }
}

fn key_of(n: u32) -> Key {
    format!("k{n:02}").into_bytes()
}

/// The membership views the observer keeps, one per service.
struct Views {
    quicksilver: Arc<View>,
    cdn: Arc<View>,
    kv: Arc<View>,
}

impl Views {
    fn of(&self, dataset: &str) -> &View {
        match dataset {
            "cdn" => &self.cdn,
            _ => &self.quicksilver,
        }
    }
}

struct Run {
    chaos: Arc<Chaos>,
    disks: Arc<Disks>,
    store: SimFactory,
    writing: AtomicBool,
    writers: AtomicUsize,
    history: Mutex<History>,
    samples: Mutex<Vec<Sample>>,
    failure: Mutex<Option<Violation>>,
    trace: Mutex<Vec<(u128, String)>>,
    started: Instant,
    /// Processes that exited with an error, waiting for the supervisor.
    exited: Mutex<BTreeSet<String>>,
    /// Processes a fault driver is working on.
    busy: Mutex<BTreeSet<String>>,
    views: Mutex<Option<Arc<Views>>>,
    /// The second API process of the stale-publisher fault, once started.
    stale: Mutex<Option<NodeHandle>>,
    /// The latest acknowledged write, which a client can always send again.
    last_ack: Mutex<Option<Acknowledged>>,
}

type Acknowledged = (Stream, String, Key, Option<Value>);

impl Run {
    fn record(&self, event: String) {
        let at = self.started.elapsed().as_nanos();
        self.trace.lock().unwrap().push((at, event));
    }

    /// Keeps the first violation, which ends the run.
    fn check(&self, result: Result<(), Violation>) {
        if let Err(violation) = result {
            self.failure.lock().unwrap().get_or_insert(violation);
        }
    }

    fn failed(&self) -> bool {
        self.failure.lock().unwrap().is_some()
    }

    fn views(&self) -> Option<Arc<Views>> {
        self.views.lock().unwrap().clone()
    }
}

fn data_dir(name: &str) -> PathBuf {
    format!("/data/{name}").into()
}

fn node(name: &str, ip: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: "eu-1".into(),
            cell: CELL.into(),
            failure_domain: String::new(),
        },
        listen: format!("{ip}:7000").parse().unwrap(),
        advertise: None,
        admin_listen: format!("{ip}:7001").parse().unwrap(),
        data_dir: data_dir(name),
        budget: Budget::default(),
        tls: None,
    }
}

fn api(name: &str, ip: &str) -> ApiConfig {
    ApiConfig {
        node: node(name, ip),
        write_listen: format!("{ip}:7002").parse().unwrap(),
        write_token: TOKEN.into(),
        retention: Retention {
            event_retention_ms: EVENT_RETENTION_MS,
            retry_retention_ms: RETRY_RETENTION_MS,
        },
        s3: S3Config {
            endpoint: "http://10.0.5.99:9000".into(),
            bucket: "bedrock".into(),
            region: "us-east-1".into(),
            access_key_id: "sim".into(),
            secret_access_key: "sim".into(),
            max_attempts: 3,
            attempt_timeout_ms: 30_000,
        },
    }
}

fn relay(name: &str, ip: &str, replay_max_bytes: Option<u64>) -> RelayConfig {
    RelayConfig {
        node: node(name, ip),
        tier: Tier::Cell,
        upstreams: REGION_RELAYS.map(|ip| format!("{ip}:7000")).to_vec(),
        replay_peers: RELAYS
            .iter()
            .filter(|peer| **peer != ip)
            .map(|peer| format!("{peer}:7000"))
            .collect(),
        replay: ReplaySettings {
            max_bytes: replay_max_bytes,
            window_ms: Some(REPLAY_WINDOW_MS),
            ..ReplaySettings::default()
        },
        membership: Some(MembershipSettings {
            api: format!("{API}:7002"),
            grace_ms: GRACE.as_millis() as u64,
        }),
        write_token: Some(TOKEN.into()),
    }
}

fn region_relay(name: &str, ip: &str, replay_max_bytes: Option<u64>) -> RelayConfig {
    RelayConfig {
        node: node(name, ip),
        tier: Tier::Region,
        upstreams: vec![format!("{API}:7000")],
        replay_peers: REGION_RELAYS
            .iter()
            .filter(|peer| **peer != ip)
            .map(|peer| format!("{peer}:7000"))
            .collect(),
        replay: ReplaySettings {
            max_bytes: replay_max_bytes,
            window_ms: Some(REPLAY_WINDOW_MS),
            ..ReplaySettings::default()
        },
        membership: None,
        write_token: None,
    }
}

fn relay_addresses() -> Vec<String> {
    RELAYS.iter().map(|ip| format!("{ip}:7000")).collect()
}

fn service(name: &str, ip: &str, datasets: &[&str]) -> ServiceConfig {
    ServiceConfig {
        node: node(name, ip),
        roles: vec![Role::L2],
        datasets: datasets.iter().map(|d| (*d).to_owned()).collect(),
        relays: relay_addresses(),
        api: format!("{API}:7002"),
        s3_endpoint: "http://10.0.5.99:9000".into(),
    }
}

/// A process the fault drivers may hit.
#[derive(Clone)]
struct Process {
    name: String,
    ip: String,
    id: NodeId,
    dir: PathBuf,
    /// Datasets it stores in hash ranges.
    datasets: &'static [&'static str],
    /// Whether it has an admin endpoint and a health manager, so damage gets noticed and rebuilt.
    probed: bool,
}

/// Starts a process that reports an error exit to the supervisor, instead of failing the run.
fn spawn<F>(
    run: &Arc<Run>,
    name: &str,
    ip: &str,
    start: impl Fn() -> F + Send + Sync + 'static,
) -> NodeId
where
    F: Future<Output = Result<(), ConfigError>> + 'static,
{
    let (run, owned) = (run.clone(), name.to_owned());
    Handle::current()
        .create_node()
        .name(name)
        .ip(ip.parse().unwrap())
        .init(move || {
            let (run, owned, running) = (run.clone(), owned.clone(), start());
            async move {
                if let Err(error) = running.await {
                    run.record(format!("{owned} exited: {error}"));
                    run.exited.lock().unwrap().insert(owned);
                }
            }
        })
        .build()
        .id()
}

fn start_cluster(run: &Arc<Run>) -> Vec<Process> {
    let mut processes = Vec::new();
    let mut add = |name: String, ip: &str, datasets, probed, id: NodeId| {
        processes.push(Process {
            dir: data_dir(&name),
            name,
            ip: ip.to_owned(),
            id,
            datasets,
            probed,
        });
    };
    let config = api("api", API);
    let id = spawn(run, "api", API, move || bedrock_api::run(config.clone()));
    add("api".into(), API, &[], true, id);
    for (i, ip) in REGION_RELAYS.iter().enumerate() {
        let name = format!("region-{}", i + 1);
        let config = region_relay(&name, ip, run.chaos.profile.replay_max_bytes);
        let id = spawn(run, &name, ip, move || bedrock_relay::run(config.clone()));
        add(name, ip, &[], true, id);
    }
    for (i, ip) in RELAYS.iter().enumerate() {
        let name = format!("relay-{}", i + 1);
        let config = relay(&name, ip, run.chaos.profile.replay_max_bytes);
        let id = spawn(run, &name, ip, move || bedrock_relay::run(config.clone()));
        add(name, ip, &[], true, id);
    }
    for (i, ip) in QUICKSILVERS.iter().enumerate() {
        let name = format!("quicksilver-{}", i + 1);
        let config = service(&name, ip, &["quicksilver", "auth"]);
        let id = spawn(run, &name, ip, move || {
            bedrock_quicksilver::run(config.clone())
        });
        add(name, ip, &["quicksilver", "auth"], true, id);
    }
    for (i, ip) in CDNS.iter().enumerate() {
        let name = format!("cdn-{}", i + 1);
        let config = service(&name, ip, &["cdn"]);
        let id = spawn(run, &name, ip, move || bedrock_cdn::run(config.clone()));
        add(name, ip, &["cdn"], true, id);
    }
    for (i, ip) in KVS.iter().enumerate() {
        let name = format!("kv-{}", i + 1);
        let config = service(&name, ip, &[]);
        let id = spawn(run, &name, ip, move || bedrock_kv::run(config.clone()));
        add(name, ip, &[], true, id);
    }
    add("subscriber".into(), SUBSCRIBER, &[], false, subscriber(run));
    processes
}

fn followed_prefix(namespace: &str) -> Vec<u8> {
    format!("sub/{namespace}/").into_bytes()
}

/// The external subscriber: whole namespaces followed through the relays, resuming from its own disk.
fn subscriber(run: &Arc<Run>) -> NodeId {
    let disks = run.disks.clone();
    spawn(run, "subscriber", SUBSCRIBER, move || {
        let disks = disks.clone();
        async move {
            let engine = disks.open(&data_dir("subscriber"), "subscribed");
            let targets = FOLLOWED
                .iter()
                .map(|(dataset, namespace)| Target {
                    selection: Selection {
                        dataset: (*dataset).into(),
                        namespace: (*namespace).into(),
                    },
                    engine: engine.clone(),
                    prefix: followed_prefix(namespace),
                    span: None,
                })
                .collect();
            let peers = Arc::new(Peers::new(&Admin::new()));
            let session = Session::start(peers, relay_addresses(), targets)
                .map_err(|error| ConfigError(error.to_string()))?;
            // Runs until killed; the session task does the work.
            std::future::pending::<()>().await;
            drop(session);
            Ok(())
        }
    })
}

/// Keeps membership views for the checkers, as an operator's dashboard would.
fn start_observer(run: &Arc<Run>) {
    let run = run.clone();
    let node = Handle::current()
        .create_node()
        .name("observer")
        .ip(OBSERVER.parse().unwrap())
        .build();
    node.spawn(async move {
        let (quicksilver, quicksilver_target) = View::new("quicksilver", CELL);
        let (cdn, cdn_target) = View::new("cdn", CELL);
        let (kv, kv_target) = View::new("kv", CELL);
        let views = Arc::new(Views {
            quicksilver: Arc::new(quicksilver),
            cdn: Arc::new(cdn),
            kv: Arc::new(kv),
        });
        *run.views.lock().unwrap() = Some(views.clone());
        let peers = Arc::new(Peers::new(&Admin::new()));
        let targets = vec![quicksilver_target, cdn_target, kv_target];
        let session = Session::start(peers.clone(), relay_addresses(), targets).unwrap();
        loop {
            sleep(Duration::from_millis(200)).await;
            for view in [&views.quicksilver, &views.cdn, &views.kv] {
                let _ = view.refresh(&session, &peers);
            }
        }
    });
}

/// One write through `address`, returning its sequence. `None` means the outcome is unknown.
async fn submit(
    address: &str,
    stream: &Stream,
    id: &str,
    key: &[u8],
    value: Option<&[u8]>,
) -> Option<u64> {
    let attempt = async {
        let tcp = tokio::net::TcpStream::connect(address).await.ok()?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .ok()?;
        tokio::spawn(connection);
        let (dataset, namespace) = stream;
        let key = String::from_utf8(key.to_vec()).unwrap();
        let request = hyper::Request::builder()
            .method(if value.is_some() { "PUT" } else { "DELETE" })
            .uri(format!("/v1/{dataset}/{namespace}/{key}"))
            .header("host", "api")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("idempotency-key", id)
            .body(Full::new(Bytes::from(value.unwrap_or_default().to_vec())))
            .unwrap();
        let response = sender.send_request(request).await.ok()?;
        let status = response.status();
        let body = response.into_body().collect().await.ok()?.to_bytes();
        assert_ne!(status.as_u16(), 400, "the workload sent an invalid write");
        status.is_success().then(|| {
            std::str::from_utf8(&body)
                .unwrap()
                .parse()
                .expect("the response body is the sequence")
        })
    };
    tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .ok()
        .flatten()
}

/// Writes until told to stop. A write with an unknown outcome is retried with the same idempotency key until it is
/// acknowledged, so a finished run knows every sequence. Sometimes it retries even after success, as a client that lost
/// the response would, and the retry must return the same sequence.
async fn writer(run: Arc<Run>, id: usize) {
    let arrival = &run.chaos.profile.arrival;
    let mut n = 0u64;
    while run.writing.load(Ordering::SeqCst) && !run.failed() {
        n += 1;
        let operation = format!("w{id}-{n}");
        let stream = arrival.stream();
        let key = arrival.key();
        let delete = thread_rng().gen_bool(DELETES);
        let value = (!delete).then(|| arrival.value(&operation));
        loop {
            let address = format!("{API}:7002");
            let answer = submit(&address, &stream, &operation, &key, value.as_deref()).await;
            if let Some(sequence) = answer {
                let mut history = run.history.lock().unwrap();
                let acknowledged =
                    history.acknowledged(&stream, sequence, &operation, &key, value.as_deref());
                drop(history);
                run.check(acknowledged);
                run.record(format!("ack {operation} {stream:?} {sequence}"));
                *run.last_ack.lock().unwrap() = Some((
                    stream.clone(),
                    operation.clone(),
                    key.clone(),
                    value.clone(),
                ));
                if !run.chaos.strikes(Class::Rpc) {
                    break;
                }
            }
            if run.failed() {
                break;
            }
            sleep(Duration::from_millis(thread_rng().gen_range(50..500))).await;
        }
        if let Some(pause) = arrival.pause(n) {
            sleep(pause).await;
        }
    }
    run.writers.fetch_sub(1, Ordering::SeqCst);
}

/// Every validated copy `process` holds now. An engine with a damaged record is skipped: it cannot pass as healthy, since
/// reads of the record fail.
fn copies(run: &Run, process: &Process) -> Vec<Sample> {
    let mut samples = Vec::new();
    for dataset in process.datasets {
        let Some(frozen) = readable(run, &process.dir, dataset) else {
            continue;
        };
        for (namespace, span, revision) in applied_ranges(&frozen, dataset).unwrap() {
            let entries = frozen.scan(&l2_span(span.0, span.1)).unwrap();
            let held = entries
                .map(Result::unwrap)
                .filter_map(|(key, value)| {
                    let (stored, key) = l2_parts(&key)?;
                    (stored == namespace.as_bytes()).then(|| (key.to_vec(), value))
                })
                .collect();
            samples.push(Sample {
                node: process.name.clone(),
                stream: ((*dataset).to_owned(), namespace.clone()),
                revision,
                span: Some(span),
                held,
            });
        }
    }
    if let Some(frozen) = readable(run, &process.dir, "subscribed") {
        for (dataset, namespace) in FOLLOWED {
            let prefix = followed_prefix(namespace);
            let Some(revision) = applied_at(&frozen, dataset, namespace, &prefix, None).unwrap()
            else {
                continue;
            };
            let entries = frozen.scan(&KeyRange::prefix(prefix.clone())).unwrap();
            samples.push(Sample {
                node: process.name.clone(),
                stream: (dataset.to_owned(), namespace.to_owned()),
                revision,
                span: None,
                held: entries
                    .map(Result::unwrap)
                    .map(|(key, value)| (key[prefix.len()..].to_vec(), value))
                    .collect(),
            });
        }
    }
    samples
}

fn readable(run: &Run, dir: &Path, name: &str) -> Option<Frozen> {
    let frozen = run.disks.get(dir, name)?.frozen();
    (!frozen.damaged()).then_some(frozen)
}

fn placement(directory: &Directory) -> Placement {
    let mut entries = directory.iter().peekable();
    let mut placement = Vec::new();
    while let Some((&start, owners)) = entries.next() {
        let end = entries.peek().map_or(0, |(next, _)| **next);
        let owners = owners
            .iter()
            .map(|owner| {
                let state = match owner.state {
                    State::Populating => OwnerState::Populating,
                    State::Ready => OwnerState::Ready,
                    State::Leaving => OwnerState::Leaving,
                };
                (owner.node.clone(), state)
            })
            .collect();
        placement.push(((start, end), owners));
    }
    placement
}

/// Checks every sample whose revision the history can already explain, so memory stays flat over a long run and a
/// violation ends it when it happens. The rest wait for writes with unknown outcomes to resolve.
fn check_samples(run: &Run) {
    if let Some(violation) = run.chaos.cell.lock().unwrap().violation.take() {
        run.check(Err(violation));
    }
    let history = run.history.lock().unwrap();
    run.samples.lock().unwrap().retain(|sample| {
        let waiting = sample.revision > history.known_through(&sample.stream);
        if !waiting {
            run.check(sample.check(&history));
        }
        waiting
    });
}

/// Samples copies throughout the run.
async fn sampler(run: Arc<Run>, processes: Vec<Process>) {
    while !run.failed() {
        sleep(Duration::from_millis(thread_rng().gen_range(500..2_000))).await;
        let process = &processes[thread_rng().gen_range(0..processes.len())];
        run.samples.lock().unwrap().extend(copies(&run, process));
        check_samples(&run);
    }
}

fn claim(run: &Run, process: &Process) -> bool {
    run.busy.lock().unwrap().insert(process.name.clone())
}

fn release(run: &Run, process: &Process) {
    run.busy.lock().unwrap().remove(&process.name);
}

/// Damages a random record of `engine`.
fn damage(engine: &SimEngine) -> Option<Key> {
    let frozen = engine.frozen();
    let keys: Vec<Key> = frozen
        .scan(&KeyRange::all())
        .unwrap()
        .map(|entry| entry.unwrap().0)
        .collect();
    if keys.is_empty() {
        return None;
    }
    let key = keys[thread_rng().gen_range(0..keys.len())].clone();
    engine.damage(&key);
    Some(key)
}

/// Kills `process`, loses what a power failure may take, and restarts it.
async fn crash(run: &Run, process: &Process, down: Duration) {
    let handle = Handle::current();
    handle.kill(process.id);
    if process.name == "api" {
        run.store.crash().unwrap();
    }
    if run.chaos.on(Class::LostTail) {
        for engine in run.disks.of(&process.dir) {
            let lost = engine.crash(thread_rng().gen_range(0..4));
            if lost > 0 {
                run.chaos.count(Class::LostTail);
            }
            // A torn write: the log truncated, and the record at its end fails verification.
            if lost > 0 && process.probed && thread_rng().gen_bool(0.25) {
                damage(&engine);
            }
            run.record(format!("{} lost {lost} batches", process.name));
        }
    }
    sleep(down).await;
    handle.restart(process.id);
}

/// A stale publisher: the writer stalls, a second API process takes the namespace and commits, and the first must fence
/// itself when it wakes. Only a restart returns it to service.
async fn stale_publisher(run: &Arc<Run>, primary: &Process, processes: &[Process]) {
    let handle = Handle::current();
    handle.pause(primary.id);
    let started = run.stale.lock().unwrap().as_ref().map(NodeHandle::id);
    let stale = match started {
        Some(id) => {
            handle.restart(id);
            id
        }
        None => {
            let config = api("api-stale", STALE_API);
            let node = handle
                .create_node()
                .name("api-stale")
                .ip(STALE_API.parse().unwrap())
                .init(move || bedrock_api::run(config.clone()))
                .build();
            let id = node.id();
            *run.stale.lock().unwrap() = Some(node);
            id
        }
    };
    sleep(Duration::from_secs(1)).await;
    let arrival = &run.chaos.profile.arrival;
    let stream = arrival.stream();
    let operation = format!("stale-{}", run.started.elapsed().as_nanos());
    let key = arrival.key();
    let value = arrival.value(&operation);
    let address = format!("{STALE_API}:7002");
    let mut answer = submit(&address, &stream, &operation, &key, Some(&value)).await;
    handle.kill(stale);
    handle.resume(primary.id);
    sleep(Duration::from_secs(2)).await;
    crash(run, primary, Duration::from_millis(100)).await;
    // An unknown outcome is resolved like any client's: retry the same key, here through the restarted writer.
    let address = format!("{API}:7002");
    while answer.is_none() && !run.failed() {
        answer = submit(&address, &stream, &operation, &key, Some(&value)).await;
        sleep(Duration::from_millis(200)).await;
        // This driver may be the only one, and the writer may have exited. It also holds the writer, so nothing else can
        // replace it if a write gets it fenced.
        restart_exited(run, processes).await;
        if run.chaos.fenced.swap(false, Ordering::SeqCst) {
            crash(run, primary, Duration::from_millis(500)).await;
        }
    }
    if let Some(sequence) = answer {
        let mut history = run.history.lock().unwrap();
        let acknowledged = history.acknowledged(&stream, sequence, &operation, &key, Some(&value));
        drop(history);
        run.check(acknowledged);
    }
}

/// Sends an acknowledged write again, as a client that lost the answer would.
async fn retry(run: &Run, (stream, id, key, value): Acknowledged) {
    while !run.failed() {
        let address = format!("{API}:7002");
        if let Some(sequence) = submit(&address, &stream, &id, &key, value.as_deref()).await {
            let mut history = run.history.lock().unwrap();
            let acknowledged = history.acknowledged(&stream, sequence, &id, &key, value.as_deref());
            drop(history);
            run.check(acknowledged);
            return;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Writes around the publisher until a checker notices.
async fn rogue(run: Arc<Run>, kind: Rogue) {
    use pb::range::owner::State as Pb;
    const FIRST: &str = "range/quicksilver/0000000000000000";
    let (owners, members) = loop {
        sleep(Duration::from_secs(1)).await;
        if run.failed() {
            return;
        }
        let Some(views) = run.views() else {
            continue;
        };
        let view = views.of("quicksilver");
        let (Ok(directory), Ok(members)) = (view.directory("quicksilver"), view.members()) else {
            continue;
        };
        let ready = directory
            .get(&0)
            .is_some_and(|owners| owners.iter().all(|o| o.state == State::Ready));
        if directory.len() > 1 && ready && members.len() == QUICKSILVERS.len() {
            break (directory[&0].clone(), members);
        }
    };
    let stream = ("membership".to_owned(), format!("quicksilver.{CELL}"));
    let owner = |node: &str, state: Pb| pb::range::Owner {
        node_id: node.to_owned(),
        state: state as i32,
    };
    let range = |owners: Vec<pb::range::Owner>| pb::Range { owners }.encode_to_vec();
    let sick = members
        .values()
        .find(|member| !owners.iter().any(|o| o.node == member.node_id))
        .unwrap()
        .node_id
        .clone();
    if kind == Rogue::Unhealthy {
        for engine in run.disks.of(&data_dir(&sick)) {
            engine.wedge();
        }
    }
    let began = run.started.elapsed();
    while !run.failed() {
        let writes = match kind {
            Rogue::Gap => vec![(FIRST.to_owned(), None)],
            Rogue::Unready => {
                let owners = owners.iter().map(|o| owner(&o.node, Pb::Populating));
                vec![(FIRST.to_owned(), Some(range(owners.collect())))]
            }
            Rogue::Unhealthy => {
                // Long enough for the sampler to see it unhealthy first.
                if run.started.elapsed() < began + Duration::from_secs(10) {
                    Vec::new()
                } else {
                    let nodes = owners.iter().map(|o| o.node.as_str());
                    let owners = nodes.chain([sick.as_str()]).map(|n| owner(n, Pb::Ready));
                    vec![(FIRST.to_owned(), Some(range(owners.collect())))]
                }
            }
        };
        for (key, value) in writes {
            let id = format!("rogue-{}", run.started.elapsed().as_nanos());
            let address = format!("{API}:7002");
            submit(&address, &stream, &id, key.as_bytes(), value.as_deref()).await;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

/// Injects process and network faults until `deadline`. Each action undoes itself, so the network is whole on return.
async fn driver(run: Arc<Run>, processes: Vec<Process>, deadline: Instant) {
    let chaos = run.chaos.clone();
    let mut reported = bedrock_lib::monotonic_now();
    while bedrock_lib::monotonic_now() < deadline && !run.failed() {
        sleep(Duration::from_millis(thread_rng().gen_range(200..3_000))).await;
        if reported.elapsed() >= PROGRESS {
            reported = bedrock_lib::monotonic_now();
            eprintln!("progress: {:?} simulated", run.started.elapsed());
            // madsim holds a finished task's memory until a node dies or its tasks are counted, so a soak would keep them all.
            Handle::current().metrics().num_tasks();
        }
        let a = &processes[thread_rng().gen_range(0..processes.len())];
        let b = &processes[thread_rng().gen_range(0..processes.len())];
        if a.name == b.name || !claim(&run, a) {
            continue;
        }
        let pick = thread_rng().gen_range(0..5);
        // Now and then long enough to outlast recovery leases, the longest breaker backoff and every idle timeout.
        let longest = if thread_rng().gen_bool(0.05) {
            90_000
        } else {
            10_000
        };
        let time = Duration::from_millis(thread_rng().gen_range(100..longest));
        match pick {
            0 if chaos.on(Class::Crash) => {
                run.record(format!("crash {} for {time:?}", a.name));
                chaos.count(Class::Crash);
                crash(&run, a, time).await;
            }
            1 if chaos.on(Class::Partition) => {
                let one_way = thread_rng().gen_bool(0.3);
                run.record(format!(
                    "partition {} {} {one_way} {time:?}",
                    a.name, b.name
                ));
                chaos.count(Class::Partition);
                rpc::partition(a.id, b.id, one_way, time).await;
            }
            2 if chaos.on(Class::Pause) => {
                run.record(format!("pause {} {time:?}", a.name));
                chaos.count(Class::Pause);
                rpc::pause(a.id, time).await;
            }
            // External subscribers own their recovery; they have no health manager to notice.
            3 if chaos.on(Class::Corruption) && a.probed => {
                for engine in run.disks.of(&a.dir) {
                    if let Some(key) = damage(&engine) {
                        run.record(format!("corrupt {} {}", a.name, key.escape_ascii()));
                        chaos.count(Class::Corruption);
                    }
                }
            }
            4 if chaos.on(Class::Fencing) && a.name == "api" => {
                run.record("stale publisher".into());
                chaos.count(Class::Fencing);
                stale_publisher(&run, a, &processes).await;
            }
            _ => {}
        }
        release(&run, a);
        restart_exited(&run, &processes).await;
        replace_fenced(&run, &processes).await;
    }
}

/// A fenced writer never recovers by itself: the deployment replaces it.
async fn replace_fenced(run: &Run, processes: &[Process]) {
    let api = processes.iter().find(|p| p.name == "api").unwrap();
    if run.chaos.fenced.load(Ordering::SeqCst) && claim(run, api) {
        run.chaos.fenced.store(false, Ordering::SeqCst);
        run.record("replacing the fenced writer".into());
        crash(run, api, Duration::from_millis(500)).await;
        release(run, api);
    }
}

async fn restart_exited(run: &Run, processes: &[Process]) {
    let exited = std::mem::take(&mut *run.exited.lock().unwrap());
    for process in processes.iter().filter(|p| exited.contains(&p.name)) {
        run.record(format!("supervisor restarts {}", process.name));
        rpc::bounce(process.id).await;
    }
}

/// Whether the process's admin endpoint answers ready.
async fn ready(ip: &str) -> bool {
    let attempt = async {
        let tcp = tokio::net::TcpStream::connect(format!("{ip}:7001"))
            .await
            .ok()?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .ok()?;
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .uri("/ready")
            .header("host", "admin")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(request).await.ok()?;
        Some(response.status().is_success())
    };
    tokio::time::timeout(Duration::from_secs(2), attempt)
        .await
        .ok()
        .flatten()
        == Some(true)
}

/// Why the cluster has not converged, naming the invariant it would break if this lasts past the bound.
fn unconverged(run: &Run, processes: &[Process]) -> Option<Violation> {
    let waiting = |invariant, detail: String| Some(Violation { invariant, detail });
    if run.writers.load(Ordering::SeqCst) > 0 {
        return waiting("Converged", "writes are still outstanding".into());
    }
    let Some(views) = run.views() else {
        return waiting("Converged", "no observer yet".into());
    };
    let history = run.history.lock().unwrap();
    for dataset in ["quicksilver", "auth", "cdn"] {
        let view = views.of(dataset);
        let (Ok(directory), Ok(members)) = (view.directory(dataset), view.members()) else {
            return waiting("RedundancyRestored", format!("no {dataset} view"));
        };
        if let Some(sick) = members
            .values()
            .find(|member| member.unhealthy.iter().any(|d| d == dataset))
        {
            let detail = format!("{} is still unhealthy for {dataset}", sick.node_id);
            return waiting("RedundancyRestored", detail);
        }
        let hosts = processes
            .iter()
            .filter(|process| process.datasets.contains(&dataset))
            .count();
        let placed = placement(&directory);
        if placed.is_empty() {
            return waiting("RedundancyRestored", format!("no {dataset} directory"));
        }
        for (range, owners) in &placed {
            let settled = owners.iter().all(|(_, s)| *s == OwnerState::Ready);
            if !settled || owners.len() != hosts.min(3) {
                let detail = format!("{dataset} range {range:x?} has owners {owners:?}");
                return waiting("RedundancyRestored", detail);
            }
            for (owner, _) in owners {
                let Some(process) = processes.iter().find(|p| p.name == *owner) else {
                    return waiting("RedundancyRestored", format!("unknown owner {owner}"));
                };
                let copies = copies(run, process);
                for stream in history.streams().filter(|(d, _)| d == dataset) {
                    let head = history.head(stream);
                    let caught_up = copies.iter().any(|copy| {
                        copy.stream == *stream
                            && copy.revision == head
                            && copy.span.is_some_and(|span| covers(span, *range))
                    });
                    if !caught_up {
                        let detail = format!("{owner} has not reached {head} of {stream:?}");
                        return waiting("Converged", detail);
                    }
                }
            }
        }
        // A node acts on a revoke after the directory shows it, so this has to come true rather than hold at every moment.
        for process in processes.iter().filter(|p| p.datasets.contains(&dataset)) {
            let held = held_hashes(run, process, dataset);
            if let Err(violation) = check_held(&placed, &process.name, held) {
                return Some(violation);
            }
        }
    }
    let subscriber = processes.iter().find(|p| p.name == "subscriber")?;
    let copies = copies(run, subscriber);
    for (dataset, namespace) in FOLLOWED {
        let stream = (dataset.to_owned(), namespace.to_owned());
        let head = history.head(&stream);
        if head > 0
            && !copies
                .iter()
                .any(|c| c.stream == stream && c.revision == head)
        {
            let detail = format!("the subscriber has not reached {head} of {stream:?}");
            return waiting("Converged", detail);
        }
    }
    None
}

fn covers(outer: Range, inner: Range) -> bool {
    within(outer, inner.0) && (outer.1 == 0 || (inner.1 != 0 && inner.1 <= outer.1))
}

fn stays_down(run: &Run, process: &Process) -> bool {
    run.chaos.profile.kv != Kv::Serving && process.name.starts_with("kv-")
}

/// What a failing run leaves behind for debugging.
async fn report(run: &Run, processes: &[Process]) {
    eprintln!("profile: {:?}", run.chaos.profile);
    for process in processes {
        let copies = copies(run, process);
        let progress: Vec<_> = copies
            .iter()
            .map(|c| (c.stream.1.as_str(), c.span, c.revision))
            .collect();
        let up = !process.probed || ready(&process.ip).await;
        eprintln!("{} ready={up} {progress:x?}", process.name);
    }
    if let Some(views) = run.views() {
        for (service, view) in [
            ("quicksilver", &views.quicksilver),
            ("cdn", &views.cdn),
            ("kv", &views.kv),
        ] {
            let members = view.members().unwrap_or_default();
            let members: Vec<_> = members
                .values()
                .map(|member| (member.node_id.as_str(), &member.unhealthy))
                .collect();
            eprintln!("{service} members: {members:?}");
        }
    }
    let trace = run.trace.lock().unwrap();
    let events: Vec<_> = trace
        .iter()
        .filter(|(_, event)| !event.starts_with("ack "))
        .collect();
    for (at, event) in events.iter().rev().take(25).rev() {
        eprintln!("{:>10.3}s {event}", *at as f64 / 1e9);
    }
    let history = run.history.lock().unwrap();
    for stream in history.streams() {
        eprintln!("head {stream:?} {}", history.head(stream));
    }
}

/// Stops faults and waits for convergence, restarting what a supervisor would.
async fn liveness(run: &Arc<Run>, processes: &[Process]) -> Result<(), Violation> {
    run.chaos.active.store(false, Ordering::SeqCst);
    run.writing.store(false, Ordering::SeqCst);
    run.record("healed".into());
    let deadline = bedrock_lib::monotonic_now() + LIVENESS_BOUND;
    let mut unready: BTreeMap<String, Instant> = BTreeMap::new();
    loop {
        if run.failed() {
            return Ok(());
        }
        let Some(why) = unconverged(run, processes) else {
            return Ok(());
        };
        if bedrock_lib::monotonic_now() >= deadline {
            return Err(why);
        }
        restart_exited(run, processes).await;
        replace_fenced(run, processes).await;
        let now = bedrock_lib::monotonic_now();
        for process in processes.iter().filter(|p| p.probed && !stays_down(run, p)) {
            if ready(&process.ip).await {
                unready.remove(&process.name);
            } else {
                let since = *unready.entry(process.name.clone()).or_insert(now);
                if now.duration_since(since) >= SUPERVISOR_PATIENCE {
                    run.record(format!("supervisor restarts unready {}", process.name));
                    rpc::bounce(process.id).await;
                    unready.remove(&process.name);
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// The finished history must explain everything observed.
fn verify(run: &Run, processes: &[Process]) -> Result<(), Violation> {
    if let Some(violation) = run.chaos.cell.lock().unwrap().violation.take() {
        return Err(violation);
    }
    let history = run.history.lock().unwrap();
    history.gap_free()?;
    for sample in run.samples.lock().unwrap().iter() {
        sample.check(&history)?;
    }
    for process in processes {
        for sample in copies(run, process) {
            sample.check(&history)?;
        }
    }
    Ok(())
}

/// The hash of every L2 record `process` stores for `dataset`.
fn held_hashes(run: &Run, process: &Process, dataset: &str) -> Vec<u64> {
    let Some(frozen) = readable(run, &process.dir, dataset) else {
        return Vec::new();
    };
    let entries = frozen.scan(&KeyRange::prefix(*b"l2/")).unwrap();
    entries
        .map(Result::unwrap)
        .map(|(key, _)| u64::from_be_bytes(key[3..11].try_into().unwrap()))
        .collect()
}

/// The whole run so far: what the harness did, and every decision and write underneath it.
fn trace(run: &Run) -> u64 {
    let mut hasher = DefaultHasher::new();
    run.trace.lock().unwrap().hash(&mut hasher);
    run.chaos.fingerprint.value().hash(&mut hasher);
    hasher.finish()
}

/// What a finished seed reports.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    trace: u64,
    acknowledged: u64,
    simulated: Duration,
    injected: BTreeMap<Class, u64>,
    arrival: String,
}

async fn scenario(options: Options) -> Result<Outcome, Violation> {
    let chaos = Arc::new(Chaos {
        profile: Profile::draw(&options),
        active: AtomicBool::new(true),
        fenced: AtomicBool::new(false),
        injected: Mutex::default(),
        fingerprint: Fingerprint::new(),
        cell: Mutex::default(),
    });
    let bug = match options.bug {
        Some(Deliberate::Engine(bug)) => Some(bug),
        _ => None,
    };
    let losable = chaos.profile.classes.contains(&Class::LostTail);
    let fingerprint = chaos.fingerprint.clone();
    let disks = Arc::new(Disks::new(
        storage_faults(&chaos),
        bug,
        losable,
        fingerprint.clone(),
    ));
    let store = SimFactory::new(store_faults(&chaos, options.bug))
        .recorded(fingerprint.clone())
        .observed(watch_membership(&chaos));
    install(Hooks {
        engine: {
            let disks = disks.clone();
            Box::new(move |dir, name| disks.open(dir, name) as Arc<dyn StorageEngine>)
        },
        namespaces: {
            let store = store.clone();
            Box::new(move || Arc::new(store.clone()) as Arc<dyn NamespaceFactory>)
        },
        rpc: {
            let chaos = chaos.clone();
            let strikes = Arc::new(move || chaos.strikes(Class::Rpc));
            Box::new(rpc::hook(strikes, fingerprint))
        },
    });
    let run = Arc::new(Run {
        chaos,
        disks,
        store,
        writing: AtomicBool::new(true),
        writers: AtomicUsize::new(0),
        history: Mutex::default(),
        samples: Mutex::default(),
        failure: Mutex::default(),
        trace: Mutex::default(),
        started: bedrock_lib::monotonic_now(),
        exited: Mutex::default(),
        busy: Mutex::default(),
        views: Mutex::default(),
        stale: Mutex::default(),
        last_ack: Mutex::default(),
    });
    run.record(format!("{:?}", run.chaos.profile));
    let processes = start_cluster(&run);
    start_observer(&run);
    let client = Handle::current()
        .create_node()
        .name("client")
        .ip(CLIENT.parse().unwrap())
        .build();
    // Everything from here runs on the client, which has an address to reach the processes from.
    client
        .spawn(drive(run, processes, options))
        .await
        .expect("the client does not crash")
}

async fn drive(
    run: Arc<Run>,
    processes: Vec<Process>,
    options: Options,
) -> Result<Outcome, Violation> {
    let profile = &run.chaos.profile;
    run.writers.store(WRITERS, Ordering::SeqCst);
    for id in 0..WRITERS {
        tokio::spawn(writer(run.clone(), id));
    }
    tokio::spawn(sampler(run.clone(), processes.clone()));
    let deadline = bedrock_lib::monotonic_now() + profile.safety;
    let drivers: Vec<_> = (0..profile.drivers)
        .map(|_| tokio::spawn(driver(run.clone(), processes.clone(), deadline)))
        .collect();
    if let Some(Deliberate::Rogue(rogue_write)) = options.bug {
        tokio::spawn(rogue(run.clone(), rogue_write));
    }
    let lossy = matches!(
        options.bug,
        Some(Deliberate::LostFlush | Deliberate::LostRetries)
    );
    if options.restart_all || lossy {
        sleep(profile.safety / 2).await;
        // Acknowledged before the restart, so only what the store kept can recognise it afterwards.
        let last = run.last_ack.lock().unwrap().clone();
        for process in &processes {
            run.record(format!("restart {}", process.name));
            crash(&run, process, Duration::from_millis(500)).await;
        }
        if options.bug == Some(Deliberate::LostRetries) {
            retry(&run, last.expect("a write was acknowledged by now")).await;
        }
    }
    if let Some(Deliberate::Wedged(name)) = options.bug {
        sleep(profile.safety / 2).await;
        let process = processes.iter().find(|p| p.name == name).unwrap();
        for engine in run.disks.of(&process.dir) {
            engine.wedge();
        }
    }
    for driver in drivers {
        driver.await.unwrap();
    }
    let kv = || processes.iter().filter(|p| stays_down(&run, p));
    if options.cold_damage {
        run.writing.store(false, Ordering::SeqCst);
        while run.writers.load(Ordering::SeqCst) > 0 {
            sleep(Duration::from_millis(100)).await;
        }
        let process = processes.iter().find(|p| !p.datasets.is_empty()).unwrap();
        for engine in run.disks.of(&process.dir) {
            damage(&engine);
        }
    }
    for process in kv() {
        match profile.kv {
            Kv::Stalled => Handle::current().pause(process.id),
            _ => Handle::current().kill(process.id),
        }
    }
    let converged = liveness(&run, &processes).await;
    let failure = run.failure.lock().unwrap().clone();
    let checked = match failure {
        Some(violation) => Err(violation),
        None => converged.and_then(|()| verify(&run, &processes)),
    };
    if let Err(violation) = &checked {
        // Failures replay too: the same seed must reach the same violation through the same run.
        eprintln!("VOPR failed trace={:016x} {violation}", trace(&run));
        report(&run, &processes).await;
    }
    checked?;
    if profile.kv != Kv::Serving {
        for process in kv() {
            match profile.kv {
                Kv::Stalled => Handle::current().resume(process.id),
                _ => rpc::bounce(process.id).await,
            }
        }
        let views = run.views().unwrap();
        let joined = async {
            while views.kv.members().map_or(0, |members| members.len()) < KVS.len() {
                sleep(Duration::from_millis(200)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(60), joined)
            .await
            .map_err(|_| Violation {
                invariant: "Converged",
                detail: "bedrock-kv did not rejoin".into(),
            })?;
    }
    let trace = trace(&run);
    let history = run.history.lock().unwrap();
    let acknowledged = history.streams().map(|stream| history.head(stream)).sum();
    Ok(Outcome {
        trace,
        acknowledged,
        simulated: run.started.elapsed(),
        injected: run.chaos.injected.lock().unwrap().clone(),
        arrival: format!("{:?}", profile.arrival),
    })
}

#[allow(clippy::disallowed_methods)] // A fresh OS thread resets madsim's seeded std map hashing.
fn simulate(seed: u64, options: Options) -> Result<Outcome, Violation> {
    if std::env::var_os("RUST_LOG").is_some() {
        bedrock_lib::node::init_tracing();
    }
    std::thread::spawn(move || {
        let mut runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
        runtime.set_time_limit(safety() + LIVENESS_BOUND + Duration::from_secs(300));
        runtime.block_on(scenario(options))
    })
    .join()
    .unwrap_or_else(|panic| {
        let faults = std::env::var("BEDROCK_VOPR_FAULTS");
        eprintln!("{}", replay(seed, faults.as_deref().unwrap_or(ALL)));
        std::panic::resume_unwind(panic)
    })
}

/// The command that reruns `seed`. A seed only reproduces at the same length.
fn replay(seed: u64, faults: &str) -> String {
    let seconds = safety().as_secs();
    format!(
        "Replay: BEDROCK_VOPR_SECONDS={seconds} BEDROCK_VOPR_FAULTS={faults} scripts/vopr.sh replay {seed}"
    )
}

/// Runs `seed`, failing with its replay command.
fn run_seed(seed: u64, faults: &str) -> Outcome {
    let outcome = simulate(seed, Options::with_faults(faults)).unwrap_or_else(|violation| {
        panic!("seed {seed} broke {violation}\n{}", replay(seed, faults))
    });
    println!(
        "VOPR seed={seed} trace={:016x} acknowledged={} injected={:?} arrival={} simulated={:?}",
        outcome.trace, outcome.acknowledged, outcome.injected, outcome.arrival, outcome.simulated
    );
    outcome
}

/// madsim's maps and task IDs keep state across a process, so a seed run after others can take a different path. Separate
/// processes make these runs the ones a sweep and `scripts/vopr.sh replay` see.
#[allow(clippy::disallowed_methods)]
fn run_each_in_its_own_process() {
    let test = std::env::current_exe().unwrap();
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    for batch in SEEDS.concat().chunks(cores) {
        let running: Vec<_> = batch
            .iter()
            .map(|(seed, faults)| {
                let child = std::process::Command::new(&test)
                    .args(["--exact", "seeds_hold_every_invariant", "--nocapture"])
                    .env("BEDROCK_VOPR_SEED", seed.to_string())
                    .env("BEDROCK_VOPR_FAULTS", faults)
                    .spawn();
                (seed, child.unwrap())
            })
            .collect();
        for (seed, mut child) in running {
            assert!(child.wait().unwrap().success(), "seed {seed} failed");
        }
    }
}

#[test]
fn seeds_hold_every_invariant() {
    match std::env::var("BEDROCK_VOPR_SEED") {
        Ok(seed) => {
            let faults = std::env::var("BEDROCK_VOPR_FAULTS");
            run_seed(
                seed.parse().expect("BEDROCK_VOPR_SEED is a u64"),
                faults.as_deref().unwrap_or(ALL),
            );
        }
        Err(_) => run_each_in_its_own_process(),
    }
}

#[test]
fn same_seed_replays_the_same_schedule() {
    // Compares every random draw of two runs, which also covers the HTTP write and subscriber session paths.
    let run = || scenario(Options::with_faults(ALL));
    Runtime::check_determinism(SAMPLE[0].0, madsim::Config::default(), run).unwrap();
}

#[test]
fn different_seeds_take_different_paths() {
    let run = |seed| simulate(seed, Options::with_faults(ALL)).map(|outcome| outcome.trace);
    assert_ne!(run(1), run(2), "the run must depend on the seed");
}

#[test]
fn a_restart_keeps_acknowledged_data() {
    let options = Options {
        restart_all: true,
        ..Options::default()
    };
    let outcome = simulate(7, options).unwrap_or_else(|violation| panic!("{violation}"));
    assert!(outcome.acknowledged > 0);
}

/// Found by seed 21641 (#25 / B-23): damage only shows when something reads the record, and a cold record is read by
/// nothing but the scrubber.
#[test]
fn damage_in_a_cold_key_is_found() {
    let options = Options {
        cold_damage: true,
        ..Options::default()
    };
    simulate(7, options).unwrap_or_else(|violation| panic!("{violation}"));
}

#[test]
fn deliberate_bugs_break_their_invariants() {
    for (bug, invariant) in [
        (Deliberate::Engine(Bug::SwallowDelete), "NoResurrection"),
        (Deliberate::Engine(Bug::StaleValue), "NoStaleOverwrite"),
        (Deliberate::Engine(Bug::DropData), "GapFreeApply"),
        (Deliberate::LostFlush, "NoLostAck"),
        (Deliberate::Engine(Bug::KeepRange), "OwnedOrLeaving"),
        (Deliberate::LostRetries, "RetryProtection"),
        (Deliberate::Rogue(Rogue::Gap), "DirectoryCoverage"),
        (Deliberate::Rogue(Rogue::Unready), "NoDropBeforeReady"),
        (Deliberate::Rogue(Rogue::Unhealthy), "NoUnhealthyRouting"),
        (Deliberate::Wedged("subscriber"), "Converged"),
        (Deliberate::Wedged("quicksilver-1"), "RedundancyRestored"),
    ] {
        let options = Options {
            bug: Some(bug),
            ..Options::default()
        };
        match simulate(11, options) {
            Err(violation) => assert_eq!(violation.invariant, invariant, "{bug:?}: {violation}"),
            Ok(_) => panic!("{bug:?} went unnoticed"),
        }
    }
}
