//! Real-binary crash and fault checks for #25 / B-23 Layer 3.
//!
//! A run starts the five binaries as processes on loopback ports, with RocksDB, the real namespace engine and an S3
//! service, writes through the real HTTP API and kills processes with SIGKILL. It reuses the simulation's checker
//! ([`bedrock_sim::history`]): every acknowledged write, and every copy a node holds, must match the history.
//!
//! Differences from the simulation (`tests/sim/tests/vopr.rs`), kept explicit as the issue asks:
//! - **Not deterministic.** The seed picks the workload and the faults, but the kernel decides the interleaving, so a
//!   failure is reported with its seed, commit and fault log and may not repeat.
//! - **Copies are read after a kill.** A running node holds its RocksDB lock, so the harness inspects a node's disk only
//!   after killing it. Each kill is therefore also a crash-consistency check.
//! - **SIGKILL keeps the page cache.** Writes a process handed to the kernel survive, so power loss and fsync are not
//!   tested here.
#![cfg(not(madsim))]
// The B-23a I/O rules keep the product simulatable. This harness drives real processes, files, ports and clocks from the
// outside on purpose, and never runs under madsim.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod proxy;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use bedrock_lib::admin::Admin;
use bedrock_lib::membership::View;
use bedrock_lib::node::TlsConfig;
use bedrock_lib::placement::{Directory, State, hash_of};
use bedrock_lib::relays::client::{Selection, Session, Target, applied_at, applied_ranges};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::rocksdb::RocksDbAdapter;
use bedrock_lib::storage::{KeyRange, StorageEngine, WriteBatch, l2_key, l2_parts, l2_span};
use bedrock_lib::transport::tls::{Identity, Tls};
use bedrock_lib::transport::{Io, Peers};
use bedrock_sim::history::{
    History, Key, OwnerState, Placement, Range, Stream, Value, Violation, check_coverage,
    check_held, within,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use tokio::time::sleep;

use proxy::{Mode, Proxy};

const TOKEN: &str = "crash-writer-token";
const CELL: &str = "eu-1a";
/// The workload's streams, as in the simulation: every pushed dataset's namespaces.
const STREAMS: [(&str, &str); 3] = [
    ("quicksilver", "infrastructure"),
    ("cdn", "pages"),
    ("cdn", "workers"),
];
/// Streams the harness's own subscriber follows whole.
const FOLLOWED: [(&str, &str); 2] = [("quicksilver", "infrastructure"), ("cdn", "pages")];
const WRITERS: usize = 4;
/// Fault drivers at once, as in the simulation's busier profiles.
const DRIVERS: u64 = 2;
const KEYS: u32 = 64;
const DELETES: f64 = 0.2;
const PADDING: usize = 256;
/// Short, so long runs reach the bootstrap of a cursor whose history expired.
const EVENT_RETENTION_MS: u64 = 20_000;
const RETRY_RETENTION_MS: u64 = 120_000;
const REPLAY_WINDOW_MS: u64 = 30_000;
const GRACE_MS: u64 = 5_000;
/// How long the cluster has after the faults stop. Longer than the simulation's: real recoveries wait on real time.
const LIVENESS_BOUND: Duration = Duration::from_secs(300);

/// What a run does, from `BEDROCK_CRASH_*` variables.
#[derive(Debug, Clone)]
pub struct Options {
    pub seed: u64,
    /// How long faults run before the cluster must converge.
    pub safety: Duration,
    /// The five binaries, built with `--release`.
    pub bins: PathBuf,
    /// Data, configs and logs; a fresh directory per run.
    pub dir: PathBuf,
    /// An S3 endpoint that accepts any credentials, such as SeaweedFS.
    pub s3: String,
    /// The fault classes the run injects.
    pub faults: Vec<Fault>,
}

/// What the harness does to the cluster during the safety phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// SIGKILL a process, then restart it.
    Kill,
    /// While a storage node is killed: cut its RocksDB WAL tail, flip bytes in an SST file, or wipe the dataset.
    Disk,
    /// Refuse, stall, half-stall or slow the traffic into a node.
    Network,
    /// Refuse, slow or stall S3 requests, or cut their replies so the outcome is unknown.
    S3,
}

const FAULTS: [(&str, Fault); 4] = [
    ("kill", Fault::Kill),
    ("disk", Fault::Disk),
    ("network", Fault::Network),
    ("s3", Fault::S3),
];

impl Options {
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(format!("BEDROCK_CRASH_{name}")).ok();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let seed = var("SEED").map_or_else(
            || {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            },
            |seed| seed.parse().expect("BEDROCK_CRASH_SEED is a number"),
        );
        Options {
            seed,
            safety: Duration::from_secs(var("SECONDS").map_or(60, |s| {
                s.parse().expect("BEDROCK_CRASH_SECONDS is a number")
            })),
            bins: var("BINS").map_or_else(|| workspace.join("target/release"), PathBuf::from),
            dir: var("DIR").map_or_else(
                || std::env::temp_dir().join(format!("bedrock-crash-{seed}")),
                PathBuf::from,
            ),
            s3: var("S3").unwrap_or_else(|| "http://127.0.0.1:8333".into()),
            faults: {
                let named = var("FAULTS").unwrap_or_else(|| "all".into());
                let named: Vec<&str> = named.split(',').collect();
                for name in &named {
                    assert!(
                        matches!(*name, "all" | "none")
                            || FAULTS.iter().any(|(known, _)| known == name),
                        "unknown fault class {name}"
                    );
                }
                FAULTS
                    .iter()
                    .filter(|(name, _)| {
                        !named.contains(&"none") && (named.contains(name) || named.contains(&"all"))
                    })
                    .map(|(_, fault)| *fault)
                    .collect()
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Api,
    Region,
    Relay,
    Quicksilver,
    Cdn,
    Kv,
}

impl Kind {
    fn binary(self) -> &'static str {
        match self {
            Kind::Api => "bedrock-api",
            Kind::Region | Kind::Relay => "bedrock-relay",
            Kind::Quicksilver => "bedrock-quicksilver",
            Kind::Cdn => "bedrock-cdn",
            Kind::Kv => "bedrock-kv",
        }
    }

    fn datasets(self) -> &'static [&'static str] {
        match self {
            Kind::Quicksilver => &["quicksilver"],
            Kind::Cdn => &["cdn"],
            _ => &[],
        }
    }
}

/// One process of the cluster, started and killed by the harness.
pub struct Node {
    pub name: String,
    kind: Kind,
    listen: u16,
    admin: u16,
    /// The API's HTTP write port; 0 for the others.
    write: u16,
    /// What every other process dials instead of `listen`, and `write`.
    proxy: Proxy,
    write_proxy: Option<Proxy>,
    data: PathBuf,
    config: PathBuf,
    log: PathBuf,
    child: Mutex<Option<Child>>,
}

impl Node {
    /// The address other processes dial: the node's proxy.
    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.proxy.port)
    }

    fn running(&self) -> bool {
        self.child.lock().unwrap().is_some()
    }
}

/// A free loopback port, held in `reserved` so no later bind gets it before its process does.
fn free_port(reserved: &mut Vec<std::net::TcpListener>) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    reserved.push(listener);
    port
}

fn certificate_paths(dir: &Path, name: &str) -> TlsConfig {
    let dir = dir.join("tls");
    TlsConfig {
        cert: dir.join(format!("{name}.crt")),
        key: dir.join(format!("{name}.key")),
        ca: dir.join("ca.crt"),
    }
}

/// Every process and the harness trust one run's CA; disk faults leave these credentials intact.
fn certificates(dir: &Path, nodes: &[Arc<Node>]) -> TlsConfig {
    fs::create_dir_all(dir.join("tls")).unwrap();
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = params.self_signed(&key).unwrap();
    fs::write(dir.join("tls/ca.crt"), ca.pem()).unwrap();
    let issuer = Issuer::new(params, key);
    let identities = nodes
        .iter()
        .map(|node| {
            (
                node.name.as_str(),
                node.kind.binary().trim_start_matches("bedrock-"),
            )
        })
        .chain(std::iter::once(("harness", "relay")));
    for (name, service) in identities {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![
            SanType::URI(
                format!("bedrock://{service}/{CELL}/{name}")
                    .try_into()
                    .unwrap(),
            ),
            SanType::IpAddress("127.0.0.1".parse().unwrap()),
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &issuer).unwrap();
        let paths = certificate_paths(dir, name);
        fs::write(paths.cert, cert.pem()).unwrap();
        fs::write(paths.key, key.serialize_pem()).unwrap();
    }
    certificate_paths(dir, "harness")
}

/// The cluster of the simulation, on loopback: an API, two region relays, three cell relays publishing membership,
/// four `bedrock-quicksilver`, four `bedrock-cdn` and two `bedrock-kv`, so nodes differ in what they own.
pub struct Cluster {
    options: Options,
    bucket: String,
    nodes: Vec<Arc<Node>>,
    /// What the API dials for S3.
    s3: Proxy,
    tls: Arc<Tls>,
}

impl Cluster {
    async fn new(options: Options) -> Self {
        let _ = fs::remove_dir_all(&options.dir);
        fs::create_dir_all(options.dir.join("logs")).unwrap();
        let mut names = vec![("api".to_owned(), Kind::Api)];
        let counts = [
            (Kind::Region, "region", 2),
            (Kind::Relay, "relay", 3),
            (Kind::Quicksilver, "quicksilver", 4),
        ];
        let more = [(Kind::Cdn, "cdn", 4), (Kind::Kv, "kv", 2)];
        for (kind, prefix, count) in counts.into_iter().chain(more) {
            names.extend((1..=count).map(|i| (format!("{prefix}-{i}"), kind)));
        }
        let (mut nodes, mut reserved) = (Vec::new(), Vec::new());
        for (name, kind) in names {
            let data = options.dir.join(&name);
            fs::create_dir_all(&data).unwrap();
            let listen = free_port(&mut reserved);
            let write = if kind == Kind::Api {
                free_port(&mut reserved)
            } else {
                0
            };
            nodes.push(Arc::new(Node {
                config: options.dir.join(format!("{name}.toml")),
                log: options.dir.join("logs").join(format!("{name}.log")),
                name,
                kind,
                listen,
                admin: free_port(&mut reserved),
                write,
                proxy: Proxy::start(listen).await,
                write_proxy: if write == 0 {
                    None
                } else {
                    Some(Proxy::start(write).await)
                },
                data,
                child: Mutex::new(None),
            }));
        }
        let s3_port = options
            .s3
            .rsplit(':')
            .next()
            .and_then(|port| port.parse().ok());
        let s3 = Proxy::start(s3_port.expect("BEDROCK_CRASH_S3 ends with a port")).await;
        // Every port and proxy is bound, so the processes can have theirs.
        drop(reserved);
        // Every run writes a fresh bucket: the API would otherwise open an earlier run's namespaces.
        let bucket = format!("crash-{}-{}", options.seed, std::process::id());
        let tls = Tls::start(
            &certificates(&options.dir, &nodes),
            Identity {
                service: "relay".into(),
                cell: CELL.into(),
                node_id: "harness".into(),
                host: "127.0.0.1".into(),
            },
            &Admin::new(),
        )
        .await
        .unwrap();
        let cluster = Cluster {
            options,
            bucket,
            nodes,
            s3,
            tls,
        };
        for node in &cluster.nodes {
            fs::write(&node.config, cluster.config(node)).unwrap();
        }
        cluster
    }

    fn of(&self, kind: Kind) -> impl Iterator<Item = &Arc<Node>> {
        self.nodes.iter().filter(move |node| node.kind == kind)
    }

    fn addresses(&self, kind: Kind) -> Vec<String> {
        self.of(kind).map(|node| node.address()).collect()
    }

    fn api(&self) -> &Node {
        self.of(Kind::Api).next().unwrap()
    }

    fn write_address(&self) -> String {
        format!(
            "127.0.0.1:{}",
            self.api().write_proxy.as_ref().unwrap().port
        )
    }

    fn config(&self, node: &Node) -> String {
        let list = |addresses: Vec<String>| {
            let quoted: Vec<_> = addresses.iter().map(|a| format!("{a:?}")).collect();
            format!("[{}]", quoted.join(", "))
        };
        let others = |kind| {
            let addresses = self.addresses(kind);
            list(
                addresses
                    .into_iter()
                    .filter(|a| *a != node.address())
                    .collect(),
            )
        };
        let mut top = format!(
            "node_id = {:?}\nlisten = \"127.0.0.1:{}\"\nadvertise = {:?}\nadmin_listen = \"127.0.0.1:{}\"\ndata_dir = {:?}\n",
            node.name,
            node.listen,
            node.address(),
            node.admin,
            node.data,
        );
        let mut tables = format!("[location]\nregion = \"eu\"\ndc = \"arn1\"\ncell = {CELL:?}\n");
        let tls = certificate_paths(&self.options.dir, &node.name);
        tables += &format!(
            "[tls]\ncert = {:?}\nkey = {:?}\nca = {:?}\n",
            tls.cert, tls.key, tls.ca
        );
        let replay = format!("[replay]\nwindow_ms = {REPLAY_WINDOW_MS}\n");
        match node.kind {
            Kind::Api => {
                top += &format!(
                    "write_listen = \"127.0.0.1:{}\"\nwrite_token = {TOKEN:?}\n",
                    node.write
                );
                tables += &format!(
                    "[s3]\nendpoint = {:?}\nbucket = {:?}\naccess_key_id = \"local\"\nsecret_access_key = \"local\"\n\
                     [retention]\nevent_retention_ms = {EVENT_RETENTION_MS}\nretry_retention_ms = {RETRY_RETENTION_MS}\n",
                    format!("http://127.0.0.1:{}", self.s3.port),
                    self.bucket,
                );
            }
            Kind::Region => {
                top += &format!(
                    "tier = \"region\"\nupstreams = {}\nreplay_peers = {}\n",
                    list(vec![self.api().address()]),
                    others(Kind::Region),
                );
                tables += &replay;
            }
            Kind::Relay => {
                top += &format!(
                    "tier = \"cell\"\nupstreams = {}\nreplay_peers = {}\nwrite_token = {TOKEN:?}\n",
                    list(self.addresses(Kind::Region)),
                    others(Kind::Relay),
                );
                tables += &replay;
                tables += &format!("[membership]\nbootstrap = true\ngrace_ms = {GRACE_MS}\n");
                tables += "[membership.voter_ids]\n";
                for voter in self.of(Kind::Relay) {
                    tables += &format!("{:?} = {:?}\n", voter.address(), voter.name);
                }
            }
            Kind::Quicksilver | Kind::Cdn | Kind::Kv => {
                let datasets: Vec<String> = node
                    .kind
                    .datasets()
                    .iter()
                    .map(|d| (*d).to_owned())
                    .collect();
                top += &format!(
                    "roles = [\"l2\"]\ndatasets = {}\nrelays = {}\napi = {:?}\ns3_endpoint = {:?}\n",
                    list(datasets),
                    list(self.addresses(Kind::Relay)),
                    self.write_address(),
                    self.options.s3,
                );
                if node.kind == Kind::Kv {
                    tables += &format!(
                        "[s3]\nendpoint = {:?}\nbucket = {:?}\naccess_key_id = \"local\"\nsecret_access_key = \"local\"\n",
                        format!("http://127.0.0.1:{}", self.s3.port),
                        self.bucket,
                    );
                }
            }
        }
        top + &tables
    }

    fn start(&self, node: &Node) {
        let log = File::options()
            .create(true)
            .append(true)
            .open(&node.log)
            .unwrap();
        let child = Command::new(self.options.bins.join(node.kind.binary()))
            .env("BEDROCK_CONFIG", &node.config)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap_or_else(|error| panic!("{} did not start: {error}", node.kind.binary()));
        *node.child.lock().unwrap() = Some(child);
    }

    /// SIGKILL, as a crash or `kill -9` would.
    fn kill(&self, node: &Node) {
        if let Some(mut child) = node.child.lock().unwrap().take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// A process that exited by itself, and how. `None` while it runs or after the harness killed it.
    fn exited(&self, node: &Node) -> Option<std::process::ExitStatus> {
        let mut child = node.child.lock().unwrap();
        let status = child.as_mut()?.try_wait().ok()??;
        *child = None;
        Some(status)
    }

    async fn create_bucket(&self) {
        let endpoint = self.options.s3.trim_start_matches("http://");
        let status = request(
            None,
            endpoint,
            "PUT",
            &format!("/{}", self.bucket),
            &[],
            Vec::new(),
        )
        .await
        .map(|(status, _)| status);
        assert!(
            status.is_some_and(|status| status < 300),
            "S3 at {} did not create bucket {}: {status:?}",
            self.options.s3,
            self.bucket
        );
    }

    fn stop(&self) {
        for node in &self.nodes {
            self.kill(node);
        }
    }
}

/// One HTTP/1.1 request to `address`; `None` when there is no answer.
async fn request(
    tls: Option<&Tls>,
    address: &str,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
) -> Option<(u16, Bytes)> {
    let attempt = async {
        let io: Box<dyn Io> = match tls {
            Some(tls) => tls.dial(address).await.ok()?,
            None => Box::new(tokio::net::TcpStream::connect(address).await.ok()?),
        };
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .ok()?;
        tokio::spawn(connection);
        let mut builder = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "bedrock");
        for (name, value) in headers {
            builder = builder.header(*name, value);
        }
        let response = sender
            .send_request(builder.body(Full::new(Bytes::from(body))).unwrap())
            .await
            .ok()?;
        let status = response.status().as_u16();
        Some((
            status,
            response.into_body().collect().await.ok()?.to_bytes(),
        ))
    };
    tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .ok()
        .flatten()
}

async fn ready(node: &Node) -> bool {
    let address = format!("127.0.0.1:{}", node.admin);
    request(None, &address, "GET", "/ready", &[], Vec::new())
        .await
        .is_some_and(|(status, _)| status == 200)
}

/// One write, returning its sequence. `None` means the outcome is unknown.
async fn submit(
    tls: &Tls,
    address: &str,
    (dataset, namespace): &Stream,
    id: &str,
    key: &[u8],
    value: Option<&[u8]>,
) -> Option<u64> {
    let key = String::from_utf8(key.to_vec()).unwrap();
    let headers = [
        ("authorization", format!("Bearer {TOKEN}")),
        ("idempotency-key", id.to_owned()),
    ];
    let method = if value.is_some() { "PUT" } else { "DELETE" };
    let path = format!("/v1/{dataset}/{namespace}/{key}");
    let (status, body) = request(
        Some(tls),
        address,
        method,
        &path,
        &headers,
        value.unwrap_or_default().to_vec(),
    )
    .await?;
    assert_ne!(status, 400, "the workload sent an invalid write");
    (status < 300).then(|| {
        std::str::from_utf8(&body)
            .unwrap()
            .parse()
            .expect("the response body is the sequence")
    })
}

fn key_of(n: u32) -> Key {
    format!("k{n:02}").into_bytes()
}

fn followed_prefix(namespace: &str) -> Vec<u8> {
    format!("sub/{namespace}/").into_bytes()
}

/// A copy of one stream's selected keys, checked against the history once it can explain the copy's revision.
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

/// What a killed node's disk holds: each validated range copy, and the hash of every L2 record per dataset. A
/// quarantined or unreadable instance is skipped: its node must notice and rebuild it, which liveness checks.
fn inspect(node: &Node) -> (Vec<Sample>, BTreeMap<&'static str, Vec<u64>>) {
    read_disk(&node.name, &node.data, node.kind.datasets())
}

/// [`inspect`] for the data directory `data` of node `name`, which stores `datasets`.
fn read_disk(
    name: &str,
    data: &Path,
    datasets: &[&'static str],
) -> (Vec<Sample>, BTreeMap<&'static str, Vec<u64>>) {
    let mut samples = Vec::new();
    let mut hashes = BTreeMap::new();
    for dataset in datasets {
        let Ok(engine) = RocksDbAdapter::open(&data.join(dataset), *dataset, &Default::default())
        else {
            continue;
        };
        let Ok(snapshot) = engine.snapshot() else {
            continue;
        };
        if snapshot
            .revision("health:quarantined")
            .ok()
            .flatten()
            .is_some()
        {
            continue;
        }
        let Ok(ranges) = applied_ranges(&*snapshot, dataset) else {
            continue;
        };
        let mut copies = Vec::new();
        let read = (|| {
            for (namespace, span, revision) in ranges {
                let mut held = BTreeMap::new();
                for entry in snapshot.scan(&l2_span(span.0, span.1)).ok()? {
                    let (key, value) = entry.ok()?;
                    let (stored, key) = l2_parts(&key)?;
                    if stored == namespace.as_bytes() {
                        held.insert(key.to_vec(), value);
                    }
                }
                copies.push(Sample {
                    node: name.to_owned(),
                    stream: ((*dataset).to_owned(), namespace),
                    revision,
                    span: Some(span),
                    held,
                });
            }
            let mut held = Vec::new();
            for entry in snapshot.scan(&KeyRange::prefix(*b"l2/")).ok()? {
                let (key, _) = entry.ok()?;
                held.push(u64::from_be_bytes(key.get(3..11)?.try_into().ok()?));
            }
            Some(held)
        })();
        if let Some(held) = read {
            samples.extend(copies);
            hashes.insert(*dataset, held);
        }
    }
    (samples, hashes)
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

fn covers(outer: Range, inner: Range) -> bool {
    within(outer, inner.0) && (outer.1 == 0 || (inner.1 != 0 && inner.1 <= outer.1))
}

/// The membership views an operator's dashboard would keep, one per service, over the real relays.
struct Views {
    quicksilver: View,
    cdn: View,
    observer: Arc<Session>,
    unavailable: Mutex<BTreeMap<&'static str, String>>,
}

impl Views {
    fn of(&self, dataset: &str) -> &View {
        match dataset {
            "cdn" => &self.cdn,
            _ => &self.quicksilver,
        }
    }

    fn read<T, E: std::fmt::Display>(
        &self,
        dataset: &str,
        read: impl FnOnce(&View) -> Result<T, E>,
    ) -> Result<T, String> {
        let unavailable = self.unavailable.lock().unwrap();
        if let Some(error) = unavailable.get(dataset) {
            return Err(error.clone());
        }
        if !self.observer.ready() {
            return Err("observer has no validated membership revision".into());
        }
        let namespace = bedrock_lib::membership::namespace(dataset, CELL);
        let revision = self
            .observer
            .applied(bedrock_lib::membership::DATASET, &namespace)
            .ok_or("observer has no validated membership revision")?;
        let value = read(self.of(dataset)).map_err(|error| error.to_string())?;
        if !self.observer.ready()
            || self
                .observer
                .applied(bedrock_lib::membership::DATASET, &namespace)
                != Some(revision)
        {
            return Err("observer revision changed during read".into());
        }
        Ok(value)
    }

    fn refresh(&self, peers: &Peers) {
        let mut unavailable = self.unavailable.lock().unwrap();
        for dataset in ["quicksilver", "cdn"] {
            let namespace = bedrock_lib::membership::namespace(dataset, CELL);
            let refreshed = if !self.observer.ready()
                || self
                    .observer
                    .applied(bedrock_lib::membership::DATASET, &namespace)
                    .is_none()
            {
                Err("observer has no validated membership revision".to_owned())
            } else {
                self.of(dataset)
                    .refresh(&self.observer, peers)
                    .map_err(|error| error.to_string())
            };
            match refreshed {
                Ok(()) => {
                    unavailable.remove(dataset);
                }
                Err(error) => {
                    unavailable.insert(dataset, error);
                }
            }
        }
    }
}

struct Run {
    cluster: Cluster,
    history: Mutex<History>,
    samples: Mutex<Vec<Sample>>,
    failure: Mutex<Option<Violation>>,
    log: Mutex<Vec<String>>,
    started: Instant,
    writing: AtomicBool,
    writers: AtomicUsize,
    faults: AtomicBool,
    /// Nodes a fault is working on, which the supervisor leaves alone.
    busy: Mutex<Vec<String>>,
    subscribed: Arc<MemoryEngine>,
    views: Views,
}

impl Run {
    fn record(&self, event: String) {
        let line = format!("{:>8.3}s {event}", self.started.elapsed().as_secs_f64());
        self.log.lock().unwrap().push(line);
    }

    fn check(&self, result: Result<(), Violation>) {
        if let Err(violation) = result {
            self.record(format!("violation {violation}"));
            self.failure.lock().unwrap().get_or_insert(violation);
        }
    }

    fn failed(&self) -> bool {
        self.failure.lock().unwrap().is_some()
    }

    /// Checks every sample whose revision the history can already explain; the rest wait for unknown outcomes.
    fn check_samples(&self) {
        let history = self.history.lock().unwrap();
        let mut failure = None;
        self.samples.lock().unwrap().retain(|sample| {
            let waiting = sample.revision > history.known_through(&sample.stream);
            if !waiting && failure.is_none() {
                failure = sample.check(&history).err();
            }
            waiting
        });
        drop(history);
        self.check(failure.map_or(Ok(()), Err));
    }

    /// The harness's own subscriber copies, as samples.
    fn subscriber_copies(&self) -> Vec<Sample> {
        let snapshot = self.subscribed.snapshot().unwrap();
        let mut samples = Vec::new();
        for (dataset, namespace) in FOLLOWED {
            let prefix = followed_prefix(namespace);
            let Some(revision) = applied_at(&*snapshot, dataset, namespace, &prefix, None).unwrap()
            else {
                continue;
            };
            let held = snapshot
                .scan(&KeyRange::prefix(prefix.clone()))
                .unwrap()
                .map(Result::unwrap)
                .map(|(key, value)| (key[prefix.len()..].to_vec(), value))
                .collect();
            samples.push(Sample {
                node: "subscriber".into(),
                stream: (dataset.to_owned(), namespace.to_owned()),
                revision,
                span: None,
                held,
            });
        }
        samples
    }
}

/// Writes until told to stop. A write with an unknown outcome is retried with the same idempotency key until it is
/// acknowledged, so a finished run knows every sequence.
async fn writer(run: Arc<Run>, id: usize) {
    let mut rng = StdRng::seed_from_u64(run.cluster.options.seed ^ ((id as u64 + 1) << 32));
    let mut n = 0u64;
    while run.writing.load(Ordering::SeqCst) && !run.failed() {
        n += 1;
        let operation = format!("w{id}-{n}");
        let (dataset, namespace) = STREAMS[rng.gen_range(0..STREAMS.len())];
        let stream = (dataset.to_owned(), namespace.to_owned());
        let key = key_of(rng.gen_range(0..KEYS));
        let value = (!rng.gen_bool(DELETES)).then(|| {
            let mut value = format!("{operation}:").into_bytes();
            value.resize(value.len() + PADDING, b'x');
            value
        });
        loop {
            let address = run.cluster.write_address();
            if let Some(sequence) = submit(
                &run.cluster.tls,
                &address,
                &stream,
                &operation,
                &key,
                value.as_deref(),
            )
            .await
            {
                let acknowledged = run.history.lock().unwrap().acknowledged(
                    &stream,
                    sequence,
                    &operation,
                    &key,
                    value.as_deref(),
                );
                run.check(acknowledged);
                break;
            }
            if run.failed() {
                break;
            }
            sleep(Duration::from_millis(rng.gen_range(50..500))).await;
        }
        sleep(Duration::from_millis(rng.gen_range(10..200))).await;
    }
    run.writers.fetch_sub(1, Ordering::SeqCst);
}

/// Restarts processes that exited by themselves, as a process supervisor would. A panic is a finding: release builds
/// abort on one, which the harness sees as SIGABRT.
async fn supervisor(run: Arc<Run>) {
    use std::os::unix::process::ExitStatusExt;
    while !run.failed() {
        sleep(Duration::from_millis(500)).await;
        for node in &run.cluster.nodes {
            if run.busy.lock().unwrap().contains(&node.name) {
                continue;
            }
            let Some(status) = run.cluster.exited(node) else {
                continue;
            };
            if status.signal() == Some(SIGABRT) {
                run.check(Err(Violation {
                    invariant: "NoPanic",
                    detail: format!("{} aborted; see {}", node.name, node.log.display()),
                }));
                return;
            }
            run.record(format!("{} exited ({status}); restarting", node.name));
            sleep(Duration::from_secs(1)).await;
            run.cluster.start(node);
        }
    }
}

const SIGABRT: i32 = 6;

/// Injects one fault at a time from the run's classes until the deadline. Several drivers run at once.
async fn driver(run: Arc<Run>, id: u64, deadline: Instant) {
    let faults = &run.cluster.options.faults;
    let mut rng = StdRng::seed_from_u64(run.cluster.options.seed ^ (id << 40));
    while Instant::now() < deadline && !run.failed() {
        sleep(Duration::from_millis(rng.gen_range(1_000..5_000))).await;
        let time = Duration::from_millis(rng.gen_range(100..10_000));
        let fault = faults[rng.gen_range(0..faults.len())];
        if fault == Fault::S3 {
            let mode = [
                Mode::Refuse,
                Mode::CutReplies,
                Mode::StallReplies,
                Mode::Delay(rng.gen_range(50..2_000)),
            ][rng.gen_range(0..4)];
            run.record(format!("s3 {mode:?} for {time:?}"));
            run.cluster.s3.set(mode);
            sleep(time).await;
            run.cluster.s3.set(Mode::Open);
            continue;
        }
        let node = run.cluster.nodes[rng.gen_range(0..run.cluster.nodes.len())].clone();
        if !node.running() || run.busy.lock().unwrap().contains(&node.name) {
            continue;
        }
        run.busy.lock().unwrap().push(node.name.clone());
        match fault {
            Fault::Network => {
                let mode = [
                    Mode::Refuse,
                    Mode::Stall,
                    Mode::StallReplies,
                    Mode::Delay(rng.gen_range(20..500)),
                ][rng.gen_range(0..4)];
                run.record(format!("network {} {mode:?} for {time:?}", node.name));
                for proxy in std::iter::once(&node.proxy).chain(&node.write_proxy) {
                    proxy.set(mode);
                }
                sleep(time).await;
                for proxy in std::iter::once(&node.proxy).chain(&node.write_proxy) {
                    proxy.set(Mode::Open);
                }
            }
            Fault::Kill | Fault::Disk => {
                run.record(format!("kill {} for {time:?}", node.name));
                run.cluster.kill(&node);
                let disk = fault == Fault::Disk && !node.kind.datasets().is_empty();
                // A cut WAL tail is a write the disk lost at the crash, so the inspection must see what remains.
                let dataset = node.kind.datasets().first().map(|_| {
                    let datasets = node.kind.datasets();
                    datasets[rng.gen_range(0..datasets.len())]
                });
                let damage = disk.then(|| rng.gen_range(0..3));
                if let (Some(0), Some(dataset)) = (damage, dataset) {
                    run.record(format!(
                        "cut the WAL tail of {} {dataset}: {:?}",
                        node.name,
                        cut_wal(&node.data.join(dataset), &mut rng)
                    ));
                }
                let (samples, _) = inspect(&node);
                run.samples.lock().unwrap().extend(samples);
                run.check_samples();
                match (damage, dataset) {
                    (Some(1), Some(dataset)) => {
                        let flipped = flip_sst(&node.data.join(dataset), &mut rng);
                        run.record(format!(
                            "flip a byte of {} {dataset}: {flipped:?}",
                            node.name
                        ));
                    }
                    (Some(2), Some(dataset)) => {
                        run.record(format!("wipe {} {dataset}", node.name));
                        if let Err(error) = fs::remove_dir_all(node.data.join(dataset)) {
                            assert_eq!(error.kind(), std::io::ErrorKind::NotFound, "{error}");
                        }
                    }
                    _ => {}
                }
                sleep(time).await;
                run.cluster.start(&node);
            }
            Fault::S3 => unreachable!(),
        }
        run.busy.lock().unwrap().retain(|name| *name != node.name);
    }
}

/// The files in `dir` with `extension`, in name order: RocksDB numbers them, so the newest is last.
fn files_ending(dir: &Path, extension: &str) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == extension))
        .collect();
    files.sort();
    files
}

/// Cuts up to 4 KiB off the newest WAL, as a crash that lost unsynced writes would. Returns the file and bytes cut.
fn cut_wal(dir: &Path, rng: &mut StdRng) -> Option<(PathBuf, u64)> {
    let wal = files_ending(dir, "log").pop()?;
    let length = fs::metadata(&wal).ok()?.len();
    let cut = rng.gen_range(0..=length.min(4_096));
    File::options()
        .write(true)
        .open(&wal)
        .ok()?
        .set_len(length - cut)
        .ok()?;
    Some((wal, cut))
}

/// Flips one byte in the first half of an SST file, where data blocks are, so reads fail their checksums. The index
/// and footer at the end are left alone: damage there stops RocksDB opening at all, which an operator repairs by
/// replacing the disk, as `wipe` does.
fn flip_sst(dir: &Path, rng: &mut StdRng) -> Option<(PathBuf, u64)> {
    let tables = files_ending(dir, "sst");
    let table = tables.get(rng.gen_range(0..tables.len().max(1)))?.clone();
    let mut bytes = fs::read(&table).ok()?;
    let at = rng.gen_range(0..(bytes.len() / 2).max(1));
    *bytes.get_mut(at)? ^= 0xff;
    fs::write(&table, bytes).ok()?;
    Some((table, at as u64))
}

/// Samples the subscriber's copies and the directory throughout the run.
async fn sampler(run: Arc<Run>) {
    while !run.failed() && run.faults.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(1_000)).await;
        let copies = run.subscriber_copies();
        run.samples.lock().unwrap().extend(copies);
        run.check_samples();
        for dataset in ["quicksilver", "cdn"] {
            if let Ok(directory) = run.views.read(dataset, |view| view.directory(dataset)) {
                let placed = placement(&directory);
                if !placed.is_empty() {
                    run.check(check_coverage(&placed));
                }
            }
        }
    }
}

/// Each dataset's current placement, which must cover the hash space.
fn placements(run: &Run) -> Result<BTreeMap<&'static str, Placement>, Violation> {
    let mut placements = BTreeMap::new();
    for dataset in ["quicksilver", "cdn"] {
        let directory = run
            .views
            .read(dataset, |view| view.directory(dataset))
            .map_err(|error| Violation {
                invariant: "RedundancyRestored",
                detail: format!("no {dataset} directory: {error}"),
            })?;
        let placed = placement(&directory);
        check_coverage(&placed)?;
        placements.insert(dataset, placed);
    }
    Ok(placements)
}

/// Whether the directory lists `node` as `ready` wherever it lists it: `/ready` counts only the ranges a node already
/// follows, so a range it was just given would not hold it back.
fn settled(run: &Run, node: &Node) -> bool {
    node.kind.datasets().iter().all(|dataset| {
        run.views
            .read(dataset, |view| view.directory(dataset))
            .is_ok_and(|directory| {
                directory
                    .values()
                    .flatten()
                    .all(|owner| owner.node != node.name || owner.state == State::Ready)
            })
    })
}

/// Whether `node`'s published member record reports none of its datasets unhealthy.
fn healthy(run: &Run, node: &Node) -> bool {
    node.kind.datasets().iter().all(|dataset| {
        run.views.read(dataset, View::members).is_ok_and(|members| {
            members
                .values()
                .filter(|member| member.node_id == node.name)
                .all(|member| !member.unhealthy.iter().any(|d| d == dataset))
        })
    })
}

/// Why the cluster has not converged yet, naming the invariant that breaks if it lasts past the bound.
async fn unconverged(run: &Run) -> Option<Violation> {
    let waiting = |invariant, detail: String| Some(Violation { invariant, detail });
    if run.writers.load(Ordering::SeqCst) > 0 {
        return waiting("Converged", "writes are still outstanding".into());
    }
    for node in &run.cluster.nodes {
        if !ready(node).await {
            return waiting("RedundancyRestored", format!("{} is not ready", node.name));
        }
    }
    for dataset in ["quicksilver", "cdn"] {
        let observed = run.views.read(dataset, |view| {
            Ok::<_, String>((
                view.directory(dataset).map_err(|error| error.to_string())?,
                view.members().map_err(|error| error.to_string())?,
            ))
        });
        let (directory, members) = match observed {
            Ok(observed) => observed,
            Err(error) => {
                return waiting("RedundancyRestored", format!("no {dataset} view: {error}"));
            }
        };
        if let Some(sick) = members
            .values()
            .find(|member| member.unhealthy.iter().any(|d| d == dataset))
        {
            let detail = format!("{} is still unhealthy for {dataset}", sick.node_id);
            return waiting("RedundancyRestored", detail);
        }
        let hosts = run
            .cluster
            .nodes
            .iter()
            .filter(|node| node.kind.datasets().contains(&dataset))
            .count();
        let placed = placement(&directory);
        if placed.is_empty() {
            return waiting("RedundancyRestored", format!("no {dataset} directory"));
        }
        for (range, owners) in &placed {
            let settled = owners.iter().all(|(_, state)| *state == OwnerState::Ready);
            if !settled || owners.len() != hosts.min(3) {
                let detail = format!("{dataset} range {range:x?} has owners {owners:?}");
                return waiting("RedundancyRestored", detail);
            }
        }
    }
    let history = run.history.lock().unwrap();
    let copies = run.subscriber_copies();
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

/// Stops faults and writes and waits for convergence, restarting what a supervisor would.
async fn liveness(run: &Arc<Run>) -> Result<(), Violation> {
    run.faults.store(false, Ordering::SeqCst);
    run.writing.store(false, Ordering::SeqCst);
    run.record("healed".into());
    let deadline = Instant::now() + LIVENESS_BOUND;
    loop {
        if let Some(violation) = run.failure.lock().unwrap().clone() {
            return Err(violation);
        }
        let Some(why) = unconverged(run).await else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            return Err(why);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// With the cluster converged and no writes, every storage node in turn is killed and its disk checked: every copy
/// matches the history, each range's owners hold it at the head, and nobody holds data outside its ranges.
async fn verify(run: &Run) -> Result<(), Violation> {
    {
        let history = run.history.lock().unwrap();
        history.gap_free()?;
        for sample in run.samples.lock().unwrap().iter() {
            sample.check(&history)?;
        }
        for sample in run.subscriber_copies() {
            sample.check(&history)?;
        }
    }
    placements(run)?;
    let storage: Vec<_> = run
        .cluster
        .nodes
        .iter()
        .filter(|node| !node.kind.datasets().is_empty())
        .cloned()
        .collect();
    // Readiness does not wait for the last writes, so a node may still be applying them: inspect it again until it has
    // caught up or the bound passes. Every inspection is checked for safety.
    let deadline = Instant::now() + LIVENESS_BOUND;
    for node in &storage {
        loop {
            run.cluster.kill(node);
            let (samples, hashes) = inspect(node);
            run.cluster.start(node);
            // Ranges keep merging and moving while the cluster settles (#66 / B-61), so ownership is read afresh and has
            // to come true: a node drops a revoked range only after its view shows the revoke.
            let placements = placements(run)?;
            let held = hashes.into_iter().find_map(|(dataset, hashes)| {
                check_held(&placements[dataset], &node.name, hashes).err()
            });
            let behind = {
                let history = run.history.lock().unwrap();
                for sample in &samples {
                    sample.check(&history)?;
                }
                held.or_else(|| behind(node, &samples, &placements, &history))
            };
            let Some(behind) = behind else {
                break;
            };
            // A node a rebuild emptied must bootstrap again, which takes longer than a fixed pause: killing it before it
            // is ready would restart that every time. `/ready` leaves out a quarantined dataset, so a rebuild must also
            // have finished, which the node's member record shows.
            sleep(Duration::from_secs(2)).await;
            while !(ready(node).await && healthy(run, node) && settled(run, node))
                && Instant::now() < deadline
            {
                sleep(Duration::from_millis(500)).await;
            }
            if Instant::now() >= deadline {
                return Err(behind);
            }
        }
    }
    Ok(())
}

/// Where `node` has not applied a stream through its head in a range the directory gives it.
fn behind(
    node: &Node,
    copies: &[Sample],
    placements: &BTreeMap<&str, Placement>,
    history: &History,
) -> Option<Violation> {
    for (dataset, placed) in placements {
        for (range, owners) in placed {
            if !owners.iter().any(|(owner, _)| *owner == node.name) {
                continue;
            }
            for stream in history.streams().filter(|(d, _)| d == dataset) {
                let head = history.head(stream);
                let caught_up = copies.iter().any(|copy| {
                    copy.stream == *stream
                        && copy.revision == head
                        && copy.span.is_some_and(|span| covers(span, *range))
                });
                if !caught_up {
                    let held: Vec<_> = copies
                        .iter()
                        .filter(|copy| copy.stream == *stream)
                        .map(|copy| (copy.span, copy.revision))
                        .collect();
                    return Some(Violation {
                        invariant: "Converged",
                        detail: format!(
                            "{} has not reached {head} of {stream:?} in {range:x?}; it holds {held:x?}",
                            node.name
                        ),
                    });
                }
            }
        }
    }
    None
}

/// What a finished run reports.
#[derive(Debug)]
pub struct Outcome {
    pub acknowledged: usize,
    pub kills: usize,
}

/// One run: start the cluster, write and kill through the safety phase, converge, then check every node's disk.
pub async fn run(options: Options) -> Result<Outcome, Violation> {
    let cluster = Cluster::new(options.clone()).await;
    cluster.create_bucket().await;
    for node in &cluster.nodes {
        cluster.start(node);
    }
    let peers = Arc::new(Peers::new(&Admin::new()).with_tls(Some(cluster.tls.clone())));
    let subscribed = Arc::new(MemoryEngine::new("subscribed"));
    let targets = FOLLOWED
        .iter()
        .map(|(dataset, namespace)| Target {
            selection: Selection {
                dataset: (*dataset).into(),
                namespace: (*namespace).into(),
            },
            engine: subscribed.clone(),
            prefix: followed_prefix(namespace),
            span: None,
        })
        .collect();
    let relays = cluster.addresses(Kind::Relay);
    let subscriber = Session::start(peers.clone(), relays.clone(), targets).unwrap();
    let (quicksilver, quicksilver_target) = View::new("quicksilver", CELL);
    let (cdn, cdn_target) = View::new("cdn", CELL);
    let observer = Arc::new(
        Session::start(peers.clone(), relays, vec![quicksilver_target, cdn_target]).unwrap(),
    );
    let run = Arc::new(Run {
        cluster,
        history: Mutex::default(),
        samples: Mutex::default(),
        failure: Mutex::default(),
        log: Mutex::default(),
        started: Instant::now(),
        writing: AtomicBool::new(true),
        writers: AtomicUsize::new(WRITERS),
        faults: AtomicBool::new(true),
        busy: Mutex::default(),
        subscribed,
        views: Views {
            quicksilver,
            cdn,
            observer,
            unavailable: Mutex::new(BTreeMap::from([
                ("quicksilver", "observer has not refreshed".into()),
                ("cdn", "observer has not refreshed".into()),
            ])),
        },
    });
    let refresher = {
        let (run, peers) = (run.clone(), peers.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(200)).await;
                run.views.refresh(&peers);
            }
        })
    };
    let outcome = drive(&run).await;
    refresher.abort();
    drop(subscriber);
    run.cluster.stop();
    let log = run.log.lock().unwrap().join("\n");
    fs::write(run.cluster.options.dir.join("faults.log"), log).unwrap();
    outcome
}

/// Proves the disk checks can fire on real RocksDB data: copies of one converged node's disk, each damaged in a known
/// way, must break the invariant named beside the damage. The copies are checked, never the node.
fn checks_fire(run: &Run) -> Result<(), Violation> {
    let node = run.cluster.of(Kind::Quicksilver).next().unwrap();
    let clean = run.cluster.options.dir.join("damage/clean");
    run.cluster.kill(node);
    let copied = copy_dir(&node.data, &clean);
    run.cluster.start(node);
    let unnoticed = |detail: String| Violation {
        invariant: "ChecksFire",
        detail,
    };
    copied.map_err(|error| unnoticed(format!("copy clean fixture: {error}")))?;
    let datasets = node.kind.datasets();
    let history = run.history.lock().unwrap();
    let (samples, _) = read_disk("clean", &clean, datasets);
    for sample in &samples {
        sample.check(&history)?;
    }
    let target = samples
        .iter()
        .find(|sample| !sample.held.is_empty())
        .ok_or_else(|| unnoticed("no copy with data to damage".into()))?;
    let (dataset, namespace) = &target.stream;
    let span = target.span.unwrap();
    let (key, _) = target.held.iter().next().unwrap();
    let stored = |key: &[u8]| l2_key(hash_of(dataset, namespace, key), namespace, key);
    let ghost = (0..)
        .map(|i| format!("ghost{i}").into_bytes())
        .find(|key| within(span, hash_of(dataset, namespace, key)))
        .unwrap();
    for (damage, invariant) in [
        ("drop", "GapFreeApply"),
        ("stale", "NoStaleOverwrite"),
        ("resurrect", "NoResurrection"),
        ("ahead", "NoLostAck"),
    ] {
        let copy = run.cluster.options.dir.join("damage").join(damage);
        copy_dir(&clean, &copy)
            .map_err(|error| unnoticed(format!("copy {damage} fixture: {error}")))?;
        {
            let engine =
                RocksDbAdapter::open(&copy.join(dataset), dataset.clone(), &Default::default())
                    .map_err(|error| unnoticed(format!("{damage}: {error}")))?;
            let mut batch = WriteBatch::default();
            match damage {
                "drop" => {
                    batch.delete(stored(key));
                }
                "stale" => {
                    batch.put(stored(key), b"stale".to_vec());
                }
                "resurrect" => {
                    batch.put(stored(&ghost), b"ghost".to_vec());
                }
                _ => {
                    for (name, revision) in engine.snapshot().unwrap().revisions().unwrap() {
                        if name.starts_with("relay:cursor:") {
                            batch.set_revision(name, revision + 1_000);
                        }
                    }
                }
            }
            engine.write(batch).unwrap();
        }
        let (damaged, _) = read_disk(damage, &copy, datasets);
        let fired = damaged
            .iter()
            .find_map(|sample| sample.check(&history).err());
        match fired {
            Some(violation) if violation.invariant == invariant => {
                run.record(format!("damage {damage} broke {invariant}"));
            }
            other => {
                return Err(unnoticed(format!(
                    "{damage} should break {invariant}, not {other:?}"
                )));
            }
        }
    }
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(to) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            copy_dir(&path, &to.join(entry.file_name()))?;
        } else {
            fs::copy(&path, to.join(entry.file_name()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/checks.rs"]
mod checks;

async fn drive(run: &Arc<Run>) -> Result<Outcome, Violation> {
    let deadline = Instant::now() + run.cluster.options.safety;
    tokio::spawn(supervisor(run.clone()));
    tokio::spawn(sampler(run.clone()));
    for id in 0..WRITERS {
        tokio::spawn(writer(run.clone(), id));
    }
    let drivers: Vec<_> = (0..DRIVERS)
        .filter(|_| !run.cluster.options.faults.is_empty())
        .map(|id| tokio::spawn(driver(run.clone(), id, deadline)))
        .collect();
    sleep(run.cluster.options.safety).await;
    for driver in drivers {
        let _ = driver.await;
    }
    liveness(run).await?;
    verify(run).await?;
    checks_fire(run)?;
    let kills = run
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains(" kill "))
        .count();
    let history = run.history.lock().unwrap();
    let acknowledged = history
        .streams()
        .map(|stream| history.head(stream) as usize)
        .sum();
    Ok(Outcome {
        acknowledged,
        kills,
    })
}
