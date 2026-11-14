//! Actor IDs and placement (#109 / S-01, `docs/implementation-prototype/statefull/v2.md` §4).
//!
//! Gateway routing, Compute's actor host and the volume nodes must agree on where an actor lives without a directory
//! lookup, so everything here is a pure function of the ID and Quicksilver's topology record, and every node computes the
//! same answer from the same inputs.
//! - [`ActorId`] is `{region}_{base32(128 bits)}`. A region the topology does not list is rejected.
//! - [`Topology`] (`topology/region/{code}`) and [`ActorNamespace`] (`actor_ns/{id}`) are records in Quicksilver's
//!   `infrastructure` namespace.
//! - [`dc_of`], [`cells_of`], [`slots`] and [`wake_host`] place an actor by rendezvous hashing with the stable hash of
//!   [`crate::placement`].

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::client::Client;
use crate::replication::pb;

/// Where the records live.
pub const SERVICE: &str = crate::datasets::capabilities::QUICKSILVER;
pub const DATASET: &str = crate::datasets::capabilities::QUICKSILVER;
pub const NAMESPACE: &str = "infrastructure";
const TOPOLOGY_PREFIX: &str = "topology/region/";
const ACTOR_NS_PREFIX: &str = "actor_ns/";
/// Records this code writes; any other version is refused rather than misread.
pub const FORMAT_VERSION: u32 = 1;
/// Replica slots per actor (§4).
pub const SLOTS: usize = 3;
const MAX_REGION: usize = 32;
/// 128 bits in base32.
const ENCODED: usize = 26;
/// RFC 4648 lowercase.
const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Not `{region}_{base32(128 bits)}`, or a record that fails validation.
    Malformed(String),
    /// The region has no topology record.
    UnknownRegion(String),
    /// A read failed with this status. The code says why: `Unavailable` and `DeadlineExceeded` are retryable, `DataLoss`
    /// is corruption, and `Unimplemented`, `InvalidArgument` and `FailedPrecondition` are unsupported or refused.
    Read(tonic::Code, String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(detail) => write!(f, "malformed: {detail}"),
            Self::UnknownRegion(region) => write!(f, "unknown region {region:?}"),
            Self::Read(code, detail) => write!(f, "read failed ({code}): {detail}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self::Read(status.code(), status.message().to_owned())
    }
}

fn malformed<T>(detail: impl Into<String>) -> Result<T, Error> {
    Err(Error::Malformed(detail.into()))
}

/// The kinds of actor resource. A D1 database and a queue share the actor ID format (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum Kind {
    Do = 0,
    D1 = 1,
    Queue = 2,
    Cron = 3,
    Workflow = 4,
    Kv = 5,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Do => "do",
            Self::D1 => "d1",
            Self::Queue => "queue",
            Self::Cron => "cron",
            Self::Workflow => "workflow",
            Self::Kv => "kv",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [
            Self::Do,
            Self::D1,
            Self::Queue,
            Self::Cron,
            Self::Workflow,
            Self::Kv,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == name)
    }
}

/// An actor ID: a region code and 128 bits. Storage and routing keys carry it with the actor namespace's ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorId {
    region: String,
    bits: u128,
}

/// The topology records by region code; [`ActorId`]s are valid only for a region listed here.
pub type Regions = BTreeMap<String, Topology>;

impl ActorId {
    fn new(regions: &Regions, region: &str, bits: u128) -> Result<Self, Error> {
        if !regions.contains_key(region) {
            return Err(Error::UnknownRegion(region.to_owned()));
        }
        Ok(Self {
            region: region.to_owned(),
            bits,
        })
    }

    /// Parses `{region}_{base32(128 bits)}`. The encoding is canonical, so every ID has one spelling.
    pub fn parse(regions: &Regions, text: &str) -> Result<Self, Error> {
        let (region, bits) = decode(text)?;
        Self::new(regions, region, bits)
    }

    /// The rendezvous and range hash of the ID `text`, checking its format but not its region. `bedrock-volume` keys and
    /// routes by it without reading the topology; the holder has already checked the region.
    pub fn hash_of(text: &str) -> Result<u64, Error> {
        Ok((decode(text)?.1 >> 64) as u64)
    }

    /// `idFromName`: the first 128 bits of SHA-256(`namespace_id` ‖ `name`) in `region`, the binding's region. IDs of
    /// one namespace are fixed length, so the concatenation is unambiguous.
    pub fn from_name(
        regions: &Regions,
        region: &str,
        namespace_id: &str,
        name: &str,
    ) -> Result<Self, Error> {
        let digest = Sha256::new()
            .chain_update(namespace_id)
            .chain_update(name)
            .finalize();
        Self::new(
            regions,
            region,
            u128::from_be_bytes(digest[..16].try_into().unwrap()),
        )
    }

    /// `newUniqueId`: random bits from [`crate::uuid`], seeded under madsim.
    pub fn unique(regions: &Regions, region: &str) -> Result<Self, Error> {
        let bits = u128::from_str_radix(&crate::uuid(), 16).expect("uuid() is 32 hex digits");
        Self::new(regions, region, bits)
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// The ID's position in the 64-bit rendezvous space. Its bits are uniform (a hash or random), so the top 64 do.
    fn hash(&self) -> u64 {
        (self.bits >> 64) as u64
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_", self.region)?;
        // 26 groups of 5 bits from the top; the last group has two zero padding bits.
        for i in 0..ENCODED {
            let shift = 128 - 5 * (i as i32 + 1);
            let group = if shift >= 0 {
                (self.bits >> shift) & 31
            } else {
                (self.bits << -shift) & 31
            };
            write!(f, "{}", ALPHABET[group as usize] as char)?;
        }
        Ok(())
    }
}

/// Decodes `{region}_{base32(128 bits)}`. The encoding is canonical, so every ID has one spelling.
fn decode(text: &str) -> Result<(&str, u128), Error> {
    let Some((region, encoded)) = text.rsplit_once('_') else {
        return malformed("missing '_' between region and ID");
    };
    if encoded.len() != ENCODED {
        return malformed(format!("ID must be {ENCODED} base32 characters"));
    }
    let mut bits = 0u128;
    for (i, byte) in encoded.bytes().enumerate() {
        let Some(value) = ALPHABET.iter().position(|a| *a == byte) else {
            return malformed("ID has a character outside a-z and 2-7");
        };
        let value = value as u128;
        if i + 1 < ENCODED {
            bits = (bits << 5) | value;
        } else if value & 0b11 != 0 {
            // 26 characters hold 130 bits; the last two are padding.
            return malformed("ID has nonzero padding bits");
        } else {
            bits = (bits << 3) | (value >> 2);
        }
    }
    if !valid_region(region) {
        return malformed("region must be 1 to 32 characters of a-z, 0-9 and '-'");
    }
    Ok((region, bits))
}

fn valid_region(region: &str) -> bool {
    (1..=MAX_REGION).contains(&region.len())
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// A DC of a region's actor topology and its cells.
#[derive(Clone, PartialEq, Message)]
pub struct Dc {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, repeated, tag = "2")]
    pub cells: Vec<String>,
}

/// `topology/region/{code}`: the region's actor DCs and each DC's cells. Static Quicksilver config, versioned.
#[derive(Clone, PartialEq, Message)]
pub struct Topology {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(string, tag = "2")]
    pub region: String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(message, repeated, tag = "4")]
    pub dcs: Vec<Dc>,
}

/// The key of region `code`'s topology record.
pub fn topology_key(code: &str) -> Vec<u8> {
    [TOPOLOGY_PREFIX.as_bytes(), code.as_bytes()].concat()
}

impl Topology {
    /// A stored record, validated against its key: unknown versions, a mismatched region, and empty or repeated DCs and
    /// cells are corruption, never a topology.
    pub fn decode_record(code: &str, bytes: &[u8]) -> Result<Self, Error> {
        let record = Self::decode(bytes).map_err(|e| Error::Malformed(e.to_string()))?;
        if record.format_version != FORMAT_VERSION {
            return malformed("unknown format version");
        }
        if record.region != code || !valid_region(code) {
            return malformed("record region does not match its key");
        }
        let mut dcs = BTreeSet::new();
        let mut cells = BTreeSet::new();
        for dc in &record.dcs {
            if dc.name.is_empty() || !dcs.insert(&dc.name) || dc.cells.is_empty() {
                return malformed("a DC needs a unique name and at least one cell");
            }
            if dc.cells.iter().any(|c| c.is_empty() || !cells.insert(c)) {
                return malformed("a cell needs a unique non-empty name");
            }
        }
        if dcs.is_empty() {
            return malformed("a region needs at least one DC");
        }
        Ok(record)
    }

    /// Region `code`'s topology and the revision that proves it, from a holder at least at `min_revision`.
    pub async fn read(
        client: &Client,
        code: &str,
        min_revision: u64,
    ) -> Result<(Self, u64), Error> {
        let found = client
            .get(
                SERVICE,
                DATASET,
                NAMESPACE,
                &topology_key(code),
                min_revision,
            )
            .await?;
        match found.value {
            Some(bytes) => Ok((Self::decode_record(code, &bytes)?, found.revision)),
            None => Err(Error::UnknownRegion(code.to_owned())),
        }
    }
}

/// `actor_ns/{id}`: the Worker class an actor namespace runs and its kind.
#[derive(Clone, PartialEq, Message)]
pub struct ActorNamespace {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(string, tag = "2")]
    pub worker: String,
    #[prost(string, tag = "3")]
    pub class: String,
    #[prost(enumeration = "Kind", tag = "4")]
    pub kind: i32,
}

/// The key of actor namespace `id`'s record.
pub fn actor_ns_key(id: &str) -> Vec<u8> {
    [ACTOR_NS_PREFIX.as_bytes(), id.as_bytes()].concat()
}

impl ActorNamespace {
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let record = Self::decode(bytes).map_err(|e| Error::Malformed(e.to_string()))?;
        if record.format_version != FORMAT_VERSION {
            return malformed("unknown format version");
        }
        if Kind::try_from(record.kind).is_err() {
            return malformed("unknown kind");
        }
        Ok(record)
    }

    /// Actor namespace `id`'s record and the revision that proves it; `None` when it does not exist.
    pub async fn read(
        client: &Client,
        id: &str,
        min_revision: u64,
    ) -> Result<(Option<Self>, u64), Error> {
        let found = client
            .get(SERVICE, DATASET, NAMESPACE, &actor_ns_key(id), min_revision)
            .await?;
        let record = found
            .value
            .map(|bytes| Self::decode_record(&bytes))
            .transpose()?;
        Ok((record, found.revision))
    }
}

/// The ID's region: its prefix. Never changes.
pub fn region_of(id: &ActorId) -> &str {
    id.region()
}

/// The topology of the ID's region; the caller must pass the record of [`region_of`]. Its fields are public, so one built
/// without [`Topology::decode_record`] may have no DC or a DC without cells, which would leave nothing to rank.
fn check<'a>(id: &ActorId, topology: &'a Topology) -> Result<&'a Topology, Error> {
    if topology.region != id.region {
        return Err(Error::UnknownRegion(id.region.clone()));
    }
    if topology.dcs.is_empty() || topology.dcs.iter().any(|dc| dc.cells.is_empty()) {
        return malformed("a region needs at least one DC, and every DC at least one cell");
    }
    Ok(topology)
}

/// Candidates by rendezvous hash of `id` with each name, most preferred first. Adding or removing a candidate moves only
/// the IDs it ranks first (or ranked first).
fn rank<'a, T>(id: &ActorId, items: &'a [T], name: impl Fn(&T) -> &str) -> Vec<&'a T> {
    let mut scored: Vec<_> = items
        .iter()
        .map(|item| (crate::placement::score(id.hash(), name(item)), item))
        .collect();
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| name(a.1).cmp(name(b.1))));
    scored.into_iter().map(|(_, item)| item).collect()
}

/// The ID's DC: the rendezvous top of the region's DCs. Health does not matter: moving an actor to another DC would
/// need a fence across DCs, and a DC outage is accepted downtime (§4).
pub fn dc_of<'a>(id: &ActorId, topology: &'a Topology) -> Result<&'a Dc, Error> {
    let topology = check(id, topology)?;
    Ok(rank(id, &topology.dcs, |dc| &dc.name)[0])
}

/// The DC's cells, most preferred first.
pub fn cells_of<'a>(id: &ActorId, topology: &'a Topology) -> Result<Vec<&'a str>, Error> {
    let dc = dc_of(id, topology)?;
    Ok(rank(id, &dc.cells, |cell| cell)
        .into_iter()
        .map(String::as_str)
        .collect())
}

/// The three replica slots: the first three cells of [`cells_of`]. A DC with fewer cells wraps round, so one cell holds
/// slots 1 or 2 as well.
pub fn slots<'a>(id: &ActorId, topology: &'a Topology) -> Result<[&'a str; SLOTS], Error> {
    let cells = cells_of(id, topology)?;
    Ok(std::array::from_fn(|i| cells[i % cells.len()]))
}

/// Where to wake the actor: rendezvous over the healthy `compute` members of the first cell of [`cells_of`] that has
/// any, weighted by capacity (`cpus`; a member that reports none counts as 1). `members` are the region's membership
/// and `healthy` says which nodes peers can reach. This only chooses where to wake; it never moves an awake actor, so
/// it may differ between callers whose membership differs.
// ponytail: `f64::ln` is libm, not IEEE-exact, so two platforms could differ in the last bit and pick differently when
// two scores tie to that bit; harmless for a wake choice, use an integer log if it must be bit-exact.
pub fn wake_host<'a>(
    id: &ActorId,
    topology: &Topology,
    members: &'a [pb::Member],
    healthy: impl Fn(&str) -> bool,
) -> Result<Option<&'a pb::Member>, Error> {
    for cell in cells_of(id, topology)? {
        let candidates: Vec<&pb::Member> = members
            .iter()
            .filter(|m| {
                m.cell == cell
                    && m.roles.iter().any(|r| r == "compute")
                    && m.status == pb::member::Status::Ready as i32
                    && healthy(&m.node_id)
            })
            .collect();
        // Smallest -ln(u) / weight wins: a member's chance is proportional to its weight.
        let host = candidates.into_iter().min_by(|a, b| {
            let key = |m: &pb::Member| {
                let bits = crate::placement::score(id.hash(), &m.node_id);
                // 53 bits plus a half step, so u lies strictly inside (0, 1).
                let u = ((bits >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
                let weight = if m.cpus > 0.0 { m.cpus } else { 1.0 };
                -u.ln() / weight
            };
            key(a)
                .total_cmp(&key(b))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        if host.is_some() {
            return Ok(host);
        }
    }
    Ok(None)
}

#[cfg(test)]
#[path = "../tests/unit/actors.rs"]
mod tests;
