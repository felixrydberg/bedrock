//! The cells below an upper-tier Gateway (#89 / B-57, `docs/spec/cells.tla`). Each relay lists its live child relay
//! sessions in `Relay.Cells`: a DC relay lists its cell relays, a region relay its DC relays. A DC Gateway asks its DC
//! relays; a region Gateway asks its region relays, then the DC relays they list. The answers are unioned, since each
//! child is connected to only one of its parent's relays.
//!
//! A listing can be stale or wrong, and that can only cost availability: the cell's own relay answers `Routes` with its
//! own cell's Gateways, only members of the target cell are tried, and a cell Gateway refuses other cells' requests.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::replication::pb;
use tokio::time::Instant;
use tonic::Status;
use tonic::transport::Channel;

/// The shortest time between two walks, so requests for an unknown cell cannot make the Gateway walk on every one.
pub const MIN_REFRESH: Duration = Duration::from_secs(1);
/// Newer gateway tables or listings fetched by one forward after every candidate failed.
const REFRESHES: usize = 2;

/// Cell → its relays' addresses.
pub type Listing = BTreeMap<String, BTreeSet<String>>;

pub struct Cells {
    client: Arc<Client>,
    /// This Gateway's own tier's relays.
    relays: Vec<String>,
    tier: u32,
    known: Mutex<Known>,
    /// Held for a walk, so concurrent refreshes share one.
    walking: tokio::sync::Mutex<()>,
}

/// The last walk's outcome. A failed walk keeps an earlier listing; with none, its error is reused until
/// [`MIN_REFRESH`] passes.
#[derive(Default)]
struct Known {
    listing: Arc<Listing>,
    error: Option<Status>,
    at: Option<Instant>,
}

impl Cells {
    pub fn new(client: Arc<Client>, relays: Vec<String>, tier: u32) -> Self {
        Self {
            client,
            relays,
            tier,
            known: Mutex::default(),
            walking: tokio::sync::Mutex::new(()),
        }
    }

    pub fn tier(&self) -> u32 {
        self.tier
    }

    /// The children every one of `parents` that answered lists. Fails only if none answered.
    async fn children(&self, parents: &BTreeSet<String>) -> Result<Vec<pb::Child>, Status> {
        let mut last = Status::unavailable("no relays to ask for cells");
        let mut answered = false;
        let mut children = Vec::new();
        let parents: Vec<String> = parents.iter().cloned().collect();
        for parent in self.client.route(&parents) {
            let reply = self
                .client
                .call_relay(parent, |channel| async move {
                    pb::relay_client::RelayClient::new(channel)
                        .cells(pb::CellsRequest {})
                        .await
                })
                .await;
            match reply {
                Ok(reply) => {
                    answered = true;
                    children.extend(reply.children);
                }
                Err(status) => last = status,
            }
        }
        if answered { Ok(children) } else { Err(last) }
    }

    /// Walks down from this Gateway's relays, keeping the cell relays found at every level: a cell relay may hang
    /// directly under a region relay.
    async fn walk(&self) -> Result<Listing, Status> {
        let mut parents: BTreeSet<String> = self.relays.iter().cloned().collect();
        let mut listing = Listing::new();
        // Live edges strictly increase tier, so a walk from tier t ends within t - 1 levels.
        for _ in 1..self.tier {
            let mut below = BTreeSet::new();
            let children = match self.children(&parents).await {
                Ok(children) => children,
                Err(status) if listing.is_empty() => return Err(status),
                // A deeper level that fails keeps the cells already found.
                Err(_) => break,
            };
            for child in children {
                if child.tier == 1 {
                    listing.entry(child.cell).or_default().insert(child.address);
                } else {
                    below.insert(child.address);
                }
            }
            if below.is_empty() {
                break;
            }
            parents = below;
        }
        Ok(listing)
    }

    /// The current listing, walked again when `refresh` asks and the last walk is at least [`MIN_REFRESH`] old.
    pub async fn listing(&self, refresh: bool) -> Result<Arc<Listing>, Status> {
        if let Some(known) = self.known(refresh) {
            return known;
        }
        let _walking = self.walking.lock().await;
        // A caller that waited reuses the walk it waited for.
        if let Some(known) = self.known(refresh) {
            return known;
        }
        let now = bedrock_lib::monotonic_now();
        let walked = self.walk().await;
        let mut known = self.known.lock().unwrap();
        let listed = known.at.is_some() && known.error.is_none();
        known.at = Some(now);
        match walked {
            Ok(listing) => {
                known.listing = Arc::new(listing);
                known.error = None;
            }
            Err(status) if !listed => known.error = Some(status),
            Err(_) => {}
        }
        match &known.error {
            Some(status) => Err(status.clone()),
            None => Ok(known.listing.clone()),
        }
    }

    /// The last walk's outcome, unless `refresh` asks for a new one and it is at least [`MIN_REFRESH`] old.
    fn known(&self, refresh: bool) -> Option<Result<Arc<Listing>, Status>> {
        let known = self.known.lock().unwrap();
        let at = known.at?;
        if refresh && bedrock_lib::monotonic_now().duration_since(at) >= MIN_REFRESH {
            return None;
        }
        Some(match &known.error {
            Some(status) => Err(status.clone()),
            None => Ok(known.listing.clone()),
        })
    }

    /// Calls one of `cell`'s Gateways. A cell missing from the listing, or whose Gateways all fail, walks again (at most
    /// once per [`MIN_REFRESH`]) and fetches a newer gateway table, a bounded number of times.
    pub async fn forward<T, F, Fut>(&self, cell: &str, call: F) -> Result<T, Status>
    where
        F: Fn(Channel) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let mut last = Status::unavailable(format!("no relays listed for cell {cell}"));
        let mut stale = None;
        for attempt in 0..=REFRESHES {
            let listing = self.listing(attempt > 0).await?;
            let Some(relays) = listing.get(cell) else {
                continue;
            };
            let relays: Vec<String> = relays.iter().cloned().collect();
            let table = match self.client.routes_at(cell, &relays, GATEWAY, stale).await {
                Ok(table) => table,
                Err(status) => {
                    last = status;
                    continue;
                }
            };
            stale = Some(table.revision);
            last = Status::unavailable(format!("no gateway of cell {cell} answered"));
            // `routes_at` refuses a table naming another cell's members, so these are all `cell`'s Gateways.
            let addresses: Vec<String> =
                table.members.values().map(|m| m.address.clone()).collect();
            for address in self.client.route(&addresses) {
                let Some(member) = table.members.values().find(|m| m.address == *address) else {
                    continue;
                };
                match self.client.call_member(member, &call).await {
                    Ok(reply) => return Ok(reply),
                    Err(status) => last = status,
                }
            }
        }
        Err(last)
    }
}
