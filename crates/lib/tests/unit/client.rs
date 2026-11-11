//! The shared client against fake relays and holders on loopback: stale tables, behind holders, bounded retries,
//! idle expiry and merged listing.
#![cfg(not(madsim))]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use super::*;
use crate::replication::pb;

const DATASET: &str = "quicksilver";
const NS: &str = "infrastructure";

#[derive(Clone)]
enum Mode {
    Serve,
    /// Serves one entry and reports more, as when the next would pass the reply size limit.
    Short,
    NotOwner,
    Behind,
}

/// A holder answering every read the same way, with `entries` as its data.
struct Holder {
    mode: Mode,
    revision: u64,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[tonic::async_trait]
impl pb::reads_server::Reads for Holder {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        self.check()?;
        Ok(Response::new(pb::GetReply {
            revision: self.revision,
            value: self.entries.get(&request.get_ref().key).cloned(),
        }))
    }
    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListReply>, Status> {
        self.check()?;
        let request = request.into_inner();
        let mut entries: Vec<pb::StateEntry> = self
            .entries
            .iter()
            .filter(|(key, _)| key.starts_with(&request.prefix) && **key > request.start_after)
            .map(|(key, value)| pb::StateEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let limit = match self.mode {
            Mode::Short => 1,
            _ => request.limit as usize,
        };
        let more = entries.len() > limit;
        entries.truncate(limit);
        Ok(Response::new(pb::ListReply {
            revision: self.revision,
            entries,
            more,
        }))
    }
}

impl Holder {
    fn check(&self) -> Result<(), Status> {
        match self.mode {
            Mode::Serve | Mode::Short => Ok(()),
            Mode::NotOwner => Err(Status::failed_precondition("not an owner")),
            Mode::Behind => Err(Status::aborted("behind")),
        }
    }
}

/// A relay serving whatever table is current, counting fetches.
#[derive(Clone, Default)]
struct Relay {
    table: Arc<Mutex<pb::RoutesReply>>,
    fetches: Arc<AtomicUsize>,
}

type Frames = Pin<Box<dyn Stream<Item = Result<pb::RelayFrame, Status>> + Send>>;

#[tonic::async_trait]
impl pb::relay_server::Relay for Relay {
    type SessionStream = Frames;
    async fn session(
        &self,
        _: Request<Streaming<pb::RelayRequest>>,
    ) -> Result<Response<Frames>, Status> {
        Err(Status::unimplemented("routes only"))
    }
    async fn routes(
        &self,
        _: Request<pb::RoutesRequest>,
    ) -> Result<Response<pb::RoutesReply>, Status> {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(self.table.lock().unwrap().clone()))
    }
}

async fn serve(router: tonic::transport::server::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(
        router.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    address
}

async fn holder(mode: Mode, revision: u64, entries: &[(&str, &str)]) -> String {
    let entries = entries
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    serve(
        tonic::transport::Server::builder().add_service(pb::reads_server::ReadsServer::new(
            Holder {
                mode,
                revision,
                entries,
            },
        )),
    )
    .await
}

/// A table at `revision` whose ranges (by start) are owned, ready, by the named holders at their addresses.
fn table(revision: u64, ranges: &[(u64, &[(&str, &str)])]) -> pb::RoutesReply {
    let mut entries = Vec::new();
    let mut members = BTreeMap::new();
    for (start, owners) in ranges {
        for (node, address) in *owners {
            members.insert(node.to_string(), address.to_string());
        }
        let range = pb::Range {
            owners: owners
                .iter()
                .map(|(node, _)| pb::range::Owner {
                    node_id: node.to_string(),
                    state: pb::range::owner::State::Ready as i32,
                })
                .collect(),
        };
        entries.push(pb::StateEntry {
            key: format!("range/{DATASET}/{start:016x}").into_bytes(),
            value: range.encode_to_vec(),
        });
    }
    for (node, address) in members {
        let member = pb::Member {
            node_id: node.clone(),
            service: "quicksilver".into(),
            cell: "c1".into(),
            address,
            datasets: vec![DATASET.into()],
            ..Default::default()
        };
        entries.push(pb::StateEntry {
            key: node.into_bytes(),
            value: member.encode_to_vec(),
        });
    }
    pb::RoutesReply { revision, entries }
}

async fn client(relay: &Relay) -> Client {
    let address = serve(
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(relay.clone())),
    )
    .await;
    let admin = Admin::new();
    Client::new(
        &admin,
        Arc::new(Peers::new(&admin)),
        vec![address],
        String::new(),
    )
}

#[tokio::test]
async fn a_stale_table_is_refetched_after_its_owner_turns_the_read_away() {
    let gone = holder(Mode::NotOwner, 0, &[]).await;
    let owner = holder(Mode::Serve, 7, &[("k", "v")]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() = table(1, &[(0, &[("old", &gone)])]);
    let client = client(&relay).await;
    assert!(client.routes("quicksilver", None).await.is_ok());
    *relay.table.lock().unwrap() = table(2, &[(0, &[("new", &owner)])]);
    let found = client
        .get("quicksilver", DATASET, NS, b"k", 0)
        .await
        .unwrap();
    assert_eq!(
        found,
        Found {
            value: Some(b"v".to_vec()),
            revision: 7
        }
    );
    assert_eq!(relay.fetches.load(Ordering::Relaxed), 2);
}

/// Cell relays apply the view separately; a caller that needs a newer table asks past one that lags.
#[tokio::test]
async fn a_stale_caller_skips_a_relay_that_lags() {
    let owner = holder(Mode::Serve, 7, &[("k", "v")]).await;
    let (lagging, current) = (Relay::default(), Relay::default());
    *lagging.table.lock().unwrap() = table(1, &[(0, &[("a", &owner)])]);
    *current.table.lock().unwrap() = table(2, &[(0, &[("a", &owner)])]);
    let mut relays = Vec::new();
    for relay in [&lagging, &current] {
        relays.push(
            serve(
                tonic::transport::Server::builder()
                    .add_service(pb::relay_server::RelayServer::new(relay.clone())),
            )
            .await,
        );
    }
    let admin = Admin::new();
    let client = Client::new(&admin, Arc::new(Peers::new(&admin)), relays, String::new());
    assert_eq!(
        client.routes("quicksilver", None).await.unwrap().revision,
        1
    );
    assert_eq!(
        client
            .routes("quicksilver", Some(1))
            .await
            .unwrap()
            .revision,
        2
    );
    // With no newer table anywhere, the newest one is used rather than failing.
    assert_eq!(
        client
            .routes("quicksilver", Some(2))
            .await
            .unwrap()
            .revision,
        2
    );
}

#[tokio::test]
async fn a_behind_holder_passes_the_read_to_the_next_owner() {
    let behind = holder(Mode::Behind, 0, &[]).await;
    let fresh = holder(Mode::Serve, 9, &[]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() = table(1, &[(0, &[("a", &behind), ("b", &fresh)])]);
    let client = client(&relay).await;
    let found = client
        .get("quicksilver", DATASET, NS, b"absent", 5)
        .await
        .unwrap();
    assert_eq!(
        found,
        Found {
            value: None,
            revision: 9
        }
    );
    assert_eq!(relay.fetches.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retries_are_bounded_when_no_holder_serves() {
    let gone = holder(Mode::NotOwner, 0, &[]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() = table(1, &[(0, &[("old", &gone)])]);
    let client = client(&relay).await;
    let error = client
        .get("quicksilver", DATASET, NS, b"k", 0)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(relay.fetches.load(Ordering::Relaxed), REFRESHES + 1);
}

#[tokio::test]
async fn an_idle_table_is_dropped_and_a_used_one_kept() {
    let owner = holder(Mode::Serve, 1, &[("k", "v")]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() = table(1, &[(0, &[("a", &owner)])]);
    let client = client(&relay).await;
    let read = || client.get("quicksilver", DATASET, NS, b"k", 0);
    let idle = |by| async move {
        tokio::time::pause();
        tokio::time::advance(by).await;
        tokio::time::resume();
    };
    read().await.unwrap();
    // Used again before the deadline: kept, and its idle time starts over.
    idle(IDLE_TTL / 2).await;
    read().await.unwrap();
    idle(IDLE_TTL / 2 + Duration::from_secs(1)).await;
    read().await.unwrap();
    assert_eq!(relay.fetches.load(Ordering::Relaxed), 1);
    idle(IDLE_TTL).await;
    read().await.unwrap();
    assert_eq!(relay.fetches.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn a_list_merges_ranges_in_key_order_and_pages() {
    let low = holder(Mode::Serve, 4, &[("p/b", "2"), ("p/d", "4"), ("q/x", "-")]).await;
    let high = holder(Mode::Serve, 3, &[("p/a", "1"), ("p/c", "3")]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() =
        table(1, &[(0, &[("low", &low)]), (1 << 63, &[("high", &high)])]);
    let client = client(&relay).await;
    let page = client
        .list("quicksilver", DATASET, NS, b"p/", b"", 3, 0)
        .await
        .unwrap();
    let keys: Vec<_> = page.entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, [b"p/a".as_slice(), b"p/b", b"p/c"]);
    assert!(page.more);
    assert_eq!(page.revision, 3);
    let rest = client
        .list("quicksilver", DATASET, NS, b"p/", b"p/c", 3, 0)
        .await
        .unwrap();
    assert_eq!(rest.entries, vec![(b"p/d".to_vec(), b"4".to_vec())]);
    assert!(!rest.more);
}

/// A range that stopped early, for its byte limit, has keys after its last one; none of another range's may pass them.
#[tokio::test]
async fn a_list_page_ends_where_a_range_stopped_early() {
    let short = holder(Mode::Short, 4, &[("p/b", "2"), ("p/d", "4")]).await;
    let full = holder(Mode::Serve, 3, &[("p/a", "1"), ("p/c", "3")]).await;
    let relay = Relay::default();
    *relay.table.lock().unwrap() = table(
        1,
        &[(0, &[("short", &short)]), (1 << 63, &[("full", &full)])],
    );
    let client = client(&relay).await;
    let page = client
        .list("quicksilver", DATASET, NS, b"p/", b"", 3, 0)
        .await
        .unwrap();
    let keys: Vec<_> = page.entries.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, [b"p/a".as_slice(), b"p/b"]);
    assert!(page.more);
}
