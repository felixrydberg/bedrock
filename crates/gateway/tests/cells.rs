//! An upper-tier Gateway's listing of cells (#89 / B-57): concurrent refreshes share one walk, a failed walk is reused
//! until `MIN_REFRESH` passes, and a deeper level that fails keeps the cells already found.
#![cfg(not(madsim))]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bedrock_gateway::cells::{Cells, MIN_REFRESH};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::replication::pb;
use bedrock_lib::transport::Peers;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

/// A relay answering `Cells` with `children`, or failing when there are none, counting the calls.
#[derive(Clone, Default)]
struct Relay {
    children: Vec<pb::Child>,
    calls: Arc<AtomicUsize>,
}

type Frames = Pin<Box<dyn Stream<Item = Result<pb::RelayFrame, Status>> + Send>>;

#[tonic::async_trait]
impl pb::relay_server::Relay for Relay {
    type SessionStream = Frames;
    async fn session(
        &self,
        _: Request<Streaming<pb::RelayRequest>>,
    ) -> Result<Response<Frames>, Status> {
        Err(Status::unimplemented("cells only"))
    }
    async fn routes(
        &self,
        _: Request<pb::RoutesRequest>,
    ) -> Result<Response<pb::RoutesReply>, Status> {
        Err(Status::unimplemented("cells only"))
    }
    async fn cells(
        &self,
        _: Request<pb::CellsRequest>,
    ) -> Result<Response<pb::CellsReply>, Status> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.children.is_empty() {
            return Err(Status::unavailable("cut off"));
        }
        Ok(Response::new(pb::CellsReply {
            children: self.children.clone(),
        }))
    }
}

async fn cells(relay: &Relay, tier: u32) -> Cells {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(relay.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let admin = Admin::new();
    let client = Arc::new(Client::new(
        &admin,
        Arc::new(Peers::new(&admin)),
        Vec::new(),
        String::new(),
    ));
    Cells::new(client, vec![address], tier)
}

#[tokio::test]
async fn concurrent_refreshes_share_one_walk_and_a_failure_is_reused_until_min_refresh() {
    let relay = Relay::default();
    let cells = cells(&relay, 2).await;
    let (a, b, c, d) = tokio::join!(
        cells.listing(true),
        cells.listing(true),
        cells.listing(false),
        cells.listing(true)
    );
    assert!(a.is_err() && b.is_err() && c.is_err() && d.is_err());
    assert_eq!(relay.calls.load(Ordering::Relaxed), 1, "one walk for all");
    assert!(cells.listing(true).await.is_err());
    assert_eq!(relay.calls.load(Ordering::Relaxed), 1, "failure reused");
    tokio::time::sleep(MIN_REFRESH).await;
    assert!(cells.listing(true).await.is_err());
    assert_eq!(relay.calls.load(Ordering::Relaxed), 2, "walked again");
}

#[tokio::test]
async fn a_deeper_level_that_fails_keeps_the_cells_found() {
    let relay = Relay {
        children: vec![
            pb::Child {
                tier: 1,
                cell: "c1".into(),
                address: "10.0.0.1:1".into(),
                node_id: "r1".into(),
            },
            // A DC relay that never answers.
            pb::Child {
                tier: 2,
                cell: String::new(),
                address: "127.0.0.1:1".into(),
                node_id: "d1".into(),
            },
        ],
        ..Default::default()
    };
    let listing = cells(&relay, 3).await.listing(false).await.unwrap();
    assert_eq!(
        listing.keys().collect::<Vec<_>>(),
        ["c1"],
        "the region relay's own cell relay"
    );
}
