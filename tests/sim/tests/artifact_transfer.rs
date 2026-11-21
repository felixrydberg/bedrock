//! #79 / B-31 acceptance: artifact transfers that saturate a peer's bulk connection do not push control traffic, which
//! relay sessions share, past 500 ms (the B-03 bound; user decision 2026-11-21). Runs over real HTTP/2 in normal builds
//! and simulated under `--cfg madsim`.

use std::sync::Arc;
use std::time::Duration;

use bedrock_cdn::artifact_storage::{Artifacts, Hash, Memory};
use bedrock_cdn::artifact_transfer::pb::FetchRequest;
use bedrock_cdn::artifact_transfer::pb::artifact_transfer_client::ArtifactTransferClient;
use bedrock_cdn::artifact_transfer::{ArtifactTransferServer, Transfer};
use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection;
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::transport::{Class, Peers};
use bytes::{Bytes, BytesMut};
use tokio::time::sleep;
use tonic::{Request, Response, Status};

mod pb {
    tonic::include_proto!("sim");
}
use pb::Msg;
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};

const TIMEOUT: Duration = Duration::from_millis(500);
/// Transfers opened and never read. In normal builds, 4 × hyper's 2 MiB stream window exceeds its 5 MiB connection
/// window, so the bulk connection stalls completely.
const TRANSFERS: usize = 4;

/// Eight MiB, more than HTTP/2 lets through unread.
fn artifact() -> Bytes {
    (0..8 << 20).map(|i: usize| (i * 7 % 251) as u8).collect()
}

/// The control service sharing the node.
struct Ping;

#[tonic::async_trait]
impl Echo for Ping {
    async fn send(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    type WatchStream = tokio_stream::Empty<Result<Msg, Status>>;

    async fn watch(&self, _: Request<Msg>) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

/// A CDN node whose S3 holds the artifact, serving transfers and pings.
fn router() -> tonic::transport::server::Router {
    let admin = Admin::new();
    let artifacts = Artifacts::new(
        Arc::new(Memory::with(&[&artifact()])),
        Arc::new(Memory::default()),
        StorageHealth::new(&admin),
    );
    let transfer = Transfer::new(
        Arc::new(artifacts),
        Arc::new(Peers::new(&admin)),
        &protection::Metrics::new(&admin),
    );
    tonic::transport::Server::builder()
        .add_service(ArtifactTransferServer::new(transfer))
        .add_service(EchoServer::new(Ping))
}

fn ping(id: u32) -> Msg {
    Msg {
        id,
        body: "ping".into(),
    }
}

/// The receiving side; `server` is the other node's `host:port`.
async fn transfer(server: &str) {
    let admin = Admin::new();
    let peers = Peers::new(&admin);
    let echo = |class| EchoClient::new(peers.channel(server, class).unwrap());
    let hash = Hash::of(&artifact());

    let mut transfers = Vec::new();
    for _ in 0..TRANSFERS {
        let mut client = ArtifactTransferClient::new(peers.channel(server, Class::Bulk).unwrap());
        let request = FetchRequest {
            hash: Bytes::copy_from_slice(&hash.0),
        };
        let call = client.fetch(request);
        let response = peers.call(server, Class::Bulk, TIMEOUT, call).await;
        transfers.push(response.unwrap().into_inner());
    }
    sleep(Duration::from_millis(200)).await;

    for id in 0..10 {
        peers
            .call(
                server,
                Class::Control,
                TIMEOUT,
                echo(Class::Control).send(ping(id)),
            )
            .await
            .unwrap_or_else(|s| panic!("control message {id} delayed by artifact transfers: {s}"));
    }

    // Shows the test has teeth: the same message on the saturated bulk connection misses its deadline. Simulated
    // tonic has no flow control, so only real HTTP/2 stalls.
    #[cfg(not(madsim))]
    {
        let err = peers
            .call(
                server,
                Class::Bulk,
                TIMEOUT,
                echo(Class::Bulk).send(ping(0)),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }

    // The transfers were real: one read to the end carries the whole artifact.
    let mut stream = transfers.remove(0);
    drop(transfers);
    let mut body = BytesMut::new();
    while let Some(chunk) = peers
        .call(server, Class::Bulk, TIMEOUT, stream.message())
        .await
        .unwrap()
    {
        body.extend_from_slice(&chunk.data);
    }
    assert_eq!(Hash::of(&body), hash);
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn transfers_do_not_delay_control_traffic() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        router().serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    transfer(&addr.to_string()).await;
}

#[cfg(madsim)]
#[madsim::test]
async fn transfers_do_not_delay_control_traffic() {
    let handle = madsim::runtime::Handle::current();
    let server: std::net::SocketAddr = "10.0.0.2:7000".parse().unwrap();
    handle
        .create_node()
        .name("server")
        .ip(server.ip())
        .build()
        .spawn(async move { router().serve(server).await.unwrap() });
    handle
        .create_node()
        .name("client")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            // Let the server bind first.
            sleep(Duration::from_millis(10)).await;
            transfer(&server.to_string()).await;
        })
        .await
        .unwrap();
}
