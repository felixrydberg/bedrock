//! B-03 acceptance: two nodes exchange requests and streams over `bedrock_lib::transport`, and saturated bulk streams do
//! not delay control messages past their timeout. Runs over real HTTP/2 in normal builds and simulated under `--cfg madsim`.

use std::time::Duration;

use bedrock_lib::transport::admin::Admin;
use bedrock_lib::transport::{Class, Peers};
use tokio::time::sleep;
use tonic::{Request, Response, Status};

mod pb {
    tonic::include_proto!("sim");
}
use pb::Msg;
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};

const TIMEOUT: Duration = Duration::from_millis(500);
/// Bulk streams opened and never read. In normal builds, 4 × hyper's 2 MiB stream window exceeds its 5 MiB connection
/// window, so the bulk connection stalls completely.
const BULK_STREAMS: u32 = 4;

struct Node;

#[tonic::async_trait]
impl Echo for Node {
    async fn send(&self, req: Request<Msg>) -> Result<Response<Msg>, Status> {
        Ok(Response::new(req.into_inner()))
    }

    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<Msg, Status>>;

    /// Three ticks, or with body `bulk` 8 MiB in 256 KiB chunks, held open until the reader goes away. 8 MiB per stream
    /// is more than HTTP/2 lets through unread; simulated streams have no flow control, so the producer must stop itself.
    async fn watch(&self, req: Request<Msg>) -> Result<Response<Self::WatchStream>, Status> {
        let req = req.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            if req.body == "bulk" {
                let chunk = "x".repeat(256 << 10);
                for id in 0..32 {
                    let msg = Msg {
                        id,
                        body: chunk.clone(),
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                }
                tx.closed().await;
            } else {
                for id in req.id..req.id + 3 {
                    let _ = tx
                        .send(Ok(Msg {
                            id,
                            body: "tick".into(),
                        }))
                        .await;
                }
            }
        });
        Ok(Response::new(rx.into()))
    }
}

fn msg(id: u32, body: &str) -> Msg {
    Msg {
        id,
        body: body.into(),
    }
}

/// The client side; `server` is the other node's `host:port`.
async fn exchange(server: &str) {
    let admin = Admin::new();
    let peers = Peers::new(&admin);
    let client = |class| EchoClient::new(peers.channel(server, class).unwrap());

    // Request/response.
    let reply = peers
        .call(
            server,
            Class::Control,
            TIMEOUT,
            client(Class::Control).send(msg(1, "ping")),
        )
        .await
        .unwrap();
    assert_eq!(reply.into_inner().id, 1);

    // Server stream.
    let mut stream = peers
        .call(
            server,
            Class::Control,
            TIMEOUT,
            client(Class::Control).watch(msg(100, "")),
        )
        .await
        .unwrap()
        .into_inner();
    let mut ids = Vec::new();
    while let Some(m) = peers
        .call(server, Class::Control, TIMEOUT, stream.message())
        .await
        .unwrap()
    {
        ids.push(m.id);
    }
    assert_eq!(ids, [100, 101, 102]);

    // Saturate the bulk class: open streams and never read them.
    let mut bulk = Vec::new();
    for _ in 0..BULK_STREAMS {
        let rsp = peers
            .call(
                server,
                Class::Bulk,
                TIMEOUT,
                client(Class::Bulk).watch(msg(0, "bulk")),
            )
            .await
            .unwrap();
        bulk.push(rsp.into_inner());
    }
    sleep(Duration::from_millis(200)).await;

    for id in 0..10 {
        peers
            .call(
                server,
                Class::Control,
                TIMEOUT,
                client(Class::Control).send(msg(id, "ping")),
            )
            .await
            .unwrap_or_else(|s| panic!("control message {id} delayed by bulk traffic: {s}"));
    }

    // Shows the test has teeth: the same message on the stalled bulk connection misses its deadline. Simulated tonic
    // opens a fresh stream per call with no flow control, so only real HTTP/2 stalls.
    #[cfg(not(madsim))]
    {
        let err = peers
            .call(
                server,
                Class::Bulk,
                TIMEOUT,
                client(Class::Bulk).send(msg(0, "ping")),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }
    drop(bulk);

    let metrics = admin.render();
    assert!(
        metrics.contains(r#"bedrock_transport_calls_total{class="Control",outcome="Ok"} 16"#),
        "{metrics}"
    );
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn exchange_and_bulk_isolation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(Node))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    exchange(&addr.to_string()).await;
}

#[cfg(madsim)]
#[madsim::test]
async fn exchange_and_bulk_isolation() {
    let handle = madsim::runtime::Handle::current();
    let server: std::net::SocketAddr = "10.0.0.2:7000".parse().unwrap();
    handle
        .create_node()
        .name("server")
        .ip(server.ip())
        .build()
        .spawn(async move {
            tonic::transport::Server::builder()
                .add_service(EchoServer::new(Node))
                .serve(server)
                .await
                .unwrap();
        });
    handle
        .create_node()
        .name("client")
        .ip("10.0.0.1".parse().unwrap())
        .build()
        .spawn(async move {
            // Let the server bind first.
            sleep(Duration::from_millis(10)).await;
            exchange(&server.to_string()).await;
        })
        .await
        .unwrap();
}
