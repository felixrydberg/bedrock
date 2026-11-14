//! The sibling handshake against a real cell control group (#15 / B-12): the Hub and the sibling stream live in
//! `bedrock-lib`, the control group in `bedrock-relay`.

use std::sync::Arc;

use bedrock_lib::admin::Admin;
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::siblings::Server;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::pb::siblings_server::Siblings as _;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::Peers;
use bedrock_relay::membership::control::{Config, Control};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::test]
async fn sibling_handshake_requires_the_same_cell_authority_before_sharing_members() {
    let peers = Arc::new(Peers::new(&Admin::new()));
    let (control, running) = Control::start(
        Arc::new(MemoryEngine::new("control")),
        peers.clone(),
        Config {
            region: "eu".into(),
            dc: "sto".into(),
            cell: "sto-1".into(),
            me: "a:7000".into(),
            voters: vec!["a:7000".into(), "b:7000".into(), "c:7000".into()],
            voter_ids: ["a", "b", "c"]
                .into_iter()
                .map(|id| (format!("{id}:7000"), id.into()))
                .collect(),
            clock: || 1000,
            operator_token: None,
            bootstrap: true,
        },
    )
    .unwrap();
    running.abort();
    let identity = control.identity();
    let cell = Hub::relay("relay".into(), 1, peers, Vec::new(), Vec::new(), None)
        .with_cell("sto-1".into())
        .with_control(control);
    let server = Server(cell);
    let mut mismatches = vec![None];
    for field in ["region", "dc", "cell", "voters", "voter_ids"] {
        let mut peer = identity.clone();
        match field {
            "region" => peer.region = "other".into(),
            "dc" => peer.dc = "other".into(),
            "cell" => peer.cell = "other".into(),
            "voters" => peer.voters[2] = "replacement:7000".into(),
            _ => {
                peer.voter_ids.insert("b:7000".into(), "other".into());
            }
        }
        mismatches.push(Some(peer));
    }
    for control in mismatches {
        let error = server
            .watch(Request::new(pb::SiblingWatch { control }))
            .await
            .expect_err("mismatched peer must be refused before snapshot");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    let mut stream = server
        .watch(Request::new(pb::SiblingWatch {
            control: Some(identity.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    let snapshot = stream.next().await.unwrap().unwrap();
    assert!(snapshot.synced);
    assert_eq!(snapshot.control, Some(identity.clone()));
    drop(stream);

    let upper = Server(Hub::relay(
        "upper".into(),
        2,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        None,
    ));
    let error = upper
        .watch(Request::new(pb::SiblingWatch {
            control: Some(identity),
        }))
        .await
        .expect_err("authority peer cannot join an upper-tier sibling stream");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    let mut stream = upper
        .watch(Request::new(pb::SiblingWatch { control: None }))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.next().await.unwrap().unwrap().control.is_none());
}
