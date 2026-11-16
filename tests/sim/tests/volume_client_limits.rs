//! #112 / S-04: the volume client against three real `bedrock-volume` services over loopback HTTP/2, where tonic's 4 MiB
//! default decode limit applies (the simulated transport has none). Values that each fit an entry, but together exceed
//! the limit, still come back from `read` and `scan`.
#![cfg(not(madsim))]

use std::collections::BTreeMap;
use std::sync::Arc;

use bedrock_compute::actors::volume::{Host, MAX_ENTRY_BYTES, Slot, Writer};
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume::{self as pb, volume_server::VolumeServer};
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;

async fn slot(i: usize) -> Slot {
    let name = format!("volume-{i}");
    let cell = format!("cell-{i}");
    let store = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    let service = Service::new(&Admin::new(), store, &name, &cell, "dc1").unwrap();
    *service.directory.write().unwrap() = BTreeMap::from([(
        0,
        vec![Owner {
            node: name.clone(),
            state: State::Ready,
        }],
    )]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(VolumeServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Slot {
        cell: cell.clone(),
        address,
        identity: tls::Identity {
            service: "volume".into(),
            cell,
            node_id: name,
            host: "127.0.0.1".into(),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn values_that_together_exceed_the_decode_limit_are_read_in_parts() {
    let slots = [slot(0).await, slot(1).await, slot(2).await];
    let holder = pb::Holder {
        cell: "cell-0".into(),
        host: "compute-0".into(),
    };
    let host = Host::start(Arc::new(Peers::new(&Admin::new())), holder);
    let actor = pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    };
    let writer = Writer::open(host, actor, slots, 0).await.unwrap();
    // Each value fits an entry; three of them are well over the 4 MiB default.
    let value = |n: u8| vec![n; MAX_ENTRY_BYTES / 2];
    let keys: Vec<Vec<u8>> = (0..3u8).map(|n| vec![b'k', n]).collect();
    for (n, key) in keys.iter().enumerate() {
        let change = pb::Change {
            key: key.clone(),
            value: Some(value(n as u8)),
        };
        writer.commit(vec![change], None, 0).await.unwrap();
    }

    let values = writer.read(keys.clone()).await.unwrap();
    assert_eq!(values.len(), 3);
    for (n, read) in values.iter().enumerate() {
        assert_eq!(read.key, keys[n]);
        assert_eq!(read.value.as_deref(), Some(value(n as u8).as_slice()));
    }

    let mut scanned = Vec::new();
    let mut start = Vec::new();
    loop {
        let (page, more) = writer.scan(start.clone(), 10).await.unwrap();
        assert!(!page.is_empty());
        start = [page.last().unwrap().key.as_slice(), &[0]].concat();
        scanned.extend(page.into_iter().map(|v| v.key));
        if !more {
            break;
        }
    }
    assert_eq!(scanned, keys);
}
