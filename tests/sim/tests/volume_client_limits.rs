//! #112 / S-04: the volume client against three real `bedrock-volume` services over loopback HTTP/2, where tonic's 4 MiB
//! default decode limit applies (the simulated transport has none). Values that each fit an entry, but together exceed
//! the limit, still come back from `read` and `scan`.
#![cfg(not(madsim))]

use std::collections::BTreeMap;
use std::sync::Arc;

use bedrock_compute::actors::volume::{Host, Slot, Writer};
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume::{self as pb, volume_server::VolumeServer};
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;
use prost::Message;

async fn slot(i: usize, message_bytes: usize) -> Slot {
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
            .add_service(
                VolumeServer::new(service)
                    .max_decoding_message_size(bedrock_lib::kv::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(message_bytes),
            )
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

/// #126 / S-18: one maximum-size value and a nearly 100 MiB atomic bulk, including its retained event,
/// traverse real tonic rather than the simulation's unlimited message transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_kv_bulk_and_its_retained_event_fit_one_quorum_entry() {
    use bedrock_lib::kv;
    let slots = [
        slot(0, kv::MAX_MESSAGE_BYTES).await,
        slot(1, kv::MAX_MESSAGE_BYTES).await,
        slot(2, kv::MAX_MESSAGE_BYTES).await,
    ];
    let recovery_slots = slots.clone();
    let holder = pb::Holder {
        cell: "cell-0".into(),
        host: "compute-large".into(),
    };
    let host = Host::start(Arc::new(Peers::new(&Admin::new())), holder.clone());
    let actor = pb::Actor {
        namespace: "large".into(),
        id: format!("eu_{}a", "d".repeat(25)),
        kind: 5,
    };
    let writer = Writer::open(host, actor.clone(), slots, 0).await.unwrap();
    let mut changes = Vec::new();
    let mut original = kv::Write::default();
    for n in 0..4u8 {
        let key = vec![b'd', b'/', b'a' + n];
        let size = kv::MAX_VALUE - if n == 0 { 0 } else { 1024 };
        let value = vec![n + 1; size];
        original.changes.push(kv::Mutation {
            key: key.clone(),
            value: Some(kv::Stored {
                value: value.clone(),
                ..Default::default()
            }),
            ..Default::default()
        });
        changes.push(pb::Change {
            key,
            value: Some(value),
        });
    }
    changes.push(pb::Change {
        key: b"e/1".to_vec(),
        value: Some(original.encode_to_vec()),
    });
    drop(original);
    let entry = pb::Entry {
        changes,
        ..Default::default()
    };
    let entry_bytes = entry.encoded_len();
    assert!(entry_bytes < kv::MAX_ENTRY_BYTES);
    let started = bedrock_lib::monotonic_now();
    let changes = entry.changes;
    let sequence = writer.commit(changes.clone(), None, 1234).await.unwrap();
    let commit = started.elapsed();
    writer.commit(changes, None, 1235).await.unwrap();
    let mut caught_up = None;
    for slot in recovery_slots {
        let mut client =
            pb::volume_client::VolumeClient::connect(format!("http://{}", slot.address))
                .await
                .unwrap()
                .max_decoding_message_size(kv::MAX_MESSAGE_BYTES);
        let head = client
            .log(pb::LogRequest {
                actor: Some(actor.clone()),
                holder: Some(holder.clone()),
                epoch: writer.epoch(),
                from: 0,
                limit: 1,
            })
            .await
            .unwrap()
            .into_inner();
        if head
            .answer
            .and_then(|answer| answer.slot)
            .and_then(|slot| slot.end)
            .is_some_and(|end| end.seq > sequence)
        {
            caught_up = Some(client);
            break;
        }
    }
    let mut volume = caught_up.expect("an acknowledged quorum holds the entire second entry");
    let page = volume
        .log(pb::LogRequest {
            actor: Some(actor.clone()),
            holder: Some(holder.clone()),
            epoch: writer.epoch(),
            from: 0,
            limit: 64,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        page.more,
        "two complete maximum bulks cannot share a recovery page"
    );
    let recovery_page_bytes = page.encoded_len();
    assert!(recovery_page_bytes < kv::MAX_MESSAGE_BYTES);
    let next = page.entries.last().unwrap().seq + 1;
    drop(page);
    let final_page = volume
        .log(pb::LogRequest {
            actor: Some(actor),
            holder: Some(holder),
            epoch: writer.epoch(),
            from: next,
            limit: 64,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!final_page.more);
    assert_eq!(final_page.entries.last().unwrap().seq, sequence + 1);
    drop(final_page);
    let keys: Vec<_> = (0..4u8).map(|n| vec![b'd', b'/', b'a' + n]).collect();
    let read_started = bedrock_lib::monotonic_now();
    let read = writer.read(keys.clone()).await.unwrap();
    let read_duration = read_started.elapsed();
    assert_eq!(read.len(), 4);
    for (n, value) in read.iter().enumerate() {
        assert_eq!(value.key, keys[n]);
        let bytes = value.value.as_ref().unwrap();
        assert_eq!(bytes.len(), kv::MAX_VALUE - if n == 0 { 0 } else { 1024 });
        assert!(bytes.iter().all(|byte| *byte == n as u8 + 1));
    }
    eprintln!(
        "S-18 real HTTP/2: entry_bytes={entry_bytes}, recovery_page_bytes={recovery_page_bytes}, quorum_sequence={sequence}, commit_ms={}, read_ms={}",
        commit.as_millis(),
        read_duration.as_millis()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn values_that_together_exceed_the_decode_limit_are_read_in_parts() {
    let slots = [
        slot(0, 4 << 20).await,
        slot(1, 4 << 20).await,
        slot(2, 4 << 20).await,
    ];
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
    let value = |n: u8| vec![n; 2 << 20];
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
