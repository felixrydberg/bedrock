use std::collections::BTreeMap;

use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::volume::volume_server::Volume;
use tonic::Code;

use super::*;

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

fn service(state: State) -> Service {
    let store = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    let service = Service::new(&Admin::new(), store, "volume-1", "cell-1", "dc1").unwrap();
    let owner = Owner {
        node: "volume-1".into(),
        state,
    };
    *service.directory.write().unwrap() = BTreeMap::from([(0, vec![owner])]);
    service
}

/// A populating owner refuses reservations, claims and appends outright (`Voting` and `Takes` in `volume.tla`); a ready
/// or leaving owner serves them.
#[tokio::test]
async fn only_ready_or_leaving_owners_serve_slot_requests() {
    let holder = pb::Holder {
        cell: "cell-1".into(),
        host: "h".into(),
    };
    let reserve = || {
        Request::new(pb::ReserveRequest {
            actor: Some(actor()),
            epoch: 1,
            version: 0,
        })
    };
    let claim = || {
        Request::new(pb::ClaimRequest {
            actor: Some(actor()),
            holder: Some(holder.clone()),
            epoch: 1,
            version: 0,
        })
    };
    let append = || {
        Request::new(pb::AppendRequest {
            actor: Some(actor()),
            holder: Some(holder.clone()),
            epoch: 1,
            entries: vec![pb::Entry {
                seq: 1,
                epoch: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
    };
    let populating = service(State::Populating);
    for code in [
        populating.reserve(reserve()).await.unwrap_err().code(),
        populating.claim(claim()).await.unwrap_err().code(),
        populating.append(append()).await.unwrap_err().code(),
    ] {
        assert_eq!(code, Code::FailedPrecondition);
    }
    for state in [State::Ready, State::Leaving] {
        let owner = service(state);
        assert!(owner.reserve(reserve()).await.is_ok());
        assert!(owner.claim(claim()).await.is_ok());
        assert!(owner.append(append()).await.is_ok());
    }
}

/// Over mutual TLS the caller is checked: only the claimant's own Compute host reads its log, and only a volume node
/// copies a range's raw records.
#[tokio::test]
async fn the_log_is_read_only_by_the_promised_holder_and_repair_only_by_a_volume_node() {
    let owner = service(State::Ready);
    let holder = pb::Holder {
        cell: "cell-1".into(),
        host: "h".into(),
    };
    let caller = |service: &str, node: &str| Caller {
        service: service.into(),
        cell: "cell-1".into(),
        node_id: node.into(),
        hosts: Vec::new(),
    };
    fn from<T>(caller: Caller, mut request: Request<T>) -> Request<T> {
        request.extensions_mut().insert(caller);
        request
    }
    let claim = Request::new(pb::ClaimRequest {
        actor: Some(actor()),
        holder: Some(holder.clone()),
        epoch: 1,
        version: 0,
    });
    owner
        .claim(from(caller("compute", "h"), claim))
        .await
        .unwrap();
    let append = Request::new(pb::AppendRequest {
        actor: Some(actor()),
        holder: Some(holder.clone()),
        epoch: 1,
        entries: vec![pb::Entry {
            seq: 1,
            epoch: 1,
            ..Default::default()
        }],
        ..Default::default()
    });
    owner
        .append(from(caller("compute", "h"), append))
        .await
        .unwrap();
    let log = |holder: &pb::Holder| {
        Request::new(pb::LogRequest {
            actor: Some(actor()),
            holder: Some(holder.clone()),
            epoch: 1,
            from: 1,
            limit: 10,
        })
    };
    let reply = owner
        .log(from(caller("compute", "h"), log(&holder)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.entries.len(), 1);
    // Another host may not read as the holder, and a host the slot did not promise reads nothing.
    let denied = owner
        .log(from(caller("compute", "other"), log(&holder)))
        .await;
    assert_eq!(denied.unwrap_err().code(), Code::PermissionDenied);
    let other = pb::Holder {
        host: "other".into(),
        ..holder.clone()
    };
    let reply = owner
        .log(from(caller("compute", "other"), log(&other)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        reply.answer.unwrap().refused,
        pb::Refusal::NotPromised as i32
    );
    assert!(reply.entries.is_empty());
    // A range's raw records hold every actor in it, so a Compute host may not copy them.
    let repair = || {
        Request::new(pb::RepairRequest {
            start: 0,
            end: 0,
            after: Vec::new(),
            limit: 10,
        })
    };
    let denied = owner.repair(from(caller("compute", "h"), repair())).await;
    assert_eq!(denied.unwrap_err().code(), Code::PermissionDenied);
    assert!(
        owner
            .repair(from(caller("volume", "volume-2"), repair()))
            .await
            .is_ok()
    );
}
