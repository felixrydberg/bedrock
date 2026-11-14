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
