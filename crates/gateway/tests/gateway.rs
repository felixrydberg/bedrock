//! The internal listener's admission rules (#89 / B-57): a cell Gateway serves only its own cell
//! (`docs/spec/cells.tla` `ServedInTarget`), only known services, and at most its own limit at once. Metric labels stay
//! within roles, services and outcomes.
#![cfg(not(madsim))]

use std::sync::Arc;

use bedrock_gateway::{Gateway, Role};
use bedrock_lib::admin::Admin;
use bedrock_lib::client::{Client, targeted};
use bedrock_lib::replication::pb;
use bedrock_lib::replication::pb::reads_server::Reads;
use bedrock_lib::transport::Peers;
use tonic::{Code, Request};

fn gateway(admin: &Arc<Admin>, limit: usize) -> Gateway {
    // No relays: nothing in these tests may reach a holder.
    let client = Arc::new(Client::new(
        admin,
        Arc::new(Peers::new(admin)),
        Vec::new(),
        String::new(),
    ));
    Gateway::new(admin, Role::Cell("c1".into()), client, limit)
}

fn get(service: &str, cell: &str) -> Request<pb::GetRequest> {
    targeted(pb::GetRequest::default(), service, cell).unwrap()
}

#[tokio::test]
async fn a_cell_gateway_refuses_other_cells_unknown_services_and_untargeted_reads() {
    let admin = Admin::new();
    let gateway = gateway(&admin, 4);
    let code = |result: Result<_, tonic::Status>| result.unwrap_err().code();
    assert_eq!(
        code(gateway.get(get("quicksilver", "c2")).await),
        Code::FailedPrecondition
    );
    assert_eq!(
        code(gateway.get(get("customer-chosen", "c1")).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(gateway.get(Request::new(pb::GetRequest::default())).await),
        Code::InvalidArgument
    );
    // Its own cell's read reaches the shared client, which has no relays here.
    assert_eq!(
        code(gateway.get(get("quicksilver", "c1")).await),
        Code::Unavailable
    );
    let metrics = admin.render();
    assert!(
        metrics.contains(r#"role="cell",service="invalid",outcome="rejected""#),
        "{metrics}"
    );
    assert!(!metrics.contains("customer-chosen"), "{metrics}");
}

#[tokio::test]
async fn the_internal_listener_refuses_reads_over_its_limit() {
    let admin = Admin::new();
    let gateway = gateway(&admin, 0);
    let refused = gateway.get(get("quicksilver", "c1")).await.unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted);
}
