//! Bindings, checked outside the sandbox (`statefull/v2.md` §8). A Worker's call to an actor leaves this host for one of
//! its own cell's Gateways on the internal listener, which routes it to the holder (#116 / S-08, §6 step 1).

use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::volume as pb;
use tonic::Status;

/// Sends `payload` to `actor` through this cell's Gateway and returns the actor's answer. A failed call is not sent again:
/// the event may have run. It fetches a newer Gateway table for the caller's retry, which also skips a Gateway whose
/// breaker opened.
pub async fn call(client: &Client, actor: pb::Actor, payload: Vec<u8>) -> Result<Vec<u8>, Status> {
    let mut table = client.routes(GATEWAY, None).await?;
    if table.members.is_empty() {
        // A cached table may predate the cell's Gateways.
        table = client.routes(GATEWAY, Some(table.revision)).await?;
    }
    let addresses: Vec<String> = table.members.values().map(|m| m.address.clone()).collect();
    let address = client
        .route(&addresses)
        .into_iter()
        .next()
        .ok_or_else(|| Status::unavailable("no gateway in this cell"))?;
    let member = table
        .members
        .values()
        .find(|m| m.address == *address)
        .expect("routed from the table");
    let request = pb::InvokeRequest {
        actor: Some(actor),
        payload,
        host: String::new(),
        alarm_ms: None,
    };
    let reply = client
        .call_member(member, |channel| async move {
            pb::actors_client::ActorsClient::new(channel)
                .invoke(request)
                .await
        })
        .await;
    if reply.is_err() {
        // The table may name Gateways that left; the call's own error is what the caller sees.
        let _ = client.routes(GATEWAY, Some(table.revision)).await;
    }
    Ok(reply?.payload)
}
