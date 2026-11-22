//! Bindings, checked outside the sandbox (`statefull/v2.md` §8). A Worker's call to an actor leaves this host for one of
//! its own cell's Gateways on the internal listener, which routes it to the holder (#116 / S-08, §6 step 1).

use bedrock_lib::client::Client;
use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::volume as pb;
use tonic::Status;

/// Sends `payload` to `actor` through this cell's Gateway and returns the actor's answer.
// ponytail: one Gateway per call, in health order; a failed call is the caller's to retry, which then skips a Gateway
// whose breaker opened. Fail over inside the call if binding retries get noisy.
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
    };
    let reply = client
        .call_member(member, |channel| async move {
            pb::actors_client::ActorsClient::new(channel)
                .invoke(request)
                .await
        })
        .await?;
    Ok(reply.payload)
}
