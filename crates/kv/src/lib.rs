//! `bedrock-kv`: customer KV. It hosts no relay-fed datasets; it only joins the cell membership as service `kv`.

use bedrock_lib::membership;
use bedrock_lib::node::{self, ConfigError, ServiceConfig};

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ServiceConfig) -> Result<(), ConfigError> {
    config.validate()?;
    check_hosts_nothing_pushed(&config)?;
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-kv");
    // ponytail: customer-KV readers (per-namespace S3 databases behind read-only caches) arrive with B-39; roles
    // are only logged until then.
    for role in &config.roles {
        tracing::info!(?role, "role enabled");
    }
    let admin = node::start_admin(&config.node).await?;
    let (members, target) = membership::View::new("kv", &config.node.location.cell);
    let peers = std::sync::Arc::new(bedrock_lib::transport::Peers::new(&admin));
    let session = bedrock_lib::relays::client::Session::join(
        peers.clone(),
        config.relays.clone(),
        vec![target],
        membership::member(&config, "kv"),
    )
    .map_err(|e| ConfigError(e.to_string()))?;
    let mut readiness = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut shutdown = std::pin::pin!(node::shutdown_signal());
    loop {
        tokio::select! {biased;
            ()=&mut shutdown=>break,
            _=readiness.tick()=>{
                admin.set_ready(session.ready());
                if let Err(error)=members.refresh(&session,&peers){
                    tracing::warn!(%error,"membership view unreadable");
                }
            },
        }
    }
    admin.set_ready(false);
    session.drain().await;
    tracing::info!("shutting down");
    Ok(())
}

/// Customer KV has no pushed data: quicksilver, auth and cdn belong to other binaries.
fn check_hosts_nothing_pushed(config: &ServiceConfig) -> Result<(), ConfigError> {
    if config.datasets.is_empty() && config.subscriptions.is_empty() {
        return Ok(());
    }
    Err(ConfigError(
        "bedrock-kv hosts no relay-fed datasets; datasets and subscriptions must be empty".into(),
    ))
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
