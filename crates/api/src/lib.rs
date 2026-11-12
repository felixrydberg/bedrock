//! `bedrock-api`: authoritative writes and revision ordering (B-09).
//!
//! Writes arrive over HTTP on `write_listen` and commit through [`Writer`]; relays subscribe to the committed log on
//! `listen` (B-10).
//!
//! `PUT /v1/{dataset}/{namespace}/{key}` sets the key to the request body and `DELETE` deletes it. The response body is
//! the committed sequence. An `Idempotency-Key` retry returns the original sequence while it remains
//! within the configured retry duration (24 hours by default). An expired retry is a new write. An `If-Match` header
//! holding a revision commits only if that is still the namespace's head, otherwise it answers 412 with the head
//! as the body and writes nothing.
//! Every write requires `Authorization: Bearer <write_token>`; the configured token grants access to all writes.
//! Only pushed datasets are written here. Customer KV (`kv`) is written by each namespace's owning `bedrock-kv` node
//! (#58 / B-26): this writer neither takes its writes nor opens its databases, which would fence that owner.
//! With `tls` configured, both listeners use TLS (B-55): relays need a node certificate, while write callers may
//! present one but still authenticate with the token.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::routing::put;
use bedrock_lib::datasets::capabilities;
use bedrock_lib::node::{self, ApiConfig, ConfigError};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::writer::{
    CommitError, Persistence, PersistenceMetrics, Write, Writer,
};
use bedrock_lib::storage::StorageResult;
use bedrock_lib::storage::namespace::{NamespaceDatabase, NamespaceFactory};
use bedrock_lib::transport;
use tokio::net::TcpListener;

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ApiConfig) -> Result<(), ConfigError> {
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-api");
    let admin = node::start_admin(&config.node).await?;
    let tls = transport::tls::start(&config.node, "api", &admin).await?;
    config
        .retention
        .validate()
        .map_err(|error| ConfigError(error.to_string()))?;
    #[cfg(not(madsim))]
    let factory = Arc::new(bedrock_lib::storage::slate::SlateFactory::new(
        bedrock_lib::s3::object_store(&config.s3)
            .map_err(|error| ConfigError(error.to_string()))?,
    ));
    #[cfg(madsim)]
    let factory = bedrock_lib::sim::namespaces();
    let writer = Writer::open(
        now,
        Persistence {
            factory: Arc::new(Pushed(factory)),
            retention: config.retention,
            metrics: PersistenceMetrics::new(&admin),
        },
    )
    .await
    .map_err(|e| ConfigError(format!("opening durable state: {e}")))?;
    let app = app(writer.clone(), config.write_token)?;
    let writes = TcpListener::bind(config.write_listen)
        .await
        .map_err(|e| ConfigError(format!("write_listen {}: {e}", config.write_listen)))?;
    let writes = tokio::spawn(bedrock_lib::http::serve(writes, app, tls.clone()));
    let router = transport::server(tls.as_ref()).add_service(
        bedrock_lib::replication::pb::relay_server::RelayServer::new(
            bedrock_lib::relays::Hub::writer(
                config.node.node_id.clone(),
                writer.clone(),
                Arc::new(transport::Peers::new(&admin).with_tls(tls.clone())),
            ),
        ),
    );
    let replication = transport::serve(router, config.node.listen, tls);

    admin.set_ready(true);
    tokio::select! {
        biased;
        () = node::shutdown_signal() => {}
        served = replication => {
            served.map_err(|e| ConfigError(format!("listen {}: {e}", config.node.listen)))?;
        }
    }
    admin.set_ready(false);
    writes.abort();
    writer
        .close()
        .await
        .map_err(|e| ConfigError(format!("closing durable state: {e}")))?;
    tracing::info!("shutting down");
    Ok(())
}

/// The write routes over `writer`, restricted to holders of `write_token`.
pub fn app(writer: Arc<Writer>, write_token: String) -> Result<Router, ConfigError> {
    let routes = Router::new()
        .route("/v1/{dataset}/{namespace}/{*key}", put(write).delete(write))
        .with_state(writer);
    bedrock_lib::http::bearer(routes, &write_token)
}

async fn write(
    State(writer): State<Arc<Writer>>,
    Path((dataset, namespace, key)): Path<(String, String, String)>,
    method: Method,
    headers: HeaderMap,
    value: Bytes,
) -> (StatusCode, String) {
    let idempotency_key = match headers.get("idempotency-key").map(|k| k.to_str()) {
        None => None,
        Some(Ok(key)) if !key.is_empty() => Some(key.to_owned()),
        Some(_) => return (StatusCode::BAD_REQUEST, "invalid Idempotency-Key".into()),
    };
    let expected = match headers.get(header::IF_MATCH).map(|revision| {
        let revision = revision.to_str().ok();
        revision.and_then(|revision| revision.parse::<u64>().ok())
    }) {
        None => None,
        Some(Some(revision)) => Some(revision),
        Some(None) => return (StatusCode::BAD_REQUEST, "invalid If-Match".into()),
    };
    if !capabilities::pushed(&dataset) {
        return (
            StatusCode::BAD_REQUEST,
            format!("{dataset} is not written here; its own service writes it"),
        );
    }
    let operation = match method {
        Method::DELETE => Operation::Delete,
        _ => Operation::Set(value.to_vec()),
    };
    let write = Write {
        dataset,
        namespace,
        key: key.into_bytes(),
        operation,
        idempotency_key,
        expected,
    };
    match writer.commit(write).await {
        Ok(sequence) => (StatusCode::OK, sequence.to_string()),
        Err(error) => {
            let status = match error {
                CommitError::Invalid(_) => StatusCode::BAD_REQUEST,
                CommitError::Busy => StatusCode::TOO_MANY_REQUESTS,
                // The body is the head, so the caller can re-read at it and plan again.
                CommitError::Conflict { head } => {
                    return (StatusCode::PRECONDITION_FAILED, head.to_string());
                }
                CommitError::Fenced | CommitError::Unavailable(_) => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            };
            (status, error.to_string())
        }
    }
}

/// The bucket as this writer sees it: startup recovery lists only pushed datasets' databases. Opening another service's
/// database would take it from that service's writer.
struct Pushed(Arc<dyn NamespaceFactory>);

#[async_trait]
impl NamespaceFactory for Pushed {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        let mut scopes = self.0.namespaces().await?;
        scopes.retain(|(dataset, _)| capabilities::pushed(dataset));
        Ok(scopes)
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        self.0.open(dataset, namespace).await
    }
}

/// Milliseconds since the Unix epoch, for `committed_at`. Wall time is the point here; madsim simulates it.
#[allow(clippy::disallowed_methods)]
fn now() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    since_epoch.as_millis() as u64
}

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
