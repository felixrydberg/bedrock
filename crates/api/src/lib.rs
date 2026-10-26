//! `bedrock-api`: authoritative writes and revision ordering (B-09).
//!
//! Writes arrive over HTTP on `write_listen` and commit through [`Writer`]; relays subscribe to the committed log on
//! `listen` (B-10).
//!
//! `PUT /v1/{dataset}/{namespace}/{key}` sets the key to the request body and `DELETE` deletes it. The response body is
//! the committed sequence. With an `Idempotency-Key` header, a retry returns the first commit's sequence.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::put;
use bedrock_lib::node::{self, ApiConfig, ConfigError};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::subscriptions::Source;
use bedrock_lib::replication::subscriptions::pb::replication_server::ReplicationServer;
use bedrock_lib::replication::writer::{CommitError, Write, Writer};
use bedrock_lib::s3::ObjectStore;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ApiConfig) -> Result<(), ConfigError> {
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-api");
    let admin = node::start_admin(&config.node).await?;
    #[cfg(not(madsim))]
    let store = bedrock_lib::s3::aws::S3Store::new(&config.s3);
    // The simulated S3 SDK has no conditional writes, so simulation builds use the in-memory store (B-07).
    #[cfg(madsim)]
    let store = bedrock_lib::s3::memory::MemoryStore::default();
    let writer = Writer::open(store, now)
        .await
        .map_err(|e| ConfigError(format!("loading the log: {e}")))?;
    let writes = TcpListener::bind(config.write_listen)
        .await
        .map_err(|e| ConfigError(format!("write_listen {}: {e}", config.write_listen)))?;
    tokio::spawn(serve(writes, app(writer.clone())));
    let replication = tonic::transport::Server::builder()
        .add_service(ReplicationServer::new(Source::new(writer.log())))
        .serve(config.node.listen);

    admin.set_ready(true);
    tokio::select! {
        biased;
        () = node::shutdown_signal() => {}
        served = replication => {
            served.map_err(|e| ConfigError(format!("listen {}: {e}", config.node.listen)))?;
        }
    }
    admin.set_ready(false);
    tracing::info!("shutting down");
    Ok(())
}

/// The write routes over `writer`.
pub fn app<S: ObjectStore + 'static>(writer: Arc<Writer<S>>) -> Router {
    Router::new()
        .route(
            "/v1/{dataset}/{namespace}/{*key}",
            put(write::<S>).delete(write::<S>),
        )
        .with_state(writer)
}

/// Serves `app` over HTTP/1 through hyper. `axum::serve` does not run under madsim (#9).
pub async fn serve(listener: TcpListener, app: Router) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let service = TowerToHyperService::new(app.clone());
                tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            }
            Err(error) => tracing::warn!(%error, "accepting a write connection failed"),
        }
    }
}

async fn write<S: ObjectStore + 'static>(
    State(writer): State<Arc<Writer<S>>>,
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
    };
    match writer.commit(write).await {
        Ok(sequence) => (StatusCode::OK, sequence.to_string()),
        Err(error) => {
            let status = match error {
                CommitError::Invalid(_) => StatusCode::BAD_REQUEST,
                CommitError::Busy => StatusCode::TOO_MANY_REQUESTS,
                CommitError::Fenced | CommitError::Unavailable(_) => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            };
            (status, error.to_string())
        }
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
