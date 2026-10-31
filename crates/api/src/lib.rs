//! `bedrock-api`: authoritative writes and revision ordering (B-09).
//!
//! Writes arrive over HTTP on `write_listen` and commit through [`Writer`]; relays subscribe to the committed log on
//! `listen` (B-10).
//!
//! `PUT /v1/{dataset}/{namespace}/{key}` sets the key to the request body and `DELETE` deletes it. The response body is
//! the committed sequence. An `Idempotency-Key` retry returns the original sequence while it remains
//! within the configured retry duration (24 hours by default). An expired retry is a new write.
//! Every write requires `Authorization: Bearer <write_token>`; the configured token grants access to all writes.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use bedrock_lib::node::{self, ApiConfig, ConfigError};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::writer::{
    CommitError, Persistence, PersistenceMetrics, Write, Writer,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;

/// Runs the node until Ctrl-C or SIGTERM. `main` and the simulation both start here.
pub async fn run(config: ApiConfig) -> Result<(), ConfigError> {
    tracing::info!(node_id = %config.node.node_id, "starting bedrock-api");
    let admin = node::start_admin(&config.node).await?;
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
    let factory = Arc::new(bedrock_lib::storage::namespace::memory::MemoryFactory::default());
    let writer = Writer::open(
        now,
        Persistence {
            factory,
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
    let writes = tokio::spawn(serve(writes, app));
    let replication = tonic::transport::Server::builder()
        .add_service(
            bedrock_lib::replication::pb::relay_server::RelayServer::new(
                bedrock_lib::relays::Hub::writer(
                    config.node.node_id.clone(),
                    writer.clone(),
                    Arc::new(bedrock_lib::transport::Peers::new(&admin)),
                ),
            ),
        )
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
    if write_token.is_empty() || !write_token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(ConfigError(
            "write_token must be nonempty printable ASCII without spaces".into(),
        ));
    }
    Ok(Router::new()
        .route("/v1/{dataset}/{namespace}/{*key}", put(write).delete(write))
        .route_layer(middleware::from_fn_with_state(
            Arc::<str>::from(write_token),
            authorize,
        ))
        .with_state(writer))
}

async fn authorize(State(write_token): State<Arc<str>>, request: Request, next: Next) -> Response {
    let mut credentials = request.headers().get_all(header::AUTHORIZATION).iter();
    let authorized = match (credentials.next(), credentials.next()) {
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(|value| value.split_once(' '))
            .is_some_and(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("Bearer")
                    && bool::from(token.as_bytes().ct_eq(write_token.as_bytes()))
            }),
        _ => false,
    };
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
        )
            .into_response();
    }
    next.run(request).await
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

#[cfg(test)]
#[path = "../tests/unit/startup.rs"]
mod tests;
