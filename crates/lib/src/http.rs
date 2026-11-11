//! HTTP listeners for token-authenticated APIs: `bedrock-api` writes and `bedrock-kv` namespace management (#56 / B-25).
//! Served through hyper because `axum::serve` does not run under madsim (#9).

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;

use crate::node::ConfigError;
use crate::transport::tls::Tls;

/// `router` restricted to callers sending `Authorization: Bearer <token>`.
pub fn bearer(router: Router, token: &str) -> Result<Router, ConfigError> {
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(ConfigError(
            "token must be nonempty printable ASCII without spaces".into(),
        ));
    }
    Ok(router.route_layer(middleware::from_fn_with_state(
        Arc::<str>::from(token),
        authorize,
    )))
}

async fn authorize(State(token): State<Arc<str>>, request: Request, next: Next) -> Response {
    let mut credentials = request.headers().get_all(header::AUTHORIZATION).iter();
    let authorized = match (credentials.next(), credentials.next()) {
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(|value| value.split_once(' '))
            .is_some_and(|(scheme, presented)| {
                scheme.eq_ignore_ascii_case("Bearer")
                    && bool::from(presented.as_bytes().ct_eq(token.as_bytes()))
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

/// Serves `app` over HTTP/1 through hyper, over TLS when `tls` is set.
pub async fn serve(listener: TcpListener, app: Router, tls: Option<Arc<Tls>>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let service = TowerToHyperService::new(app.clone());
                let Some(tls) = tls.clone() else {
                    tokio::spawn(
                        http1::Builder::new().serve_connection(TokioIo::new(stream), service),
                    );
                    continue;
                };
                tokio::spawn(async move {
                    match tls.accept_optional(stream).await {
                        Ok(stream) => {
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        }
                        Err(error) => tracing::debug!(%error, "http tls handshake rejected"),
                    }
                });
            }
            Err(error) => tracing::warn!(%error, "accepting an http connection failed"),
        }
    }
}

/// Sends one HTTP/1 request to `addr` (`host:port`), over TLS when `tls` is set, and returns the status and body.
/// Fails after `timeout` without an answer.
pub async fn send(
    addr: &str,
    tls: Option<&Tls>,
    request: hyper::Request<http_body_util::Full<axum::body::Bytes>>,
    timeout: std::time::Duration,
) -> Result<(StatusCode, axum::body::Bytes), String> {
    use http_body_util::BodyExt;
    let exchange = async {
        let stream: Box<dyn crate::transport::Io> = match tls {
            Some(tls) => tls.dial(addr).await.map_err(|e| e.to_string())?,
            None => Box::new(
                tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|e| e.to_string())?,
            ),
        };
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        Ok((status, body))
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| format!("{addr} did not answer within {timeout:?}"))?
}
