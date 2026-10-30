//! Observability baseline every binary serves on `admin_listen` (B-03):
//! - `/metrics`: the node's metrics in OpenMetrics text. Modules [`Admin::register`] theirs, labelled only by
//!   low-cardinality dimensions (`feature-drafts.md` §4.8).
//! - `/health`: the process is up.
//! - `/ready`: 200 once the node has validated its state and serves traffic, 503 otherwise.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus_client::registry::{Metric, Registry};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

const MAX_ADMIN_CONNECTIONS: usize = 128;

pub struct Admin {
    registry: Mutex<Registry>,
    ready: AtomicBool,
}

impl Admin {
    pub fn new() -> Arc<Self> {
        Arc::new(Admin {
            registry: Mutex::new(Registry::with_prefix("bedrock")),
            ready: AtomicBool::new(false),
        })
    }

    pub fn register(&self, name: &str, help: &str, metric: impl Metric) {
        self.registry.lock().unwrap().register(name, help, metric);
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// The `/metrics` body.
    pub fn render(&self) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry.lock().unwrap())
            .expect("writing to a String does not fail");
        out
    }

    /// Answers HTTP/1 on `listener` until the task is dropped.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        let connections = Arc::new(Semaphore::new(MAX_ADMIN_CONNECTIONS));
        loop {
            let permit = connections.clone().acquire_owned().await.unwrap();
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    drop(permit);
                    // Out of file descriptors and similar; back off instead of spinning.
                    tracing::warn!(error = %e, "admin accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let admin = self.clone();
            let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                let rsp = admin.respond(req.uri().path());
                async move { Ok::<_, Infallible>(rsp) }
            });
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .timer(RuntimeTimer)
                    .serve_connection(TokioIo::new(stream), svc)
                    .await
                {
                    tracing::debug!(error = %e, "admin connection failed");
                }
            });
        }
    }

    fn respond(&self, path: &str) -> Response<Full<Bytes>> {
        let (status, body) = match path {
            "/metrics" => (StatusCode::OK, self.render()),
            "/health" => (StatusCode::OK, "ok".into()),
            "/ready" if self.ready.load(Ordering::Relaxed) => (StatusCode::OK, "ready".into()),
            "/ready" => (StatusCode::SERVICE_UNAVAILABLE, "not ready".into()),
            _ => (StatusCode::NOT_FOUND, "not found".into()),
        };
        let content_type = if path == "/metrics" {
            "application/openmetrics-text; version=1.0.0; charset=utf-8"
        } else {
            "text/plain; charset=utf-8"
        };
        Response::builder()
            .status(status)
            .header(CONTENT_TYPE, content_type)
            .body(Full::new(Bytes::from(body)))
            .expect("static status and header are valid")
    }
}

/// hyper's timer (for its header read timeout) on the runtime clock. hyper-util's `TokioTimer` calls the real tokio,
/// which has no reactor under madsim.
#[derive(Clone)]
struct RuntimeTimer;

impl hyper::rt::Timer for RuntimeTimer {
    // madsim's `Instant` is std's, and madsim simulates its `now`; the lints guard normal builds.
    #[allow(clippy::useless_conversion)]
    #[cfg_attr(madsim, allow(clippy::disallowed_methods))]
    fn now(&self) -> std::time::Instant {
        tokio::time::Instant::now().into()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(RuntimeSleep(Box::pin(tokio::time::sleep(duration))))
    }

    #[allow(clippy::useless_conversion)] // madsim's `Instant` is std's
    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(RuntimeSleep(Box::pin(tokio::time::sleep_until(
            deadline.into(),
        ))))
    }
}

struct RuntimeSleep(Pin<Box<tokio::time::Sleep>>);

impl Future for RuntimeSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.0.as_mut().poll(cx)
    }
}

impl hyper::rt::Sleep for RuntimeSleep {}

#[cfg(test)]
#[path = "../../tests/unit/transport/admin.rs"]
mod tests;
