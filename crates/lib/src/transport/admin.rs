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
    #[allow(clippy::useless_conversion)] // madsim's `Instant` is std's
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
mod tests {
    use super::*;
    use prometheus_client::metrics::counter::Counter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        // Simulated TCP sends only on flush.
        stream.flush().await.unwrap();
        let mut rsp = String::new();
        stream.read_to_string(&mut rsp).await.unwrap();
        rsp
    }

    #[tokio::test]
    async fn metrics_health_and_readiness_respond() {
        let admin = Admin::new();
        let requests = Counter::<u64>::default();
        admin.register("test_requests", "Test counter", requests.clone());
        requests.inc();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(admin.clone().serve(listener));

        let metrics = get(addr, "/metrics").await;
        assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
        assert!(
            metrics.contains("bedrock_test_requests_total 1"),
            "{metrics}"
        );
        assert!(get(addr, "/health").await.starts_with("HTTP/1.1 200"));
        assert!(get(addr, "/ready").await.starts_with("HTTP/1.1 503"));
        admin.set_ready(true);
        assert!(get(addr, "/ready").await.starts_with("HTTP/1.1 200"));
        assert!(get(addr, "/nope").await.starts_with("HTTP/1.1 404"));
    }

    #[tokio::test]
    async fn admin_connections_are_bounded() {
        let admin = Admin::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(admin.serve(listener));

        let mut held = Vec::with_capacity(MAX_ADMIN_CONNECTIONS);
        for _ in 0..MAX_ADMIN_CONNECTIONS {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(b"GET /health HTTP/1.1\r\n").await.unwrap();
            held.push(stream);
        }
        tokio::task::yield_now().await;

        let mut excess = TcpStream::connect(addr).await.unwrap();
        excess
            .write_all(b"GET /health HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        excess.flush().await.unwrap();
        let mut response = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), excess.read(&mut response))
                .await
                .is_err()
        );

        drop(held.pop());
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), excess.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"));
    }

    #[cfg_attr(not(madsim), tokio::test(start_paused = true))]
    #[cfg_attr(madsim, tokio::test)]
    async fn timer_deadline_uses_runtime_clock() {
        use hyper::rt::Timer;

        tokio::time::sleep(Duration::from_secs(120)).await;
        let delay = Duration::from_secs(10);
        let started = tokio::time::Instant::now();
        let mut sleep = RuntimeTimer.sleep_until(RuntimeTimer.now() + delay);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut sleep)
                .await
                .is_err(),
            "deadline must not expire early"
        );
        tokio::time::timeout(delay, sleep)
            .await
            .expect("deadline must expire on the runtime clock");
        assert!(started.elapsed() >= delay);
    }

    // Virtual time makes hyper's 30 s header read timeout instant.
    #[cfg(madsim)]
    #[tokio::test]
    async fn stalled_request_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(Admin::new().serve(listener));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /health HTTP/1.1\r\n").await.unwrap();
        stream.flush().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut response))
            .await
            .expect("server closes a stalled connection")
            .unwrap();
    }
}
