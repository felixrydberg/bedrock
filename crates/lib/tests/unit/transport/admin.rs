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
#[cfg_attr(madsim, allow(clippy::disallowed_methods))] // as on `RuntimeTimer::now`
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
