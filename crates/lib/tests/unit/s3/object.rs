use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use object_store::{ObjectStore, path::Path, prefix::PrefixStore};

use super::*;
use crate::s3::contract::{Harness, s3_contract};
use crate::storage::StorageError;

struct Memory;

impl Harness for Memory {
    async fn open(&mut self) -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }
}

mod memory {
    use super::*;

    s3_contract!(Memory);
}

fn config(endpoint: String) -> S3Config {
    let env = |name, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    S3Config {
        endpoint,
        bucket: env("BEDROCK_TEST_S3_BUCKET", "bedrock-test"),
        region: "us-east-1".into(),
        access_key_id: env("BEDROCK_TEST_S3_ACCESS_KEY_ID", "bedrock"),
        secret_access_key: env("BEDROCK_TEST_S3_SECRET_ACCESS_KEY", "bedrock"),
        max_attempts: 2,
        attempt_timeout_ms: 1_000,
    }
}

struct LiveS3 {
    endpoint: String,
}

fn live_s3() -> Option<LiveS3> {
    match std::env::var("BEDROCK_TEST_S3_ENDPOINT") {
        Ok(endpoint) => Some(LiveS3 { endpoint }),
        Err(_) if std::env::var_os("CI").is_some() => {
            panic!("CI must set BEDROCK_TEST_S3_ENDPOINT")
        }
        Err(_) => {
            eprintln!("skipped: BEDROCK_TEST_S3_ENDPOINT is not set");
            None
        }
    }
}

impl Harness for LiveS3 {
    async fn open(&mut self) -> Arc<dyn ObjectStore> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        #[allow(clippy::disallowed_methods)]
        // Adapter tests isolate repeated runs through a unique object prefix.
        let run = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!(
            "contract-{run}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut config = config(self.endpoint.clone());
        config.attempt_timeout_ms = 10_000;
        Arc::new(PrefixStore::new(object_store(&config).unwrap(), prefix))
    }
}

mod live {
    use super::*;

    // An absent endpoint skips locally and fails CI, where the bucket is provisioned separately.
    s3_contract!(match live_s3() {
        Some(h) => h,
        None => return,
    });

    #[tokio::test]
    async fn missing_bucket_is_an_error_not_a_miss() {
        let Some(h) = live_s3() else { return };
        let mut config = config(h.endpoint);
        config.bucket = format!("bedrock-missing-{}", std::process::id());
        let store = object_store(&config).unwrap();
        let result = get_checked(store.as_ref(), &Path::from("absent")).await;
        assert!(
            matches!(
                result,
                Err(StorageError::IoError(_) | StorageError::Unavailable(_))
            ),
            "{result:?}"
        );
    }
}

#[tokio::test]
async fn unreachable_endpoint_is_unavailable_after_bounded_retries() {
    let mut config = config("http://127.0.0.1:1".into());
    config.access_key_id = "private-access-key".into();
    config.secret_access_key = "private-secret-key".into();
    let store = object_store(&config).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        get_checked(store.as_ref(), &Path::from("k")),
    )
    .await
    .expect("object read exceeded its retry bound");
    assert!(
        matches!(result, Err(StorageError::Unavailable(_))),
        "{result:?}"
    );
    let diagnostic = format!("{result:?}");
    assert!(!diagnostic.contains(&config.access_key_id));
    assert!(!diagnostic.contains(&config.secret_access_key));
}

#[test]
fn builder_rejects_unbounded_request_settings_without_leaking_configuration() {
    for (attempts, timeout) in [(0, 1_000), (2, 0)] {
        let mut config = config("https://private-user:private-password@example.invalid".into());
        config.max_attempts = attempts;
        config.attempt_timeout_ms = timeout;
        let result = object_store(&config);
        assert!(
            matches!(result, Err(StorageError::Unsupported(_))),
            "{result:?}"
        );
        let diagnostic = format!("{result:?}");
        assert!(!diagnostic.contains("private-user"));
        assert!(!diagnostic.contains("private-password"));
        assert!(!diagnostic.contains(&config.endpoint));
    }
}

#[tokio::test]
async fn incomplete_response_bodies_are_bounded_unavailable_errors() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    for stall in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(format!("http://{}", listener.local_addr().unwrap()));
        config.max_attempts = 1;
        config.attempt_timeout_ms = 200;
        let store = object_store(&config).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut request = String::new();
            while !request.ends_with("\r\n\r\n") {
                assert_ne!(stream.read_line(&mut request).await.unwrap(), 0);
            }
            assert!(request.starts_with("GET "), "{request}");
            stream.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nETag: \"etag\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nx-amz-meta-sha256: invalid\r\nConnection: close\r\n\r\nv").await.unwrap();
            stream.get_mut().flush().await.unwrap();
            if stall {
                std::future::pending::<()>().await;
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            get_checked(store.as_ref(), &Path::from("k")),
        )
        .await;
        server.abort();
        let result = result.expect("incomplete body exceeded its request timeout");
        assert!(
            matches!(result, Err(StorageError::Unavailable(_))),
            "{result:?}"
        );
    }
}
