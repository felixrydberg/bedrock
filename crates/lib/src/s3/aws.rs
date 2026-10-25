//! [`ObjectStore`] on S3, or an S3-compatible store such as SeaweedFS, through the AWS SDK. Not built under madsim.
//!
//! The SHA-256 of each body is sent as `x-amz-checksum-sha256`, so the server rejects a body damaged in transit, and
//! kept as `x-amz-meta-sha256`, which every read is verified against.

use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use super::{ObjectStore, Put, S3Config, S3Error, S3Result, hex, sha256, verify};

/// User metadata key holding the hex SHA-256 of the body.
const CHECKSUM_META: &str = "sha256";

pub struct S3Store {
    client: Client,
    bucket: String,
    attempt_timeout: Duration,
}

impl S3Store {
    pub fn new(config: &S3Config) -> Self {
        let attempt_timeout = Duration::from_millis(config.attempt_timeout_ms);
        let sdk = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(&config.endpoint)
            .region(Region::new(config.region.clone()))
            .credentials_provider(Credentials::new(
                &config.access_key_id,
                &config.secret_access_key,
                None,
                None,
                "bedrock",
            ))
            .force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(config.max_attempts))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(attempt_timeout)
                    .build(),
            )
            // The store sends and checks its own SHA-256. The SDK's default CRC32 trailers would only duplicate it,
            // and some S3-compatible stores reject them.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        S3Store {
            client: Client::from_conf(sdk),
            bucket: config.bucket.clone(),
            attempt_timeout,
        }
    }
}

impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> S3Result<Option<Vec<u8>>> {
        let out = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => out,
            // Only a missing key; a missing bucket is also a 404 but must stay an error.
            Err(e) if e.code() == Some("NoSuchKey") => return Ok(None),
            Err(e) => return Err(classify(key, e)),
        };
        let expected = out.metadata().and_then(|m| m.get(CHECKSUM_META)).cloned();
        let body = out.body.collect().await.map_err(|e| S3Error::Unavailable {
            detail: format!("{key}: reading body: {e}"),
        })?;
        verify(key, body.to_vec(), expected.as_deref()).map(Some)
    }

    async fn put(&self, key: &str, body: Vec<u8>, mode: Put) -> S3Result<()> {
        let sum = sha256(&body);
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .checksum_sha256(BASE64.encode(sum))
            .metadata(CHECKSUM_META, hex(&sum))
            .body(ByteStream::from(body.clone()));
        if mode == Put::Create {
            request = request.if_none_match("*");
        }
        match request.send().await {
            Ok(_) => Ok(()),
            Err(e) if status(&e) == Some(412) => {
                // The key exists. It may hold our own body from an earlier attempt whose response was lost.
                let existing = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| classify(key, e))?;
                if existing.metadata().and_then(|m| m.get(CHECKSUM_META)) == Some(&hex(&sum)) {
                    let stored =
                        tokio::time::timeout(self.attempt_timeout, existing.body.collect())
                            .await
                            .map_err(|_| S3Error::Unavailable {
                                detail: format!("{key}: reading body: timed out"),
                            })?
                            .map_err(|e| S3Error::Unavailable {
                                detail: format!("{key}: reading body: {e}"),
                            })?;
                    if stored.into_bytes().as_ref() == body {
                        return Ok(());
                    }
                }
                Err(S3Error::AlreadyExists { key: key.into() })
            }
            Err(e) => Err(classify(key, e)),
        }
    }

    async fn list(&self, prefix: &str) -> S3Result<Vec<String>> {
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        let mut keys = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| classify(prefix, e))?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|o| o.key().map(str::to_owned)),
            );
        }
        // S3 lists in byte order already; not every compatible store promises to.
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> S3Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| classify(key, e))
    }
}

fn status<E>(err: &SdkError<E, HttpResponse>) -> Option<u16> {
    err.raw_response().map(|r| r.status().as_u16())
}

/// Maps a request failure the SDK's retries did not resolve.
fn classify<E>(key: &str, err: SdkError<E, HttpResponse>) -> S3Error
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    let checksum_rejected = matches!(
        err.code(),
        Some("BadDigest" | "XAmzContentChecksumMismatch" | "XAmzContentSHA256Mismatch")
    );
    let transient = match &err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(_) => status(&err).is_some_and(|s| s == 429 || s >= 500),
        _ => false,
    };
    let detail = format!("{key}: {}", DisplayErrorContext(&err));
    if checksum_rejected {
        S3Error::Corruption {
            key: key.into(),
            detail,
        }
    } else if transient {
        S3Error::Unavailable { detail }
    } else {
        S3Error::Failed { detail }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::*;
    use crate::s3::contract::{Harness, s3_contract};

    fn config(endpoint: String, bucket: String) -> S3Config {
        let env = |name, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
        S3Config {
            endpoint,
            bucket,
            region: "us-east-1".into(),
            access_key_id: env("BEDROCK_TEST_S3_ACCESS_KEY_ID", "bedrock"),
            secret_access_key: env("BEDROCK_TEST_S3_SECRET_ACCESS_KEY", "bedrock"),
            max_attempts: 2,
            attempt_timeout_ms: 10_000,
        }
    }

    /// Talks to the S3-compatible store at `BEDROCK_TEST_S3_ENDPOINT` (SeaweedFS in CI). Without it the contract tests
    /// are skipped locally, and fail in CI so they cannot be skipped silently there.
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
        type Store = S3Store;

        /// A store on a new bucket, so tests and reruns never see each other's objects.
        async fn open(&mut self) -> S3Store {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            #[allow(clippy::disallowed_methods)]
            // wall time only makes bucket names unique across runs
            let run = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let bucket = format!(
                "bedrock-test-{run}-{}",
                NEXT.fetch_add(1, Ordering::Relaxed)
            );
            let store = S3Store::new(&config(self.endpoint.clone(), bucket.clone()));
            store
                .client
                .create_bucket()
                .bucket(bucket)
                .send()
                .await
                .unwrap();
            store
        }

        /// Uploads a damaged body with the original checksum metadata, bypassing the store.
        async fn corrupt(&mut self, store: &S3Store, key: &str) {
            let bucket = &store.bucket;
            let out = store
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            let stored = out.metadata().unwrap()[CHECKSUM_META].clone();
            let mut body = out.body.collect().await.unwrap().to_vec();
            body[0] ^= 1;
            store
                .client
                .put_object()
                .bucket(bucket)
                .key(key)
                .metadata(CHECKSUM_META, stored)
                .body(ByteStream::from(body))
                .send()
                .await
                .unwrap();
        }
    }

    // Each generated test returns early when `live_s3()` skips.
    s3_contract!(match live_s3() {
        Some(h) => h,
        None => return,
    });

    #[tokio::test]
    async fn unreachable_endpoint_is_unavailable_after_bounded_retries() {
        let store = S3Store::new(&config("http://127.0.0.1:1".into(), "b".into()));
        let got = store.get("k").await;
        assert!(matches!(got, Err(S3Error::Unavailable { .. })), "{got:?}");
    }

    #[tokio::test]
    async fn create_retry_body_timeout_and_read_error_are_unavailable() {
        for stall in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = config(
                format!("http://{}", listener.local_addr().unwrap()),
                "b".into(),
            );
            config.max_attempts = 1;
            config.attempt_timeout_ms = 1_000;
            let store = S3Store::new(&config);
            let server = tokio::spawn(async move {
                for method in ["PUT", "GET"] {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = BufReader::new(stream);
                    let mut request = String::new();
                    while !request.ends_with("\r\n\r\n") {
                        assert_ne!(stream.read_line(&mut request).await.unwrap(), 0);
                    }
                    assert!(request.starts_with(method), "{request}");
                    if method == "PUT" {
                        stream.read_exact(&mut [0; 5]).await.unwrap();
                        stream.get_mut().write_all(
                            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        ).await.unwrap();
                    } else {
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nx-amz-meta-sha256: {}\r\nConnection: close\r\n\r\nv",
                            hex(&sha256(b"value")),
                        );
                        stream
                            .get_mut()
                            .write_all(response.as_bytes())
                            .await
                            .unwrap();
                        stream.get_mut().flush().await.unwrap();
                        // Keep the advertised body incomplete, either stalled or disconnected.
                        if stall {
                            std::future::pending::<()>().await;
                        }
                    }
                }
            });
            let got = tokio::time::timeout(
                Duration::from_secs(5),
                store.put("k", b"value".to_vec(), Put::Create),
            )
            .await;
            server.abort();
            let got = got.expect("create retry did not bound its body read");
            let Err(S3Error::Unavailable { detail }) = got else {
                panic!("{got:?}");
            };
            assert!(detail.starts_with("k: reading body:"), "{detail}");
            if stall {
                assert!(detail.contains("timed out"), "{detail}");
            }
        }
    }
}
