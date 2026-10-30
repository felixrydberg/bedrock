//! Native object-store configuration and full-object checksum operations.

use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, Checksum, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    Attribute, ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutResult,
    RetryConfig,
};
use tokio_stream::StreamExt;

use super::{S3Config, checksum, context, verify_checksum};
use crate::storage::{StorageError, StorageResult};

/// Native strict create/CAS, range reads and multipart I/O, shared by all consumers.
pub fn object_store(config: &S3Config) -> StorageResult<Arc<dyn ObjectStore>> {
    if config.max_attempts == 0 || config.attempt_timeout_ms == 0 {
        return Err(StorageError::Unsupported(context(
            "configuration",
            "S3 attempts and attempt timeout must be positive",
        )));
    }
    let retries = RetryConfig {
        max_retries: config.max_attempts.saturating_sub(1) as usize,
        retry_timeout: Duration::from_millis(
            config
                .attempt_timeout_ms
                .saturating_mul(u64::from(config.max_attempts)),
        ),
        ..RetryConfig::default()
    };
    AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_checksum_algorithm(Checksum::SHA256)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(config.endpoint.starts_with("http://"))
                .with_timeout(Duration::from_millis(config.attempt_timeout_ms)),
        )
        .with_retry(retries)
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|_| {
            StorageError::Unsupported(context("configuration", "invalid S3 client configuration"))
        })
}

/// Write a whole artifact with checksum metadata, preserving native put conditions.
/// Create is strict: an existing object is an error even for identical bytes.
pub async fn put_checked(
    store: &dyn ObjectStore,
    path: &Path,
    body: Vec<u8>,
    mode: PutMode,
) -> object_store::Result<PutResult> {
    let mut options = PutOptions::from(mode);
    options
        .attributes
        .insert(Attribute::Metadata("sha256".into()), checksum(&body).into());
    store.put_opts(path, body.into(), options).await
}

fn read_error(path: &Path, error: object_store::Error) -> StorageError {
    // Backend errors can carry configured URLs. Expose classifications without secrets.
    match error {
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. } => {
            StorageError::Unsupported(context(path.as_ref(), "object read operation unsupported"))
        }
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => {
            StorageError::IoError(context(path.as_ref(), "object read permission denied"))
        }
        _ => StorageError::Unavailable(context(path.as_ref(), "object read unavailable")),
    }
}

/// A complete validated artifact, or an absent key. Native engine ranges bypass
/// this helper and retain their own block integrity checks.
pub async fn get_checked(store: &dyn ObjectStore, path: &Path) -> StorageResult<Option<Vec<u8>>> {
    let result = match store.get(path).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => {
            // S3 maps missing buckets and keys to the same error. A successful LIST
            // confirms the backing store exists before this becomes an application miss.
            if let Some(result) = store.list(Some(path)).next().await {
                result.map_err(|error| read_error(path, error))?;
            }
            return Ok(None);
        }
        Err(error) => return Err(read_error(path, error)),
    };
    let expected = result
        .attributes
        .get(&Attribute::Metadata("sha256".into()))
        .map(|value| value.as_ref().to_owned());
    let body = result
        .bytes()
        .await
        .map_err(|error| read_error(path, error))?;
    verify_checksum(path.as_ref(), &body, expected.as_deref())?;
    Ok(Some(body.to_vec()))
}

#[cfg(test)]
#[path = "../../tests/unit/s3/object.rs"]
mod tests;
