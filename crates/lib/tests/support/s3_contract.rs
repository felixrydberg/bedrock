//! B-07 contract for upstream object stores and Bedrock's full-object checksum helpers.

use std::sync::Arc;

use object_store::{Attribute, ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use tokio_stream::StreamExt;

use crate::s3::object::{get_checked, put_checked};
use crate::storage::StorageError;

pub trait Harness {
    /// A new, empty object namespace.
    async fn open(&mut self) -> Arc<dyn ObjectStore>;

    /// Damage a body through the native API, preserving its checksum metadata.
    async fn corrupt(&mut self, store: &dyn ObjectStore, key: &str) {
        let path = Path::from(key);
        let object = store.get(&path).await.unwrap();
        let attributes = object.attributes.clone();
        let mut body = object.bytes().await.unwrap().to_vec();
        body[0] ^= 1;
        store
            .put_opts(
                &path,
                body.into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
}

macro_rules! s3_contract {
    ($harness:expr) => {
        $crate::s3::contract::s3_contract!(
            $harness;
            missing_key_is_none_not_error,
            put_overwrite_delete,
            create_only_if_absent,
            list_by_prefix_in_byte_order,
            corruption_is_an_error_not_a_miss,
            missing_or_wrong_checksum_is_corruption,
            native_ranges_preserve_partial_reads,
            native_compare_and_swap_rejects_stale_versions,
        );
    };
    ($harness:expr; $($case:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $case() {
                $crate::s3::contract::$case(&mut $harness).await;
            }
        )*
    };
}
pub(crate) use s3_contract;

async fn put(store: &dyn ObjectStore, key: &str, body: &[u8]) {
    put_checked(store, &Path::from(key), body.to_vec(), PutMode::Overwrite)
        .await
        .unwrap();
}

async fn keys(store: &dyn ObjectStore, prefix: Option<&Path>) -> Vec<String> {
    let mut stream = store.list(prefix);
    let mut keys = Vec::new();
    while let Some(object) = stream.next().await {
        keys.push(object.unwrap().location.to_string());
    }
    // Upstream listing does not promise ordering; callers sort collected results.
    keys.sort();
    keys
}

pub async fn missing_key_is_none_not_error(h: &mut impl Harness) {
    let store = h.open().await;
    assert_eq!(
        get_checked(store.as_ref(), &Path::from("absent")).await,
        Ok(None)
    );
    store.delete(&Path::from("absent")).await.unwrap();
    assert!(keys(store.as_ref(), None).await.is_empty());
}

pub async fn put_overwrite_delete(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    for body in [b"1".as_slice(), b"2", b""] {
        put(store.as_ref(), "k", body).await;
        assert_eq!(
            get_checked(store.as_ref(), &path).await,
            Ok(Some(body.to_vec()))
        );
    }
    store.delete(&path).await.unwrap();
    assert_eq!(get_checked(store.as_ref(), &path).await, Ok(None));
}

pub async fn create_only_if_absent(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    put_checked(store.as_ref(), &path, b"1".to_vec(), PutMode::Create)
        .await
        .unwrap();
    for body in [b"2", b"1"] {
        let result = put_checked(store.as_ref(), &path, body.to_vec(), PutMode::Create).await;
        assert!(
            matches!(result, Err(object_store::Error::AlreadyExists { .. })),
            "{result:?}"
        );
    }
    assert_eq!(
        get_checked(store.as_ref(), &path).await,
        Ok(Some(b"1".to_vec()))
    );
}

pub async fn list_by_prefix_in_byte_order(h: &mut impl Harness) {
    let store = h.open().await;
    for key in ["b/2", "a/1", "b/1", "b/10", "c"] {
        put(store.as_ref(), key, key.as_bytes()).await;
    }
    assert_eq!(
        keys(store.as_ref(), Some(&Path::from("b/"))).await,
        ["b/1", "b/10", "b/2"]
    );
    assert_eq!(
        keys(store.as_ref(), None).await,
        ["a/1", "b/1", "b/10", "b/2", "c"]
    );
    assert!(
        keys(store.as_ref(), Some(&Path::from("x")))
            .await
            .is_empty()
    );
}

pub async fn corruption_is_an_error_not_a_miss(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    put(store.as_ref(), "k", b"value").await;
    h.corrupt(store.as_ref(), "k").await;
    assert!(matches!(
        get_checked(store.as_ref(), &path).await,
        Err(StorageError::Corruption(_))
    ));
    assert!(matches!(
        put_checked(store.as_ref(), &path, b"value".to_vec(), PutMode::Create).await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    put(store.as_ref(), "k", b"value").await;
    assert_eq!(
        get_checked(store.as_ref(), &path).await,
        Ok(Some(b"value".to_vec()))
    );
}

pub async fn missing_or_wrong_checksum_is_corruption(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    for checksum in [None, Some("invalid")] {
        let mut options = PutOptions::default();
        if let Some(checksum) = checksum {
            options
                .attributes
                .insert(Attribute::Metadata("sha256".into()), checksum.into());
        }
        store
            .put_opts(&path, b"value".to_vec().into(), options)
            .await
            .unwrap();
        assert!(matches!(
            get_checked(store.as_ref(), &path).await,
            Err(StorageError::Corruption(_))
        ));
    }
}

pub async fn native_ranges_preserve_partial_reads(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    put(store.as_ref(), "k", b"0123456789").await;
    assert_eq!(store.get_range(&path, 2..5).await.unwrap().as_ref(), b"234");
    let ranges = store.get_ranges(&path, &[7..10, 0..2]).await.unwrap();
    assert_eq!(ranges[0].as_ref(), b"789");
    assert_eq!(ranges[1].as_ref(), b"01");
}

pub async fn native_compare_and_swap_rejects_stale_versions(h: &mut impl Harness) {
    let store = h.open().await;
    let path = Path::from("k");
    let first = put_checked(store.as_ref(), &path, b"first".to_vec(), PutMode::Create)
        .await
        .unwrap();
    let version = object_store::UpdateVersion::from(first);
    put_checked(
        store.as_ref(),
        &path,
        b"second".to_vec(),
        PutMode::Update(version.clone()),
    )
    .await
    .unwrap();
    let stale = put_checked(
        store.as_ref(),
        &path,
        b"stale".to_vec(),
        PutMode::Update(version),
    )
    .await;
    assert!(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "{stale:?}"
    );
    assert_eq!(
        get_checked(store.as_ref(), &path).await,
        Ok(Some(b"second".to_vec()))
    );
}
