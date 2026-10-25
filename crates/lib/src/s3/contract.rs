//! Contract suite every [`ObjectStore`] must pass (B-07): the in-memory store, and the S3 client against SeaweedFS.
//!
//! A store implements [`Harness`] in its test module and calls `s3_contract!(harness_expr)`, which defines one test
//! per case with a fresh harness each.

use super::{Manifest, ObjectStore, Put, S3Error, latest_manifest, put_manifest};

pub trait Harness {
    type Store: ObjectStore;

    /// A new, empty store.
    async fn open(&mut self) -> Self::Store;

    /// Damages the stored body at `key` behind the store's back, keeping its checksum.
    async fn corrupt(&mut self, store: &Self::Store, key: &str);
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
            manifests_are_versioned_write_once,
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

fn bytes(body: &str) -> Vec<u8> {
    body.as_bytes().to_vec()
}

pub async fn missing_key_is_none_not_error(h: &mut impl Harness) {
    let store = h.open().await;
    assert_eq!(store.get("absent").await, Ok(None));
    store.delete("absent").await.unwrap();
    assert_eq!(store.list("").await, Ok(vec![]));
}

pub async fn put_overwrite_delete(h: &mut impl Harness) {
    let store = h.open().await;
    store.put("k", bytes("1"), Put::Overwrite).await.unwrap();
    assert_eq!(store.get("k").await, Ok(Some(bytes("1"))));
    store.put("k", bytes("2"), Put::Overwrite).await.unwrap();
    assert_eq!(store.get("k").await, Ok(Some(bytes("2"))));
    store
        .put("empty", Vec::new(), Put::Overwrite)
        .await
        .unwrap();
    assert_eq!(store.get("empty").await, Ok(Some(Vec::new())));
    store.delete("k").await.unwrap();
    assert_eq!(store.get("k").await, Ok(None));
}

pub async fn create_only_if_absent(h: &mut impl Harness) {
    let store = h.open().await;
    store.put("k", bytes("1"), Put::Create).await.unwrap();
    assert_eq!(
        store.put("k", bytes("2"), Put::Create).await,
        Err(S3Error::AlreadyExists { key: "k".into() })
    );
    assert_eq!(store.get("k").await, Ok(Some(bytes("1"))));
    // A retried create finds its own body and succeeds.
    store.put("k", bytes("1"), Put::Create).await.unwrap();
}

pub async fn list_by_prefix_in_byte_order(h: &mut impl Harness) {
    let store = h.open().await;
    for key in ["b/2", "a/1", "b/1", "b/10", "c"] {
        store.put(key, bytes(key), Put::Overwrite).await.unwrap();
    }
    assert_eq!(store.list("b/").await.unwrap(), ["b/1", "b/10", "b/2"]);
    assert_eq!(
        store.list("").await.unwrap(),
        ["a/1", "b/1", "b/10", "b/2", "c"]
    );
    assert_eq!(store.list("x").await, Ok(vec![]));
}

pub async fn corruption_is_an_error_not_a_miss(h: &mut impl Harness) {
    let store = h.open().await;
    store
        .put("k", bytes("value"), Put::Overwrite)
        .await
        .unwrap();
    h.corrupt(&store, "k").await;
    let got = store.get("k").await;
    assert!(
        matches!(&got, Err(S3Error::Corruption { key, .. }) if key == "k"),
        "{got:?}"
    );
    // Rewriting the key repairs it.
    store
        .put("k", bytes("value"), Put::Overwrite)
        .await
        .unwrap();
    assert_eq!(store.get("k").await, Ok(Some(bytes("value"))));
}

pub async fn manifests_are_versioned_write_once(h: &mut impl Harness) {
    let store = h.open().await;
    assert_eq!(latest_manifest(&store, "kv/ns").await, Ok(None));
    put_manifest(&store, "kv/ns", 1, bytes("one"))
        .await
        .unwrap();
    put_manifest(&store, "kv/ns", 2, bytes("two"))
        .await
        .unwrap();
    // A writer that also read version 1 loses the race for version 2.
    assert!(matches!(
        put_manifest(&store, "kv/ns", 2, bytes("other")).await,
        Err(S3Error::AlreadyExists { .. })
    ));
    let latest = |version, body| {
        Ok(Some(Manifest {
            version,
            body: bytes(body),
        }))
    };
    assert_eq!(latest_manifest(&store, "kv/ns").await, latest(2, "two"));
    put_manifest(&store, "kv/ns", 10, bytes("ten"))
        .await
        .unwrap();
    assert_eq!(latest_manifest(&store, "kv/ns").await, latest(10, "ten"));
    // Prefixes are separate, including one that extends another.
    assert_eq!(latest_manifest(&store, "kv/n").await, Ok(None));
    assert_eq!(latest_manifest(&store, "kv/ns2").await, Ok(None));
}
