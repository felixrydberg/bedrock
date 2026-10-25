//! In-memory [`ObjectStore`] for tests and for simulation builds, where the S3 client is compiled out (B-07).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::{ObjectStore, Put, S3Error, S3Result, hex, sha256, verify};

/// Clones share their objects, like nodes sharing one bucket.
#[derive(Clone, Default)]
pub struct MemoryStore {
    objects: Arc<Mutex<BTreeMap<String, Object>>>,
}

struct Object {
    body: Vec<u8>,
    /// Hex SHA-256 the body was stored with.
    sha256: String,
}

impl MemoryStore {
    /// Fault injection: damages the stored body but keeps its checksum, so reads of `key` fail verification until it
    /// is written again.
    pub fn corrupt(&self, key: &str) {
        let mut objects = self.lock();
        let body = &mut objects.get_mut(key).expect("corrupt a stored key").body;
        match body.first_mut() {
            Some(byte) => *byte ^= 1,
            None => body.push(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Object>> {
        self.objects.lock().unwrap()
    }
}

impl ObjectStore for MemoryStore {
    async fn get(&self, key: &str) -> S3Result<Option<Vec<u8>>> {
        let objects = self.lock();
        let Some(object) = objects.get(key) else {
            return Ok(None);
        };
        verify(key, object.body.clone(), Some(&object.sha256)).map(Some)
    }

    async fn put(&self, key: &str, body: Vec<u8>, mode: Put) -> S3Result<()> {
        let sha256 = hex(&sha256(&body));
        let mut objects = self.lock();
        if mode == Put::Create
            && let Some(existing) = objects.get(key)
        {
            return if existing.body == body {
                verify(key, body, Some(&existing.sha256)).map(|_| ())
            } else {
                Err(S3Error::AlreadyExists { key: key.into() })
            };
        }
        objects.insert(key.into(), Object { body, sha256 });
        Ok(())
    }

    async fn list(&self, prefix: &str) -> S3Result<Vec<String>> {
        Ok(self
            .lock()
            .range(prefix.to_owned()..)
            .map(|(key, _)| key)
            .take_while(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> S3Result<()> {
        self.lock().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::contract::{Harness, s3_contract};

    struct Memory;

    impl Harness for Memory {
        type Store = MemoryStore;

        async fn open(&mut self) -> MemoryStore {
            MemoryStore::default()
        }

        async fn corrupt(&mut self, store: &MemoryStore, key: &str) {
            store.corrupt(key);
        }
    }

    s3_contract!(Memory);

    #[tokio::test]
    async fn create_retry_rejects_invalid_checksum() {
        let store = MemoryStore::default();
        let body = b"value".to_vec();
        store.put("k", body.clone(), Put::Create).await.unwrap();
        store.lock().get_mut("k").unwrap().sha256 = "invalid".into();

        let got = store.put("k", body, Put::Create).await;
        assert!(matches!(got, Err(S3Error::Corruption { .. })), "{got:?}");
        assert!(matches!(
            store.get("k").await,
            Err(S3Error::Corruption { .. })
        ));
    }
}
