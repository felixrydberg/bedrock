//! Stable key hashing. L2 ownership is relay-assigned ranges of [`hash_of`] (#19 / B-16, `placement`); relay
//! selectors still filter by [`bucket_of`] until #22 / B-21 and #41 / B-12b change them.
use sha2::{Digest, Sha256};

pub const COUNT: u32 = 1 << 20;

/// A key's position in the 64-bit L2 hash space (#18 / B-15); customer KV ranks its readers by it.
pub fn hash_of(dataset: &str, namespace: &str, key: &[u8]) -> u64 {
    let mut hash = Sha256::new();
    for part in [dataset.as_bytes(), namespace.as_bytes(), key] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    u64::from_be_bytes(hash.finalize()[..8].try_into().unwrap())
}

pub fn bucket_of(dataset: &str, namespace: &str, key: &[u8]) -> u32 {
    (hash_of(dataset, namespace, key) >> 32) as u32 & (COUNT - 1)
}

#[cfg(test)]
#[path = "../tests/unit/buckets.rs"]
mod tests;
