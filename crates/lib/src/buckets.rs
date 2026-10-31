//! Stable bucket identity; ownership and placement are separate (#19 / B-16).
use sha2::{Digest, Sha256};

pub const COUNT: u32 = 1 << 20;

pub fn bucket_of(dataset: &str, namespace: &str, key: &[u8]) -> u32 {
    let mut hash = Sha256::new();
    for part in [dataset.as_bytes(), namespace.as_bytes(), key] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    let digest = hash.finalize();
    u32::from_be_bytes(digest[..4].try_into().unwrap()) & (COUNT - 1)
}

#[cfg(test)]
#[path = "../tests/unit/buckets.rs"]
mod tests;
