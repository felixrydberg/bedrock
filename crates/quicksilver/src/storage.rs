//! Quicksilver/Auth keyspaces inside a dataset's engine instance (`kv.md` §7). Eviction lands here later (B-28).
//!
//! Namespace IDs contain `/` (`customer-123/store-A`), so a NUL byte ends the namespace instead: with `/` the prefix
//! of namespace `a` would also cover every key of namespace `a/b`. Namespace IDs must not contain NUL. L2 keys use the
//! shared hash-first layout ([`bedrock_lib::storage::l2_key`]) the relay client writes, so an L2 hash range is one
//! contiguous span (#22 / B-21).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyspace {
    /// `l1/{ns}\0{key}`
    L1,
    /// `l2/{8-byte big-endian hash}/{ns}\0{key}`
    L2 { hash: u64 },
    /// `meta/{ns}\0...`
    Meta,
}

impl Keyspace {
    /// Returns a prefix for `namespace` in this keyspace; for L2, it selects only keys with the chosen hash. Use it
    /// with `KeyRange::prefix`.
    ///
    /// # Panics
    /// Panics if `namespace` contains NUL.
    pub fn prefix(self, namespace: &str) -> Vec<u8> {
        assert!(!namespace.contains('\0'), "namespace ID contains NUL");
        let mut out = match self {
            Keyspace::L1 => b"l1/".to_vec(),
            Keyspace::L2 { hash } => return bedrock_lib::storage::l2_key(hash, namespace, b""),
            Keyspace::Meta => b"meta/".to_vec(),
        };
        out.extend_from_slice(namespace.as_bytes());
        out.push(0);
        out
    }

    pub fn key(self, namespace: &str, key: &[u8]) -> Vec<u8> {
        [self.prefix(namespace), key.to_vec()].concat()
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
