//! KV keyspaces inside a dataset's engine instance (`kv.md` §7). Eviction lands here later (B-28).
//!
//! Namespace IDs contain `/` (`customer-123/store-A`), so a NUL byte ends the namespace instead: with `/` the prefix
//! of namespace `a` would also cover every key of namespace `a/b`. Namespace IDs must not contain NUL. Buckets are
//! big-endian `u32`, so keys sort by bucket.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyspace {
    /// `l1/{ns}\0{key}`
    L1,
    /// `l2/{bucket}/{ns}\0{key}`
    L2 { bucket: u32 },
    /// `meta/{ns}\0...`
    Meta,
}

impl Keyspace {
    /// Every key of `namespace` in this keyspace starts with this; use it with `KeyRange::prefix`.
    ///
    /// # Panics
    /// Panics if `namespace` contains NUL.
    pub fn prefix(self, namespace: &str) -> Vec<u8> {
        assert!(!namespace.contains('\0'), "namespace ID contains NUL");
        let mut out = match self {
            Keyspace::L1 => b"l1/".to_vec(),
            Keyspace::L2 { bucket } => [b"l2/".as_slice(), &bucket.to_be_bytes(), b"/"].concat(),
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
