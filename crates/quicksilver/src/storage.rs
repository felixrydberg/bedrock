//! Quicksilver/Auth keyspaces inside a dataset's engine instance (`kv.md` §7). Eviction lands here later (B-28).
//!
//! Namespace IDs contain `/` (`customer-123/store-A`), so a NUL byte ends the namespace instead: with `/` the prefix
//! of namespace `a` would also cover every key of namespace `a/b`. Namespace IDs must not contain NUL. L2 keys start
//! with the big-endian `buckets::hash_of`, so an L2 hash range (#19 / B-16) is one contiguous span.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyspace {
    /// `l1/{ns}\0{key}`
    L1,
    /// `l2/{bucket}/{ns}\0{key}`
    L2 { hash: u64 },
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
            Keyspace::L2 { hash } => [b"l2/".as_slice(), &hash.to_be_bytes(), b"/"].concat(),
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
