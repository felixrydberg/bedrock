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
mod tests {
    use super::*;
    use bedrock_lib::storage::KeyRange;

    #[test]
    fn rejects_nul_in_namespace_for_every_keyspace() {
        for ks in [Keyspace::L1, Keyspace::L2 { bucket: 7 }, Keyspace::Meta] {
            assert!(std::panic::catch_unwind(|| ks.prefix("ns\0other")).is_err());
        }
    }

    #[test]
    fn namespace_prefix_excludes_nested_namespace() {
        for ks in [Keyspace::L1, Keyspace::L2 { bucket: 7 }, Keyspace::Meta] {
            let a = KeyRange::prefix(ks.prefix("customer-1"));
            assert!(a.contains(&ks.key("customer-1", b"store-A/x")));
            assert!(!a.contains(&ks.key("customer-1/store-A", b"x")));
        }
    }

    #[test]
    fn keyspaces_and_buckets_are_disjoint_and_ordered() {
        let l1 = KeyRange::prefix(Keyspace::L1.prefix("ns"));
        assert!(!l1.contains(&Keyspace::Meta.key("ns", b"k")));
        assert!(!l1.contains(&Keyspace::L2 { bucket: 0 }.key("ns", b"k")));
        assert!(
            Keyspace::L2 { bucket: 9 }.key("ns", b"") < Keyspace::L2 { bucket: 10 }.key("ns", b"")
        );
        assert!(
            !KeyRange::prefix(Keyspace::L2 { bucket: 1 }.prefix("ns"))
                .contains(&Keyspace::L2 { bucket: 256 }.key("ns", b""))
        );
    }
}
