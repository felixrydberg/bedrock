//! Path rows and ref keys of the `cdn` dataset (#80 / B-32, `feature-drafts.md` §4 decisions 14 and 17), and the one
//! codec for path-row values, shared with the publish client (B-33) and retrieval (B-35).
//!
//! | Key | Value | Placed by |
//! |---|---|---|
//! | `path/{deployment_id}/{path}` | [`PathRow`] | the normal `(dataset, namespace, key)` hash |
//! | `ref/{sha256}/{deployment_id}` | empty | the artifact's content hash ([`bedrock_lib::placement::hash_of`]) |
//!
//! Namespaces and deployment IDs are opaque to the CDN. A deployment ID is any non-empty string without `/`, so a key
//! has one parse; the path is the rest of the key.

use std::fmt;

use crate::artifact_storage::Hash;

/// The value-encoding version. A row with another version is [`Malformed`].
const VERSION: u8 = 1;
/// Version, SHA-256 and size; the content type follows.
const HEADER: usize = 1 + 32 + 8;

/// Which artifact a URL path serves, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathRow {
    pub sha256: Hash,
    /// The `Content-Type` header value: visible ASCII, spaces and tabs.
    pub content_type: String,
    /// The artifact's size in bytes.
    pub size: u64,
}

/// A key or value that is not in the canonical form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Malformed(pub &'static str);

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed cdn record: {}", self.0)
    }
}

impl std::error::Error for Malformed {}

impl PathRow {
    /// `{version}{sha256}{size as u64 BE}{content type}`.
    pub fn encode(&self) -> Result<Vec<u8>, Malformed> {
        header_value(&self.content_type)?;
        let mut out = Vec::with_capacity(HEADER + self.content_type.len());
        out.push(VERSION);
        out.extend_from_slice(&self.sha256.0);
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(self.content_type.as_bytes());
        Ok(out)
    }

    pub fn decode(value: &[u8]) -> Result<Self, Malformed> {
        if value.len() < HEADER {
            return Err(Malformed("path row too short"));
        }
        if value[0] != VERSION {
            return Err(Malformed("unknown path row version"));
        }
        let content_type = std::str::from_utf8(&value[HEADER..])
            .map_err(|_| Malformed("content type is not UTF-8"))?;
        header_value(content_type)?;
        Ok(PathRow {
            sha256: Hash(value[1..33].try_into().unwrap()),
            size: u64::from_be_bytes(value[33..HEADER].try_into().unwrap()),
            content_type: content_type.to_owned(),
        })
    }
}

/// `path/{deployment_id}/{path}`.
pub fn path_key(deployment_id: &str, path: &str) -> Result<Vec<u8>, Malformed> {
    deployment(deployment_id)?;
    Ok(format!("path/{deployment_id}/{path}").into_bytes())
}

/// `ref/{sha256}/{deployment_id}`: keeps the artifact while it exists.
pub fn ref_key(sha256: &Hash, deployment_id: &str) -> Result<Vec<u8>, Malformed> {
    deployment(deployment_id)?;
    Ok(format!("{}{deployment_id}", ref_prefix(sha256)).into_bytes())
}

/// `ref/{sha256}/`: the prefix of every ref key of one artifact, whatever its deployment.
pub fn ref_prefix(sha256: &Hash) -> String {
    format!("ref/{sha256}/")
}

fn deployment(id: &str) -> Result<(), Malformed> {
    if id.is_empty() || id.contains('/') {
        return Err(Malformed("deployment ID is empty or contains '/'"));
    }
    Ok(())
}

/// A valid HTTP field value, so the Gateway can serve it as `Content-Type`.
fn header_value(value: &str) -> Result<(), Malformed> {
    if value
        .bytes()
        .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
    {
        Ok(())
    } else {
        Err(Malformed("content type is not a valid header value"))
    }
}

#[cfg(test)]
#[path = "../tests/unit/paths.rs"]
mod tests;
