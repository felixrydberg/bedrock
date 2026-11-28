//! Path rows and ref keys of the `cdn` dataset (#80 / B-32). They and the publish client (#81 / B-33) live in
//! [`bedrock_lib::cdn`], which the deploy control plane also links.

pub use bedrock_lib::cdn::{Malformed, PathRow, path_key, ref_key, ref_prefix};
