//! Checkers shared by the simulation tests. They compile in every build, so normal `cargo test` proves they can fire and
//! the later real-binary layer can reuse them (#25 / B-23).

pub mod history;
#[cfg(madsim)]
pub mod rpc;
