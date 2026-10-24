//! Normal builds compile the simulation tests to nothing; this shows up as "ignored" so a green run isn't mistaken for them.
#![cfg(not(madsim))]

#[test]
#[ignore = "simulation tests need RUSTFLAGS=\"--cfg madsim\" on Linux or macOS (WSL on Windows)"]
fn simulation_tests_skipped() {}
