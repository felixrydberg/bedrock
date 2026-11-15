//! Source checks for what makes a seed replay its trace (#25 / B-23).
//!
//! `madsim-tokio` re-exports real tokio's `select!`. Without `biased;` it starts at a branch drawn from tokio's thread
//! RNG, whose seed mixes in a process-wide counter (`tokio::loom::std::rand::seed`), not the simulation seed. Two runs
//! of one seed then poll ready branches in different orders, and the membership replication scenario changed its trace
//! about once in fifteen runs of its test binary. Every `select!` the simulation can reach is therefore `biased;`.

use std::path::{Path, PathBuf};

/// Every `.rs` file under `dir`, in order.
#[allow(clippy::disallowed_methods)] // a source check reads the repository, not simulated state
fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

/// The `select!` invocations in `text` whose body does not open with `biased;`, as line numbers.
fn unbiased(text: &str) -> Vec<usize> {
    let mut lines = Vec::new();
    for (at, _) in text.match_indices("select!") {
        let rest = text[at + "select!".len()..].trim_start();
        let Some(body) = rest.strip_prefix('{') else {
            continue;
        };
        if !body.trim_start().starts_with("biased;") {
            lines.push(text[..at].lines().count());
        }
    }
    lines
}

#[test]
#[allow(clippy::disallowed_methods)] // a source check reads the repository, not simulated state
fn every_select_the_simulation_reaches_is_biased() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for crate_dir in std::fs::read_dir(root.join("crates")).unwrap() {
        let src = crate_dir.unwrap().path().join("src");
        if src.is_dir() {
            sources(&src, &mut files);
        }
    }
    sources(&root.join("tests/sim/src"), &mut files);
    sources(&root.join("tests/sim/tests"), &mut files);
    let this = Path::new(file!()).file_name().unwrap();
    let mut found = Vec::new();
    for file in files.iter().filter(|f| f.file_name() != Some(this)) {
        let text = std::fs::read_to_string(file).unwrap();
        for line in unbiased(&text) {
            found.push(format!("{}:{line}", file.display()));
        }
    }
    assert!(
        found.is_empty(),
        "tokio::select! without `biased;` draws its branch order outside the simulation seed: {found:#?}"
    );
}

#[test]
fn the_check_finds_an_unbiased_select() {
    assert_eq!(unbiased("x\ntokio::select! {\n  a => {},\n}"), [2]);
    assert!(unbiased("tokio::select! {biased;\n a => {},\n}").is_empty());
    assert!(unbiased("tokio::select!{\n  biased;\n a => {},\n}").is_empty());
}
