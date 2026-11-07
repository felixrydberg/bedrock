//! The real-binary run (#25 / B-23 Layer 3). It needs release binaries and an S3 service, so plain `cargo test` skips
//! it; `scripts/crash.sh` provides both.
#![cfg(not(madsim))]

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the release binaries and an S3 service: scripts/crash.sh"]
async fn real_binaries_hold_every_invariant() {
    let options = bedrock_crash::Options::from_env();
    let (seed, dir) = (options.seed, options.dir.clone());
    match bedrock_crash::run(options).await {
        Ok(outcome) => println!("CRASH seed={seed} {outcome:?}"),
        Err(violation) => panic!(
            "seed {seed} broke {violation}\nFaults and logs: {}\nRerun: BEDROCK_CRASH_SEED={seed} scripts/crash.sh run",
            dir.display()
        ),
    }
}
