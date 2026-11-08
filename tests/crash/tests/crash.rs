//! The real-binary run (#25 / B-23 Layer 3). It needs release binaries and an S3 service, so plain `cargo test` skips
//! it; `scripts/crash.sh` provides both.
#![cfg(not(madsim))]

#[test]
#[allow(clippy::disallowed_types)] // separate processes isolate environment-based options
fn fault_selection() {
    for (names, expected) in [
        ("all", Some("[Kill, Disk, Network, S3]")),
        ("none", Some("[]")),
        ("none,kill", Some("[]")),
        ("kill,s3", Some("[Kill, S3]")),
        ("network,typo", None),
        ("all,typo", None),
        ("none,typo", None),
        ("", None),
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fault_selection_child", "--nocapture"])
            .env("BEDROCK_CRASH_OPTIONS_CHILD", "1")
            .env("BEDROCK_CRASH_FAULTS", names)
            .env("BEDROCK_CRASH_SEED", "1")
            .env("BEDROCK_CRASH_SECONDS", "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if let Some(expected) = expected {
            assert!(output.status.success(), "{names}: {stdout}{stderr}");
            assert!(
                stdout.contains(&format!("selected faults: {expected}")),
                "{names}: {stdout}"
            );
        } else {
            assert!(!output.status.success(), "{names}: {stdout}{stderr}");
            assert!(stderr.contains("unknown fault class"), "{names}: {stderr}");
        }
    }
}

#[test]
fn fault_selection_child() {
    if std::env::var_os("BEDROCK_CRASH_OPTIONS_CHILD").is_some() {
        println!(
            "selected faults: {:?}",
            bedrock_crash::Options::from_env().faults
        );
    }
}

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
