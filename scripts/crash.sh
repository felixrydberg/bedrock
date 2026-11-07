#!/usr/bin/env bash
# Runs the real-binary crash and fault checks (#25 / B-23 Layer 3) on Linux or WSL: the five release binaries, RocksDB
# and SeaweedFS as S3, killed and checked by tests/crash.
#
#   scripts/crash.sh run          One run of BEDROCK_CRASH_SECONDS (default 60) of faults, then convergence and checks.
#   scripts/crash.sh sweep 10     Ten runs with consecutive seeds; lists the failures.
#
# BEDROCK_CRASH_SEED picks the workload and faults; a run is not deterministic, so a failing seed may not repeat.
# BEDROCK_CRASH_FAULTS=none runs without faults. Each run's configs, data, logs and faults.log stay in
# BEDROCK_CRASH_DIR, by default /tmp/bedrock-crash-<seed>. Needs Docker for SeaweedFS (container bedrock-crash-s3).
set -euo pipefail

cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/bedrock-target-crash}"
export BEDROCK_CRASH_BINS="$CARGO_TARGET_DIR/release"

build() {
    cargo build --release -p bedrock-api -p bedrock-relay -p bedrock-quicksilver -p bedrock-kv -p bedrock-cdn
    cargo test --release -p bedrock-crash --test crash --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p'
}

s3() {
    if ! docker inspect -f '{{.State.Running}}' bedrock-crash-s3 2>/dev/null | grep -q true; then
        docker rm -f bedrock-crash-s3 >/dev/null 2>&1 || true
        docker run -d --name bedrock-crash-s3 -p 8333:8333 chrislusf/seaweedfs:4.01 server -s3 -dir=/data > /dev/null
    fi
    for _ in $(seq 1 60); do
        curl -s -o /dev/null http://127.0.0.1:8333/ && return 0
        sleep 1
    done
    echo "SeaweedFS did not start on :8333" >&2
    return 1
}

run() {
    "$1" --ignored --exact real_binaries_hold_every_invariant --nocapture 2>&1 | grep -E '^CRASH |broke|Faults and logs|Rerun|panicked'
}

case "${1:-}" in
    run)
        test="$(build | tail -1)"
        s3
        run "$test"
        ;;
    sweep)
        test="$(build | tail -1)"
        s3
        first="${3:-$(date +%s)}"
        failed=0
        for seed in $(seq "$first" "$((first + $2 - 1))"); do
            BEDROCK_CRASH_SEED="$seed" run "$test" || failed=$((failed + 1))
        done
        echo "$2 runs, $failed failed"
        [ "$failed" -eq 0 ]
        ;;
    *) sed -n '2,10p' "$0"; exit 2 ;;
esac
