#!/usr/bin/env bash
# Runs the VOPR simulation (#25 / B-23) on Linux or WSL. madsim does not build on Windows, and CI does not run this.
#
#   scripts/vopr.sh quick                    Runs the retained seeds and deliberate bugs: seconds, before every change.
#   scripts/vopr.sh sweep 500 [first-seed]   Runs seeds across all cores and lists failures: minutes, before merging.
#   scripts/vopr.sh soak 3600 8 [first-seed] Runs 8 seeds for 3600 simulated seconds each, for what only shows over time.
#   scripts/vopr.sh replay 42 [more seeds]   Runs each seed in two processes and requires the same result, failures included.
#   scripts/vopr.sh watch [branch]           Forever: tests the branch's latest commit with fresh seeds and writes reports.
#   scripts/vopr.sh profile 42               Lists where one seed spends its CPU time and writes its stacks.
#
# BEDROCK_VOPR_FAULTS=storage,rpc restricts the fault classes. BEDROCK_VOPR_SECONDS sets the simulated run length; a seed
# only reproduces at the commit and length it ran with. VOPR_TIMEOUT caps one seed in wall-clock seconds, 0 for no limit.
# Each failing seed is rerun with logs and a stack trace, and a report goes to VOPR_REPORTS, by default ~/bedrock-vopr/reports.
# Long runs also sample time and memory into VOPR_REPORTS and print whether they climb. The VOPR binary uses mimalloc, so
# memory figures are its, not glibc's: judge growth by its trend.
# Builds go to CARGO_TARGET_DIR, by default ~/bedrock-target-madsim-release: off the Windows drive, and apart from normal builds.
set -euo pipefail

cd "$(dirname "$0")/.."
export RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/bedrock-target-madsim-release}"
export VOPR_REPORTS="${VOPR_REPORTS:-$HOME/bedrock-vopr/reports}"

binary() {
    cargo test --release -p bedrock-sim --test vopr --no-run 2>&1 |
        sed -n 's/.*Executable .*(\(.*\))$/\1/p'
}

# Prints one seed's output; fails if the seed did.
run_seed() {
    BEDROCK_VOPR_SEED="$2" timeout "${VOPR_TIMEOUT:-300}" \
        "$1" --exact seeds_hold_every_invariant --nocapture 2>&1
}

# What a run must reproduce: its trace, or the violation or panic that ended it. Thread ids differ between processes.
result() {
    grep -E -A1 '^VOPR |panicked at' | grep -vE '^(--|note:|test |$)' | sed -E 's/\([0-9]+\)//; s/^.*panicked at/panicked at/'
}

# Reruns a failing seed with logs and a stack trace, and records whether it reproduced.
report() {
    local test="$1" seed="$2" log="$3" again verdict file
    file="$VOPR_REPORTS/$(git rev-parse --short HEAD)-$seed.md"
    again="$(mktemp)"
    RUST_BACKTRACE=1 RUST_LOG=bedrock_lib=info run_seed "$test" "$seed" > "$again" || true
    if [ "$(result < "$log")" = "$(result < "$again")" ]; then verdict=yes; else verdict=NO; fi
    {
        echo "# Seed $seed failed"
        echo
        echo "- $(commit)"
        echo "- Seconds: ${BEDROCK_VOPR_SECONDS:-60}"
        echo "- Faults: ${BEDROCK_VOPR_FAULTS:-all}"
        echo "- Reproduced with logs on: $verdict"
        echo "- $(grep -m1 '^Replay:' "$log" || echo "Replay: scripts/vopr.sh replay $seed")"
        echo
        echo '## Failure'
        echo '```'
        result < "$log"
        echo '```'
        echo '## Stack trace and final state'
        echo '```'
        sed 's/\x1b\[[0-9;]*m//g' "$again" | grep -vE '^\S+ +(INFO|WARN|ERROR|DEBUG) ' | tail -120
        echo '```'
        echo '## Last log lines'
        echo '```'
        sed 's/\x1b\[[0-9;]*m//g' "$again" | grep -E '^\S+ +(WARN|ERROR) ' | tail -40
        echo '```'
    } > "$file"
    rm -f "$again"
    echo "  Report: $file"
}

commit() {
    local dirty=""
    git diff --quiet HEAD -- || dirty=" (with uncommitted changes)"
    echo "commit $(git rev-parse HEAD)$dirty"
}

# Once a minute, for each running seed that has reported progress: real seconds, seed, simulated seconds and memory in MB.
sample() {
    local test="$1" logs="$2" out="$3" started="$SECONDS" pid seed simulated rss
    while sleep 60; do
        for pid in $(pgrep -f "^$test " || true); do
            seed="$(tr '\0' '\n' 2>/dev/null < "/proc/$pid/environ" | sed -n 's/^BEDROCK_VOPR_SEED=//p' || true)"
            simulated="$(sed -n 's/^progress: \([0-9]*\).*/\1/p' "$logs/$seed.log" 2>/dev/null | tail -1 || true)"
            rss="$(ps -o rss= -p "$pid" || true)"
            [ -n "$simulated" ] && [ -n "$rss" ] || continue
            echo "$((SECONDS - started)) $seed $simulated $((rss / 1024))" >> "$out"
        done
    done
}

# Prints whether each seed climbed. Real time also falls as other seeds finish and free their cores.
trend() {
    [ -s "$1" ] || return 0
    echo "Real seconds per simulated hour, first half then second, and memory at the start, middle and end. Samples: $1"
    sort -k2,2n -k1,1n "$1" | awk '
        function show(   m) {
            if (n < 3) return
            m = int((n + 1) / 2)
            printf "  seed %s to %d simulated seconds: %d then %d s/h, %d, %d, %d MB\n", seed, s[n],
                (t[m] - t[1]) * 3600 / (s[m] - s[1]), (t[n] - t[m]) * 3600 / (s[n] - s[m]), r[1], r[m], r[n]
        }
        $2 != seed { show(); seed = $2; n = 0 }
        n == 0 || $3 != s[n] { n++; t[n] = $1; s[n] = $3; r[n] = $4 }
        END { show() }'
}

sweep() {
    local count="$1" first="${2:-1}" test logs metrics sampler started seconds failures
    test="$(binary)"
    logs="$(mktemp -d)"
    mkdir -p "$VOPR_REPORTS"
    metrics="$VOPR_REPORTS/$(git rev-parse --short HEAD)-$first.metrics"
    : > "$metrics"
    started="$SECONDS"
    echo "Logs, with progress for long runs: $logs"
    sample "$test" "$logs" "$metrics" &
    sampler=$!
    export -f run_seed
    seq "$first" "$((first + count - 1))" | xargs -P "$(nproc)" -I{} bash -c \
        'run_seed "$1" "$2" > "$3/$2.log" || touch "$3/$2.failed"' _ "$test" {} "$logs" || true
    kill "$sampler" 2>/dev/null || true
    seconds="$((SECONDS - started))"
    failures="$(find "$logs" -name '*.failed' | wc -l)"
    commit
    (grep -hs '^VOPR seed' "$logs"/*.log || true) | sed 's/.*simulated=//' | awk -v seeds="$count" -v wall="$seconds" '
        /ms$/ { s += $0 / 1000; next }
        { s += $0 }
        END { printf "%d seeds in %d s, %d simulated seconds across the seeds that passed\n", seeds, wall, s }'
    (grep -hs '^VOPR seed' "$logs"/*.log || true) | (grep -oE '[A-Z][A-Za-z]+: [0-9]+' || true) | awk -F': ' '
        { n[$1] += $2 }
        END { for (class in n) printf "  %s: %d injected\n", class, n[class] }' | sort
    trend "$metrics"
    for failed in "$logs"/*.failed; do
        [ -e "$failed" ] || continue
        seed="$(basename "$failed" .failed)"
        echo "seed $seed FAILED: $(grep -m1 -E '^seed [0-9]+ broke' "$logs/$seed.log" || grep -m1 -A1 'panicked at' "$logs/$seed.log" || echo 'timed out or crashed')"
        echo "  $(grep -m1 '^Replay:' "$logs/$seed.log" || echo "Replay: scripts/vopr.sh replay $seed")"
        echo "  Log: $logs/$seed.log"
        report "$test" "$seed" "$logs/$seed.log"
    done
    [ "$failures" -eq 0 ]
}

replay() {
    local test first second status=0
    test="$(binary)"
    commit
    for seed in "$@"; do
        first="$(run_seed "$test" "$seed" | result || true)"
        second="$(run_seed "$test" "$seed" | result || true)"
        if [ -n "$first" ] && [ "$first" = "$second" ]; then
            echo "$first"
        else
            status=1
            echo "seed $seed did not replay:"
            echo "  first:  ${first:-no result}"
            echo "  second: ${second:-no result}"
        fi
    done
    return "$status"
}

# Frame pointers need their own build. Samples come from the kernel, so a profiled seed takes the same path.
profile() {
    local seed="$1" test out
    export CARGO_TARGET_DIR="$CARGO_TARGET_DIR-profile" RUSTFLAGS="$RUSTFLAGS -C force-frame-pointers=yes"
    test="$(binary)"
    mkdir -p "$VOPR_REPORTS"
    out="$VOPR_REPORTS/$(git rev-parse --short HEAD)-$seed.folded"
    BEDROCK_VOPR_SEED="$seed" python3 scripts/vopr-profile.py "$out" "$test" --exact seeds_hold_every_invariant --nocapture 2>&1 |
        grep -E '^VOPR |%  |samples, by|panicked at|perf_event'
    echo "Stacks, for speedscope.app: $out"
}

# A dedicated clone, so it never touches a working checkout.
watch() {
    local branch="${1:-main}" url home first
    url="$(git remote get-url origin)"
    home="${VOPR_WATCH_DIR:-$HOME/bedrock-vopr}"
    mkdir -p "$home/reports"
    [ -d "$home/repo" ] || git clone -q "$url" "$home/repo"
    export VOPR_REPORTS="$home/reports" CARGO_TARGET_DIR="$home/target"
    while true; do
        git -C "$home/repo" fetch -q origin "$branch"
        git -C "$home/repo" checkout -q --detach FETCH_HEAD
        first="$(shuf -i 1-4000000000 -n 1)"
        bash "$home/repo/scripts/vopr.sh" sweep "${VOPR_WATCH_SEEDS:-500}" "$first" || true
        bash "$home/repo/scripts/vopr.sh" soak "${VOPR_WATCH_SECONDS:-3600}" "$(nproc)" "$first" || true
    done
}

case "${1:-}" in
    quick) "$(binary)" ;;
    watch) shift; watch "$@" ;;
    sweep) shift; sweep "$@" ;;
    soak)
        export BEDROCK_VOPR_SECONDS="$2" VOPR_TIMEOUT="${VOPR_TIMEOUT:-0}"
        shift 2
        sweep "$@"
        ;;
    replay) shift; replay "$@" ;;
    profile) shift; profile "$@" ;;
    *) sed -n '2,15p' "$0"; exit 2 ;;
esac
