#!/usr/bin/env bash
# Checks the TLA+ models in docs/spec with TLC (#25 / B-23 Layer 1): every model's config must pass, and every
# deliberate bug must break the invariant it names, checked alone so no other invariant fires first.
#
#   scripts/tlc.sh          Every model, then every bug: about 10 minutes, mostly handoff.
#   scripts/tlc.sh bugs     Only the bugs: under a minute.
#   scripts/tlc.sh relays   One model and its bugs.
#
# Needs Java 11 or later and the TLA+ tools jar from https://github.com/tlaplus/tlaplus/releases, at TLA2TOOLS or by
# default .codex-tmp/tla2tools.jar. Paths stay relative, so Windows Java works from Git Bash too.
set -euo pipefail
cd "$(dirname "$0")/../docs/spec"
jar="${TLA2TOOLS:-../../.codex-tmp/tla2tools.jar}"
work=../../target/tlc
rm -rf "$work" && mkdir -p "$work"

# Model, the value of its Bug constant, and the invariant or property that must catch it. TypeOK has none: it only
# catches mistakes in the model itself. handoff's single point cannot split a range, so membership checks
# DirectoryCoverage.
BUGS='
relays skip OrderedCoverage
relays lease ProtectedBootstrap
relays gc ProtectedTail
relays payload GapFreeApply
relays unconfirmed ReadyAfterConfirmation
relays report ReportedHolds
relays lost ReadyNotLost
materialized torn AtomicBuffered
materialized partial AtomicDurable
materialized ack NoLostAck
materialized crash OrderedPositions
materialized cleanup YoungEventsSurvive
materialized retry RetryProtection
materialized event PendingRetained
publisher unconditional NoLostUpdate
publisher unconditional NoRegression
publisher ahead ViewsLag
membership minority OnePublisher
membership clock ClockBound
membership gap DirectoryCoverage
membership draining NewOwnersPublished
membership sick NoUnhealthyRouting
membership early-drop NoDropBeforeReady
membership early-removal NoEarlyRemoval
membership minority-write Freeze
handoff unready ReadyHasData
handoff view ServesHeld
handoff keep OwnedOrLeaving
handoff absent NewOwnersPublished
handoff early-drop NoDropBeforeReady
handoff any-ready KeepsHealthyHolder
handoff minority-write Freeze
follows untagged NoFalseGap
follows ahead HeldBeforeNext
'

tlc() {
    local name="$1" config="$2" model="$3"
    java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -noGenerateSpecTE -metadir "$work/$name" \
        -config "$config" "$model.tla" 2>&1
}

failed=0
pass() {
    local model="$1" started=$SECONDS out
    out="$(tlc "$model" "$model.cfg" "$model" || true)"
    if grep -q "No error has been found" <<< "$out"; then
        echo "PASS $model: $(grep -m1 "0 states left on queue" <<< "$out") ($((SECONDS - started)) s)"
    else
        echo "FAIL $model"
        grep -E "Error|violated" <<< "$out" | head -5
        failed=1
    fi
}

bug() {
    local model="$1" value="$2" invariant="$3" kind config out
    kind="$(grep -E "^(INVARIANTS|PROPERTIES) .*\b$invariant\b" "$model.cfg" | cut -d' ' -f1)"
    config="$work/$model-$value-$invariant.cfg"
    sed -e "s/Bug = \"none\"/Bug = \"$value\"/" -e '/^INVARIANTS /d' -e '/^PROPERTIES /d' "$model.cfg" > "$config"
    echo "$kind $invariant" >> "$config"
    out="$(tlc "$model-$value-$invariant" "$config" "$model" || true)"
    if grep -qE "(Invariant|property) $invariant is violated" <<< "$out"; then
        echo "PASS $model Bug=$value breaks $invariant"
    else
        echo "FAIL $model Bug=$value did not break $invariant"
        grep -E "Error|violated|distinct states found" <<< "$out" | head -5
        failed=1
    fi
}

models="relays follows materialized publisher membership handoff"
case "${1:-}" in
    bugs) ;;
    "") for model in $models; do pass "$model"; done ;;
    *) models="$1"; pass "$1" ;;
esac
while read -r model value invariant; do
    [ -n "$model" ] && [[ " $models " == *" $model "* ]] && bug "$model" "$value" "$invariant"
done <<< "$BUGS"
exit "$failed"
