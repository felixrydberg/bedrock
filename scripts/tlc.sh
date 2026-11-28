#!/usr/bin/env bash
# Checks the TLA+ models in docs/spec with TLC (#25 / B-23 Layer 1): every model's config must pass, and every
# deliberate bug must break the invariant it names, checked alone so no other invariant fires first.
#
#   scripts/tlc.sh          Every model, then every bug: many hours since per-point health (#103 / B-62); `handoff` alone
#                           took 2 hours at 4 workers, and `handoff-volume-faulty` did not finish in 2.5 (membership.md).
#                           It leaves out handoff-ranges, which did not finish (its bugs only run with it).
#   scripts/tlc.sh handoff-ranges   That model and its bugs.
#   scripts/tlc.sh bugs     Only the bugs: about 40 minutes, mostly volume-base's previous.
#   scripts/tlc.sh relays   One model and its bugs.
#
# A model with several configs names each one model-variant.cfg (volume-handoff.cfg checks volume.tla).
#
# Needs Java 11 or later. The TLA+ tools jar (v1.8.0 from https://github.com/tlaplus/tlaplus/releases, sha256
# e1f11b75c4b5bc4478ba0ac5cb6ae28e40a01b0016bc33e0aa97da22dfdbcd7f) is checked in beside this script; set TLA2TOOLS to use another. Paths stay relative, so Windows Java works
# from Git Bash too.
set -euo pipefail
cd "$(dirname "$0")/../docs/spec"
jar="${TLA2TOOLS:-../../scripts/tla2tools.jar}"
work=../../target/tlc
rm -rf "$work" && mkdir -p "$work"

# Model, the value of its Bug constant, and the invariant or property that must catch it. TypeOK has none: it only
# catches mistakes in the model itself. handoff's single point cannot split a range, so membership checks
# DirectoryCoverage.
BUGS='
websockets accept-before-commit AcceptedRecorded
websockets forget-roster RecoveredRoster
websockets keep-attachment AttachmentLifetime
websockets reuse-id IncarnationSafe
websockets ungated OutputGated
websockets old-effects OutputGated
websockets unchecked-owner OwnerBound
websockets timeout-prune UnknownRetained
websockets retired-input RetiredInputRefused
websockets replay-input NoReplay
websockets replay-output NoReplay
websockets stale-callback OutputOrdered
websockets event-only EpochAdmission
websockets overflow-all OverflowIsolated
websockets unbounded QueueBound
websockets slow-stalls HealthyNotStalled
websockets-local accept-before-commit AcceptedRecorded
websockets-local forget-roster RecoveredRoster
websockets-local keep-attachment AttachmentLifetime
websockets-local ungated OutputGated
websockets-local old-effects OutputGated
websockets-local unchecked-owner OwnerBound
websockets-local timeout-prune UnknownRetained
websockets-local retired-input RetiredInputRefused
websockets-local replay-input NoReplay
websockets-local replay-output NoReplay
websockets-local slow-stalls HealthyNotStalled
alarms consume CommitOnly
alarms stale CurrentAlarmOnly
alarms early-fallback LateFallbackOnly
alarms released-only CrashRecoverable
alarms committed-only CrashRecoverable
cell buffered DurableCommit
cell uncommitted CommittedApply
cell cas OrderedCAS
cell torn AtomicApply
relays skip OrderedCoverage
relays lease ProtectedBootstrap
relays gc ProtectedTail
relays payload GapFreeApply
relays unfiltered GapFreeApply
relays first-key GapFreeApply
relays unconfirmed ReadyAfterConfirmation
relays report ReportedHolds
relays lost ReadyNotLost
materialized torn AtomicBuffered
materialized partial AtomicDurable
materialized half AtomicBuffered
materialized ack NoLostAck
materialized crash OrderedPositions
materialized cleanup YoungEventsSurvive
materialized retry RetryProtection
materialized event PendingRetained
kv_actor bulk AtomicBuffered
kv_actor torn AtomicCommitted
kv_actor ack NoLostAck
kv_actor claim AtomicCommitted
kv_actor bytes ExactQuota
kv_actor quota ExactQuota
kv_actor retry-drop RetryProtection
kv_actor retry-expiry AtomicCommitted
kv_actor retry-result RetryOriginalResult
kv_actor retry-time RetryOriginalResult
kv_actor staged-retry AtomicBuffered
kv_actor staged-event AtomicBuffered
kv_actor events YoungEventsSurvive
kv_actor cache-regress CacheMonotonic
kv_actor cache-ttl CacheLifetime
kv_actor minimum ReadMinimum
kv_actor expired ReadVisibility
kv_actor-local bulk AtomicBuffered
kv_actor-local torn AtomicCommitted
kv_actor-local ack NoLostAck
kv_actor-local claim AtomicCommitted
kv_actor-local bytes ExactQuota
kv_actor-local quota ExactQuota
kv_actor-local retry-drop RetryProtection
kv_actor-local retry-expiry AtomicCommitted
kv_actor-local retry-result RetryOriginalResult
kv_actor-local retry-time RetryOriginalResult
kv_actor-local staged-retry AtomicBuffered
kv_actor-local staged-event AtomicBuffered
kv_actor-local events YoungEventsSurvive
kv_actor-local cache-regress CacheMonotonic
kv_actor-local cache-ttl CacheLifetime
kv_actor-local minimum ReadMinimum
kv_actor-local expired ReadVisibility
kv_actor-overwrite bulk AtomicBuffered
kv_actor-overwrite torn AtomicCommitted
kv_actor-overwrite ack NoLostAck
kv_actor-overwrite claim AtomicCommitted
kv_actor-overwrite bytes ExactQuota
kv_actor-overwrite quota ExactQuota
kv_actor-overwrite retry-drop RetryProtection
kv_actor-overwrite retry-expiry AtomicCommitted
kv_actor-overwrite retry-result RetryOriginalResult
kv_actor-overwrite retry-time RetryOriginalResult
kv_actor-overwrite staged-retry AtomicBuffered
kv_actor-overwrite staged-event AtomicBuffered
kv_actor-overwrite events YoungEventsSurvive
kv_actor-overwrite cache-regress CacheMonotonic
kv_actor-overwrite cache-ttl CacheLifetime
kv_actor-overwrite minimum ReadMinimum
kv_actor-overwrite expired ReadVisibility
kv_actor-review bulk AtomicBuffered
kv_actor-review torn AtomicCommitted
kv_actor-review ack NoLostAck
kv_actor-review claim AtomicCommitted
kv_actor-review bytes ExactQuota
kv_actor-review quota ExactQuota
kv_actor-review retry-drop RetryProtection
kv_actor-review retry-expiry AtomicCommitted
kv_actor-review retry-result RetryOriginalResult
kv_actor-review retry-time RetryOriginalResult
kv_actor-review staged-retry AtomicBuffered
kv_actor-review staged-event AtomicBuffered
kv_actor-review events YoungEventsSurvive
kv_actor-review cache-regress CacheMonotonic
kv_actor-review cache-ttl CacheLifetime
kv_actor-review minimum ReadMinimum
kv_actor-review expired ReadVisibility
kv_admission admin-bypass PurgeAuthorized
kv_admission stale-floor KnownDeletionFences
publisher unconditional NoLostUpdate
publisher unconditional NoRegression
publisher ahead ViewsLag
membership minority OnePublisher
membership clock ClockBound
membership gap DirectoryCoverage
membership draining NewOwnersPublished
membership drain-record NewOwnersPublished
membership early-completion DrainCompletionAfterHandoff
membership sick NoUnhealthyRouting
membership early-drop NoDropBeforeReady
membership early-removal NoEarlyRemoval
membership minority-write Freeze
membership merge-unequal NoDropBeforeReady
membership range-only NoUnhealthyRouting
service_isolation global-freeze HealthyServiceEnabled
service_isolation stale-clock ClockBound
service_isolation stale-clock NoEarlyRemoval
service_isolation cas OrderedCAS
service_isolation unreadable-write ServiceFreeze
service_isolation minority-write Freeze
handoff unready ReadyHasData
handoff view ServesHeld
handoff keep OwnedOrLeaving
handoff absent NewOwnersPublished
handoff early-drop NoDropBeforeReady
handoff any-ready KeepsHealthyHolder
handoff minority-write Freeze
handoff-ranges start-only KeepsHealthyHolder
handoff-ranges unverified DamageRefused
handoff-ranges-restart forget DamageRefused
handoff-ranges-restart unverified DamageRefused
handoff-volume early-report ReadyCopied
handoff-volume no-gen ReadyCopied
handoff-volume-split early-report ReadyCopied
handoff-volume-split no-gen ReadyCopied
handoff-volume-faulty early-report ReadyCopied
handoff-volume-faulty no-gen ReadyCopied
follows untagged NoFalseGap
follows ahead HeldBeforeNext
merge lower GapFreeApply
merge higher GapFreeApply
merge drift OneCursor
volume-base appoint NoLostAck
volume-base previous NoLostAck
volume-base flush S3Committed
volume-handoff populating NoLostAck
volume-handoff unsealed NoLostAck
volume-wipe self-epoch NoLostAck
volume-wipe early NoLostAck
volume-topology new-only NoLostAck
volume-topology new-only OneWriter
volume-topology new-only NoStaleAppend
volume-truncate learn S3Committed
volume-truncate truncate TruncatedInS3
volume-truncate skip-keep TruncatedInS3
volume-truncate skip-any TruncatedInS3
volume-delete-wipe undelete DeletedNeverServes
volume-delete-handoff undelete DeletedNeverServes
volume-delete-truncate undelete DeletedNeverServes
volume_cold-split no-retire NoLostFlush
volume_cold-split no-cas NoLostFlush
volume_cold-split prefix-truncate NoLostFlush
volume_cold-move no-adopt Consistent
volume_cold-move prefix-truncate NoLostFlush
cells unchecked ListedLive
cells sticky ListedLive
cells unscoped ServedInTarget
cells unattributed TablesOfTarget
volume_dc no-import NoLostAck
volume_dc early-import NoLostAck
volume_dc any-move NoLostAck
volume_dc serve-moved OneWriter
volume_dc flush S3Committed
volume_dc reach NewNeverServes
volume_dc-small no-import NoLostAck
volume_dc-small early-import NoLostAck
volume_dc-small any-move NoLostAck
volume_dc-small serve-moved OneWriter
volume_dc-small flush S3Committed
volume_dc-small reach NewNeverServes
'

tlc() {
    local name="$1" config="$2" model="${3%%-*}"
    # TLC_WORKERS and TLC_HEAP override the defaults; progress goes to $work/$name.log while TLC runs.
    mkdir -p "$work"
    java -Xmx"${TLC_HEAP:-4g}" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-2}" -noGenerateSpecTE \
        -metadir "$work/$name" -config "$config" "$model.tla" 2>&1 | tee "$work/$name.log"
}

failed=0
pass() {
    local model="$1" started=$SECONDS out
    out="$(tlc "$model" "$model.cfg" "$model" || true)"
    if grep -q "No error has been found" <<< "$out"; then
        echo "PASS $model: $(grep -m1 "distinct states found, 0 states left on queue" <<< "$out")" \
            "depth $(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' <<< "$out")" \
            "($((SECONDS - started)) s)"
    else
        echo "FAIL $model"
        grep -E "Error|violated" <<< "$out" | head -5
        failed=1
    fi
}

bug() {
    local model="$1" value="$2" invariant="$3" kind config out
    kind="$(grep -E "^(INVARIANTS|PROPERTIES) .*\b$invariant\b" "$model.cfg" | cut -d' ' -f1 || true)"
    # A reachability check is not among the model's invariants: it is an invariant TLC must find violated.
    kind="${kind:-INVARIANTS}"
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

models="websockets websockets-local alarms relays cells follows merge materialized kv_actor kv_actor-local kv_actor-overwrite kv_actor-review kv_admission publisher membership service_isolation handoff handoff-ranges-restart handoff-volume handoff-volume-faulty cell volume-base volume-handoff volume-wipe volume-topology volume-truncate volume-delete-wipe volume-delete-handoff volume-delete-truncate volume_cold-split volume_cold-move volume_dc volume_dc-small"
case "${1:-}" in
    bugs) ;;
    "") for model in $models; do pass "$model"; done ;;
    *) models="$1"; pass "$1" ;;
esac
while read -r model value invariant; do
    [ -n "$model" ] && [[ " $models " == *" $model "* ]] && bug "$model" "$value" "$invariant"
done <<< "$BUGS"
exit "$failed"
