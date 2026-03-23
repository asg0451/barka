#!/usr/bin/env bash
# scripts/loadtest.sh — medium-load produce stress test for barka
#
# Sends random produce traffic to already-running produce-nodes via barka-cli.
# Assumes produce-nodes are already up (e.g. via multi-node.sh).
#
# N workers are round-robin assigned to M topic-partitions (N >= M recommended).
# Each worker pipes batches of records into barka-cli produce via stdin.
#
# Requires: cargo build artifacts (target/debug/barka-cli)

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
PARALLELISM=10
RATE=10000        # target total records/sec across all workers
S3_PREFIX=""
LE_PREFIX=""
TOPICS="default:4"
BATCH=10000        # records per barka-cli invocation
DURATION=60
S3_ENDPOINT="http://localhost:4566"
S3_BUCKET="barka"

usage() {
    cat <<'EOF'
Usage: scripts/loadtest.sh [OPTIONS]

Sends random produce traffic to already-running produce-nodes via barka-cli.
N workers are round-robin assigned to M topic-partitions.

Options:
  --parallelism N              Concurrent workers (default: 10)
  --rate N                     Target total records/sec (default: 10000)
  --s3-prefix PREFIX           S3 key prefix (default: from /tmp/barka-prefix.txt)
  --leader-election-prefix P   Leader election prefix (default: same as --s3-prefix)
  --s3-endpoint URL            S3 endpoint (default: http://localhost:4566)
  --s3-bucket BUCKET           S3 bucket (default: barka)
  --batch N                    Records per barka-cli invocation (default: 10000)
  --topics SPEC                TOPIC:PARTITIONS[,...] (default: default:4)
  --duration SECS              Test duration in seconds (default: 60)
  -h, --help                   Show this help
EOF
    exit 0
}

# ── parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --parallelism)              PARALLELISM="$2"; shift 2 ;;
        --rate)                     RATE="$2"; shift 2 ;;
        --s3-prefix)                S3_PREFIX="$2"; shift 2 ;;
        --leader-election-prefix)   LE_PREFIX="$2"; shift 2 ;;
        --s3-endpoint)              S3_ENDPOINT="$2"; shift 2 ;;
        --s3-bucket)                S3_BUCKET="$2"; shift 2 ;;
        --batch)                    BATCH="$2"; shift 2 ;;
        --topics)                   TOPICS="$2"; shift 2 ;;
        --duration)                 DURATION="$2"; shift 2 ;;
        -h|--help)                  usage ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$S3_PREFIX" ]]; then
    if [[ -f /tmp/barka-prefix.txt ]]; then
        S3_PREFIX=$(cat /tmp/barka-prefix.txt)
    else
        S3_PREFIX="loadtest-$(date +%s)"
    fi
fi
[[ -z "$LE_PREFIX" ]] && LE_PREFIX="$S3_PREFIX"

# ── expand topic specs into flat partition list ───────────────────────────────
# e.g. "load-0:2,load-1:3" -> ("load-0 0" "load-0 1" "load-1 0" "load-1 1" "load-1 2")
IFS=',' read -ra TOPIC_SPECS <<< "$TOPICS"
ALL_PARTITIONS=()
for spec in "${TOPIC_SPECS[@]}"; do
    topic="${spec%%:*}"
    nparts="${spec##*:}"
    for p in $(seq 0 $((nparts - 1))); do
        ALL_PARTITIONS+=("$topic $p")
    done
done
NUM_PARTITIONS=${#ALL_PARTITIONS[@]}

# Per-worker sleep between calls to approximate target rate
# Each worker does RATE/PARALLELISM rec/s, at BATCH rec/call -> delay = BATCH*PARALLELISM/RATE
# RATE=0 means no sleeping (max throughput)
if [[ "$RATE" -eq 0 ]]; then
    DELAY=0
else
    DELAY=$(awk "BEGIN {printf \"%.6f\", ($BATCH * $PARALLELISM) / $RATE}")
fi

CLI=./target/debug/barka-cli
STATS_DIR=$(mktemp -d)
WORKER_PIDS=()

# ── cleanup ───────────────────────────────────────────────────────────────────
report_stats() {
    local p_ok=0 errs=0
    for f in "$STATS_DIR"/worker-*.stats; do
        [[ -f "$f" ]] || continue
        while IFS='=' read -r key val; do
            case "$key" in
                produce_ok) p_ok=$((p_ok + val)) ;;
                errors)     errs=$((errs + val)) ;;
            esac
        done < "$f"
    done
    local total=$((p_ok + errs))

    echo ""
    echo "=== results ==="
    echo "  produced: $p_ok records"
    echo "  errors:   $errs records"
    echo "  total:    $total records"
    if [[ $DURATION -gt 0 ]] && [[ $total -gt 0 ]]; then
        awk "BEGIN {printf \"  avg rate: %.1f records/sec\n\", $total / $DURATION}"
    fi
}

cleanup() {
    trap - INT TERM EXIT
    echo ""
    echo "--- shutting down ---"

    for pid in "${WORKER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null

    report_stats

    rm -rf "$STATS_DIR"
    echo ""
    echo "done."
}
trap cleanup INT TERM EXIT

# ── print config ──────────────────────────────────────────────────────────────
echo "config:"
echo "  parallelism:  $PARALLELISM workers -> $NUM_PARTITIONS partitions"
echo "  target rate:  $RATE records/sec"
echo "  batch size:   $BATCH records/call"
echo "  duration:     ${DURATION}s"
echo "  topics:       $TOPICS"
echo "  s3-prefix:    $S3_PREFIX"
echo "  le-prefix:    $LE_PREFIX"
echo "  s3-endpoint:  $S3_ENDPOINT"
echo "  worker delay: ${DELAY}s"
echo ""

# ── worker ────────────────────────────────────────────────────────────────────
worker() {
    local id=$1
    local end_time=$2
    local topic=$3
    local part=$4
    local p_ok=0 errs=0
    local last_report=$SECONDS
    local recs_since_report=0

    while true; do
        local now
        now=$(date +%s)
        [[ $now -lt $end_time ]] || break

        # Generate BATCH lines and pipe to barka-cli via stdin
        if seq 1 "$BATCH" \
            | awk -v id="$id" -v s="$SECONDS" '{printf "w%s-%s-%d\n", id, s, NR+int(rand()*100000)}' \
            | $CLI \
                --s3-endpoint "$S3_ENDPOINT" \
                --s3-bucket "$S3_BUCKET" \
                --leader-election-prefix "$LE_PREFIX" \
                produce -t "$topic" -n "$part" \
                > /dev/null 2>&1; then
            p_ok=$((p_ok + BATCH))
        else
            errs=$((errs + BATCH))
        fi

        recs_since_report=$((recs_since_report + BATCH))

        # Progress report every 10s
        if [[ $((SECONDS - last_report)) -ge 10 ]]; then
            local elapsed=$((SECONDS - last_report))
            local wrate=0
            [[ $elapsed -gt 0 ]] && wrate=$((recs_since_report / elapsed))
            echo "  [w$id $topic/$part] ok=$p_ok err=$errs (~${wrate} rec/s)"
            last_report=$SECONDS
            recs_since_report=0
        fi

        [[ "$DELAY" != "0" ]] && sleep "$DELAY"
    done

    echo "  [w$id $topic/$part] finished: ok=$p_ok err=$errs"
    printf 'produce_ok=%d\nerrors=%d\n' "$p_ok" "$errs" \
        > "$STATS_DIR/worker-${id}.stats"
}

# ── launch workers (round-robin across partitions) ────────────────────────────
END_TIME=$(($(date +%s) + DURATION))

echo "launching $PARALLELISM workers for ${DURATION}s..."

for i in $(seq 0 $((PARALLELISM - 1))); do
    pidx=$((i % NUM_PARTITIONS))
    tp="${ALL_PARTITIONS[$pidx]}"
    topic="${tp%% *}"
    part="${tp##* }"
    echo "  worker $i -> $topic/$part"
    worker "$i" "$END_TIME" "$topic" "$part" &
    WORKER_PIDS+=($!)
done

echo ""

# Wait for all workers to finish; cleanup fires on EXIT
for pid in "${WORKER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
