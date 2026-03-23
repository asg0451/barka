#!/usr/bin/env bash
# Starts multiple barka nodes that share the same S3 prefix so they compete for leadership.
# Usage: ./scripts/multi-node.sh [NUM_NODES]  (default: 3)
# Requires: LocalStack running on :4566.

set -euo pipefail

NUM_NODES="${1:-3}"
BASE_RPC_PORT=9292
BASE_GATEWAY_PORT=9392
S3_ENDPOINT="http://localhost:4566"
S3_PREFIX="multi-node-$(date +%s)"
BIN="./target/debug/barka"

echo "building barka..."
cargo build
echo ""

PGIDS=()

cleanup() {
    echo ""
    echo "--- shutting down all nodes ---"
    for pgid in "${PGIDS[@]}"; do
        kill -- -"$pgid" 2>/dev/null || true
    done
    wait 2>/dev/null
    echo "done."
    exit 0
}
trap cleanup INT TERM

echo "starting $NUM_NODES nodes (s3-prefix=$S3_PREFIX)"
echo ""

for i in $(seq 0 $((NUM_NODES - 1))); do
    rpc_port=$((BASE_RPC_PORT + i))
    gw_port=$((BASE_GATEWAY_PORT + i))

    echo "node $i: rpc=:$rpc_port  gateway=:$gw_port"

    set -m
    RUST_LOG=barka=info RUST_BACKTRACE=1 \
        "$BIN" \
        --node-id "$i" \
        --rpc-port "$rpc_port" \
        --jepsen-gateway-port "$gw_port" \
        --s3-endpoint "$S3_ENDPOINT" \
        --s3-prefix "$S3_PREFIX" \
        2>&1 | sed "s/^/[node-$i] /" &

    PGIDS+=($!)
    set +m
done

echo ""
echo "all nodes started. press ctrl-c to stop."
wait
