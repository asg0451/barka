#!/usr/bin/env bash
# Starts multiple barka produce-nodes (competing for leadership) and one consume-node.
# Usage: ./scripts/multi-node.sh [NUM_NODES]  (default: 3)
# Requires: LocalStack running on :4566.

set -euo pipefail

NUM_NODES="${1:-3}"
BASE_PRODUCE_PORT=9292
CONSUME_PORT=9392
S3_ENDPOINT="http://localhost:4566"
S3_PREFIX="multi-node-$(date +%s)"

echo "building barka..."
cargo build
echo ""

cleanup() {
    echo ""
    echo "--- shutting down all nodes ---"
    trap - INT TERM
    pkill -P $$ 2>/dev/null || true
    wait 2>/dev/null
    echo "done."
    exit 0
}
trap cleanup INT TERM

echo "starting $NUM_NODES produce-nodes + 1 consume-node (s3-prefix=$S3_PREFIX)"
echo ""

# tracing default target is the binary crate (e.g. produce_node), not the library `barka`.
RUST_LOG_BINARIES="barka=info,produce_node=info,consume_node=info"

for i in $(seq 0 $((NUM_NODES - 1))); do
    rpc_port=$((BASE_PRODUCE_PORT + i))
    echo "produce-node $i: rpc=:$rpc_port"

    RUST_LOG="$RUST_LOG_BINARIES" RUST_BACKTRACE=1 \
        ./target/debug/produce-node \
        --node-id "$i" \
        --rpc-port "$rpc_port" \
        --s3-endpoint "$S3_ENDPOINT" \
        --s3-prefix "$S3_PREFIX" \
        --leader-election-prefix "$S3_PREFIX" \
        2>&1 | sed "s/^/[node-$i] /" &
done

echo "consume-node: rpc=:$CONSUME_PORT"

RUST_LOG="$RUST_LOG_BINARIES" RUST_BACKTRACE=1 \
    ./target/debug/consume-node \
    --rpc-port "$CONSUME_PORT" \
    --s3-endpoint "$S3_ENDPOINT" \
    --s3-prefix "$S3_PREFIX" \
    2>&1 | sed "s/^/[consume] /" &

echo ""
echo "all nodes started. press ctrl-c to stop."
wait
