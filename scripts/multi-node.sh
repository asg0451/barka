#!/usr/bin/env bash
# Starts multiple barka produce-nodes (competing for leadership), one consume-node,
# and one rebalancer.
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
    rm -f "$PREFIX_FILE"
    echo "done."
    exit 0
}
trap cleanup INT TERM

PREFIX_FILE="/tmp/barka-prefix.txt"
echo "$S3_PREFIX" > "$PREFIX_FILE"

echo "starting $NUM_NODES produce-nodes + 1 consume-node + 1 rebalancer (s3-prefix=$S3_PREFIX)"
echo ""

CONSOLE_PORT=6669

for i in $(seq 0 $((NUM_NODES - 1))); do
    rpc_port=$((BASE_PRODUCE_PORT + i))

    if [ "$i" -eq 0 ]; then
        echo "produce-node $i: rpc=:$rpc_port  console=:$CONSOLE_PORT"
        TOKIO_CONSOLE_BIND="127.0.0.1:$CONSOLE_PORT" \
        RUST_LOG=barka=info RUST_BACKTRACE=1 \
            ./target/debug/produce-node \
            --node-id "$i" \
            --rpc-port "$rpc_port" \
            --s3-endpoint "$S3_ENDPOINT" \
            --s3-prefix "$S3_PREFIX" \
            --leader-election-prefix "$S3_PREFIX" \
            2>&1 | sed "s/^/[node-$i] /" &
    else
        echo "produce-node $i: rpc=:$rpc_port"
        RUST_LOG=barka=info RUST_BACKTRACE=1 \
            ./target/debug/produce-node \
            --node-id "$i" \
            --rpc-port "$rpc_port" \
            --s3-endpoint "$S3_ENDPOINT" \
            --s3-prefix "$S3_PREFIX" \
            --leader-election-prefix "$S3_PREFIX" \
            2>&1 | sed "s/^/[node-$i] /" &
    fi
done

echo "consume-node: rpc=:$CONSUME_PORT"

RUST_LOG=barka=info RUST_BACKTRACE=1 \
    ./target/debug/consume-node \
    --rpc-port "$CONSUME_PORT" \
    --s3-endpoint "$S3_ENDPOINT" \
    --s3-prefix "$S3_PREFIX" \
    2>&1 | sed "s/^/[consume] /" &

echo "rebalancer: interval=30s"

RUST_LOG=barka=info RUST_BACKTRACE=1 \
    ./target/debug/rebalancer \
    --s3-endpoint "$S3_ENDPOINT" \
    --leader-election-prefix "$S3_PREFIX" \
    --interval-secs 30 \
    2>&1 | sed "s/^/[rebalancer] /" &

echo ""
echo "all nodes started. press ctrl-c to stop."
wait
