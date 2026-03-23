#!/usr/bin/env bash
# Idempotent LocalStack (S3) for tests and dev. Used by Makefile, Cursor cloud agent, and CI-style setups.
set -euo pipefail

HEALTH_URL="${LOCALSTACK_HEALTH_URL:-http://127.0.0.1:4566/_localstack/health}"
NAME="${LOCALSTACK_CONTAINER_NAME:-barka-localstack}"
IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack}"

if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
	exit 0
fi

CONTAINER_RT="${CONTAINER_RT:-docker}"
RT0="${CONTAINER_RT%% *}"
if ! command -v "$RT0" >/dev/null 2>&1; then
	echo "ensure-localstack: container runtime not found: $RT0 (set CONTAINER_RT)" >&2
	exit 1
fi

rt() {
	# shellcheck disable=SC2086
	$CONTAINER_RT "$@"
}

if [[ "$RT0" == "docker" ]]; then
	sudo service docker start 2>/dev/null || true
fi

rt rm -f "$NAME" 2>/dev/null || true
if ! rt image inspect "$IMAGE" >/dev/null 2>&1; then
	rt pull "$IMAGE"
fi

# Bind to loopback only (matches leader_election / Rust test endpoints).
rt run -d --name "$NAME" \
	-p 127.0.0.1:4566:4566 \
	-e SERVICES=s3 \
	-e DEFAULT_REGION=us-east-1 \
	-e EAGER_SERVICE_LOADING=1 \
	-e DEBUG=0 \
	"$IMAGE"

for _ in $(seq 1 120); do
	if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
		exit 0
	fi
	sleep 1
done

echo "ensure-localstack: timed out waiting for $HEALTH_URL" >&2
exit 1
