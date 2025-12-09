#!/usr/bin/env bash
set -euo pipefail

# Advanced observability smoke test for Chronos
# - builds the binary if needed
# - starts a single node on a random port
# - waits for /health and /metrics on the HTTP admin port (grpc_port+1000)
# - asserts key fields/metrics exist
# - shuts down the node and cleans up

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT_DIR/target/debug/chronos"

if [[ ! -x "$BIN" ]]; then
  echo "[obs] Building chronos binary..." >&2
  (cd "$ROOT_DIR" && cargo build --bin chronos >/dev/null)
fi

DATA_DIR="$(mktemp -d -p "$ROOT_DIR" chronos_obs_XXXXXX)"
PORT=$(( 25000 + (RANDOM % 10000) ))
GRPC_ADDR="127.0.0.1:${PORT}"
HTTP_PORT=$(( PORT + 1000 ))
HTTP_ADDR="127.0.0.1:${HTTP_PORT}"

echo "[obs] Using data dir: $DATA_DIR" >&2
echo "[obs] gRPC addr: $GRPC_ADDR" >&2
echo "[obs] HTTP admin addr: $HTTP_ADDR" >&2

cleanup() {
  local ec=$?
  if [[ -n "${NODE_PID:-}" ]]; then
    echo "[obs] Stopping node (pid=$NODE_PID)..." >&2
    kill "$NODE_PID" 2>/dev/null || true
    wait "$NODE_PID" 2>/dev/null || true
  fi
  if [[ -d "$DATA_DIR" ]]; then
    rm -rf "$DATA_DIR"
  fi
  exit "$ec"
}
trap cleanup EXIT

# Start Chronos node with HTTP admin enabled (it always binds to gRPC+1000)
"$BIN" node \
  --id obs \
  --data-dir "$DATA_DIR" \
  --address "$GRPC_ADDR" \
  --clean \
  >/dev/null 2>&1 &
NODE_PID=$!

echo "[obs] Started node with PID $NODE_PID" >&2

# Wait for /health to be available
TIMEOUT_SECS=20
START_TS=$(date +%s)

while true; do
  if curl -fsS "http://$HTTP_ADDR/health" >/dev/null 2>&1; then
    break
  fi
  NOW=$(date +%s)
  if (( NOW - START_TS > TIMEOUT_SECS )); then
    echo "[obs] ERROR: /health did not become ready within ${TIMEOUT_SECS}s" >&2
    exit 1
  fi
  sleep 0.5
done

echo "[obs] /health is reachable" >&2

HEALTH_JSON="$(curl -fsS "http://$HTTP_ADDR/health")"
echo "[obs] /health: $HEALTH_JSON" >&2

# Basic assertions on /health payload
if ! grep -q '"status":"ok"' <<<"$HEALTH_JSON"; then
  echo "[obs] ERROR: /health missing or wrong status" >&2
  exit 1
fi

if ! grep -q '"role":"' <<<"$HEALTH_JSON"; then
  echo "[obs] ERROR: /health missing role field" >&2
  exit 1
fi

if ! grep -q '"term":' <<<"$HEALTH_JSON"; then
  echo "[obs] ERROR: /health missing term field" >&2
  exit 1
fi

echo "[obs] /health JSON looks valid" >&2

METRICS="$(curl -fsS "http://$HTTP_ADDR/metrics")"
echo "[obs] /metrics snippet:" >&2
printf '%s
' "$METRICS" | head -n 10 >&2

# Check for key Prometheus metrics
need_metric() {
  local name="$1"
  if ! grep -q "$name" <<<"$METRICS"; then
    echo "[obs] ERROR: metric '$name' not found in /metrics" >&2
    exit 1
  fi
}

need_metric 'chronos_sql_requests_total{kind="read"}'
need_metric 'chronos_sql_requests_total{kind="write"}'
need_metric 'chronos_raft_term'
need_metric 'chronos_raft_role'
need_metric 'chronos_storage_size_bytes'

echo "[obs] All expected metrics are present" >&2

echo "[obs] Observability smoke test PASSED" >&2
exit 0
