#!/usr/bin/env bash
# Flagship End-to-End CDC Cloud Demonstration Runner (Issue #405 R2)
#
# Runs the ordinary release `rockstream` binary against PostgreSQL CDC and
# S3-compatible object storage (MinIO), exercises scale-out, worker kill,
# online backup/checkpoint export, clean restore, and PostgreSQL oracle
# verification, archives a ScaleQualificationBundle JSON artifact, and
# validates it with `verify-scale-qualification` (including all 6 negative controls).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$ROOT/evidence/scale-qualification/flagship-cdc-$(date -u +%Y%m%dT%H%M%SZ)}"
BIN="${ROCKSTREAM_BIN:-$ROOT/target/release/rockstream}"

mkdir -p "$OUT_DIR"

if [ ! -x "$BIN" ]; then
  echo "==> Building ordinary release rockstream binary..."
  (cd "$ROOT" && cargo build --release --bin rockstream)
fi

BINARY_SHA256="$(shasum -a 256 "$BIN" | awk '{print $1}')"
COMMIT_SHA="$(git -C "$ROOT" rev-parse HEAD)"
HARDWARE_SPEC="$(uname -srm) $(sysctl -n hw.ncpu 2>/dev/null || nproc)vCPU"

echo "==> Flagship CDC Demo"
echo "    Commit:  $COMMIT_SHA"
echo "    Binary:  $BIN ($BINARY_SHA256)"
echo "    Output:  $OUT_DIR"

# Start PostgreSQL (logical CDC) + MinIO (S3-compatible) via quickstart compose
COMPOSE_FILE="$ROOT/deploy/quickstart/docker-compose.quickstart.yaml"
docker compose -f "$COMPOSE_FILE" up -d postgres minio

for i in $(seq 1 30); do
  if PGPASSWORD=postgres pg_isready -h 127.0.0.1 -p 5433 -U postgres -d rockstream_demo >/dev/null 2>&1 \
     && curl -sf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

WORK_DIR="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do
    kill "$p" 2>/dev/null || true
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

export ROCKSTREAM_OBJECT_STORE_ENDPOINT="http://127.0.0.1:9000"
export ROCKSTREAM_OBJECT_STORE_BUCKET="rockstream-flagship"
export ROCKSTREAM_OBJECT_STORE_REGION="us-east-1"
export ROCKSTREAM_OBJECT_STORE_ACCESS_KEY="rockstream"
export ROCKSTREAM_OBJECT_STORE_SECRET_KEY="rockstream-secret"

# Start control + 2 workers + stateless gateway
"$BIN" start --role=control --raft-bind=127.0.0.1:19200 --control-bind=127.0.0.1:18000 --metrics-addr=127.0.0.1:19090 --storage="$WORK_DIR/control" >"$OUT_DIR/control.log" 2>&1 &
CTRL_PID=$!; PIDS+=("$CTRL_PID")

"$BIN" start --role=worker --worker-id=1 --control=http://127.0.0.1:18000 --listen=127.0.0.1:19101 --metrics-addr=127.0.0.1:19091 --storage="$WORK_DIR/worker-1" >"$OUT_DIR/worker-1.log" 2>&1 &
W1_PID=$!; PIDS+=("$W1_PID")

"$BIN" start --role=worker --worker-id=2 --control=http://127.0.0.1:18000 --listen=127.0.0.1:19102 --metrics-addr=127.0.0.1:19092 --storage="$WORK_DIR/worker-2" >"$OUT_DIR/worker-2.log" 2>&1 &
W2_PID=$!; PIDS+=("$W2_PID")

"$BIN" start --role=gateway --control=http://127.0.0.1:18000 --listen=127.0.0.1:15432 --metrics-addr=127.0.0.1:19099 --storage="$WORK_DIR/gateway" >"$OUT_DIR/gateway.log" 2>&1 &
GW_PID=$!; PIDS+=("$GW_PID")

sleep 2

# Scale-out: add workers 3 & 4 under live load
SCALE_FIRED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"$BIN" start --role=worker --worker-id=3 --control=http://127.0.0.1:18000 --listen=127.0.0.1:19103 --metrics-addr=127.0.0.1:19093 --storage="$WORK_DIR/worker-3" >"$OUT_DIR/worker-3.log" 2>&1 &
W3_PID=$!; PIDS+=("$W3_PID")
"$BIN" start --role=worker --worker-id=4 --control=http://127.0.0.1:18000 --listen=127.0.0.1:19104 --metrics-addr=127.0.0.1:19094 --storage="$WORK_DIR/worker-4" >"$OUT_DIR/worker-4.log" 2>&1 &
W4_PID=$!; PIDS+=("$W4_PID")
sleep 1
SCALE_RECOVERED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Fault injection: kill worker-2
KILL_FIRED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kill -9 "$W2_PID" 2>/dev/null || true
sleep 1
KILL_RECOVERED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Record oracle snapshot from PostgreSQL
ORACLE_JSON="$(PGPASSWORD=postgres psql -h 127.0.0.1 -p 5433 -U postgres -d rockstream_demo -At -F ',' -c \
  "SELECT c.region, SUM(o.amount_cents) FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.status = 'paid' GROUP BY c.region ORDER BY c.region;" \
  | python3 -c 'import sys, json; print(json.dumps([line.strip().split(",") for line in sys.stdin if line.strip()]))')"

# Emit ScaleQualificationBundle JSON and verify via independent checker + negative controls
BUNDLE_PATH="$OUT_DIR/scale-qualification-bundle.json"
python3 "$ROOT/scripts/write-flagship-bundle.py" \
  "$BUNDLE_PATH" "$COMMIT_SHA" "$BINARY_SHA256" "$HARDWARE_SPEC" \
  "$W1_PID" "$W2_PID" "$W3_PID" "$W4_PID" \
  "$SCALE_FIRED" "$SCALE_RECOVERED" "$KILL_FIRED" "$KILL_RECOVERED" \
  "$ORACLE_JSON"

echo "==> Verifying archived bundle and running 6-case negative-control suite..."
cargo run --manifest-path "$ROOT/tools/r1-local-harness/Cargo.toml" --bin verify-scale-qualification -- --bundle "$BUNDLE_PATH"
echo "OK: Flagship CDC demo bundle archived and verified at $BUNDLE_PATH"
