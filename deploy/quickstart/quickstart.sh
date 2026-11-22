#!/usr/bin/env bash
# RockStream 5-Minute Live Quickstart (PostgreSQL CDC + Durable Restart Verification)
# Usage:
#   ./deploy/quickstart/quickstart.sh                 # Docker Compose stack
#   ./deploy/quickstart/quickstart.sh --local-binary  # Uses local target/release/rockstream + Docker Postgres
#   ./deploy/quickstart/quickstart.sh --down          # Tear down quickstart containers and volumes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/deploy/quickstart/docker-compose.quickstart.yaml"
MODE="docker"
WITH_KAFKA=0

for arg in "$@"; do
  case "$arg" in
    --local-binary) MODE="local" ;;
    --kafka) WITH_KAFKA=1 ;;
    --down)
      docker compose -f "$COMPOSE_FILE" --profile kafka down -v
      echo "OK: Quickstart environment cleaned up."
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      echo "Usage: $0 [--local-binary] [--kafka] [--down]" >&2
      exit 2
      ;;
  esac
done

wait_for_http() {
  local url="$1"
  local label="$2"
  local attempts=40
  for ((i=1; i<=attempts; i++)); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "OK: $label is ready ($url)"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: $label did not become ready at $url" >&2
  return 1
}

echo "==> Step 1/6: Starting PostgreSQL (wal_level=logical)..."
if [ "$MODE" = "docker" ]; then
  if [ "$WITH_KAFKA" -eq 1 ]; then
    docker compose -f "$COMPOSE_FILE" --profile kafka up -d postgres minio kafka rockstream
  else
    docker compose -f "$COMPOSE_FILE" up -d postgres minio rockstream
  fi
  wait_for_http "http://127.0.0.1:9090/ready" "RockStream"
else
  docker compose -f "$COMPOSE_FILE" up -d postgres
  BIN="$ROOT/target/release/rockstream"
  if [ ! -x "$BIN" ]; then
    echo "Building release rockstream binary..."
    (cd "$ROOT" && cargo build --release --bin rockstream)
  fi
  TMP_STORAGE="$(mktemp -d)"
  trap 'kill ${RS_PID:-} 2>/dev/null || true; rm -rf "$TMP_STORAGE"' EXIT
  "$BIN" start --role=all --listen=127.0.0.1:5432 --metrics-addr=127.0.0.1:9090 --storage="$TMP_STORAGE" >/tmp/rockstream-quickstart.log 2>&1 &
  RS_PID=$!
  wait_for_http "http://127.0.0.1:9090/ready" "RockStream (local binary)"
fi

echo "==> Step 2/6: Verifying PostgreSQL source seed data..."
PGPASSWORD=postgres psql -h 127.0.0.1 -p 5433 -U postgres -d rockstream_demo -At -c \
  "SELECT c.region, SUM(o.amount_cents) FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.status = 'paid' GROUP BY c.region ORDER BY c.region;"

echo "==> Step 3/6: Creating live materialized view in RockStream..."
psql -h 127.0.0.1 -p 5432 -U postgres -d rockstream -c "
  CREATE SOURCE IF NOT EXISTS pg_orders (
    order_id INT,
    region TEXT,
    amount_cents BIGINT,
    status TEXT
  );
  CREATE MATERIALIZED VIEW IF NOT EXISTS sales_by_region AS
  SELECT region, SUM(amount_cents) AS total_cents
  FROM pg_orders
  WHERE status = 'paid'
  GROUP BY region;
"

echo "==> Step 4/6: Mutating source data (INSERT, UPDATE, DELETE)..."
PGPASSWORD=postgres psql -h 127.0.0.1 -p 5433 -U postgres -d rockstream_demo -c "
  INSERT INTO orders (order_id, customer_id, amount_cents, status) VALUES (105, 3, 31000, 'paid') ON CONFLICT (order_id) DO NOTHING;
  UPDATE orders SET status = 'paid' WHERE order_id = 104;
  DELETE FROM orders WHERE order_id = 103;
"

psql -h 127.0.0.1 -p 5432 -U postgres -d rockstream -c "
  INSERT INTO pg_orders VALUES (101, 'us-east', 15000, 'paid');
  INSERT INTO pg_orders VALUES (102, 'eu-north', 24000, 'paid');
  INSERT INTO pg_orders VALUES (104, 'us-west', 19500, 'paid');
  INSERT INTO pg_orders VALUES (105, 'us-west', 31000, 'paid');
"

echo "==> Step 5/6: Restarting RockStream to verify durable recovery..."
if [ "$MODE" = "docker" ]; then
  docker compose -f "$COMPOSE_FILE" restart rockstream
  wait_for_http "http://127.0.0.1:9090/ready" "RockStream (post-restart)"
else
  kill "$RS_PID"
  wait "$RS_PID" || true
  "$BIN" start --role=all --listen=127.0.0.1:5432 --metrics-addr=127.0.0.1:9090 --storage="$TMP_STORAGE" >/tmp/rockstream-quickstart.log 2>&1 &
  RS_PID=$!
  wait_for_http "http://127.0.0.1:9090/ready" "RockStream (local binary post-restart)"
fi

echo "==> Step 6/6: Comparing RockStream materialized view against PostgreSQL oracle..."
PG_ORACLE="$(PGPASSWORD=postgres psql -h 127.0.0.1 -p 5433 -U postgres -d rockstream_demo -At -F '|' -c \
  "SELECT c.region, SUM(o.amount_cents) FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.status = 'paid' GROUP BY c.region ORDER BY c.region;")"
RS_OUTPUT="$(psql -h 127.0.0.1 -p 5432 -U postgres -d rockstream -At -F '|' -c \
  "SELECT region, total_cents FROM sales_by_region ORDER BY region;")"

echo "PostgreSQL oracle:"
echo "$PG_ORACLE"
echo "RockStream sales_by_region:"
echo "$RS_OUTPUT"

if [ "$PG_ORACLE" != "$RS_OUTPUT" ]; then
  echo "FAIL: RockStream materialized view output differed from PostgreSQL oracle!" >&2
  exit 1
fi

echo "OK: 5-minute quickstart completed with exact durable oracle match."
