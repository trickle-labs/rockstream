#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-connector-admission.sh"
TMP_ROOT="$(mktemp -d)"
OUT_BAD="$(mktemp)"
OUT_GOOD="$(mktemp)"

cleanup() {
  rm -rf "$TMP_ROOT"
  rm -f "$OUT_BAD" "$OUT_GOOD"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

mkdir -p "$TMP_ROOT/crates/rockstream-connectors/src" "$TMP_ROOT/docs"
cp "$ROOT/crates/rockstream-connectors/src/lib.rs" "$TMP_ROOT/crates/rockstream-connectors/src/lib.rs"
cp "$ROOT/docs/connectors.md" "$TMP_ROOT/docs/connectors.md"
printf '\npub mod synthetic_sink;\n' >> "$TMP_ROOT/crates/rockstream-connectors/src/lib.rs"
printf 'pub struct SyntheticSink;\nimpl SinkConnector for SyntheticSink {}\n' \
  > "$TMP_ROOT/crates/rockstream-connectors/src/synthetic_sink.rs"

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed without a synthetic admission record"
fi
EXPECTED_BAD=$'VIOLATION: synthetic_sink: missing admission record (### Admission: synthetic_sink)\nFAIL: connector admission check found 1 violation(s).'
[ "$(cat "$OUT_BAD")" = "$EXPECTED_BAD" ] || { cat "$OUT_BAD"; fail "checker failure output changed"; }

printf '%s\n' \
  '' \
  '### Admission: synthetic_sink' \
  'core_ivm_improvement: Reduces the cost of the retained incremental path.' \
  'no_kafka_or_postgres_boundary: Does not add another Kafka or PostgreSQL boundary.' \
  'demonstrated_production_demand: A production workload requires this connector.' \
  'failure_and_recovery_semantics: Recovery and failure behavior are documented.' \
  'acceptable_maintenance_burden: The maintenance cost is bounded and owned.' \
  'permanent_compatibility_value: It preserves a durable compatibility boundary.' \
  >> "$TMP_ROOT/docs/connectors.md"

if ! bash "$CHECKER" "$TMP_ROOT" >"$OUT_GOOD" 2>&1; then
  cat "$OUT_GOOD"
  fail "checker rejected a complete synthetic admission record"
fi
[ "$(cat "$OUT_GOOD")" = "OK: connector admission check passed." ] || {
  cat "$OUT_GOOD"
  fail "checker success output changed"
}

echo "OK: check-connector-admission.sh self-test passed."
