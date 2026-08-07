#!/usr/bin/env bash
# Regression test for check-no-mock-connectors.sh.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-mock-connectors.sh"
TMP_ROOT="$(mktemp -d)"
OUT_BAD="$(mktemp)"
OUT_REAL="$(mktemp)"

cleanup() {
  rm -rf "$TMP_ROOT"
  rm -f "$OUT_BAD" "$OUT_REAL"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

mkdir -p "$TMP_ROOT/crates/rockstream-connectors/src"
printf '/// This is a simulated connector.\n' > "$TMP_ROOT/crates/rockstream-connectors/src/kafka_source.rs"
touch "$TMP_ROOT/crates/rockstream-connectors/src/kafka_sink.rs" "$TMP_ROOT/crates/rockstream-connectors/src/object_store_sink.rs"

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed on a simulated connector docstring"
fi
EXPECTED_BAD=$'VIOLATION: crates/rockstream-connectors/src/kafka_source.rs:1:/// This is a simulated connector.: forbidden mock-connector prose\nFAIL: 1 forbidden mock-connector prose violation(s) found.'
[ "$(cat "$OUT_BAD")" = "$EXPECTED_BAD" ] || { cat "$OUT_BAD"; fail "checker failure output changed"; }

if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real tree"
fi
[ "$(cat "$OUT_REAL")" = "OK: no mock-connector prose found." ] || { cat "$OUT_REAL"; fail "checker success output changed"; }

echo "OK: check-no-mock-connectors.sh self-test passed."
