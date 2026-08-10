#!/usr/bin/env bash
# check-operator-state-bytes.test.sh — self-test for the operator state accounting gate.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-operator-state-bytes.sh"
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

# 1. Verify against real repository tree
if ! bash "$CHECKER" "$ROOT" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against real repository tree"
fi
grep -Fqx "OK: all stateful operators implement state_bytes accounting for arrangement fields." "$OUT_REAL" \
  || { cat "$OUT_REAL"; fail "checker success output mismatch"; }

# 2. Verify failure on injected un-accounted field
mkdir -p "$TMP_ROOT/crates/rockstream-ops/src"
cp -R "$ROOT/crates/rockstream-ops/src/"* "$TMP_ROOT/crates/rockstream-ops/src/"

# Inject an un-accounted collection field into JoinState
sed -i.bak 's/left_arr: HashMap/unaccounted_extra_arr: HashMap<Vec<u8>, Vec<u8> >,\n    left_arr: HashMap/' "$TMP_ROOT/crates/rockstream-ops/src/join.rs"

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed on an un-accounted arrangement field"
fi

echo "OK: check-operator-state-bytes.sh self-test passed."
