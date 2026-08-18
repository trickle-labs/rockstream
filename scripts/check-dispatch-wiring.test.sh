#!/usr/bin/env bash
# check-dispatch-wiring.test.sh — self-test harness for check-dispatch-wiring.sh.
#
# Verifies the checker detects broken or missing dispatch-wiring pipelines:
#   1. Fails (exit 1) on a tree with an invalid dispatch symbol in capabilities.toml.
#   2. Fails (exit 1) on a tree with a missing dispatcher handler in source code.
#   3. Passes (exit 0) on the real, unmodified tree.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-dispatch-wiring.sh"

TMP_BUG1="$(mktemp -d)"
TMP_BUG2="$(mktemp -d)"
OUT_BUG1="$(mktemp)"
OUT_BUG2="$(mktemp)"
OUT_REAL="$(mktemp)"
cleanup() {
  rm -rf "$TMP_BUG1" "$TMP_BUG2"
  rm -f "$OUT_BUG1" "$OUT_BUG2" "$OUT_REAL"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# ── Case 1: invalid dispatch entry in capabilities.toml must fail ─────────────
mkdir -p "$TMP_BUG1/crates" "$TMP_BUG1/scripts" "$TMP_BUG1/docs"
cp -R "$ROOT/crates/." "$TMP_BUG1/crates/"
cp -R "$ROOT/scripts/." "$TMP_BUG1/scripts/"
cp "$ROOT/capabilities.toml" "$TMP_BUG1/capabilities.toml"
sed -i.bak 's/symbol = "handle_create_view"/symbol = "nonexistent_handler_mutation"/' "$TMP_BUG1/capabilities.toml"
rm -f "$TMP_BUG1/capabilities.toml.bak"

if bash "$CHECKER" "$TMP_BUG1" >"$OUT_BUG1" 2>&1; then
  cat "$OUT_BUG1"
  fail "checker passed on a tree with broken capabilities.toml dispatch symbol (expected exit 1)"
fi
grep -q "nonexistent_handler_mutation" "$OUT_BUG1" \
  || { cat "$OUT_BUG1"; fail "checker's failure output did not name nonexistent_handler_mutation"; }

# ── Case 2: missing dispatcher handler in crates/ must fail ───────────────────
mkdir -p "$TMP_BUG2/crates" "$TMP_BUG2/scripts" "$TMP_BUG2/docs"
cp -R "$ROOT/crates/." "$TMP_BUG2/crates/"
cp -R "$ROOT/scripts/." "$TMP_BUG2/scripts/"
cp "$ROOT/capabilities.toml" "$TMP_BUG2/capabilities.toml"
sed -i.bak 's/handle_create_view/mutated_create_view/g' "$TMP_BUG2/crates/rockstream-gateway/src/server.rs"
rm -f "$TMP_BUG2/crates/rockstream-gateway/src/server.rs.bak"

if bash "$CHECKER" "$TMP_BUG2" >"$OUT_BUG2" 2>&1; then
  cat "$OUT_BUG2"
  fail "checker passed on a tree with deleted handle_create_view dispatch (expected exit 1)"
fi
grep -q "MISSING path for \[Views" "$OUT_BUG2" \
  || { cat "$OUT_BUG2"; fail "checker's failure output did not report missing view dispatch path"; }

# ── Case 3: the real, unmodified tree must pass (exit 0) ────────────────────
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real, unmodified tree"
fi

echo "OK: check-dispatch-wiring.sh self-test passed."
