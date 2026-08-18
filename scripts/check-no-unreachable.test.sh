#!/usr/bin/env bash
# check-no-unreachable.test.sh — self-test harness for check-no-unreachable.sh.
#
# Verifies the checker detects unreachable!() in production source files:
#   1. Fails (exit 1) on a tree with an injected unreachable!() in crates/*/src/.
#   2. Passes (exit 0) on the real, unmodified tree.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-unreachable.sh"
MUTATED_FILE_REL="crates/rockstream-types/src/laws/weight_add.rs"

TMP_BUG="$(mktemp -d)"
OUT_BUG="$(mktemp)"
OUT_REAL="$(mktemp)"
cleanup() {
  rm -rf "$TMP_BUG"
  rm -f "$OUT_BUG" "$OUT_REAL"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# ── Case 1: injected unreachable!() in crates/*/src/ must fail ─────────────────
mkdir -p "$TMP_BUG/crates"
cp -R "$ROOT/crates/." "$TMP_BUG/crates/"

cat >>"$TMP_BUG/$MUTATED_FILE_REL" <<'RUST_EOF'

fn check_no_unreachable_self_test_injected_bug() {
    unreachable!("injected bug for self-test");
}
RUST_EOF

if bash "$CHECKER" "$TMP_BUG" >"$OUT_BUG" 2>&1; then
  cat "$OUT_BUG"
  fail "checker passed on a tree with an injected unreachable!() (expected exit 1)"
fi
grep -q "$MUTATED_FILE_REL" "$OUT_BUG" \
  || { cat "$OUT_BUG"; fail "checker's failure output did not name $MUTATED_FILE_REL"; }

# ── Case 2: the real, unmodified tree must pass (exit 0) ────────────────────
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real, unmodified tree"
fi

echo "OK: check-no-unreachable.sh self-test passed."
