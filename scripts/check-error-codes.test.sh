#!/usr/bin/env bash
# check-error-codes.test.sh — self-test harness for check-error-codes.sh.
#
# Verifies the checker actually *detects* a missing RS-XXXX code (not just
# vacuously passing) by mutating a temp copy of the tree to append a
# deliberately bare `Err(format!("boom"))` (with no trailing `?`, no inline
# code) to a copied source file, then confirms:
#   1. The checker exits 1 against the mutated copy and names the mutated
#      file in its output.
#   2. The checker exits 0 against the real, unmodified tree.
#
# Invoked as its own CI step, alongside (not instead of) the
# check-error-codes.sh step itself, so a regression in the checker script is
# caught even if error-code coverage in crates/ never changes.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-error-codes.sh"
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

# ── Case 1: a tree with a deliberately bare Err(format!("boom")) appended
#    to a copied source file must fail (exit 1) ─────────────────────────────
mkdir -p "$TMP_BUG/crates"
cp -R "$ROOT/crates/." "$TMP_BUG/crates/"

cat >>"$TMP_BUG/$MUTATED_FILE_REL" <<'RUST_EOF'

fn check_error_codes_self_test_injected_bug() -> Result<(), String> {
    Err(format!("boom"))
}
RUST_EOF

if bash "$CHECKER" "$TMP_BUG" >"$OUT_BUG" 2>&1; then
  cat "$OUT_BUG"
  fail "checker passed on a tree with an injected bare Err(format!(\"boom\")) (expected exit 1)"
fi
grep -q "boom\|$MUTATED_FILE_REL" "$OUT_BUG" \
  || { cat "$OUT_BUG"; fail "checker's failure output did not name the mutated file"; }
grep -q "$MUTATED_FILE_REL" "$OUT_BUG" \
  || { cat "$OUT_BUG"; fail "checker's failure output did not name $MUTATED_FILE_REL"; }

# ── Case 2: the real, unmodified tree must pass (exit 0) ────────────────────
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real, unmodified tree"
fi

echo "OK: check-error-codes.sh self-test passed."
