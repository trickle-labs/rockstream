#!/usr/bin/env bash
# Self-test for scripts/check-no-hidden-skips.sh.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-hidden-skips.sh"
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

# 1. Test that unrecorded #[ignore] fails
mkdir -p "$TMP_ROOT/crates/dummy/tests"
cat > "$TMP_ROOT/crates/dummy/tests/dummy_test.rs" << 'EOF'
#[test]
#[ignore]
fn test_unrecorded_ignore() {}
EOF

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed on an unrecorded #[ignore]"
fi
grep -q "undocumented" "$OUT_BAD" || {
  cat "$OUT_BAD"
  fail "checker did not report undocumented #[ignore]"
}

# 2. Test that silent docker skip fails
cat > "$TMP_ROOT/crates/dummy/tests/dummy_test.rs" << 'EOF'
#[test]
fn test_silent_skip() {
    if !common::docker_available() {
        return;
    }
}
EOF

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed on a silent docker skip"
fi
grep -q "silent early return on docker availability" "$OUT_BAD" || {
  cat "$OUT_BAD"
  fail "checker did not report silent return"
}

# 3. Test that the real tree passes
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real tree"
fi
[ "$(cat "$OUT_REAL")" = "OK: no hidden test skips found." ] || {
  cat "$OUT_REAL"
  fail "checker success output changed"
}

echo "OK: check-no-hidden-skips.sh self-test passed."
