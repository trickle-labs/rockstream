#!/usr/bin/env bash
# check-invariant-pairs.test.sh — self-test harness for check-invariant-pairs.sh.
#
# Verifies the checker actually *detects* missing invariant coverage (not
# just vacuously passing) by mutating a temp copy of the tree to delete
# every M2-S3 reference (and, separately, every M4-S1 reference) from
# crates/, then confirms:
#   1. The checker exits 1 against each mutated copy and names the deleted
#      ID in its output.
#   2. The checker exits 0 against the real, unmodified tree.
#
# Invoked as its own CI step, alongside (not instead of) the
# check-invariant-pairs.sh step itself, so a regression in the checker
# script is caught even if invariant coverage in crates/ never changes.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-invariant-pairs.sh"

TMP_M2S3="$(mktemp -d)"
TMP_M4S1="$(mktemp -d)"
TMP_EDGE_QUOTA="$(mktemp -d)"
TMP_EDGE_SOURCEFAIL="$(mktemp -d)"
TMP_EDGE_BROWNOUT="$(mktemp -d)"
TMP_EDGE_MISCONFIG="$(mktemp -d)"
TMP_EDGE_LATE="$(mktemp -d)"
OUT_M2S3="$(mktemp)"
OUT_M4S1="$(mktemp)"
OUT_EDGE_QUOTA="$(mktemp)"
OUT_EDGE_SOURCEFAIL="$(mktemp)"
OUT_EDGE_BROWNOUT="$(mktemp)"
OUT_EDGE_MISCONFIG="$(mktemp)"
OUT_EDGE_LATE="$(mktemp)"
OUT_REAL="$(mktemp)"
cleanup() {
  rm -rf "$TMP_M2S3" "$TMP_M4S1" "$TMP_EDGE_QUOTA" "$TMP_EDGE_SOURCEFAIL" "$TMP_EDGE_BROWNOUT" "$TMP_EDGE_MISCONFIG" "$TMP_EDGE_LATE"
  rm -f "$OUT_M2S3" "$OUT_M4S1" "$OUT_EDGE_QUOTA" "$OUT_EDGE_SOURCEFAIL" "$OUT_EDGE_BROWNOUT" "$OUT_EDGE_MISCONFIG" "$OUT_EDGE_LATE" "$OUT_REAL"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

make_mutated_copy() {
  local dest="$1"
  local deleted_id="$2"
  mkdir -p "$dest/crates" "$dest/formal"
  cp -R "$ROOT/crates/." "$dest/crates/"
  cp -R "$ROOT/formal/." "$dest/formal/"
  # Delete every line referencing $deleted_id from the copied Rust sources,
  # simulating a deliberately-reverted assertion.
  local file
  while IFS= read -r file; do
    sed -i.bak "/${deleted_id}/d" "$file"
    rm -f "$file.bak"
  done < <(grep -rl --include="*.rs" -- "$deleted_id" "$dest/crates" || true)
}

# ── Case 1: a tree with M2-S3 coverage deleted must fail (exit 1) ────────────
make_mutated_copy "$TMP_M2S3" "M2-S3"
if bash "$CHECKER" "$TMP_M2S3" >"$OUT_M2S3" 2>&1; then
  cat "$OUT_M2S3"
  fail "checker passed on a tree with M2-S3 coverage deleted (expected exit 1)"
fi
grep -q "M2-S3" "$OUT_M2S3" || fail "checker's failure output did not name M2-S3"

# ── Case 2: a tree with M4-S1 coverage deleted must fail (exit 1) ────────────
make_mutated_copy "$TMP_M4S1" "M4-S1"
if bash "$CHECKER" "$TMP_M4S1" >"$OUT_M4S1" 2>&1; then
  cat "$OUT_M4S1"
  fail "checker passed on a tree with M4-S1 coverage deleted (expected exit 1)"
fi
grep -q "M4-S1" "$OUT_M4S1" || fail "checker's failure output did not name M4-S1"

# ── Cases 3-7: every EDGE recovery proof needs a real Rust assert! ─────────
for edge in EDGE-QUOTA EDGE-SOURCEFAIL EDGE-BROWNOUT EDGE-MISCONFIG EDGE-LATE; do
  normalized="${edge//-/_}"
  tmp_var="TMP_${normalized}"
  out_var="OUT_${normalized}"
  tmp="${!tmp_var}"
  out="${!out_var}"
  make_mutated_copy "$tmp" "$edge"
  if bash "$CHECKER" "$tmp" >"$out" 2>&1; then
    cat "$out"
    fail "checker passed on a tree with $edge coverage deleted (expected exit 1)"
  fi
  grep -q "$edge" "$out" || fail "checker's failure output did not name $edge"
done

# ── Case 8: the real, unmodified tree must pass (exit 0) ────────────────────
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real, unmodified tree"
fi

echo "OK: check-invariant-pairs.sh self-test passed."
