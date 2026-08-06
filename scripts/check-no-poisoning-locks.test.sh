#!/usr/bin/env bash
# check-no-poisoning-locks.test.sh — regression tests for the audited-lock gate.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-poisoning-locks.sh"
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

mkdir -p "$TMP_ROOT/crates"
cp -R "$ROOT/crates/rockstream-gateway" "$TMP_ROOT/crates/"
cp -R "$ROOT/crates/rockstream-control" "$TMP_ROOT/crates/"
cp -R "$ROOT/crates/rockstream-sql" "$TMP_ROOT/crates/"
cp -R "$ROOT/crates/rockstream-types" "$TMP_ROOT/crates/"
cp -R "$ROOT/crates/rockstream-connectors" "$TMP_ROOT/crates/"
printf '\nuse std::sync::Mutex;\nfn injected_lock() { let _lock = Mutex::new(()); }\n' >> "$TMP_ROOT/crates/rockstream-types/src/dlq.rs"

if bash "$CHECKER" "$TMP_ROOT" >"$OUT_BAD" 2>&1; then
  cat "$OUT_BAD"
  fail "checker passed on an unannotated poisoning lock"
fi
grep -Fq "crates/rockstream-types/src/dlq.rs" "$OUT_BAD" \
  || { cat "$OUT_BAD"; fail "checker failure did not name the injected path"; }

if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real tree"
fi
grep -Fqx "OK: no unapproved std::sync poisoning locks in audited sources." "$OUT_REAL" \
  || { cat "$OUT_REAL"; fail "checker success output changed"; }

echo "OK: check-no-poisoning-locks.sh self-test passed."
