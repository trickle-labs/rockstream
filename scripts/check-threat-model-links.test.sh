#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-threat-model-links.sh"
DOC="$ROOT/docs/threat-model.md"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

run_bad() {
  local name="$1"
  local doc="$2"
  mkdir -p "$TMP_ROOT/docs" "$TMP_ROOT/scripts"
  cp "$doc" "$TMP_ROOT/docs/threat-model.md"
  if bash "$CHECKER" "$TMP_ROOT" >"$TMP_ROOT/$name.out" 2>&1; then
    fail "$name mutation was accepted"
  fi
  grep -q "VIOLATION:" "$TMP_ROOT/$name.out" || fail "$name did not report a violation"
}

bash "$CHECKER" "$ROOT" >/dev/null || fail "real threat model did not pass"

grep -v '^| Pgwire DDL/DML authorization |' "$DOC" >"$TMP_ROOT/missing-boundary.md"
run_bad missing-boundary "$TMP_ROOT/missing-boundary.md"

sed 's/AclStore role checks/MISSING control/' "$DOC" >"$TMP_ROOT/missing-control.md"
run_bad missing-control "$TMP_ROOT/missing-control.md"

sed 's#scripts/check-dependency-audit.test.sh#scripts/missing-proof.test.sh#' "$DOC" >"$TMP_ROOT/missing-proof.md"
run_bad missing-proof "$TMP_ROOT/missing-proof.md"

sed 's#auth_scram_tests.rs::test_scram_wrong_password#auth_scram_tests.rs::missing_test#' "$DOC" >"$TMP_ROOT/dangling-proof.md"
run_bad dangling-proof "$TMP_ROOT/dangling-proof.md"

echo "OK: threat-model checker self-test passed."
