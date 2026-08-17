#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-failure-matrix.sh"
DOC="$ROOT/docs/failure-matrix.md"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

run_bad() {
  local name="$1"
  local doc="$2"
  mkdir -p "$TMP_ROOT/docs" "$TMP_ROOT/scripts" "$TMP_ROOT/crates/rockstream-sim/tests"
  cp "$doc" "$TMP_ROOT/docs/failure-matrix.md"
  cp "$ROOT/scripts/check-failure-matrix.py" "$TMP_ROOT/scripts/check-failure-matrix.py"
  cp "$ROOT/scripts/check-failure-matrix.sh" "$TMP_ROOT/scripts/check-failure-matrix.sh"
  cp "$ROOT/crates/rockstream-sim/tests/failure_matrix_tests.rs" "$TMP_ROOT/crates/rockstream-sim/tests/failure_matrix_tests.rs"
  
  if bash "$TMP_ROOT/scripts/check-failure-matrix.sh" "$TMP_ROOT" >"$TMP_ROOT/$name.out" 2>&1; then
    fail "$name mutation was accepted"
  fi
  grep -q "VIOLATION:" "$TMP_ROOT/$name.out" || fail "$name did not report a violation"
}

# 1. Base check: real failure matrix must pass cleanly
bash "$CHECKER" "$ROOT" >/dev/null || fail "real failure matrix did not pass"

# 2. Mutation: deleted scenario / missing FM-003
grep -v '^| `FM-003` |' "$DOC" >"$TMP_ROOT/missing-scenario.md"
run_bad mutation_deleted_scenario "$TMP_ROOT/missing-scenario.md"

# 3. Mutation: vacuous assertion "did not crash"
sed 's/Zero data loss, zero duplicates, reassignment <= 30s p99, freshness recovery <= 60s p99/System did not crash during test/' "$DOC" >"$TMP_ROOT/vacuous-did-not-crash.md"
run_bad mutation_vacuous_assertion_did_not_crash "$TMP_ROOT/vacuous-did-not-crash.md"

# 4. Mutation: vacuous assertion "passes without panic"
sed 's/Zero split-brain, election <= 5s p99, epoch progress resumes without lost\/duplicated commits/passes without panic/' "$DOC" >"$TMP_ROOT/vacuous-no-panic.md"
run_bad mutation_vacuous_no_panic "$TMP_ROOT/vacuous-no-panic.md"

# 5. Mutation: missing / invalid test file link
sed 's#crates/rockstream-sim/tests/failure_matrix_tests.rs#crates/rockstream-sim/tests/nonexistent_tests.rs#' "$DOC" >"$TMP_ROOT/missing-test-file.md"
run_bad mutation_missing_test_file "$TMP_ROOT/missing-test-file.md"

# 6. Mutation: missing test symbol
sed 's#test_fm001_worker_loss_recovery#test_fm001_missing_symbol#' "$DOC" >"$TMP_ROOT/missing-test-symbol.md"
run_bad mutation_missing_test_symbol "$TMP_ROOT/missing-test-symbol.md"

# 7. Mutation: missing permanent seeds
sed 's#`0x0001_0001_0000_0001`, `0x0001_0001_0000_0002`#---#' "$DOC" >"$TMP_ROOT/missing-seeds.md"
run_bad mutation_missing_seeds "$TMP_ROOT/missing-seeds.md"

echo "OK: check-failure-matrix self-test passed."
