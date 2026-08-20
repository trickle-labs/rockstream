#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-r1-local-evidence.py"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_failure() {
  local root="$1"
  local expected="$2"
  local actual
  if actual="$(python3 "$root/scripts/check-r1-local-evidence.py" "$root" 2>&1)"; then
    fail "mutation was accepted"
  fi
  [[ "$actual" == "$expected" ]] || fail "unexpected verifier output: $actual"
}

fresh_copy() {
  local destination="$1"
  mkdir -p "$destination/scripts"
  cp -R "$ROOT/benchmarks" "$destination/benchmarks"
  cp "$CHECKER" "$destination/scripts/check-r1-local-evidence.py"
}

[[ "$(python3 "$CHECKER" "$ROOT")" == "R1 local contract verified" ]] || fail "base contract did not verify"
DIGEST_ONE="$("$ROOT/scripts/run-r1-local.sh" digest)"
DIGEST_TWO="$("$ROOT/scripts/run-r1-local.sh" digest)"
[[ "$DIGEST_ONE" == "$DIGEST_TWO" ]] || fail "repeated corpus generation changed digests"
[[ "$DIGEST_ONE"$'\n' == "$(<"$ROOT/benchmarks/r1-local/generated-digests.sha256")"$'\n' ]] || fail "generated digests differ from the frozen corpus"

fresh_copy "$TMP_ROOT/threshold"
python3 -c 'from pathlib import Path; p=Path(__import__("sys").argv[1]); p.write_text(p.read_text().replace("max_sample_cv = 0.15", "max_sample_cv = 0.16"))' "$TMP_ROOT/threshold/benchmarks/r1-local/thresholds.toml"
assert_failure "$TMP_ROOT/threshold" "VIOLATION: thresholds.toml differs from R1 Section 3"

fresh_copy "$TMP_ROOT/workload"
python3 -c 'from pathlib import Path; p=Path(__import__("sys").argv[1]); p.write_text(p.read_text().replace("fan_out = 100", "fan_out = 101"))' "$TMP_ROOT/workload/benchmarks/r1-local/workloads/factorized-join.toml"
assert_failure "$TMP_ROOT/workload" "VIOLATION: workloads/factorized-join.toml differs from the frozen corpus"

fresh_copy "$TMP_ROOT/profile"
python3 -c 'from pathlib import Path; p=Path(__import__("sys").argv[1]); p.write_text(p.read_text().replace("revision = 1", "revision = 2"))' "$TMP_ROOT/profile/benchmarks/r1-local/profile.toml"
assert_failure "$TMP_ROOT/profile" "VIOLATION: profile.toml has unexpected profile identity or revision"

fresh_copy "$TMP_ROOT/sql"
python3 -c 'from pathlib import Path; p=Path(__import__("sys").argv[1]); p.write_text(p.read_text().replace("SUM(value)", "MAX(value)"))' "$TMP_ROOT/sql/benchmarks/r1-local/sql/ordinary-aggregate.sql"
assert_failure "$TMP_ROOT/sql" "VIOLATION: generated input or SQL digests differ from the frozen corpus"

fresh_copy "$TMP_ROOT/generated-digest"
python3 -c 'from pathlib import Path; p=Path(__import__("sys").argv[1]); p.write_text(p.read_text().replace("input_sha256=", "input_sha256=0"))' "$TMP_ROOT/generated-digest/benchmarks/r1-local/generated-digests.sha256"
assert_failure "$TMP_ROOT/generated-digest" "VIOLATION: generated input or SQL digests differ from the frozen corpus"

printf 'r1 local contract tests passed\n'
