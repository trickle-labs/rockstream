#!/usr/bin/env bash
# check-error-catalog.test.sh — self-test check-error-catalog.sh against deliberate mutations.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-error-catalog.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

# 1. Base check: real repo tree must pass cleanly
bash "$CHECKER" "$ROOT" >/dev/null || fail "real error catalog did not pass"

setup_tree() {
  rm -rf "$TMP_ROOT"
  mkdir -p "$TMP_ROOT/contracts" "$TMP_ROOT/docs" "$TMP_ROOT/crates/rockstream-types/src" "$TMP_ROOT/scripts"
  cp "$ROOT/contracts/errors.toml" "$TMP_ROOT/contracts/"
  cp "$ROOT/docs/error-codes.md" "$TMP_ROOT/docs/"
  cp "$ROOT/crates/rockstream-types/src/error_code.rs" "$TMP_ROOT/crates/rockstream-types/src/"
  cp "$ROOT/scripts/check-error-catalog.py" "$TMP_ROOT/scripts/"
  cp "$ROOT/scripts/check-error-catalog.sh" "$TMP_ROOT/scripts/"
}

run_bad() {
  local name="$1"
  local expected="${2:-VIOLATION:}"
  if bash "$TMP_ROOT/scripts/check-error-catalog.sh" "$TMP_ROOT" >"$TMP_ROOT/$name.out" 2>&1; then
    cat "$TMP_ROOT/$name.out"
    fail "$name mutation was accepted"
  fi
  grep -q "$expected" "$TMP_ROOT/$name.out" || {
    cat "$TMP_ROOT/$name.out"
    fail "$name did not report expected error"
  }
}

# 2. Mutation: missing required field in errors.toml
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/contracts/errors.toml"); s=p.read_text(); p.write_text(s.replace("sqlstate = \"XX000\"", "sqlstate = \"\"", 1))'
run_bad "missing_sqlstate"

# 3. Mutation: invalid severity in errors.toml
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/contracts/errors.toml"); s=p.read_text(); p.write_text(s.replace("severity = \"Fatal\"", "severity = \"Critical\"", 1))'
run_bad "invalid_severity"

# 4. Mutation: invalid retry_class in errors.toml
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/contracts/errors.toml"); s=p.read_text(); p.write_text(s.replace("retry_class = \"NonRetryable\"", "retry_class = \"NeverRetry\"", 1))'
run_bad "invalid_retry_class"

# 5. Mutation: duplicate error code in errors.toml
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/contracts/errors.toml"); s=p.read_text(); p.write_text(s.replace("code = \"RS-0002\"", "code = \"RS-0001\"", 1))'
run_bad "duplicate_error_code"

# 6. Mutation: documentation drift
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/docs/error-codes.md"); s=p.read_text(); p.write_text(s.replace("Internal error", "Corrupted error text", 1))'
run_bad "doc_drift" "drifted from contracts/errors.toml"

# 7. Mutation: missing Rust constant in error_code.rs
setup_tree
python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/crates/rockstream-types/src/error_code.rs"); s=p.read_text(); p.write_text(s.replace("pub const RS_0001: ErrorCode = ErrorCode::new(1);", "", 1))'
run_bad "missing_rust_constant" "Missing Rust constant for RS_0001"

echo "OK: check-error-catalog.sh self-test passed."
