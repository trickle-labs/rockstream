#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-release-candidate-gate.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

# 1. Base check: real repository must pass all RC gates cleanly
bash "$CHECKER" "$ROOT" >/dev/null || fail "real repository did not pass RC gate check"

# 2. Mutation: missing reachability test file (Gate 1 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
rm -f "$TMP_DIR/crates/rockstream-gateway/tests/unscoped_pgwire_reachability_tests.rs"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "missing reachability test file was accepted"
fi
grep -q "VIOLATION: Gate 1:" "$TMP_DIR/out.log" || fail "Gate 1 missing test violation was not reported"
rm -rf "$TMP_DIR"

# 3. Mutation: broken recovery SLO target (Gate 2 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import sys
p = sys.argv[1]
with open(p, "r") as f: c = f.read()
with open(p, "w") as f: f.write(c.replace("\"failure_detection_ms\": 5000", "\"failure_detection_ms\": 99999"))
' "$TMP_DIR/docs/chaos-recovery-baseline.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "broken recovery SLO target was accepted"
fi
grep -q "VIOLATION: Gate 2:" "$TMP_DIR/out.log" || fail "Gate 2 broken SLO violation was not reported"
rm -rf "$TMP_DIR"

# 4. Mutation: disallowed SlateDB range deletion in production crate (Gate 3 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
echo 'pub fn bad_delete(db: &slatedb::Db) { db.delete_range(b"a"..b"z"); }' > "$TMP_DIR/crates/rockstream-control/src/bad_module.rs"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "range deletion mutation was accepted"
fi
grep -q "VIOLATION: Gate 3:" "$TMP_DIR/out.log" || fail "Gate 3 range deletion violation was not reported"
rm -rf "$TMP_DIR"

# 5. Mutation: missing rolling upgrade doc (Gate 5 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
rm -f "$TMP_DIR/docs/rolling-upgrades.md"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "missing rolling upgrades doc was accepted"
fi
grep -q "VIOLATION: Gate 5:" "$TMP_DIR/out.log" || fail "Gate 5 missing doc violation was not reported"
rm -rf "$TMP_DIR"

# 6. Mutation: open P0 vulnerability in security review commission (Gate 6 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import sys
p = sys.argv[1]
with open(p, "r") as f: c = f.read()
with open(p, "w") as f: f.write(c.replace("Open P0 Vulnerabilities: 0", "Open P0 Vulnerabilities: 1"))
' "$TMP_DIR/SECURITY_REVIEW_COMMISSION.md"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "open P0 vulnerability mutation was accepted"
fi
grep -q "VIOLATION: Gate 6:" "$TMP_DIR/out.log" || fail "Gate 6 open P0 violation was not reported"
rm -rf "$TMP_DIR"

# 7. Mutation: steady state throughput below 2500 rows/s (Gate 7 violation)
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import sys
p = sys.argv[1]
with open(p, "r") as f: c = f.read()
with open(p, "w") as f: f.write(c.replace("\"steady_state_throughput_rows_per_sec\": 2500", "\"steady_state_throughput_rows_per_sec\": 100"))
' "$TMP_DIR/docs/chaos-recovery-baseline.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "low steady state throughput was accepted"
fi
grep -q "VIOLATION: Gate 7:" "$TMP_DIR/out.log" || fail "Gate 7 low throughput violation was not reported"
rm -rf "$TMP_DIR"

echo "OK: check-release-candidate-gate self-test passed."
