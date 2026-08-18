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

# 8. Mutation: candidate version mismatch in Cargo.toml / manifest
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import sys
p = sys.argv[1]
with open(p, "r") as f: c = f.read()
with open(p, "w") as f: f.write(c.replace("version = \"0.59.1\"", "version = \"0.59.0\""))
' "$TMP_DIR/Cargo.toml"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "candidate version mismatch was accepted"
fi
grep -q "VIOLATION: Candidate Identity:" "$TMP_DIR/out.log" || fail "Candidate Identity violation was not reported"
rm -rf "$TMP_DIR"

# 9. Mutation: artifact digest mismatch in evidence manifest
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import json, sys
p = sys.argv[1]
with open(p, "r") as f: d = json.load(f)
for k in d["artifacts"]:
    d["artifacts"][k] = "0" * 64
with open(p, "w") as f: json.dump(d, f, indent=2)
' "$TMP_DIR/docs/evidence-manifest.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "tampered artifact digest in evidence manifest was accepted"
fi
grep -q "VIOLATION: Evidence Manifest: artifact" "$TMP_DIR/out.log" || fail "Evidence manifest digest mismatch was not reported"
rm -rf "$TMP_DIR"

# 10. Mutation: skipped mandatory tests in evidence manifest
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import json, sys
p = sys.argv[1]
with open(p, "r") as f: d = json.load(f)
d["test_results"]["candidate_identity_tests"]["mandatory_skipped"] = 1
with open(p, "w") as f: json.dump(d, f, indent=2)
' "$TMP_DIR/docs/evidence-manifest.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "skipped mandatory tests in evidence manifest was accepted"
fi
grep -q "skipped mandatory tests" "$TMP_DIR/out.log" || fail "Skipped mandatory tests violation was not reported"
rm -rf "$TMP_DIR"

# 11. Mutation: missing raw observation data for summary metrics
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import json, sys
p = sys.argv[1]
with open(p, "r") as f: d = json.load(f)
d["raw_metrics"].clear()
with open(p, "w") as f: json.dump(d, f, indent=2)
' "$TMP_DIR/docs/evidence-manifest.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "missing raw data in evidence manifest was accepted"
fi
grep -q "missing raw observation data" "$TMP_DIR/out.log" || fail "Missing raw data violation was not reported"
rm -rf "$TMP_DIR"

# 12. Mutation: summary metrics mathematical regeneration mismatch
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import json, sys
p = sys.argv[1]
with open(p, "r") as f: d = json.load(f)
d["summary_metrics"]["failure_detection_ms"]["p99"] = 999.0
with open(p, "w") as f: json.dump(d, f, indent=2)
' "$TMP_DIR/docs/evidence-manifest.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "mathematical summary regeneration mismatch was accepted"
fi
grep -q "mismatch: expected" "$TMP_DIR/out.log" || fail "Summary regeneration mismatch violation was not reported"
rm -rf "$TMP_DIR"

# 13. Mutation: target threshold used directly as measured result
TMP_DIR="$(mktemp -d)"
python3 -c '
import shutil, sys
shutil.copytree(sys.argv[1], sys.argv[2], symlinks=True, ignore=shutil.ignore_patterns("target", ".git"), dirs_exist_ok=True)
' "$ROOT" "$TMP_DIR"
python3 -c '
import json, sys
p = sys.argv[1]
with open(p, "r") as f: d = json.load(f)
d["raw_metrics"]["failure_detection_ms"] = [5000.0]
d["summary_metrics"]["failure_detection_ms"] = {
    "p50": 5000.0, "p95": 5000.0, "p99": 5000.0, "mean": 5000.0, "min": 5000.0, "max": 5000.0, "sample_count": 1
}
with open(p, "w") as f: json.dump(d, f, indent=2)
' "$TMP_DIR/docs/evidence-manifest.json"
if bash "$TMP_DIR/scripts/check-release-candidate-gate.sh" "$TMP_DIR" >"$TMP_DIR/out.log" 2>&1; then
  fail "target threshold used as measured result was accepted"
fi
grep -q "target threshold cannot satisfy measured result" "$TMP_DIR/out.log" || fail "Target cannot satisfy measured result was not reported"
rm -rf "$TMP_DIR"

echo "OK: check-release-candidate-gate self-test passed."
