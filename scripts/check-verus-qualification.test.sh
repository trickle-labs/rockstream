#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-verus-qualification.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'RS-0906: FAIL: %s\n' "$1" >&2
  exit 1
}

expect_failure() {
  local output_path="$1"
  shift
  set +e
  "$@" >"$output_path" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    cat "$output_path" >&2
    fail "command unexpectedly succeeded: $*"
  fi
}

mkdir -p "$TMP_ROOT/formal/verus/negative" "$TMP_ROOT/docs/implementation-plans" \
  "$TMP_ROOT/sign-offs/verus" "$TMP_ROOT/.github/workflows" "$TMP_ROOT/scripts" \
  "$TMP_ROOT/benchmarks/r1-local"
cp "$ROOT/formal/verus/qualification.toml" "$ROOT/formal/verus/manifest.toml" \
  "$ROOT/formal/verus/toolchain.lock.toml" "$ROOT/formal/verus/MAINTENANCE.md" \
  "$TMP_ROOT/formal/verus/"
cp "$ROOT/formal/verus/assumptions.toml" "$TMP_ROOT/formal/verus/"
cp "$ROOT/formal/verus/negative/invalid_arithmetic.rs" \
  "$ROOT/formal/verus/negative/invalid_sign_encoding.rs" \
  "$ROOT/formal/verus/negative/invalid_validation.rs" \
  "$ROOT/formal/verus/negative/invalid_publication_guard.rs" \
  "$ROOT/formal/verus/negative/invalid_durable_marker.rs" \
  "$ROOT/formal/verus/negative/unchecked_assumption.rs" \
  "$TMP_ROOT/formal/verus/negative/"
cp "$ROOT/docs/implementation-plans/rockstream-verus-implementation-plan.md" \
  "$TMP_ROOT/docs/implementation-plans/"
cp "$ROOT/sign-offs/verus/VS6.md" "$TMP_ROOT/sign-offs/verus/"
cp "$ROOT/Cargo.toml" "$ROOT/Cargo.lock" "$ROOT/rust-toolchain.toml" "$TMP_ROOT/"
cp "$ROOT/Makefile" "$TMP_ROOT/"
cp "$ROOT/.github/workflows/ci.yml" "$TMP_ROOT/.github/workflows/"
cp "$ROOT/scripts/check-verus-qualification.py" "$TMP_ROOT/scripts/"
cp "$ROOT/scripts/run-verus-qualification.sh" "$ROOT/scripts/record-verus-qualification.py" \
  "$ROOT/scripts/test-verus-production-mutations.py" "$TMP_ROOT/scripts/"
cp "$ROOT/benchmarks/r1-local/profile-v0611.toml" "$ROOT/benchmarks/r1-local/thresholds-v0611.toml" \
  "$TMP_ROOT/benchmarks/r1-local/"
mkdir -p "$TMP_ROOT/crates/rockstream-verified/src" "$TMP_ROOT/crates/rockstream-storage/src"
cp "$ROOT/crates/rockstream-verified/src/arithmetic.rs" \
  "$ROOT/crates/rockstream-verified/src/keys.rs" \
  "$ROOT/crates/rockstream-verified/src/frontier.rs" \
  "$ROOT/crates/rockstream-verified/src/persistence.rs" \
  "$ROOT/crates/rockstream-verified/src/aggregate.rs" \
  "$ROOT/crates/rockstream-verified/src/lib.rs" \
  "$TMP_ROOT/crates/rockstream-verified/src/"
cp "$ROOT/crates/rockstream-storage/src/keys.rs" "$TMP_ROOT/crates/rockstream-storage/src/"

bash "$CHECKER" "$ROOT" >/dev/null || fail "real qualification contract did not pass"

expected="verus qualification: 33 claims classified; 6 builds, 7 smoke/policy controls, and 6 production mutations registered."
actual="$(bash "$CHECKER" "$ROOT")"
[[ "$actual" == "$expected" ]] || fail "positive checker output changed: $actual"

printf 'verifier\t0\t1\tverus smoke\n' >"$TMP_ROOT/steps.tsv"
missing_runtime="$TMP_ROOT/missing-runtime"
expect_failure "$TMP_ROOT/missing-runtime.log" python3 "$TMP_ROOT/scripts/record-verus-qualification.py" \
  --root "$TMP_ROOT" --run-id test --status passed --steps "$TMP_ROOT/steps.tsv" \
  --runtime-artifact "$missing_runtime" --output "$TMP_ROOT/record.json"
missing_runtime_output="$(<"$TMP_ROOT/missing-runtime.log")"
[[ "$missing_runtime_output" == "RS-0906: missing runtime artifact: $missing_runtime" ]] || \
  fail "missing runtime artifact was accepted: $missing_runtime_output"

sed -i.bak 's/^id = "VS0-02"$/id = "VS9-99"/' "$TMP_ROOT/formal/verus/qualification.toml"
expect_failure "$TMP_ROOT/unknown.log" python3 "$TMP_ROOT/scripts/check-verus-qualification.py" --root "$TMP_ROOT"
unknown_output="$(<"$TMP_ROOT/unknown.log")"
expected_unknown=$'verus qualification: RS-0906: claim evidence references unknown claim: VS9-99\nverus qualification: RS-0906: claim has no VS6 evidence record: VS0-02'
if [[ "$unknown_output" != "$expected_unknown" ]]; then
  fail "unknown claim evidence was accepted"
fi

cp "$ROOT/formal/verus/qualification.toml" "$TMP_ROOT/formal/verus/qualification.toml"
sed -i.bak '/id = "VS0-02"/,/qualification_evidence =/ s/^qualification_evidence = "blocked:/qualification_evidence = "skipped:/' \
  "$TMP_ROOT/formal/verus/qualification.toml"
expect_failure "$TMP_ROOT/skipped.log" python3 "$TMP_ROOT/scripts/check-verus-qualification.py" --root "$TMP_ROOT"
skipped_output="$(<"$TMP_ROOT/skipped.log")"
if [[ "$skipped_output" != "verus qualification: RS-0906: VS0-02 marks qualification_evidence as skipped; use blocked" ]]; then
  fail "skipped evidence was accepted"
fi

cp "$ROOT/formal/verus/qualification.toml" "$TMP_ROOT/formal/verus/qualification.toml"
cp "$ROOT/formal/verus/manifest.toml" "$TMP_ROOT/formal/verus/manifest.toml"
sed -i.bak '/id = "VS0-02"/,/status =/ s/status = "kernel-verified"/status = "release-qualified"/' \
  "$TMP_ROOT/formal/verus/manifest.toml"
expect_failure "$TMP_ROOT/release.log" python3 "$TMP_ROOT/scripts/check-verus-qualification.py" --root "$TMP_ROOT"
release_output="$(<"$TMP_ROOT/release.log")"
if [[ "$release_output" != "verus qualification: RS-0906: release-qualified claim VS0-02 has blocked qualification evidence" ]]; then
  fail "release-qualified blocked evidence was accepted"
fi

cp "$ROOT/formal/verus/qualification.toml" "$TMP_ROOT/formal/verus/qualification.toml"
cp "$ROOT/formal/verus/manifest.toml" "$TMP_ROOT/formal/verus/manifest.toml"
python3 -c 'from pathlib import Path; import re,sys; p=Path(sys.argv[1]); s,n=re.subn(r"(?ms)^\[\[production_mutations\]\]\nid = \"PM06\".*?(?=^\[\[claim_evidence\]\])", "", p.read_text()); assert n==1; p.write_text(s)' \
  "$TMP_ROOT/formal/verus/qualification.toml"
expect_failure "$TMP_ROOT/missing-production.log" python3 "$TMP_ROOT/scripts/check-verus-qualification.py" --root "$TMP_ROOT"
[[ "$(<"$TMP_ROOT/missing-production.log")" == "verus qualification: RS-0906: exactly six production mutations are required" ]] || \
  fail "missing production mutation was accepted: $(<"$TMP_ROOT/missing-production.log")"

cp "$ROOT/formal/verus/qualification.toml" "$TMP_ROOT/formal/verus/qualification.toml"
sed -i.bak '/id = "PM06"/,/required = true/s/required = true/required = false/' "$TMP_ROOT/formal/verus/qualification.toml"
expect_failure "$TMP_ROOT/skipped-production.log" python3 "$TMP_ROOT/scripts/check-verus-qualification.py" --root "$TMP_ROOT"
[[ "$(<"$TMP_ROOT/skipped-production.log")" == "verus qualification: RS-0906: PM06 must require a verifier error" ]] || \
  fail "skipped production mutation was accepted"

echo "OK: VS6 qualification checker rejects unknown, skipped, and blocked release evidence."

# Setup temporary shims for runner regression tests
SHIM_BIN="$TMP_ROOT/bin"
mkdir -p "$SHIM_BIN"

REAL_CP="$(command -v cp)"

printf '#!/usr/bin/env bash\nexit 0\n' > "$SHIM_BIN/cargo-verus"
chmod +x "$SHIM_BIN/cargo-verus"

printf '#!/usr/bin/env bash\nif [ "${1:-}" = "--version" ]; then\n  echo "verus 0.2026.09.13"\n  exit 0\nfi\nexit 0\n' > "$SHIM_BIN/verus"
chmod +x "$SHIM_BIN/verus"

printf '#!/usr/bin/env bash\n' > "$SHIM_BIN/cargo"
printf 'if [ "${1:-}" = "--version" ]; then\n' >> "$SHIM_BIN/cargo"
printf '  echo "cargo 1.88.0"\n' >> "$SHIM_BIN/cargo"
printf '  exit 0\n' >> "$SHIM_BIN/cargo"
printf 'fi\n' >> "$SHIM_BIN/cargo"
printf 'if [ "${1:-}" = "verus" ]; then\n' >> "$SHIM_BIN/cargo"
printf '  if [ -n "${VERUS_MUTATION_ID:-}" ]; then\n' >> "$SHIM_BIN/cargo"
printf '    if [ "${MOCK_VERUS_MODE:-}" = "compile_error" ]; then\n' >> "$SHIM_BIN/cargo"
printf '      echo "error: simulated compiler failure"\n' >> "$SHIM_BIN/cargo"
printf '      exit 1\n' >> "$SHIM_BIN/cargo"
printf '    fi\n' >> "$SHIM_BIN/cargo"
printf '    case "$VERUS_MUTATION_ID" in\n' >> "$SHIM_BIN/cargo"
printf '      PM01|PM03) target="crates/rockstream-verified/src/frontier.rs" ;;\n' >> "$SHIM_BIN/cargo"
printf '      PM02|PM04|PM05) target="crates/rockstream-verified/src/persistence.rs" ;;\n' >> "$SHIM_BIN/cargo"
printf '      PM06) target="crates/rockstream-verified/src/aggregate.rs" ;;\n' >> "$SHIM_BIN/cargo"
printf '    esac\n' >> "$SHIM_BIN/cargo"
printf '    echo "$target: assertion failed"\n' >> "$SHIM_BIN/cargo"
printf '    echo "verification results:: 67 verified, 1 errors"\n' >> "$SHIM_BIN/cargo"
printf '    exit 1\n' >> "$SHIM_BIN/cargo"
printf '  fi\n' >> "$SHIM_BIN/cargo"
printf '  echo "verification results:: 68 verified, 0 errors"\n' >> "$SHIM_BIN/cargo"
printf '  exit 0\n' >> "$SHIM_BIN/cargo"
printf 'fi\n' >> "$SHIM_BIN/cargo"
printf 'target_prefix=""\n' >> "$SHIM_BIN/cargo"
printf 'is_release=0\n' >> "$SHIM_BIN/cargo"
printf 'is_build=0\n' >> "$SHIM_BIN/cargo"
printf 'prev=""\n' >> "$SHIM_BIN/cargo"
printf 'for arg in "$@"; do\n' >> "$SHIM_BIN/cargo"
printf '  if [ "$prev" = "--target" ]; then\n' >> "$SHIM_BIN/cargo"
printf '    target_prefix="${arg}/"\n' >> "$SHIM_BIN/cargo"
printf '  fi\n' >> "$SHIM_BIN/cargo"
printf '  if [ "$arg" = "--release" ]; then\n' >> "$SHIM_BIN/cargo"
printf '    is_release=1\n' >> "$SHIM_BIN/cargo"
printf '  elif [ "$arg" = "build" ]; then\n' >> "$SHIM_BIN/cargo"
printf '    is_build=1\n' >> "$SHIM_BIN/cargo"
printf '  fi\n' >> "$SHIM_BIN/cargo"
printf '  prev="$arg"\n' >> "$SHIM_BIN/cargo"
printf 'done\n' >> "$SHIM_BIN/cargo"
printf 'base="${CARGO_TARGET_DIR:-target}"\n' >> "$SHIM_BIN/cargo"
printf 'if [ "$is_release" -eq 1 ]; then\n' >> "$SHIM_BIN/cargo"
printf '  mkdir -p "$base/${target_prefix}release"\n' >> "$SHIM_BIN/cargo"
printf '  printf "mock-release-binary\\n" > "$base/${target_prefix}release/rockstream"\n' >> "$SHIM_BIN/cargo"
printf '  chmod +x "$base/${target_prefix}release/rockstream"\n' >> "$SHIM_BIN/cargo"
printf '  exit 0\n' >> "$SHIM_BIN/cargo"
printf 'fi\n' >> "$SHIM_BIN/cargo"
printf 'if [ "$is_build" -eq 1 ]; then\n' >> "$SHIM_BIN/cargo"
printf '  mkdir -p "$base/${target_prefix}debug"\n' >> "$SHIM_BIN/cargo"
printf '  printf "mock-debug-binary\\n" > "$base/${target_prefix}debug/rockstream"\n' >> "$SHIM_BIN/cargo"
printf '  chmod +x "$base/${target_prefix}debug/rockstream"\n' >> "$SHIM_BIN/cargo"
printf '  exit 0\n' >> "$SHIM_BIN/cargo"
printf 'fi\n' >> "$SHIM_BIN/cargo"
printf 'exit 0\n' >> "$SHIM_BIN/cargo"
chmod +x "$SHIM_BIN/cargo"

printf '#!/usr/bin/env bash\n' > "$SHIM_BIN/cp"
printf 'case "${MOCK_CP_MODE:-normal}" in\n' >> "$SHIM_BIN/cp"
printf '  fail)\n' >> "$SHIM_BIN/cp"
printf '    echo "cp: simulated copy failure" >&2\n' >> "$SHIM_BIN/cp"
printf '    exit 1\n' >> "$SHIM_BIN/cp"
printf '    ;;\n' >> "$SHIM_BIN/cp"
printf '  fail_release)\n' >> "$SHIM_BIN/cp"
printf '    for arg in "$@"; do\n' >> "$SHIM_BIN/cp"
printf '      if [[ "$arg" == *release* ]]; then\n' >> "$SHIM_BIN/cp"
printf '        echo "cp: simulated release copy failure" >&2\n' >> "$SHIM_BIN/cp"
printf '        exit 1\n' >> "$SHIM_BIN/cp"
printf '      fi\n' >> "$SHIM_BIN/cp"
printf '    done\n' >> "$SHIM_BIN/cp"
printf '    exec "%s" "$@"\n' "$REAL_CP" >> "$SHIM_BIN/cp"
printf '    ;;\n' >> "$SHIM_BIN/cp"
printf '  fail_debug)\n' >> "$SHIM_BIN/cp"
printf '    for arg in "$@"; do\n' >> "$SHIM_BIN/cp"
printf '      if [[ "$arg" == *debug* ]]; then\n' >> "$SHIM_BIN/cp"
printf '        echo "cp: simulated debug copy failure" >&2\n' >> "$SHIM_BIN/cp"
printf '        exit 1\n' >> "$SHIM_BIN/cp"
printf '      fi\n' >> "$SHIM_BIN/cp"
printf '    done\n' >> "$SHIM_BIN/cp"
printf '    exec "%s" "$@"\n' "$REAL_CP" >> "$SHIM_BIN/cp"
printf '    ;;\n' >> "$SHIM_BIN/cp"
printf '  partial)\n' >> "$SHIM_BIN/cp"
printf '    target="${!#}"\n' >> "$SHIM_BIN/cp"
printf '    printf "partial-corrupted-content\\n" > "$target"\n' >> "$SHIM_BIN/cp"
printf '    echo "cp: simulated partial copy failure" >&2\n' >> "$SHIM_BIN/cp"
printf '    exit 1\n' >> "$SHIM_BIN/cp"
printf '    ;;\n' >> "$SHIM_BIN/cp"
printf '  *)\n' >> "$SHIM_BIN/cp"
printf '    exec "%s" "$@"\n' "$REAL_CP" >> "$SHIM_BIN/cp"
printf '    ;;\n' >> "$SHIM_BIN/cp"
printf 'esac\n' >> "$SHIM_BIN/cp"
chmod +x "$SHIM_BIN/cp"

"$REAL_CP" "$ROOT/crates/rockstream-verified/src/frontier.rs" "$TMP_ROOT/original-frontier.rs"
COMPILE_ERROR_OUT="$TMP_ROOT/compile-error-evidence"
expect_failure "$TMP_ROOT/compile-error.log" env PATH="$SHIM_BIN:$PATH" \
  MOCK_VERUS_MODE="compile_error" VERUS_MUTATION_OUTPUT="$COMPILE_ERROR_OUT" \
  python3 "$ROOT/scripts/test-verus-production-mutations.py"
expected_compile_error="production mutations: PM01: intended verifier rejection was not observed; full log: $COMPILE_ERROR_OUT/PM01.log"
[[ "$(<"$TMP_ROOT/compile-error.log")" == "$expected_compile_error" ]] || fail "compiler error counted as mutation detection"
[[ "$(<"$COMPILE_ERROR_OUT/PM01.log")" == "error: simulated compiler failure" ]] || fail "compiler failure log was lost"
cmp -s "$ROOT/crates/rockstream-verified/src/frontier.rs" "$TMP_ROOT/original-frontier.rs" || \
  fail "production mutation changed the source worktree"
# Exercise legacy all-phase artifact failure cases independent of CI's split phase.
export VERUS_QUALIFICATION_PHASE=all

# Case A — stale destination
CASE_A_OUT="$TMP_ROOT/case-a-out"
mkdir -p "$CASE_A_OUT"
printf 'stale-release-data\n' > "$CASE_A_OUT/rockstream-release"
printf 'stale-debug-data\n' > "$CASE_A_OUT/rockstream-debug"

set +e
PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE="fail" VERUS_QUALIFICATION_OUTPUT="$CASE_A_OUT" \
  bash "$ROOT/scripts/run-verus-qualification.sh" >"$CASE_A_OUT/run.log" 2>&1
case_a_status=$?
set -e

[[ "$case_a_status" -ne 0 ]] || fail "Case A: runner unexpectedly succeeded when cp failed"
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
if data.get("status") == "passed":
    sys.exit(1)
if "rockstream-release" in data.get("runtime_artifacts", {}) or "rockstream-debug" in data.get("runtime_artifacts", {}):
    sys.exit(2)
sys.exit(0)
' "$CASE_A_OUT/qualification.json" || fail "Case A: stale destination accepted or status recorded as passed"
[[ ! -f "$CASE_A_OUT/rockstream-release" ]] || fail "Case A: stale release file was not cleared"
[[ ! -f "$CASE_A_OUT/rockstream-debug" ]] || fail "Case A: stale debug file was not cleared"

# Case B — partial destination
CASE_B_OUT="$TMP_ROOT/case-b-out"
mkdir -p "$CASE_B_OUT"

set +e
PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE="partial" VERUS_QUALIFICATION_OUTPUT="$CASE_B_OUT" \
  bash "$ROOT/scripts/run-verus-qualification.sh" >"$CASE_B_OUT/run.log" 2>&1
case_b_status=$?
set -e

[[ "$case_b_status" -ne 0 ]] || fail "Case B: runner unexpectedly succeeded on partial copy"
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
if data.get("status") == "passed":
    sys.exit(1)
if "rockstream-release" in data.get("runtime_artifacts", {}) or "rockstream-debug" in data.get("runtime_artifacts", {}):
    sys.exit(2)
sys.exit(0)
' "$CASE_B_OUT/qualification.json" || fail "Case B: partial destination accepted or status recorded as passed"
[[ ! -f "$CASE_B_OUT/rockstream-release" ]] || fail "Case B: partial release file was staged"
[[ ! -f "$CASE_B_OUT/rockstream-debug" ]] || fail "Case B: partial debug file was staged"
leftover_case_b=$(find "$CASE_B_OUT" -name "rockstream-*.tmp.*" | wc -l)
[[ "$leftover_case_b" -eq 0 ]] || fail "Case B: temporary file was not cleaned up on failure"

# Failure to copy release artifact causes non-zero exit and status != passed
CASE_FAIL_REL_OUT="$TMP_ROOT/case-fail-rel-out"
mkdir -p "$CASE_FAIL_REL_OUT"
set +e
PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE="fail_release" VERUS_QUALIFICATION_OUTPUT="$CASE_FAIL_REL_OUT" \
  bash "$ROOT/scripts/run-verus-qualification.sh" >"$CASE_FAIL_REL_OUT/run.log" 2>&1
case_fail_rel_status=$?
set -e
[[ "$case_fail_rel_status" -ne 0 ]] || fail "Failure to copy release artifact did not cause non-zero exit"
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
if data.get("status") == "passed":
    sys.exit(1)
if "rockstream-release" in data.get("runtime_artifacts", {}):
    sys.exit(2)
if "rockstream-debug" not in data.get("runtime_artifacts", {}):
    sys.exit(3)
sys.exit(0)
' "$CASE_FAIL_REL_OUT/qualification.json" || fail "Release failure did not record proper failed status or staged debug artifact"

# Failure to copy debug artifact causes non-zero exit and status != passed
CASE_FAIL_DBG_OUT="$TMP_ROOT/case-fail-dbg-out"
mkdir -p "$CASE_FAIL_DBG_OUT"
set +e
PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE="fail_debug" VERUS_QUALIFICATION_OUTPUT="$CASE_FAIL_DBG_OUT" \
  bash "$ROOT/scripts/run-verus-qualification.sh" >"$CASE_FAIL_DBG_OUT/run.log" 2>&1
case_fail_dbg_status=$?
set -e
[[ "$case_fail_dbg_status" -ne 0 ]] || fail "Failure to copy debug artifact did not cause non-zero exit"
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
if data.get("status") == "passed":
    sys.exit(1)
if "rockstream-debug" in data.get("runtime_artifacts", {}):
    sys.exit(2)
if "rockstream-release" not in data.get("runtime_artifacts", {}):
    sys.exit(3)
sys.exit(0)
' "$CASE_FAIL_DBG_OUT/qualification.json" || fail "Debug failure did not record proper failed status or staged release artifact"

# Case C — normal success
CASE_C_OUT="$TMP_ROOT/case-c-out"
mkdir -p "$CASE_C_OUT"

set +e
PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE="normal" VERUS_QUALIFICATION_OUTPUT="$CASE_C_OUT" \
  bash "$ROOT/scripts/run-verus-qualification.sh" >"$CASE_C_OUT/run.log" 2>&1
case_c_status=$?
set -e

[[ "$case_c_status" -eq 0 ]] || fail "Case C: runner unexpectedly failed: $(<"$CASE_C_OUT/run.log")"
[[ -f "$CASE_C_OUT/rockstream-release" ]] || fail "Case C: rockstream-release missing"
[[ -f "$CASE_C_OUT/rockstream-debug" ]] || fail "Case C: rockstream-debug missing"
python3 -c '
import hashlib, json, sys
data = json.load(open(sys.argv[1]))
if data.get("status") != "passed":
    sys.exit(1)
for name in ("rockstream-release", "rockstream-debug"):
    with open(f"{sys.argv[2]}/{name}", "rb") as f:
        expected = hashlib.sha256(f.read()).hexdigest()
    actual = data.get("runtime_artifacts", {}).get(name)
    if actual != expected:
        sys.exit(2)
sys.exit(0)
' "$CASE_C_OUT/qualification.json" "$CASE_C_OUT" || fail "Case C: recorded digests do not match staged files"

echo "OK: VS6 qualification runner enforces atomic fail-closed artifact staging."

# Each phase must contain exactly its own commands and binary, never a full claim.
for phase in debug release; do
  phase_out="$TMP_ROOT/phases/$phase"
  mkdir -p "$phase_out"
  PATH="$SHIM_BIN:$PATH" MOCK_CP_MODE=normal VERUS_QUALIFICATION_PHASE="$phase" \
    VERUS_QUALIFICATION_OUTPUT="$phase_out" VERUS_QUALIFICATION_TARGET=x86_64-unknown-linux-gnu \
    bash "$ROOT/scripts/run-verus-qualification.sh" >"$phase_out/run.log" 2>&1 || \
    fail "qualification phase $phase failed: $(<"$phase_out/run.log")"
  [[ ! -f "$phase_out/qualification.json" ]] || fail "partial phase emitted full qualification"
done
PATH="$SHIM_BIN:$PATH" python3 "$ROOT/scripts/record-verus-qualification.py" \
  --root "$ROOT" --run-id parallel-test --status passed --merge "$TMP_ROOT/phases" \
  --target x86_64-unknown-linux-gnu --output "$TMP_ROOT/complete/qualification.json" >/dev/null
python3 -c '
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
records = {phase: json.loads((root / "phases" / phase / "phase.json").read_text()) for phase in ("debug", "release")}
commands = [
    "env RUSTUP_TOOLCHAIN=1.98.1 cargo verus verify -p rockstream-verified --locked --target x86_64-unknown-linux-gnu",
    "env VERUS_MUTATION_OUTPUT=" + str(root / "phases/debug/mutations") + " python3 " + str(pathlib.Path.cwd() / "scripts/test-verus-production-mutations.py"),
    "env CARGO_INCREMENTAL=1 RUSTUP_TOOLCHAIN=1.98.1 cargo verus verify -p rockstream-verified --locked --target x86_64-unknown-linux-gnu",
    "env RUSTUP_TOOLCHAIN=1.88 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace --locked --target x86_64-unknown-linux-gnu",
    "env RUSTUP_TOOLCHAIN=1.88 cargo check --workspace --locked --target x86_64-unknown-linux-gnu",
    "env RUSTUP_TOOLCHAIN=1.88 cargo check --workspace --all-features --locked --target x86_64-unknown-linux-gnu",
    "env RUSTUP_TOOLCHAIN=1.88 cargo build --workspace --locked --release --target x86_64-unknown-linux-gnu",
]
for phase, expected in (("debug", commands[:6]), ("release", commands[6:])):
    assert records[phase]["phase"] == phase
    assert [step.split("\t", 3)[3] for step in records[phase]["steps"]] == expected
    assert list(records[phase]["runtime_artifacts"]) == ["rockstream-" + phase]
complete = json.loads((root / "complete/qualification.json").read_text())
assert complete["phase"] == "all" and complete["status"] == "passed"
assert complete["tool_versions"] == records["debug"]["tool_versions"]
assert complete["phase_tool_versions"] == {phase: record["tool_versions"] for phase, record in records.items()}
assert complete["steps"] == records["debug"]["steps"] + records["release"]["steps"]
assert complete["runtime_artifacts"] == {**records["debug"]["runtime_artifacts"], **records["release"]["runtime_artifacts"]}
import hashlib
mutation_dir = root / "phases/debug/mutations"
assert complete["production_mutations"] == {
    "mutations/" + path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in mutation_dir.iterdir()
}
assert complete["production_mutations"] == {
    "mutations/" + path.name: hashlib.sha256(path.read_bytes()).hexdigest()
    for path in (root / "complete/mutations").iterdir()
}
' "$TMP_ROOT" || fail "parallel phase evidence differs from the full qualification commands"
python3 -c 'import json, pathlib, sys; p=pathlib.Path(sys.argv[1]); d=json.loads(p.read_text()); d["status"]="failed"; p.write_text(json.dumps(d))' \
  "$TMP_ROOT/phases/release/phase.json"
expect_failure "$TMP_ROOT/merge-failed.log" python3 "$ROOT/scripts/record-verus-qualification.py" \
  --root "$ROOT" --run-id parallel-test --status passed --merge "$TMP_ROOT/phases" \
  --target x86_64-unknown-linux-gnu --output "$TMP_ROOT/complete/qualification.json"
[[ "$(<"$TMP_ROOT/merge-failed.log")" == "RS-0906: incomplete qualification: release: qualification identity or status mismatch" ]] || \
  fail "phase rejection output changed: $(<"$TMP_ROOT/merge-failed.log")"
[[ ! -f "$TMP_ROOT/complete/qualification.json" ]] || fail "failed merge retained stale passing qualification"
python3 -B "$ROOT/scripts/record-verus-qualification.test.py"
echo "OK: VS6 parallel phases assemble only complete source-bound evidence."

