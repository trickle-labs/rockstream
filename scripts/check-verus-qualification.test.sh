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
cp "$ROOT/scripts/run-verus-qualification.sh" "$ROOT/scripts/record-verus-qualification.py" "$TMP_ROOT/scripts/"
cp "$ROOT/benchmarks/r1-local/profile-v0611.toml" "$ROOT/benchmarks/r1-local/thresholds-v0611.toml" \
  "$TMP_ROOT/benchmarks/r1-local/"
mkdir -p "$TMP_ROOT/crates/rockstream-verified/src" "$TMP_ROOT/crates/rockstream-storage/src"
cp "$ROOT/crates/rockstream-verified/src/arithmetic.rs" \
  "$ROOT/crates/rockstream-verified/src/keys.rs" \
  "$ROOT/crates/rockstream-verified/src/frontier.rs" \
  "$ROOT/crates/rockstream-verified/src/persistence.rs" \
  "$ROOT/crates/rockstream-verified/src/lib.rs" \
  "$TMP_ROOT/crates/rockstream-verified/src/"
cp "$ROOT/crates/rockstream-storage/src/keys.rs" "$TMP_ROOT/crates/rockstream-storage/src/"

bash "$CHECKER" "$ROOT" >/dev/null || fail "real qualification contract did not pass"

expected="verus qualification: 30 claims classified; 6 builds and 7 mutation controls registered."
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

echo "OK: VS6 qualification checker rejects unknown, skipped, and blocked release evidence."
