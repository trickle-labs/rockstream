#!/usr/bin/env bash
# Self-test the capability contract gate against deliberate source and output
# mutations, then verify the unmodified tree still passes.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-capability-contract.sh"
TMP_ROOT="$ROOT/.capability-contract-test.$$"
mkdir "$TMP_ROOT"
OUT="$TMP_ROOT/output"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

cp "$ROOT/capabilities.toml" "$TMP_ROOT/"
cp "$ROOT/NEW_ROADMAP.md" "$TMP_ROOT/"
cp "$ROOT/README.md" "$ROOT/DESIGN.md" "$ROOT/NEW_IMPLEMENTATION_PLAN.md" "$TMP_ROOT/"
cp -R "$ROOT/crates" "$TMP_ROOT/"
cp -R "$ROOT/docs" "$TMP_ROOT/"
mkdir -p "$TMP_ROOT/scripts"
cp "$ROOT/scripts/generate-capability-matrix.py" "$TMP_ROOT/scripts/"

run_bad() {
  local label="$1"
  local expected="${2:-VIOLATION:}"
  if bash "$CHECKER" "$TMP_ROOT" >"$OUT" 2>&1; then
    cat "$OUT"
    fail "$label mutation was accepted"
  fi
  grep -q "$expected" "$OUT" || {
    cat "$OUT"
    fail "$label mutation did not report the expected violation"
  }
}

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("tier = \"Core\"", "tier = \"\"", 1))'
run_bad "missing tier"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("proof = \"crates/rockstream-gateway/tests/gateway_dml_tests.rs::test_create_view_and_select\"", "proof = \"\"", 1))'
run_bad "missing Core proof"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("proof = \"crates/rockstream-gateway/tests/gateway_dml_tests.rs::test_create_view_and_select\"", "proof = \"crates/rockstream-gateway/tests/missing.rs::missing_proof\"", 1))'
run_bad "unresolvable proof target"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("behavior = \"incremental\"", "behavior = \"removed_behavior\"", 1))'
run_bad "missing Core semantic behavior" "missing Core semantic behavior incremental"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("core_query_read_incremental", "", 1))'
run_bad "missing Core semantic proof" "has an invalid proof target"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/docs/language-features.md"); s=p.read_text(); p.write_text(s.replace("A committed row delta", "A drifted row delta", 1))'
run_bad "generated Core semantics drift" "generated Core semantics blocks are not byte-identical"
cp "$ROOT/docs/language-features.md" "$TMP_ROOT/docs/language-features.md"

python3 -c 'from pathlib import Path; import re; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(re.sub(r"^reason = .*$", "reason = \"\"", s, count=1, flags=re.M))'
run_bad "missing tier decision reason" "tier decision is missing reason"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("dispatch = [\"query_async_entry\", \"sql_lowering\", \"response_encoding\"]", "dispatch = []", 1))'
run_bad "Core entry without dispatch evidence"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("query_async_entry", "missing_dispatch", 1))'
run_bad "unknown dispatch identity"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

printf '\nmutation\n' >> "$TMP_ROOT/docs/capability-matrix.md"
run_bad "generated matrix drift"
cp "$ROOT/docs/capability-matrix.md" "$TMP_ROOT/docs/capability-matrix.md"

python3 -c 'from pathlib import Path; import re; root=Path("'"$TMP_ROOT"'"); version=re.search(r"^version = \"([^\"]+)\"$", (root/"capabilities.toml").read_text(), re.M).group(1); p=root/"NEW_ROADMAP.md"; s=p.read_text(); mutated=re.sub(rf"^\| {re.escape(version)} \|(?=.*✅ Done)", "| v0.590 |", s, count=1, flags=re.M); assert mutated != s; p.write_text(mutated)'
run_bad "roadmap drift"
cp "$ROOT/NEW_ROADMAP.md" "$TMP_ROOT/NEW_ROADMAP.md"


python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/README.md"); s=p.read_text(); p.write_text(s.replace("RockStream ingests changing data", "RockStream mutates changing data", 1))'
run_bad "promise drift"
cp "$ROOT/README.md" "$TMP_ROOT/README.md"

printf '%s\n' \
  '' \
  '[[capability]]' \
  'id = "connector.synthetic"' \
  'kind = "connector"' \
  'name = "Synthetic connector"' \
  'tier = "Maintain"' \
  'reachability = "Connector runtime"' \
  'dispatch = []' \
  'proof = ""' \
  'documentation = "docs/connectors.md#synthetic"' \
  >> "$TMP_ROOT/capabilities.toml"
run_bad "unknown connector"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("proof_levels_achieved = [\"L0\", \"L1\", \"L2\", \"L3\", \"L5\"]\nmin_proof_level = [\"L0\", \"L1\", \"L2\", \"L3\", \"L5\"]\ndocumentation = \"docs/language-features.md#implemented-today\"\nbehavior = [\n  { behavior = \"incremental\", statement = \"INSERT, UPDATE,", "proof_levels_achieved = [\"L0\", \"L1\", \"L2\", \"L3\"]\nmin_proof_level = [\"L0\", \"L1\", \"L2\", \"L3\", \"L5\"]\ndocumentation = \"docs/language-features.md#implemented-today\"\nbehavior = [\n  { behavior = \"incremental\", statement = \"INSERT, UPDATE,", 1))'
run_bad "missing_proof_level_is_rejected" "missing required proof level"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("postgres_cdc_invalid_slot_fails_closed", "removed_proof", 1))'
run_bad "missing connector failure proof" "proof target does not resolve"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("{ behavior = \"incremental\", statement = \"A PostgreSQL logical replication delta", "{ behavior = \"dropped_behavior\", statement = \"A PostgreSQL logical replication delta", 1))'
run_bad "missing connector incremental behavior" "missing Core semantic behavior incremental"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("kafka_sink_buffer_bounded_with_fill_metric", "missing_sink_metric", 1))'
run_bad "missing sink state growth proof" "proof target does not resolve"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

if ! python3 "$ROOT/scripts/check-capability-contract.py" "$ROOT" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "unmodified tree failed the capability contract check without --full-semantics"
fi

if ! bash "$CHECKER" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "unmodified tree failed the capability contract check with --full-semantics"
fi

echo "OK: check-capability-contract.sh self-test passed."
