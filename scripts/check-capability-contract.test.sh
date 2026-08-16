#!/usr/bin/env bash
# Self-test the capability contract gate against deliberate source and output
# mutations, then verify the unmodified tree still passes.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-capability-contract.sh"
TMP_ROOT="$(mktemp -d)"
OUT="$(mktemp)"
trap 'rm -rf "$TMP_ROOT"; rm -f "$OUT"' EXIT

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
  if bash "$CHECKER" "$TMP_ROOT" >"$OUT" 2>&1; then
    cat "$OUT"
    fail "$label mutation was accepted"
  fi
  grep -q "VIOLATION:" "$OUT" || {
    cat "$OUT"
    fail "$label mutation did not report a violation"
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

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("dispatch = [\"query_async_entry\", \"sql_lowering\", \"response_encoding\"]", "dispatch = []", 1))'
run_bad "Core entry without dispatch evidence"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/capabilities.toml"); s=p.read_text(); p.write_text(s.replace("query_async_entry", "missing_dispatch", 1))'
run_bad "unknown dispatch identity"
cp "$ROOT/capabilities.toml" "$TMP_ROOT/capabilities.toml"

printf '\nmutation\n' >> "$TMP_ROOT/docs/capability-matrix.md"
run_bad "generated matrix drift"
cp "$ROOT/docs/capability-matrix.md" "$TMP_ROOT/docs/capability-matrix.md"

python3 -c 'from pathlib import Path; p=Path("'"$TMP_ROOT"'/NEW_ROADMAP.md"); s=p.read_text(); p.write_text(s.replace("| v0.57 |", "| v0.570 |", 1))'
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

if ! bash "$CHECKER" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "unmodified tree failed the capability contract check"
fi

echo "OK: check-capability-contract.sh self-test passed."
