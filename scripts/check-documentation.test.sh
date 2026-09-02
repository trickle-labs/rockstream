#!/usr/bin/env bash
# check-documentation.test.sh — self-test the documentation admission gate.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-documentation.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

setup_tree() {
  rm -rf "$TMP_ROOT"
  mkdir -p "$TMP_ROOT"
  cp "$ROOT"/*.md "$ROOT/Makefile" "$ROOT/deny.toml" "$ROOT/Cargo.lock" "$ROOT/capabilities.toml" "$TMP_ROOT/"
  cp -R "$ROOT/docs" "$ROOT/.claude" "$ROOT/sign-offs" "$ROOT/examples" "$TMP_ROOT/"
  mkdir -p "$TMP_ROOT/scripts" "$TMP_ROOT/crates/rockstream-cli/tests" "$TMP_ROOT/crates/rockstream-gateway/tests" "$TMP_ROOT/crates/rockstream-sql/tests"
  cp "$ROOT/scripts/check-documentation.sh" "$ROOT/scripts/check-documentation.py" "$ROOT/scripts/check-documentation.test.sh" "$TMP_ROOT/scripts/"
  cp "$ROOT/crates/rockstream-cli/tests/documentation_transcript_tests.rs" "$TMP_ROOT/crates/rockstream-cli/tests/"
  cp "$ROOT/crates/rockstream-gateway/tests/gateway_dml_tests.rs" "$TMP_ROOT/crates/rockstream-gateway/tests/"
  cp "$ROOT/crates/rockstream-gateway/tests/gateway_proof_tests.rs" "$TMP_ROOT/crates/rockstream-gateway/tests/"
  cp "$ROOT/crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs" "$TMP_ROOT/crates/rockstream-gateway/tests/"
  cp "$ROOT/crates/rockstream-gateway/tests/live_exec_tumble_window_durability_lfs_tests.rs" "$TMP_ROOT/crates/rockstream-gateway/tests/"
  cp "$ROOT/crates/rockstream-gateway/tests/serving_path_aggregate_matrix_tests.rs" "$TMP_ROOT/crates/rockstream-gateway/tests/"
  cp "$ROOT/crates/rockstream-sql/tests/lfs_catalog.rs" "$TMP_ROOT/crates/rockstream-sql/tests/"
  cp "$ROOT/crates/rockstream-sql/tests/tpch_plans.rs" "$TMP_ROOT/crates/rockstream-sql/tests/"
}

run_bad() {
  local name="$1"
  local expected="$2"
  if bash "$TMP_ROOT/scripts/check-documentation.sh" "$TMP_ROOT" >"$TMP_ROOT/$name.out" 2>&1; then
    cat "$TMP_ROOT/$name.out"
    fail "$name mutation was accepted"
  fi
  [[ "$(<"$TMP_ROOT/$name.out")" == "ERROR: $expected" ]] || {
    cat "$TMP_ROOT/$name.out"
    fail "$name did not report exactly '$expected'"
  }
}

bash "$CHECKER" "$ROOT" >/dev/null || fail "real documentation did not pass"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/README.md"); p.write_text(p.read_text().replace("reference/cli.md", "reference/missing.md", 1))'
run_bad missing_link "missing documentation target: docs/README.md:16 -> reference/missing.md"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/operator.md"); p.write_text(p.read_text().replace("README.md#reference", "README.md#missing"))'
run_bad missing_anchor "missing documentation anchor: docs/operator.md:14 -> README.md#missing"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/schema-evolution.md"); p.write_text(p.read_text() + "\n<!-- claim: missing proof -->\n")'
run_bad unsupported_claim "unsupported documentation claim: docs/schema-evolution.md:47"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/README.md"); p.write_text(p.read_text().replace("A **view** is", "A **pipeline** is", 1))'
run_bad obsolete_term "obsolete term 'pipeline'; use 'view' at docs/README.md:28"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/test-commands.md"); p.write_text(p.read_text().replace("make fmt", "make missing", 1))'
run_bad missing_command "undocumented or nonexistent contributor command: 'make missing'"

setup_tree
python3 -c 'from pathlib import Path; p=Path("'$TMP_ROOT'/docs/reference/cli.md"); p.write_text(p.read_text().replace("# CLI reference", "# stale", 1))'
run_bad stale_reference "stale generated documentation: docs/reference/cli.md"

echo "OK: check-documentation.sh self-test passed."
