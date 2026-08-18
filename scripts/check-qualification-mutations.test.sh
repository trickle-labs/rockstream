#!/usr/bin/env bash
# check-qualification-mutations.test.sh — Mutation self-test for qualification recovery observations
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHECKER="$ROOT/scripts/check-qualification-mutations.sh"
TEST_FILE="$ROOT/crates/rockstream-sim/tests/qualification_recovery_tests.rs"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

# 1. Base check: unmutated suite passes
bash "$CHECKER" >/dev/null || fail "Unmutated recovery observations failed checker"

# 2. Mutation tests: each observation missing must fail
OBSERVATIONS=(
  "test_observe_heartbeat_loss_and_shard_reassignment"
  "test_observe_control_leader_failover"
  "test_observe_fencing_epoch_advancement"
  "test_observe_selected_checkpoint_recovery"
  "test_observe_source_offset_lsn_recovery"
  "test_observe_view_frontier_monotonicity"
  "test_observe_sink_2pc_transaction_atomicity"
  "test_observe_first_post_recovery_query_correctness"
)

mkdir -p "$TMP_DIR/scripts" "$TMP_DIR/crates/rockstream-sim/tests"
cp "$ROOT/scripts/check-qualification-mutations.sh" "$TMP_DIR/scripts/"

for obs in "${OBSERVATIONS[@]}"; do
  # Create mutated file with one observation removed
  grep -v "$obs" "$TEST_FILE" > "$TMP_DIR/crates/rockstream-sim/tests/qualification_recovery_tests.rs"
  if bash "$TMP_DIR/scripts/check-qualification-mutations.sh" >/dev/null 2>&1; then
    fail "Mutation removing '$obs' was erroneously accepted"
  fi
done

echo "OK: All 8 recovery observation mutation checks passed (falsifiability proven)."
