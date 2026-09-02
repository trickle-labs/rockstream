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

mkdir -p "$TMP_DIR/scripts" "$TMP_DIR/crates/rockstream-sim/tests" "$TMP_DIR/crates/rockstream-types/tests"
cp "$ROOT/scripts/check-qualification-mutations.sh" "$TMP_DIR/scripts/"
cp "$ROOT/crates/rockstream-types/tests/qualification_harness_mutation_tests.rs" "$TMP_DIR/crates/rockstream-types/tests/"

for obs in "${OBSERVATIONS[@]}"; do
  # Create mutated file with one observation removed
  grep -v "$obs" "$TEST_FILE" > "$TMP_DIR/crates/rockstream-sim/tests/qualification_recovery_tests.rs"
  if (cd "$TMP_DIR" && bash "$TMP_DIR/scripts/check-qualification-mutations.sh" >/dev/null 2>&1); then
    fail "Mutation removing '$obs' was erroneously accepted"
  fi
done
cp "$TEST_FILE" "$TMP_DIR/crates/rockstream-sim/tests/"

MUTATIONS=(
  "single_process_simulation_rejected"
  "duplicate_worker_id_rejected"
  "idle_worker_rejected"
  "unowned_shard_rejected"
  "generator_saturation_rejected"
  "sink_consumer_lag_rejected"
  "stale_oracle_result_rejected"
  "duplicate_or_lost_sink_output_rejected"
  "constant_timestamp_rejected"
  "skipped_workload_rejected"
  "environment_shift_rejected"
)

MUTATION_SRC="$ROOT/crates/rockstream-types/tests/qualification_harness_mutation_tests.rs"
for mut in "${MUTATIONS[@]}"; do
  grep -v "$mut" "$MUTATION_SRC" > "$TMP_DIR/crates/rockstream-types/tests/qualification_harness_mutation_tests.rs"
  if (cd "$TMP_DIR" && bash "$TMP_DIR/scripts/check-qualification-mutations.sh" >/dev/null 2>&1); then
    fail "Mutation removing '$mut' was erroneously accepted"
  fi
done

echo "OK: All 8 recovery observation and 11 anti-cheat mutation checks passed (falsifiability proven)."
