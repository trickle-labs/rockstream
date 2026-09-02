#!/usr/bin/env bash
# check-qualification-mutations.sh — Verify qualification recovery observation falsifiability
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TEST_FILE="$ROOT/crates/rockstream-sim/tests/qualification_recovery_tests.rs"

if [ ! -f "$TEST_FILE" ]; then
  echo "FAIL: Qualification recovery test file not found at $TEST_FILE" >&2
  exit 1
fi

REQUIRED_OBSERVATIONS=(
  "test_observe_heartbeat_loss_and_shard_reassignment"
  "test_observe_control_leader_failover"
  "test_observe_fencing_epoch_advancement"
  "test_observe_selected_checkpoint_recovery"
  "test_observe_source_offset_lsn_recovery"
  "test_observe_view_frontier_monotonicity"
  "test_observe_sink_2pc_transaction_atomicity"
  "test_observe_first_post_recovery_query_correctness"
)

VIOLATIONS=0
for obs in "${REQUIRED_OBSERVATIONS[@]}"; do
  if ! grep -q "$obs" "$TEST_FILE"; then
    echo "VIOLATION: Missing required recovery observation test: $obs" >&2
    VIOLATIONS=$((VIOLATIONS + 1))
  fi
done

REQUIRED_MUTATIONS=(
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

MUTATION_TEST_FILE="$ROOT/crates/rockstream-types/tests/qualification_harness_mutation_tests.rs"
if [ ! -f "$MUTATION_TEST_FILE" ]; then
  echo "FAIL: Qualification mutation test file not found at $MUTATION_TEST_FILE" >&2
  exit 1
fi

for mut in "${REQUIRED_MUTATIONS[@]}"; do
  if ! grep -q "$mut" "$MUTATION_TEST_FILE"; then
    echo "VIOLATION: Missing required anti-cheat mutation test: $mut" >&2
    VIOLATIONS=$((VIOLATIONS + 1))
  fi
done

if [ "$VIOLATIONS" -gt 0 ]; then
  echo "FAIL: $VIOLATIONS missing recovery observation or anti-cheat mutation tests found." >&2
  exit 1
fi

echo "OK: All 8 qualification recovery observation tests and 11 anti-cheat mutation tests present and verified."
