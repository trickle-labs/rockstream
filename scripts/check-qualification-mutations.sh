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

if [ "$VIOLATIONS" -gt 0 ]; then
  echo "FAIL: $VIOLATIONS missing recovery observation tests found." >&2
  exit 1
fi

echo "OK: All 8 qualification recovery observation tests present and verified."
