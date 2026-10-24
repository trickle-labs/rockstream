#!/usr/bin/env bash
# Pragmatic v0.72.0 supported-path smoke. Captures observed results only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
SHA="$(git rev-parse HEAD)"
if [[ -n "${V072_EXPECTED_SHA:-}" && "$SHA" != "$V072_EXPECTED_SHA" ]]; then
  echo "ERROR: candidate SHA mismatch: $SHA" >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: dirty release candidate worktree" >&2
  exit 1
fi

OUT="$ROOT/target/v072-release-smoke/$SHA"
mkdir -p "$OUT"
printf '%s\n' "$SHA" > "$OUT/source-sha.txt"
rustc --version > "$OUT/compiler.txt"
sha256sum Cargo.lock > "$OUT/lockfile-sha256.txt"
printf 'case\tstatus\tresult\n' > "$OUT/report.tsv"

run_case() {
  local name="$1"
  shift
  local log="$OUT/${name}.log"
  echo "Running: $name"
  if cargo test --locked "$@" >"$log" 2>&1 && \
     grep -Eq '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;' "$log"; then
    printf '%s\tPASS\t%s\n' "$name" "$(grep '^test result:' "$log" | tail -n 1)" >> "$OUT/report.tsv"
  else
    printf '%s\tFAIL\t%s\n' "$name" "inspect $log" >> "$OUT/report.tsv"
    tail -n 70 "$log" >&2
    echo "ERROR: $name did not pass all required tests; see $OUT/report.tsv" >&2
    exit 1
  fi
}

run_case "quota-lifecycle" -p rockstream-runtime --test v072_quota_lifecycle
run_case "source-pressure" -p rockstream-runtime --test v072_source_pressure_tests
run_case "commit-overload" -p rockstream-runtime --test concurrency_limits_overload_tests
run_case "adaptive-epochs" -p rockstream-runtime --test v072_adaptive_epoch_tests
run_case "competing-workloads" -p rockstream-ops --lib saturated_pipeline_does_not_starve_separate_workload
run_case "operator-failure" -p rockstream-ops --lib operator_error_closes_pipeline_before_later_epochs_can_escape
run_case "frontier-failure" -p rockstream-ops --lib rejected_frontier_stops_pipeline_without_publishing_later_batches
run_case "dml-crash-recovery" -p rockstream-gateway --test dml_crash_recovery_tests
run_case "gateway-recovery-fault" -p rockstream-gateway --test recovery_crash_injection_tests
run_case "bounded-cursor-client" -p rockstream-gateway --test v072_bounded_cursor_pgwire_tests
run_case "subscription-auth" -p rockstream-gateway --test v072_subscription_authorization_tests
run_case "committed-subscription" -p rockstream-gateway --test v072_durable_subscription_publication_tests
run_case "revisioned-edits" -p rockstream-gateway --test v072_workload_application_pgwire_tests
run_case "bounded-local-select" -p rockstream-gateway --test v072_local_select_bounds_pgwire_tests
run_case "decoder-malformed-client" -p rockstream-gateway --test v072_pgwire_malformed_frame_tests
run_case "fallback-row-byte-limits" -p rockstream-gateway --lib v072_datafusion_fallback_limits
run_case "storage-scan-bounds" -p rockstream-gateway --lib v072_storage_select_scan_tests

printf 'PASS: %s; logs and report at %s\n' "$SHA" "$OUT"
