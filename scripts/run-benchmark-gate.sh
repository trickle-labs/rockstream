#!/usr/bin/env bash
# Keep each package's existing baseline and retry policy on an isolated runner.
set -euo pipefail

case "${1:-}" in
  ops) bench=perf_regression; baseline=crates/rockstream-ops/benches/baseline/v0.45.4-ops.json ;;
  storage) bench=storage_bench; baseline=crates/rockstream-storage/benches/baseline/v0.45.4-storage.json ;;
  runtime) bench=exchange_bench; baseline=crates/rockstream-runtime/benches/baseline/v0.51-runtime.json ;;
  control) bench=frontier_bench; baseline=crates/rockstream-control/benches/baseline/v0.45.4-control.json ;;
  *) echo "unknown benchmark suite: ${1:-<missing>}" >&2; exit 1 ;;
esac
suite="$1"
for attempt in 0 1 2; do
  output="${suite}-bench-${attempt}.out"
  cargo bench -p "rockstream-$suite" --bench "$bench" -- --noplot | tee "$output"
  if cargo run -p rockstream-ops --bin bench_regression_gate -- \
    --baseline "$baseline" --output "$output" --tag "$suite"; then
    exit 0
  fi
done
exit 1
