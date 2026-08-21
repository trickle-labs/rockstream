#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS=(cargo run --manifest-path "$ROOT/tools/r1-local-harness/Cargo.toml" --locked --)

case "${1:-}" in
  digest)
    exec python3 "$ROOT/scripts/check-r1-local-evidence.py" --digest "$ROOT"
    ;;
  verify)
    exec python3 "$ROOT/scripts/check-r1-local-evidence.py" "$ROOT"
    ;;
  candidates)
    exec python3 "$ROOT/scripts/r1-local-candidates.py" record --root "$ROOT"
    ;;
  verify-candidates)
    exec python3 "$ROOT/scripts/r1-local-candidates.py" verify --root "$ROOT"
    ;;
  prepare)
    python3 "$ROOT/scripts/check-r1-local-evidence.py" "$ROOT"
    exec "${HARNESS[@]}" prepare \
      --profile "$ROOT/benchmarks/r1-local/profile.toml" \
      --corpus "$ROOT/benchmarks/r1-local/corpus.toml"
    ;;
  structural)
    python3 "$ROOT/scripts/r1-local-candidates.py" verify --root "$ROOT"
    exec "${HARNESS[@]}" structural --output "$ROOT/evidence/r1-local"
    ;;
  measure)
    python3 "$ROOT/scripts/r1-local-candidates.py" verify --root "$ROOT"
    for workload_workers in \
      "shared-arrangement 1" \
      "factorized-join 1" \
      "ordinary-aggregate 1" \
      "ordinary-join 1" \
      "uniform-worker-scaling 1" \
      "uniform-worker-scaling 2" \
      "uniform-worker-scaling 4"; do
      read -r workload workers <<<"$workload_workers"
      "${HARNESS[@]}" run --workload "$workload" --workers "$workers" --output "$ROOT/evidence/r1-local"
    done
    ;;
  *)
    printf 'Usage: %s {digest|verify|candidates|verify-candidates|prepare|structural|measure}\n' "$0" >&2
    exit 2
    ;;
esac
