#!/usr/bin/env bash
# Run the same cheap admission checks locally and before CI fan-out.
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1-}" in
  full)
    cargo fmt --all --check
    ;;
  prose)
    ;;
  *)
    printf 'Invalid CI preflight scope: %s\n' "${1-}" >&2
    exit 2
    ;;
esac

bash scripts/check-documentation.sh
bash scripts/check-documentation.test.sh
