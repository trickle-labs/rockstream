#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

case "${1:-}" in
  digest)
    exec python3 "$ROOT/scripts/check-r1-local-evidence.py" --digest "$ROOT"
    ;;
  verify)
    exec python3 "$ROOT/scripts/check-r1-local-evidence.py" "$ROOT"
    ;;
  *)
    printf 'Usage: %s {digest|verify}\n' "$0" >&2
    exit 2
    ;;
esac
