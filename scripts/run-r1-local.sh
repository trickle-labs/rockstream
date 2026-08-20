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
  candidates)
    exec python3 "$ROOT/scripts/r1-local-candidates.py" record --root "$ROOT"
    ;;
  verify-candidates)
    exec python3 "$ROOT/scripts/r1-local-candidates.py" verify --root "$ROOT"
    ;;
  *)
    printf 'Usage: %s {digest|verify|candidates|verify-candidates}\n' "$0" >&2
    exit 2
    ;;
esac
