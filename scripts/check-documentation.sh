#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
exec python3 "$ROOT/scripts/check-documentation.py" "$ROOT"
