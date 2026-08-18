#!/usr/bin/env bash
# check-dispatch-wiring.sh — verify 4-stage pipeline wiring across documented SQL surface.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
exec python3 "$ROOT/scripts/check-dispatch-wiring.py" --root "$ROOT"
