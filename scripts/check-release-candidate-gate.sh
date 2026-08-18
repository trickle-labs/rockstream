#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
python3 "$ROOT/scripts/check-release-candidate-gate.py" "$ROOT"
