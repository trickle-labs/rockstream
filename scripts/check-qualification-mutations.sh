#!/usr/bin/env bash
# Execute the recovery assertions and anti-cheat negative controls; helper evidence only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/release-qualification.py" "$@" --fast --suite mutations
