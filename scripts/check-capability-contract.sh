#!/usr/bin/env bash
# Validate v0.57.1 capability data, documentation, and generated matrix output.
set -euo pipefail

if [ "$#" -eq 1 ] && [ "$1" = "--full-semantics" ]; then
  ROOT="$(git rev-parse --show-toplevel)"
elif [ "$#" -ge 1 ]; then
  ROOT="$1"
else
  ROOT="$(git rev-parse --show-toplevel)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/check-capability-contract.py" "$ROOT" --full-semantics
