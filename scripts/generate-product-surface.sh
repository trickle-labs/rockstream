#!/usr/bin/env bash
# generate-product-surface.sh — generate deterministic docs/product-surface.json (DOC-001, DOC-004).
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
exec python3 "$ROOT/scripts/generate-product-surface.py" --root "$ROOT" "$@"
