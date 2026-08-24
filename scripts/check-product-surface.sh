#!/usr/bin/env bash
# check-product-surface.sh — enforce zero drift on single-source product surface manifest (DOC-001, DOC-004).
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
exec python3 "$ROOT/scripts/check-product-surface.py" "$ROOT"
