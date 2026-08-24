#!/usr/bin/env bash
# check-error-catalog.sh — enforce zero drift between errors.toml, Rust constants, and error-codes.md (DOC-01).
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
exec python3 "$ROOT/scripts/check-error-catalog.py" "$ROOT"
