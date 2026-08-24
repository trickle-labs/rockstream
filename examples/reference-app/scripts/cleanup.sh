#!/usr/bin/env bash
set -euo pipefail

echo "==> Tearing down Reference Application environment..."
if command -v docker >/dev/null 2>&1; then
    docker compose down -v --remove-orphans || true
fi
rm -rf ./storage
echo "==> Cleanup complete."
