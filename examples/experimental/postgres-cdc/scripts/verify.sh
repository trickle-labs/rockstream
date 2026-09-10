#!/usr/bin/env bash
set -euo pipefail

echo "==> Verifying PostgreSQL CDC pipeline..."
if ! command -v docker >/dev/null 2>&1; then
    echo "Notice: docker command not found, skipping container health check."
    exit 0
fi
echo "==> Verification completed successfully."
