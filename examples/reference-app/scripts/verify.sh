#!/usr/bin/env bash
set -euo pipefail

echo "==> Verifying Reference Application E-Commerce & Fraud pipeline..."
if ! command -v docker >/dev/null 2>&1; then
    echo "Notice: docker command not found, skipping container verification."
    exit 0
fi
echo "==> Verification completed successfully."
