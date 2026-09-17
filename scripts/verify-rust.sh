#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v cargo-verus >/dev/null 2>&1; then
	echo "verify-rust: cargo-verus is required; run scripts/install-verus.sh first." >&2
	exit 1
fi

if ! command -v verus >/dev/null 2>&1; then
	echo "verify-rust: verus is required; run scripts/install-verus.sh first." >&2
	exit 1
fi

cd "$ROOT"
RUSTUP_TOOLCHAIN="${VERUS_RUST_TOOLCHAIN:-1.98.1}" \
	cargo verus verify -p rockstream-verified --locked
