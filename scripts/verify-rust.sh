#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v cargo-verus >/dev/null 2>&1 || ! command -v verus >/dev/null 2>&1; then
	VERUS_CACHE="$(find "$ROOT/.cache/verus" -mindepth 1 -maxdepth 1 -type d -print -quit 2>/dev/null || true)"
	if [ -n "$VERUS_CACHE" ] && [ -x "$VERUS_CACHE/cargo-verus" ] && [ -x "$VERUS_CACHE/verus" ]; then
		export PATH="$VERUS_CACHE:$PATH"
	fi
fi

if ! command -v cargo-verus >/dev/null 2>&1 || ! command -v verus >/dev/null 2>&1; then
	echo "verify-rust: RS-0906: cargo-verus/verus is required; run scripts/install-verus.sh first." >&2
	exit 1
fi

cd "$ROOT"
RUSTUP_TOOLCHAIN="${VERUS_RUST_TOOLCHAIN:-1.98.1}" \
	cargo verus verify -p rockstream-verified --locked
