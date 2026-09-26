#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

git -C "$TMP_ROOT" init -q
git -C "$TMP_ROOT" config user.email test@example.com
git -C "$TMP_ROOT" config user.name test
mkdir -p "$TMP_ROOT/crates/rockstream-control" "$TMP_ROOT/crates/rockstream-storage/src/catalog" "$TMP_ROOT/formal/verus" "$TMP_ROOT/formal"
touch "$TMP_ROOT/README.md"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m baseline

assert_output() {
	local expected="$1"
	local actual
	actual="$(cd "$TMP_ROOT" && GITHUB_BASE_REF= BASE=HEAD~1 "$ROOT/scripts/check-path-coupling.sh" 2>&1)"
	if [[ "$actual" != "$expected" ]]; then
		echo "$actual"
		echo "expected: $expected" >&2
		exit 1
	fi
}

touch "$TMP_ROOT/formal/verus/README.md"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m verus-docs
assert_output "check-path-coupling: no changed files found for range 'HEAD~1..HEAD' — skipping."

touch "$TMP_ROOT/crates/rockstream-storage/src/catalog/snapshot.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m catalog-recovery-without-formal-model
assert_output "check-path-coupling: only data-plane tests/benchmarks changed — OK."

touch "$TMP_ROOT/crates/rockstream-control/src.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m coordination-without-model
if (cd "$TMP_ROOT" && GITHUB_BASE_REF= BASE=HEAD~1 "$ROOT/scripts/check-path-coupling.sh") >/dev/null 2>&1; then
	echo "coordination-only change unexpectedly passed" >&2
	exit 1
fi

touch "$TMP_ROOT/crates/rockstream-control/src2.rs"
touch "$TMP_ROOT/formal/m2_frontier_agg.fizz"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m coordination-with-model
assert_output "check-path-coupling: coordination change accompanied by model touch — OK."

echo "check-path-coupling tests: Verus-only and catalog recovery changes are excluded; protocol changes require model updates."
