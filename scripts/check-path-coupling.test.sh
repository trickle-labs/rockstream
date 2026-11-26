#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

git -C "$TMP_ROOT" init -q
git -C "$TMP_ROOT" config user.email test@example.com
git -C "$TMP_ROOT" config user.name test
mkdir -p "$TMP_ROOT/crates/rockstream-control/src" "$TMP_ROOT/crates/rockstream-storage/src/catalog" "$TMP_ROOT/crates/rockstream-runtime/src/exchange" "$TMP_ROOT/formal/verus" "$TMP_ROOT/formal"
touch "$TMP_ROOT/README.md"
printf 'const VALUE: &str = "ab";\n' > "$TMP_ROOT/crates/rockstream-control/src/lib.rs"
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

mkdir -p "$TMP_ROOT/crates/rockstream-storage"
printf '[dev-dependencies]\nserial_test = "3"\n' > "$TMP_ROOT/crates/rockstream-storage/Cargo.toml"
printf 'version = 3\n' > "$TMP_ROOT/Cargo.lock"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m dependency-baseline
printf '[dev-dependencies]\nserial_test = "4"\n' > "$TMP_ROOT/crates/rockstream-storage/Cargo.toml"
printf 'version = 4\n' > "$TMP_ROOT/Cargo.lock"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m dev-dependency-upgrade
assert_output "check-path-coupling: no coordination source or DESIGN.md changes — OK."

touch "$TMP_ROOT/formal/verus/README.md"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m verus-docs
assert_output "check-path-coupling: no changed files found for range 'HEAD~1..HEAD' — skipping."

printf 'const VALUE: &str = "ab";\n\n' > "$TMP_ROOT/crates/rockstream-control/src/lib.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m whitespace-only-coordination
assert_output "check-path-coupling: no changed files found for range 'HEAD~1..HEAD' — skipping."

touch "$TMP_ROOT/crates/rockstream-storage/src/catalog/snapshot.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m catalog-recovery-without-formal-model
assert_output "check-path-coupling: only data-plane tests/benchmarks changed — OK."

printf 'const VALUE: &str = "a b";\n' > "$TMP_ROOT/crates/rockstream-control/src/lib.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m coordination-without-model
if (cd "$TMP_ROOT" && GITHUB_BASE_REF= BASE=HEAD~1 "$ROOT/scripts/check-path-coupling.sh") >/dev/null 2>&1; then
	echo "coordination-only change unexpectedly passed" >&2
	exit 1
fi

touch "$TMP_ROOT/crates/rockstream-runtime/src/exchange/multiplexer.rs"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m exchange-without-model
set +e
actual="$(cd "$TMP_ROOT" && GITHUB_BASE_REF= BASE=HEAD~1 "$ROOT/scripts/check-path-coupling.sh" 2>&1)"
status=$?
set -e
expected="$(printf '\nPATH-COUPLING VIOLATION: coordination source or DESIGN.md changed without a\ncorresponding touch to formal/*.fizz or FIZZBEE_TEST_PLAN.md.\n\nChanged coordination files:\ncrates/rockstream-runtime/src/exchange/multiplexer.rs\n\nFix: update the relevant FizzBee model and/or FIZZBEE_TEST_PLAN.md §3.6\nto reflect the protocol change before merging.\n\nSee FIZZBEE_TEST_PLAN.md §4.4 DC.2 for the full policy.')"
if [[ "$status" -ne 1 || "$actual" != "$expected" ]]; then
	echo "$actual" >&2
	echo "expected: $expected" >&2
	exit 1
fi

touch "$TMP_ROOT/crates/rockstream-control/src/other.rs"
touch "$TMP_ROOT/formal/m2_frontier_agg.fizz"
git -C "$TMP_ROOT" add .
git -C "$TMP_ROOT" commit -q -m coordination-with-model
assert_output "check-path-coupling: coordination change accompanied by model touch — OK."

echo "check-path-coupling tests: dependency metadata, Verus-only, blank-line-only, and catalog recovery changes are excluded; protocol changes require model updates."
