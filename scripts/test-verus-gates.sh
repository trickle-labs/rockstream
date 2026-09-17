#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
	echo "test-verus-gates: RS-0906: $1" >&2
	exit 1
}

command -v verus >/dev/null 2>&1 || fail "verus is required; run scripts/install-verus.sh first"

valid_log="$TMP_ROOT/valid.log"
if ! verus "$ROOT/formal/verus/negative/valid_smoke.rs" >"$valid_log" 2>&1; then
	cat "$valid_log" >&2
	fail "valid smoke proof failed"
fi

invalid_log="$TMP_ROOT/invalid.log"
if verus "$ROOT/formal/verus/negative/invalid_theorem.rs" >"$invalid_log" 2>&1; then
	cat "$invalid_log" >&2
	fail "invalid theorem fixture unexpectedly passed"
fi
grep -q "assertion failed" "$invalid_log" || {
	cat "$invalid_log" >&2
	fail "invalid theorem failed for an unexpected reason"
}

grep -q "assume(" "$ROOT/formal/verus/negative/unchecked_assumption.rs" ||
	fail "unchecked-assumption fixture no longer exercises assume"
grep -q "cfg(verus_only)" "$ROOT/formal/verus/negative/verus_only_substitution.rs" ||
	fail "verus-only fixture no longer exercises executable substitution"

for fixture in invalid_arithmetic invalid_sign_encoding invalid_validation \
	invalid_publication_guard invalid_durable_marker; do
	mutation_log="$TMP_ROOT/$fixture.log"
	if verus "$ROOT/formal/verus/negative/$fixture.rs" >"$mutation_log" 2>&1; then
		cat "$mutation_log" >&2
		fail "$fixture unexpectedly passed"
	fi
	grep -Fq "assertion failed" "$mutation_log" && grep -Fq "$fixture.rs" "$mutation_log" || {
		cat "$mutation_log" >&2
		fail "$fixture failed for an unexpected reason"
	}
done

policy_root="$TMP_ROOT/policy"
mkdir -p "$policy_root/formal/verus" "$policy_root/crates/rockstream-verified/src" \
	"$policy_root/crates/rockstream-plan/src" "$policy_root/crates/rockstream-plan/tests"
cp "$ROOT/formal/verus/manifest.toml" "$policy_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$policy_root/formal/verus/assumptions.toml"
cp "$ROOT/crates/rockstream-verified/src/lib.rs" "$policy_root/crates/rockstream-verified/src/lib.rs"
cp "$ROOT/crates/rockstream-plan/src/virtual_bucket.rs" "$policy_root/crates/rockstream-plan/src/virtual_bucket.rs"
cp "$ROOT/crates/rockstream-plan/tests/virtual_bucket_routing_tests.rs" \
	"$policy_root/crates/rockstream-plan/tests/virtual_bucket_routing_tests.rs"
for fixture in unchecked_assumption verus_only_substitution; do
	cp "$ROOT/formal/verus/negative/$fixture.rs" \
		"$policy_root/crates/rockstream-verified/src/$fixture.rs"
	policy_log="$TMP_ROOT/$fixture.log"
	if python3 "$ROOT/scripts/check-verus-manifest.py" --root "$policy_root" >"$policy_log" 2>&1; then
		cat "$policy_log" >&2
		fail "$fixture bypassed the trust-policy checker"
	fi
	grep -q "unchecked proof construct found" "$policy_log" || {
		cat "$policy_log" >&2
		fail "$fixture was rejected for an unexpected reason"
	}
done

fixture_root="$TMP_ROOT/mutated"
mkdir -p "$fixture_root/formal/verus" "$fixture_root/crates/rockstream-verified/src" \
	"$fixture_root/crates/rockstream-plan/src" "$fixture_root/crates/rockstream-plan/tests"
cp "$ROOT/formal/verus/manifest.toml" "$fixture_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$fixture_root/formal/verus/assumptions.toml"
cp "$ROOT/crates/rockstream-verified/src/lib.rs" "$fixture_root/crates/rockstream-verified/src/lib.rs"
cp "$ROOT/crates/rockstream-plan/src/virtual_bucket.rs" "$fixture_root/crates/rockstream-plan/src/virtual_bucket.rs"
cp "$ROOT/crates/rockstream-plan/tests/virtual_bucket_routing_tests.rs" "$fixture_root/crates/rockstream-plan/tests/virtual_bucket_routing_tests.rs"
sed -i.bak 's/id = "VS0-02"/id = "UNKNOWN"/' "$fixture_root/formal/verus/manifest.toml"
manifest_log="$TMP_ROOT/manifest.log"
if python3 "$ROOT/scripts/check-verus-manifest.py" --root "$fixture_root" >"$manifest_log" 2>&1; then
	cat "$manifest_log" >&2
	fail "manifest checker accepted an unregistered claim"
fi
grep -q "unregistered verus claim markers" "$manifest_log" || {
	cat "$manifest_log" >&2
	fail "manifest checker rejected the mutation for an unexpected reason"
}

echo "verus gates: valid proof passed; invalid proof rejected; trust-policy fixtures rejected; manifest mutation rejected."
