#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
	echo "test-verus-gates: RS-0906: $1" >&2
	exit 1
}

if ! command -v verus >/dev/null 2>&1; then
	VERUS_CACHE="$(find "$ROOT/.cache/verus" -mindepth 1 -maxdepth 1 -type d -print -quit 2>/dev/null || true)"
	if [ -n "$VERUS_CACHE" ] && [ -x "$VERUS_CACHE/verus" ]; then
		export PATH="$VERUS_CACHE:$PATH"
	fi
fi

if ! command -v verus >/dev/null 2>&1; then
	fail "verus is required; run scripts/install-verus.sh first"
fi

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
mkdir -p "$policy_root/formal/verus" "$policy_root/crates"
cp "$ROOT/formal/verus/manifest.toml" "$policy_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$policy_root/formal/verus/assumptions.toml"
cp -R "$ROOT/crates/." "$policy_root/crates/"
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

unmarked_root="$TMP_ROOT/unmarked"
mkdir -p "$unmarked_root/formal/verus" "$unmarked_root/crates"
cp "$ROOT/formal/verus/manifest.toml" "$unmarked_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$unmarked_root/formal/verus/assumptions.toml"
cp -R "$ROOT/crates/." "$unmarked_root/crates/"
printf '\npub fn unregistered_runtime_only() -> u64 { 0 }\n' \
  >>"$unmarked_root/crates/rockstream-verified/src/routing.rs"
unmarked_log="$TMP_ROOT/unmarked.log"
if python3 "$ROOT/scripts/check-verus-manifest.py" --root "$unmarked_root" >"$unmarked_log" 2>&1; then
	cat "$unmarked_log" >&2
	fail "manifest checker accepted unmarked covered code"
fi
grep -q "missing verus claim marker" "$unmarked_log" || {
	cat "$unmarked_log" >&2
	fail "unmarked code was rejected for an unexpected reason"
}

conditional_root="$TMP_ROOT/conditional"
mkdir -p "$conditional_root/formal/verus" "$conditional_root/crates"
cp "$ROOT/formal/verus/manifest.toml" "$conditional_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$conditional_root/formal/verus/assumptions.toml"
cp -R "$ROOT/crates/." "$conditional_root/crates/"
printf '\n#[cfg(not(verus_only))]\npub fn runtime_only_branch() -> u64 { 0 }\n' \
  >>"$conditional_root/crates/rockstream-verified/src/routing.rs"
conditional_log="$TMP_ROOT/conditional.log"
if python3 "$ROOT/scripts/check-verus-manifest.py" --root "$conditional_root" >"$conditional_log" 2>&1; then
	cat "$conditional_log" >&2
	fail "manifest checker accepted conditional executable substitution"
fi
grep -q "unchecked proof construct found" "$conditional_log" || {
	cat "$conditional_log" >&2
	fail "conditional substitution was rejected for an unexpected reason"
}

fixture_root="$TMP_ROOT/mutated"
mkdir -p "$fixture_root/formal/verus" "$fixture_root/crates"
cp "$ROOT/formal/verus/manifest.toml" "$fixture_root/formal/verus/manifest.toml"
cp "$ROOT/formal/verus/assumptions.toml" "$fixture_root/formal/verus/assumptions.toml"
cp -R "$ROOT/crates/." "$fixture_root/crates/"
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
