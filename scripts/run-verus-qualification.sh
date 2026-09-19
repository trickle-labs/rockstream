#!/usr/bin/env bash
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$ROOT" rev-parse --short HEAD)"
OUTPUT_ROOT="${VERUS_QUALIFICATION_OUTPUT:-$ROOT/evidence/verus/$RUN_ID}"
TARGET_ROOT="$(mktemp -d)"
STEPS="$OUTPUT_ROOT/steps.tsv"
QUALIFY_TARGET="${VERUS_QUALIFICATION_TARGET:-}"
TARGET_ARGS=()
if [ -n "$QUALIFY_TARGET" ]; then
  TARGET_ARGS=(--target "$QUALIFY_TARGET")
fi
OVERALL=0
trap 'rm -rf "$TARGET_ROOT"' EXIT
mkdir -p "$OUTPUT_ROOT"

QUALIFICATION_TOML="$ROOT/formal/verus/qualification.toml"
COLD_VERIFICATION_BUDGET="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["budgets"]["cold_verification_seconds"])' "$QUALIFICATION_TOML")"
INCREMENTAL_VERIFICATION_BUDGET="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["budgets"]["incremental_verification_seconds"])' "$QUALIFICATION_TOML")"
PRODUCTION_COMPILE_BUDGET="$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["budgets"]["production_compile_seconds"])' "$QUALIFICATION_TOML")"

run_step() {
  local id="$1"
  shift
  local start end status
  start="$(date +%s)"
  if CARGO_TARGET_DIR="$TARGET_ROOT" CARGO_INCREMENTAL=0 "$@" >"$OUTPUT_ROOT/$id.log" 2>&1; then
    status=0
  else
    status=$?
    OVERALL=1
  fi
  end="$(date +%s)"
  local elapsed=$((end - start))
  local budget=0
  case "$id" in
    verifier) budget="$COLD_VERIFICATION_BUDGET" ;;
    verifier-incremental) budget="$INCREMENTAL_VERIFICATION_BUDGET" ;;
    cargo-*) budget="$PRODUCTION_COMPILE_BUDGET" ;;
  esac
  if [ "$status" -eq 0 ] && [ "$budget" -gt 0 ] && [ "$elapsed" -gt "$budget" ]; then
    printf 'verus qualification: RS-0906: %s exceeded budget (%ss > %ss)\n' \
      "$id" "$elapsed" "$budget" >>"$OUTPUT_ROOT/$id.log"
    printf 'verus qualification: RS-0906: %s exceeded budget (%ss > %ss)\n' \
      "$id" "$elapsed" "$budget" >&2
    status=1
    OVERALL=1
  fi
  printf '%s\t%s\t%s\t%s\n' "$id" "$status" "$elapsed" "$*" >>"$STEPS"
}

if ! python3 "$ROOT/scripts/check-verus-qualification.py" --root "$ROOT"; then
  exit 1
fi
if ! command -v cargo-verus >/dev/null 2>&1 || ! command -v verus >/dev/null 2>&1; then
  echo "verus qualification: RS-0906: pinned cargo-verus and verus are required" >&2
  exit 127
fi

run_step verifier env RUSTUP_TOOLCHAIN="${VERUS_RUST_TOOLCHAIN:-1.98.1}" \
  cargo verus verify -p rockstream-verified --locked "${TARGET_ARGS[@]}"
run_step verifier-incremental env CARGO_INCREMENTAL=1 \
  RUSTUP_TOOLCHAIN="${VERUS_RUST_TOOLCHAIN:-1.98.1}" \
  cargo verus verify -p rockstream-verified --locked "${TARGET_ARGS[@]}"
run_step cargo-debug env RUSTUP_TOOLCHAIN="${ROCKSTREAM_RUST_TOOLCHAIN:-1.88}" \
  cargo build --workspace --locked "${TARGET_ARGS[@]}"
run_step cargo-check env RUSTUP_TOOLCHAIN="${ROCKSTREAM_RUST_TOOLCHAIN:-1.88}" \
  cargo check --workspace --locked "${TARGET_ARGS[@]}"
run_step cargo-all-features env RUSTUP_TOOLCHAIN="${ROCKSTREAM_RUST_TOOLCHAIN:-1.88}" \
  cargo check --workspace --all-features --locked "${TARGET_ARGS[@]}"
run_step cargo-release env RUSTUP_TOOLCHAIN="${ROCKSTREAM_RUST_TOOLCHAIN:-1.88}" \
  cargo build --workspace --locked --release "${TARGET_ARGS[@]}"

RUNTIME_ARTIFACT="$OUTPUT_ROOT/rockstream-release"
DEBUG_ARTIFACT="$OUTPUT_ROOT/rockstream-debug"
TARGET_SUFFIX=""
if [ -n "$QUALIFY_TARGET" ]; then
  TARGET_SUFFIX="$QUALIFY_TARGET/"
fi

# Ensure any pre-existing final destination files are removed so stale files
# from earlier runs cannot count as evidence.
rm -f "$RUNTIME_ARTIFACT" "$DEBUG_ARTIFACT"

RUNTIME_ARGS=()

stage_runtime_artifact() {
  local label="$1"
  local src="$2"
  local dest="$3"
  local tmp="${dest}.tmp.$$"

  rm -f "$tmp"
  if [ ! -f "$src" ]; then
    echo "verus qualification: RS-0906: $label runtime artifact was not produced" >&2
    OVERALL=1
    return 1
  fi
  if ! cp "$src" "$tmp"; then
    echo "verus qualification: RS-0906: $label runtime artifact copy failed" >&2
    rm -f "$tmp"
    OVERALL=1
    return 1
  fi
  if ! mv "$tmp" "$dest"; then
    echo "verus qualification: RS-0906: $label runtime artifact move failed" >&2
    rm -f "$tmp"
    OVERALL=1
    return 1
  fi
  RUNTIME_ARGS+=(--runtime-artifact "$dest")
  return 0
}

stage_runtime_artifact "release" "$TARGET_ROOT/${TARGET_SUFFIX}release/rockstream" "$RUNTIME_ARTIFACT"
stage_runtime_artifact "debug" "$TARGET_ROOT/${TARGET_SUFFIX}debug/rockstream" "$DEBUG_ARTIFACT"

if [ "$OVERALL" -eq 0 ]; then
  STATUS=passed
else
  STATUS=failed
fi
python3 "$ROOT/scripts/record-verus-qualification.py" \
  --root "$ROOT" --run-id "$RUN_ID" --status "$STATUS" \
  --steps "$STEPS" --target "$QUALIFY_TARGET" \
  ${RUNTIME_ARGS[@]+"${RUNTIME_ARGS[@]}"} \
  --output "$OUTPUT_ROOT/qualification.json" || OVERALL=1
exit "$OVERALL"

