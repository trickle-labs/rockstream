#!/usr/bin/env bash
# Sample one real process with a bounded history, then apply the Rust trend gate.
set -euo pipefail

usage() {
  echo "usage: $0 --pid PID --artifact-dir DIR [--duration-secs N] [--interval-secs N]" >&2
  echo "       $0 --samples-file TSV --artifact-dir DIR" >&2
  exit 64
}

ROOT="$(git rev-parse --show-toplevel)"
PID=""
SAMPLES_FILE=""
ARTIFACT_DIR=""
DURATION_SECS="${ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS:-14400}"
INTERVAL_SECS="${ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS:-60}"

while (($#)); do
  case "$1" in
    --pid) PID="${2:-}"; shift 2 ;;
    --samples-file) SAMPLES_FILE="${2:-}"; shift 2 ;;
    --artifact-dir) ARTIFACT_DIR="${2:-}"; shift 2 ;;
    --duration-secs) DURATION_SECS="${2:-}"; shift 2 ;;
    --interval-secs) INTERVAL_SECS="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "$ARTIFACT_DIR" ]] || usage
[[ "$INTERVAL_SECS" =~ ^[1-9][0-9]*$ ]] || { echo "interval must be a positive integer" >&2; exit 64; }
[[ "$DURATION_SECS" =~ ^[1-9][0-9]*$ ]] || { echo "duration must be a positive integer" >&2; exit 64; }

if [[ -n "$PID" && -n "$SAMPLES_FILE" ]]; then
  echo "choose exactly one of --pid or --samples-file" >&2
  exit 64
fi

if [[ -n "$PID" ]]; then
  [[ "$PID" =~ ^[1-9][0-9]*$ ]] || { echo "PID must be a positive integer" >&2; exit 64; }
  mkdir -p "$ARTIFACT_DIR"
  SAMPLES_FILE="$ARTIFACT_DIR/resource-leak-soak-samples.tsv"
  : > "$SAMPLES_FILE"
  CAPACITY=$((DURATION_SECS / INTERVAL_SECS + 1))
  STARTED_AT="$(date +%s)"
  SAMPLE_COUNT=0
  while :; do
    if ((SAMPLE_COUNT >= CAPACITY)); then
      echo "RS-0002: sample capacity $CAPACITY is full; refusing unbounded accumulation" >&2
      exit 1
    fi
    [[ -r "/proc/$PID/status" && -d "/proc/$PID/fd" ]] || {
      echo "RS-0002: target process $PID disappeared or /proc data is unavailable" >&2
      exit 1
    }
    RSS_KIB="$(awk '/^VmRSS:/ { print $2; found=1 } END { if (!found) exit 1 }' "/proc/$PID/status")" || {
      echo "RS-0002: target process $PID has no VmRSS sample" >&2
      exit 1
    }
    OPEN_FDS="$(find "/proc/$PID/fd" -mindepth 1 -maxdepth 1 -print | wc -l | tr -d ' ')"
    OPEN_SOCKETS="$(find "/proc/$PID/fd" -mindepth 1 -maxdepth 1 -exec readlink {} \; | grep -c '^socket:\[' || true)"
    NOW="$(date +%s)"
    printf '%s\t%s\t%s\t%s\n' "$((NOW - STARTED_AT))" "$RSS_KIB" "$OPEN_FDS" "$OPEN_SOCKETS" >> "$SAMPLES_FILE"
    ((SAMPLE_COUNT += 1))
    ((NOW - STARTED_AT >= DURATION_SECS)) && break
    sleep "$INTERVAL_SECS"
  done
fi

[[ -n "$SAMPLES_FILE" && -f "$SAMPLES_FILE" ]] || usage
ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS="$DURATION_SECS" \
ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS="$INTERVAL_SECS" \
cargo run -q -p rockstream-sim --bin resource_leak_soak_gate -- \
  --samples-file "$SAMPLES_FILE" --artifact-dir "$ARTIFACT_DIR"
