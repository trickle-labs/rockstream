#!/usr/bin/env bash
# Exact-contract tests for the real-process sampler and its trend gate.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
SAMPLER="$ROOT/scripts/sample-resource-leak-soak.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

flat="$TMP_ROOT/flat.tsv"
leak="$TMP_ROOT/leak.tsv"
printf '0\t100\t10\t3\n60\t100\t10\t3\n120\t104\t11\t4\n180\t104\t11\t4\n' > "$flat"
printf '0\t100\t10\t3\n60\t100\t10\t3\n120\t400\t10\t3\n180\t500\t10\t3\n' > "$leak"

ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS=180 \
ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS=60 \
ROCKSTREAM_RESOURCE_SOAK_WARMUP_SAMPLES=2 \
ROCKSTREAM_RESOURCE_SOAK_ROLLING_WINDOW=2 \
ROCKSTREAM_RESOURCE_SOAK_RSS_TOLERANCE_KIB=5 \
ROCKSTREAM_RESOURCE_SOAK_OPEN_FD_TOLERANCE=1 \
ROCKSTREAM_RESOURCE_SOAK_OPEN_SOCKET_TOLERANCE=1 \
bash "$SAMPLER" --samples-file "$flat" --artifact-dir "$TMP_ROOT/flat-artifact"

expected_flat='# Rockstream resource-leak soak

status: PASS
samples: 4/4 (fill: 100%)
warmup samples: 2
rolling window: 2

| resource | unit | baseline | tolerance | final rolling median | slope verdict |
| --- | --- | ---: | ---: | ---: | --- |
| RSS | KiB | 100 | 5 | 104 | PASS |
| open FD | count | 10 | 1 | 11 | PASS |
| open socket | count | 3 | 1 | 4 | PASS |

diagnostic: all rolling medians are within their resource baseline + tolerance'
[[ "$(< "$TMP_ROOT/flat-artifact/resource-leak-soak-summary.md")" == "$expected_flat" ]] \
  || { echo "FAIL: flat resource summary changed" >&2; exit 1; }

if actual_error="$(ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS=180 \
  ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS=60 \
  ROCKSTREAM_RESOURCE_SOAK_WARMUP_SAMPLES=2 \
  ROCKSTREAM_RESOURCE_SOAK_ROLLING_WINDOW=2 \
  ROCKSTREAM_RESOURCE_SOAK_RSS_TOLERANCE_KIB=5 \
  bash "$SAMPLER" --samples-file "$leak" --artifact-dir "$TMP_ROOT/leak-artifact" 2>&1)"; then
  echo "FAIL: injected resource leak passed the trend gate" >&2
  exit 1
fi

expected_error='RS-0002: RSS rolling median 450 KiB exceeds baseline 100 KiB + tolerance 5 KiB. next_steps: inspect resource-leak-soak-summary.md'
[[ "$actual_error" == "$expected_error" ]] || { echo "FAIL: leak diagnostic changed: $actual_error" >&2; exit 1; }

echo "OK: sample-resource-leak-soak.sh exact-contract tests passed."
