#!/usr/bin/env bash
set -euo pipefail

docker info >/dev/null
docker load --input target/ci-image/candidate.tar
image="rockstream:ci-${GITHUB_SHA:?}"
test -n "${EXPECTED_IMAGE_ID:?}"
test "$(docker image inspect --format '{{.Id}}' "$image")" = "$EXPECTED_IMAGE_ID"
test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")" = "$GITHUB_SHA"
docker tag "$image" rockstream-tc-test:latest
mkdir -p target/ci-test-results
printf '%s\t%s\n' "$GITHUB_SHA" "$EXPECTED_IMAGE_ID" > target/ci-test-results/candidate-image.tsv
