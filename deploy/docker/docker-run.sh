#!/usr/bin/env bash
# Reference Docker run profiles for RockStream v0.59.22
# Enforces non-root execution (UID 10001), read-only root filesystem, and named volume persistence.

set -euo pipefail

IMAGE="${ROCKSTREAM_IMAGE:-ghcr.io/trickle-labs/rockstream:0.59.22}"
VOLUME_NAME="${ROCKSTREAM_VOLUME:-rockstream_data}"

echo "Creating persistent volume: ${VOLUME_NAME}..."
docker volume create "${VOLUME_NAME}"

# Standalone All-In-One Profile
echo "Starting RockStream standalone container..."
docker run -d \
  --name rockstream-standalone \
  --restart unless-stopped \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -u 10001:10001 \
  -v "${VOLUME_NAME}:/data:rw" \
  -p 5432:5432 \
  -p 9090:9090 \
  "${IMAGE}" \
  start \
  --role=all \
  --listen=0.0.0.0:5432 \
  --metrics-addr=0.0.0.0:9090 \
  --storage=/data
