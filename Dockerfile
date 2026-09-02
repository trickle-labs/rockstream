# Multi-Architecture Production Container Image for RockStream
# Target architectures: linux/amd64, linux/arm64
# Security standard: Non-root execution (UID 10001), read-only rootfs compatible

FROM rust:1.88-slim-bookworm AS builder
WORKDIR /usr/src/rockstream
RUN apt-get update && apt-get install -y g++ make pkg-config libssl-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin rockstream

FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="rockstream" \
      org.opencontainers.image.description="RockStream streaming IVM engine & SQL database" \
      org.opencontainers.image.version="0.59.24" \
      org.opencontainers.image.revision="main" \
      org.opencontainers.image.created="2026-09-01T12:00:00Z" \
      org.opencontainers.image.source="https://github.com/trickle-labs/rockstream" \
      org.opencontainers.image.vendor="Trickle Labs" \
      org.opencontainers.image.licenses="Apache-2.0" \
      rockstream.lockfile_digest="auto"

# Install minimal runtime dependencies and create unprivileged service user
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/* && \
    groupadd -g 10001 rockstream && \
    useradd -u 10001 -g rockstream -m -d /data -s /usr/sbin/nologin rockstream && \
    mkdir -p /data && \
    chown -R rockstream:rockstream /data

COPY --from=builder /usr/src/rockstream/target/release/rockstream /usr/local/bin/rockstream

# Standard exposed ports:
# 5432: PostgreSQL Wire Protocol (SQL Gateway)
# 9090: Management HTTP (/live, /ready, /health, /metrics)
# 9100: Worker DataPlane & Exchange RPC
# 9200: Control Plane Raft consensus & RPC
EXPOSE 5432 9090 9100 9200

USER rockstream:rockstream
WORKDIR /data
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:9090/ready || exit 1

ENTRYPOINT ["rockstream"]
CMD ["start", "--role=all", "--listen=0.0.0.0:5432", "--metrics-addr=0.0.0.0:9090", "--storage=/data"]
