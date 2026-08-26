FROM rust:1.94.1-slim-bookworm AS builder
WORKDIR /usr/src/rockstream
RUN apt-get update && apt-get install -y g++ make pkg-config libssl-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin rockstream

FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="rockstream" \
      org.opencontainers.image.description="RockStream incremental view maintenance engine" \
      org.opencontainers.image.version="0.59.9" \
      org.opencontainers.image.revision="main" \
      org.opencontainers.image.created="2026-08-18T12:00:00Z" \
      org.opencontainers.image.source="https://github.com/trickle-labs/rockstream" \
      rockstream.lockfile_digest="auto"
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/rockstream/target/release/rockstream /usr/local/bin/rockstream
ENTRYPOINT ["rockstream"]
