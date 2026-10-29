# syntax=docker/dockerfile:1
# Overlay only the scenario adapter when qualifying an older source revision.
FROM rust:1.95-slim-bookworm AS builder
RUN apt-get update && apt-get install -y g++ make cmake pkg-config libssl-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/rockstream
COPY Cargo.toml Cargo.lock capabilities.toml ./
COPY crates/ crates/
COPY fuzz/ fuzz/
COPY formal/ formal/
COPY contracts/ contracts/
COPY docs/testing/ docs/testing/
ARG SOURCE_REVISION
ENV ROCKSTREAM_QUALIFICATION_REVISION=${SOURCE_REVISION} CARGO_PROFILE_DEV_DEBUG=0 CARGO_BUILD_JOBS=2
# Refresh archived sources while holding the target-cache lock across revisions.
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/src/rockstream/target,sharing=locked \
    find crates formal contracts docs/testing -type f -exec touch {} + && \
    cargo build --locked -p rockstream-sim --features simulation,docker_tests --example upgrade_worker && \
    cp target/debug/examples/upgrade_worker /upgrade-worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 libstdc++6 && rm -rf /var/lib/apt/lists/*
ARG SOURCE_REVISION
ARG SOURCE_VERSION
LABEL org.opencontainers.image.revision=${SOURCE_REVISION} \
      org.opencontainers.image.version=${SOURCE_VERSION} \
      rockstream.qualification.protocol="rolling-upgrade-kafka-minio-v1"
COPY --from=builder /upgrade-worker /usr/local/bin/upgrade-worker
ENTRYPOINT ["/usr/local/bin/upgrade-worker"]
