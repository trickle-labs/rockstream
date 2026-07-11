FROM rust:1.88-slim-bookworm AS builder
WORKDIR /usr/src/rockstream
RUN apt-get update && apt-get install -y pkg-config libssl-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin rockstream

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/rockstream/target/release/rockstream /usr/local/bin/rockstream
ENTRYPOINT ["rockstream"]
