# Reproducible Build Runbook (v1.0 / v0.59.3)

RockStream release builds are strictly reproducible. Given identical source commits and toolchain versions, compiling on any independent build host yields byte-identical binaries and OCI container images.

---

## Deterministic Compilation Environment

Reproducible builds require eliminating three sources of non-determinism:
1. **File Timestamps**: Standardized to `SOURCE_DATE_EPOCH` (UNIX timestamp).
2. **Build Path Differences**: Stripped via compiler flags (`--remap-path-prefix`).
3. **Linker Non-Determinism**: Controlled with deterministic `RUSTFLAGS`.

### Environment Variables & Flags

```bash
export SOURCE_DATE_EPOCH=1723939200
export RUSTFLAGS="--remap-path-prefix=$(pwd)=/build -C link-arg=-Wl,--build-id=none"
```

---

## Target Architectures

RockStream publishes reproducible binaries for:
- `x86_64-unknown-linux-gnu` (Linux AMD64)
- `aarch64-unknown-linux-gnu` (Linux ARM64)

### Building Binaries

```bash
# Build Linux x86-64 release binary
cargo build --release --target x86_64-unknown-linux-gnu

# Build Linux ARM64 release binary
cargo build --release --target aarch64-unknown-linux-gnu
```

---

## Multi-Architecture OCI Container Image

The OCI container image is built with Docker multi-stage builds using pinned base image digests:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t rockstream:0.59.3 \
  .
```

---

## Verifying Bit-for-Bit Reproducibility

1. Clone clean repository at release commit.
2. Build artifacts in two clean directories.
3. Compare SHA-256 digests against `SHA256SUMS` and `docs/evidence-manifest.json`.
