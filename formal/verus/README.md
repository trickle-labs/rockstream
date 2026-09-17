# Verus verification

VS0 pins the Verus release, solver source, support-crate versions, and Rust
compiler in [`toolchain.lock.toml`](toolchain.lock.toml). The archive contains
`verus`, `cargo-verus`, and the matching solver.

RockStream production code uses Rust 1.88. Verus uses its own pinned verifier
compiler, currently Rust 1.98.1 on supported macOS and Linux hosts. Install
that compiler before running the proof gate if Verus requests it.

Install the pinned tools on a supported host:

```sh
export PATH="$(bash scripts/install-verus.sh):$PATH"
make verify-rust
make verify-proof-contracts
```

The verified crate is also compiled and exercised with ordinary Cargo:

```sh
cargo test --locked -p rockstream-plan --test virtual_bucket_routing_tests
cargo build --locked --release -p rockstream-cli
```

`cargo verus verify` is the proof gate. Missing tooling, an empty verified
target, a failed theorem, or a failed contract checker is an error. The
production function remains the only implementation; specifications and proof
code do not select a different runtime algorithm.
