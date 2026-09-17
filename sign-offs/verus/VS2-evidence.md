# VS2 evidence record

Tested source: `e6f41d25489a5c3ef194af44d5b2df05dbe37593` plus the VS2 working tree before commit.

Artifact identity: workspace `0.67.0`; `Cargo.lock`; Verus release record in
[`formal/verus/toolchain.lock.toml`](../../formal/verus/toolchain.lock.toml).

Implementation and contract sources:

- [`formal/verus/manifest.toml`](../../formal/verus/manifest.toml), claims VS2-01 through VS2-06.
- [`formal/verus/key-layouts.toml`](../../formal/verus/key-layouts.toml), selected layouts and explicit exclusions.
- [`formal/verus/model-map.md`](../../formal/verus/model-map.md), caller obligations.
- [`docs/implementation-plans/rockstream-verus-implementation-plan.md`](../../docs/implementation-plans/rockstream-verus-implementation-plan.md), VS2 acceptance criteria.

Commands and results:

```text
python3 scripts/check-verus-manifest.py -> 12 claim(s) checked; 6 source marker(s); zero unchecked application assumptions.
cargo fmt -- --check -> passed
cargo test --locked -p rockstream-verified --lib -> 3 passed
cargo test --locked -p rockstream-storage keys --lib -> 20 passed; 79 filtered out
cargo test --locked -p rockstream-plan --test virtual_bucket_routing_tests -> 5 passed
cargo test --locked -p rockstream-ops --test factorized_join_durability_lfs_tests -> 1 passed
cargo test --locked -p rockstream-storage --test lfs_backend -> 14 passed
cargo test --locked -p rockstream-ops --test lfs_time_window -> 6 passed
cargo test --locked -p rockstream-storage --test minio_backend -> 7 passed
cargo test --locked --workspace --all-targets -> FAILED: unrelated existing `real_multi_process_management_status_is_an_exact_json_worker_lifecycle_transcript` did not observe memory pressure; changed crates' focused suites passed.
```

The pinned Verus command is recorded in `VS2.md` but was not runnable in this
environment because `verus` and `cargo-verus` were not installed. Full workspace
test results include one unrelated memory-pressure failure noted above. MinIO tests use the
repository's testcontainers setup; unsupported variable arrangement layouts and
all backend internals remain outside the claim.
