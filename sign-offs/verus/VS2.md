# VS2 sign-off

Status: local implementation complete; repository required-check enforcement and
the pinned Verus proof invocation remain owned by the applicable release gates.

- [x] VS2-01: [layout inventory](../../formal/verus/key-layouts.toml) records prefixes, discriminators, namespaces, and allocation assumptions; [prefix tests](../../crates/rockstream-storage/src/keys.rs) pass.
- [x] VS2-02: [signed-order kernel](../../crates/rockstream-verified/src/keys.rs) serves MIN/MAX and window adapters with extreme-value round trips and ordering tests.
- [x] VS2-03: [fixed-width codecs](../../crates/rockstream-verified/src/codecs.rs) and [factor-payload framing](../../crates/rockstream-storage/src/keys.rs) reject malformed lengths before slicing.
- [x] VS2-04: existing generic/catalog bytes and prefix behavior remain unchanged; [LFS and MinIO compatibility results](VS2-evidence.md) are recorded.
- [x] VS2-05: [power-of-two routing](../../crates/rockstream-verified/src/routing.rs) uses verified prefix clamping and mask bounds while retaining FNV-1a behavior.
- [x] VS2-06: [mutation matrix](../../formal/verus/vs2-mutation-matrix.toml) maps exact-output assertions covering sign, endian, length, discriminator, and mask regressions.

Definition-of-done record: [VS2 evidence](VS2-evidence.md). Unsupported cases are
the owner-specific unframed variable arrangements listed as `verified = false` in
the layout inventory; no persistent-format migration, balance, collision-freedom,
or minimal-movement claim is made.

- [ ] VS2-GATE: run the pinned `cargo-verus` proof command and register the required repository check.

Commands:

```sh
.cache/verus/0.2026.09.13.671956e/cargo-verus verify -p rockstream-verified --locked
python3 scripts/check-verus-manifest.py
cargo test --locked -p rockstream-verified --lib
cargo test --locked -p rockstream-storage keys --lib
cargo test --locked -p rockstream-plan --test virtual_bucket_routing_tests
```
