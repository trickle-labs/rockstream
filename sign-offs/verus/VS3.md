# VS3 sign-off

Status: local implementation complete; repository required-check enforcement and
the external backend qualification remain owned by the existing gates.

- [x] VS3-01: `ArrowZSet::try_new`/`validate` enforce aligned row weights; `select_rows` is ordered gather with repeated-index preservation, explicit out-of-range errors, and frontier preservation for empty output.
- [x] VS3-02: verified primitive alignment, index, and checked-weight kernels serve the Z-set adapter; Arrow decoding remains explicitly outside the proof boundary.
- [x] VS3-03: staging computes candidate sum/count and byte accounting before mutation; reused-stager overflow leaves the prior entry unchanged.
- [x] VS3-04: the verified aggregate transition serves `AggregateOp` and rejects invalid negative-count and zero-count/nonzero-sum states.
- [x] VS3-05: aggregate output and dirty-key mutations are built before planned state is installed; later transition failure leaves earlier state authoritative and emits no result.
- [x] VS3-06: existing oracle, capacity, delta-native, constant-write, NULL, decimal, deletion, and release-path tests remain the qualification surface; large-state spill qualification remains owned by v0.67.1 and VS5.

Commands:

```sh
cargo test --locked -p rockstream-verified --lib
cargo test --locked -p rockstream-ops --lib zset::tests
cargo test --locked -p rockstream-ops --lib aggregate::tests
cargo test --locked -p rockstream-ops --test delta_native_aggregate_tests
cargo test --locked -p rockstream-ops --test constant_write_amplification_scale_tests
```

The pinned `cargo-verus` command remains the proof gate. Its current checkout
also reports pre-existing VS2 codec, routing, and signed-order proof/library
compatibility failures; those are not treated as VS3 evidence.
