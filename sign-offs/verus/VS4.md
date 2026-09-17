# VS4 sign-off

Status: local implementation complete; multi-process authority, external
backend qualification, and repository required-check enforcement remain owned
by the existing runtime and CI gates.

- [x] VS4-01: ADR 0005 defines scope, configured/reporting/active membership,
  generations, incarnations, activation/retirement, empty membership, and the
  exclusive frontier boundary.
- [x] VS4-02: the versioned report admission kernel and control adapter reject
  bad versions, scopes, generations, incarnations, and legacy scalar reports;
  stale accepted reports are no-ops.
- [x] VS4-03: activation and replacement require a bootstrap epoch at or above
  publication; retirement and replacement advance configuration generations.
- [x] VS4-04: `formal/verus/model-map.md` maps the executable kernels to the
  bounded M2 model, including rejected/no-op transitions and generation-scoped
  durable publication.
- [x] VS4-05: exclusive frontier tests and the FreshnessToken diagnostic-hash
  test keep semantic progress separate from token bookkeeping.
- [x] VS4-06: the worker V2 envelope routes through the guarded public service
  path; local tests cover reordering, stale authority, failover storage keys,
  and the new-member regression. Real multi-process qualification remains
  explicitly open.

Commands:

```sh
cargo test --locked -p rockstream-verified --lib
cargo test --locked -p rockstream-types --lib frontier
cargo test --locked -p rockstream-control --lib frontier::tests
fizz formal/m2_frontier_agg.fizz
python3 scripts/check-verus-manifest.py
```

The pinned `cargo-verus` command remains the proof gate. It is not installed in
this checkout (`cargo verus` is unavailable), so this sign-off does not claim
backend proof results; the VS4 evidence is the executable kernel tests,
manifest check, and bounded model run above.
