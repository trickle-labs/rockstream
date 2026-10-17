# v0.72 comparison arithmetic preflight — #139

**Structural validation only. Not capacity qualification or release sign-off.**

Command:

```
cargo run --manifest-path tools/r1-local-harness/Cargo.toml --bin verify-v072-comparisons -- --profile benchmarks/r1-local/profile-v072.toml --measurements evidence/v072/comparison-measurements.json
```

The command consumes **55 independently collected samples**: five repeats for
each of the 11 scenarios `one_key1k`, `one_key100k`, `one_key10m`,
`views_one`, `views20_private`, `views20_shared`, `worker1`, `worker2`,
`worker4`, `worker8`, `mixed_read`. It does not execute benchmarks, invent
values, fill gaps, or qualify process provenance. The profile must match
issued SHA-256 `d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a`.
No evidence is produced by the negative tests.

Comparison inputs must have one candidate binary digest, the same offered
load, 128-byte payload, Durable commits, 100 ms freshness target, and frozen
Apple M5 Pro/native LFS host. The worker count must match actually identified
nonzero unique PIDs. Group cardinalities must be 1K, 100K, and 10M;
compatible views must include one private view, 20 private views, and 20
shared views; worker scale must include 1, 2, 4 and 8 workers.

The gate checks the frozen five-run throughput CV <= 0.15; one-key state
writes per committed change max/min <= 1.10 with lookup latency separate;
shared-vs-private-20 state, writes, and CPU ratios <= 0.20, 0.20, 0.85;
and worker 2/4/8 throughput ratios >= 1.25/2.00/2.00.
It separately checks mixed read p99 <= 10 ms, durable commit p99 <= 25 ms,
correct-result freshness p99 <= 100 ms and generator delay p99 <= 5 ms.

Even with all arithmetic passing, the result explicitly says
`structural_preflight_only_not_capacity_qualified`. Input digest strings are
not independently cross-checked against retained file bytes by this gate.
Full independent oracle results, real executed worker/process identities,
freshness provenance, machine identity and repeated raw release runs must be
qualified externally under #138/#139/#141 before any claim or sign-off.
