# v0.72 overload and beyond-RAM structural preflight (#140)

**This is not a qualification run or a release sign-off.** It validates only
completeness, topology, frozen thresholds, and internal consistency of supplied
records. It cannot independently attest that supplied process, binary, or
oracle digests match real retained artifacts. No generated sample becomes evidence.

Run after collecting two **real, distinct** release-process scenarios:

```shell
cargo run --manifest-path tools/r1-local-harness/Cargo.toml   --bin verify-v072-overload --   --profile benchmarks/r1-local/profile-v072.toml   --bundle evidence/v072/overload-and-recovery.json
```

The JSON must deserialize as `FaultEvidenceBundle` from `overload.rs`.
The `large_state` scenario must exceed the issued 48 GiB host's RAM, exercise
eviction, compaction and migration, and complete paged full-result recovery
within the frozen 900 seconds. The `overload` scenario must cover at least
1,800 seconds with bounded samples, active workers, useful control heartbeats
and committed changes, all eight fault types, and recovery against its **own**
independent complete-result oracle. The scenarios share the candidate release
binary but legitimately may have different full-result oracle digests.

The accepted ceilings come directly from the immutable issued
`profile-v072.toml` (SHA-256
`d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a`).
The 1–60 second declared sampling interval and two-interval maximum observation
gap are **tooling completeness requirements**, not newly issued product
thresholds. Every required field must be observed, not filled with fake or
historical fixture numbers. A successful CLI response reports exactly
`structural_preflight_only_not_capacity_qualified`.

Still required for #140: actual M5 Pro release-process fault injection, real
worker/process capture provenance, complete independent oracle derivation,
compaction/migration/restart transcripts, worker/resource RSS reconciliation,
bounded queue/retry and control-plane behavior, and retained raw log files.
The source-level tests use **synthetic negative-fixture data only**, which
must never be accepted as #132 capacity evidence.
