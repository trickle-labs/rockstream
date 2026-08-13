# RockStream Formal Specifications

This directory contains FizzBee specifications for the RockStream distributed coordination protocols.

## Index of Specifications

| Spec File | M-id | Description | DESIGN.md Anchor |
|---|---|---|---|
| [`smoke.fizz`](smoke.fizz) | — | Smoke test specification to verify toolchain correctness. | — |
| [`m1_epoch_commit.fizz`](m1_epoch_commit.fizz) | M1 | CALM Epoch-Commit Protocol & Per-Shard WriteBatch Atomicity | §8.4, §9 |
| [`m2_frontier_agg.fizz`](m2_frontier_agg.fizz) | M2 | Asynchronous Frontier Aggregation Protocol | §3.2, §8.3–§8.6 |
| [`m3_sink_2pc.fizz`](m3_sink_2pc.fizz) | M3 | Exactly-Once Sink 2PC Protocol (`NativeIdempotent`/`FencingTokenRequired`/`CheckBeforeCommit` profiles) | §11 |
| [`m4_self_fencing.fizz`](m4_self_fencing.fizz) | M4 | Worker Self-Fencing, Lease Uniqueness & Recovery Progress | §11.6 |
| [`m6_shard_migration.fizz`](m6_shard_migration.fizz) | M6 | Shard migration protocol | §8 |
| [`m7_control_plane_ha.fizz`](m7_control_plane_ha.fizz) | M7 | Control-Plane Raft Leader Election & Leader-Only Write Fencing (3-node quorum, composed with M4's shard fencing) | §3 (Three Logical Tiers, Tier 3 Raft bootstrap) |

The six active specs are a hard gate in the `formal-verify` CI job
(`.github/workflows/ci.yml`) as of v0.45.2 — a red result on any of them
blocks merge; `continue-on-error` is not used for any spec.

## Running Verifications

To verify all specifications, run:
```bash
make verify
```
This runs the FizzBee model checker on each `.fizz` file and fails the check if any safety, liveness, or coverage assertion is violated.

`retired/m5_cold_tier_sink.fizz` records the v0.52.4 retirement of the removed
cold-tier connector protocol; it is historical and not a verification gate.
