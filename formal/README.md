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

All five specs are a hard gate in the `formal-verify` CI job
(`.github/workflows/ci.yml`) as of v0.42.3 — a red result on any of them
blocks merge; `continue-on-error` is not used for any spec.

## Running Verifications

To verify all specifications, run:
```bash
make verify
```
This runs the FizzBee model checker on each `.fizz` file and fails the check if any safety, liveness, or coverage assertion is violated.
