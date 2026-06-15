# RockStream Formal Specifications

This directory contains FizzBee specifications for the RockStream distributed coordination protocols.

## Index of Specifications

| Spec File | M-id | Description | DESIGN.md Anchor |
|---|---|---|---|
| [`formal/smoke.fizz`](file:///Users/grove/projects/rockstream/formal/smoke.fizz) | — | Smoke test specification to verify toolchain correctness. | — |
| [`formal/m1_epoch_commit.fizz`](file:///Users/grove/projects/rockstream/formal/m1_epoch_commit.fizz) | M1 | CALM Epoch-Commit Protocol & Per-Shard WriteBatch Atomicity | §8.4, §9 |
| [`formal/m2_frontier_agg.fizz`](file:///Users/grove/projects/rockstream/formal/m2_frontier_agg.fizz) | M2 | Asynchronous Frontier Aggregation Protocol | §3.2, §8.3–§8.6 |

## Running Verifications

To verify all specifications, run:
```bash
make verify
```
This runs the FizzBee model checker on each `.fizz` file and fails the check if any safety, liveness, or coverage assertion is violated.
