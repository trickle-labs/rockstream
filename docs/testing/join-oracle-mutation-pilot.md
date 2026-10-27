# Join oracle mutation pilot

Baseline: `2bb60012318fa966e1dc98866122daceabc59607` (issue #262 source review).
All mutations below were temporary source edits and were restored after their
targeted tests. This is a focused correctness pilot, not a general mutation score.

## Weighted history

The deterministic 2-way test uses valid weighted deltas for one matching key:

| Epoch | Left delta | Right delta | Expected complete output |
| --- | --- | --- | --- |
| 0 | `(1,10,+2)` | `(1,100,+3)` | `[(1,10,100,6)]` |
| 1 | `(1,10,+1)` | — | `[(1,10,100,9)]` |
| 2 | — | `(1,100,-1)` | `[(1,10,100,6)]` |
| 3 | `(1,10,-1)` | `(1,100,-1)` | `[(1,10,100,2)]` |
| 4 | `(1,10,-2)` | — | `[]` |
| 5 | `(1,10,+2)` | `(1,100,+1)` | `[(1,10,100,4)]` |

The source test asserts each literal output and also compares incremental and
independently accumulated batch bags after every epoch. The history covers
duplicate multiplicity, both prior-state probe terms, simultaneous updates,
cancellation, and reinsertion.

## Ranked mutation results

The model ranked candidates from the bilinear join equation. A small
non-LLM source-enumeration baseline independently listed `ΔL ⋈ R₀`,
`L₀ ⋈ ΔR`, `ΔL ⋈ ΔR`, preservation of negative weights, and staging of
negative deltas. Both selections chose the same five join mutations. All
produced the predicted exact mismatch; no false positive or surviving mutant
was observed. Confidence was high for each candidate because it changes one
explicit weight or retraction term while preserving the output key.

| Rank | Temporary fault | Predicted detector result | Observed exact rows |
| --- | --- | --- | --- |
| 1 | `process_right_delta`: use `w` instead of `l_weight * w` | Weighted history, epoch 2 | expected `[(1,10,100,6)]`; got `[(1,10,100,8)]` |
| 2 | `process_left_delta`: use `w` instead of `w * r_weight` | Weighted history, epoch 1 | expected `[(1,10,100,9)]`; got `[(1,10,100,7)]` |
| 3 | `commit_epoch`: use `*l_w` instead of `l_w * r_w` | Weighted history, epoch 0 | expected `[(1,10,100,6)]`; got `[(1,10,100,2)]` |
| 4 | Make negative right weights positive with `w.abs()` | Weighted history, epoch 2 | expected `[(1,10,100,6)]`; got `[(1,10,100,12)]` |
| 5 | Skip negative right deltas | Insert then delete right | expected `[]`; got `[(3,30,300,1)]` at epoch 1 |

The last case demonstrates an externally visible stale join row after a legal
insert/delete history. The per-epoch weighted assertions detect it before a
later epoch can hide the fault.

## Existing detector coverage

The independent `rockstream-ops` test
`correction_preserves_exact_weighted_many_to_many_join_rows` already detects
the `ΔL ⋈ ΔR` product fault: it expected weights `6,-4,-3,2,4` and observed
`2,2,-1,-1,1`. `join_delete_retracts_previous_output` also detects a dropped
negative right delta (`delete: []`). These are existing full-suite detectors,
so those faults are not suite-wide blind spots. Existing prior-state probe
tests use unit weights; the new oracle history adds exact multiweight coverage
for `ΔL ⋈ R₀` and `L₀ ⋈ ΔR`.

The 3-way deterministic case uses weights `2 * 3 * 4 = 24`. In the isolated
branch, applying the same missing-product fault to the chained join failed at
epoch 0 with expected `[(1,10,100,1000,24)]` and observed
`[(1,10,100,1000,2)]`. The 10k 3-way property also compares weighted bags at
each epoch.

## Shared average helper

`avg_from_sum_count` is used by both the aggregate implementation and batch
reference, so their comparison alone is correlated. The existing
`avg_computes_correct_fractional_average` and
`oracle_aggregate_genuinely_fractional_average` tests independently require
`(sum,count)=(7,3)` to yield `7/3`. A temporary integer-division mutation
returned `2`; the oracle test failed with `expected avg ~= 7/3, got 2`.
Both literal-result tests pass unchanged after restoration.
The adjacent contract checks are also source-grounded: count zero returns
`None` (`avg_undefined_when_count_zero`), negative input values have literal
operator and DataFusion expectations, and sum/count merge overflow is rejected
(`overflow_detected`). This pilot did not change those paths; it makes no new
claim about nullable SQL inputs or floating-point precision limits.

## Recovery and run scope

`lfs_join_crash_replay_bit_identical` now accumulates signed tuple weights and
asserts the complete result `[(5,50,500,1),(5,50,600,1)]` after loading the
persisted arrangement and replaying epoch 2. This uses the local filesystem
backend and a new operator instance; it is a simulated close/reload recovery,
not a process-kill test or a gateway-level materialized-view restart.

Targets run with no external service for oracle and aggregate unit tests. The
recovery target uses `TempDir` plus `LocalFileSystem`. Baseline targets:

```text
rtk cargo test -p rockstream-oracle --lib oracle_join_weighted_deltas_across_epochs
rtk cargo test -p rockstream-oracle --lib oracle_join_insert_then_delete_right
rtk cargo test -p rockstream-oracle --lib oracle_3way_join_deterministic
rtk cargo test -p rockstream-ops --lib correction_preserves_exact_weighted_many_to_many_join_rows
rtk cargo test -p rockstream-ops --lib join_delete_retracts_previous_output
rtk cargo test -p rockstream-ops --lib
rtk cargo test -p rockstream-ops --test lfs_join lfs_join_crash_replay_bit_identical
rtk cargo test -p rockstream-ops --lib avg_computes_correct_fractional_average
rtk cargo test -p rockstream-oracle --lib oracle_aggregate_genuinely_fractional_average
rtk cargo test -p rockstream-types --lib avg_undefined_when_count_zero
rtk cargo test -p rockstream-types --lib overflow_detected
```

Targeted test bodies complete in under 0.1s; source edits trigger rebuilds of
roughly 20–50s in the isolated workspace. The full oracle library suite took
107.54s.

On the isolated candidate branch, the complete `rockstream-oracle` library
suite passed: 140 tests (1 suite, 107.54s). After every mutation was restored,
its targeted baseline passed. The `rockstream-ops` library suite passed 204
tests (4 suites, 1.51s); the focused recovery, average, zero-count, and
overflow checks also passed. Related mutation workflow: #261.
