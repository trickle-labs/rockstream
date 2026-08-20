# Scalability Value Review - v0.59.7

**Reviewed:** 2026-08-20
**Candidate:** `b5754fe` (`main`)
**Decision:** **RED**

## Decision

The implementation sign-offs do not establish that delta-native persistence,
durable shared arrangements, and factorized/filtered IVM produced enough
measurable value to continue to v0.59.8. This is an evidence-readiness failure,
not a measurement that the architecture missed a numerical threshold. The
claimed historical baselines and raw v0.59.6/v0.59.7 comparisons were not
published, several proof tests do not execute the cases their sign-offs claim,
and registered standalone workers do not execute assigned dataflows.

The roadmap has since amended R1 to a bounded developer-profile architecture
gate. That makes a credible rerun feasible on the available MacBook; it does
not convert missing evidence into a pass. Every amended row is still unproven,
so `GREEN` and `YELLOW` remain unavailable. Per the roadmap, v0.59.8 cannot
begin while this decision is `RED`.

## Contract Amendment

The active R1 profile is `MBP-M5Pro-48GB-v1`: a 16-inch MacBook Pro with M5 Pro
and 48 GB RAM. R1 uses native LFS, public PGWire/process surfaces, exact
structural counters, complete independent-oracle output, and five alternating
A/B pairs for timing/resource rows, giving five samples per side with
coefficient of variation no greater than 15%. Sample CV is
`sqrt(sum((x_i - mean)^2) / 4) / abs(mean)`; five all-zero values have CV zero,
and a zero mean with any nonzero value is invalid. Timed workloads use the
sealed eight-lane, 256-row, visibility-bounded closed-loop PGWire load shape.

R1 rebuilds `a4e4ad4` only for the one-worker ordinary-workload B0 comparison.
No historical B1 artifact exists, and none will be invented. Kafka,
PostgreSQL CDC, MinIO, eight-worker, state-over-RAM, multi-host, production
cost, and component-headroom qualification remain mandatory at R2/v0.59.24.
The reduced gate proves local architecture value, not production capacity.

## Gate Results

| R1 developer-profile property | GREEN gate | Result | Current evidence |
|---|---:|---|---|
| One-key persistence at 1K/100K/10M live groups | Same mutation count, ≤10% logical-byte variation, zero full-state entries visited | **NOT PROVEN** | `constant_write_amplification_scale_tests.rs` executes 1K, 100K, and **1M**, not 10M. It applies one positive delta, not insert/update/delete. No full-state-visit counter exists. |
| Twenty shared consumers versus one at 100K rows | ≤1.5× logical and LFS state; ≥80% less than twenty private arrangements | **NOT PROVEN** | `scale_proof_20_views_sharing_tests.rs` compares `trace.byte_size()` with itself after registering twenty consumers. It has no independent one-consumer or private-arrangement fixtures. |
| Shared source-index maintenance | ≤1.5× key/trace work and ≤1.75× process CPU per accepted change | **NO MEASUREMENT** | The scale test records neither maintenance work nor CPU time. |
| Factorized intermediates at fan-out 100 | ≥10× fewer flattened intermediate tuples | **NOT PROVEN** | Oracle tests prove exact output and bounded payload behavior, but no classic denominator or actual target-workload intermediate count exists. |
| Factorized throughput or resources | ≥1.5× throughput or both CPU and encoded exchange bytes per accepted, query-visible change reduced by ≥33% | **NO MEASUREMENT** | No forced classic/factorized runner or comparable raw samples exist. |
| One-worker ordinary aggregate and join versus rebuilt B0 | Throughput and p99 freshness regress by ≤15% | **NO MEASUREMENT** | No local B0 rebuild and current-candidate paired results exist. |
| Real-worker uniform aggregate at 1/2/4 workers | Nonzero work on every worker; GREEN ≥2.0×, YELLOW ≥1.5× and <2.0×, RED <1.5× at four workers | **NOT PROVEN** | The only four-worker value is a hard-coded `152000.0` serialization fixture. Standalone workers register and heartbeat but do not execute assigned dataflows. |

No unmeasured row is treated as a pass.

Decision precedence matches the roadmap. An environmentally invalid batch gets
one complete clean rerun; a second unstable batch is RED. With stable evidence,
one failed timing/resource row is YELLOW and more than one is RED. Wrong output,
a full-state scan, missing evidence, an idle worker, or four-worker scaling below
1.5× is always RED. Only GREEN unblocks v0.59.8.

## Evidence Audit

### Baseline artifacts

The v0.59.4 and v0.59.5 repository trees contain no committed B0/B1 workload
manifest, benchmark result, or raw CPU/network/storage profile. This absence is
why the amended contract requires an honestly labeled local B0 source rebuild
and forbids a synthetic B1. The files added for v0.59.5 provide:

- `ProcessIsolationAuditor`, which validates supplied PID and `WorkerId`
  values but does not launch or measure workers.
- `MultisetOracle`, whose test validates a small in-memory aggregate fixture by
   passing its own expected result back to the oracle, not output from a running
   engine process.
- `S1BaselineMetrics`, a nine-field serialization type.
- `test_s1_baseline_metrics_structure`, which constructs hard-coded values and
  verifies only JSON round-tripping.

`S1BaselineMetrics` cannot represent either the historical production contract
or the amended local contract: it has no candidate or workload identity,
repetitions, variance, CPU, RSS, exchange bytes, topology, or raw sample
references.

### v0.59.6 and v0.59.7 artifacts

The v0.59.6 scale proof establishes canonical sharing, one physical catalog
entry, exact attachment output, and zero source-rescan requests. Its state ratio
does not measure one consumer against twenty, and it has no CPU measurement.

The v0.59.7 correctness suites establish oracle equivalence, bounded governor
refusal, persistence/recovery, pgwire reachability, and the absence of live
plan cutover. They do not implement the plan's required shadow benchmark or
publish its comparison artifact. The v0.59.7 evidence file describes the
external harness result only as “benchmark oracle and baseline serialization.”
The factorized operator's `joined_intermediate_rows()` reports zero by design,
but no target-workload run measures a classic intermediate-row denominator, so
that counter alone cannot establish the required reduction ratio.

The checked-in evidence manifest does not fill this gap. Its workload list has
no B0/B1 corpus identity, and its raw metrics contain only generic recovery and
steady-state samples. It identifies semantic version `0.59.7` with source SHA
`2bdf75e...`, not the reviewed `b5754fe` candidate.

## Raw Review Evidence

Repository-tree audit:

```text
git ls-tree -r --name-only f341b92^
git ls-tree -r --name-only f341b92
```

Neither tree contains a v0.59.5 B0/B1 benchmark artifact or raw profile. The
only newly added benchmark files are the process-isolation and external-harness
tests described above.

Source assertions inspected:

```text
crates/rockstream-ops/tests/constant_write_amplification_scale_tests.rs
crates/rockstream-sim/tests/scale_proof_20_views_sharing_tests.rs
crates/rockstream-sim/tests/external_benchmark_harness_tests.rs
crates/rockstream-test-support/src/external_harness.rs
docs/evidence-manifest.json
```

Focused validation after the audit:

```text
rtk cargo test -p rockstream-sim \
   --test external_benchmark_harness_tests \
   --test scale_proof_20_views_sharing_tests
# 3 passed

rtk cargo test -p rockstream-ops \
   --test constant_write_amplification_scale_tests \
   --test factorized_join_aggregate_oracle_tests \
   --test factorized_star_join_oracle_tests \
   --test delta_amplification_governor_tests
# 20 passed
```

These tests confirm their local correctness assertions. They do not provide
the amended structural proofs or quantitative R1 comparisons.

## Required Rerun

1. Seal `MBP-M5Pro-48GB-v1`, the local corpus, closed-loop load shape,
   thresholds, configurations, seeds, and independent oracle inputs.
2. Rebuild `a4e4ad4` as B0 and build the final current candidate. Bind each run
   to source, binary, profile, corpus, and configuration digests. Do not create
   B1.
3. Implement the process-bound worker data plane and prove that 1/2/4 workers
   own shards, execute changes, and produce exact output through public
   interfaces.
4. Correct the 1K/100K/10M insert/update/delete proof and independently measure
   one shared consumer, twenty shared consumers, and twenty private
   arrangements, including source-index work and CPU.
5. Add public `auto|classic|factorized` strategy control and actual work
   counters. Run identical fan-out-100 input through both strategies and
   compare complete output, intermediates, throughput, CPU, and encoded
   exchange bytes.
6. Run the ordinary B0/current and current 1/2/4-worker timing cells for five
   alternating A/B pairs on native LFS. Publish every raw sample and reject
   coefficient of variation above 15%.
7. Regenerate this review and its machine-readable decision independently from
   checked-in local evidence. Record YELLOW when exactly one stable
   timing/resource row misses, but do not begin v0.59.8 until every amended R1
   row passes and the decision is GREEN.