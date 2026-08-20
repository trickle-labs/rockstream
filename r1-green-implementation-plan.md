# R1 GREEN Implementation Plan

**Prepared:** 2026-08-20
**Reference profile:** `MBP-M5Pro-48GB-v1`
**Current decision:** `RED`
**Target decision:** `GREEN`

## 1. Objective

R1 must answer one question before v0.59.8 begins: did delta-native
persistence, shared arrangements, and factorized/filtered IVM reduce the work
and state caused by a changed row enough to continue the architecture program?

This plan proves that value on one available developer machine: a 16-inch
MacBook Pro with M5 Pro and 48 GB RAM. It does not claim production capacity.
R2 and v0.59.24 retain the full Kafka, PostgreSQL CDC, MinIO, 1/2/4/8-worker,
state-over-RAM, multi-host, cost, and headroom qualification.

R1 remains blocking. The smaller profile changes how the value is measured, not
whether wrong results, fake workers, or missing evidence are acceptable.

## 2. Reduced Scope

### Included in R1

- Exact one-key persistence behavior at 1K, 100K, and 10M live groups.
- One versus twenty shared consumers at 100K source rows.
- Shared source-index work and CPU per accepted change.
- Classic versus factorized execution at fan-out 100.
- One-worker ordinary aggregate and join regression against a local rebuild of
  v0.59.4 source.
- Current-candidate 1/2/4 real-worker scaling.
- Inserts, updates, deletes/retractions, complete output comparison, LFS
  persistence, and machine-readable raw evidence.

### Deferred to R2 and v0.59.24

- Eight-worker performance and production scaling floors.
- Kafka and PostgreSQL CDC throughput.
- MinIO and object-store request/cost measurements.
- State larger than RAM, overload, skew, migration, and worker-loss performance.
- Linux `perf`, cgroups v2, NUMA placement, multi-host network measurement, and
  production component headroom.
- Production cost per million accepted changes.

Existing Kafka, PostgreSQL, MinIO, recovery, and oracle correctness tests still
run as normal regression tests. Their performance is not part of local R1.

## 3. GREEN Criteria

Timing and resource comparisons use five alternating A/B pairs: pair 1 runs
A then B, pair 2 runs B then A, and so on, giving five samples per side. Each
pair uses the same seed and change-stream digest. For five samples `x_i`, the
sample coefficient of variation is `sqrt(sum((x_i - mean)^2) / 4) /
abs(mean)`. It must be no greater than 0.15. Five all-zero values have CV zero;
a zero mean with any nonzero value is invalid. Structural rows use exact
counters and complete output and are not softened by timing variance.

| Property | GREEN gate |
|---|---|
| One-key persistence | At 1K, 100K, and 10M live groups, insert/update/delete have the same mutation count per operation, no more than 10% logical-byte variation, and zero full-state entries visited. |
| Shared state | At 100K source rows, twenty shared consumers use no more than 1.5x the logical and LFS state of one consumer and at least 80% less state than twenty private arrangements. |
| Shared source-index maintenance | Twenty shared consumers use no more than 1.5x the key/trace work and 1.75x the process CPU per accepted change of one consumer. |
| Factorized intermediates | At fan-out 100, factorized execution produces at least 10x fewer flattened intermediate tuples than classic execution. |
| Factorized value | Factorized execution provides at least 1.5x throughput, or both CPU and encoded exchange bytes per accepted change fall by at least 33%. |
| Ordinary regression | Current one-worker aggregate and join throughput and p99 freshness regress by no more than 15% against the local B0 rebuild. |
| Four-worker trajectory | Every worker owns shards and performs nonzero input/output work. Four-worker uniform aggregate throughput is at least 2.0x one-worker throughput. |

Gate arithmetic is binding:

- For timing/resource comparisons, calculate each A/B ratio within its pair and
  gate on the median of the five unrounded paired ratios. Report formatting may
  round; evaluators may not round before deciding.
- For each insert, update, and delete persistence case, mutation counts must be
  identical at all three scales and `max(logical_bytes) / min(logical_bytes)`
  must be no greater than 1.10. Every full-state-visit counter must be zero.
- For logical bytes and LFS bytes independently, `shared_20 / shared_1` must be
  no greater than 1.50 and `shared_20 / private_20` no greater than 0.20.
- Source key builds and trace rows written each use their own
  `shared_20 / shared_1` ratio, no greater than 1.50. Source-index CPU seconds
  per accepted change use a ratio no greater than 1.75.
- Classic intermediates must be nonzero and
  `classic_intermediates / factorized_intermediates` at least 10. A zero
  factorized denominator passes when the classic count is nonzero.
- Factorized throughput passes at `factorized / classic >= 1.50`. The alternate
  resource gate requires both
  `factorized_cpu_per_visible_change / classic_cpu_per_visible_change <= 0.67`
  and `factorized_exchange_bytes_per_visible_change /
  classic_exchange_bytes_per_visible_change <= 0.67`.
- For aggregate and join separately, ordinary throughput requires
  `current / B0 >= 0.85` and p99 freshness requires `current / B0 <= 1.15`.
- Worker scaling uses `four_worker / one_worker >= 2.00`. The two-worker sample
  is published and checked for exact output and worker activity but has no
  numerical GREEN floor.

The decision rules are:

- `GREEN`: every row passes, all outputs match, and all evidence verifies.
- `YELLOW`: all correctness and structural rows pass, but exactly one timing or
  resource row misses its GREEN threshold. Pause for one profiling cycle.
- `RED`: any wrong result, full-state scan, missing evidence, idle declared
  worker, unstable measurement after one clean rerun, or more than one failed
  timing/resource row.

For the four-worker row only, 1.5x to less than 2.0x is YELLOW and below 1.5x is
RED.

## 4. Local Reference Profile

Add `benchmarks/r1-local/profile.toml`. The harness fills observed values before
sealing the profile:

- Exact Mac model, chip, performance/efficiency core counts, and 48 GB RAM.
- macOS version, filesystem, available disk, and Rust target/toolchain.
- AC power state, Low Power Mode state, and thermal-pressure state.
- Build flags, enabled features, lockfile digest, source SHA, and binary SHA-256.
- Docker Desktop version, CPU allocation, and memory allocation when Docker is
  used by a non-scored regression test.
- Unsupported controls such as NUMA placement or hard CPU affinity, recorded as
  unsupported rather than omitted.

Scored runs use native processes and LFS. The machine must be on AC power with
Low Power Mode disabled. Stop unrelated Docker containers and other sustained
CPU or disk workloads. The harness records free memory and thermal pressure
before and after every repetition.

The 10M structural proof runs by itself with Docker Desktop stopped. It is not a
five-repetition timing benchmark.

## 5. Deliverables

```text
benchmarks/r1-local/
  README.md
  profile.toml
  corpus.toml
  thresholds.toml
  schemas/
    profile.schema.json
    corpus.schema.json
    raw-sample.schema.json
    structural-results.schema.json
    decision.schema.json
  workloads/
    one-key-persistence.toml
    shared-arrangement.toml
    factorized-join.toml
    ordinary-aggregate.toml
    ordinary-join.toml
    uniform-worker-scaling.toml
  sql/
    *.sql

tools/r1-local-harness/
  Cargo.toml
  Cargo.lock
  src/
    main.rs
    artifact.rs
    cluster.rs
    corpus.rs
    evidence.rs
    load.rs
    metrics.rs
    oracle.rs
    process.rs
    report.rs
  tests/
    corpus_determinism.rs
    decision_mutations.rs
    process_isolation.rs
    summary_regeneration.rs

scripts/
  run-r1-local.sh
  check-r1-local-evidence.py
  check-r1-local-evidence.test.sh
  check-architecture-gates.sh
  check-architecture-gates.test.sh

evidence/r1-local/
  profile.json
  corpus.json
  candidates.json
  raw-samples.jsonl
  structural-results.json
  decision.json
  artifacts.sha256
```

Keep the evidence compact enough to commit. Profiles may use folded stack text
rather than large binary traces. No remote artifact store is required.

## 6. Implementation Slices

### S0: Freeze the local contract

Create the profile, corpus, and thresholds files before scored measurement.
`thresholds.toml` contains exactly the values from Section 3. The corpus fixes
schemas, SQL, seeds, row counts, fan-out, update/delete mix, the fixed
closed-loop load shape, warm-up duration, measurement duration, freshness
histogram buckets, and repetition order. Every timed workload uses eight
PGWire load lanes, 256-row transactions, at most one transaction awaiting
visibility per lane, 30 seconds of warm-up, and 60 seconds of measurement. A
lane issues its next transaction only after the prior transaction's output
frontier is query-visible, so accepted work cannot create an unbounded queue.

Add a digest command to `run-r1-local.sh`. Once the first scored run starts, a
profile, corpus, or threshold change creates a new profile revision and
invalidates all prior local R1 samples.

Green check:

- Repeated corpus generation produces byte-identical input and SQL digests.
- The Python verifier rejects any changed threshold, workload, or profile.

### S1: Identify B0 and the current candidate

Do not create B1.

Build `a4e4ad4` in a detached worktree using that commit's lockfile and pinned
Rust toolchain. Label the result `b0-v0.59.4-local-rebuild`; record that it is a
rebuild, not an archived historical binary. Hash the source tree, lockfile,
toolchain, binary, and effective configuration.

Build the final current candidate only after all R1 implementation changes are
complete. Query its public version surface and verify the reported source SHA
against the binary record.

B0 runs only the ordinary one-worker aggregate and join workloads. Use the SQL
and protocol subset supported by both artifacts. Current sharing,
factorization, and worker scaling are paired within the current candidate and do
not need a fabricated historical comparator.

Green check:

- Both binaries reproduce from clean worktrees.
- Candidate records and binary hashes match before every run.
- The verifier rejects a changed source or binary digest.

### S2: Make standalone workers execute real dataflows

The current worker role registers and sends heartbeats but does not execute an
assigned compiled view. Fix this before measuring worker scaling.

Implement the smallest process-bound path that supports the local uniform
aggregate and ordinary equi-join corpus while preserving the architecture
boundary needed by later versions:

1. Control assigns and fences shards to registered workers and sends a
   versioned deployment descriptor with shard, plan, schema, frontier, and
   storage identities.
2. Worker mode opens shard-local LFS storage, accepts the deployment, compiles
   deterministic operator IDs, restores state, and acknowledges readiness.
3. Gateway/source routing sends each source delta to the fenced shard owner
   rather than executing the worker-owned pipeline locally.
4. Runtime exchange messages carry workload, shard, epoch, operator, and fence
   identities. Stale owners fail closed.
5. Workers execute and persist deltas and publish output/frontier progress.
   Gateway reads merge shard results without running the same operator again.

Set `worker.execution_threads = 1` for the local scaling profile. One-worker and
four-worker runs then expose one and four worker execution threads without
requiring macOS CPU affinity. Gateway, control, and harness settings remain
identical between runs.

Likely owning files:

- `crates/rockstream-control/src/service.rs`, scheduler, and shard modules.
- `crates/rockstream-runtime/src/client.rs` and `exchange/*`.
- Worker startup in `crates/rockstream-cli/src/lib.rs`.
- Gateway commit and read routing.
- Versioned deployment messages in `rockstream-types`/`rockstream-plan`.
- Config, validation, resolver, and effective-config output for
  `worker.execution_threads`.

Add `crates/rockstream-cli/tests/r1_real_worker_data_plane_tests.rs`. Start the
real binary at 1, 2, and 4 workers through public interfaces. Assert exact final
rows, distinct PIDs and `WorkerId`s, shard ownership, and nonzero input/output
on every worker. A repeated label or idle process fails the test.

Green check:

- The process test passes at 1/2/4 workers on LFS.
- Killing a worker in a non-scored correctness case fences the old owner and
  preserves exact committed output after reassignment.

### S3: Add actual work and resource counters

Add counters at the production boundaries used by the local gate.

Delta persistence:

- State mutations and logical mutation bytes.
- Dirty keys.
- `full_state_entries_visited`, incremented whenever an ordinary commit walks
  existing state outside the changed keys.

Shared arrangements:

- Logical trace bytes, consumer metadata bytes, and LFS files/bytes by
  arrangement.
- Source key/capsule builds, trace rows written, and accepted source changes.
- Source-index CPU using the native thread CPU clock around canonical key
  construction plus `SharedArrangementTrace::commit_trace_batch` once per
  batch. Exclude attachment, reads, final projection, and sink work.

Factorized and classic paths:

- Input deltas, actual arrangement probes, flattened intermediate tuples,
  output deltas, changed state writes, encoded exchange bytes, factor payload
  rows, and factor payload bytes.
- Count classic intermediates immediately before the downstream aggregate.
- Count encoded exchange bytes at serialization, attributed by worker,
  workload, shard, operator, and strategy.

Worker activity:

- Shards owned, input rows, output rows, state writes, and exchange bytes per
  `WorkerId`.

Expose snapshots through the existing Prometheus endpoint. Restart processes
between repetitions so lifetime counters cannot leak between cells. Avoid
unbounded labels.

Green check:

- Focused counter tests assert exact deltas for known fixtures.
- Attachment/read-only work does not increment source-index maintenance.
- Encoded exchange counters equal the bytes produced by fixture serialization.

### S4: Add compile-time strategy control

Add one startup/deployment setting:

```text
[execution]
join_strategy = "auto"       # auto | classic | factorized
```

Thread it through gateway compilation and `try_compile_join_shape`.

- `auto` keeps the versioned selection rule.
- `classic` builds the normal join plus aggregate stages.
- `factorized` builds `FactorizedJoinAggregateOp` for eligible SQL and returns a
  coded error for ineligible SQL. It never silently falls back.

The setting is fixed when the view is compiled. It cannot switch a live graph
or create dual active graphs.

Add `crates/rockstream-cli/tests/r1_strategy_selection_process_tests.rs`.
Start the current binary in all three modes, assert effective configuration and
reported strategy through public surfaces, feed identical changes, and compare
complete outputs.

### S5: Correct the deterministic structural proofs

#### One-key persistence

Update `constant_write_amplification_scale_tests.rs` to use 1K, 100K, and 10M
live groups. At each scale:

1. Insert one new key.
2. Update one existing key with old-row retraction plus new-row insertion.
3. Delete one existing key.

Assert exact output Z-sets, exact state mutations, dirty keys, logical bytes,
final state, and zero `full_state_entries_visited`. Run this test in release
mode. Do not mock 10M or mark the test ignored.

#### Shared arrangements

Rewrite `scale_proof_20_views_sharing_tests.rs` with three independent clean
fixtures at 100K source rows:

- One consumer.
- Twenty equivalent consumers sharing one arrangement.
- Twenty deliberately private arrangements.

Drive each to the same frontier, flush LFS, and measure logical and physical
bytes. Assert complete output for every consumer, the 1.5x sharing ceiling, 80%
private-state savings, zero source rescans, and final reclamation.

#### Factorized execution

Feed the same fan-out-100 change stream to classic and factorized plans. Assert
both complete outputs, a nonzero classic intermediate count, at least 10x
intermediate reduction, and exact reconciliation of all work counters.

Structural test failures are RED. Wall-clock speed cannot override them.

### S6: Implement the independent local harness

`tools/r1-local-harness` is a small standalone Cargo workspace with its own
lockfile and no RockStream crate dependency. It interacts through process,
PGWire, HTTP metrics, and filesystem surfaces.

Use `tokio-postgres` for PGWire and bundled SQLite through `rusqlite` as the
independent oracle. Replay the canonical change log into SQLite outside the
timed interval, execute the admitted aggregate/join query, canonicalize rows and
multiplicities, and compare the complete result with RockStream. The harness
must not import DataFusion, `rockstream-oracle`, or operator helpers.

Implement commands:

```text
r1-local-harness prepare --profile <file> --corpus <file>
r1-local-harness build-candidates --output <dir>
r1-local-harness structural --output <dir>
r1-local-harness run --workload <name> --workers <n> --output <dir>
r1-local-harness evaluate --evidence <dir>
r1-local-harness verify --evidence <dir>
```

The harness:

- Starts and terminates complete process groups.
- Allocates unique ports and temporary LFS directories.
- Observes PIDs, `WorkerId`s, shard ownership, and worker counters rather than
  accepting caller-supplied identities.
- Writes each repetition atomically to `raw-samples.jsonl`.
- Records canonical input, RockStream output, and SQLite oracle output digests
  for every run, and retains full logs and canonical rows on failure.
- Exits nonzero on wrong output, missing worker activity, unstable identity,
  missing counters, or incomplete evidence.

### S7: Collect local measurements

Use fixed seeds and the same generated input for every paired comparison.

| Workload | Scale and comparison |
|---|---|
| One-key persistence | Structural release-mode test at 1K/100K/10M. |
| Shared arrangement | Current candidate, 100K rows, one shared versus twenty shared versus twenty private. |
| Factorized join | Current candidate, at least 10K changed rows, fan-out 100, classic versus factorized. |
| Ordinary aggregate | B0 rebuild versus current, one worker, 100K rows. |
| Ordinary join | B0 rebuild versus current, one worker, 100K rows, fan-out 4. |
| Uniform scaling | Current candidate, 100K live groups plus a fixed change stream, 1/2/4 workers. |

For each timing/resource cell:

- Run five paired repetitions, five samples for each comparison side.
- Alternate A/B and B/A order with the fixed order in `corpus.toml`.
- Warm for 30 seconds, then measure for 60 seconds.
- Use eight PGWire lanes with 256-row transactions and one transaction awaiting
  visibility per lane.
- Record accepted and query-visible changes, throughput, p50/p95/p99 freshness,
  process CPU,
  source-index CPU when applicable, RSS, encoded exchange bytes, queue depth,
  and all structural counters.
- At 60 seconds, stop submission and include the bounded drain until every
  accepted transaction is query-visible. Define throughput as all accepted and
  query-visible changed rows divided by measurement plus drain duration.
  Freshness is the interval from successful source-transaction commit to the
  corresponding output frontier becoming query-visible.
- Calculate p99 as the upper bound of the first sealed microsecond histogram
  bucket whose cumulative count reaches `ceil(0.99 * sample_count)`.
- Calculate median paired ratios and sample coefficient of variation exactly as
  specified in Section 3. One complete five-pair rerun is allowed after clearing
  an environmental problem; retain the rejected batch.

The harness records thermal pressure and free memory before and after every
repetition. A repetition with macOS serious/critical thermal pressure, swap
thrash, or an unrelated sustained process is invalid; invalidate the whole
five-repetition batch rather than dropping that one sample.

### S8: Generate and enforce the decision

`r1-local-harness evaluate` generates `evidence/r1-local/decision.json` from raw
samples and structural results. `scripts/check-r1-local-evidence.py` uses only
the Python standard library to independently regenerate medians, coefficients
of variation, ratios, and the final decision.

All evidence files validate against the versioned JSON Schemas in
`benchmarks/r1-local/schemas`. A raw sample includes schema version, run and
pair IDs, A/B order, candidate and binary IDs, profile/corpus/threshold digests,
workload, strategy, worker count, seed and change-stream digest, monotonic
duration, accepted and visible changes, freshness histogram bounds/counts,
per-process user plus system CPU nanoseconds, RSS bytes, logical/LFS/exchange
bytes, queue depth, operator counters, per-worker activity, canonical
RockStream-output digest, canonical SQLite-oracle-output digest, and their
equality verdict. Structural results carry the same input and output digests.
Units are part of field names or schema descriptions.

Process CPU is the sum of user plus system CPU deltas for control, gateway, and
all worker PIDs in the candidate cluster, excluding harness, SQLite oracle, and
build processes. Each process exposes cumulative CPU through its bounded
metrics surface; the harness snapshots it at measurement boundaries. LFS bytes
are allocated bytes under the run's sealed state directories after flush and
compaction. Logical bytes are bytes submitted to the state mutation API before
encoding or compression. Encoded exchange bytes are counted after serialization
and before transport framing. CPU and exchange resource gates divide these
totals by accepted changes, all of which must be query-visible after the bounded
drain. The decision schema records each raw numerator, denominator, paired
ratio, CV, threshold, inclusive comparator, and verdict.

Mutation tests must reject:

- Changed candidate, profile, corpus, threshold, or binary digest.
- Missing, duplicated, or non-finite samples.
- A summary that does not regenerate from raw values.
- Wrong output, unequal RockStream/oracle digests, or a missing input,
  RockStream-output, or oracle-output digest.
- A declared worker with no shard, input, or output work.
- A GREEN decision with any failed structural or timing row.

`check-architecture-gates.sh` blocks v0.59.8 implementation/sign-off unless the
local decision is GREEN and every checked-in evidence digest verifies. Its
self-test removes and mutates each required artifact and proves the gate fails.

## 7. Execution Order

Implement in this order:

1. Commit the amended R1 contract, profile, corpus, and thresholds.
2. Implement and prove the real 1/2/4-worker data path.
3. Add work/resource counters and strategy control.
4. Correct the three deterministic structural proofs.
5. Implement and self-test the standalone local harness.
6. Build the final current candidate and local B0 rebuild.
7. Run structural proofs and all five-pair timing/resource cells.
8. Generate, independently verify, and publish the checked-in decision.
9. Update the R1 sign-off and enable the architecture gate.

Do not tune against scored data and keep the same candidate identity. A product
change creates a new current candidate and reruns all current-candidate cells.
A harness, corpus, profile, or threshold change invalidates all local R1 runs.

## 8. Validation Commands

During implementation:

```text
rtk cargo test -p rockstream-cli --test r1_real_worker_data_plane_tests
rtk cargo test -p rockstream-cli --test r1_strategy_selection_process_tests
rtk cargo test -p rockstream-ops --test constant_write_amplification_scale_tests --release
rtk cargo test -p rockstream-sim --test scale_proof_20_views_sharing_tests
rtk cargo test -p rockstream-ops \
  --test factorized_join_aggregate_oracle_tests \
  --test factorized_star_join_oracle_tests \
  --test delta_amplification_governor_tests
rtk cargo test --manifest-path tools/r1-local-harness/Cargo.toml --locked
rtk cargo fmt --manifest-path tools/r1-local-harness/Cargo.toml -- --check
rtk cargo clippy --manifest-path tools/r1-local-harness/Cargo.toml \
  --locked --all-targets -- -D warnings
rtk bash scripts/check-r1-local-evidence.test.sh
rtk bash scripts/check-architecture-gates.test.sh
```

Run the local gate:

```text
rtk ./scripts/run-r1-local.sh prepare
rtk ./scripts/run-r1-local.sh structural
rtk ./scripts/run-r1-local.sh measure
rtk ./scripts/run-r1-local.sh evaluate
rtk python3 scripts/check-r1-local-evidence.py evidence/r1-local
```

Final repository checks:

```text
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --locked --all-targets -- -D warnings
rtk cargo test --workspace --locked
rtk ./scripts/check-exit-criteria.sh
rtk bash scripts/check-architecture-gates.sh
```

On the 48 GB machine, run one heavy command at a time. Stop Docker for the 10M
proof, use bounded Cargo build jobs, and check for orphaned Cargo/test processes
after any interrupted run.

## 9. Failure Routing

| Failed row | Reopen |
|---|---|
| One-key persistence | v0.59.5 delta-state commit path. |
| Shared state or source-index work | v0.59.6 arrangement catalog, trace, and key maintenance. |
| Factorized intermediate or value | v0.59.7 compiler, factorized operator, predicate transfer, and governor. |
| Ordinary regression | Profile first, then the owning v0.59.5-v0.59.7 hot path. |
| Idle workers or four-worker trajectory | v0.59.5 worker deployment, routing, exchange, and shard execution. |

A RED result remains useful. It identifies the architecture slice that must be
repaired without requiring production hardware.

## 10. Definition of Done

- [ ] The roadmap and thresholds define the Mac-local R1 gate and defer full
      capacity qualification to R2/v0.59.24.
- [ ] B0 is honestly labeled as a local source rebuild; no B1 is invented.
- [ ] Current 1/2/4-worker processes own shards and perform nonzero work.
- [ ] The standalone harness imports no RockStream crate and uses complete
      SQLite oracle output.
- [ ] The 1K/100K/10M insert/update/delete proof passes with zero full scans.
- [ ] One versus twenty sharing and private-state savings pass at 100K rows.
- [ ] Source-index work and CPU ratios pass.
- [ ] Classic and factorized complete outputs and actual counters reconcile.
- [ ] Factorized intermediate and throughput/resource gates pass.
- [ ] Ordinary B0/current regressions remain within 15%.
- [ ] Four current workers reach at least 2.0x one-worker throughput with exact
      output and nonzero work on every worker.
- [ ] Five-repetition timing cells have coefficient of variation no greater
      than 15%.
- [ ] Raw local evidence regenerates the same GREEN result in Rust and Python.
- [ ] CI rejects v0.59.8 when local R1 evidence is absent or altered.
