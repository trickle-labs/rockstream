# PR critical-path investigation, 2026-10-22

## Result and limits

Remove the production-image dependency from gateway, which never consumes that
image, then balance test owners and parallelize independent builds/benchmarks.
Every existing case and assertion is retained. The four million-row proportionality
cases move to the existing nightly/tag lane; four new 10,000-row PR cases execute
the same shared assertions for all operator families. HA still runs the
identity-checked production image. Durability, recovery, transaction, protocol,
security, formal and performance checks stay mandatory. Existing required-check
names, features and waivers are preserved; the aggregate gains the independently
scheduled CLI-only build. No retries or selective source execution are added.

For the initial dependency-only commit, a queue-free replay of eight successful PR runs using measured job
durations estimates median critical-path time **95.92 → 69.70 minutes (27.3%)**.
This is an estimate, not a hosted before/after benchmark. The observed baseline
median including queues is **130.67 minutes**; it must not be compared directly
with the queue-free estimate. The 50% target is not established: gateway and the
parallel Verus jobs still set a roughly one-hour floor in that initial replay.
The subsequent changes below attack those independent costs. Their hosted end-to-end
effect remains unmeasured; the original 27.3% estimate is not a measurement of the
expanded change.

## Baseline method and sample

Inspected the latest 100 CI runs, then the previous API page to find a rerun.
The detailed sample is the eight latest completed successful PR runs and six
latest completed failed PR runs available when collection began, October 21–22.
Cancelled/superseded updates, scheduled runs and pushes are excluded from these
distributions. This is a convenience sample of adjacent revisions, not a controlled
benchmark. The separate five-attempt October 20 rerun is discussed below.

Sources: GitHub Actions run metadata; `actions/runs/{id}/jobs` and attempt jobs;
job logs; `ci-test-accounting-*` artifacts from runs 39726733128 and 39682713757.
The local source baseline is `90c468a69c00796810e24a646a65d0f5340e9854`.
Run links below preserve candidate identity and runner information. Timings are
minutes rounded to two decimals; calculations use unrounded timestamps.

* Time-to-green: workflow `created_at` to successful `Check.completed_at`.
  This is a proxy for PR-update latency; webhook delivery before workflow creation
  is not measured. It describes the CI aggregate, not unrelated workflows.
* Time-to-first-failure: workflow creation to the earliest failed step completion,
  not the later aggregate failure. A failure inside a buffered inventory invocation
  may have occurred earlier; the old artifacts lack case timestamps.
* Queue: job `created_at` to `started_at`. Dependency waiting precedes dependent
  job creation and is accounted separately. Summed job queue time is neither
  elapsed critical-path time nor billable cost.
* Runner-minutes: sum of active jobs' `completed_at - started_at`, excluding skipped
  jobs. These are occupied runner minutes, not billing-rounded or OS-priced minutes.
  Test/image jobs use `ubuntu-latest`; Verus also uses `macos-14`.
* Critical path: trace the final prerequisite back through the workflow DAG;
  never sum parallel jobs. All 14 sampled runs finish their prerequisites at gateway.

| Run | Result | Green/end | First failure | Runner-minutes |
| --- | --- | ---: | ---: | ---: |
| [39616074604](https://github.com/trickle-labs/rockstream/actions/runs/39616074604) | success | 123.00 | — | 438.27 |
| [39616089401](https://github.com/trickle-labs/rockstream/actions/runs/39616089401) | success | 138.33 | — | 442.95 |
| [39616365011](https://github.com/trickle-labs/rockstream/actions/runs/39616365011) | success | 139.87 | — | 446.43 |
| [39650684782](https://github.com/trickle-labs/rockstream/actions/runs/39650684782) | success | 152.97 | — | 346.48 |
| [39667646178](https://github.com/trickle-labs/rockstream/actions/runs/39667646178) | success | 90.92 | — | 446.13 |
| [39677010177](https://github.com/trickle-labs/rockstream/actions/runs/39677010177) | success | 103.98 | — | 435.32 |
| [39682713757](https://github.com/trickle-labs/rockstream/actions/runs/39682713757) | success | 153.35 | — | 436.67 |
| [39726733128](https://github.com/trickle-labs/rockstream/actions/runs/39726733128) | success | 89.87 | — | 449.80 |
| [39745118211](https://github.com/trickle-labs/rockstream/actions/runs/39745118211) | failure | 160.93 | 118.22 | 360.00 |
| [39746786309](https://github.com/trickle-labs/rockstream/actions/runs/39746786309) | failure | 189.05 | 145.82 | 371.17 |
| [39772037789](https://github.com/trickle-labs/rockstream/actions/runs/39772037789) | failure | 96.87 | 6.35 | 415.68 |
| [39772077172](https://github.com/trickle-labs/rockstream/actions/runs/39772077172) | failure | 126.70 | 29.73 | 431.72 |
| [39792260674](https://github.com/trickle-labs/rockstream/actions/runs/39792260674) | failure | 96.73 | 51.18 | 361.98 |
| [39792742597](https://github.com/trickle-labs/rockstream/actions/runs/39792742597) | failure | 81.63 | 17.70 | 395.45 |

Successful PRs: n=8, median green 130.67, range 89.87–153.35, median cost 440.61.
Failed PRs: n=6, median first failure 40.46, range 6.35–145.82. Neither sample
supports a reliable p95; report maxima rather than presenting them as stable tail
percentiles. A cancelled update is censored, not a fast green or detected regression.

### Phase separation and queue effects

The low-queue [39726733128](https://github.com/trickle-labs/rockstream/actions/runs/39726733128)
path is selection → Docker → gateway → Check, finishing in 89.87 minutes:

| Phase | Measured minutes | Evidence / interpretation |
| --- | ---: | --- |
| Initial selection queue / execution | 0.03 / 0.12 | Job timestamps |
| Docker queue / execution | 0.05 / 24.93 | Job timestamps |
| Image build step | 24.58 | BuildKit step; includes build and image/cache export |
| Cargo release compilation within image step | 21.15 | `Finished release … in 21m 09s` in image log |
| Gateway queue / execution | 0.05 / 64.05 | Starts only after image completes |
| Gateway cache restore / disk cleanup | 0.25 / 1.50 | Steps; cache log reports 864 MB restored |
| Gateway compilation | 20.58 | Inventory package build time, counted once |
| Gateway harness execution | 41.31 | Sum of target execution time |
| Aggregate queue / execution | 0.53 / 0.08 | Job timestamps |

The remaining image-step time is mixed setup/export work, not test execution.
Container startup inside tests is included in target execution; existing reports
cannot separate it from assertions. HA compiles for 3.87 minutes and executes for
0.48 minutes after the image. It is not the critical path.

The slow successful [39682713757](https://github.com/trickle-labs/rockstream/actions/runs/39682713757)
takes 153.35 minutes: selection queues for 21.12 minutes, Docker queues for 31.95
then runs 27.73, gateway queues for 0.05 then runs 72.22. Gateway compilation is
29.37 and execution 40.26 minutes. Similar execution but very different queue and
compile time explains much of the variance. The sample's summed job queue median
is 180.12 minutes; it must not be added to the elapsed baseline.

### Reruns and retries

[39377498652](https://github.com/trickle-labs/rockstream/actions/runs/39377498652)
took 723.63 minutes from initial creation to final Check success over five attempts.
Attempt 1 consumed 385.75 runner-minutes; subsequent actual execution consumed
29.57, 11.95, 29.62 and 35.90, for **492.78 total**, including 107.03 extra minutes.
GitHub attempt responses carry forward successful jobs with new IDs but their old
timestamps: deduplicate by job name and start/completion times, not job ID.

The performance gate failed four attempts and succeeded on the fifth. Its first
attempt log shows the existing ops benchmark retries 1/2 and 2/2, each about 2.7
minutes, before failure. The final attempt spends 12m34s, 6m38s and 9m49s compiling
the ops, storage and runtime bench profiles, plus 1m37s compiling the dev comparator.
Long gaps between attempts are not runner consumption. These data do not prove
host noise caused the failures. No retries or benchmark thresholds are changed.

## Ranked bottlenecks and findings at the initial inspection

Costs below use the low-queue run's accounting unless stated otherwise. Classification
is by executed boundaries, not file names. No arbitrary unit/integration/E2E ratio
is used.

1. **Unused image dependency: median 25.96-minute image job on the critical path.**
   Gateway tests construct `GatewayServer` in process, with ephemeral listeners;
   service tests use their own MinIO, PostgreSQL, Kafka or driver images. Searching
   gateway/test-support sources found no candidate-image alias or image-loader
   consumer. `control_plane_ha_tests.rs::TcCluster::boot` does start the production
   image. Cheapest reliable protection: retain gateway's real sockets/services,
   independently retain production build/smoke and HA. Implemented by changing only
   gateway's prerequisite; no additional runner, build or test invocation.
2. **Gateway execution: 41.31 minutes, of which `gateway_proof_tests` is 18.19.**
   This target mixes in-process PGWire/transaction checks with real MinIO replay and
   50,000-row COPY buffering. Those storage boundaries are unique and remain on PRs.
   `proof_orm_schema_reflection_testcontainers` is misleadingly named: it and
   `proof_orm_schema_reflection_queries` call the exact same in-process helper.
   This is confirmed duplication, but its individual cost was not recorded; no
   large saving is attributed to deleting it. Per-case timing is added before
   choosing further changes. Wire encoding, SQLSTATE and recovery-after-error tests
   must retain actual client/server protocol exchanges; pure parser branches can
   use decoder/component tests, with representative wire regressions retained.
3. **Parallel compilation/qualification floor.** In the same run, Verus takes
   57.85 minutes on Linux and 63.27 on macOS; clean qualification takes 33.90/37.38
   and ordinary release consumer builds 20.42/21.48. The performance gate takes
   57.37 minutes, including sequential profile builds. Removing a clean build or
   reusing a differently featured binary would invalidate proof/consumer evidence.
   The current per-shard Rust cache and BuildKit cache remain. A restored gateway
   cache still needs 20.58 minutes of compilation; cache presence is not a warm-build
   guarantee. Cross-profile/platform artifacts cannot simply be shared.
4. **Large-scale correctness checks: candidate for a later bounded/stress split.**
   `view_refresh_proportionality_tests` costs 7.44 minutes. Four families each load
   one million rows; the assertion measures delta-row work, not elapsed performance.
   `unified_data_plane_tests` already covers a smaller representative proportionality
   case, but not this entire family/scale matrix. A smaller all-family component
   test can cheaply catch rescans; the million-row boundary still requires its
   scale run. No scale is reduced here. `ivm_join_arrangement_cache_benchmark` costs
   5.41 minutes and exercises real ShardDb/JoinOp plus local NVMe and a counting
   in-memory store, including persisted arrangement rehydration: it is not just a
   benchmark to discard. `tpch_soak_e2e` costs 3.73 minutes and exercises an
   in-process query/operator oracle, not a production cluster.
5. **Workspace broad oracle exploration and seed expansion.** The oracle library
   costs 11.63 minutes; `harness.rs` includes 10,000/5,000-case property tests.
   `nexmark_retraction` costs 10.48 minutes and uses real gateway/storage paths;
   its 100-seed LFS test already bounds internal concurrency at eight. Increasing
   libtest threads would multiply that work. Small deterministic counterexamples
   are the cheapest protection for known algebra bugs; broad randomized
   exploration and storage replay add different coverage. Both remain on PRs.

Additional findings and rejected changes:

* `cli_inspection_sim_tests` costs 115.5 seconds but starts a real control service
  and polls client routines 50 times. Its explicit 2ms jitter cannot alone explain
  the cost. Do not infer a sleep bottleneck from the file's simulation label.
* Gateway cancellation probes use a 50ms wait before cancellation and a slow reader.
  An explicit reader-start notification would improve determinism, but milliseconds
  here do not explain the measured hour-long path. No unrelated timing rewrite.
* Gateway metrics reset, COPY buffer counters, and view-refresh global counters
  prevent blanket intra-process concurrency. The proportionality suite uses a
  shared mutex. TempDirs and random ports alone do not establish isolation.
  Containers use per-test mappings/stores; reusing a database would risk state
  leakage between durability/recovery cases. Serial conformance producers must
  finish before `conformance_doc_tests` consumes the one freshly generated report.
* Shards are unbalanced (workspace 48.98, crates 59.50, sim-unit 6.20,
  sim-integration 18.05 minutes), but reassigning gateway targets would duplicate
  expensive feature-specific builds and complicate conformance ownership. Remove
  the unused dependency first; use the new case timings for the next decision.
* Existing nightly/release ownership and invocation in
  `scripts/ci-test-matrix.json` / `test-accounting-expensive.yml` already cover broad
  seeds, real-cluster soaks and rolling upgrades. No new deferrals are introduced.
  Selection still recognizes only allowlisted prose; shared config, unknown paths
  and source changes run full CI, so no transitive dependency selector is needed.

## Initial dependency-only replay and validation

For each successful run, let P=image-independent longest parallel job, D=Docker,
G=gateway, H=HA, S=selection and C=Check, using active job durations. The queue-free
replay is `S + max(D+G, D+H, P) + C` before and
`S + max(G, D+H, P) + C` after. This uses the existing runners, measured workload
and observed build/cache costs; it assumes earlier scheduling does not change job
durations. It is not a prediction of hosted queue delays.

| Metric | Before | After |
| --- | ---: | ---: |
| Observed successful PR median, queues included | 130.67 min, n=8 | Not measured |
| Queue-free critical-path replay median | 95.92 min | 69.70 min, estimated |
| Low-queue run replay | 89.18 min | 64.25 min, estimated |
| Occupied runner-minute median | 440.61 | Approximately unchanged, estimated |

No cost saving is credited for removed image download/load/verification (seconds,
not minutes), nor for new timing instrumentation overhead. Earlier gateway starts
can contend for runners with other jobs; only hosted runs can establish net gains.
The 50% target would require additional measured reductions in gateway and the
parallel qualification floor, not merely excluding meaningful checks.

Local validation: 6 policy tests, 16 inventory tests and 8 scope/aggregate tests
pass; live Cargo metadata assigns all 604 targets including 69 simulation targets.
Exact-output inventory checks cover timed success/failure/ignored/missing events,
nonzero harness exit, raw report reuse, and doc-gate ordering. A real Rust 1.88
libtest harness verifies `--report-time` JSON for pass/fail/ignored cases and the
accounting results. The unchanged full/prose aggregate tests reject failed,
cancelled, missing and unexpected skipped dependencies. No branch protection or
required-check name changes. Workflow YAML/dependency validation, the documentation
gate, production CLI fixture-isolation gate, and `git diff --check` also pass.

Those checks describe the first commit. The follow-up below changes test scheduling
and introduces the explicitly described scale split. Full Rust, Docker and hosted
suites have not been run locally. After publication, compare candidate job/step
timings and exact accounting case IDs/outcomes against a base run on matching
runners/cache conditions; publish an observed reduction only after those runs complete.

## Follow-up toward a 30-minute target

The initial hosted run [39826874980](https://github.com/trickle-labs/rockstream/actions/runs/39826874980)
was still queued when follow-up work began. Queue latency remains outside these
execution changes; a 30-minute execution path alone does not guarantee 30 minutes
from a PR update.

### Changes and coverage

* Split the former crate owner into connectors/control/runtime, ops, and storage/SQL;
  move oracle to its own PR owner. Split gateway into conformance producers plus
  the documentation consumer, and the remaining targets. Unknown new targets still
  inherit the package's PR owner. All explicit simulation assignment rules remain.
  Every target runs serially within its job; separate runners isolate global
  counters, environment variables, files, ports and Docker services. Conformance
  report ordering is unchanged. No harness transfer or cross-feature binary reuse.
* Move only `view_refresh_proportionality_tests` to `nightly-seeds`, which already
  runs daily, on dispatch and on release tags. Preserve all four original case IDs,
  one million rows, chunk sizes and assertions. Add a PR target calling the same
  functions at 10,000 rows in ten commits. This still exercises every family through
  real gateway sockets, commits, local storage and compiled views. Full rescans
  exceed the same delta-work threshold of 100 at either scale. Scale-dependent
  failures beyond 10,000 rows may now be detected by the next daily run instead of
  the PR. Real MinIO durability, COPY buffering, recovery and transaction cases stay
  on PRs. No new ignore or waiver is added.
* Run ops/storage/runtime/control benchmark packages on independent runners. Keep
  their exact baselines, tags, comparator and existing retry limit. A shell pipeline
  failure is fatal. The original named performance gate validates the matrix result
  and classification, rejecting missing, failed, cancelled or unexpectedly skipped
  results. Prose skips are explicit exclusions, not benchmark passes.
* Move the ordinary CLI-only release builds to independent Linux/macOS jobs and add
  their matrix to `Check`. Do not delete those builds: a local Cargo 1.88 feature-tree
  comparison found workspace-only `url/serde` and `either/default` features, so a
  successful workspace build is not equivalent evidence. Keep the clean Verus
  matrix commands, verifier checks, production-wrapper test and source-bound evidence.
  Smoke-test both debug/release workspace artifacts produced by qualification.
* Split clean qualification into debug/verification and release phases on separate
  runners for both platforms. The original serial runner remains the default. Both
  phases retain fresh build directories, commands and budgets; partial phase records
  cannot become full qualification without successful matrix results and validation
  of source revision, target, source digests, all six step IDs/statuses/budgets, and
  both binary hashes. The original named Verus jobs assemble and smoke-test the
  complete result. Missing, corrupted or mixed-revision inputs fail closed.

### Timing and cost implications

These are the original run 39726733128 execution times reallocated by the new
ownership, excluding compilation, setup and queues. They are useful for balancing,
not predictions of total job time:

| New owner | Existing execution minutes |
| --- | ---: |
| Workspace excluding oracle | 2.18 |
| Oracle | 23.24 |
| Connectors/control/runtime | 9.67 |
| Ops | 13.31 |
| Storage/SQL | 8.69 |
| Gateway conformance | 20.79 |
| Gateway components excluding million-row scale | 13.07, plus the new small suite |

The macOS Verus artifact's `steps.tsv` separates cold verification (80 seconds),
incremental verification (5), debug build (364), default check (219), all-feature
check (33), and release build (1,536). That is **11.68 minutes debug/verification +
25.60 release**. The original job also spent 21.48 minutes building the CLI-only
consumer. Overlapping all three paths removes their serial sum; the measured
longest constituent is 25.60 minutes, before setup, artifact transfer, assembly,
wrapper tests and smoke tests. Cold release may lose build-script reuse from the
debug phase, so 25.60 is a reference cost, not a promised phase duration.

This supports pursuing an approximately **30–40-minute execution path**, conditional
on acceptable shard compilation and phase overhead. It does not establish a
30-minute hosted result. Four-package benchmark execution/builds can overlap, but
their separate runners may recompile shared dependencies. Shard compilation costs
cannot be inferred by splitting the old package times: earlier packages warmed
dependencies for later ones.

Thus neither 30-minute time-to-green nor a runner-minute saving is claimed. The
first baseline remains 130.67 minutes median including queues and 440.61 occupied
runner-minutes. More parallel jobs may increase cost and queue pressure. Nightly
adds the retained million-row execution and its compilation; PRs save that execution
but still run all-family proportionality coverage at the smaller scale. A new hosted
run is needed to quantify the final critical path, build duplication and total cost.

### Follow-up validation

The policy, inventory, scope, benchmark and qualification-assembly tests cover exact shard ownership,
unchanged conformance ordering/ownership, nightly schedule and report invocation,
both scale configurations, all four benchmark command sequences and baseline paths,
comparator retry exhaustion, immediate build failure, and fail-closed matrix
aggregation. Qualification tests exercise the actual runner with command shims,
compare every phase command against the serial qualification contract, and reject
missing phases/steps/binaries, failed statuses, mixed revisions/targets/digests,
duplicate steps, exceeded budgets and altered binary content. Live Cargo metadata accounts for 605 targets (one new PR target),
including the same 69 simulation integration targets. All 39 Python self-tests and
the broader `make verify-proof-contracts` checks pass, including atomic staging and
actual phase-runner command checks. Workflow YAML/matrix validation, documentation,
shell syntax, Rust formatting and diff whitespace checks pass.

### Local scale comparison (measured, not hosted time-to-green)

One sequential pair on the same macOS ARM64 host, Rust/Cargo 1.99.1, debug test
profile, `testcontainers` feature, one test thread, and shared compiled dependencies:

| Workload | Cases and outcomes | Harness wall time |
| --- | --- | ---: |
| Retained million-row suite | Exact four original case IDs passed | 305.45 s |
| New 10,000-row PR suite | Exact four new case IDs passed | 20.39 s |

This is **93.3% less execution time for this suite**, achieved by moving scale
coverage rather than claiming faster production behavior. Both binaries were built
together before timing; the warmed no-run build took 0.66 seconds. The same libtest
JSON parser/accounting function verified every case and rejected missing/ignored
outcomes. The shared functions retain every original assertion. Compilation of the
new feature-specific binaries beforehand took 2m56s and is excluded from these
execution numbers. This single pair does not support p95 estimates or predictions
for Ubuntu CI or the whole workflow. CI uses Rust 1.88; the local execution here
used the installed Homebrew Rust 1.99.1. Rust 1.88 formatting and the earlier libtest
timing compatibility check also pass.

The first local attempt failed only because sandbox loopback binds are prohibited;
the host-authorized rerun and this complete comparison passed. Full workspace,
Docker and real Verus compilation were not rerun locally. The hosted workflow is
still needed to measure the final critical path and increased parallel runner cost.
