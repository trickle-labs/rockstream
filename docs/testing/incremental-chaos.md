# Deterministic incremental recovery scenario (#125)

`chaos_tests::proof_chaos_output_matches_reference_run` replaces the former
epoch-count comparison with production `SourceRuntimeCoordinator::poll_delta`
and `commit_epoch`, `AggregateOp`, `ViewSinkOp`, SlateDB M3 durability, and
checkpoint/operator restoration after reopening storage. A fixed six-operation
history includes inserts, retractions, and a fractional average. Independent
batch arithmetic feeds the typed signed-bag comparator introduced for #111.
Every committed frontier and the restored frontier compare full output, source
epoch/token, and persisted frontier; upstream acknowledgements are checked too.

The workload RNG and fault RNG are separate. The fault schedule chooses a
pre-ack restart frontier and source acknowledgement outage. A `SimClock` drives
100 ms retries against the production acknowledgement method; elapsed recovery
is the difference between the observed failed commit and successful recovered
acknowledgement timestamps. It includes the simulated outage, not storage wall
time. This is a deterministic source-outage model, not a measured cluster SLO.

The four negative controls corrupt one persisted value without changing row
count, add one row's multiplicity, silently suppress the recovered source
acknowledgement, and keep the source unavailable until 60,100 ms. Each asserts
the complete expected failure. Failures save the shortest input prefix that
reproduces the exact failure, retaining original offsets and the fault schedule.
This minimizes prefixes, not arbitrary subsequences.

Run the regression and negative controls:

```sh
cargo test -p rockstream-sim --features simulation --test chaos_tests
```

The scheduled `simulation-soak.yml` additionally runs 32 seeds on each of two
independent axes (64 workload/fault pairs), bounded to 128 per axis in the
runner and 30 minutes in the job. The normal test default is two per axis.
`ROCKSTREAM_INCREMENTAL_CHAOS_SEEDS` controls this bound. JSON artifacts in
`target/incremental-chaos/` (or `ROCKSTREAM_INCREMENTAL_CHAOS_ARTIFACT_DIR`) record
both seeds, full reference/faulted polled histories, actual production
transitions, typed frontier observations, exact failure, and minimized failing
history. The workflow uploads them even on failure.

Coverage is one in-process source adapter, Int64 SUM/COUNT/AVG, one shard, and a
durable commit before acknowledgement followed by a runtime/storage reopen.
The source journal and object store survive restart. This does not qualify
Kafka, process-kill durability, distributed reassignment, other operators, or
the legacy manufactured timing/data-loss scenarios still present in this suite.
