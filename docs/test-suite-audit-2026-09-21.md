# Rockstream test suite audit

Audit date: 2026-09-21. Source revision: `5d9c3b919682140ec0b365d7dba848cd5cf5ecbb`, version `0.68.0`.

Rockstream has a large test suite, but several important tests do not observe the behavior their names claim. Some manufacture successful recovery observations, some exercise a different operator, and many simulation targets are absent from CI. The largest quality improvement will come from repairing those checks and adding exact failure-boundary regressions.

This audit also reproduced two catalog bugs using the actual implementation:

- After snapshot and compaction, retrying an old create operation resurrects a deleted table and regresses the catalog revision from 2 to 1.
- Workload IDs change after both log-only recovery and snapshot recovery.

The three failing regression tests are preserved in [catalog_recovery_regressions.rs](../evidence/test-suite-audit/catalog_recovery_regressions.rs). They assert the intended behavior and are outside Cargo's automatic test discovery so this report does not leave the normal suite intentionally failing. Production code has not been changed.

## Scope, limits, and current strengths

The audit compared test bodies with production implementations, callers, feature gates, workflow commands, failure injection, and expected results. It covered storage, control, operators, runtime exchange, SQL, gateway, connectors, oracle infrastructure, simulation, fuzzing, CLI process tests, and qualification scripts. Individual tests were sampled in depth. This was not a line-by-line review of every test.

Three evidence levels matter:

- **Executed:** a command or regression run during this audit demonstrated the finding.
- **Source-confirmed gap:** a test or workflow demonstrably does not exercise the claimed behavior.
- **Defect candidate:** production code suggests a specific failure, but the proposed regression has not been run. Do not treat these as reproduced production incidents.

The full workspace, Docker suites, long-duration tests, fuzz engines, coverage instrumentation, and formal verification were not run. Current remote CI results and branch-protection settings were not inspected. This report evaluates the checked-in implementation and test design at the revision above.

The inventory found 3,932 ordinary Rust test attributes, 23 `proptest!` blocks, and 542 Cargo integration-test targets across 17 crates. Attribute and macro counts are static occurrences, not executed-case totals. Properties, doctests, feature gates, conditional ignores, and helper files make runtime counts different.

| Crate | Ordinary test attributes | Proptest blocks |
| --- | ---: | ---: |
| gateway | 764 | 1 |
| ops | 492 | 0 |
| cli | 417 | 0 |
| types | 393 | 1 |
| storage | 322 | 0 |
| control | 318 | 1 |
| sim | 310 | 0 |
| runtime | 248 | 0 |
| connectors | 210 | 2 |
| sql | 196 | 0 |
| oracle | 177 | 18 |
| docgen | 27 | 0 |
| plan | 27 | 0 |
| diff | 12 | 0 |
| test-support | 9 | 0 |
| verified | 6 | 0 |
| management-proto | 4 | 0 |

Retain and extend the parts that already provide useful evidence:

- [Aggregate tests](../crates/rockstream-ops/tests/delta_native_aggregate_tests.rs) include randomized oracle comparisons and arithmetic failures, around lines 884 and 929. Missing operator cases should follow that pattern.
- LFS and MinIO suites exercise real storage. [Migration state-machine tests](../crates/rockstream-control/tests/shard_migration_state_machine_tests.rs), around line 293, already inject copy-intent and progress-persistence failures.
- [Checkpoint loading](../crates/rockstream-control/src/checkpoint_store.rs), around line 462, tests rejection of a corrupt latest manifest. Apply the same standard to catalog and Raft loading.
- [Management backup process tests](../crates/rockstream-cli/tests/management_backup_process_tests.rs) exercise actual processes, restore, restart idempotency, and concurrent checkpoint allocation. Reuse this infrastructure for new integration cases.
- [Resource leak tests](../crates/rockstream-sim/tests/resource_leak_soak_real_binary_tests.rs) include an injected teardown leak that must fail the gate. Preserve that negative control.
- Formal kernels and protocol models provide useful bounded guarantees. [ADR 0006](adr/0006-persistence-replay-compaction.md) explicitly excludes storage internals, cross-process fencing, and distributed liveness from the proof claim. Adapter and process tests remain necessary.

## Repair the evidence before adding more test volume

### A1. Schedule the tests that already exist

Priority: P1. Source-confirmed execution gap.

Cargo discovers 67 integration targets in `rockstream-sim`. The main CI workflow explicitly selects 17, including the separate HA and Docker invocations. Across all seven workflows, 18 distinct targets are selected. The workspace invocation excludes `rockstream-sim`; the other broad simulation invocation uses `--lib`. Coverage repeats the narrow selection. This leaves **49 targets without a workflow invocation**.

Evidence: [ci.yml](../.github/workflows/ci.yml), lines 220 and 239 to 245; [coverage.yml](../.github/workflows/coverage.yml), lines 56 to 69; [simulation-soak.yml](../.github/workflows/simulation-soak.yml), line 36. The Verus qualification commands do not fill this simulation-target gap.

Omitted targets include crash boundaries, CDC transactions, dead-letter recovery, concurrent maintenance, spill coordination, internal mTLS simulation, and the SQL fuzz simulation target. The complete list and reproduction command are at the end of this report.

Use Cargo metadata to assign every test target to a PR, nightly, or release job. Make the assignment check reject newly added targets with no job. Prefer discovery for ordinary tests and a short explicit list for expensive ones. Do not put every 100,000-seed and Docker case on every edit.

Account separately for ignored cases. The [real-worker aggregate/join/failover test](../crates/rockstream-cli/tests/r1_real_worker_data_plane_tests.rs#L644) is ignored because direct gateway-to-worker source transport is missing. Three MinIO cases in [lfs_catalog.rs](../crates/rockstream-sql/tests/lfs_catalog.rs), at lines 152, 757, and 1173, are ignored too. Resolve the prerequisite or narrow the supported claim. Running a test binary does not execute its ignored cases.

Add gate self-tests for an unassigned target, empty selection, missing features, and an ignored mandatory case. Require exact executed test IDs and explicit skip reasons in the result manifest.

### A2. Replace manufactured chaos outcomes with observed results

Priority: P1. Source-confirmed gap. The existing 15-test `chaos_tests` target passed during this audit.

The scheduled [100,000-seed test](../crates/rockstream-sim/tests/chaos_tests.rs#L87) writes a value into a simulated store, sends an empty message, advances time, and returns `SeedOutcome::Pass`. It does not read the value back or check delivery, query results, recovery, or a production operator.

In [chaos.rs](../crates/rockstream-sim/src/chaos.rs#L129), `data_loss_events` is permanently zero and every staged epoch is committed by the model's loop. Recovery durations are generated from ranges guaranteed to satisfy the limits, around line 192. `output_matches`, at line 109, compares only epoch counts. Fault decisions consume the same random stream as workload generation, so faulted and reference runs do not receive identical row values.

The Docker chaos suite has a similar problem despite starting real containers. At [real_cluster_chaos_soak_tests.rs:364](../crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs#L364), `committed` is copied directly from `submitted`. At line 421, expected results are built from `submitted` again. Failure detection and reassignment measure `docker kill` and `docker start` command duration. The brownout buffer is the literal `5`, and the sink staging assertion counts a local set. Broker offset checks establish broker behavior, not Rockstream ingestion or recovered view correctness.

Replace one scenario first. Pre-generate an input history, ingest it through Rockstream, interrupt a worker or storage operation, resume, and query the actual view and sink. Compare complete typed weighted results and source offsets with an independent batch evaluation. In simulation, separate workload randomness from fault randomness.

Measure recovery from injection to actual failure detection, new lease ownership, and the first correct query at the required frontier. Keep command duration as a different metric. One observation does not establish a meaningful recovery p99.

Add negative controls that change one output value without changing count, duplicate a row, suppress an acknowledgement, and exceed the recovery deadline. Each must fail the relevant test.

### A3. Make qualification and upgrades test the system they name

Priority: P1. Source-confirmed gaps and executed script probes.

The Kafka and CDC cases in [e2e_qualification_tests.rs](../crates/rockstream-sim/tests/e2e_qualification_tests.rs#L61) assign `live_state = expected.clone()`. [Recovery qualification](../crates/rockstream-sim/tests/qualification_recovery_tests.rs#L21) supplies invented heartbeat and reassignment durations to an observer. Its recovered-query case also copies the oracle's state. [QualificationCluster::kill_node](../crates/rockstream-sim/src/qualification/orchestrator.rs#L280) changes entries in a map. These test helper bookkeeping, not distributed recovery.

The [rolling-upgrade test](../crates/rockstream-sim/tests/rolling_upgrade_tests.rs#L31) starts images with `--version`, defaults both versions to the same image, and compares a constant epoch vector with an identical constant. It cannot detect lost writes during an upgrade.

Two executed probes establish defects in supporting tooling:

- [check-qualification-mutations.sh](../scripts/check-qualification-mutations.sh) searches for function names. A fixture containing only comments with those names passed and reported the observations and mutations as verified.
- [run-release-qualification.sh](../scripts/run-release-qualification.sh), lines 78 to 105, accepts an unknown suite, runs neither test target, and writes a fixed report of eight passed scenarios. In an isolated fixture with build and Docker commands stubbed, `--suite nonexistent-audit-suite --fast` returned exit 0 and that report. This demonstrates runner control flow, not actual release qualification.

Both checker self-test scripts passed too. Their mutations protect textual presence, not execution or semantic sensitivity.

Keep helper tests under accurate names. Generate qualification reports from executed results and actual observations. Reject unknown suites, zero mandatory execution, absent observations, test-process failures, and unavailable required services. List exact scenario IDs and outcomes rather than a fixed count.

For upgrades, use distinct pinned version digests and a real cluster under insert/update/delete traffic. Replace one process at a time. Compare the full final relation, acknowledged operation effects, schema compatibility, ownership history, and post-upgrade writes. Test rollback or downgrade only where the product promises support.

### A4. Fix the oracle and prove the intended operator runs

Priority: P1. Source-confirmed gaps.

[sql_fuzzer.rs:992](../crates/rockstream-oracle/src/sql_fuzzer.rs#L992) returns successfully on any compilation error. A regression rejecting supported SQL can pass without comparison. The claimed 900 comparisons in the 300-seed test are not guaranteed.

The reference comparison casts output to Int64 and reads values without validity checks at [line 916](../crates/rockstream-oracle/src/sql_fuzzer.rs#L916). Incremental accumulation ignores validity too. Generated inputs are non-null Int64. This does not establish NULL, decimal, string, or output-type correctness.

Some matrix names also overstate their execution:

| Test location | What it actually exercises |
| --- | --- |
| [delta_native_join_tests.rs](../crates/rockstream-ops/tests/delta_native_join_tests.rs), lines 90 and 117 | Inner `JoinOp` in cases named left and right joins |
| [delta_native_distinct_tests.rs](../crates/rockstream-ops/tests/delta_native_distinct_tests.rs), lines 78 and 104 | `DistinctOp` under TopK and arrangement names |
| [delta_native_window_tests.rs](../crates/rockstream-ops/tests/delta_native_window_tests.rs), line 31 | Input batch construction and row count |
| [operator_oracle_qualification_tests.rs](../crates/rockstream-runtime/tests/operator_oracle_qualification_tests.rs) | Handwritten maps and arithmetic, without production operators |
| [pg18_differential_conformance_tests.rs](../crates/rockstream-gateway/tests/pg18_differential_conformance_tests.rs), lines 43 and 92 | Rockstream alone; numeric precision case checks non-emptiness |

Fail supported queries on compilation errors. Explicitly list unsupported cases with exact expected errors. Record seed, SQL, epoch, production operator, and comparison outcome. Compare schema and typed nullable rows with signed multiplicity. Normalize only semantically irrelevant order.

Test the oracle by replacing NULL with zero, changing a decimal fraction, dropping one duplicate, changing a type, and substituting values while preserving count and sum. Each mutation must fail. Reuse DataFusion where its semantics match the contract and a pinned PostgreSQL instance for claimed PostgreSQL compatibility. Expected results must not come from the incremental implementation itself.

### A5. Run a fuzz engine, not only corpus replay

Priority: P2 after A4. Source-confirmed workflow gap.

[fuzz-soak.yml](../.github/workflows/fuzz-soak.yml) runs fixed corpus replay, panic-safety tests, and `cargo check --package rockstream-fuzz`. It never invokes a fuzz engine. The seven targets in [fuzz/Cargo.toml](../fuzz/Cargo.toml) cover SQL, PGWire, PostgreSQL CDC, control-worker messages, Raft messages, Kafka payloads, and OIDC JWT decoding.

Add bounded scheduled fuzz-engine runs for the existing targets. Save minimized failures and corpus additions with toolchain, seed, duration, timeout, and memory limit. Keep regression replay in PR CI. After fixing the known examples below, expand to stateful Parse/Bind/Execute/Sync, CDC transaction/reconnect, and checkpoint/replay/compaction sequences. Start with valid messages so mutations reach state transitions. Check bounded allocation and termination as well as panic safety.

## Specific regressions to add

P1 means address the case before relying on its related safety claim. P2 follows the initial repairs. These are remediation priorities, not verified exploit severities. Production failures remain defect candidates unless explicitly marked executed.

### B1. Catalog retries after snapshot and compaction

Priority: P1. **Executed failure.** Extend [catalog_transaction_tests.rs](../crates/rockstream-storage/tests/catalog_transaction_tests.rs).

[commit_txn](../crates/rockstream-storage/src/catalog/store.rs#L538) deduplicates operation IDs, but [CatalogSnapshot](../crates/rockstream-storage/src/catalog/snapshot.rs#L21) omits `committed_operations`. Recovery rebuilds that set only from later logs. The existing deduplication test retries before recovery and does not cross compaction.

The audit created T with operation 555 at revision 1, deleted T with operation 556 at revision 2, snapshotted, compacted, recovered, then retried operation 555. Before retry, the catalog was correctly empty at revision 2. After retry:

| Observation | Required | Actual |
| --- | --- | --- |
| Returned revision | 2 | 1 |
| Current revision | 2 | 1 |
| Complete table list | Empty | Original deleted table T |

Make this a permanent regression. Compare full metadata and exact log objects. Repeat recovery and retry; also retry an old ALTER after a newer definition, which must remain unchanged.

### B2. Workload identity with allocation gaps

Priority: P1 regression, narrower impact than B1. **Executed failures in two recovery modes.** Extend [catalog_identity_tests.rs](../crates/rockstream-storage/tests/catalog_identity_tests.rs).

[PutWorkload](../crates/rockstream-storage/src/catalog/store.rs#L493) allocates implicitly. Snapshot recovery assigns IDs by enumeration at line 325. The current identity test separately allocates an ID, never checks the live mapping, and queries only after recovery. Replay makes its originally incorrect ID appear valid.

Consume ID 1, create the workload, and establish the exact live mapping `{1:None, 2:Some(full_definition)}`. Both log-only and snapshot recovery instead produced `{1:Some(full_definition), 2:None}` in the audit probes.

Preserve the full mapping across both recovery modes. Then create several workloads with allocation gaps, delete one, and replay deletion by its original ID. Compare every lookup and full definition. Name-only lookup misses the defect. Gateway workload lookup currently uses names, so this does not establish failure of every workload path.

### B3. Reject incomplete catalog recovery

Priority: P1. Extend [catalog_storage_tests.rs](../crates/rockstream-storage/tests/catalog_storage_tests.rs).

[Recovery](../crates/rockstream-storage/src/catalog/store.rs#L255) ignores snapshot-list errors, snapshot read/decode failures, and log-list errors. The corrupt-snapshot test around line 152 retains all logs and therefore tests a recoverable case.

Persist tables, views, indexes, and dependencies; snapshot and compact. Independently fail listing, snapshot GET, body reading, decoding, and log listing after one successful item. Require an explicit recovery error and no published catalog. Compare stored objects before and after to prove evidence was preserved. Keep safe fallback tests when complete earlier history exists, comparing full recovered records rather than counts.

### B4. Preserve Raft authority when storage is unreadable

Priority: P1. Extend [raft_state_durability_tests.rs](../crates/rockstream-control/tests/raft_state_durability_tests.rs).

[RaftPersistentStore::load](../crates/rockstream-control/src/raft.rs#L96) defaults term and vote for malformed JSON, body-read failure, and every GET error. Current round trips and truly-missing-state tests do not distinguish absence from unavailable data.

Persist term 7 and vote A. Inject corruption, permission failure, transient GET failure, and body-read failure separately. Require startup failure with the defined storage/corruption error, no granted vote, and byte-identical stored state. A truly absent object must still allow bootstrap.

### B5. Force out-of-order Raft persistence

Priority: P1. Extend [raft.rs](../crates/rockstream-control/src/raft.rs#L467) tests through actual RPC handlers.

Vote handlers release the state lock before saving, and the store uses unconditional PUT. Hold the term-7 PUT, complete a term-8 vote for B, then release term 7 and restart. Require exact recovered state `{term:8,vote:B}` and exact rejection of another candidate's term-8 request. Repeat across heartbeat and election writers. Successful sequential persistence tests cannot detect this schedule.

### B6. Complete changelog checkpoints concurrently

Priority: P1. Extend [checkpoint.rs](../crates/rockstream-control/src/checkpoint.rs#L330) tests.

`record_shard_changelog_checkpoint` clones contributions under one lock, then completes the shard under another. Pause A after it clones `{A}`. Let B insert `{A,B}` and confirm B. Resume A as the final confirmation. Its callback currently checks the stale clone against the complete manifest.

Require no panic, exactly one successful callback with both full contributions and the complete manifest, the correct `latest_committed`, and zero held credits. Existing confirmations are serial. Use deterministic barriers at the boundary; sleeps are not a reliable concurrency regression.

### B7. Resume or explicitly block migration after restart

Priority: P1. Extend [migration_record_durability_tests.rs](../crates/rockstream-control/tests/migration_record_durability_tests.rs) with an actual service restart.

[Service startup](../crates/rockstream-control/src/service.rs#L450) loads active migration records but discards the returned records. Current tests load structures and manually advance them.

Restart at Copying, FencingOld, Verifying, and GcEligible. Also interrupt after partial donor deletion, Done persistence, and history persistence before active-record deletion. Require exact donor and recipient key-value maps, authoritative leases, full history, and active-record state. Unrelated buckets must remain byte-identical and a second restart must be idempotent. If automatic resumption is unsupported, require an explicit blocked status and retained data.

### B8. Kill or isolate a real leader while workers write

Priority: P1. Extend [lease_ha_leader_kill_tests.rs](../crates/rockstream-control/tests/lease_ha_leader_kill_tests.rs).

The case around line 42 starts a peerless bootstrap node, calls orderly shutdown, and creates another bootstrap node. A second case calls `force_step_down_for_test`. These protect adoption and role guards, not loss-of-authority detection. Healthy three-node election tests already exist.

Run three control processes, acquire leases, and acknowledge worker writes. Kill the elected leader and require replacement without new bootstrap flags. Separately isolate the old leader while it remains alive. Attempt old-token and new-token writes across the transition. Compare the exact accepted history and recovered rows; no stale write may become durable after new ownership.

### B9. Kill immediately after durable acknowledgement

Priority: P2. Extend [aggregate_crash_recovery_tests.rs](../crates/rockstream-cli/tests/aggregate_crash_recovery_tests.rs).

An abrupt-process test already exists. It waits 200 ms for storage flush after checking the view, around line 146, and recreates table/view definitions after restart, around line 182. Other storage tests commonly flush and close cleanly.

Use a child-process barrier immediately after the successful durable commit response and kill without a grace interval. Reopen with a fresh process. Compare complete base rows, aggregate rows, catalog definitions, frontier, and idempotency metadata. Include UPDATE and DELETE. On injected flush failure, require no successful acknowledgement. Do not require unacknowledged writes to be absent if the documented outcome is unknown.

### C1. Preserve NULL validity in outer joins

Priority: P1. Extend [lfs_outer_join.rs](../crates/rockstream-ops/tests/lfs_outer_join.rs) and the operator matrix.

[OuterJoinOp](../crates/rockstream-ops/src/outer_join.rs#L272) serializes Int64 values without validity. The NULL-key suffix at lines 350 and 369 uses the same row identity on both sides. Identical NULL-key rows can receive identical supposedly nonmatching keys. Existing nullable-key coverage exercises inner joins. The LFS outer-join helper treats zero as NULL.

Use identical `(NULL,7)` rows on both sides and require these complete weighted bags:

| Operator | Expected bag |
| --- | --- |
| LEFT | `{(NULL,7,NULL,NULL):1}` |
| FULL | `{(NULL,7,NULL,NULL):1, (NULL,NULL,NULL,7):1}` |
| SEMI | Empty |
| ANTI | `{(NULL,7):1}` |

Retract the right input and compare complete deltas. Also join `(1,NULL)` with `(1,0)` and preserve the NULL payload. Check validity bits, schema, values, and weights before and after reopen.

### C2. Cross the TopK spill threshold

Priority: P1. Extend [arrangement_spill_correctness_tests.rs](../crates/rockstream-ops/tests/arrangement_spill_correctness_tests.rs) and [arrangement_spill_recovery_tests.rs](../crates/rockstream-ops/tests/arrangement_spill_recovery_tests.rs).

The threshold is 100,000 in [topk.rs:38](../crates/rockstream-ops/src/topk.rs#L38). The named spill test uses 50 rows across two partitions and only checks success. The named LFS-and-MinIO recovery test uses two rows, LFS only, and never reopens. Genuine ordinary LFS recovery exists separately.

With descending K=2, fill one partition with ranks 1 through 100,000, then insert rank 100,001 twice. Require final bag `{100001:2}` and second-insert delta `{100001:+1,100000:-1}`. Delete one copy, requiring `{100001:1,100000:1}`; delete the other, requiring `{100000:1,99999:1}`. Reopen between transitions and compare uninterrupted execution.

Run another operator in the same DB and partition containing only rank -1. Its K=1 result must remain `{-1:1}`. Spill keys around line 217 omit operator identity, so this needs an explicit isolation test.

Inject spill scan failure and truncated persisted entries. Scanning uses `unwrap_or_default` around line 363; restore skips malformed entries around line 544. Require errors and no installed partial state. Keep one actual-threshold integration test. A private smaller threshold can make repeated state-machine cases cheaper if needed.

### C3. Preserve window multiplicity and order independence

Priority: P1. Extend [time_window.rs](../crates/rockstream-ops/src/time_window.rs) tests.

Tumble processing tracks input weights around line 557, builds emitted state from presence at line 586, and emits unit weights at lines 609 and 619. Existing helpers also discard positive multiplicity.

For size 10 and row `(timestamp=1,value=7)`, apply `+2,-1,-1` over three epochs. Require deltas `{(0,1,7):+2}`, `{(0,1,7):-1}`, `{(0,1,7):-1}` and accumulated weights 2,1,0. Downstream grouped COUNT/SUM must be `(2,14)`, `(1,7)`, then absent. Repeat across reopen.

The late-data logic around line 506 reopens a finalized window only after seeing a negative row. Finalize window 0, then submit `{(1,10):-1,(2,20):+1}` under Drop in both orders. Require the same accepted correction and exact delta. If corrections intentionally require ordering, enforce that contract at the producer boundary instead of assuming arbitrary Z-set row order is meaningful.

### C4. Test resource limits through the production sender

Priority: P1. Extend [flow_control_saturation_tests.rs](../crates/rockstream-runtime/tests/flow_control_saturation_tests.rs) and exchange integration tests.

The saturation tests call batch-permit APIs enforcing rows, bytes, batch count, and pending requests. Production [send_frame](../crates/rockstream-runtime/src/exchange/multiplexer.rs#L220) calls row-credit acquisition instead.

Use real `send_frame` with delayed acknowledgements. Independently saturate byte, batch, and pending limits while row credits remain. Require precisely one accepted frame until release, then exactly the two original weighted batches at the receiver. Compare all accounting fields and require zero residual usage. Oversized frames must produce the contractual error with no delivery or accounting change.

### C5. Cancel sends and force the credit-notification race

Priority: P1 for cancellation, P2 for the additional wakeup schedule. Same production entry point as C4.

`send_frame` releases credit after an awaited send returns an error. Cancellation before that return is a different path. The existing error-path test drops a standalone permit that production does not hold.

Pause after reservation but before delivery, abort the task, and require zero delivered rows, no pending request, and the original full credit budget. The next full-budget frame must complete exactly once. Define and separately test cancellation after delivery but before acknowledgement.

Also release capacity between a failed check and waiter registration in [flow_control.rs:522](../crates/rockstream-runtime/src/exchange/flow_control.rs#L522). Release uses `notify_waiters`, while acquire creates its waiter later. Acquisition must finish without another release. Repeat with two waiters, requiring one completion per returned unit. Existing sleeps avoid the interesting interleaving.

### D1. Test prepared parameters in SQL lexical contexts

Priority: P1. Extend [gateway_extended_query_tests.rs](../crates/rockstream-gateway/tests/gateway_extended_query_tests.rs).

[substitute_params_typed](../crates/rockstream-gateway/src/server.rs#L17962) globally replaces text and strips cast names. Add exact typed results:

| Query and parameters | Expected row |
| --- | --- |
| `SELECT '$1' AS literal, $1 AS actual`, parameter `x` | `('$1','x')` |
| `SELECT $1 AS a, $2 AS b`, parameters `x`, `literal $1` | `('x','literal $1')` |

Include comments, dollar-quoted strings, `$1` beside `$10`, repeated parameters, NULL parameters, and precision-qualified casts. Compare schema and values. Parameter text must not become a second substitution instruction. Compare equivalent literal SQL through simple query too.

### D2. Require controlled errors for malformed Bind data

Priority: P1. Extend [untrusted_input_panic_safety_tests.rs](../crates/rockstream-gateway/tests/untrusted_input_panic_safety_tests.rs) and [binary_format_round_trip_tests.rs](../crates/rockstream-gateway/tests/binary_format_round_trip_tests.rs).

[decode_param_bytes](../crates/rockstream-gateway/src/server.rs#L18022) accepts a 12-byte array header before reading bytes 12 through 15. Other branches stop at truncated elements or replace invalid numeric widths with defaults. Valid round trips do not cover this. The malformed-Parse test also discards a read result, potentially masking connection-task failure.

Exercise real Parse/Bind/Execute with valid setup, truncated headers, short payloads, invalid element widths, impossible dimensions, and inconsistent lengths. Require the complete contractual error transcript and SQLSTATE, no partial mutation, correct state after Sync, and a subsequent successful query where recovery is supported. Capture server task panics. A disconnect alone must not count as graceful rejection.

### D3. DML comparisons must respect declared types

Priority: P1. Extend [dml_predicate_eval_tests.rs](../crates/rockstream-sql/tests/dml_predicate_eval_tests.rs) and gateway DML tests.

[DML predicates](../crates/rockstream-sql/src/dml.rs#L337) guess types from strings. Float equality uses absolute machine epsilon around line 382. Existing normal text and integer examples miss ambiguous values.

Insert TEXT rows `[(1,'01'),(2,'1'),(3,'true'),(4,'t')]`. Deleting where `text_col='1'` with `RETURNING id,text_col` must return exactly `[(2,'1')]` and leave the other three rows unchanged. Repeat with UPDATE, boolean-looking text, rollback, and durable reopen.

For DOUBLE rows 0 and `1e-17`, equality with zero must affect only zero. For DECIMAL above exact binary-float precision, verify one-unit arithmetic and exact stored/returned values. Compare command tags, returning rows, schema, and all nonmatching rows. These regressions protect against wrong-row deletion.

### D4. Distinguish unknown Kafka delivery from absent delivery

Priority: P1. Extend [kafka_sink_guarantee_matrix_tests.rs](../crates/rockstream-connectors/tests/kafka_sink_guarantee_matrix_tests.rs).

[check_epoch_delivered](../crates/rockstream-connectors/src/kafka_sink.rs#L207) returns false for consumer failures and timeout as well as absent delivery. Recovery republishes after false. The cases named crash-during-commit and uncertain-response await successful commit before dropping the sink, around lines 114 and 130.

Commit epoch 1 at the broker, lose the acknowledgement, restart, and prevent delivery lookup while producer access remains possible. Require a retryable unknown-outcome error and no second publication. Restore lookup and retry twice. The entire topic must contain exactly one original epoch-1 record, with no residual staging. Repeat with interruption before broker commit, which must eventually produce the same single record.

The payload guarantee also needs a real test. [capabilities.toml:696](../capabilities.toml#L696) claims view-change and snapshot delivery. [kafka_sink.rs:293](../crates/rockstream-connectors/src/kafka_sink.rs#L293) emits an epoch/row-count marker, while the named payload-matches-view test passes only a count. Start with `(1,'a'),(2,'b')`, snapshot, update key 1 to `c`, delete key 2, and recover. Compare the complete change stream and reconstructed sink relation `[(1,'c')]`. If only markers are supported, narrow the claim until row delivery exists. Kafka sink is documented as Core in [connectors.md](connectors.md), so this mismatch matters to the stated product guarantee.

## Test design that will find more bugs

Every new regression should identify the production entry point, independent expected result, failure boundary, and durable state afterward. A name or capability-table link does not prove the path ran.

For queries, compare schema and typed row-to-signed-weight maps. Preserve NULL validity, precision, and multiplicity. Use ordered transcripts when order is contractual. Sets hide duplicate rows; counts hide wrong values. Do not use hashes as the only oracle for small results that can be compared directly.

For failures, compare the defined error code, prior or explicitly outcome-unknown state, resource release, and subsequent recovery. Generic `is_err()` assertions do not verify the promised failure. Keep contractual diagnostics stable rather than matching incidental log text.

Use existing proptest, simulation, Tokio test utilities, object-store adapters, and testcontainers. Start with short histories that shrink well: insert, duplicate, update, delete, commit, crash, reopen, compact, retry. Compare production state after every committed step with a small independent model. Preserve minimized SQL or operation histories as well as seeds, because generator changes can change a seed's meaning.

| Property | Concrete application |
| --- | --- |
| Equivalent batch partitions produce the same result | Group commit and aggregate consolidation |
| Permuting an unordered signed batch preserves results | Joins, distinct, and finalized-window corrections |
| Retry after restart is a no-op for committed operations | Catalog, source checkpoints, and sink recovery |
| Reopen equals uninterrupted execution | Spill, migration, arrangement state, and checkpoints |
| Failed atomic operations preserve prior state | Overflow, storage errors, cancellation, and admission |
| Unrelated identities remain isolated | Shared DB operators, shards, workloads, namespaces, and principals |
| Available capacity is not stranded | Credit cancellation and notification races |

Keep direct examples even after adding properties. Cover empty and all-NULL inputs, zero versus NULL, duplicates, negative weights, integer limits, decimal precision, threshold minus one/equal/plus one, and stale authority tokens. Use the existing type/capability contracts to select supported combinations. A large seed count does not establish coverage of those boundaries.

Once the regressions exist, deliberately suppress a flush error, drop a retraction, replace NULL with zero, remove deduplication, bypass a limit, and discard a recovery record. Require the appropriate test to fail for the intended semantic reason. A compiler error does not demonstrate assertion strength. Begin with a few targeted mutations; a large new testing framework is unnecessary.

## CI and release allocation

The following is a proposed allocation, not a measured runtime claim.

| Tier | Work | Evidence to retain |
| --- | --- | --- |
| Relevant PR | Deterministic regressions, short generated histories, corpus replay, scheduling and skip checks | Exact test IDs and failing histories |
| PR integration | LFS reopen, focused MinIO failures, protocol transcripts, one real worker/control failure | Actual rows, metadata, control transitions |
| Nightly | Assigned simulation targets, longer histories, bounded fuzz runs, repeated race schedules | Minimized failures, seeds, corpus additions, timeouts |
| Release candidate | Distinct-version upgrade, quorum loss, acknowledged-commit crash, broker uncertainty, sustained churn | Candidate digest, environment, commands, executed scenario manifest, observed recovery events |

Coverage currently runs on main pushes and schedule, not PRs. It gates line and region percentages. Use uncovered code to prioritize work, but do not mistake line/region coverage for branch behavior or assertion strength. Fix execution mapping and actual-output comparisons first, then add changed-code coverage for high-risk paths and branch outcomes where supported.

The aggregate `Check` job depends only on `check-quality` and `check-tests`. HA, formal, Verus, and other jobs are separate. Confirm actual required status checks in repository settings; this audit did not inspect them. A green `Check` alone does not demonstrate those separate jobs passed.

Preserve correctness failures and their schedules. Do not introduce blanket retries that turn races into intermittent green results. Host-noise handling for performance measurements is a separate policy.

## Recommended order and acceptance criteria

1. Land B1/B2 fixes with permanent regressions. Add B3 through B6 because they protect recovered state and authority.
2. Repair A1 execution and A2/A3 qualification evidence. Require actual results before presenting release claims as proven.
3. Strengthen A4's typed oracle, then implement C1 through C3 and D1/D3 with small exact examples.
4. Add real exchange, protocol rejection, broker uncertainty, migration restart, quorum, durability-boundary, and upgrade cases.
5. Enable the scheduled fuzz engine and longer histories. Increase budgets after measuring exercised paths and failure transitions.

A safety claim is covered when a scheduled test observes the relevant production behavior and fails under a targeted semantic mutation. Supported-query compilation errors, absent mandatory execution, and missing recovery observations must never become successful qualification.

## What ran during this audit

All commands used the repository's `rtk` wrapper. Passing output was restricted to summaries.

| Check | Result |
| --- | --- |
| Cargo metadata, offline, no dependencies | 542 integration targets, including 67 simulation targets |
| `rtk cargo test --offline --locked -p rockstream-sim --features simulation --test chaos_tests` | 15 passed; 2.62 seconds suite time, about 121 seconds including build |
| `rtk proxy bash scripts/check-no-hidden-skips.test.sh` | Passed |
| `rtk proxy bash scripts/check-qualification-mutations.test.sh` | Passed |
| Comments-only qualification fixture | Incorrectly accepted as verified |
| Unknown-suite runner fixture, with build/Docker stubbed | Exit 0; claimed 8 passed, 0 failed, 0 mandatory skipped |
| Temporary catalog target, offline and locked | 0 passed, 3 failed exactly at intended state assertions; about 28 seconds including build |

The three catalog failures were `audit_snapshot_preserves_operation_idempotency`, `audit_log_replay_preserves_workload_identity`, and `audit_snapshot_preserves_workload_identity`. They used the real `DurableCatalogStore` and existing in-memory object store. They prove serialization/replay defects, not physical backend durability. Script probes used isolated temporary fixtures and did not modify repository scripts or create real release evidence.

To rerun the preserved catalog regressions, use an unused temporary integration-test filename. Remove the copied file after inspecting the expected failures:

```sh
rtk proxy cp evidence/test-suite-audit/catalog_recovery_regressions.rs crates/rockstream-storage/tests/__audit_catalog_recovery.rs
rtk cargo test --offline --locked -p rockstream-storage --test __audit_catalog_recovery
rtk proxy rm crates/rockstream-storage/tests/__audit_catalog_recovery.rs
```

## Inventory reproduction

Counts refer to the audited revision before temporary probes. Enumerate Cargo targets with:

```sh
rtk proxy cargo metadata --offline --no-deps --format-version 1 > /tmp/rockstream-metadata.json
rtk proxy python3 -c 'import json; d=json.load(open("/tmp/rockstream-metadata.json")); print(sum("test" in t["kind"] for p in d["packages"] for t in p["targets"]))'
```

The static source counts used this scan:

```sh
rtk proxy python3 -c 'from pathlib import Path; import re
for crate in sorted(Path("crates").iterdir()):
    texts=[p.read_text() for p in crate.rglob("*.rs")]
    tests=sum(len(re.findall(r"#\[(?:tokio::)?test(?:\([^]]*\))?\]",s)) for s in texts)
    properties=sum(s.count("proptest!") for s in texts)
    print(crate.name, tests, properties)'
```

For simulation selection, compare direct test targets with workflow `--test` arguments, then inspect broad invocations and called scripts for implicit execution:

```sh
rtk proxy python3 -c 'from pathlib import Path; import re
targets={p.stem for p in Path("crates/rockstream-sim/tests").glob("*.rs")}
selected=set().union(*(set(re.findall(r"--test\s+([a-zA-Z0-9_]+)",p.read_text())) for p in Path(".github/workflows").glob("*.yml")))
print("targets",len(targets),"selected",len(targets & selected))
print("\n".join(sorted(targets-selected)))'
```

The unmatched targets at this revision are:

```text
accounting_gate_tests
auto_tuner_chaos_tests
backfill_fence_sim_tests
benchmark_process_isolation_tests
catalog_coordination_tests
cdc_transaction_sim_tests
checkpoint_recovery
cli_inspection_sim_tests
dlq_crash_recovery_sim_tests
dml_coordination_fault_tests
dst_fuzz_harness
e2e_delta_native_qualification_tests
e2e_qualification_tests
edge_case_recovery_tests
explainability_progress_sim_tests
external_benchmark_harness_tests
factorized_plan_path_coupling_tests
failure_matrix_tests
hostile_tenant_real_ingest_quota_tests
internal_mtls_sim_tests
lifecycle_coordination_tests
progress_soak
qualification_lifecycle_tests
qualification_recovery_tests
qualification_resource_tests
qualification_sim_coordination_tests
recovery_coordination_fault_tests
recovery_slo_tests
resource_leak_soak_tests
rolling_upgrade_sim_tests
rolling_upgrade_tests
scale_proof_20_views_sharing_tests
scenario_reproducibility_sim_tests
secrets_sim_tests
shard_migration_tc_tests
sim_aggregate_consolidation_tests
sim_concurrent_maintenance_fault_tests
sim_migration_crash_boundary_tests
sim_shared_trace_crash_boundary_tests
spill_coordination_sim_tests
spot_preemption
storage_pressure_admission_tests
v05910_introspection_sim_tests
v05911_sql_ergonomics_sim_tests
v05912_error_catalog_sim_tests
v05913_product_surface_sim_tests
v05914_golden_path_sim_tests
worker_drain_tc_tests
workload_admission_control
```
