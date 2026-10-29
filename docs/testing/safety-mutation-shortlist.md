# Safety mutation shortlist and pilot results

Status: **human-reviewed and approved by the user in chat on 2026-10-27.** Post-review pilot results are recorded below.

Pinned source: `2bb60012318fa966e1dc98866122daceabc59607` (`safety-p0-base`, 2026-10-27). The #265 and #267 experiments listed below were executed before this shortlist existed. They are historical evidence, not evidence that the review-first gate was met.

Discovery record: Codex / GPT-6 family; the serving build identifier is not exposed in this session. Prompt revision `safety-shortlist-v1` is recorded verbatim below and was used for the final ranking review. No mutation was run while drafting the shortlist.

> Using only source commit `2bb60012318fa966e1dc98866122daceabc59607` and the #261/#263 acceptance criteria, rank up to 20 credible P0/P1 mutation scenarios by data-loss/corruption severity, production reachability, suspected detector weakness, then execution cost. For each, trace production caller → durable state → ACK/recovery; give exact source and test locations, minimal legal history, concrete source edit, wrong external result, independent expected state, test package/features/services, predicted catch or survival with confidence, approximate cost, and counterevidence. Separate source-only hypotheses from historical executed results and strong controls. Do not execute or modify source while drafting.

Cost labels are observed only where a prior report gives a measurement. Otherwise cost is unmeasured and the fixture/service requirement is stated. Commands use default Cargo features unless noted.

## Ranked safety candidates

### 1. P0 — acknowledge an unknown physical flush as durable

- **Path and invariant:** `PhysicalCommitGroup::commit_epoch` → physical worker/flush result classification in `crates/rockstream-ops/src/group_commit/physical.rs:401,642`. `FlushUnknown` must not return a durable ACK, advance the frontier, or release retained epochs.
- **Legal history and mutation:** accept epoch 1, queue epoch 2, then inject the test's flush error before the LocalFileSystem-backed `ShardDb` flush. Mutate `Outcome::FlushUnknown(error)` to `(DURABLE, None)`.
- **Expected wrong observation:** epoch 1 is returned as successful while its flush failed; queued work may be released. The existing detector expects the exact injected error and reconciliation transcript.
- **Detector and prediction:** `rockstream-ops` lib test `group_commit::physical::boundary_tests::unknown_flush_keeps_later_epoch_queued_until_reconciliation`; `rtk cargo test -p rockstream-ops unknown_flush_keeps_later_epoch_queued_until_reconciliation --lib`; no external service. The same mutation was historically caught at the ACK assertion (expected `Err`, observed `Ok([1])`) in #265. Confidence high for this injected pre-flush error.
- **Counterevidence / limit:** this does not simulate power loss after an ACK; it cannot establish physical durability. Historical targeted baseline was 0.33s, mutant 0.14s; not a per-run infrastructure cost measurement.

### 2. P0 — acknowledge an unknown physical write as durable

- **Path and invariant:** write timeout becomes `Outcome::WriteUnknown` at `physical.rs:553`, then shares the durable/unknown classification at `:642`. A submitted write with unknown completion must remain unacknowledged until reconciliation.
- **Legal history and mutation:** accepted epoch write times out while its backend outcome is unknown. Map `WriteUnknown(_)` to `(DURABLE, None)` while retaining normal error handling elsewhere.
- **Expected wrong observation:** the caller receives success although the epoch may be absent after reopen. Expected independent oracle: exact ACK error plus the full reopened key/value set, frontier and retained-work accounting.
- **Detector and prediction:** the #265 test above is the narrow package target, but its fixture injects a **flush** error; it does not exercise the write-timeout arm. The post-review test `group_commit::physical::boundary_tests::unknown_unpersisted_write_is_not_acked_and_retry_recovers_lfs` blocks the submitted LocalFileSystem put before persistence, observes the timeout error, drops/reopens the shard, checks the missing value/frontier, then retries the unacknowledged epoch. No external service.
- **Counterevidence / limit:** this exercises the no-write outcome of an unknown operation and proves the wrong ACK would leave no recovered value. Drop/reopen is a process-like test, not physical power loss after ACK or a remote-backend guarantee.

### 3. P0 — lose committed-operation identities during catalog snapshot recovery

- **Path and invariant:** `DurableCatalogStore::recover` at `crates/rockstream-storage/src/catalog/store.rs:267,348` must restore operation IDs so an old create retry cannot undo a later delete.
- **Legal history and mutation:** create table with operation 555 at revision 1; delete with operation 556 at revision 2; snapshot and compact the log; recover and retry operation 555. Remove the snapshot's `committed_operations` restoration.
- **Expected wrong observation:** the deleted table returns and revision regresses from 2 to 1; the independently captured object inventory must remain unchanged.
- **Detector and prediction:** `rockstream-storage` integration test `catalog_storage_tests::test_snapshot_retry_preserves_deleted_table_and_objects`; `rtk cargo test -p rockstream-storage --test catalog_storage_tests test_snapshot_retry_preserves_deleted_table_and_objects`; in-memory object store, no external service. Historically caught at the exact catalog result in #265; baseline and mutant test bodies were each reported at 0.00s.
- **Counterevidence / limit:** the historical mutant failed before reaching the later object-inventory assertion. Keep the exact result as the intended kill and do not claim physical-backend recovery.

### 4. P0 — acknowledge a replayable PostgreSQL source transaction before its M3 commit

- **Path and invariant:** `GatewayServer` calls `SourceRuntimeCoordinator::commit_replayable_epoch` at `crates/rockstream-gateway/src/server.rs:4709`; the runtime commits its caller-built M3 batch at `crates/rockstream-connectors/src/source_runtime.rs:445`, then ACKs the PostgreSQL source at `:454`. The view writes and replayable LSN must be durable before upstream ACK.
- **Legal history and mutation:** submit a transaction with visible rows and its LSN, inject a commit failure at the M3 boundary, and move the source `commit_offset` call ahead of that commit (or continue to it after the commit error).
- **Expected wrong observation:** upstream offset advances while the row/checkpoint is absent; after restart the source resumes beyond the missing row. Independent oracle: no ACK, unchanged committed offset, and exact reopened M3 records/checkpoint.
- **Detector and prediction:** candidate targets are `rockstream-connectors/tests/cdc_transaction_durability_lfs_tests.rs` and the gateway pgoutput transaction tests; no external service for the LFS fixture. The current source-coordination simulation manually commits M3 before recovery and verifies one later ACK; it does not exercise commit failure followed by premature ACK. Prediction: existing detectors likely survive; confidence high in the assertion gap, medium in mutation reachability until a failing-store cut-point is selected. Runtime unmeasured.
- **Counterevidence / limit:** the LFS tests verify coupled bytes after reopen, but their successful path does not inject failure between M3 and source ACK. A service error is not proof of post-ACK power-loss behavior.

### 5. P0 — resume a source from an older or incomplete committed offset map

- **Path and invariant:** `SourceEpochRegistry::recovery_offsets` at `crates/rockstream-connectors/src/source_epoch.rs:672`; LFS reopen/replay detector at `tests/source_proof_tests.rs:326-427`.
- **Legal history and mutation:** persist epoch 1 with partition 0 at offset 1 and partition 1 at offset 0, close/reopen the `ShardDb`, then resume. Mutate recovery to return partition 0's prior offset (0).
- **Expected wrong observation:** the next poll returns duplicate value 10 from partition 0 instead of the exact expected value 20 from partition 1.
- **Detector and prediction:** `rockstream-connectors --test source_proof_tests test_source_offset_replay_lfs`; LocalFileSystem, no external service. Prediction: caught by the final literal value assertion; confidence medium-high. Runtime unmeasured.
- **Counterevidence / limit:** the test reopens the DB and registry but reuses the original `RecordedSource` instance. It verifies persisted offset selection, not a full connector-process restart or real broker ACK recovery.

### 6. P0 — export success without the committed shard objects

- **Path and invariant:** `CheckpointExportService::export_latest_prefix` at `crates/rockstream-control/src/checkpoint_export.rs:169-197` must inventory objects referenced by the committed checkpoint before writing a terminal success marker.
- **Legal history and mutation:** keep a committed checkpoint manifest, delete its referenced shard objects, then export. Replace `list_checkpoint_objects` with a listing of only currently visible prefix objects.
- **Expected wrong observation:** export reports `SUCCESS` although the committed checkpoint's shard objects are missing.
- **Detector and observed result:** `rockstream-control --test disaster_recovery_tests cli_prefix_export_rejects_missing_committed_shard_objects_before_success`; `InMemory`, no external service. The unchanged test passed before and after (0.11s and 0.13s test body). Removing the checkpoint shard-object inventory was caught by the intended assertion: the mutant returned `Ok(CheckpointExportOutcome { checkpoint_id: 37, generation: "missing-shard-preinventory", object_count: 3, byte_count: 166, status: "SUCCESS" })`. The mutant test body took 0.12s; the command took about 12.8s including incremental rebuild. See `mutation-pilot-results.json` and the retained failure log.
- **Counterevidence / limit:** this is the same service method used by the storage export path, but the fixture uses `InMemory`; the mutant's early assertion failure means the test did not reach its subsequent terminal-marker absence assertion. It does not qualify a physical or remote object store.

### 7. P1 — accept a stale source-owner lease

- **Path and invariant:** `SourceRuntimeCoordinator::require_active_lease` at `crates/rockstream-connectors/src/source_runtime.rs:542` guards commit/ack methods; recovery replaces owner A's fence token with owner B's.
- **Legal history and mutation:** recover a committed epoch, acquire A then B, and let A acknowledge or commit using its stale lease. Remove the active-fence comparison.
- **Expected wrong observation:** stale owner receives success or emits an ACK after B owns the source; expected transcript is RS-4013, only B's ACK, and the exact committed offset.
- **Detector and prediction:** `rockstream-connectors --test source_coordination_sim_tests recovery_uses_only_committed_checkpoint_and_fences_replaced_owner`; in-memory store, no external service. Existing test checks exact error, leases, offset and ACK list. Strong expected catch; runtime unmeasured.
- **Counterevidence / limit:** this proves the Rust caller boundary in the simulation; it does not establish a distributed lease service or process-crash guarantee.

### 8. P1 — admit a stale frontier generation

- **Path and invariant:** membership report adapter at `crates/rockstream-control/src/frontier.rs:392-429` derives generation match before calling the verified admission kernel. Mutate `generation_matches` to `true`.
- **Legal history and mutation:** two active shards report epoch 5 in generation 3; then shard 1 submits epoch 99 from generation 2.
- **Expected wrong observation:** stale report is accepted and cluster frontier can advance from 5 to 99 instead of returning `GenerationMismatch` with state unchanged.
- **Detector and prediction:** `rockstream-control` unit test `frontier::tests::stale_generation_and_incarnation_reports_leave_state_unchanged`; no service. It asserts the exact error and frontier remains 5. Strong expected catch; runtime unmeasured.
- **Counterevidence / limit:** the verified kernel depends on caller-supplied booleans. This test covers adapter-to-kernel wiring; it does not prove all distributed publication paths.

### 9. P1 — admit a stale shard incarnation

- **Path and invariant:** the same adapter at `frontier.rs:407-421` derives incarnation match from active membership. Mutate `incarnation_matches` to `true`.
- **Legal history and mutation:** generation 3 has shard 1 incarnation 7; a report from incarnation 6 advertises epoch 99.
- **Expected wrong observation:** stale incarnation is admitted instead of returning `IncarnationMismatch`; frontier must remain 5 on the valid path.
- **Detector and prediction:** same exact frontier unit test and package as rank 8; strong expected catch, no service, runtime unmeasured.
- **Counterevidence / limit:** rank 8 and rank 9 are separate predicate faults but share one regression. Count them separately only if each isolated mutant is applied and restored independently.

## Strong controls and calibration candidates

These are useful to check prediction quality and cross-crate detector selection. They do not displace the P0/P1 work above.

### 10. P1 control — dropped join retraction

- **Path and mutation:** `JoinOperator::process_right_delta` at `crates/rockstream-ops/src/join.rs:649`; skip a legal negative right delta after a matching insert.
- **Expected wrong observation:** stale committed row remains after delete. The #267 weighted history observes `[]` expected versus `[(3,30,300,1)]` actual; `join_delete_retracts_previous_output` is independent counterevidence.
- **Detector/cost:** `rockstream-oracle --lib oracle_join_insert_then_delete_right` plus `rockstream-ops --lib join_delete_retracts_previous_output`; no service. The targeted bodies were reported under 0.1s; isolated edits rebuilt in roughly 20–50s.
- **Status/limit:** this mutation was executed before shortlist review and caught by #267's exact oracle. It is historical control evidence, not a new run authorization.

### 11. P1 control — double a positive join multiplicity

- **Path and mutation:** `JoinOperator::commit_epoch` at `crates/rockstream-ops/src/join.rs:700`; change `l_w * r_w` to `2 * l_w * r_w` for a matching key.
- **Legal history and wrong result:** epoch 0 with left weight +2 and right weight +3 should produce `[(1,10,100,6)]`; the mutant produces `[(1,10,100,12)]`.
- **Detector/prediction:** #267 branch test `rockstream-oracle --lib oracle_join_weighted_deltas_across_epochs`; no service. Predicted exact catch with high confidence; test body under 0.1s and compile/rebuild 20–50s were reported for this branch. The isolated post-review mutation result is recorded below.
- **Counterevidence / limit:** `correction_preserves_exact_weighted_many_to_many_join_rows` already protects one scenario; model prediction is not a confirmed new result.

### 12. P1 control — corrupt the simultaneous 3-way delta product

- **Path and mutation:** 3-way oracle/operator cross-check in `crates/rockstream-oracle/src/join_oracle.rs` and `JoinOperator::commit_epoch`; omit one factor from `ΔL ⋈ ΔR ⋈ ΔS`.
- **Expected wrong result:** the deterministic weights 2, 3, and 4 yield 24; the reported mutant yielded 2.
- **Detector/cost:** #267 branch test `oracle_3way_join_deterministic` plus the oracle library suite; no service. Full oracle lib was 107.54s (140 tests), but that full suite is unnecessary for this targeted fault once its narrow test is confirmed.
- **Status/limit:** historical #267 experiment, caught before shortlist review; no new run proposed unless review selects it as a control.

### 13. P1 control — export newer uncommitted shard bytes

- **Path and mutation:** `list_checkpoint_objects` at `crates/rockstream-control/src/checkpoint_export.rs:216`; export the live mutable shard prefix rather than objects named by the committed snapshot.
- **Expected wrong observation:** during a write of `uncommitted-newer`, restore returns it instead of `committed` from the checkpoint.
- **Detector/prediction:** `rockstream-control --test disaster_recovery_tests lfs_export_under_writes_restores_one_committed_epoch`; LocalFileSystem, no service. Strong expected catch; runtime unmeasured.
- **Counterevidence / limit:** this is a strong control for checkpoint consistency, not a demonstrated P0 mutation result.

### 14. P1 control — Kafka sink checkpoint/transaction coupling

- **Path and mutation:** `KafkaSink::commit` and cluster committed horizon (`crates/rockstream-connectors/src/kafka_sink.rs:284,478`); allow a pending transaction to commit before its checkpoint horizon, or fail to suppress duplicate replay.
- **Expected wrong observation:** after crash/recovery and rerun, the topic contains zero or two payloads instead of exactly `[{"epoch":1,"rows":17}]`.
- **Detector/prediction:** `rockstream-connectors --test kafka_sink_guarantee_matrix_tests kafka_sink_checkpoint_coupling_has_exact_commit_transcript`; Kafka test fixture/container required. Strong-looking transcript, but Docker/service setup and runtime are unmeasured here.
- **Counterevidence / limit:** the test directly rejects committing an unapproved pending state, restarts the sink, reruns after setting the cluster horizon, and compares the exact payload. It is a control until its service actually runs in the selected environment.

### 15. P2 calibration — shared average helper integer division

- **Path and mutation:** shared `avg_from_sum_count` at `crates/rockstream-types/src/laws/sum_count.rs:164`; replace fractional division with integer division.
- **Expected wrong observation:** `(sum,count)=(7,3)` returns 2 instead of `7/3`.
- **Detector/cost:** independent literal tests `rockstream-ops::avg_computes_correct_fractional_average` and `rockstream-oracle::oracle_aggregate_genuinely_fractional_average`; no service. The oracle test was reported to fail at the exact `7/3` assertion; selected test bodies under 0.1s, rebuild 20–50s.
- **Counterevidence / limit:** both implementation and batch oracle share the helper, so their comparison alone is correlated. Independent literals already catch this fault; it is calibration, not a priority safety gap.

## Post-review pilot results

Human review was received before these runs. Candidate source edits were applied one at a time in isolated temporary Git worktrees, then reverted. The machine-readable record is [mutation-pilot-results.json](mutation-pilot-results.json). The source-only changes in the experiment worktrees were absent after each restoration.

| Candidate | Baseline | Mutant | Classification |
| --- | --- | --- | --- |
| Rank 2, `WriteUnknown` mapped to durable (original target) | `unknown_flush_keeps_later_epoch_queued_until_reconciliation` passed | Same test passed | Expected survivor: detector injected `FlushUnknown`, so the write-timeout arm was not invoked. |
| Rank 2, with unpersisted-put timeout regression | New LFS test passed (5.14s) | Exact assertion failed: ACK `Some([1])`, but reopened frontier was `0` and value was absent | Caught; restored baseline passed. The test blocks the first put before persistence, drops/reopens, then verifies retry recovers the epoch. |
| Rank 4, ignore replayable M3 write failure | Existing LFS success-path test passed | Existing test passed | Existing detector survived because it did not inject M3 failure. |
| Rank 4, with new LFS failure regression | New baseline passed; full target suite passed (6 tests) | Exact transcript failed: `Ok(())`, ACK `(1, 0/53)`, no committed checkpoint/row/frontier after reopen | Caught by intended exact-state assertion. |
| Rank 5, restore empty source offset map | LFS replay baseline passed | Exact value assertion failed: observed `10`, expected `20` | Caught; restored baseline passed. |
| Rank 8, force generation match | Frontier baseline passed | Exact `GenerationMismatch` assertion failed | Caught; restored baseline passed. |
| Rank 11, double join multiplicity | Weighted oracle baseline passed | Epoch 0 expected weight `6`, observed `12` | Caught; restored baseline passed. |

The new source regression drives the production `commit_replayable_epoch` caller with a LocalFileSystem-backed `ShardDb` and injected write failure, records the upstream ACK, closes/reopens the DB, and compares the exact result, ACK, committed offset, checkpoint, row and frontier. It is evidence for an ACK-before-durable fault at the M3 failure boundary; it does not simulate power loss after an ACK.

### Selection comparison and cost

- **LLM shortlist result:** the two sampled weak-detector hypotheses (ranks 2 and 4) were both confirmed and fixed, an observed precision of 2/2 (100%) with no false positives in this small, stratified sample. Rank 2's original target was flush-only; the unpersisted-put timeout regression catches `WriteUnknown` mapped to durable and reports the missing recovered value/frontier. The source-offset detector and two strong controls (frontier generation and join multiplicity) also caught their intended mutants. This point estimate is not a stable precision estimate for the wider shortlist.
- **Non-LLM baseline:** before running, selected the three simplest issue-enumerated source patterns without model ranking: unknown outcome → durable (rank 2), membership predicate → constant true (rank 8), and removed join-weight factor (rank 11). The original rank 2 target did not exercise the write-timeout arm; the later unpersisted-write regression caught it, and both controls were caught. This small deterministic comparison found no additional actionable gap. The sample is too small and too stratified to estimate actionable-flag precision or findings per runner-hour; no random selection was run.
- **Toolchain/configuration:** `rustc 1.99.1 (58cf8f9c3 2026-10-13)` and `cargo 1.99.1 (5f94df478 2026-08-27)`, default Cargo features, commands use `rtk cargo test --locked --offline`. Source baselines were `2bb60012318fa966e1dc98866122daceabc59607` and, for weighted join, `b66f71f6f1eb67f6644805f5c4c797ad9b5866b3`.
- **Observed local cost:** the five unique mutant variants had selected mutant test invocations of about 4.4–11.3s each including incremental compilation; rank 4 also reran its existing success-path detector. The new unpersisted-write test body took 5.14s on baseline and 5.03s under mutation with Rust 1.88; the restored baseline passed again. No CPU/RSS runner metrics or repeated flake sample was collected for this targeted test.
- A first M3 mutant spelling failed compilation with E0282 because the ignored generic result had no type. It was corrected to an explicit `Result<(), SourceError>` before execution; the compile failure is not counted as detection.

## Remaining qualification gaps

No physical power-loss-after-ACK test was run. The new timeout test blocks the LocalFileSystem put before persistence, then proves the caller withholds its ACK, observes the missing value/frontier after drop/reopen, and can retry the unacknowledged epoch. It does not simulate physical power loss after ACK or qualify a real remote backend. The static-heuristic comparison is small, no random baseline was run, and two post-fix hosted runs are insufficient to estimate a broad flake rate. Hosted CPU/RSS and runner-cost measurements are recorded in [the lane report](safety-mutation-lane.md). The weekly run is bounded and remains partial evidence, not an end-to-end durability qualification.
