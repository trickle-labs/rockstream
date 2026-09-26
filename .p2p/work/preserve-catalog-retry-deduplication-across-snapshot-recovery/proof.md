# PROVEN: trickle-labs/rockstream#101

Requirements: 5/5
Counterexamples tested: 5
Contract: `work/preserve-catalog-retry-deduplication-across-snapshot-recovery.md`, v1
Contract SHA-256: `1ec42b3f5f7e40411f36b2def29c20ab11e02a7782879b083ee712aae3ad2ed7`
Source: `work/sources/issue-101.md`, SHA-256 `1675f2decaf1ac45de4edfbe2240c64d38b3f095d637796b95aa9a0e42a8668c`; all five criteria and both comments checked against live issue #101.
Candidate: commit `71ec6550b11453bbc02336dcc8ddfc0d3b2d2d3a`
Comparison base: `6442838183ffd913b6a24860072575aacaa9c96a`
Candidate stability: unchanged; candidate helper validated the full tree against the captured commit before and after checks.
Contract stability: unchanged.
Environment: macOS arm64, rustc 1.98.1 (48a229cea 2026-09-01), isolated checkout `/private/tmp/rockstream-pr142-readiness`.

## Outcome

Retrying committed catalog operations after snapshot, compaction, and recovery is an idempotent no-op: the deleted table stays absent, revision does not regress, and no object bytes change. The full catalog storage integration suite passed on the exact candidate. The catalog implementation and regression file are byte-identical to the previously reviewed/proven candidate; the current candidate also includes the requested contract migration and CI-only fixes.

## Contract reconciliation

Read the current issue body and comments from https://github.com/trickle-labs/rockstream/issues/101. The issue still has the same five acceptance criteria; its two comments record contract preparation and readiness, not amendments. The source contract cites audit B1 at revision `5d9c3b919682140ec0b365d7dba848cd5cf5ecbb`; that commit resolves in the repository but no audit report file exists in its tree. All material promises are present in the issue body and are captured locally. No parent or prerequisites apply. The migration preserved revision v1 and R1-R5.

## Requirement verdicts

| ID | Observation and oracle | Evidence reference | Verdict |
|---|---|---|---|
| R1 | `test_snapshot_retry_preserves_deleted_table_and_objects` creates operation 555 at revision 1, deletes with 556 at revision 2, snapshots, removes logs, and recovers exactly revision 2 with an empty complete table list before retry. Oracle: contract state. | `crates/rockstream-storage/tests/catalog_storage_tests.rs:49`; E1 | proven |
| R2 | The same test retries 555 and asserts returned revision 2, current revision 2, and the empty complete table list on both fresh recovery cycles. Oracle: expected post-delete state. | `catalog_storage_tests.rs:81`; E1 | proven |
| R3 | The test compares the complete map of object paths to bytes before and after retry; only the expected durable snapshot exists after log removal, and retry changes no object. Oracle: byte-for-byte inventory. | `catalog_storage_tests.rs:10,65-90`; E1 | proven |
| R4 | `test_snapshot_retry_preserves_latest_alter_and_objects` repeats deletion recovery/retry and retries an old ALTER after newer revision 3; it checks the entire latest table record and object bytes across snapshot-plus-log and compacted-snapshot recovery. Oracle: explicit latest TableInfo and revision. | `catalog_storage_tests.rs:95-157`; E1 | proven |
| R5 | Log-only replay asserts all five exact records and log bytes; snapshot recovery asserts complete snapshot records and full post-replay records. The legacy snapshot fixture also decodes and re-encodes byte-for-byte. Oracle: explicit transaction/table records. | `catalog_storage_tests.rs:22,160-251`; E1 | proven |

## Evidence

- E1: `rtk cargo test -p rockstream-storage --test catalog_storage_tests` — exit 0; `cargo test: 10 passed (1 suite, 0.01s)`. Saved output: `.p2p/work/preserve-catalog-retry-deduplication-across-snapshot-recovery/evidence/catalog-storage-tests.log`.
- E2: `rtk cargo test -p rockstream-control --test shard_migration_state_machine_tests test_migration_progress_monotonic_all_phases -- --exact` — exit 0; `1 passed, 12 filtered out`. This checks the CI flake repair. Saved output: `evidence/control-progress-test.log`.
- E3: `rtk bash scripts/check-path-coupling.test.sh` — exit 0; exact expected self-test summary saved in `evidence/path-coupling-selftest.log`.
- E4: `BASE=6442838183ffd913b6a24860072575aacaa9c96a rtk bash scripts/check-path-coupling.sh` — exit 0 on the candidate range. Saved output: `evidence/path-coupling-candidate.log`.
- E5: `rtk git diff --quiet 82dd1c4cfd0c9c85428bd7d9d100ee692bab3416..HEAD -- crates/rockstream-storage/src/catalog/snapshot.rs crates/rockstream-storage/src/catalog/store.rs crates/rockstream-storage/tests/catalog_storage_tests.rs` — exit 0; those exact implementation and acceptance-test bytes are unchanged from the prior reviewed/proven candidate.

Evidence limitation: the catalog suite uses the in-memory ObjectStore implementation, exercising real catalog serialization, reads, writes, listing, and deletion; it does not establish physical disk crash durability. No R1-R5 gap depends on a stronger storage seam.

## Unresolved gaps

None within R1-R5.

## Repairs needed

None.

Next steps:

1. Complete the current full GitHub CI run for candidate `71ec6550b11453bbc02336dcc8ddfc0d3b2d2d3a` under the newly active `main` ruleset.
2. Refresh `/review-implementation work/preserve-catalog-retry-deduplication-across-snapshot-recovery.md; candidate 71ec6550b11453bbc02336dcc8ddfc0d3b2d2d3a against 6442838183ffd913b6a24860072575aacaa9c96a`.
3. When proof and review are saved for the final PR head, run `/merge-readiness https://github.com/trickle-labs/rockstream/pull/142; review .p2p/work/preserve-catalog-retry-deduplication-across-snapshot-recovery/review.md; proof .p2p/work/preserve-catalog-retry-deduplication-across-snapshot-recovery/proof.md`.
