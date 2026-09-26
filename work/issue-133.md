# Acceptance contract: trickle-labs/rockstream#133 — Enforce the workload resource envelope

Contract revision: v1
Source: [issue #133 and parent source captures](issue-133-source.md); [v0.72 plan](../docs/implementation-plans/v0.72.md) at commit `6442838183ffd913b6a24860072575aacaa9c96a`; [frozen profile](../benchmarks/r1-local/profile-v072.toml).
Source identity: issue #133 response SHA-256 `8eda061ab39508bd4128fec8b8bba90ab3b6fd5d918d297bd026e01876752502`; no issue comments. Parent and decomposition capture identities are recorded in `issue-133-source.md`.
Parent: [issue #132 contract](../docs/acceptance-contracts/132.md) v2
Parent snapshot: commit `7a73050dbb28fdf61584d052b9521b2e7dce9835`; exact bytes SHA-256 `cc91b392f193e771e38336c7da3c62480ef7337083ce1a5c301f576096335734`.
Contribution: [canonical decomposition excerpt](issue-133-source.md#canonical-decomposition-excerpt), S1 for #133. S1 owns the workload resource envelope and complete limit/fill/overflow inventory. It refines `trickle-labs/rockstream#132 v2:R2`, `R3`, `R4`, `R5`, and `R6`; S1 consolidates the R6 inventory. S2/#134 owns commit resources, S3/#135 interactive-query resources, and S4/#136 subscription resources. S1 records those delegated inventory entries but does not claim their behavior.
Prerequisites: Before dependent qualification, confirm the sign-off outcomes and linked evidence for v0.71, v0.61.1, v0.61.2, v0.62.1, v0.65.1, v0.67.1, v0.68, v0.69, and v0.70 as named in the parent contract. A checked box or closed issue alone does not establish these outcomes.

Intended outcome: Worker and workload allocations have one owner, soft pressure slows producers, hard pressure rejects or truthfully degrades work within the bounded envelope, and bounded resources expose fill and overflow behavior without silent loss.

## Acceptance matrix

| ID | Source | Requirement | Boundaries / counterexamples | Seam | Oracle | Planned evidence | Plan state |
|---|---|---|---|---|---|---|---|
| R1 | #133 criterion 1; `v0.72.md` V072-01; `trickle-labs/rockstream#132 v2:R2` | Account for operator state as `OperatorState`, source buffers as `SourceBuffers`, exchange buffers as `ExchangeBuffers`, query work as `QueryWorkMemory`, catalog metadata/projection caches as `CatalogCaches`, checkpoint staging as `CheckpointStaging`, migration buffers as `MigrationBuffers`, and worker-wide SlateDB write buffers, block cache, and metadata cache in distinct categories. Assign each allocation one stable owner and charge shared allocations once, even when referenced by multiple shards or consumers. Report category totals for worker and workload-owned allocations through public status. Report local object-store cache bytes and capacity separately from RAM accounting. | Any omitted or unowned allocation, duplicate shared charge, inaccurate category or owner total, cache disk bytes included in RAM totals, disk-byte mismatch, or RSS above the frozen allowance fails. | Public worker/workload budget status and workload execution; `WorkerBudgetLedger`/`MemoryCategory`, allocation permits, `WorkerStorageContext`, and SlateDB disk-cache configuration. | The v0.72 inventory and frozen profile determine expected categories and limits. Independently sum file bytes beneath the canonical worker cache root; verify reported bytes and capacity against that sum and the 32 GiB per-worker cap. Measure process RSS separately against the 2 GiB worker budget plus the frozen 15% allowance. | `v072_memory_ownership`: exact category, owner, and ledger deltas for each allocation class, including a shared cache referenced by multiple consumers; cache-fill status compared with an independent filesystem byte sum and capacity; separate RSS sample checked against the profile allowance. | planned |
| R2 | #133 criterion 2; `v0.72.md` V072-01; `trickle-labs/rockstream#132 v2:R3` | Reserve prospectively and release reservations on success, error, cancellation, shard removal, and restart; reconcile ledger totals with RSS and documented allocator overhead. | Allocation before admission, a retained reservation after a terminal path, unexplained RSS excess, or treating RSS as identical to ledger bytes fails. | Public workload/worker status and operations across workload execution and process lifecycle. | Frozen worker envelope and an independent allocation/lifecycle transcript; expected live allocations follow active work, and RSS is compared using the documented profile allowance. | `v072_memory_lifecycle`: exact ledger and RSS records after successful, failed, cancelled, shard-removal, and restart paths, including before/after reservation values. | planned |
| R3 | #133 criterion 3; `v0.72.md` V072-02; `trickle-labs/rockstream#132 v2:R4` | At the frozen workload soft limit (429496729 bytes), throttle or adapt source and exchange producers and expose the resulting pressure state. | Unbounded unchanged production, silent result loss, or pressure that cannot be observed fails. | Public ingestion/workload execution and worker/workload status; source and exchange producer paths. | Frozen profile soft limit and an independently scheduled input transcript with complete committed output. | `v072_soft_pressure`: two-workload boundary and over-limit process run recording exact status, producer backpressure/adaptation, and complete result multisets for source and exchange paths. | planned |
| R4 | #133 criterion 3; `v0.72.md` V072-02; `trickle-labs/rockstream#132 v2:R5` | Reject or truthfully degrade work before the frozen workload hard limit (536870912 bytes) can exceed the documented allocation envelope, and roll back a failed reservation. | Allocating first, hidden rejection, false success, retained failed reservation, or lost committed output fails. | Public ingestion/query errors and worker/workload status; prospective admission at the quota/budget seam. | Frozen hard limit and documented allocation envelope, plus exact admitted input and expected complete committed results. | `v072_hard_pressure`: exact at-limit and over-limit responses, admitted work, ledger deltas before/after failed reservation, and recovery after capacity is released. | planned |
| R5 | #133 criterion 4; `v0.72.md` V072-11; `trickle-labs/rockstream#132 v2:R6` | Inventory every queue, retry set, scan, migration buffer, result, and waiter with a named limit, fill observation, and overflow/backpressure or error policy. Record commit resources as S2/#134-owned, interactive-query resources as S3/#135-owned, and subscription resources as S4/#136-owned; S1 covers its resources and the inventory, with consolidated behavior checked under parent R6. | An omitted or unnamed resource, missing bound/fill/overflow policy, unobservable fill, silent drop, or claiming delegated sibling behavior as S1 evidence fails. | Inventory and public metrics/affected operations for S1-owned resource paths; the inventory records delegated S2–S4 seams without claiming their outcomes. | V072-11 and parent R6 determine required coverage; each S1-owned entry has an observable fill and a defined overflow result. | `v072_bounded_resources`: resource inventory and S1-owned boundary/overflow transcript, including allocation failure, cancellation cleanup, and shard churn; preserve delegated entries for the consolidated parent check. | planned |

## Unresolved gaps

None.

## Open questions

None. The parent v2 contract and decomposition settle the outcome and sibling allocation.

## Out of scope

- S2 scheduling, durable adaptive epochs, and commit-resource enforcement beyond recording its R6 inventory owner.
- S3 interactive-query behavior and S4 subscription behavior beyond recording their R6 inventory owners.
- S5 workload revision propagation; S6–S9 capacity qualification/reporting; cost claims; and v0.73.1 console/API, CSV/download, or unrestricted whole-dataset export.
- Unrelated allocator/cache redesign, new workload classes, silent drops, synthetic capacity claims, and unmeasured cloud claims.

## Change notes

- Migrated the existing `docs/acceptance-contracts/133.md` v1 agreement to this canonical work-item path, preserving requirements R1–R5 and their outcomes. R1's owner/category map and independent evidence path are now explicit; no material promise or exclusion changed, so the revision remains v1.

## Implementation handoff

Implement the complete agreement within the stated scope. Preserve requirement IDs and outcomes. Capture the fixed candidate for separate review and proof.

## Proof handoff

Evaluate every requirement against this contract revision and one fixed candidate. Record actual evidence and verdicts in a separate proof report.
