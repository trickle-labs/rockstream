# Recommended Future Roadmap — Disposition

**Status: merged into [NEW_ROADMAP.md](NEW_ROADMAP.md) on 2026-08-11.**

This document originally proposed six future work items in prose. It has been
rewritten into this repository's Version / Focus / Scope / Proof / Backends
form and each item has been given an explicit disposition, because a proposal
with no version number, no exit criterion, and no statement of whether it lands
before or after v1.0 cannot be admitted into the version table.

The review that produced these dispositions found the original document
strategically sound and independently convergent with the 2026-08-11
rebaseline in [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) —
narrow the surface, deepen the lifecycle — but misfiled two items as "future"
that are in fact prerequisites of the v0.57 v1 contract, and duplicated work
already committed at v0.54 and v0.58.

---

## Disposition summary

| # | Original item | Disposition | Lands as |
|---|---|---|---|
| 1 | Resumable online backfill | **Accepted, pulled forward — v1 blocker** | v0.52.1 (new Phase 16.5) |
| 2 | Transaction-preserving PostgreSQL CDC | **Accepted, pulled forward — v1 blocker** | v0.52.2 (new Phase 16.5) |
| 3 | Durable `SUBSCRIBE` history | **Accepted in direction, no v1 slot** | Post-1.0 candidate, built on the v0.52.1 fence |
| 4 | Storage-pressure-aware admission | **Folded into existing scope** | v0.58 (signal set extended) |
| 5 | Barrier-latency audit & control-plane priority | **Folded into existing scope** | v0.54 (measure first, mechanism conditional) |
| 6 | Retraction-producing temporal filters | **Deferred by decision** | Deferred table, with readmission evidence |

Two gaps the original document did not cover were added: **upstream schema
evolution** (folded into v0.52.2 — an upstream `ALTER TABLE` is a first-order
concern the moment `pgoutput` becomes the canonical `Core` CDC path) and
**multi-region disaster recovery** (recorded as deferred rather than left
neither planned nor rejected).

---

## Why 1 and 2 are v1 blockers, not successors

v0.57 freezes a public contract that commits to documenting, for every `Core`
operator, its *incremental, backfill, checkpoint/recovery, state-growth, and
failure* semantics — and names **PostgreSQL CDC** as one of the two `Core`
connectors.

- Backfill today is a non-resumable one-shot. `MigrationState`'s
  `Snapshotting`/`CatchingUp` phases in
  [crates/rockstream-types/src/migration.rs](crates/rockstream-types/src/migration.rs)
  belong to *shard migration*, not view creation, and `SHOW BACKFILL STATUS` is
  documented in [docs/concepts.md](docs/concepts.md) but was found unwired to
  gateway dispatch at v0.45.5. The v0.57 backfill clause cannot be written
  honestly against that.
- CDC today decodes `pgoutput` row-at-a-time in
  [crates/rockstream-connectors/src/postgres_cdc.rs](crates/rockstream-connectors/src/postgres_cdc.rs)
  with no transaction envelope, so a two-table upstream transaction can be
  observed half-applied. Freezing that as `Core` is the wrong order of
  operations.

Both therefore land as **Phase 16.5 (v0.52.1–v0.52.2)**, before the operability
and contract versions, not after v1.0.

## The structural correction: build the fence once

Item 1's "atomic snapshot/delta fence tying the initial snapshot to the exact
source position from which live changes resume" and item 3's
"snapshot-plus-live subscription" are the same problem. Scheduled as
independent versions they would be implemented twice with subtly different
semantics. v0.52.1 therefore ships the fence as a **named, reusable primitive**,
and the post-1.0 durable-`SUBSCRIBE` work consumes it rather than reinventing
it.

## What was folded rather than scheduled

- **Item 4** overlaps v0.58, which already commits to object-store brownout,
  spill, and compaction pressure in the published failure matrix, and to
  SLO-driven control loops. v0.51.23 already wired real state-byte accounting
  into [crates/rockstream-gateway/src/admission.rs](crates/rockstream-gateway/src/admission.rs).
  What is genuinely new is the LSM signal set, which is an extension of an
  existing controller, not a version. The proposed composite `storage_debt`
  scalar is deliberately **not** adopted: collapsing five heterogeneous signals
  into one number is how a control loop starts oscillating with no way to
  explain why. Signals stay separate and individually attributable.
- **Item 5** is half already committed: v0.54 promises checkpoint-alignment lag
  as a named contributor and `rockstream checkpoint show` naming the shard and
  operator holding the barrier. Only the reserved control channel / credit
  budget is new, and the original document was right to gate it behind "if
  necessary". v0.54 now also measures **barrier flight time separately from
  checkpoint completion time**, and the mechanism is admitted only if that
  measurement shows barriers are actually being delayed behind data traffic.

## What was deferred

**Item 6** is new SQL breadth by the project's own admission rule, and
event-time TTL already produces retractions at the operator level in
[crates/rockstream-ops/src/time_window.rs](crates/rockstream-ops/src/time_window.rs).
It is recorded in the roadmap's "Deferred by decision" table with its
readmission evidence stated, rather than kept in the roadmap body marked
"optional" — where it would invite growth.

## Scope guardrails

The original document's guardrail list is **not** carried over as a separate
list. [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §8 already
defines an admission rule and NEW_ROADMAP.md already carries a "Deferred by
decision" table with per-item readmission evidence. A second parallel list of
prohibitions is precisely the kind of duplicate that drifts. Every guardrail in
the original that was not already covered is now a row in that table instead.

The strategic goal is unchanged and is stated once, in
[ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md): a small number of
excellent ingestion paths, durable object-storage-backed incremental state,
correct materialized-view lifecycle behaviour, PostgreSQL-compatible serving,
and a robust resumable change stream.

## Still unowned

Recorded here so it is not mistaken for settled:

- **Cost and write amplification.** Every item above adds durable writes —
  backfill cursors, the durable changelog, the v0.52 DLQ. Object-storage write
  amplification and per-view cost have no owner in any scheduled version.