---
mode: agent
description: "Phase 2: Plan — deferred-item audit, vertical slices, test commitments, proof mapping. Saves plan to .claude/<version>-plan.md."
---

# Implement `${input:version}` — Phase 2: Plan

Prerequisite: Phases 0–1 (`implement-version-orient`) must be complete and proof
obligations agreed. Start by reading the `${input:version}` row from
[NEW_ROADMAP.md](../../NEW_ROADMAP.md) to re-establish context.

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
- One binary, one CLI, one config: every role is a flag on the same `rockstream`
  binary. `main` must remain runnable through it at the end of this version.
- No code path may depend on SlateDB range deletion. Cleanup is scan-and-delete
  or snapshot-safe compaction filters, and a test must assert this.
- Unbounded in-memory accumulation is never acceptable. Every new queue, buffer,
  or scan window needs a named upper bound, a fill-level metric, and a
  backpressure or error path.
- Do not weaken, skip (`#[ignore]`), or delete tests to make a build pass. Do not
  use `--no-verify`. Fix the root cause.
- Make only changes this version requires. No speculative features, no drive-by
  refactors, no scope outside the two pillars (IVM engine + Postgres wire layer).
- **Nothing may be deferred to a later version.**
- **Pick up all prior deferrals.** Scan every previous `sign-offs/v*.md` for
  items marked deferred, postponed, or out-of-scope that overlap this version's
  Scope.

---

## Phase 2: Plan

1. **Deferred-Item Audit**: List every deferred/out-of-scope item from prior
   versions that overlaps this version's Scope.
2. **Vertical Slices**: Break the Scope into thin vertical slices, each ending
   in a green test.
3. **Test Commitments**: For every new operator, pre-commit to its **oracle
   property test** (`incremental == batch`) before writing the operator.
4. **Durability Slices**: For every new durability path (commit, replay,
   checkpoint, compaction, WAL), pre-commit to **both** a SlateDB LFS test
   **and** a MinIO (S3, via TestContainers) test.
5. **Coordination Slices**: For every new distributed-coordination path,
   pre-commit to at least one seeded `SimRuntime` test with `buggify!()`
   annotations.
6. **Proof Mapping**: Map each Proof claim to the specific test(s) that will
   prove it. If a claim has no test mapped to it, the plan is not finished.

**For DESIGN.md and IVM.md, always search first — never read entire files or sections:**
```
rtk grep "topic keyword" DESIGN.md    # returns ~10-50 relevant lines
rtk grep "topic keyword" IVM.md
```
Only read a bounded excerpt (`offset` + `limit`) when grep output alone is insufficient.

---

## Exit

Once every Proof claim is mapped to a test:

1. Write the plan to `.claude/${input:version}-plan.md` — vertical slices list
   and proof-claim → test mapping. This file survives the upcoming compaction.
2. Output **exactly** this message and nothing else:
   > "Phase 2 done. Plan saved to `.claude/${input:version}-plan.md`. Run `/compact` now, then run `/implement-version-implement` with version `${input:version}`."
3. Stop. Do not proceed. Do not read any further prompt files.
