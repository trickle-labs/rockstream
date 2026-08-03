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
   versions that overlaps this version's Scope. Any undocumented deferral found in Phase 1 must be included here as "must implement this version".
2. **Dispatch-Wiring Audit** (SQL/wire-protocol features only):
   - For every SQL keyword, command, or operator the Scope claims to add:
     - List every code path it **must** traverse (parser → dispatcher → executor → response encoder)
     - For each path, record the actual file/line where it's wired in codebase (or "MISSING")
     - **Pre-commit**: "If any of these paths is missing, this feature is incomplete"
   - Example: A `SELECT ... WHERE` feature needs: DataFusion parser (`rockstream-sql`) → `dispatch_query` → `try_datafusion_select` or incremental executor → response encoder. All four must exist and be connected.
3. **Coverage Matrix** (for aggregate/type/join/window features):
   - Commit to an explicit matrix of what must work by version end:
     - Aggregates: (key_type × value_type × agg_func) — e.g., `(int32, int64, SUM)`, `(text, int64, SUM)`, etc.
     - Joins: (join_type × key_type)
     - Windows: (window_func × key_type)
   - Any cell without a test commitment is a Phase 4 gap → implementation is incomplete.
   - CI verification: every matrix cell has ≥1 test name listed.
4. **Vertical Slices**: Break the Scope into thin vertical slices, each ending
   in a green test. Slices must include dispatch-wiring and matrix coverage.
5. **Test Commitments**: For every new operator, pre-commit to its **oracle
   property test** (`incremental == batch`) before writing the operator.
6. **Durability Slices**: For every new durability path (commit, replay,
   checkpoint, compaction, WAL), pre-commit to **both** a SlateDB LFS test
   **and** a MinIO (S3, via TestContainers) test.
7. **Coordination Slices**: For every new distributed-coordination path,
   pre-commit to at least one seeded `SimRuntime` test with `buggify!()`
   annotations.
8. **Proof Mapping**: Map each Proof claim to the specific test(s) that will
   prove it. If a claim has no test mapped to it, the plan is not finished.

**For DESIGN.md and IVM.md, always search first — never read entire files or sections:**
```
rtk grep "topic keyword" DESIGN.md    # returns ~10-50 relevant lines
rtk grep "topic keyword" IVM.md
```
Only read a bounded excerpt (`offset` + `limit`) when grep output alone is insufficient.

---

## Exit

Once every Proof claim is mapped to a test, dispatch wiring is audited (with missing wires reported as "MUST ADD"), and coverage matrices are committed:

1. Write the plan to `.claude/${input:version}-plan.md` containing:
   - Vertical slices list
   - Dispatch-wiring audit (file/line for each path, flagging any MISSING)
   - Coverage matrices with test commitments for each cell
   - Proof-claim → test mapping
   This file survives the upcoming compaction.
2. Output **exactly** this message and nothing else:
   > "Phase 2 done. Plan saved to `.claude/${input:version}-plan.md` (including dispatch-wiring audit and coverage matrices). Run `/compact` now, then run `/implement-version-implement-3a` with version `${input:version}`."
3. Stop. Do not proceed. Do not read any further prompt files.
