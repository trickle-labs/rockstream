---
mode: agent
description: Implement one version from NEW_ROADMAP.md to release quality, with exhaustive testing and a complete sign-off.
---

# Implement RockStream Version `${input:version:e.g. v0.4}`

You are the implementing engineer for a single RockStream version. Your job is to
take **`${input:version}`** from end to end — design, code, exhaustive tests,
benchmarks, docs, and a complete sign-off — so that the result is an
**extremely well-made, release-quality** increment.

This is a correctness-critical, cloud-native streaming database. There is no
credit for "mostly working". A version is done only when its proof is done.

---

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
- Never use HEREDOC. For multi-line commit/PR/sign-off bodies, write a temp file
  and use `git commit -F` / `--body-file`; then re-read it to confirm it is not
  garbled.
- One binary, one CLI, one config: every role is a flag on the same `rockstream`
  binary. `main` must remain runnable through it at the end of this version.
- Strict ordering: do **not** start this version until the previous version's
  `sign-offs/v*.md` is complete. If it is not, stop and report.
- No code path may depend on SlateDB range deletion. Cleanup is scan-and-delete
  or snapshot-safe compaction filters, and a test must assert this.
- Unbounded in-memory accumulation is never acceptable. Every new queue, buffer,
  or scan window needs a named upper bound, a fill-level metric, and a
  backpressure or error path.
- Do not weaken, skip (`#[ignore]`), or delete tests to make a build pass. Do not
  use `--no-verify`. Fix the root cause.
- Make only changes this version requires. No speculative features, no drive-by
  refactors, no scope outside the two pillars (IVM engine + Postgres wire layer).
- **Nothing may be deferred to a later version.** If the Scope or Proof requires
  it, it is implemented now, in full. Partial implementations, stubs, and TODO
  placeholders are build failures, not progress.
- **Pick up all prior deferrals.** Before writing any new code, scan every
  previous `sign-offs/v*.md` for items marked deferred, postponed, or out-of-scope.
  Any such item whose topic overlaps this version's Scope **must** be implemented
  in this version.

---

## Phase 0: Validate Input

1. Confirm `${input:version}` exists in [NEW_ROADMAP.md](../../NEW_ROADMAP.md) (I'll read just that row)
2. Check that the **previous version's** `sign-offs/v*.md` is **complete** (all boxes checked)
3. Report the version's **Focus, Scope, Proof, and Backends** from the roadmap row
4. Stop if the previous version is incomplete. Otherwise, proceed to Phase 1.

---

## Phase 1: Orient (Read scope, restate proof)

Once Phase 0 is confirmed:

1. **I will fetch and read only the `${input:version}` row** from NEW_ROADMAP.md
2. **I will check** `sign-offs/` for any prior version with deferred items overlapping
   this version's Scope. I'll list any found items explicitly.
3. **You will restate**, in your own words, the **exact proof obligations** for this
   version. List every concrete claim in the Proof column as a checkable assertion.
   This list is your contract — nothing is "done" until every item is independently
   verifiable.
4. **I will confirm** the deferred-item audit is complete

**Phase 1 Checkpoint**: Once you've restated proof obligations and we agree they're
complete, confirm "Ready for Phase 2" to proceed to planning.

---

## Phase 2: Plan

Once Phase 1 is locked:

1. **Deferred-Item Audit**: You will list every deferred/out-of-scope item from prior
   versions that overlaps this version's Scope.
2. **Vertical Slices**: Break the Scope into thin vertical slices, each ending in a
   green test.
3. **Test Commitments**: For every new operator, pre-commit to its **oracle property
   test** (`incremental == batch`) before writing the operator.
4. **Durability Slices**: For every new durability path (commit, replay, checkpoint,
   compaction, WAL), pre-commit to **both** a SlateDB LFS test **and** a MinIO
   (S3, via TestContainers) test.
5. **Coordination Slices**: For every new distributed-coordination path, pre-commit
   to at least one seeded `SimRuntime` test with `buggify!()` annotations.
6. **Proof Mapping**: Map each Proof claim to the specific test(s) that will prove it.
   If a claim has no test mapped to it, the plan is not finished.

**I will fetch DESIGN.md and IVM.md sections on-demand as you reference them.**

**Phase 2 Checkpoint**: Once the plan is complete with every test mapped to a Proof
claim, confirm "Ready for Phase 3" to proceed to implementation.

---

## Phase 3: Implement (Test-first, in slices)

Once Phase 2 is locked:

Work one slice at a time. For each slice:

1. Write or extend the failing test first (oracle/property, LFS, MinIO, SimRuntime,
   or integration — whichever the slice's risk demands).
2. Implement the smallest correct code that satisfies it.
3. Wire user/operator-visible failures to an `RS-XXXX` error code with actionable
   `next_steps` text. Register new codes; CI fails on any returned `Error` or
   logged `error!` without a code.
4. Emit an audit event for any control-plane action.
5. Add a fill-level metric and a bound to any new queue/buffer/scan window.
6. Keep `main` runnable through the single `rockstream` binary.

Re-run the relevant tests after each slice. Diagnose and fix failures at the root
cause; do not paper over them.

**Phase 3 Checkpoint**: Once all slices are implemented and tests pass, confirm
"Ready for Phase 4" to proceed to proving.

---

## Phase 4: Prove

Once Phase 3 is locked (all code complete and tests passing):

Satisfy every rung the version's **Backends** and **Proof** require:

- **Oracle / property**: Every new operator passes `incremental == batch` over
  randomized insert/update/delete/retract sequences at the scenario count the
  Proof names. Use the DataFusion batch reference in `rockstream-oracle`.
- **LFS backend**: Real SlateDB on the local filesystem proves on-disk correctness
  for every durability path.
- **MinIO backend**: Real S3-compatible store (via TestContainers) proves S3
  semantics — list/get/put latency, conditional writes, multipart, retries,
  brownout, coalesced shuffle objects.
- **TestContainers integration**: Any real external process (MinIO, Postgres,
  Kafka) is brought up and torn down by the test itself.
- **Deterministic simulation**: Seeded `SimRuntime` proves correctness under
  reordering, partial failure, and crash-replay. **Any fault found by simulation
  is checked in as a permanent regression seed.**
- **Crash/chaos**: Where the Proof calls for it, inject the fault and prove
  bit-identical recovery against a non-faulty reference.
- **Benchmark**: Any performance claim gets a `criterion` benchmark or recorded
  measurement note; establish/refresh baseline.
- **Soak (if gated)**: Run the required long soak to completion and capture clean
  results. A soak is a gate, not a loophole.

For every Proof claim, capture concrete evidence (test name, command, CI output,
benchmark number, seed, or measurement note).

**Phase 4 Checkpoint**: Once all Proof claims have passed and evidence is collected,
confirm "Ready for Phase 5" to proceed to sign-off.

---

## Phase 5: Sign Off

Once Phase 4 is locked (all proof complete):

1. Create the sign-off template: `rtk make approve VERSION=<X.Y>` (note: pass the
   number without the `v`).
2. Edit `sign-offs/v<X.Y>.md` so it enumerates **every** Proof claim from the
   roadmap row, each with the concrete evidence that proves it, and check off
   every box. The exit-criteria CI gate (`scripts/check-exit-criteria.sh`) fails
   if any `- [ ]` remains.
3. Update the version's status in the roadmap to `✅ Done` only after the
   sign-off is complete.
4. Run `rtk ./scripts/check-exit-criteria.sh` and `rtk make e2e` and confirm both
   pass.

**Phase 5 Checkpoint**: Once all checks pass, confirm "Ready for Phase 6" to
proceed to final reporting.

---

## Phase 6: Report

Finish with a concise report containing:

- A table mapping each **Proof** claim → the test/benchmark/seed that proves it →
  pass/fail with evidence.
- The exact commands to reproduce the proof locally.
- The full Definition-of-Done checklist:
  - `rtk cargo fmt --check`, `rtk cargo clippy -- -D warnings`, and
    `rtk cargo test --workspace` all pass.
  - New behavior has unit tests **plus** oracle/property, LFS, MinIO, and/or
    TestContainers tests.
  - Every user/operator-visible failure has an `RS-XXXX` code with actionable text.
  - Every control-plane action writes an audit event.
  - Every new performance claim has a `criterion` benchmark or measurement note.
  - Every new public surface (SQL syntax, CLI command, config key, system table) is
    documented.
  - Every new queue/buffer/scan window has a named bound, a fill-level metric, and
    backpressure/error path.
  - Every new distributed-coordination path has at least one seeded `SimRuntime` test.
  - No code path depends on SlateDB range deletion.
  - `main` remains runnable through the single `rockstream` binary.
  - Any soak gate has been run clean.
- Any new `RS-XXXX` codes, docs pages, metrics, and regression seeds added.
- **Confirmation that no item has been deferred.** If anything remains incomplete,
  the version is **not done**.

If — and only if — every Proof claim is independently verified, every Definition
of Done item holds, and the sign-off is complete, declare `${input:version}`
**done**. Otherwise, state precisely what remains.

---

## 📋 Starting Phase 1 Now

I'll read the NEW_ROADMAP.md for `${input:version}` and check prior sign-offs.
Ready?
