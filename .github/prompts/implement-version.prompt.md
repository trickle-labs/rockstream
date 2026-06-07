---
mode: agent
description: Implement one version from NEW_ROADMAP.md to release quality, with exhaustive testing and a complete sign-off.
---

# Implement RockStream Version `${input:version:e.g. v0.4}`

You are the implementing engineer for a single RockStream version. Your job is to
take **`${input:version}`** from end to end — design, code, exhaustive tests,
benchmarks, docs, and a complete sign-off — so that the result is an
**extremely well-made, release-quality** increment. You do not stop until the
version's **Proof** is real, reproducible, and checked into the repository.

This is a correctness-critical, cloud-native streaming database. There is no
credit for "mostly working". A version is done only when its proof is done.

---

## 0. Ground Rules (non-negotiable)

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
  in this version. Add it to the Plan and track it alongside the new work.

---

## 1. Orient (read before you write any code)

1. Read the `${input:version}` row in [NEW_ROADMAP.md](../../NEW_ROADMAP.md):
   its **Focus**, **Scope**, **Proof**, and required **Backends**.
2. Read, in this order, the binding context:
   - The Roadmap's **Testing Conventions**, **Common Definition of Done**, and the
     milestone this version belongs to.
   - [NEW_IMPLEMENTATION_PLAN.md](../../NEW_IMPLEMENTATION_PLAN.md) — the matching
     phase and its exit gate, plus the cross-cutting Testing Strategy ladder.
   - [DESIGN.md](../../DESIGN.md) and [IVM.md](../../IVM.md) for any section the
     Scope references (e.g. `IVM-n` tags, DESIGN.md §-numbers).
3. Read **all** previous `sign-offs/v*.md` files. For each one, collect every item
   explicitly marked as deferred, postponed, skipped, or out-of-scope. Any such
   item whose topic overlaps this version's Scope is now a mandatory work item —
   treat it the same as if it were listed in the roadmap row. Read the crates the
   Scope touches so you build on the real current state, not an assumed one.
4. Restate, in your own words, the **exact proof obligations** for this version.
   List every concrete claim in the Proof column as a checkable assertion. This
   list is your contract — nothing is "done" until every item is independently
   verifiable.

---

## 2. Plan

Produce a short, ordered implementation plan and track it with a todo list:

- Begin with a **Deferred-Item Audit**: list every item collected in step 1.3
  that must be picked up, and include each as an explicit slice in the plan.
- Break the Scope into thin vertical slices, each ending in a green test.
- For every new operator, pre-commit to its **oracle property test**
  (`incremental == batch`) before writing the operator.
- For every new durability path (commit, replay, checkpoint, compaction, WAL),
  pre-commit to **both** a SlateDB local-filesystem (LFS) test **and** a MinIO
  (S3, via TestContainers) test.
- For every new distributed-coordination path, pre-commit to at least one seeded
  `SimRuntime` test, with `buggify!()` annotations on the coordination code.
- Map each Proof claim to the specific test(s) that will prove it. If a claim has
  no test mapped to it, the plan is not finished.

---

## 3. Implement (test-first, in slices)

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

---

## 4. Prove (the binding part — do all that apply to this version)

Satisfy every rung the version's **Backends** and **Proof** require:

- **Oracle / property:** Every new operator passes `incremental == batch` over
  randomized insert/update/delete/retract sequences at the scenario count the
  Proof names (e.g. ≥100k). Use the DataFusion batch reference in
  `rockstream-oracle`.
- **LFS backend:** Real SlateDB on the local filesystem proves on-disk
  correctness for every durability path.
- **MinIO backend:** Real S3-compatible store (via TestContainers) proves S3
  semantics — list/get/put latency, conditional writes, multipart, retries,
  brownout, coalesced shuffle objects — for anything depending on them.
- **TestContainers integration:** Any real external process (MinIO, Postgres,
  Kafka) is brought up and torn down by the test itself. No shared or
  hand-managed infrastructure.
- **Deterministic simulation:** Seeded `SimRuntime` proves correctness under
  reordering, partial failure, and crash-replay. **Any fault found by simulation
  is checked in as a permanent regression seed** and replayed in CI forever.
- **Crash/chaos:** Where the Proof calls for it (e.g. `kill -9` mid-`WriteBatch`,
  partitions, disk-full, object-store throttling), inject the fault and prove
  bit-identical recovery against a non-faulty reference.
- **Benchmark:** Any performance claim gets a `criterion` benchmark or a recorded
  measurement note; establish/refresh the baseline so CI can fail on >10%
  regression.
- **Soak (if gated):** Run the required long soak (e.g. 1h fuzzer, 24h chaos,
  multi-day availability) to completion and capture clean results. A soak is a
  gate, not a loophole.

For every Proof claim, capture concrete evidence (test name, command, CI output,
benchmark number, seed, or measurement note). Vague "it works" is a failure.

---

## 5. Definition of Done (Common — every item must hold)

- `rtk cargo fmt --check`, `rtk cargo clippy -- -D warnings`, and
  `rtk cargo test --workspace` all pass.
- New behavior has unit tests **plus** the oracle/property, LFS, MinIO, and/or
  TestContainers tests its risk requires.
- Every user/operator-visible failure has an `RS-XXXX` code with actionable
  `next_steps`.
- Every control-plane action writes an audit event.
- Every new performance claim has a `criterion` benchmark or measurement note.
- Every new public surface (SQL syntax, CLI command, config key, system table) is
  documented under `docs/`.
- Every new queue/buffer/scan window has a named bound, a fill-level metric, and a
  backpressure or error path.
- Every new distributed-coordination path has at least one seeded `SimRuntime`
  test.
- No code path depends on SlateDB range deletion (asserted by a test).
- `main` remains runnable through the single `rockstream` binary.
- Any soak gate the version names has been run clean.

---

## 6. Sign Off

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

---

## 7. Report

Finish with a concise report containing:

- A table mapping each **Proof** claim → the test/benchmark/seed that proves it →
  pass/fail with evidence.
- The exact commands to reproduce the proof locally.
- The full Definition-of-Done checklist, each item marked with how it was met.
- Any new `RS-XXXX` codes, docs pages, metrics, and regression seeds added.
- Confirmation that **no item has been deferred**. If anything remains incomplete,
  the version is **not done** — state exactly what is missing and do not declare
  the version complete until it is resolved.

If — and only if — every Proof claim is independently verified, every Definition
of Done item holds, and the sign-off is complete, declare `${input:version}`
**done**. Otherwise, state precisely what remains and stop.
