---
mode: agent
description: "Phase 0–1: Validate input and orient — confirm version exists, prior sign-off complete, and restate proof obligations."
---

# Implement `${input:version}` — Phase 0–1: Orient

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
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

1. Confirm `${input:version}` exists in [NEW_ROADMAP.md](../../NEW_ROADMAP.md)
   (read just that row).
2. Check that the **previous version's** `sign-offs/v*.md` is **complete**
   (all boxes checked).
3. Report the version's **Focus, Scope, Proof, and Backends** from the roadmap row.
4. Stop if the previous version is incomplete. Otherwise, proceed to Phase 1.

---

## Phase 1: Orient

1. **Read only the `${input:version}` row** from NEW_ROADMAP.md.
2. **Extended Deferred-Item Audit** (three-pass scan):
   - **Pass A**: Read every prior sign-off for items marked: "deferred", "TODO", 
     "stub", "placeholder", "out-of-scope". List any that overlap this version's Scope.
   - **Pass B**: Grep codebase for `// TODO`, `unimplemented!()`, `panic!("not yet")`, 
     `FIXME:` in files this version likely touches (compiler, gateway, ops, plan, storage).
   - **Pass C** (SQL features only): For every feature claimed "implemented" in 
     `docs/language-features.md`, verify it appears in actual source:
     - Parser: `rtk grep "<keyword>" crates/rockstream-sql/src/`
     - Dispatch: `rtk grep "<keyword>" crates/rockstream-gateway/src/server.rs`
     - Lowering: `rtk grep "<keyword>" crates/rockstream-sql/src/lower.rs`
     - If a claimed-implemented feature has **zero matches** in dispatch/parser/lowering, 
       it is an undocumented deferral — list it.
   - Report all three categories: explicit deferrals, code TODOs, and implicit (undocumented) deferrals.
3. **Restate**, in your own words, the **exact proof obligations** for this
   version. List every concrete claim in the Proof column as a checkable
   assertion. This list is your contract — nothing is "done" until every item
   is independently verifiable.
4. **Confirm** the extended deferred-item audit is complete and all categories reported.

---

## Exit

If any undocumented deferrals (Pass C findings) are discovered, they must be picked up and implemented in this version — do not defer. Confirm this explicitly before proceeding.

Output **exactly** this message and nothing else:

> "Phases 0–1 done. Proof obligations restated above. Extended audit (explicit deferrals, code TODOs, undocumented gaps) complete. Run `/implement-version-plan` with version `${input:version}` to continue."

Stop. Do not proceed further.
