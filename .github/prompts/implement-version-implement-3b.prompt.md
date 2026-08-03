---
mode: agent
description: "Phase 3b: Implement (part 2) — test-first implementation of remaining slices. Reads .claude/<version>-plan.md and checks git status."
---

# Implement `${input:version}` — Phase 3b: Implement (Part 2)

Prerequisite: Phase 3a (`implement-version-implement-3a`) must be complete.
Start by reading `.claude/${input:version}-plan.md` and running `rtk git status`
to see which slices have been completed. Continue with the remaining slices.

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

---

## Test Output: Summary Lines Only

**After any `cargo test` run, extract only the summary — do not paste full passing
test output into the conversation:**

```bash
rtk cargo test ... 2>&1 | grep -E "^test .* (ok|FAILED|ignored)|^test result:|^FAILED$|^error"
```

Show full output **only for FAILED tests**. Passing test noise balloons the cache
and gets re-read on every subsequent turn, multiplying token costs 10–30×.

---

## Phase 3b: Implement (Part 2)

Implement the remaining slices from `.claude/${input:version}-plan.md`.

For each slice:

1. Write or extend the failing test first (oracle/property, LFS, MinIO,
   SimRuntime, or integration — whichever the slice's risk demands).
2. **For SQL/wire-protocol features**: Add tests in this order before implementation:
   - **Reachability test** (e.g., `*_e2e_pgwire_tests.rs`): Send the raw SQL/command text through the actual entry point. Prove the feature is callable without importing private modules.
   - **Negative test** (e.g., `*_error_handling_tests.rs`): Test invalid input, missing prerequisites. Must return an `RS-XXXX` error with actionable text, never a silent empty response.
   - **Coverage matrix cells**: Unit tests covering dispatch-wiring paths and coverage matrix cells from Phase 2.
3. Implement the smallest correct code that satisfies it.
4. Wire user/operator-visible failures to an `RS-XXXX` error code with actionable
   `next_steps` text. Register new codes; CI fails on any returned `Error` or
   logged `error!` without a code.
5. **Dispatch-wiring verification** (SQL features): Verify every path in the Phase 2 audit is connected (parser → dispatcher → executor → response). If any wire is MISSING or disconnected, implementation is incomplete.
6. Emit an audit event for any control-plane action.
7. Add a fill-level metric and a bound to any new queue/buffer/scan window.
8. Keep `main` runnable through the single `rockstream` binary.

Re-run the relevant tests after each slice. Diagnose and fix failures at the root
cause; do not paper over them.

---

## Exit

Once all remaining slices are implemented and tests pass, verify:

- [ ] Reachability tests (e2e pgwire) are green for all SQL/wire features in second half
- [ ] Negative tests are green for all features (invalid input returns `RS-XXXX`, not silent OK)
- [ ] All dispatch-wiring audit paths are verified as connected (no MISSING wires)
- [ ] All coverage matrix cells have passing tests

1. Output **exactly** this message and nothing else:
   > "Phase 3b done. All remaining slices implemented, all dispatch wiring verified, reachability and negative tests passing, coverage matrix complete. Run `/compact` now, then run `/implement-version-prove` with version `${input:version}`."
2. Stop. Do not proceed. Do not read any further prompt files.
