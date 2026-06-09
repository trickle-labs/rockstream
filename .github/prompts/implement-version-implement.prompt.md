---
mode: agent
description: "Phase 3: Implement — test-first slice-by-slice implementation. Reads .claude/<version>-plan.md."
---

# Implement `${input:version}` — Phase 3: Implement

Prerequisite: Phase 2 (`implement-version-plan`) must be complete.
Start by reading `.claude/${input:version}-plan.md` to load the vertical slices
and proof-claim → test mapping before writing any code.

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
- Never use HEREDOC. For multi-line commit/PR/sign-off bodies, write a temp file
  and use `git commit -F` / `--body-file`; then re-read it to confirm it is not
  garbled.
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

## Phase 3: Implement (Test-first, in slices)

Work one slice at a time. For each slice:

1. Write or extend the failing test first (oracle/property, LFS, MinIO,
   SimRuntime, or integration — whichever the slice's risk demands).
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

## Exit

Once all slices are implemented and tests pass:

1. Output **exactly** this message and nothing else:
   > "Phase 3 done. All slices implemented and tests passing. Run `/compact` now, then run `/implement-version-prove` with version `${input:version}`."
2. Stop. Do not proceed. Do not read any further prompt files.
