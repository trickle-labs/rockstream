---
mode: agent
description: "Phase 5–6: Sign off and report — write sign-off doc, check exit criteria, produce final proof table."
---

# Implement `${input:version}` — Phase 5–6: Sign Off & Report

Prerequisite: Phase 4 (`implement-version-prove`) must be complete — all proof
claims passing and evidence collected.
Start by reading `.claude/${input:version}-evidence.md` to reload the evidence
table before writing the sign-off.

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
- Never use HEREDOC. For multi-line commit/PR/sign-off bodies, write a temp file
  and use `git commit -F` / `--body-file`; then re-read it to confirm it is not
  garbled.
- **Nothing may be deferred to a later version.** If anything remains incomplete,
  the version is not done.

---

## Phase 5: Sign Off

1. Create the sign-off template: `rtk make approve VERSION=<X.Y>` (pass the
   number without the `v`).
2. Edit `sign-offs/v<X.Y>.md` so it enumerates **every** Proof claim from the
   roadmap row, each with the concrete evidence that proves it, and check off
   every box. The exit-criteria CI gate (`scripts/check-exit-criteria.sh`) fails
   if any `- [ ]` remains.
3. Update the version's status in the roadmap to `✅ Done` only after the
   sign-off is complete.
4. Run `rtk ./scripts/check-exit-criteria.sh` and `rtk make e2e` and confirm
   both pass.

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
  - Every new public surface (SQL syntax, CLI command, config key, system table)
    is documented.
  - Every new queue/buffer/scan window has a named bound, a fill-level metric,
    and backpressure/error path.
  - Every new distributed-coordination path has at least one seeded `SimRuntime`
    test.
  - No code path depends on SlateDB range deletion.
  - `main` remains runnable through the single `rockstream` binary.
  - Any soak gate has been run clean.
- Any new `RS-XXXX` codes, docs pages, metrics, and regression seeds added.
- **Confirmation that no item has been deferred.**

If — and only if — every Proof claim is independently verified, every
Definition-of-Done item holds, and the sign-off is complete, declare
`${input:version}` **done**. Otherwise, state precisely what remains.
