---
mode: agent
description: "Phase 4: Prove — satisfy all proof obligations. Reads .claude/<version>-plan.md, saves .claude/<version>-evidence.md."
---

# Implement `${input:version}` — Phase 4: Prove

Prerequisite: Phase 3 (`implement-version-implement`) must be complete — all
slices implemented and tests passing.
Start by reading `.claude/${input:version}-plan.md` to reload the proof-claim →
test mapping before running any proofs.

## ⚡ Ground Rules (non-negotiable)

- Prefix every shell command with `rtk` (e.g. `rtk cargo test`, `rtk git status`).
- Do not weaken, skip (`#[ignore]`), or delete tests to make a build pass. Do not
  use `--no-verify`. Fix the root cause.
- **Nothing may be deferred to a later version.**

---

## ⚡ Fast Test Execution & Summary Lines Only

1. **Leverage Fast E2E & Nextest**:
   - For LFS and MinIO proof obligations, run `rtk make e2e` (or `rtk make e2e-nextest`), which executes warm in ~9 seconds.
   - Scope individual proof test commands strictly to the target crate and test name:
     ```bash
     rtk cargo test -p <crate> --test <test_target> <test_name> 2>&1 | grep -E "^test .* (ok|FAILED|ignored)|^test result:|^FAILED$|^error"
     ```
   - If `cargo-nextest` is available, prefer nextest for fast parallel test execution:
     ```bash
     rtk cargo nextest run -p <crate> --test <test_target> <filter>
     ```

2. **Extract Summary Lines Only**:
   Show full output **only for FAILED tests**. Passing test noise balloons the context
   cache and gets re-read on every subsequent turn, multiplying token costs 10–30×.

## Phase 4: Prove

Satisfy every rung the version's **Backends** and **Proof** require:

- **Reachability / e2e pgwire** (SQL/wire-protocol features): Every SQL feature must pass an **end-to-end test sending raw SQL through the actual dispatcher**, proving it's callable from `psql` or a standard client without importing private modules. This is independent of unit-test coverage.
- **Negative/error handling**: Every SQL/wire feature must pass explicit negative tests — invalid input must return an `RS-XXXX` error with actionable text, never a silent empty response or wrong answer.
- **Dispatch-wiring audit verification**: Every path in the Phase 2 dispatch-wiring audit must be tested as actually connected (grep output or test proof that parser → dispatcher → executor → response encoder are all wired). Report any path still MISSING as a Phase 4 failure.
- **Coverage matrix**: Every cell in the Phase 2 coverage matrix (key_type × value_type × aggregate, etc.) must pass a test. A cell without a test is a Phase 4 gap.
- **Oracle / property**: Every new operator passes `incremental == batch` over
  randomized insert/update/delete/retract sequences at the scenario count the
  Proof names. Use the DataFusion batch reference in `rockstream-oracle`.
- **LFS backend**: Real SlateDB on the local filesystem proves on-disk
  correctness for every durability path.
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
benchmark number, seed, or measurement note). For SQL/wire features, additionally capture:
- Test file name and test function name for reachability test (e2e pgwire)
- Test file name and test function name for negative tests (error handling)
- Grep output or test proof showing each dispatch-wiring path is connected

---

## Exit

Once all Proof claims have passed and evidence is collected, verify no Phase 4 gaps remain:

- [ ] **Reachability tests** (e2e pgwire): Every SQL/wire feature has a test sending raw SQL through the dispatcher
- [ ] **Negative tests**: Every SQL/wire feature has tests for invalid input returning proper `RS-XXXX`
- [ ] **Dispatch-wiring audit**: Every path in Phase 2 audit is proven connected (grep or test output)
- [ ] **Coverage matrix**: Every cell has a passing test
- [ ] All other Proof obligations met (oracle, LFS, MinIO, SimRuntime, benchmarks, soak, etc.)

1. Write the proof evidence table to `.claude/${input:version}-evidence.md` —
   each claim, the test/command that proves it, and pass/fail with output excerpt.
   For SQL/wire features, include dispatch-wiring verification and reachability test output.
2. Output **exactly** this message and nothing else:
   > "Phase 4 done. All Proof claims passing (including reachability, negative tests, and dispatch-wiring verification). Evidence saved to `.claude/${input:version}-evidence.md`. Run `/compact` now, then run `/implement-version-signoff` with version `${input:version}`."
3. Stop. Do not proceed. Do not read any further prompt files.
