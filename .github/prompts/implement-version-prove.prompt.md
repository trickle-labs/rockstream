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

## Phase 4: Prove

Satisfy every rung the version's **Backends** and **Proof** require:

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
benchmark number, seed, or measurement note).

---

## Exit

Once all Proof claims have passed and evidence is collected:

1. Write the proof evidence table to `.claude/${input:version}-evidence.md` —
   each claim, the test/command that proves it, and pass/fail with output excerpt.
2. Output **exactly** this message and nothing else:
   > "Phase 4 done. Evidence saved to `.claude/${input:version}-evidence.md`. Run `/compact` now, then run `/implement-version-signoff` with version `${input:version}`."
3. Stop. Do not proceed. Do not read any further prompt files.
