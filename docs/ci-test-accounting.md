# CI test target and mandatory-case accounting (#110)

This page defines the test safety-gate contract. The source of truth for assignment is [`scripts/ci-test-matrix.json`](../scripts/ci-test-matrix.json), validated against **live Cargo metadata**, not a checked-in target count.

## Assignment and execution

`python3 scripts/ci-test-inventory.py check --out target/ci-test-results/assignment.json` discovers workspace packages and testable Cargo lib, binary, and integration targets, checks ownership, verifies feature and service requirements, and rejects unowned or stale simulation targets. Simulation integration targets have **explicit names**; other workspace test targets use package ownership. An added simulation target without an explicit group fails the PR gate. All testable targets of a newly introduced workspace package require an assignment. Non-test Cargo artifacts (examples, benchmarks, build scripts, the fuzz harness) are outside this libtest inventory.

`python3 scripts/ci-test-inventory.test.py` exercises negative controls for missing assignment, missing feature/service, empty mandatory selection, ignored mandatory case, missing terminal event, and unexpected in-test skip.

`python3 scripts/ci-policy.test.py` checks lane selection, the actual `Check` dependency expression (including failure, cancellation, missing results and unexpected skips), expensive-step guards, and unavailable/wrong-revision/wrong-ID image rejection.

`python3 scripts/ci-test-inventory.py run --job <job>` compiles selected Cargo test harnesses with declared features, executes them with machine-readable libtest output, compares `--list` IDs with terminal execution events, and writes `target/ci-test-results/<job>.json`. Every record contains the exact `package::kind/target::case` ID, outcome, job/tier, active package features, declared services, and a reason for any limitation or selection omission. A binary compiling or listing its tests is **not** a passing execution. A harness exit failure or missing terminal event is fatal. A test which prints a `SKIP` marker while returning success is not credited. A missing Docker daemon or configured image is a failed prerequisite, never success.

A known `#[ignore]` case may only be recorded as a **limitation** if individually identified with a meaningful reason under `known_limitations`. These do **not** count as passes or prove the claimed behavior. Unknown ignored cases fail. The R1 real-worker gateway-to-worker source-transport proof and the three empty MinIO placeholders in `lfs_catalog.rs` are currently limitations. They must be implemented and removed from the limitation ledger before they can be claimed as passing mandatory proofs. The ordinary mandatory tests in their targets remain required.

## Ownership and expensive jobs

| Job key | Trigger | Scope |
| --- | --- | --- |
| `pr-workspace` | CI PR/push | Default testable Cargo targets in lightweight workspace packages |
| `pr-crates` | CI PR/push | Connectors, control, ops, runtime, storage and SQL, with per-package features |
| `pr-gateway` | CI PR/push | Gateway and TestContainers feature, requires Docker |
| `pr-postgres-differential` | CI PR/push | Default-feature PostgreSQL differential target, including the ignored reference corpus, requires Docker |
| `pr-sim-unit` | CI PR/push | Simulation library |
| `pr-sim` | CI PR/push | Explicitly assigned ordinary simulation integration targets |
| `pr-ha` | CI PR/push | Separate HA suite; Docker hard prerequisite |
| `nightly-seeds` | daily schedule, dispatch | 100,000-seed chaos, SQL fuzz seed harness, longer simulation soaks |
| `nightly-docker` | daily schedule, dispatch | Docker simulation/MinIO/recovery and resource tests |
| `release-docker` | tag or explicit dispatch | Real rolling-upgrade cases; requires two configured distinct image references |
| `daily-resource` | separate daily resource-soak workflow | Four-hour MinIO resource-lifecycle gate, independently reported (the existing dedicated workflow owns it) |

The PR jobs are called in `ci.yml`, also on its nightly schedule and explicit dispatch. Seeded, Docker and release suites are called in `test-accounting-expensive.yml`; the existing specialized resource, real-cluster chaos, and fuzz workflows remain independent reports. Release image variables `ROCKSTREAM_ROLLING_N_IMAGE` and `ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE` must reference distinct, available images. If absent, the release job fails and the release claim remains blocked. The specialized real-cluster chaos job retains its per-cell SLO gate; no retry or non-blocking bypass is added.

**Mandatory PR safety checks, collectively:** `Check` (quality + five test shards + default-feature PostgreSQL differential accounting + lane selection), `Control-plane HA smoke`, `Formal verify (M1–M4, M6–M7)`, both `Verus verify and qualify (VS6)` platform checks, `Nexmark regression gate`, `Performance-regression gate (ops/storage/runtime/control)`, and `Docker image (release binary)`. The `Check` aggregator depends on these jobs and fails if any dependency is failed, skipped, cancelled, or missing. **Do not use an older green `Check` run as evidence that independently scheduled checks have passed.** Check the individual job conclusions and recorded lane selection for the PR commit. Existing main ruleset check names remain unchanged; workflow, composite-action and CI policy changes require maintainer ownership review.

## Consolidated PR execution (#199)

The gateway feature matrix is intentional: `pr-gateway` runs all owned targets once with `testcontainers` (the package's only declared feature, equivalent to the previous `--all-features` conformance pass). `pr-postgres-differential` exclusively owns `pg18_differential_conformance_tests` with default features, including `--include-ignored`. Thus default-feature reference coverage remains, while that target no longer also runs in the TestContainers pass. The eight `conformance_targets` emit raw libtest events to `target/pgwire-test-results.json` from their accounted invocation. `conformance_doc_tests` runs last and consumes the freshly cleared and regenerated report. Both raw conformance events and exact accounting JSON are uploaded even on failure; missing artifacts fail visibly. No case assertion or ignored-case waiver is weakened.

`Docker image (release binary)` builds and smoke-tests one production image per CI run, tagged and labelled with the candidate `GITHUB_SHA`. It uploads an immutable run-local artifact and exposes its Docker image ID. Gateway and HA consumers download that artifact, require Docker, verify both image ID and revision label, then create the test harness's required `rockstream-tc-test:latest` alias. Each consumer records the verified revision and ID in `candidate-image.tsv`. Missing images, unavailable artifacts, or identity mismatches fail before tests. Other shards start independently of the image build. Separate release rolling-upgrade evidence still requires its two distinct configured image references.

Only `canonical_type_tests` and `arrangement_spec_isolation_tests` use four libtest threads. These targets operate on local in-memory values with no services, environment mutation, shared ports or files; their exact terminal case identities, outcomes and captured output matched one serial execution and five four-thread repetitions locally. All other targets, including gateway, HA, global-state tests and Docker tests, remain serial. New parallel targets require explicit ownership and a bounded thread count in the existing matrix. Reports record thread counts, target execution seconds and package build seconds (the latter is repeated per target, so count it only once per package).

## Qualification lanes

| Lane | Benchmarks and both Verus platforms | Correctness and release evidence |
| --- | --- | --- |
| PR, documentation only | Explicit exclusion recorded in `ci-selection.json`; required check jobs succeed with an exclusion message | All ordinary correctness, accounting and image checks remain mandatory |
| PR, code/proof/dependency/performance/CI or unknown paths | Full benchmark gates and both platform qualification matrices before merge | Existing deterministic correctness/regression tests remain mandatory |
| Targeted/on-demand | Dispatch `CI` on the candidate branch/ref to request full qualification | Does not substitute for a different revision's required PR checks |
| Nightly | `CI` runs full benchmark and Verus qualification on the scheduled main revision | Existing seeded/Docker/soak workflows retain revision-bound failures and reproducible seed/input reporting |
| Release tag | Full benchmark and Verus qualification | Separately required rolling-upgrade, soak and release sign-off artifacts remain required |

Only PRs exclusively changing `README.md`, `CHANGELOG.md`, or Markdown files under `docs/` qualify for the documentation exclusion. Empty diffs, renames out of code (both old/new paths are inspected), unknown paths, diff errors, and missing/invalid policy outputs cannot exclude mandatory work. Main pushes, tags, nightly and manual runs always request full qualification. A successful exclusion check is selection evidence, not proof that qualification executed. The active main ruleset does not need a remote mutation: required check names stay stable, and `Check` still rejects skipped mandatory jobs. Bugs discovered by nightly or qualification runs should carry their revision and reproducible seed/input into deterministic PR regression tests where feasible.

## Cost evidence

The successful [baseline run 39044539905](https://github.com/trickle-labs/rockstream/actions/runs/39044539905) used revision `bbd608f0174b82cf53dfc6edb80df966154e1cb1` (the issue's source baseline is `2db59e45`). Its job timestamps total **462.75 runner-minutes**, with **159.65 minutes** from workflow creation to completion and **324.25 summed job queue-minutes** (job creation to start, including scheduling delays; this is not wall-clock queue time or billable usage). Gateway took **105.60 minutes**: image build **20.30**, the redundant conformance pass **26.25**, and accounting build/execution **56.65**. Its accounting artifact records 979 passing case IDs across 133 targets, including 11 differential cases transferred to exclusive default-feature ownership here. HA and release-image jobs separately spent 19.93 and 20.48 minutes building the same Dockerfile.

These are measured baseline costs, not measured savings. Image transfer, cache state and the new image dependency can affect latency. The PR's hosted run must supply the candidate measurement before claiming end-to-end savings: compare job creation/start/completion timestamps, Cargo `Finished` compilation timings in logs, per-target execution timings, and the union of exact case IDs/outcomes in gateway and differential accounting artifacts. The union must retain all baseline IDs; feature changes are the intentional differential ownership change above. Preserve run URLs and account for added/removed tests if main advances. Raw baseline libtest output did not separate target execution time, so do not infer that split from aggregate accounting duration. No GitHub rate-limit improvement is claimed.

### Explicit prose-only CI scope (partial CI-cost optimization)

`ci-policy` now shares the conservative path allowlist in `scripts/ci-scope.py`.
Only a pull request whose **every** changed file is allowlisted prose can use
lightweight CI: `AGENTS.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`,
`ROADMAP.md`, `NEW_ROADMAP.md`, `.github/copilot-instructions.md`, or
Markdown files under `.github/prompts/`, `.github/skills/`,
`docs/agents/`, `docs/implementation-plans/`, and `sign-offs/`.
Empty/unknown/mixed changes, ordinary documentation with executable examples,
all source, checker and CI file edits, and non-PR events select full CI.

For prose only, `Check quality` still runs documentation, policy, link,
sign-off, admission and checker self-tests; Rust compilation, Cargo inventory,
gateway/HA/Docker/Postgres, and mandatory execution suites are excluded.
The two required Verus platform jobs and the Nexmark/performance jobs report
explicit exclusion rather than a fabricated verification result. The `Check`
aggregator reads the complete dependency results and the selected lane:
only the named prose exclusions may be skipped; a failed, cancelled, missing,
or unexpectedly skipped job always fails. `CI scope: prose` reports
**no execution or release qualification evidence** for excluded jobs.
The original `ci-policy` candidate revision artifact and fail-closed
source classification remain in place.

Reproduce the negative gates with
`python3 -B scripts/ci-scope.test.py` and
`python3 scripts/ci-policy.test.py`. This does not relax branch protection
or the frozen release-process qualification required for #132.

**Separate scheduled/release safety reports:** `Test Accounting — Expensive Suites` (`Nightly seeded simulation accounting`, `Nightly Docker test accounting`, `Release rolling-upgrade test accounting`); `Simulation Soak`; `Long-Duration Resource Soak`; `Real Cluster Chaos Soak`; `Coverage`; `Scheduled Fuzz Soak`. These may not run on every PR and are **not implied** by green PR checks. Their own red conclusions block any safety or release claim requiring them; record their run URLs and evidence artifacts explicitly in release sign-off. The issue does not authorize claiming formal, HA, or release proof from absent/skipped jobs.

## Boundaries

The libtest accounting report proves that tests were selected and reported results, **not** that a test's assertions faithfully exercise production behavior (see [test-suite audit](test-suite-audit-2026-09-21.md)). Benchmarks, doctests, cargo-fuzz engine execution, FizzBee, Verus, and long-lived soak reports are tracked by their separate existing tools; they cannot be inferred from this test-case manifest. A test that silently early-returns without printing a skip cannot be proven to execute its claimed scenario by libtest alone: such tests need explicit scenario assertions and fail-closed prerequisites. CI sets `ROCKSTREAM_REQUIRE_DOCKER=1` on Docker-owned test suites and preflights the daemon.
