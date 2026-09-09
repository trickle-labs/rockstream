# Implement v0.61 through v0.74

Status: Planned. These documents specify future work. They do not establish
that a capability exists or that a release has passed qualification.

[ROADMAP.md](../../ROADMAP.md) is the scope authority. This directory turns its
milestones into implementation steps, documentation obligations, and checkable
exit criteria. The older `NEW_ROADMAP.md` and `NEW_IMPLEMENTATION_PLAN.md` remain
historical references. Their version assignments do not govern this work.

## Select the plan

Use the matching document for each milestone, including the five performance
patches. A version ending in `.0` uses its minor-version plan, so `v0.61.0`
uses `v0.61.md`. Other patches retain their full version number.

| Version | Plan | Required predecessor work |
|---|---|---|
| v0.61 | [Golden path and project tooling](v0.61.md) | v0.60 product truth |
| v0.61.1 | [External performance baseline](v0.61.1.md) | v0.60; collect alongside v0.61 |
| v0.61.2 | [Aggregate delta reduction](v0.61.2.md) | v0.61.1 measurements before accepting a performance claim |
| v0.62 | [Configuration and node lifecycle](v0.62.md) | v0.61 |
| v0.62.1 | [Worker memory budgets](v0.62.1.md) | v0.62 NodeConfig and v0.61.1 measurements |
| v0.63 | [Durable catalog](v0.63.md) | v0.62 |
| v0.64 | [SQL execution integrity](v0.64.md) | v0.63 |
| v0.65 | [Standalone recovery, backup, and restore](v0.65.md) | v0.64 |
| v0.65.1 | [Concurrent maintenance and group commit](v0.65.1.md) | v0.65 recovery and v0.62.1 budgets |
| v0.66 | [Management plane](v0.66.md) | v0.65 and v0.62 lifecycle |
| v0.67 | [Direct distributed data plane](v0.67.md) | v0.66 and the earlier performance gates |
| v0.67.1 | [State beyond RAM](v0.67.1.md) | v0.67 and v0.62.1 budgets |
| v0.68 | [Durable distributed lifecycle](v0.68.md) | v0.67; v0.67.1 for large-state migration |
| v0.69 | [PostgreSQL CDC](v0.69.md) | v0.68 and all earlier performance gates |
| v0.70 | [Kafka](v0.70.md) | v0.68 and all earlier performance gates |
| v0.71 | [Operational observability](v0.71.md) | v0.69 and v0.70 |
| v0.72 | [Resource control and capacity](v0.72.md) | v0.71 and earlier budget and performance patches |
| v0.73 | [Security coherence](v0.73.md) | v0.72 |
| v0.74 | [Upgrade and compatibility](v0.74.md) | v0.73 |

Follow logical dependencies rather than numeric adjacency. Kafka and PostgreSQL
CDC can proceed independently after v0.68. Do not delay the baseline until v0.72
or make v0.61 depend on its own performance patches. v0.75 and research programs
remain outside this plan set. No plan schedules v1.0.

## Carry requirements into implementation

1. Read the version's roadmap section, its plan, and the roadmap's common
   definition of done. Check predecessor evidence before starting dependent work.
2. Trace each proposed change through the current public dispatch, runtime, and
   storage paths. The code references in these plans are starting points at
   revision `5ea1bc3`; recheck them when implementation starts.
3. Copy every exit criterion ID into the working implementation plan at
   `.claude/<version>-plan.md`. Map every implementation step and documentation
   obligation to those IDs. Preserve the committed plan as the scope contract.
4. For each ID, name the implementation files, the public behavior to document,
   the positive and failure tests, and the evidence artifact to collect.
   Existing tests are regression coverage until they exercise the whole claim.
5. Resolve choices that affect formats, protocols, resource limits, or performance
   targets before writing dependent code. Record the chosen values in the plan
   and versioned configuration or workload profile. An unresolved choice blocks
   its dependent criterion.
6. Implement each slice with its tests and documentation. Update the capability
   manifest and generated references only when the public path works. Mark
   unshipped behavior as planned or experimental in user-facing documentation.
7. Compare the implemented behavior with every roadmap requirement and every
   plan ID during proof. A successful test command alone does not establish
   that an unexercised requirement works.

The [implementation prompts](../../.github/prompts/implement-version-orient.prompt.md)
carry these requirements through orientation, planning, implementation, proof,
and sign-off. Working files in `.claude` do not replace the committed plans or
the final evidence in `sign-offs`.

If work needs splitting under roadmap section 3, record the owning patch and
dependencies before implementation. Keep the parent milestone incomplete until
its mandatory criteria pass. Do not drop, rename, or weaken criteria to obtain
a passing sign-off. Scope changes require a corresponding roadmap and plan diff.

## Collect evidence that proves the claim

Every version must satisfy the [common definition of done](../../ROADMAP.md#4-common-definition-of-done)
in addition to its own exit criteria.

- Exercise public behavior through a release-mode binary or container and a
  normal client. Record the revision, binary digest, commands, configuration,
  storage backend, and actual process topology.
- Assert complete expected responses, error codes, command status, schemas, and
  result multisets. Preserve duplicate multiplicities and NULLs. Compare ordered
  results in order when the contract promises ordering. Counts, selected rows,
  grep matches, and success exit codes alone cannot prove result correctness.
- Normalize only documented nondeterministic fields, such as generated request
  IDs, and assert their relationships. Do not normalize incorrect data away.
- For each user-visible operation, inject a meaningful failure and verify its
  full response and state effects. Reject fake topology, fixture timing,
  constructed success, and mock-only qualification.
- For persistence claims, destroy the process and reconstruct it from durable
  storage. Compare all committed data and metadata with the expected result.
  Exercise LFS and MinIO where the durability path supports both. An object
  reconstruction test supplements this proof but does not replace it.
- For coordination changes, retain unit tests, formal models, and seeded
  simulation alongside real multi-process tests. Commit regression seeds for
  discovered failures. Model and simulation results alone do not qualify a
  public distributed operation.
- For each queue, cache, waiter registry, scan, or buffer, name the byte or item
  limit, fill metric, admission point, and overflow behavior. Test pressure,
  cancellation, and recovery without unbounded accumulation.
- Version changed persistent formats and protocols. Prove supported combinations,
  rejected combinations, interrupted migration, and the documented rollback
  boundary before release.

Record failures and unavailable infrastructure as failed or blocked evidence.
Do not count skipped, filtered-out, ignored, zero-test, or synthetic runs as
passing proof. Keep raw logs as artifacts and report only test summaries unless
a test fails. Preserve the test process exit status when filtering output.

## Measure performance at the owning milestone

Extend the existing [R1 runner contract](../../benchmarks/r1-local/README.md).
Freeze offered load, duration, warmup, repetition count, payload size, state size,
view count, durability mode, separate p99 targets, and regression tolerances
before comparing candidates. Store a new profile instead of rewriting frozen
evidence. Report repeated-run variation and explain failures to meet a target.

| Required experiment | Milestone that completes the gate |
|---|---|
| Measured standalone baseline and concurrent ingestion with queries | v0.61.1; rerun for each performance change |
| One-key updates at 1K, 100K, and 10M groups | v0.61.1 where executable; v0.67.1 completes cases that need state beyond RAM |
| One and twenty compatible views | v0.61.1 baseline, v0.65.1 concurrency, v0.67 shared execution |
| 1, 2, 4, and 8 actual workers | v0.67 |
| State beyond RAM through compaction and restart | v0.67.1 |
| Overload, worker loss, and migration | v0.62.1 pressure, v0.67 worker loss, v0.68 migration |
| Complete applicable matrix and reproducible capacity report | v0.72 |

List unavailable baseline cells with their owning milestone. They remain blocked
until measured; they do not support a scaling or large-state claim. All earlier
performance gates must pass before connector expansion.

Measure read latency, durable commit latency, and event-to-correct-committed-result
freshness separately. Keep scheduled offered load independent of completion and
include generator delay, backpressure, errors, and timeouts. Compare full results
with an independent oracle at the relevant committed epoch.

Report per-change state writes, intermediate rows, network bytes, object-store
requests, and physical flushes per epoch. Include RSS, queue age, control-node
load, storage backlog, and infrastructure limits. Local results establish local
performance only. Cloud cost claims need measured deployments, dated prices,
and every infrastructure component in the roadmap's cost formula.

## Sign off without losing obligations

For each criterion, write a checked evidence line in `sign-offs/<version>.md`
only after its acceptance condition passes. Use the exact ID followed by a
colon, for example `- [x] V061-01:`, then link the implementation, documentation,
test, and recorded result. Use unchecked lines for incomplete criteria.

The sign-off must also record common-definition-of-done evidence, the tested
revision and artifact identity, exact reproduction commands, supported backends,
and any explicit unsupported cases. Link raw evidence from a committed manifest
or durable CI artifact. Machine-local paths and a claimed green status are
insufficient. Include performance profiles and samples for measured gates.

Run `rtk proxy bash scripts/check-exit-criteria.sh` and its self-test before
marking a version complete. The checker rejects a Done version with missing
plan IDs or unchecked sign-off items. Reviewers must still verify that the
linked evidence proves each condition; the checker cannot determine that from
prose. Change the plan status and active roadmap overview to Done only after
all required evidence passes. This planning commit leaves every plan Planned.
