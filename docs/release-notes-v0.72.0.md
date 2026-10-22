# RockStream v0.72.0 release notes

**Scope:** Pragmatic v0.72.0 workload controls and safe interactive operations. This document describes the candidate's supported behavior, not an assertion of production-scale capacity qualification. The release exists only after an authorized maintainer publishes a signed `v0.72.0` tag pointing to the fully verified candidate commit.

## Shipped changes

- **Bounded workloads:** Prospective resource admission, zero-memory-limit rejection, quota ownership/release safeguards, source/exchange credit and queue limits, cancellation cleanup, and foreground progress under competing load. Hard limits return errors rather than silently dropping work.
- **Ordered durable progress:** Bounded adaptive epoch settings and worker validation; fail-closed operator/frontier processing so a failed epoch cannot silently pass later results downstream. Durable commit, restart/fencing and competing workload regressions are retained.
- **Authorized read-only queries:** SQL authorization, bounded query and cursor producers, checked complete-result semantics, cancellation/timeout cleanup, and normal PostgreSQL-client coverage. The standalone DataFusion SELECT fallback admits at most **10,000 result rows, 8 MiB retained Arrow-array bytes, 5 seconds, and 16 concurrent producers**. Ordinary local ShardDb-backed SELECT source scans have a **64 MiB scanned-key/value and 1,000,000-row ceiling**.
- **Committed subscriptions:** Bounded authorized snapshot/replay/update publication, whole committed epochs and explicit resnapshot when retained replay history is unavailable.
- **Revision-checked workload control:** Reject conflicting revisions and memory-cap decreases below live usage; report requested and effective/pending state distinctly rather than claiming asynchronously applied changes have taken effect.
- **PgWire decoder hardening:** A compatible security patch to the pinned `pgwire 0.28.0` dependency rejects invalid frame lengths, truncated CopyData and Execute bodies, and malformed simple-query strings without unchecked reads. Exact crash-input, valid-boundary, fuzz and normal-client TCP tests are included.

## Operational limits and compatibility

- A full-prefix `SELECT ... LIMIT n` may **fail** when the source scan exceeds its envelope; the gateway must not return a valid-looking truncated result. Query pushdown/pagination across every large relation is not supplied.
- DataFusion's returned batches are bounded at the fallback producer. This does **not** prove that all internal operator intermediates are resource bounded. Distributed query-time execution has separately scoped limits.
- Subscription replay is retained only within its bounded history; clients must resnapshot when replay is unavailable.
- The v0.72 quota, memory and scheduling tests are practical regressions, **not** measured guarantees of p99 latency, 8-worker scaling, beyond-RAM recovery, or cloud cost. No universal production qualification or all-configuration memory/RSS guarantee is asserted.
- Existing v0.71 storage/checkpoint deployments should be backed up and exercised through recovery before upgrades. The release does not introduce a new migration guarantee.
- This is not the complete typed console API or a claim that every PostgreSQL message is supported. Malformed-frame coverage is bounded by the included regressions and decoder fuzzing.

## Verification and reproducibility

Release gate: full normal GitHub CI on the **exact tagged commit**, no required failures, including Linux/macOS Verus, quality/security, gateway/workspace/crate tests, Docker binary, unchanged performance/Nexmark gates and the `Check` aggregator. Verify the normal-client, overload, cancellation, competing-workload and crash/restart smoke using:

```bash
bash scripts/run-v072-pragmatic-smoke.sh
```

That script records the candidate SHA, exact per-case logs and an execution summary under `target/v072-release-smoke/<sha>/`; a locally generated report is **not** a cloud or release-process performance benchmark. Preserve current decoder fuzz and release qualifications separately; do not turn previous failed older-head runs into claims about the final candidate.

The broader historical eight-workload / 16-cell frozen matrix, exact Apple M5 Pro capacity comparisons, fixed 30-minute fault soaks and cloud-cost studies were explicitly retired as **mandatory v0.72 shipping gates** by [issue #132's 2026-10-19 scope override](https://github.com/trickle-labs/rockstream/issues/132). Their absence must not be described as passing them.

## Source and tracking

- Parent pragmatic acceptance: [#132](https://github.com/trickle-labs/rockstream/issues/132); bounded interactive reads [#135](https://github.com/trickle-labs/rockstream/issues/135); decoder safety [#227](https://github.com/trickle-labs/rockstream/issues/227).
- Principal merged PRs: [#222](https://github.com/trickle-labs/rockstream/pull/222), [#223](https://github.com/trickle-labs/rockstream/pull/223), [#225](https://github.com/trickle-labs/rockstream/pull/225), [#226](https://github.com/trickle-labs/rockstream/pull/226), [#228](https://github.com/trickle-labs/rockstream/pull/228), [#229](https://github.com/trickle-labs/rockstream/pull/229).
- References: [query and cursor limits](v072-cursor-admission.md), [fallback limits](v072-datafusion-fallback-limits.md), [local scan limits](v072-local-select-bounds.md), [release governance](release-governance.md), and [smoke instructions](testing/v072-pragmatic-release-smoke.md).
