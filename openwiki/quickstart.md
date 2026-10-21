---
type: task-routing guide
title: RockStream task-routing quickstart
description: Find the canonical evaluation, operator, and contributor entry points, the Rust binary entry point, and the right system or workflow page for a change or incident.
tags: [quickstart, navigation, contributors, operations]
verified:
  - by: openwiki/0.7.2
    at: 2026-10-21T16:22:36.376Z
sources:
  - id: openwiki-source-8037e2358a2c4f9b2c722a11
    resource: repo://AGENTS.md
  - id: openwiki-source-651d1fb6c9e49916a916ab51
    resource: repo://Cargo.toml
  - id: openwiki-source-6fa2d8fa53a7a8e04a76d5d7
    resource: repo://crates/rockstream-cli/Cargo.toml
  - id: openwiki-source-863305bb9ecec1c7d8d2ee8e
    resource: repo://crates/rockstream-cli/src/lib.rs
  - id: openwiki-source-e4031bf4a19f095d6ab30cb9
    resource: repo://crates/rockstream-cli/src/main.rs
  - id: openwiki-source-058dede91e6d7d37ae09d225
    resource: repo://docs/adr/0001-documentation-navigation.md
  - id: openwiki-source-8658e96cd9d988d2235a8dcd
    resource: repo://docs/adr/0002-reference-compatibility.md
  - id: openwiki-source-29113916eb8ae3925f225a8b
    resource: repo://docs/contributors.md
  - id: openwiki-source-1453aa758a930efe85c45f2d
    resource: repo://docs/getting-started.md
  - id: openwiki-source-7a03f8483fdcd2072656c431
    resource: repo://docs/operator.md
  - id: openwiki-source-196170e31ff8ec60a116165b
    resource: repo://docs/README.md
  - id: openwiki-source-a695db9a98ee4a0146e5a91a
    resource: repo://docs/test-commands.md
  - id: openwiki-source-23775c3de52f3ab95a13cb8b
    resource: repo://README.md
  - id: openwiki-source-b7793decf9d7c9ba48e57e0f
    resource: repo://rust-toolchain.toml
generated: { by: "openwiki/0.7.2", at: "2026-10-21T16:22:36.376Z" }
---

# RockStream task-routing quickstart

RockStream maintains durable materialized views incrementally from changing data and serves committed results to PostgreSQL-compatible clients. Use this page to choose a task path, not as a replacement for the product tutorial or command reference.

## Start with your role

| You want to… | Canonical repository entry point |
|---|---|
| Evaluate the product | [Getting started](../docs/getting-started.md): the checked-in demo and persistent local-project transcripts |
| Operate a node or cluster | [Operator guide](../docs/operator.md): configuration, connectors, observability, recovery, and upgrades |
| Contribute code | [Contributor guide](../docs/contributors.md), then [CONTRIBUTING.md](../CONTRIBUTING.md) and the [test-command taxonomy](../docs/test-commands.md) |
| Maintain documentation or find a reference | [Documentation index](../docs/README.md) |

These persona pages are stable navigation entry points under [ADR 0001](../docs/adr/0001-documentation-navigation.md). The wiki adds an architectural reading path beneath them.

**Choose evaluation mode deliberately:** `rockstream demo` uses temporary storage and a simulated topology; it is not evidence of persistent deployment or distributed recovery. Follow the existing [local-project transcript](../docs/getting-started.md#create-a-local-project) for the persistent scaffold and its apply/verify workflow rather than adapting the demo into a deployment recipe.

## Rust and the actual executable

The workspace uses Rust edition **2021**, declares Rust **1.88**, and pins toolchain **1.88** with `rustfmt` and `clippy`. Use [Cargo.toml](../Cargo.toml) and [rust-toolchain.toml](../rust-toolchain.toml) as the setup authority.

Suggested repository commands use the RTK wrapper:

```bash
rtk cargo build -p rockstream-cli
rtk cargo run -p rockstream-cli -- --help
rtk cargo test -p rockstream-cli --test documentation_transcript_tests
```

These are suggestions, not checks executed while researching this page. The last command is the focused documentation-transcript target; it is not a release-qualification run. Choose other checks through [correctness and qualification](testing/correctness-and-qualification.md) and the repository taxonomy.

There is one operator executable, **`rockstream`**, built by `rockstream-cli`; node roles are flags, not separate server binaries. The [CLI manifest](../crates/rockstream-cli/Cargo.toml) binds it to [main.rs](../crates/rockstream-cli/src/main.rs). That entry point parses commands and dispatches to CLI implementations. For startup changes, begin at `Command::Start`: it constructs configuration overrides, resolves configuration, builds `StartOptions`, and calls `run_start` in [the CLI library](../crates/rockstream-cli/src/lib.rs). Do not treat the old “no-op node” module comment as the current startup contract.

`worker` and `frontier` startup require `--control`; a gateway can run without a control URL. For role composition, configuration, and shutdown behavior, continue to [deployment and configuration](operations/deployment-and-configuration.md), not a separate binary search.

## Route the task through the hierarchy

Start at [system ownership and execution boundaries](architecture/system-overview.md) when the responsible runtime is unclear. Then use the workflow or domain page below; follow its source and test links only after identifying the boundary you intend to change.

| Task or symptom | First page | Next boundary to examine |
|---|---|---|
| SQL request, transaction, read, or subscription behavior | [PostgreSQL requests](workflows/postgres-requests.md) | [SQL compilation and incremental execution](architecture/incremental-sql-engine.md) for planning or delta semantics |
| Add or change an incremental operator | [SQL engine](architecture/incremental-sql-engine.md) | [Correctness evidence](testing/correctness-and-qualification.md) before accepting new behavior |
| View creation, backfill, pause/resume, or schema evolution | [Views and schema](workflows/views-and-schema.md) | [Progress and checkpoints](concepts/progress-and-checkpoints.md) for publication boundaries |
| Durability, replay, checkpoint, or compaction change | [Storage and replay](concepts/storage-and-replay.md) | [Progress and checkpoints](concepts/progress-and-checkpoints.md); [recovery](operations/diagnostics-and-recovery.md) for an incident |
| Cluster routing, shard ownership, placement, or exchange | [Cluster and exchange](architecture/cluster-and-exchange.md) | [System overview](architecture/system-overview.md) for ownership boundaries |
| Connector input, source lag, or source restart | [Source ingestion](integrations/source-ingestion.md) | [Views and schema](workflows/views-and-schema.md) for schema-related failures |
| Kafka view output or delivery recovery | [Kafka view delivery](integrations/kafka-view-delivery.md) | [Progress and checkpoints](concepts/progress-and-checkpoints.md) for checkpoint coupling |
| Authentication, ACL, credential, or internal trust change | [Security boundaries](operations/security-boundaries.md) | [Deployment and configuration](operations/deployment-and-configuration.md) for rollout settings |
| Deployment, configuration, role, or shutdown change | [Deployment and configuration](operations/deployment-and-configuration.md) | [System overview](architecture/system-overview.md) for service composition |
| Operator command change, unhealthy node, stale view, or blocked operation | [Diagnostics and recovery](operations/diagnostics-and-recovery.md) | [Deployment and configuration](operations/deployment-and-configuration.md) if startup or settings are implicated |
| Select tests or judge release/recovery evidence | [Correctness and qualification](testing/correctness-and-qualification.md) | Return to the affected workflow to identify the observable invariant |

## Before changing behavior or intervening

- **Check the supported surface.** Use the [capability matrix](../docs/capability-matrix.md), [SQL support](../docs/reference/sql-support.md), and [known limitations](../docs/known-limitations.md). A PostgreSQL-compatible protocol is not a promise of full PostgreSQL SQL semantics. Kafka view delivery is Experimental, not an unrestricted export contract.
- **Separate liveness from useful progress.** The operator guide distinguishes health, freshness, durability, and capacity. For an incident, start with `rockstream health`, `rockstream status`, and `rockstream doctor`, then follow [diagnostics and recovery](operations/diagnostics-and-recovery.md); a live process alone does not prove healthy workloads.
- **Match proof to the claim.** Qualification fast/mock modes produce helper evidence with `release_qualified: false`. A passing recovery qualification is scoped to its named scenario, not every connector, HA topology, or client transport. Follow [correctness and qualification](testing/correctness-and-qualification.md) before presenting a check as release evidence.

## Keep reference URLs canonical

Use the generated canonical references: [CLI](../docs/reference/cli.md), [configuration](../docs/reference/configuration.md), [functions](../docs/reference/functions.md), [SQL support](../docs/reference/sql-support.md), [catalog](../docs/reference/catalog.md), [management API](../docs/reference/management-api.md), [metrics](../docs/reference/metrics.md), and [errors](../docs/reference/errors.md).

Older top-level reference URLs remain compatibility entry points under [ADR 0002](../docs/adr/0002-reference-compatibility.md); do not remove or repurpose them. For public surface changes, update the manifest or contributor source that owns generated content, then use the contributor guide’s matching checks rather than hand-editing generated reference output.
