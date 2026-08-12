# Proposal: Reduce Rockstream to Essential Sources and Sinks

## Status

**Accepted (2026-08-12).** Scheduled as roadmap versions **v0.52.3 – v0.52.5**
([NEW_ROADMAP.md](NEW_ROADMAP.md) Phase 16.6), which map to this document's
migration phases as follows:

| This document | Roadmap version |
| --- | --- |
| Phase 1 (announce) + Phase 2 (remove frontend exposure) | v0.52.3 |
| Phase 3 (delete implementations) + Phase 4 (delete dependencies) + Phase 5 (delete dead abstractions) | v0.52.4 |
| Tests (redirected matrix) + Extensibility Policy | v0.52.5 |

[ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) carries the matching
amendment: the removed connectors move from `Maintain` tier to removed, and
PostgreSQL CDC and Kafka become the only connectors in the v1 contract.

## Summary

Rockstream should substantially reduce its connector surface and retain only the sources and sinks that directly support its core purpose as a lean, cloud-native incremental view maintenance system.

The supported external connector set should become:

| Direction | Connector      | Status   |
| --------- | -------------- | -------- |
| Source    | PostgreSQL CDC | **Keep** |
| Source    | Kafka          | **Keep** |
| Sink      | Kafka          | **Keep** |

All other source and sink implementations should be removed from the Rockstream codebase rather than merely deprecated, hidden behind configuration, or left in maintenance mode.

This proposal removes:

* S3 source
* HTTP/webhook source
* Object-store sink
* Iceberg sink
* Delta Lake sink
* Generic cold-tier sink infrastructure
* Connector-specific cold-tier garbage collection
* External lakehouse catalog registration
* Supporting code that exists solely for the removed connectors

The goal is not simply a smaller advertised feature set. The goal is a **materially smaller implementation, dependency graph, test surface, operational model, and long-term compatibility commitment**.

---

## Motivation

Rockstream's value comes from maintaining correct, fresh, durable materialized views over changing data.

Connector breadth is not a core differentiator.

Every connector retained in the repository carries permanent costs:

* implementation complexity
* dependencies
* compile time
* CI time
* integration-test infrastructure
* security surface
* failure semantics
* configuration surface
* observability requirements
* documentation
* compatibility commitments
* cognitive load for maintainers
* additional paths that must remain correct through recovery and upgrades

A connector should therefore remain in Rockstream only when its value clearly exceeds these costs.

PostgreSQL CDC and Kafka satisfy that requirement. The other current sources and sinks do not.

---

## Product Boundary

After this change, Rockstream's external data-flow model should be intentionally narrow:

```text
PostgreSQL ──CDC──┐
                  ├──> Rockstream IVM ──> PostgreSQL wire/query interface
Kafka ────────────┘
                         │
                         └──────────────> Kafka
```

This gives Rockstream three essential integration paths:

1. **PostgreSQL CDC → Rockstream**

   The primary operational-database ingestion path.

2. **Kafka → Rockstream**

   The general-purpose streaming ingestion path and escape hatch for systems that are not PostgreSQL.

3. **Rockstream → Kafka**

   The general-purpose streaming output path for downstream systems.

The PostgreSQL wire protocol remains Rockstream's native query and application interface and is not considered an optional connector.

Rockstream's internal use of object storage for durable state also remains unchanged. Removing an object-store **sink** must not be confused with removing object storage from Rockstream's storage architecture.

---

# Keep

## PostgreSQL CDC Source

**Decision: Keep as core.**

PostgreSQL CDC is highly aligned with Rockstream's purpose.

It allows Rockstream to sit directly alongside operational PostgreSQL databases and continuously maintain derived state as source data changes.

PostgreSQL is also already central to Rockstream's user-facing model through pgwire. Supporting PostgreSQL both as an application interface and as a CDC source creates a coherent product instead of introducing another unrelated ecosystem.

### Required guarantees

PostgreSQL CDC should receive strong compatibility and testing guarantees around:

* logical replication offsets
* snapshot-to-stream handoff
* insert/update/delete semantics
* recovery from committed LSNs
* bounded buffering and backpressure
* WAL lag behavior
* replication-slot failure
* publication failure
* malformed input handling
* exactly-once interaction with Rockstream epochs

This should be one of the best-tested parts of Rockstream.

---

## Kafka Source

**Decision: Keep as core.**

Kafka provides broad ecosystem reach without requiring Rockstream to implement a large connector marketplace.

Users can bridge many external systems into Kafka using existing infrastructure such as:

* application producers
* Debezium
* Kafka Connect
* CDC platforms
* event buses
* custom adapters

Rockstream therefore gains access to a very large integration ecosystem while maintaining only one streaming protocol.

### Required guarantees

The Kafka source should remain focused on:

* consumer-group semantics
* partition offsets
* deterministic recovery
* bounded buffering
* backpressure
* rebalance behavior
* transactional offset integration where applicable
* event-time/watermark semantics
* crash recovery

Rockstream should resist expanding Kafka support into a general Kafka-management feature set.

---

## Kafka Sink

**Decision: Keep as the only external sink.**

Kafka is sufficient as Rockstream's primary external streaming output.

It allows materialized results or changes to be consumed by applications and downstream infrastructure without requiring Rockstream itself to understand each destination.

Kafka therefore serves as the equivalent of a universal adapter boundary.

### Required guarantees

The Kafka sink should retain strong guarantees around:

* epoch atomicity
* transactional writes
* exactly-once delivery
* recovery after uncertain commit outcomes
* checkpoint coupling
* bounded staged transactions
* broker timeout handling
* idempotency

Keeping one sink also allows Rockstream to invest deeply in its correctness instead of spreading effort across multiple sink semantics.

---

# Remove

## S3 Source

**Decision: Remove.**

Direct object-store ingestion is useful, but it is not essential to Rockstream's core continuous-IVM workflow.

An S3 source introduces a separate batch/file ingestion model with concerns such as:

* object enumeration
* object ordering
* file offsets
* file formats
* object mutation semantics
* eventual consistency assumptions
* large-file buffering
* credential handling
* object-store client dependencies

These semantics are substantially different from continuous PostgreSQL CDC and Kafka ingestion.

### Replacement path

Users that need to bootstrap data from S3 should use an external loader that writes the initial dataset through:

* PostgreSQL/pgwire, or
* Kafka

Rockstream should not own the file-import layer.

---

## HTTP/Webhook Source

**Decision: Remove.**

HTTP ingestion appears small at the interface level but creates a disproportionately large product surface.

A production webhook endpoint requires Rockstream to own:

* HTTP authentication
* request limits
* payload validation
* content formats
* delivery identities
* deduplication
* retry behavior
* timeout behavior
* rate limiting
* queueing
* request lifecycle semantics
* externally exposed attack surface

None of these are central to IVM.

### Replacement path

Webhook ingestion should live outside the Rockstream server.

A lightweight adapter can translate:

```text
HTTP webhook → Kafka
```

or:

```text
HTTP webhook → PostgreSQL
```

Such an adapter may exist as a separate project if useful, but it should not be part of Rockstream's core runtime.

---

## Object-Store Sink

**Decision: Remove.**

A generic object-store sink is more defensible than the lakehouse-specific sinks, but it is still unnecessary once Kafka is established as Rockstream's universal egress boundary.

Keeping it would require Rockstream to retain another set of sink semantics involving:

* object naming
* conditional writes
* partial uploads
* commit markers
* pending/final object transitions
* object-store-specific recovery
* credentials
* backend compatibility

These concerns are not needed for the minimal product.

### Important distinction

This decision applies only to an **external sink**.

Rockstream should continue using object storage wherever required for its own authoritative durable state, checkpoints, SlateDB state, spill, recovery, or internal storage architecture.

---

## Iceberg Sink

**Decision: Remove.**

Iceberg moves Rockstream toward becoming a lakehouse integration platform.

Supporting Iceberg properly creates obligations around:

* Iceberg metadata
* manifests
* snapshots
* transaction semantics
* partition specifications
* catalog integration
* format versions
* external-engine compatibility
* retention
* garbage collection

That is a major adjacent product domain.

Rockstream should not own it.

### Replacement path

Users that need Iceberg output should consume Rockstream's Kafka output and use a dedicated Kafka-to-Iceberg system.

This keeps responsibilities correctly separated:

```text
Rockstream → Kafka → Iceberg writer
```

Rockstream maintains views.

The downstream system maintains Iceberg.

---

## Delta Lake Sink

**Decision: Remove.**

Delta Lake has the same strategic problem as Iceberg.

It introduces a parallel lakehouse implementation with its own:

* transaction-log semantics
* storage behavior
* partitioning
* recovery rules
* version compatibility
* external-engine expectations

Maintaining both Iceberg and Delta compounds the problem.

### Replacement path

Use:

```text
Rockstream → Kafka → Delta writer
```

A downstream sink designed specifically for Delta Lake should own Delta Lake compatibility.

---

# Remove Associated Cold-Tier Infrastructure

Removing Iceberg and Delta should be treated as removal of the **feature family**, not just two public types.

Code that exists only to support cold-tier lakehouse exports should be deleted as well.

Candidates include:

* `cold_tier_sink`
* cold-tier snapshot scheduling
* cold-snapshot GC
* lakehouse-specific retention logic
* lakehouse partition-spec handling
* external catalog registration
* Glue catalog support
* Hive catalog support
* REST catalog support
* DuckLake catalog support
* Iceberg/Delta-specific metadata helpers
* tests dedicated solely to these capabilities
* documentation for the removed sink surface

Do not leave unused abstractions behind for hypothetical future connectors.

The objective is code deletion.

---

# Connector Crate After Simplification

The ideal public surface of `rockstream-connectors` should approximately consist of:

```text
source_connector
source_epoch
source_runtime

postgres_cdc
kafka_source

sink_connector
kafka_sink
```

Supporting modules should remain only where they are used by these capabilities.

If a module becomes unnecessary after the deletions, remove it.

The resulting crate should describe what Rockstream actually intends to support rather than preserving scaffolding for abandoned breadth.

---

# Dependency Reduction

The removal should be followed by an explicit dependency audit.

Dependencies currently justified solely by removed connector functionality should be eliminated.

Likely candidates include, subject to workspace-wide verification:

* `iceberg`
* `deltalake`
* `aws-sdk-s3`
* `aws-config`
* `csv`

Potential removals from `rockstream-connectors`, depending on remaining usage:

* `parquet`
* connector-facing `object_store` functionality
* related catalog/network libraries

Dependencies must not be removed merely because a connector no longer uses them if Rockstream's storage engine or another core subsystem still legitimately requires them.

The goal is to reduce the **effective dependency graph**, not to force artificial architectural changes.

---

# SQL and Configuration Surface

All user-visible references to removed connectors should also be removed.

This includes any DDL, configuration, documentation, error messages, catalog fields, and CLI options for:

* S3 sources
* HTTP/webhook sources
* object-store sinks
* Iceberg sinks
* Delta sinks
* cold-tier configuration
* external lakehouse catalogs

Rockstream should not retain parser or catalog support for features that no longer exist.

Failing with "unsupported" forever is still a product surface.

Delete the surface instead where compatibility constraints permit.

---

# Tests

Tests should be simplified alongside implementation code.

Remove tests dedicated exclusively to:

* S3 source ingestion
* webhook ingestion
* Iceberg recovery
* Delta recovery
* generic object-store sink behavior
* cold-tier GC
* catalog registration
* lakehouse partition layouts

Testing effort should be redirected into deeper coverage of the three remaining connectors.

The connector test matrix should emphasize:

### PostgreSQL CDC

* snapshot/CDC transition
* every mutation type
* restart at every commit boundary
* WAL lag
* malformed replication records
* slot loss
* publication loss
* backpressure
* long-running recovery scenarios

### Kafka source

* consumer rebalance
* partition expansion
* offset recovery
* broker interruption
* bounded buffers
* duplicate prevention
* source/sink transactional interaction

### Kafka sink

* crash before commit
* crash during commit
* uncertain broker response
* transaction timeout
* recovery rerun
* duplicate prevention
* checkpoint coupling

A smaller matrix should permit substantially stronger guarantees.

---

# Migration and Compatibility

Because this is an intentional scope reduction, compatibility should not prevent deletion indefinitely.

Recommended process:

## Phase 1 — Announce removal

Clearly document that Rockstream is narrowing its supported integration boundary to:

```text
Sources: PostgreSQL CDC, Kafka
Sink:    Kafka
```

Explain that the decision is architectural rather than caused by temporary implementation gaps.

## Phase 2 — Remove frontend exposure

Remove creation/configuration of the affected sources and sinks from user-facing APIs.

Existing configurations should produce a clear migration error rather than silently doing nothing.

## Phase 3 — Delete implementations

Remove the connector implementations and their supporting modules.

## Phase 4 — Delete dependencies

Run a workspace-wide dependency audit and remove dependencies that no longer serve the retained product.

## Phase 5 — Delete dead abstractions

Review the connector traits and shared code for hooks that existed only because the removed connectors required them.

Simplify the contracts where possible.

Do not retain generalized abstractions merely to preserve theoretical extensibility.

---

# Extensibility Policy

Rockstream should not aim to become a connector marketplace again.

A new source or sink should enter the core repository only if all of the following are true:

1. It materially improves Rockstream's core IVM use cases.
2. Kafka or PostgreSQL cannot reasonably serve as the integration boundary.
3. There is demonstrated production demand.
4. The connector's failure and recovery semantics can meet Rockstream's correctness standards.
5. Its ongoing maintenance burden is acceptable.
6. Adding it is worth permanently expanding Rockstream's compatibility contract.

The default answer to a proposed connector should be:

> Implement it outside Rockstream using Kafka or PostgreSQL as the boundary.

---

# Non-Goals

This proposal does **not** remove or weaken:

* PostgreSQL wire protocol access
* ordinary SQL ingestion through pgwire
* Rockstream's internal object-storage-backed durable state
* SlateDB
* checkpoint storage
* spill-to-object-storage behavior
* disaster recovery
* source epoch machinery
* sink two-phase commit machinery required by Kafka
* deterministic simulation and recovery testing

It also does not prevent independent projects from providing bridges between Rockstream and other systems.

The boundary is deliberate:

**Rockstream owns IVM. External adapters own peripheral integration.**

---

# Expected Outcome

After this proposal, Rockstream has:

**2 sources**

```text
PostgreSQL CDC
Kafka
```

**1 sink**

```text
Kafka
```

Everything else is removed.

This makes the integration story small enough to state in one sentence:

> **Rockstream ingests operational changes from PostgreSQL or Kafka, continuously maintains SQL materialized views, serves them through PostgreSQL-compatible interfaces, and can publish derived streams to Kafka.**

That is the connector surface Rockstream should carry into v1.0.
