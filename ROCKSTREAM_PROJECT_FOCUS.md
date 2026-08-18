# Rockstream: Recommended Project Focus After v0.51.26

**Status:** Proposed strategic direction  
**Baseline:** All roadmap versions through **v0.51.26 are implemented and signed off**  
**Audience:** Maintainers, contributors, design partners, and early adopters  
**Decision horizon:** The remaining path to v1.0 and the roadmap that follows it

---

## Amendment (2026-08-12): the connector surface is reduced, not maintained

[ROCKSTREAM_CONNEXTORS_CLEANUP.md](ROCKSTREAM_CONNEXTORS_CLEANUP.md) was
accepted and scheduled as **v0.52.3 – v0.52.5** ([NEW_ROADMAP.md](NEW_ROADMAP.md)
Phase 16.6). This amendment governs where it conflicts with the original text
below; everything it does not touch stands unchanged.

Rockstream's supported external integration boundary becomes:

```text
Sources: PostgreSQL CDC, Kafka
Sink:    Kafka
```

The S3 source, the HTTP/webhook source, the object-store sink, the Iceberg
sink, the Delta Lake sink, the generic cold-tier sink infrastructure,
connector-specific cold-tier garbage collection, and external lakehouse catalog
registration (Glue/Hive/REST/DuckLake) are **deleted from the codebase**, not
deprecated, feature-flagged, or left in maintenance mode.

**Why this reverses §4's "scope freeze on expansion, not a rollback" for one
subsystem.** The original document's reasoning — do not rip out proven
functionality merely to make the repository smaller — was correct as a general
rule and still governs everything else in Tier B. It fails for connectors
specifically, because `Maintain` tier is not a free classification. Every
retained connector is a permanent compatibility commitment, a dependency, CI
wall time, a security surface, a set of failure and recovery semantics, and a
row that v0.58's failure matrix and v0.59's unscoped reachability sweep must
both cover honestly. Paying that forever, for connectors whose integration
value Kafka already carries, is a worse trade than deleting them once — and
making the decision before v0.57 freezes the v1 contract means the contract is
written over the surface that actually exists.

**Three boundaries this amendment does not cross.** The PostgreSQL wire
protocol is Rockstream's native query and application interface, not an
optional connector, and is untouched. Rockstream's internal use of object
storage — SlateDB state, checkpoints, spill, recovery, disaster-recovery export
— is untouched; removing an object-store *sink* is not removing object storage
from the storage architecture. The sink two-phase-commit machinery and the M3
model that verifies it stay, now covering the single remaining sink.

**What this does not change.** Nothing else already shipped is removed.
Secondary indexes, hot-key virtual bucketing, autoscaling signals, advanced ad
hoc SQL, and advanced DML remain `Maintain` tier under the original rules. The
admission rule in §8 is unchanged and is now *machine-enforced* for connectors
by `scripts/check-connector-admission.sh` (v0.52.5).

---

## Executive summary

Rockstream should move forward as a **cloud-native incremental view maintenance (IVM) system** whose defining strength is not feature breadth, but the ability to keep SQL-defined materialized views correct, fresh, durable, and recoverable on object-storage-backed state.

That recommendation does **not** mean undoing the work already completed.

By v0.51.26, Rockstream has already implemented and verified a broad set of capabilities: distributed IVM, checkpointing and recovery, PostgreSQL wire access, real connector clients, cold-tier sinks, shard migration, hot-key handling, autoscaling signals, query-time SQL, operator state accounting, spill-to-SlateDB, async-runtime hardening, and long-lived registry cleanup. Those capabilities are now part of the project's implemented baseline.

The strategic question is therefore no longer:

> What should Rockstream remove so that it becomes small?

It is:

> **Where should Rockstream stop expanding, and which remaining investments are necessary to turn the existing system into an exceptionally reliable IVM product?**

The recommended answer is:

1. **Keep the capabilities already built.** Do not rip out proven functionality merely because it is outside the strategic core. *(Amended 2026-08-12: this holds for everything except the non-core connector surface, which is deleted at v0.52.3–v0.52.5 — see the amendment above.)*
2. **Stop treating every shipped subsystem as a new product pillar.** Existing lakehouse, advanced SQL, elasticity, and integration features do not automatically justify further expansion.
3. **Put future engineering disproportionately into correctness, failure semantics, operability, security, upgrades, disaster recovery, and production validation.**
4. **Apply a strict feature-admission rule to new breadth.** New SQL families, connectors, catalogs, transactional semantics, and governance languages should require concrete evidence that they materially improve the IVM product.
5. **Rebaseline the remaining roadmap from the v0.51.26 reality.** Preserve work that closes production-readiness gaps; defer or narrow work whose primary effect is turning Rockstream into a broader database or data platform.

The goal is not a smaller codebase at any cost. The goal is a **smaller strategic promise** backed by unusually strong implementation evidence.

---

## 1. Start from the actual v0.51.26 baseline

This strategy takes the current repository state as its starting point, not as a list of missing features.

Rockstream has already completed the difficult foundational work that a cloud-native IVM system needs:

- DBSP/Z-set-derived incremental semantics and an incremental-versus-batch correctness oracle.
- Single-shard and distributed operator execution.
- Frontiers, epochs, barriers, cluster checkpoints, fencing, and exactly-once recovery machinery.
- SlateDB-backed durable state on object storage.
- PostgreSQL wire serving and direct DML.
- Kafka, PostgreSQL CDC, object-store, and other implemented connector paths.
- Materialized-view and index lifecycle/backfill machinery with durability and crash-recovery coverage.
- Cold-tier Iceberg/Delta functionality.
- Online shard migration, proactive split/merge, hot-key virtual bucketing, and autoscaling signals.
- Query-time execution and PostgreSQL compatibility work.
- Real operator state-size accounting and workload admission enforcement.
- Bounded arrangement spill-to-SlateDB instead of OOM behavior.
- Async-runtime hygiene, task supervision, timeout/retry budgets, and circuit-breaking work.
- Leak-free long-lived registries and identity-safe resource caps through v0.51.26.

The project should preserve this investment.

The scope change proposed here is about **future prioritization and compatibility commitments**, not retroactively pretending these features were never built.

---

## 2. The strategic product definition

### North star

> **Rockstream continuously maintains correct SQL materialized views over changing data using disposable compute and object-storage-backed durable state.**

A production user should be able to:

1. Ingest changes from a small set of well-supported sources.
2. Define a materialized view using a clearly documented incremental SQL surface.
3. Query committed results through ordinary PostgreSQL tooling.
4. Survive worker, process, network, and object-store failures without losing committed state or producing wrong answers.
5. Scale compute without making local disk authoritative.
6. Understand why a view is stale, blocked, spilling, recovering, or unhealthy.
7. Upgrade or restore the system without rebuilding the world from external sources.

### What Rockstream should optimize for

The primary workloads should remain those where IVM provides the central value:

- Live operational aggregates and dashboards.
- Continuously maintained joins and denormalized read models.
- Application-facing materialized projections.
- Incremental metrics, ranking, and monitoring views.
- High-change datasets where full recomputation is too expensive or too stale.
- A durable freshness layer between operational change streams and consumers.

### What Rockstream should not optimize for

Rockstream should not let the roadmap be driven primarily by becoming:

- A general-purpose OLTP database.
- A distributed ad hoc analytical warehouse.
- A full PostgreSQL implementation.
- A connector marketplace.
- A general lakehouse management platform.
- A broad stream-processing framework.
- A general data-governance product.

Some capabilities in those directions already exist. They can remain useful. They simply should not define what the project builds next.

---

## 3. Strategic core versus implemented capability

The project needs two different classifications:

1. **Implementation status** — whether a capability exists and is verified.
2. **Strategic status** — whether further investment in that capability advances Rockstream's core product.

Conflating these creates the false choice between "keep expanding it" and "rip it out."

### Tier A — Strategic core

These areas should receive continued roadmap priority and the strongest compatibility guarantees:

- Incremental correctness and operator semantics.
- Materialized-view lifecycle correctness.
- Durable object-storage-backed state.
- Checkpointing, recovery, fencing, and replay.
- PostgreSQL wire access to committed state.
- A small number of high-value ingestion paths, especially PostgreSQL CDC and Kafka.
- Resource bounds, backpressure, spill, and storage-pressure handling.
- Observability and diagnostics for IVM state and freshness.
- Rolling upgrades and disaster recovery.
- Security required to operate the service in production.
- Deterministic simulation, formal models, chaos tests, and real-backend proof.

### Tier B — Supported, but not a strategic growth area

These capabilities are already implemented and should remain available and regression-tested, but should not automatically generate new feature families:

- Iceberg/Delta cold-tier integration.
- Existing catalog integration work.
- Advanced ad hoc analytical SQL already shipped.
- Secondary indexes beyond what serving genuinely needs.
- Automatic hot-key virtual bucketing and advanced elasticity controls.
- Existing advanced DML or PostgreSQL compatibility features.
- HTTP/webhook and other already implemented non-core ingestion paths.

**Amended 2026-08-12.** The connector entries above — Iceberg/Delta cold-tier
integration, catalog integration, and HTTP/webhook and other non-core ingestion
paths, plus the S3 source and object-store sink — are **no longer Tier B**. They
are removed at v0.52.3–v0.52.5. Tier B is now the non-connector list only:
advanced ad hoc analytical SQL, secondary indexes, hot-key virtual bucketing and
elasticity controls, and advanced DML/PostgreSQL compatibility features.

The default posture for Tier B is:

> **Maintain correctness, security, and compatibility; do not expand the surface without a demonstrated production need.**

### Tier C — New breadth

Any new capability that substantially enlarges the product surface starts here:

- A new connector family.
- A new external catalog or protocol.
- A new SQL feature family.
- General transactional-database semantics.
- A new governance DSL or policy subsystem.
- A new lakehouse management responsibility.
- A second general-purpose execution path.

Tier C work requires explicit admission before entering the roadmap.

---

## 4. The scope rule: freeze expansion, not implementation

The phrase **"limit Rockstream's scope"** should mean:

- Do not remove already-proven functionality simply to make the repository visually smaller.
- Do not break users of a shipped capability without a separate deprecation decision.
- Do not spend roadmap capacity extending a non-core subsystem merely because it exists.
- Do not let completeness goals such as "support every SQL feature" or "support every sink" become implicit product requirements.
- Do not treat code coverage of a capability as evidence that the capability deserves more product investment.

This is a **scope freeze on strategic expansion**, not a rollback.

A useful test is:

> If Rockstream did not already have this feature, would we choose to build it today because it materially improves cloud-native IVM?

If the answer is no, the feature should usually remain maintenance-only rather than becoming a new roadmap branch.

---

## 5. What should happen next

### 5.1 Finish the current hardening review

The immediate post-v0.51.26 work should continue the hardening trajectory rather than pivoting back to feature expansion.

The roadmap's v0.51.27 focus on **honest failure semantics** is strongly aligned with the product direction. In particular, Rockstream should eliminate:

- Reachable `unreachable!()` or panic paths driven by external/runtime state.
- Operator implementations capable of returning silently wrong results even when the SQL frontend currently blocks them.
- Features whose documented semantics exceed what their execution path actually does.
- Control-plane messages that are accepted, logged, and discarded rather than acted on.
- Any branch where a partial implementation can become reachable through future wiring and return an incorrect answer.

For an IVM system, "fail clearly rather than return a wrong answer" is a core product property.

### 5.2 Make operability a first-class product feature

After correctness hardening, the highest-value remaining work is not another SQL family. It is making the existing engine understandable in production.

Prioritize:

- Operator and view status that explains freshness lag.
- Source lag, compute lag, checkpoint lag, sink lag, spill activity, and compaction/storage pressure as distinct signals.
- An arrangement debugger capable of inspecting intermediate IVM state.
- A support bundle with secret redaction.
- Worker drain and migration visibility.
- Checkpoint inspection and recovery tooling.
- A stable CLI over already-existing control-plane APIs.

The planned operator CLI and arrangement debugger work is therefore highly aligned with the strategic core.

### 5.3 Finish production security

A cloud-native IVM service cannot call itself production-ready if internal communication and connector credentials are handled as development conveniences.

Prioritize the security work necessary to operate the existing product safely:

- Internal mTLS.
- Secret storage and rotation.
- Removal of plaintext connector credentials from persistent/configuration surfaces where practical.
- Audit coverage for secret access and lifecycle changes.
- Redaction in logs and support bundles.
- Independent security review before v1.0.

This is not "enterprise feature breadth." It is production hygiene for the system Rockstream already is.

### 5.4 Prove rolling upgrades and disaster recovery

This should remain a top-tier pre-1.0 milestone.

A cloud-native system whose state is authoritative in object storage should prove that:

- N/N+1 workers can coexist safely during a rolling upgrade.
- Storage-format and protocol-version incompatibilities fail closed.
- Shard assignment respects version compatibility.
- A checkpoint can be exported independently of the live cluster.
- A fresh cluster can restore that checkpoint and reproduce committed state.
- The recovery procedure is documented and rehearsed rather than inferred from unit tests.

Rolling upgrades and disaster recovery are more important to the v1 promise than additional query or governance features.

### 5.5 Continue simulation and failure-matrix maturity

The deterministic simulator, FizzBee models, runtime invariant pairing, and real-backend tests are differentiators. Keep investing in them where they validate the strategic core.

The objective should be coverage of production failure modes, not simply larger test counts:

- Worker and control-node loss.
- Exchange interruption and retry exhaustion.
- Source disconnect and offset recovery.
- Object-store brownouts and throttling.
- Spill and compaction pressure.
- Checkpoint interruption.
- Sink failure during commit/recovery.
- Shard migration interruption.
- Rolling upgrades.
- Resource exhaustion and recovery.

---

## 6. Rebaseline the remaining roadmap

The remaining roadmap should be evaluated against the strategic core rather than executed automatically because it was previously written down.

### Keep / prioritize

#### v0.51.27 — Honest failure semantics

**Recommendation:** Keep and prioritize.

Why: eliminating silent wrong answers, reachable panics, and acknowledged-but-discarded control messages is directly part of IVM correctness.

#### v0.52.3–v0.52.5 — Connector surface reduction

**Recommendation:** Keep and prioritize; it is a prerequisite of v0.57.

**Applied (2026-08-12):** [NEW_ROADMAP.md](NEW_ROADMAP.md) Phase 16.6 schedules
the accepted [ROCKSTREAM_CONNEXTORS_CLEANUP.md](ROCKSTREAM_CONNEXTORS_CLEANUP.md)
proposal as three versions — announced fail-closed removal (v0.52.3), deletion
of implementations, dependencies, and dead abstractions (v0.52.4), and the
three-connector guarantee matrix plus a machine-enforced admission gate
(v0.52.5).

Why: the v1 contract names PostgreSQL CDC and Kafka as the release-gated
connectors. Deleting the rest *before* the contract is frozen means v0.57
describes the system that exists, rather than committing the project to keep
five additional connectors correct, secure, and failure-matrix-covered through
v1 and beyond. The measurable outcome — dependency count, build time, CI time —
is published in the v0.52.4 sign-off, because a deletion version has to be
falsifiable like any other.

#### Operator CLI and arrangement debugging

**Recommendation:** Keep and prioritize.

Why: the system is already sophisticated; production users need to inspect and diagnose the existing IVM engine rather than receive more hidden machinery.

#### Internal mTLS, secrets, and security review

**Recommendation:** Keep and prioritize.

Why: necessary to operate the existing distributed system safely.

#### Rolling upgrades and disaster recovery

**Recommendation:** Keep and prioritize.

Why: necessary to make object-storage-backed disposable compute a credible production promise.

#### Automated release qualification and optional production soaks

**Recommendation:** Make the bounded, no-skip automated qualification suite the
release gate and tie every scenario to a concrete IVM/recovery claim. Longer
production soaks may reuse the same harness when resources permit, but remain
optional supplemental evidence.

#### v1.0 release verification

**Recommendation:** Keep, but define success primarily around the strategic contract: correct maintained views, safe lifecycle behavior, operability, bounded resources, security, upgradeability, and recoverability.

### Narrow / reconsider

#### v0.52–v0.53 declarative data-governance work

**Recommendation:** Split the operationally necessary part from the product-language expansion.

**Applied (2026-08-11):** [NEW_ROADMAP.md](NEW_ROADMAP.md) now carries only the
durable connector quarantine, as v0.52; the expectation/governance surface is
recorded under "Deferred by decision" with its readmission evidence.

A minimal durable quarantine/DLQ for connector decode failures can be justified because malformed source data is a real ingestion lifecycle problem. A broad expectations/governance subsystem should be deferred unless concrete design partners require it.

Prefer:

- Durable failed-record quarantine.
- Clear error codes and metrics.
- Bounded retention.
- A simple inspect/retry operational path.

Be skeptical of:

- A large policy language.
- Rich governance workflow as a new product pillar.
- Expanding catalog surface primarily to compete with data-quality platforms.

#### v0.54 broader transactional semantics

**Recommendation:** Defer by default.

**Applied (2026-08-11):** removed from the roadmap's version table and recorded
under "Deferred by decision"; `DESIGN.md` §1.1 and §12.6's isolation table, and
`docs/language-features.md`, now say deferred rather than naming a version.

Single-shard `SERIALIZABLE LOCAL` and broader ACID workflows move Rockstream toward being an OLTP database. Direct DML is useful as an ingestion/access convenience, but full transactional semantics are not central to cloud-native IVM.

Only promote this work if a concrete target workload cannot reasonably use PostgreSQL/Kafka as its source of truth and requires Rockstream itself to become the transactional authority.

### Maintain, do not expand without evidence

The following already-shipped areas should remain supported, tested, and secure, but should not receive automatic roadmap expansion:

- Iceberg/Delta and cold-tier integration.
- Catalog integrations.
- Advanced ad hoc SQL.
- Secondary-index sophistication.
- Automatic hot-key rewriting.
- Autoscaling control loops beyond what is required to keep the IVM SLO healthy.
- Additional connector families.

**Amended 2026-08-12.** The first two entries are superseded: Iceberg/Delta,
cold-tier, and catalog integration are not maintained — they are removed at
v0.52.3–v0.52.5. The remainder stands.

---

## 7. Define the v1 public contract now

The project should write the v1 compatibility contract before implementing more surface area.

### Core product promise

A reasonable v1 promise is:

> Rockstream ingests changing data, continuously maintains a documented SQL subset as durable materialized views, and serves globally committed results through PostgreSQL-compatible clients while surviving ordinary distributed-system failures without losing or silently corrupting committed state.

### Core connectors

Keep the strategic connector set intentionally small.

Recommended core external streaming connectors:

- **PostgreSQL CDC** — the most natural operational-database input.
- **Kafka** — the most broadly useful durable event-stream input/output.

PostgreSQL-wire DML remains a native access path rather than a connector.

**Amended 2026-08-12.** These are not merely the *core* connectors; after
v0.52.5 they are the *only* connectors. The sentence that stood here — "Existing
S3/object-store, HTTP/webhook, Iceberg/Delta, and related integrations may
remain supported" — is superseded. Those integrations are removed, every
removed surface fails closed with `RS-4017` naming its replacement path, and
`docs/connector-migration.md` plus `docs/connectors.md` are the two documents
the v1 contract cites. The integration story becomes small enough to state in
one sentence:

> Rockstream ingests operational changes from PostgreSQL or Kafka, continuously
> maintains SQL materialized views, serves them through PostgreSQL-compatible
> interfaces, and can publish derived streams to Kafka.

### Core SQL

The v1 SQL contract should be defined from the operator set users actually need for maintained views, not from PostgreSQL or TPC-H completeness.

The project does not need to remove already-implemented SQL. It should distinguish:

- **Core incremental SQL:** release-gated as part of the IVM promise.
- **Additional supported SQL:** maintained and regression-tested.
- **Compatibility/experimental SQL:** no guarantee of continued expansion.

The important metric is not feature count. It is whether the system can state the incremental, backfill, recovery, state-growth, and failure semantics of every core operator.

---

## 8. Add a roadmap admission rule

No new product-surface milestone should enter the roadmap without answering the following questions.

### Product fit

- Does it improve ingestion, incremental maintenance, recovery, serving, or operation of maintained views?
- Is it required by a concrete production workload or design partner?
- Would we build it if the current repository did not already contain adjacent machinery?

### Semantic fit

- Are insert, update, delete, replay, and backfill semantics defined?
- Is its checkpoint and crash-recovery behavior defined?
- Is state growth understood and bounded?
- Can the system fail clearly instead of returning a partial or wrong result?

### Operational fit

- Are all queues, retries, caches, and buffers bounded?
- Are fill level, lag, and failure mode observable?
- Does it behave predictably under object-store and network failure?
- Is there a useful operator-facing diagnostic path?

### Scope cost

- Does it create a new connector family or protocol?
- Does it create a new SQL compatibility obligation?
- Does it move Rockstream toward OLTP, warehouse, governance, or lakehouse-platform responsibilities?
- Does it require ongoing expertise and dependencies disproportionate to its IVM value?

### Proof

- Is incremental output checked against a batch oracle where applicable?
- Are coordination changes modeled or deterministically simulated?
- Are durable paths tested on LFS and a real S3-compatible backend where relevant?
- Are real external systems used for connector guarantees?
- Are negative/failure tests release-gating?

If these answers are weak, the feature should stay out of the strategic roadmap even if it sounds useful.

For machine enforcement, a roadmap table row that proposes new SQL, connector,
catalog, protocol, policy, governance, transaction, or product-surface breadth
must be followed by `### Admission: <version>` containing completed
`Product fit`, `Semantic fit`, `Operational fit`, `Scope cost`, and `Proof`
checklists. `scripts/check-exit-criteria.sh` rejects a marked row when that
block is missing, incomplete, or contains unchecked items.

---

## 9. Concrete repository and planning changes

### 9.1 Replace the old focus document

This document should supersede the earlier project-focus document that described already-implemented lifecycle capabilities as future gaps.

The older document may be retained in history, but should not remain an authoritative source of current next steps.

### 9.2 Link strategy into the planning corpus

Avoid another standalone strategy document that drifts away from implementation.

Add references from:

- `README.md` — product definition and strategic scope.
- `NEW_ROADMAP.md` — roadmap admission rule and strategic classifications.
- `NEW_IMPLEMENTATION_PLAN.md` — the v1 contract and non-goals.
- `DESIGN.md` — a short statement separating architecture capability from strategic product scope.

### 9.3 Give roadmap items a strategic status

Keep the existing implementation status (`✅ Done`, planned, etc.), but add a separate classification where useful:

- **Core** — advances the IVM product and is part of the strategic compatibility contract.
- **Maintain** — already shipped; keep correct and secure, but do not expand by default.
- **Candidate** — proposed breadth requiring explicit admission.

This prevents "Done" from being interpreted as "we must keep expanding this subsystem forever."

### 9.4 Publish one capability matrix

For public-facing features, track:

| Capability | Implementation | Shipped at | Strategic tier | v1 guarantee |
|---|---|---|---|---|
| PostgreSQL CDC source | Done | v0.51.16 | Core | Yes |
| Kafka source/sink | Done | v0.28, v0.51.21 (real client) | Core | Yes |
| Materialized-view IVM (single-shard + distributed) | Done | v0.4–v0.22 | Core | Yes |
| Checkpoint/recovery & fencing | Done | v0.20–v0.22 | Core | Yes |
| PostgreSQL wire serving (auth, txn, LISTEN/NOTIFY) | Done | v0.23–v0.42 | Core | Yes |
| Arrangement spill-to-SlateDB | Done | v0.51.24 | Core | Yes |
| Operator state-size accounting & admission control | Done | v0.51.23 | Core | Yes |
| Async-runtime hygiene / timeout & retry budgets | Done | v0.51.25 | Core | Yes |
| Long-lived registry leak closure | Done | v0.51.26 | Core | Yes |
| HTTP webhook/push source | Removed | Shipped v0.51.16, removed v0.52.3–v0.52.4 | — | No; replacement is an external HTTP→Kafka adapter |
| Object-store / S3 source & sink | Removed | Shipped v0.27–v0.29, removed v0.52.3–v0.52.4 | — | No; replacement is an external loader via pgwire/Kafka |
| Iceberg/Delta cold-tier sink, cold-tier GC & catalog registration | Removed | Shipped v0.44, removed v0.52.3–v0.52.4 | — | No; replacement is Rockstream → Kafka → a downstream lakehouse writer |
| Secondary indexes | Done | v0.32, v0.51.2 | Maintain | Existing behavior, no automatic expansion |
| Hot-key virtual buckets & proactive shard splitting | Done | v0.47 | Maintain | Existing behavior, no automatic expansion |
| Autoscaling signals | Done | v0.47 | Maintain | Tied to IVM SLO only, not general elasticity |
| Advanced ad hoc / multi-shard scatter-gather SQL | Done | v0.51.13 | Maintain | Separate from core IVM guarantee |
| Advanced DML & scatter pruning | Done | v0.48, v0.51.1 | Maintain | Separate from core IVM guarantee |
| Honest failure semantics (no silent-wrong-answer paths) | Planned | v0.51.27 | Core | Yes |
| Durable connector quarantine (bounded, replayable DLQ) | Planned | v0.52 | Core | Yes |
| Resumable online backfill & the snapshot/delta fence | Planned | v0.52.1 | Core | Yes |
| Transaction-preserving PostgreSQL CDC & upstream schema evolution | Planned | v0.52.2 | Core | Yes |
| Connector surface reduction to 2 sources / 1 sink (`RS-4017`, deletion, three-connector guarantee matrix) | Planned | v0.52.3–v0.52.5 | Core | Yes |
| Operator CLI & arrangement debugger | Planned | v0.53–v0.53.2 | Core | Yes |
| Freshness explainability & lag decomposition | Planned | v0.54–v0.54.1 | Core | Yes |
| Internal mTLS, secrets management & security review | Planned | v0.55–v0.55.2 | Core | Yes |
| Rolling upgrade proof & disaster recovery | Planned | v0.56–v0.56.1 | Core | Yes |
| The v1 public contract & compatibility freeze | Planned | v0.57–v0.57.1 | Core | Yes |
| Production failure-matrix proof | Planned | v0.58–v0.58.3 | Core | Yes |
| Evidence integrity & honest release state | Planned | v0.59.1 | Core | Yes |
| True automated end-to-end release qualification | Planned | v0.59.2 | Core | Yes |
| Security provenance, reproducible releases & contract reconciliation | Planned | v0.59.3 | Core | Yes |
| Inline expectations, lineage diagnostics & governance policy language | Deferred by decision | — | Candidate | No; readmission requires a design partner needing policy in the engine |
| Isolation & validation hooks (broader transactional semantics) | Deferred by decision | — | Candidate | No; readmission requires a workload that cannot use PostgreSQL/Kafka as source of truth |
| New connector family (beyond PostgreSQL CDC / Kafka) | Not applicable | — | Candidate | No without admission, machine-enforced from v0.52.5 |

Rows above were generated from [NEW_ROADMAP.md](NEW_ROADMAP.md)'s version table as of the
2026-08-11 rebaseline of v0.52–v0.59;
regenerate this matrix whenever the roadmap's Done/Planned status changes materially.

### 9.5 Stop using feature completeness as the primary roadmap metric

Avoid goals such as:

- "Complete PostgreSQL behavior."
- "Support all TPC-H queries" as a product objective.
- "Add another connector because competitors have it."
- "Expose every implemented subsystem through SQL."

Prefer release objectives such as:

- No silent wrong answers.
- No lost/duplicated committed state under the supported failure model.
- Predictable recovery time.
- Bounded memory and state behavior.
- Explainable freshness lag.
- Rehearsed rolling upgrades and disaster recovery.
- Secure credential handling.
- Known performance envelopes for core maintained-view workloads.

---

## 10. Recommended path from v0.51.26 to v1.0

### Stage 1 — Correctness hardening

Start from v0.51.27 and complete the post-v0.51 hardening review.

Exit when:

- No known reachable silent-wrong-answer branches remain.
- No external/runtime input can trigger an intentional `unreachable!()` or equivalent panic on a supported path.
- Partially implemented behavior fails with a documented error rather than pretending to work.
- Worker lifecycle/control messages are actually consumed by the relevant state machines.

### Stage 2 — Operational completeness

Build the operator-facing tools necessary to understand the already-complex engine.

Exit when an operator can diagnose:

- Why a view is stale.
- Which source or shard is behind.
- Whether an operator is spilling.
- Whether checkpoint alignment is stalled.
- What intermediate arrangement state exists for a key.
- Why a migration or drain is not progressing.

without reading source code or attaching an ad hoc debugger.

### Stage 3 — Security readiness

Finish internal identity, secret handling, credential rotation, redaction, and independent security review.

Exit when the system can be deployed without plaintext operational shortcuts or unauthenticated internal trust assumptions.

### Stage 4 — Upgrade and disaster-recovery proof

Prove rolling upgrade and restore into a fresh cluster from independently stored checkpoint material.

Exit when the procedures are executable runbooks backed by automated tests and a real drill.

### Stage 5 — Evidence integrity and automated v1 qualification

First bind the candidate's source SHA, artifact digests, environment, workload,
raw results, and generated summaries in an immutable evidence manifest. Then run
a bounded, repeatable, no-skip multi-process suite against those exact artifacts:
real Kafka and PostgreSQL CDC paths, public pgwire/CLI setup, independent batch
oracle and sink auditor, observed worker/control/source/storage recovery, two
distinct versions under rolling upgrade, fresh-cluster restore, and measured
resource and performance envelopes.

The release gate should prioritize:

- Correctness.
- Recovery.
- Bounded resources.
- Operability.
- Upgradeability.
- Security.
- Performance stability of core IVM workloads.

Only after those gates are satisfied should `v1.0.0` or broader feature
expansion become a default roadmap activity. Extended chaos, scale,
object-store-pressure, spill, and connector-failure soaks may run when resources
permit, but are optional and cannot replace or block the automated gate.

---

## 11. Measures of success

The strategy is working when:

### Product clarity

- The README can explain Rockstream's core value in one page.
- A user can tell which capabilities are strategic core versus retained extras.
- The roadmap is not implicitly trying to match PostgreSQL, RisingWave, Flink, and a lakehouse platform at the same time.

### Engineering focus

- Most pre-v1 work closes production-readiness gaps rather than adds feature families.
- New connectors and SQL families are rare and justified by concrete workload evidence.
- Existing advanced functionality does not create automatic roadmap commitments.

### Correctness

- Every core operator retains incremental-versus-batch proof.
- No supported path silently returns a known-wrong result.
- Checkpoint, source, migration, spill, and sink recovery retain their failure proofs.

### Operability

- Freshness and failure are explainable from metrics and tooling.
- Arrangement state can be inspected safely.
- Every long-lived resource has a lifecycle, bound, and observable fill level.
- Operators can drain, upgrade, recover, and restore the cluster using documented procedures.

### Reliability

- Worker loss does not require source reconstruction.
- Memory pressure degrades through spill/backpressure instead of OOM.
- Temporary external-system failures remain bounded.
- Rolling upgrades preserve committed epochs.
- Disaster recovery reproduces committed state in a fresh cluster.

---

## 12. Immediate decisions

The project should make the following decisions now:

1. **Adopt the strategic north star:** Rockstream is primarily a cloud-native IVM system.
2. **Take v0.51.26 as the baseline:** do not describe already-shipped capabilities as missing future architecture.
3. **Do not rip out proven features:** use strategic tiers instead of retroactive de-scoping — *with one deliberate exception, the connector surface, removed at v0.52.3–v0.52.5 (see the 2026-08-12 amendment).*
4. **Freeze unqualified feature expansion until v1 production-readiness work is complete.**
5. **Keep v0.51.27-style correctness hardening as the immediate priority.**
6. **Prioritize operator tooling, security, rolling upgrades, disaster recovery, and final production validation.**
7. **Reconsider broad governance and OLTP-style transactional milestones unless concrete target workloads require them.**
8. **Classify existing advanced SQL and elasticity work as maintained capabilities rather than default growth areas, and reduce the external connector surface to PostgreSQL CDC, Kafka source, and Kafka sink.**
9. **Add a strategic-status layer and feature-admission rule to `NEW_ROADMAP.md`.**
10. **Link this strategy from the README, roadmap, implementation plan, and design documentation so it cannot drift independently from the repository again.**

---

## Final recommendation

Rockstream has already built more than a minimal IVM prototype. That is now an asset, not a reason to keep widening the product indefinitely.

The next stage should convert breadth into confidence.

Preserve what has been implemented. Harden it. Make it observable. Make it secure. Make upgrades and recovery boring. Make every supported failure mode either recover correctly or fail loudly. Then call the resulting contract v1.

The intended product identity is:

> **Rockstream is the cloud-native IVM engine you choose when continuously maintained SQL results must remain correct, durable, scalable, and understandable under real failure.**

Future breadth should earn its place by strengthening that identity rather than diluting it.

---

## Repository references

This direction should be maintained alongside, and reconciled continuously with:

- [`README.md`](README.md)
- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`DESIGN.md`](DESIGN.md)
- [`IVM.md`](IVM.md)
- [`NEW_IMPLEMENTATION_PLAN.md`](NEW_IMPLEMENTATION_PLAN.md)
- [`NEW_ROADMAP.md`](NEW_ROADMAP.md)
- [`ROCKSTREAM_CONNEXTORS_CLEANUP.md`](ROCKSTREAM_CONNEXTORS_CLEANUP.md)
- [`sign-offs/v0.51.26.md`](sign-offs/v0.51.26.md)

The roadmap and sign-offs remain the source of truth for **what is implemented**. This document is intended to be the source of truth for **what the project should optimize for next**.
