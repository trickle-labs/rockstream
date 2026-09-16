# RockStream UI Enablement Implementation Plan

**Status:** Planned cross-cutting qualification track; no criterion is complete.

**Prepared:** 16 September 2026.

**Repository baseline:** `7bc67c4f55e11f2c293a414ae4a32d501efc1566` (v0.66 management/capability baseline).

**Roadmap span:** v0.66-v0.75, with the only new subsystem in v0.73.1.

## 1. Goal and completion rule

Prepare RockStream to support the proposed **Build, Administer, and Operate** console before starting implementation of the UI.

The deliverable is a secured, versioned, documented, tested backend product surface. A headless client must be able to perform the intended user journeys using the same interfaces a future browser application will use.

**UI implementation begins only after M11 is signed off.** No React application, component library, dashboard, visual editor, or production UI code is part of this program. API schemas, generated client types, protocol fixtures, and headless acceptance clients are backend deliverables and are in scope.

All criterion-set identifiers, API routes, new paths, test names, and configuration defaults below are **proposals**, not claims that they already exist. This plan is based on repository inspection; the release-process acceptance tests described here have not been executed as part of writing it.

### Definition of the supported first UI

The first UI attaches to **one explicitly configured deployment**. Its backend supports:

| Workspace | Required backend capabilities |
|---|---|
| Build | Authorized catalog search; object/schema/definition inspection; dependency traversal; bounded read-only SQL and result export; SQL validation and incremental-plan inspection; materialized-view creation; committed subscriptions; source/sink inspection; freshness diagnosis. |
| Administer | Effective permissions; durable grants and revocations; workload inspection and reviewed budget changes; secret-reference metadata and controlled rotation; searchable, redacted audit history. |
| Operate | Node/shard inventory; independently defined health dimensions; current and historical observations; bounded diagnostics; guarded drain/migration; capability-gated backups; durable operation inspection and cancellation. |

Kafka sources, PostgreSQL CDC sources, and Kafka sinks are the connector scope. Source pause/resume is required through a supported typed lifecycle path; this does not require introducing currently unsupported SQL syntax. Advanced replacement, destructive cascades, and connector expansion are not prerequisites.

A disabled action is acceptable **only for an explicitly supported limitation**, such as migration of an active workload on the restricted management implementation. Missing implementation of a required journey is a blocker, not a reason to mark every action unavailable and pass the gate.

### Explicit non-goals

Fleet/multi-region management, cloud provisioning, billing, arbitrary connector plugins, a new metrics database, a new identity provider, a general-purpose policy language, visual SQL generation, unrestricted SQL compatibility, shared saved-query collaboration, automatic incident remediation, notification delivery, and live-cluster restore orchestration are outside this program. A bounded scratch restore remains mandatory evidence for backup correctness.

The environment selector in the prototype becomes a deployment label in the first release. Unobserved external consumers must not become invented dependency nodes.

## 2. Baseline and relationship to the existing roadmap

The repository exposes useful building blocks, but an internal type, CLI command, or test name does not prove that an entire browser-facing workflow is production-reachable.

| Verified starting point | Planning consequence |
|---|---|
| The v0.66 management API has typed status and operation methods, but no TLS or authenticated/authorized remote callers. [R1], [R2] | Native management security is a prerequisite, not just a frontend proxy setting. |
| `GetHealth` intentionally reports unknown; shard key ranges are not published. [R1] | Preserve absent knowledge. Add authoritative health producers; do not fabricate key ranges. |
| Pgwire conformance lists portals, named cursors, cancellation, and subscriptions. The capability matrix still rejects ordinary `LIMIT`/`ORDER BY`. [R3], [R4] | Reuse protocol facilities where they pass real boundedness tests; do not implement preview limits by rewriting SQL. |
| Existing ACL types contain ordered `Viewer`, `PipelineOwner`, and `Admin` roles. [R5] | Add only the concrete action distinctions needed for data access versus operations, preserving legacy role behavior through explicit migration. |
| The roadmap separately plans canonical observability, resource control, durable security, and compatibility work. [R6], [R7], [R8], [R9], [R10] | Reuse their owners, implementations, and evidence. Do not create parallel catalogs, policy stores, diagnostic engines, or operation schedulers. |
| v0.66 migration and backup have material restrictions; broader distributed lifecycle is planned separately. [R1], [R11] | The initial console must preserve those restrictions. Active-workload migration is not unlocked by adding a button. |

### Roadmap integration

This document defines a **cross-cutting UI-readiness qualification track**, not a
second release sequence. Its M0-M11 labels group acceptance criteria; they do not
authorize implementation in numeric order. The owning roadmap milestone supplies
the implementation and primary evidence, and this track references that evidence
when the same behavior is needed by a browser-class client.

| Existing roadmap | UI-enablement work owned there |
|---|---|
| Now / v0.66-v0.70 | M0-M1 capability mapping, architecture decisions, API contracts, and the headless harness. This is planning and test infrastructure, not a product claim. |
| v0.71 observability | M4 and M6 canonical object/status read models, lineage, health dimensions, freshness diagnosis, diagnostics, and metric provenance. |
| v0.72 resource control | M5 and the workload-control portion of M7: producer-side interactive-query bounds, cancellation cleanup, subscriptions, query admission, and effective workload changes. |
| v0.73 security coherence | M2 and M8 durable principals/grants, management authentication, common authorization, separated operator/admin/data authority, secrets, audit, and redaction. |
| v0.73.1 console role/API foundation | M3: the secured browser-facing `ConsoleComponent` role, sessions, adapters, generated client, and browser-security boundary. This is the only new subsystem introduced by this track. |
| v0.74 compatibility | The compatibility portion of M10: version the console API, event stream, delegated actor assertions, public resource identities, generated client, and changed durable/protocol formats. |
| v0.75 stable technical preview | The remaining M7 integration plus M9-M11 guarded operations, complete headless persona journeys, packaging, load/security qualification, and the final **Ready for UI implementation** gate. |

v0.63-v0.70 remain prerequisite owners for durable identity, execution,
management operations, distributed lifecycle, and connector behavior. The console
may expose only capabilities those milestones actually qualify; it does not widen
migration, cancellation, backup, connector, or SQL semantics.

Completing a UIE criterion never signs off an otherwise incomplete owning release.
Conversely, one proof may satisfy both an owning version criterion and a UIE
criterion when its evidence covers both exact claims. M2 is a prerequisite for M3,
but it is fulfilled by the applicable v0.73 criteria rather than scheduled
immediately after M1. The required operator-without-data-access distinction is a
concrete v0.73 permission requirement, not authorization to build a generic policy
language. [R6], [R7]

## 3. Architecture and non-negotiable contracts

```text
Future browser or headless acceptance client
                 |
           HTTPS + session
                 |
        rockstream ConsoleComponent
       /             |               \
 scoped SQL      authenticated      bounded telemetry
 adapter         management client  and diagnostics adapter
     |                |                     |
  gateway       control/operations     canonical providers
     |                |                 + Prometheus history
     +------- existing runtime, catalog, and storage -------+
```

Implement `ConsoleComponent` in the existing `rockstream` binary's
`Component`/`NodeRuntime` model, alongside gateway, control, worker, metrics, and
connector supervision. Supporting HTTP/API modules may be factored internally,
but there is no separate console executable or parallel lifecycle. The entry
point is `rockstream start --role console`; after qualification,
`rockstream start --role all` composes exactly one console component.

The console component owns the browser-facing HTTPS listener and later static UI
assets, plus sessions, bounded query handles, ephemeral stream buffers, and
retained export artifacts. The browser communicates only with this endpoint. The
component is an authenticated, authorized boundary to pgwire, management,
telemetry, and artifact storage; those upstream interfaces are not exposed
directly to the browser. It must not become the authoritative owner of engine
objects, grants, worker placement, workload settings, or management-operation
outcomes.

### Security and authority

Authenticate both the service peer and the end-user actor. Use the same authorization decisions for HTTP, pgwire, management RPCs, and equivalent CLI actions. Credentials remain server-side; neither browser-supplied role headers nor a universal `system` connection establishes user authority.

For cross-process delegation, require an authenticated service plus a short-lived, audience- and deployment-bound actor assertion verified by the receiving service. Effective authority is the intersection of the delegation allowance and the actor's current policy. Do not forward an identity token as an unrestricted database credential. In-process calls carry the same validated context.

### Truthful observations

Every status section carries its owning source, observation time, relevant revision/epoch, units, and availability state. Distinguish `known`, `unknown`, `stale`, `unavailable`, and `not_applicable`; handle authorization without exposing protected details. Never substitute zero or healthy for missing measurements.

A composite response does not imply one transactional snapshot across unrelated providers. Committed-result metadata must describe the actual result snapshot; a separately polled frontier is not sufficient evidence.

### Feature support, permission, and eligibility

Keep these separate in the API:

```json
{
  "action": "worker.drain",
  "supported": true,
  "authorized": true,
  "eligibility": "blocked",
  "reason_codes": ["active_workload_migration_not_supported"],
  "observed_revision": "opaque-revision"
}
```

Support comes from the relevant engine/protocol/provider capability registry; authorization from policy; eligibility from current authoritative state. Eligibility may also be `unknown`. A successful preflight is not a reservation or permission grant. Execution rechecks policy, versions, and operational prerequisites at the authoritative mutation boundary.

### Boundedness and failure

Every queue, scan, query, compilation, cache, subscription, export, diagnostic bundle, and retry loop has a named limit, admission point, fill metric, and explicit overflow behavior. Cancellation must reach the work producer. Stopping an HTTP response is not proof that the query stopped.

Mutations distinguish request acceptance, durable commitment, runtime application, and eventual readiness. Unknown mutation outcome after a connection failure is reconciled using the original identity/idempotency key, not retried as a new change.

## 4. Criterion ownership and acceptance dependencies

| Criterion set | Roadmap role | Acceptance dependencies | Suggested accountable function |
|---|---|---|---|
| M0 | Cross-roadmap planning gate during v0.66-v0.70 | None | Technical lead |
| M1 | Cross-roadmap contract and test infrastructure during v0.66-v0.70 | M0 | API/platform |
| M2 | Acceptance overlay on v0.73 | M1 and applicable v0.73 criteria | Security/control |
| M3 | New v0.73.1 product-surface milestone | M1, M2, and the v0.71-v0.73 public providers it adapts | API/security |
| M4 | Acceptance overlay on v0.71 plus v0.73.1 adapters | v0.71 and M3 | Catalog/gateway |
| M5 | Acceptance overlay on v0.72 plus v0.73.1 adapters | v0.72 and M3 | Gateway/runtime |
| M6 | Acceptance overlay on v0.71 plus v0.73.1 adapters | v0.71 and M3 | Runtime/observability |
| M7 | Integration criteria spanning v0.70, v0.72, v0.73.1, and v0.75 | M3, M5, and M6 | Catalog/runtime |
| M8 | Acceptance overlay on v0.73 plus v0.73.1 adapters | M2 and M3 | Security/operations |
| M9 | v0.75 integration of qualified v0.66/v0.68 operations through v0.73.1 | M3, M8, and the applicable lifecycle evidence | Control/storage |
| M10 | v0.74 compatibility plus v0.75 packaging, security, and load qualification | M3-M9 as applicable | Release/quality |
| M11 | v0.75 no-UI journey proof and UI-readiness sign-off | M10 and all owning version criteria | Technical lead + reviewers |

This is an acceptance dependency graph, not an instruction to execute M0 through
M11 as an independent sequence. Engine work stays with its roadmap owner. API
adapters may be implemented only after the underlying public behavior exists, and
no criterion may pass against a predecessor's mock or unqualified implementation.

For every criterion below, record the implementation, documentation, named positive and negative tests, and retained evidence. All stated subcases are mandatory. Suggested test-suite names are additions to make, not tests claimed to exist today.

## 5. Criterion implementation and acceptance requirements

### M0 — Freeze the scope and establish the evidence baseline

**Outcome:** A reviewable inventory of what must work, what already works through a public production path, and what needs implementation.

**UIE-M0-01 — Inventory every proposed UI interaction.** Create `docs/ui-readiness/capability-map.yaml`. For each interaction and displayed field, record workspace, resource/action, authoritative producer, public entry point, supported deployment modes, permission, capability tier, known restrictions, owning milestone, and evidence. Classify it as `verified`, `needs_exposure`, `needs_engine_work`, or `explicitly_deferred`. Internal-only and fixture-only evidence cannot produce `verified`.

**UIE-M0-02 — Resolve architecture and scope decisions.** Commit decisions for deployment topology, identity delegation, permission presets, API/schema ownership, observation consistency, and durability of console-owned state. Map overlapping tasks to existing roadmap criterion IDs. Obtain an explicit roadmap amendment for any security-role or dependency change rather than silently overriding the version plans.

**UIE-M0-03 — Freeze proof profiles.** Define standalone filesystem, standalone shared-object-store, and actual multi-process operation profiles. Include real Kafka/PostgreSQL services for connector claims. Freeze hardware, data volumes, workload mix, resource limits, fault selectors, performance budgets, and supported version combinations before candidate qualification. Section 7 supplies proposed starting limits; changes require a versioned profile diff.

**UIE-M0-04 — Record the initial gap and risk ledger.** Trace status values to producers, authorization to enforcement, SQL limits to execution, and mutations to durable effects. Run available baseline paths and retain exact results; record unavailable infrastructure as blocked. Give every required gap an owner and milestone. Do not carry forward the earlier informal implementation-percentage estimates as measured readiness.

**Acceptance artifact:** `baseline.md`, the capability map, decision records, frozen profiles, and a complete required-interaction coverage report. No unresolved architecture choice may block a dependent owning milestone or M3 design.

### M1 — Define the public contracts and headless proof harness

**Outcome:** The future UI can be specified against stable contracts without depending on internal Rust structures or CLI text.

**UIE-M1-01 — Define `/api/v1` and event schemas.** Cover sessions, capabilities, objects, queries, subscriptions, changes, workloads, policy, secrets, observations, diagnostics, operations, and backups. Publish OpenAPI and event schemas with one authoritative generation path. Generate a TypeScript client usable from a headless process; generation and type-checking are allowed before UI work. Document error mapping, including HTTP status, retained SQLSTATE/gRPC status, RockStream code, request ID, safe message, retry classification, and remediation. [S2]

**UIE-M1-02 — Define identity and consistency in resource representations.** Use stable, namespace-aware resource identifiers that do not collide across drop/recreate. Define object revisions, per-provider observations, opaque cursor scope/expiry, and conditional writes. Serialize 64-bit identifiers, exact SQL integers, and decimals without JavaScript precision loss. Preserve NULL, duplicate multiplicities, binary data, and timestamp/time-zone semantics. State ordering guarantees explicitly.

**UIE-M1-03 — Define mutation and stream contracts.** Separate read-only query requests from reviewed changes. Specify idempotency scope, request digests, retention, uncertain outcomes, and stale-preflight rejection. Define subscription snapshot/epoch boundaries separately from operation-observation streams. Reconnect must either resume according to a proven contract or explicitly require a fresh snapshot; no implied lossless replay.

**UIE-M1-04 — Build the release-process harness.** Add a proposed `tools/ui-readiness-harness` with HTTP, pgwire, and management clients. Launch release binaries using public configuration, independently observe results, inject process/storage/network failures, and retain transcripts. Add schema/compatibility checks and per-criterion evidence indexing. Initial harness tests may use controlled fixtures to test the harness itself, but those cannot qualify a product capability.

**Proposed suite:** `ui_contracts`. Test schema examples, unknown/absent fields, identifier fidelity, unsupported protocol versions, and complete error payloads. Missing required routes or schema drift fail CI.

### M2 — Establish durable identity and shared authorization

**Outcome:** Engine and management endpoints enforce the eventual UI's permissions without depending on the console component to hide privileged operations.

**UIE-M2-01 — Persist security state.** Reuse the durable catalog for principals, grants, password verifiers where supported, and policy revisions. Define bootstrap, grant/revoke, revocation propagation, credential rotation, and recovery. Couple acknowledged security changes with durable audit evidence. Corrupt or unavailable security state must fail closed, not produce an empty permissive store. This is shared v0.73 work, not a console-local ACL database. [R7]

**UIE-M2-02 — Introduce a narrow action registry.** Define the concrete actions needed for metadata/definition inspection, data reads, subscriptions, view authoring, source lifecycle, workload changes, policy administration, secret use/rotation, diagnostics, audit access, and cluster operations. Provide fixed data-reader, builder, administrator, and operator presets. Operator does not imply business-data, secret-value, or grant authority. Preserve legacy role semantics as explicit compatibility presets; do not silently remove or widen existing grants. Administrative grant authority is itself scoped and audited.

**UIE-M2-03 — Enforce the same policy across entry points.** Bind authenticated actors to requests in HTTP, pgwire simple and extended execution, management RPCs, and CLI-driven calls. Check resource scope before reads, compiler/catalog introspection, and side effects. Secure management reads as well as mutations. Authenticate service/node identities separately from user identities; a node certificate cannot become an administrator. Verify delegated actor assertions and do not trust caller-supplied names or role flags.

**UIE-M2-04 — Secure native service transport.** Add authenticated, authorized TLS management transport and the corresponding client configuration. Protect cross-process SQL/delegation transport. Reject missing trust roots, mismatched identities, unsupported modes, and expired credentials. An insecure development mode, if retained, is explicit, loopback-only, separately advertised, and ineligible for production sign-off. Never fall back from authenticated management to the v0.66 unauthenticated listener. [R1]

**UIE-M2-05 — Bound revocation and caches.** Set a maximum authorization-staleness interval, terminate or reauthorize long-lived streams within it, and invalidate pooled-connection authority correctly. Bind caches to principal, scope, deployment, and policy revision. Bound token/JWKS decoding, refresh concurrency, audit buffering, and authentication retries.

**Proposed suite:** `ui_identity_public_paths`. Exercise an allowed/denied matrix through every applicable transport, cross-namespace access, forged delegation, cross-user connection reuse, revocation during a query/subscription, expired credentials, and process destruction after grant/revoke. Compare exact grants and audit records after recovery. M2 fails if an operator can retrieve protected data through SQL, diagnostics, exports, or management.

### M3 — Implement the secured console API `ConsoleComponent`

**Outcome:** A real browser-compatible `ConsoleComponent` exists inside the
`rockstream` binary, with no UI and no unrestricted privileged proxy.

**UIE-M3-01 — Add the console role and adapters.** Implement
`rockstream start --role console` by adding `ConsoleComponent` to the existing
`Component`/`NodeRuntime` composition and lifecycle. Add the corresponding
`NodeRole::Console` support and compose the same component exactly once for
`rockstream start --role all` after it is qualified. Targets are deployment
configuration, never arbitrary URLs supplied by users. Establish authenticated
adapters, bounded pools, request deadlines, admission limits, and graceful
shutdown. Refuse production startup against an insufficiently secured backend.
An unavailable optional telemetry provider must not prevent authorized
catalog/query use. Extend `NodeConfig` with a `[console]` section for the
HTTPS listener, authentication/session settings, upstream gateway and management
targets, optional telemetry and artifact storage, and console resource limits.
Resolve it through the existing defaults < file < environment < CLI precedence,
redacted origin reporting, unknown-key rejection, role validation, and
startup/drain lifecycle. Do not add a nested console subcommand or another
console process lifecycle.

**UIE-M3-02 — Add browser sessions.** Use OIDC authorization-code flow with PKCE and server-held tokens in `ConsoleComponent`; validate issuer, audience, signature, state, nonce, redirect targets, and expiry. Use secure, HttpOnly session cookies with an explicitly tested SameSite policy; implement CSRF/origin protections, logout, revocation, and session fixation defenses. Reject unconfigured identity modes. Use established protocol libraries rather than creating an identity provider. [S1]

**UIE-M3-03 — Publish identity, capabilities, and action decisions.** Implement session inspection, deployment identity, supported API versions, SQL capability discovery, attached management methods, optional providers, effective permissions, and per-resource action eligibility. Do not treat management `GetCapabilities` as the SQL capability registry. Keep permission failures and sensitive precondition details from becoming an object-enumeration channel.

**UIE-M3-04 — Enforce a safe HTTP boundary.** Add same-origin defaults, explicit allowed origins, content-type and request-size validation, rate limits, cache/privacy headers, safe logs, and correlation IDs. Streaming connections and downloads require authentication too. Pool cleanup must reset database/session state and prohibit identity leakage. Console-component restart invalidates ephemeral sessions/queries predictably without altering accepted engine operations. The browser has no direct route to pgwire, management, telemetry, or artifact storage.

**Proposed suite:** `ui_console_security`. Test OIDC login/logout against a controlled provider, CSRF rejection, malicious origins/targets, pooled-session isolation, rate limiting, backend outage, startup misconfiguration, and reconnect after console-component restart. Use HTTP/protocol clients; no application screens are necessary.

### M4 — Expose authoritative objects, definitions, and dependencies

**Outcome:** The catalog and object inspector can be implemented without parsing CLI output or inventing data in the browser.

**UIE-M4-01 — Implement canonical read services.** Provide bounded list/search/detail APIs for namespaces, tables, inline/materialized views, sources, sinks, workloads, nodes, shards, and checkpoints. Include schemas, definitions where authorized, lifecycle, assigned workload, and source provenance. Trace every value to durable catalog or identified runtime providers and repair missing producers. Reuse the v0.71 canonical catalog work. [R8]

**UIE-M4-02 — Make pagination and search safe.** Apply authorization before pagination and count aggregation. Bind cursors to deployment, scope, filters, and a documented revision/snapshot policy. For providers without stable snapshots, report live-list semantics and restart requirements; do not promise snapshot consistency. Bound search fan-out and caches. Distinguish absent objects from unavailable providers without leaking inaccessible names.

**UIE-M4-03 — Productize lineage.** Expose dependencies from resolved catalog/compiler/runtime records, not browser SQL parsing. Distinguish declared definitions, compiled dependency versions, source/table edges, materialized-view edges, and actual sink consumers. Preserve the version used by an existing compiled view when a reusable inline definition changes. Return traversal depth/node limits and explicit truncation. Redact unauthorized edges without implying that visible edges form the complete graph.

**UIE-M4-04 — Reconcile reads with engine behavior.** Prove that create/change/delete/restart affects the public catalog and dependency graph correctly. Return missing health/key-range/resource facts as absent rather than guessed. Index only catalog metadata, not raw user data, for global object search. Avoid a second independently maintained console catalog.

**Proposed suite:** `ui_catalog_lineage`. Test two namespaces with colliding names, drop/recreate identity, multi-level and diamond dependencies, inline-definition changes, hidden upstream objects, large catalogs, unavailable peers, and restart. Compare complete authorized records and graph edges with independently constructed expected results.

### M5 — Implement bounded SQL, previews, validation, and subscriptions

**Outcome:** A future SQL editor and data grid have safe execution semantics, not just a way to send SQL.

**UIE-M5-01 — Implement non-mutating validation and explanation.** Reuse the production compiler/binder with the target catalog, namespace, capabilities, and actor. Return structured diagnostics, source spans, required capabilities/permissions, output schema, dependency revisions, and a plan fingerprint. Define UTF-8 byte spans and editor-compatible location conversion, including non-ASCII text. Keep static estimates separate from measurements, and use explicit unavailability instead of a fabricated zero estimate. Validation must not create objects or start external ingestion.

**UIE-M5-02 — Enforce query budgets at the producer.** Support a bounded, single-statement read-only query path, including permitted inspection statements. Reject mutations and unauthorized reads in the engine dispatcher, not with a frontend regex. Use proven portals/cursors or a bounded gateway execution path. Audit whether result construction materializes the complete relation before suspension; fix it where necessary. Bound compilation, intermediate state, queued work, rows, encoded bytes, wall time, concurrency, and snapshot lifetime. Return explicit truncation/limit errors; do not append unsupported `LIMIT` or promise that client-side sorting orders the full relation. [R3], [R4]

**UIE-M5-03 — Make cancellation and result handles real.** Bind query IDs/cursors to actor, deployment, scope, and policy. Cancellation, timeout, disconnect, cursor expiry, and console-component shutdown must close or cancel upstream work and release reservations. Report cancellation complete only after cleanup is acknowledged. Define ephemeral-query loss on console restart and retain no ambiguous promise of resumability. For the required materialized-view result path, return a snapshot/commit token tied to the actual read; add gateway response metadata if the current protocol does not expose it. Other statement classes may explicitly report that this metadata is not applicable or unavailable, but cannot present that absence as a proven freshness guarantee.

**UIE-M5-04 — Implement a bounded committed subscription bridge.** Define snapshot start/end and committed epoch frames with sequence information, weighted changes, and explicit reset behavior. Preserve duplicates and atomic epoch application; never display an uncommitted half-update. Bound snapshot size, per-epoch accumulation, total buffers, stream count, and slow-client lifetime. Reject an oversized complete snapshot/epoch rather than silently dropping rows. On reconnect or lost replay history, require resnapshot unless durable replay is independently qualified.

**UIE-M5-05 — Implement safe bounded exports.** Export only an authorized result handle and its actual returned rows, with completion/truncation metadata. Bound serialization and download lifetime. Specify exact type representation and CSV text-cell safety; do not expose unrestricted storage paths or permit an export to bypass query/data permissions. Large asynchronous whole-dataset exports are deferred.

**Proposed suite:** `ui_query_execution`. Cover exact integers above JavaScript's safe range, decimals, NULLs, duplicates, binary values, large result sets, expensive intermediate plans, malformed/multi-statement SQL, deadline/cancel races, token theft, revocation, slow consumers, and process restart. Verify producer-side memory/CPU/queue cleanup, complete expected result multisets, epoch framing, and honest preview metadata. Existing portal/cancellation tests are regression coverage, not sufficient boundedness proof.

### M6 — Make freshness, health, history, and diagnosis authoritative

**Outcome:** The proposed “Why is this view behind?” interaction can be answered from real system observations.

**UIE-M6-01 — Implement canonical view-status provenance.** Expose lifecycle, input and published frontiers, freshness target/lag, memory/state ownership, assigned shards, degradation reason/code, and blocking operation. Trace every field to an actual producer and committed epoch where relevant. Preserve the engine's reason selection and tie-breaking instead of reimplementing diagnosis in the API adapter. Document whether lag components are additive; never sum unrelated gauges or mistake source idleness for stale data without the defined watermark semantics. [R8], [R12]

**UIE-M6-02 — Complete the health model.** Reuse the roadmap's separate liveness, readiness, availability, freshness, durability, capacity, and degradation dimensions. Define the observation source, sampling window, stale threshold, and unknown behavior for each. Add authoritative producers for required paths. A reachable process with inaccessible storage is not automatically ready or durable; an old heartbeat is not a fresh health observation. Preserve unknown for genuinely uninstrumented optional components. [R8]

**UIE-M6-03 — Add optional historical telemetry.** Provide a bounded adapter to a configured Prometheus service using approved metric templates and authorized label scopes. Record units, series provenance, sample times, aggregation, missing intervals, and counter resets. Cap time range, resolution, series count, points, and response bytes. No user-provided backend URL or unrestricted cross-tenant PromQL. When history is absent or unavailable, current authoritative status still works and the history response says why it is absent.

**UIE-M6-04 — Expose bounded diagnostic checks.** Reuse the existing doctor/check machinery for configuration, connectivity, storage, certificates, and relevant runtime blockers. Every check has a deadline, permission, safe evidence payload, and remediation code. Preserve PASS/WARN/FAIL/unavailable distinctions. Diagnostics are observational and cannot quietly mutate a workload or trigger remediation.

**Proposed suite:** `ui_observability_faults`. Exercise disconnected source, unavailable worker, active operation, exhausted budget, recovering view, lagging connector, unavailable storage, stale observations, and absent telemetry history. Assert complete API/status/metric records against actual committed results before, during, and after recovery. A time-controlled metrics fixture can test rendering contracts but cannot qualify a real producer.

### M7 — Implement reviewed authoring and effective configuration changes

**Outcome:** A future “Review → Apply” dialog corresponds to a safe server workflow and an observable effective result.

**UIE-M7-01 — Add prepare/apply contracts.** Prepare a change using the actor, exact input digest, deployment, resource identity, relevant catalog/policy revisions, and a short expiry. Return affected resources, limitations, required authority, and blockers. Apply must recheck all safety-relevant state at the authoritative mutation point; use transactions/reservations where required. Reject stale or modified plans without partial effects. A preflight token is not authorization and does not remove execution-time checks.

**UIE-M7-02 — Implement materialized-view creation.** Validate and apply a supported definition through the real compiler and durable catalog. Return stable resource and change identities; distinguish definition committed, deployment pending, backfill running, and result ready. Reconcile lost responses by idempotency key and object generation. Preserve the SQL capability contract; do not silently lower an unsupported construct to different semantics. Online replacement and destructive cascades remain deferred with explicit rejection.

**UIE-M7-03 — Implement actual source lifecycle controls.** Add authorized, typed pause/resume for the in-scope connectors, routed to the active ingestion owner. Persist requested lifecycle state and return runtime acknowledgement/effective state separately. Pausing must actually stop the defined ingestion behavior; resuming must preserve the documented committed-offset/transcript guarantees. Do not treat editing a status row as pausing a connector. Repair the public runtime path where the existing capability matrix says lifecycle SQL is not reachable. [R4]

**UIE-M7-04 — Make workload edits effective.** Expose observed usage, configured memory/parallelism/priority/freshness limits, assigned views, and the proposed diff. Commit with a revision condition and propagate the change to active owners. Report requested versus effective revision, including individual pending/failed owners where applicable. Prove enforcement through workload behavior, not a catalog readback alone. Initially reject reductions below the runtime's verified non-reclaimable usage and keep the previous effective limit. For accepted changes, wait for owner acknowledgement under the frozen propagation deadline; never claim instant reclamation. Reuse v0.72 accounting/admission. [R9]

**UIE-M7-05 — Make mutations recoverable and auditable.** Persist request identity before effects, use the existing authoritative change/operation machinery, and reconcile retries after restart. Couple acknowledged changes to durable audit evidence. Protect concurrent edits, policy changes during review, unavailable workers, and unknown outcomes. Do not report an accepted request as an effective configuration change.

**Proposed suite:** `ui_reviewed_changes`. Test two administrators editing the same workload, actor revocation between prepare/apply, source pause/resume across restart, lost responses after commit, definition conflicts, failed deployment, over-budget rejection, and worker outage during propagation. Assert complete catalog, runtime, audit, and committed-data effects.

### M8 — Expose access, secrets, audit, and support-artifact administration

**Outcome:** Administration screens can operate on durable security state without exposing credentials or assuming that local UI state is authoritative.

**UIE-M8-01 — Add grant/revoke administration.** Expose principals, applicable fixed permission presets, effective permissions, and scoped grants through M2's policy store. Use reviewed, revision-checked durable mutations. Restrict grant authority explicitly; operators cannot self-grant data or administrator privileges. Define the last-administrator/bootstrap recovery procedure and audit it. Show policy changes as effective only within the documented revocation/propagation contract.

**UIE-M8-02 — Add secret-reference metadata and rotation.** Expose identifiers, versions, authorized consumer references, and rotation status, never a GET endpoint for existing plaintext values. If accepting new credentials, use write-only authenticated input, encrypted durable storage, redacted errors, and no body logging. Authorize secret use separately from secret administration. Prove running connectors use the defined new version and preserve committed results during rotation; reject missing or unauthorized references safely. [R7]

**UIE-M8-03 — Expose audit history.** Provide filtered, paginated access to the authoritative audit stream with actor/service identity, action, scope, request/change/operation correlation, ordering, result, and safe before/after metadata. Bound retention and query cost. Define durable-outbox/storage-full behavior so successful protected mutations cannot become unauditable. Keep security audit separate from best-effort operational logs and from query-result storage.

**UIE-M8-04 — Add support-artifact generation and download.** Reuse bounded diagnostics and the existing support-bundle machinery. Default to sanitized operational metadata, excluding business rows, secret values, SQL literals, and raw connector payloads. Bind download authorization to actor/scope; use opaque artifact IDs and expiry instead of arbitrary filesystem paths. Track generation and cancellation using existing job/operation facilities where asynchronous work is needed.

**Proposed suite:** `ui_admin_security`. Test scoped grant changes, revocation with pooled sessions, process destruction, concurrent policy edits, live credential rotation, audit-destination failure, expired downloads, and cross-user artifact access. Scan full API responses, logs, metrics labels, error messages, and support artifacts for known test secrets and encoded equivalents. An operator's support bundle must not bypass the no-data-access policy.

### M9 — Implement guarded operations and backup readiness

**Outcome:** Operational buttons will submit and observe real durable operations, with accurate safety and cancellation boundaries.

**UIE-M9-01 — Expose one authoritative operation model.** Reuse management operation IDs and the existing Pending/Running/Waiting/Succeeded/Failed/Cancelled state machine. Add actor/correlation, typed progress, blocked reasons, observation revision, and executor-derived cancellation eligibility where absent. Unknown totals/estimates remain unknown. Do not manufacture percentages from phase names or duplicate the operation store in the console. Polling is mandatory; an SSE observation feed is optional, bounded, and reconnects by refreshing authoritative state unless retained event replay is supported. [R1], [R2]

**UIE-M9-02 — Add execution-safe preflight.** Inspect real ownership, recipient availability, capacity, active deployments, existing operations, and supported engine capabilities. Bind review to the proposed target and relevant revisions. Recheck and reserve at execution so a stale preflight cannot race another migration or an arriving workload. A failed check produces no side effect and includes authorized blocker information.

**UIE-M9-03 — Qualify drain/migration/cancellation.** Exercise the actual management executors with at least two real workers and durable shard state. Retain the restriction on active-workload movement unless the relevant v0.67/v0.68 lifecycle requirements have separately passed. Require recipient-open acknowledgement for success. Cancellation is governed by the executor's irreversible boundary; do not promise rollback after lease transfer or cancellation of an entire drain after irreversible child effects. Restart the console without changing the operation ID or outcome. [R1], [R11]

**UIE-M9-04 — Implement backup readiness and verification.** Expose attached-method support, source-store requirements, shared-store compatibility, true idle-state requirements, destination policy, and blockers. Create only eligible backups through the real management path. Use configured, authorized destinations; an operator must not exfiltrate data by selecting a personal bucket. Separate backup creation from artifact read/download authority, and enforce encryption/access policies. Verify the management export's inventory and terminal commit marker; do not run legacy local-manifest verification against a different format. Audit/operation logs excluded from that format need separate documented retention/recovery. [R1]

**UIE-M9-05 — Prove recovery, not just submission.** Inject failures before and after intent persistence, lease transfer, recipient acknowledgement, export completion, and response delivery. Retry with the same actor-scoped idempotency key and verify one authoritative operation/effect. Restore a completed backup into fresh isolated storage through the supported restore path and compare complete recovered data and required metadata. Live-cluster restore remains outside the first UI.

**Proposed suite:** `ui_management_operations`. Assert real lease ownership, full durable data, operation states, cancellation rejection, safe donor retention, and audit correlation through controller/worker/API-service restarts. Include unsupported active-workload migration, missing recipient, full operation store, missing backup executor, busy cluster, wrong destination, corrupt export, and duplicate submission. A timer-driven success response cannot pass.

### M10 — Package and qualify the backend as a deployable product

**Outcome:** The backend can be installed, upgraded, operated, and load-tested without the prototype or a developer-only runtime.

**UIE-M10-01 — Ship a reproducible deployment.** Include the console role in the release `rockstream` binary/container and provide standalone and qualified multi-process examples. Document TLS, OIDC, bootstrap, gateway/management trust, optional Prometheus, secret references, storage, limits, role composition, and shutdown. Keep network targets allowlisted and sensitive listeners isolated. Provide console-role liveness/readiness probes that do not confuse optional history availability with cluster health. Execute installation examples in CI.

**UIE-M10-02 — Version formats and qualify compatibility.** Test the API client/server and engine/API combinations frozen in M0. Version changed grant, operation, object, and session/delegation formats. Exercise interrupted migration and documented rollback boundaries. Reject incompatible or insecure backends clearly, with no silent downgrade. Test unknown optional fields and new enum values against the generated client. Do not claim upgrade safety for combinations not exercised.

**UIE-M10-03 — Prove bounds under load.** Run the frozen catalog/query/subscription/admin/operation mixes while real maintenance continues. Measure API latency, engine read/commit/freshness latency separately, sustained throughput, RSS, buffers, queue age, cancellation cleanup, and recovery. Compare API versus direct-client runs at identical offered load. Include large catalogs, result sets larger than memory budgets, slow readers, slow storage, telemetry outages, and principal-scoped saturation. Control/maintenance work must not be starved by console reads.

**UIE-M10-04 — Complete adversarial qualification.** Test untrusted input, malformed tokens, header spoofing, stale/replayed change tokens, cursor/operation enumeration, cross-scope search/metrics/download leakage, session expiry, CSRF, forbidden management access, and all declared authentication-mode failures. Fuzz parsing/serialization boundaries and scan logs/artifacts for secret canaries. Resolve security findings that undermine a required isolation or integrity property before sign-off.

**Proposed suite:** `ui_release_qualification`. Run release processes on the frozen filesystem and shared-object-store profiles; add real Kafka/PostgreSQL and at least two-worker fixtures for the corresponding claims. Retain measurements, exact process topology, binary digests, schema versions, commands, and raw results. Skipped, mock-only, or infrastructure-blocked cases remain incomplete.

### M11 — Pass the no-UI acceptance gate and freeze the handoff

**Outcome:** A frontend team can implement the proposed UI without discovering missing security, data, or operation semantics.

**UIE-M11-01 — Execute complete persona journeys.** Use the generated client or a headless HTTP client against the packaged `rockstream` binary's console role, never private Rust setters or prototype fixtures, to complete every journey in Section 6. Correlate its responses with the engine, durable state, and audit/observation sources. Include denied and faulted paths as well as success.

**UIE-M11-02 — Close the capability map.** Every first-UI interaction has a working API, authoritative producer, permission rule, absence/error semantics, resource limits, documented compatibility, and linked positive/negative evidence. Required features cannot pass solely by returning unsupported. Explicitly deferred features have a documented future owner and no first-UI affordance that suggests they work.

**UIE-M11-03 — Publish the frontend handoff package.** Deliver versioned OpenAPI/event schemas, generated TypeScript client/types, exact examples, authentication instructions, documented statuses and errors, capability/permission/eligibility contracts, local integration setup, and executable headless journeys. Include healthy, degraded, stale, denied, unavailable, conflict, cancelled, and unsupported examples derived from qualified behavior. These are contract fixtures, not substitutes for live proof.

**UIE-M11-04 — Enforce a mechanical and human gate.** Add `scripts/check-ui-readiness.sh` to check the fixed criterion inventory, complete evidence mappings, artifact availability, schema drift, and test-result statuses. Include a self-test proving it rejects missing/unchecked/skipped criteria and fabricated completion metadata. Reviewers must inspect that evidence proves the actual behavior; file existence and green commands are insufficient. Technical, security, runtime/storage, and operational reviewers sign the final manifest.

**Proposed suite:** `ui_persona_journeys`.

**Release rule:** v0.75 cannot pass its UI-readiness qualification until M0-M11
and every referenced owning criterion are signed off with no unresolved mandatory
item. Only then change the program status to **Ready for UI implementation**.
This is not a v1.0 readiness claim or a sign-off of unrelated roadmap work.

## 6. Mandatory headless journeys

| ID | Persona and journey | Required proof |
|---|---|---|
| J1 | Data reader searches the catalog, opens an authorized view, inspects its schema, runs a bounded query, and exports the preview. | Correct authorized objects and exact typed rows; true snapshot/truncation metadata; no cross-scope access; server work stops at its bounds. |
| J2 | Builder validates a materialized-view definition, inspects its plan, reviews, applies, and observes a committed result. | Validation is non-mutating; stale review fails; accepted/committed/ready states are distinct; complete results survive restart. |
| J3 | Reader subscribes to a view while inserts, updates, and deletes occur. | Exact weighted changes, duplicates, NULLs, committed epoch boundaries, bounded slow-reader behavior, and explicit reconnect/resnapshot semantics. |
| J4 | Authorized user investigates a delayed view and follows its source/dependency/blocker evidence. | Actual injected fault matches the reported cause and observations; current status remains available without historical telemetry; recovery removes the condition truthfully. |
| J5 | Administrator reviews a workload limit while another actor makes a conflicting change. | Stale revision is rejected; authorized retry commits once; active runtime owners enforce the effective limit; no success based only on metadata. |
| J6 | Administrator changes a grant and rotates an in-use secret. | Authority survives restart; revocation takes effect within its bound; the connector follows the defined rotation policy; exact committed results and complete redacted audit evidence remain. |
| J7 | Operator inspects nodes/shards, reviews an eligible drain, submits, loses the browser/API connection, and resumes inspection. | The same durable operation completes or reports a real blocker; recipient acknowledgement and leases match; cancellation obeys its boundary; operator still cannot read business data. |
| J8 | Operator examines backup blockers, creates an eligible backup, verifies it, and generates a sanitized support bundle. | Real export validation and isolated restore reproduce complete data; invalid destinations cannot exfiltrate it; bundles/downloads remain authorized and redacted. |

Run the applicable cases on filesystem and shared-object-store backends. Multi-worker claims require actual distinct processes. Connector claims require real external services. A supported limitation gets its own rejection proof; it does not replace the successful eligible case.

## 7. Initial boundedness and performance profile

These are **proposed starting decisions**, not measured current capacities or delivery estimates. M0 must freeze an executable profile with exact hardware, topology, payloads, workload mix, and pass/fail rules. Revise values through a reviewed profile change before measurement; never widen them after a failing run simply to obtain sign-off.

| Surface | Proposed initial limit or acceptance target |
|---|---|
| Catalog page | Default 50 objects, maximum 100; bounded backend scan work even when many rows are unauthorized. |
| Query preview | At most 1,000 rows and 8 MiB of encoded results per query, including all result pages. |
| Interactive query | 10-second overall budget, including admission/compilation; explicit rejection or timeout, not unbounded queuing. |
| Query concurrency | At most 2 active queries per principal and 16 per console instance, further constrained by shared engine admission. Aggregate limits must remain safe if console nodes are added. |
| Cursor lifetime | 30-second idle timeout and 60-second absolute snapshot lifetime. |
| Cancellation cleanup | Producer cancellation/resource release acknowledged within 2 seconds on the qualified profile; failures report a cancellation failure rather than false completion. |
| Subscription | At most 4 streams per principal; 4 MiB per-stream buffer with a 64 MiB aggregate service buffer budget; slow-reader reset/disconnect is explicit. |
| ConsoleComponent memory | 256 MiB accounted application budget, with a separately measured/frozen runtime/allocator overhead allowance; all component maxima remain subject to aggregate reservations. |
| History query | Maximum 24-hour range, 20 series, 2,000 points per series, and 8 MiB response; reject incompatible requests or return an explicit coarser resolution. |
| Support artifact | 10 MiB maximum; actor-scoped download expires after 10 minutes; retained artifact deletion follows a frozen policy. |
| Mutation review | 30-second preflight expiry plus mandatory execution-time checks; expiry does not substitute for version checking. |
| Revocation | At most 60 seconds until cached permissions/long-lived streams are revalidated; no newly admitted operation may ignore a newer known policy revision. |
| Catalog performance | Initial profile: 10,000 objects and 20 concurrent readers; p95 response below 250 ms and p99 below 1 second on declared hardware. |
| Adapter regression | At identical engine workload and query schedule, at most 5% throughput and 10% p99-latency regression versus direct clients, measured separately for reads, commits, and freshness. |
| Soak | At least 2 hours of the frozen mixed workload with bounded RSS/queues, no control starvation, and successful fault recovery. |

Additional limits are mandatory even when not assigned a number above: compilation complexity, request body size, result-cell size, lineage traversal, JWT/JWKS caches, authentication handshakes, audit retention/outbox capacity, operation retention, retry queues, metrics label cardinality, and export storage. Record each in the same profile with ownership and overflow behavior. Console limits must never raise a stricter engine limit.

## 8. Proposed API inventory

Route names become final in M1. This table fixes responsibilities, not a requirement to build one endpoint per visual component.

| API group | Initial responsibilities | UIE criteria / roadmap owner |
|---|---|---|
| `/session`, `/capabilities`, `/permissions` | Current actor, deployment identity, supported surfaces, effective permissions, and action eligibility. | M3 / v0.73.1 |
| `/objects`, `/objects/{id}`, `/objects/{id}/dependencies` | Authorized search/detail/schema/definition/lineage. | M4 / v0.71 + v0.73.1 |
| `/sql/validate`, `/sql/explain` | Non-mutating compilation, diagnostics, capability requirements, and typed estimates. | M5 / v0.72 + v0.73.1 |
| `/queries`, `/queries/{id}/results`, `/queries/{id}/cancel`, `/queries/{id}/export` | Bounded execution, ownership-scoped results, cancellation, and preview exports. | M5 / v0.72 + v0.73.1 |
| `/subscriptions` | Authorized creation, bounded committed stream, and explicit closure/reset. | M5 / v0.72 + v0.73.1 |
| `/objects/{id}/status`, `/health`, `/metrics`, `/diagnostics` | Current evidence, health dimensions, historical observations, and bounded checks. | M6 / v0.71 + v0.73.1 |
| `/changes/prepare`, `/changes/{id}/apply`, `/changes/{id}` | Review/commit/reconciliation for authoring, source lifecycle, and workload changes. | M7 / v0.70, v0.72, v0.73.1, v0.75 |
| `/principals`, `/grants`, `/secrets`, `/audit`, `/support-artifacts` | Durable administration and redacted artifacts. | M8 / v0.73 + v0.73.1 |
| `/nodes`, `/shards`, `/operations`, `/operations/{id}`, `/operations/{id}/cancel` | Guarded management and durable observation. | M9 / v0.66, v0.68, v0.73.1, v0.75 |
| `/backups/readiness`, `/backups`, `/backups/{id}/verification` | Eligibility, restricted destinations, creation, and management-format verification. | M9 / v0.66, v0.73.1, v0.75 |

All mutation verbs, schemas, and authorization rules must be explicit in OpenAPI. HTTP GET never performs a mutation. Authorization and boundedness apply to both creation and later retrieval of handles, streams, and artifacts.

## 9. Evidence and handoff layout

Proposed additions and outputs:

```text
crates/rockstream-cli/src/component.rs    # ConsoleComponent in NodeRuntime
crates/rockstream-cli/src/console.rs      # API handlers and bounded adapters
api/console/v1/                          # Generated OpenAPI/event schemas
packages/rockstream-console-client/       # Generated client; no UI dependencies
tools/ui-readiness-harness/               # Public-interface scenario runner
benchmarks/ui-readiness/                  # Frozen profiles and measurements
docs/ui-readiness/
  capability-map.yaml
  baseline.md
  decisions/
  limits.md
  compatibility.md
  integration-guide.md
  evidence-manifest.json
sign-offs/ui-enablement/
  M0.md ... M11.md
scripts/check-ui-readiness.sh
```

Every evidence record identifies the criterion, repository revision, release artifact digest, configuration/profile digest, topology, backend, exact invocation, test result, and retained raw artifacts. Map one criterion to multiple tests where necessary. Retain complete expected response schemas, authorized rows, result multisets, audit sequences, and operation transitions; counts and selected rows alone do not establish correctness.

Mocks, simulations, property tests, and formal models remain valuable development tools. Public reachability, identity, distributed effects, and recovery also require real release-process evidence. Record unavailable infrastructure as blocked, not passed. Any coordination or durable-format change must retain the relevant existing formal/simulation and compatibility obligations. Follow the repository's evidence discipline and preserve existing roadmap criteria. [R6]

**Final handoff condition:** A frontend engineer can use the generated client and documented setup to complete J1–J8, including their failure paths, without privileged bypasses, private APIs, fixture state, or new backend design decisions. That is the point at which implementation of the proposed UI should begin.

## 10. References

Repository references below are pinned to the inspected baseline. Planned roadmap documents describe obligations, not completed capabilities.

| Reference | Source |
|---|---|
| R1 | [Management API reference][R1] |
| R2 | [Management protocol][R2] |
| R3 | [Pgwire conformance][R3] |
| R4 | [Capability matrix][R4] |
| R5 | [Existing ACL role types][R5] |
| R6 | [Implementation and evidence rules][R6] |
| R7 | [Planned security coherence][R7] |
| R8 | [Planned operational observability][R8] |
| R9 | [Planned resource control][R9] |
| R10 | [Active roadmap][R10] |
| R11 | [Planned durable distributed lifecycle][R11] |
| R12 | [Lifecycle and degradation types][R12] |
| S1 | [OAuth security best current practice][S1] |
| S2 | [OpenAPI specifications][S2] |

[R1]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/reference/management-api.md "Management API reference"
[R2]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/crates/rockstream-management-proto/proto/management/v1/management.proto "Management protocol"
[R3]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/pgwire-conformance.md "Pgwire conformance"
[R4]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/capability-matrix.md "Capability matrix"
[R5]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/crates/rockstream-types/src/acl.rs "Existing ACL role types"
[R6]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/implementation-plans/README.md "Existing implementation and evidence rules"
[R7]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/implementation-plans/v0.73.md "Planned security coherence"
[R8]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/implementation-plans/v0.71.md "Planned operational observability"
[R9]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/implementation-plans/v0.72.md "Planned resource control"
[R10]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/ROADMAP.md "Active roadmap"
[R11]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/docs/implementation-plans/v0.68.md "Planned durable distributed lifecycle"
[R12]: https://github.com/trickle-labs/rockstream/blob/7bc67c4f55e11f2c293a414ae4a32d501efc1566/crates/rockstream-types/src/view_lifecycle.rs "Lifecycle and degradation types"
[S1]: https://www.rfc-editor.org/rfc/rfc9700.html "OAuth 2.0 Security Best Current Practice"
[S2]: https://spec.openapis.org/ "OpenAPI specifications"
