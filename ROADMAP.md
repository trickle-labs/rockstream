# RockStream Roadmap
## Usability, Durability, and Technical Maturity Before v1.0

**Status:** Active  
**Supersedes:** [NEW_ROADMAP.md](NEW_ROADMAP.md)  
**Baseline:** v0.59.24  
**v1.0 status:** Unscheduled indefinitely  
**Roadmap range:** v0.60 through v0.75  
**Primary objective:** Turn RockStream from a technically ambitious IVM engine with uneven product surfaces into a coherent, trustworthy, durable, operable database system.

---

## 1. Purpose

RockStream has reached the point where adding more isolated capabilities is less valuable than making the existing system work coherently from the perspective of an ordinary user and operator.

This roadmap supersedes [NEW_ROADMAP.md](NEW_ROADMAP.md) (which governed v0.1 through the v0.59.24 qualification program). The current v0.59 program deliberately freezes its scope and directs subsequent work to v0.60+. The workspace is currently versioned at `0.59.24`. The existing roadmap also explicitly states that promotion to v1.0 is unscheduled.  

This roadmap therefore does **not** treat v1.0 as the target.

Instead, the target is a succession of increasingly useful 0.x releases, each of which must leave RockStream materially more coherent and usable than the previous one.

The central principle is:

> **A smaller feature that works completely is more valuable than a larger feature surface that works only through selected internal paths.**

The roadmap focuses on five properties:

1. **Truthfulness** — successful commands must represent real system state.
2. **Usability** — a new user must be able to create, run, query, inspect, stop, and recover RockStream without understanding its internals.
3. **Durability** — metadata and maintained state must survive real process failure.
4. **Operational integrity** — distributed operations must correspond to durable state machines rather than simulated outcomes.
5. **Technical leverage** — internal interfaces should make later improvements easier instead of adding another compatibility layer.

---

# 2. Roadmap Philosophy

## 2.1 No feature-count milestones

Versions are not considered complete because a parser, type, trait, protocol message, or internal API exists.

A capability is complete only when its intended user path is reachable through the production artifact and its expected failure modes have also been demonstrated.

For example:

```text
parser exists
    != feature exists

Rust API exists
    != user capability exists

CLI prints success
    != operation succeeded

component unit tests pass
    != vertical product path works
```

The primary proof becomes the black-box path:

```text
published binary/container
        ↓
public configuration
        ↓
public network/client interface
        ↓
real runtime
        ↓
durable storage
        ↓
process failure/restart
        ↓
same externally observable result
```

---

## 2.2 Standalone before distributed breadth

`--role=all` becomes the reference deployment.

Standalone mode is not a toy mode. It is the smallest complete RockStream deployment and should exercise the same core components used by distributed deployments.

The priority order is:

```text
correct engine
    ↓
truthful product surface
    ↓
durable standalone
    ↓
operable standalone
    ↓
real distributed data path
    ↓
real distributed lifecycle
    ↓
external ingestion
    ↓
performance/security/upgrade maturity
```

Distributed functionality may remain available throughout this roadmap, but it should be clearly identified as experimental until the corresponding milestones below are complete.

---

## 2.3 v1.0 is not a roadmap dependency

No milestone in this document may contain acceptance criteria such as:

- "ready for v1,"
- "v1 release candidate,"
- "last feature before v1,"
- "1.0 compatibility freeze."

RockStream can remain at 0.x for as long as that is useful.

The final milestone in this roadmap, v0.75, establishes a **stable technical-preview contract**, not a v1 release candidate.

Further versions such as v0.76, v0.80, or v0.100 may be introduced later if justified.

There is no requirement to numerically release every minor version up to v0.99 before v1.0.

---

# 3. Versioning and Sizing

Each minor version is a product milestone.

Examples:

```text
v0.60
v0.61
v0.62
```

Large versions may be divided into independently reviewable patch milestones:

```text
v0.63.1
v0.63.2
v0.63.3
```

The existing roadmap's split discipline should remain:

A mandatory piece of work should become a sub-version when it:

- exceeds roughly two person-weeks,
- introduces a durable on-disk or wire format,
- introduces a new distributed protocol,
- requires independent formal modeling,
- or is large enough that it cannot be reviewed as one coherent change.

Version numbers represent ordered evidence milestones rather than time commitments.

---

# 4. Common Definition of Done

Every version from v0.60 onward must satisfy the following baseline.

## Product proof

At least one test must exercise the new capability through the same public interface used by an external user.

Mock-only success is insufficient.

## Failure proof

Every new user-visible operation must include at least one meaningful failure case.

Examples include:

- unavailable service,
- malformed configuration,
- incompatible schema,
- unavailable worker,
- interrupted operation,
- stale lease,
- invalid credentials,
- storage failure.

## Durability proof

Any feature claiming persistence must be tested across full process destruction and reconstruction.

Dropping and recreating a Rust object is insufficient when the claim is process durability.

## Boundedness

Every queue, waiter registry, request cache, result accumulator, retry queue, migration buffer, or scan window must have:

- a named limit,
- an observable fill level,
- defined overflow behavior,
- and a backpressure or error policy.

## Product truthfulness

No production command may silently substitute:

- fixture data,
- predefined topology,
- fake timing information,
- synthetic resource usage,
- mock catalog contents,
- or constructed successful outcomes.

Test fixtures must live in test-support code or explicit demo paths.

## Documentation

Documentation must describe the actual reachable product surface.

Examples shown in primary documentation must execute in CI against a release-mode binary.

## Compatibility

Changes to persistent formats or public protocols require:

- a version identifier,
- an upgrade story,
- incompatibility detection,
- and a test demonstrating both supported and rejected combinations.

---

# 5. Roadmap Overview

| Version | Theme | Primary Outcome |
|---|---|---|
| **v0.60** | Product Truth | Production CLI stops fabricating system state |
| **v0.61** | Golden Path | A generated local project actually works end-to-end |
| **v0.62** | Configuration & Lifecycle | One authoritative configuration and runtime lifecycle |
| **v0.63** | Durable Catalog | DDL and metadata survive process destruction |
| **v0.64** | SQL Execution Integrity | Standard PostgreSQL DML replaces ad-hoc command parsing |
| **v0.65** | Standalone Recovery | Complete standalone crash recovery, backup, and restore |
| **v0.66** | Management Plane | Real typed management API and truthful operational CLI |
| **v0.67** | Distributed Data Plane | Row traffic leaves the control plane |
| **v0.68** | Distributed Lifecycle | Migration, drain, and failover become durable sagas |
| **v0.69** | PostgreSQL CDC | First complete external ingestion golden path |
| **v0.70** | Kafka | Second complete external ingestion golden path |
| **v0.71** | Observability | Operators can explain health, lag, state, and failures |
| **v0.72** | Resource Control | Bounded execution and reproducible capacity behavior |
| **v0.73** | Security | Authentication and identity become production-coherent |
| **v0.74** | Upgrade Compatibility | Rolling/versioned upgrades and durable format migration |
| **v0.75** | Stable Technical Preview | Long-lived 0.x compatibility and qualification contract |

---

# 6. v0.60 — Product Truth

## Focus

Remove the distinction between what RockStream appears to do and what it actually does.

This version is intentionally subtractive.

No substantial new database functionality should be added.

## User outcome

When a production `rockstream` command reports something, the result comes from:

- a live RockStream server,
- durable RockStream state,
- or an explicitly labeled demo environment.

Unavailable functionality fails rather than returning plausible sample data.

---

## Implementation Plan

### 6.1 Separate production clients from test clients

Introduce internal interfaces:

```rust
trait CatalogApi { ... }
trait TopologyApi { ... }
trait OperationApi { ... }
trait StorageAdminApi { ... }
```

Implement:

```text
RemoteCatalogClient
RemoteTopologyClient
RemoteOperationClient

MockCatalogClient
MockTopologyClient
MockOperationClient
```

Production binaries may instantiate only the `Remote*` implementations unless the user explicitly invokes demo functionality.

Move fixture/default constructors to:

```text
rockstream-test-support
```

---

### 6.2 Remove predefined topology responses

Eliminate production fallbacks that invent:

- workers,
- shards,
- resource quotas,
- leader identities,
- migration duration,
- cluster size,
- view definitions.

A failed control connection should result in an error resembling:

```text
RS-0004: cannot reach RockStream control service at 127.0.0.1:9200

Next steps:
  - verify `rockstream start` is running
  - check `rockstream config print-effective`
  - verify the configured control endpoint
```

---

### 6.3 Separate demo behavior

Explicitly establish:

```text
rockstream demo
rockstream demo orders
```

Demo mode may use generated/sample state.

Every demo response should identify itself:

```text
Mode: demo
Durability: ephemeral
Topology: simulated
```

Demo behavior must never be reachable accidentally from:

```text
rockstream status
rockstream view list
rockstream shard list
```

---

### 6.4 Simplify the top-level CLI

Target command hierarchy:

```text
rockstream start
rockstream shell
rockstream query
rockstream status

rockstream project ...
rockstream config ...
rockstream admin ...
rockstream dev ...
rockstream demo ...
```

Move specialized commands:

```text
manifest
qualify
debug
simulation helpers
offline SQL compilation
evidence generation
```

under:

```text
rockstream dev
```

Move:

```text
worker drain
shard migration
Raft inspection
checkpoint administration
```

under:

```text
rockstream admin
```

---

### 6.5 Typed CLI values

Replace stringly typed values with Clap enums wherever possible.

Examples:

```rust
enum Role {
    All,
    Gateway,
    Worker,
    Control,
}

enum AuthMode {
    Off,
    Scram,
    Md5,
    Oidc,
    Mtls,
}
```

Reuse domain types rather than defining independent CLI strings.

---

## Required Proof

The acceptance suite must show:

```text
1. Start no RockStream services.
2. Run `rockstream status`.
3. Command fails nonzero.
4. No worker/shard/status data is printed.

5. Start a one-node RockStream instance.
6. Run `rockstream status`.
7. Returned values correspond to the live node.

8. Shut the node down.
9. Run the same command.
10. It fails rather than retaining or fabricating previous values.
```

A repository search gate should fail CI if known fixture constructors become reachable from production CLI dispatch.

---

## Non-Goals

- Durable catalog.
- New SQL.
- New connectors.
- Distributed performance work.
- New authentication modes.
- New operators.

---

# 7. v0.61 — Golden Path and Project Tooling

## Focus

Make the first 15 minutes with RockStream reliable.

## User outcome

A user can execute:

```bash
rockstream project new sales
cd sales
rockstream start
rockstream project apply
rockstream project verify
```

and obtain a real maintained materialized view.

No external `psql` installation is required.

---

## Implementation Plan

### 7.1 Introduce the project manifest

Generate:

```text
sales/
├── rockstream.toml
├── project.toml
├── schema.sql
├── data/
│   └── seed.csv
├── queries/
│   └── verify.sql
└── README.md
```

Example `project.toml`:

```toml
version = 1
name = "sales"

[[apply]]
file = "schema.sql"

[[seed]]
table = "orders"
file = "data/seed.csv"
format = "csv"

[[verify]]
query = """
SELECT store_id, total_amount
FROM sales_by_store
ORDER BY store_id
"""
expected = """
100|120
200|40
"""
```

---

### 7.2 Add an embedded PostgreSQL client

Implement:

```text
rockstream shell
rockstream query
```

using the same PostgreSQL protocol an external client uses.

Required features:

- simple query execution,
- prepared/extended query execution,
- multiline shell input,
- timing,
- tabular output,
- JSON output,
- CSV output,
- SQL-file execution,
- RockStream error-code rendering.

---

### 7.3 Add project apply

`rockstream project apply` must:

1. establish a real pgwire connection,
2. execute schema files,
3. ingest seed data,
4. stop on the first unexpected SQL error,
5. record applied project metadata.

It must be idempotent where explicitly supported.

---

### 7.4 Add project verify

Verification must execute real queries and compare structured values rather than string formatting where possible.

A missing dependency may never produce success.

Example:

```text
FAILED verification `sales_by_store`
expected rows: 2
actual rows:   1
```

---

### 7.5 Keep only one supported project template

The initially supported template is:

```text
local
```

Kafka and PostgreSQL CDC templates move to:

```text
examples/experimental/
```

until their respective roadmap milestones.

---

## Required Proof

The test must build the release binary and execute the complete project workflow without direct Rust APIs.

It must verify:

```text
CREATE TABLE
CREATE MATERIALIZED VIEW
INSERT/import
SELECT exact contents
UPDATE
SELECT changed contents
DELETE
SELECT changed contents
```

The generated README commands must themselves be executed by CI.

---

## Non-Goals

- Kafka template.
- PostgreSQL CDC template.
- Production clustering.
- GUI.
- Cloud deployment automation.

---

# 8. v0.62 — Unified Configuration and Node Lifecycle

## Focus

Create one authoritative description of a RockStream node.

## User outcome

A single configuration file can explain exactly what the process will start and how it will behave.

---

## Implementation Plan

### 8.1 Introduce `NodeConfig`

Target structure:

```rust
struct NodeConfig {
    version: ConfigVersion,
    node: NodeSection,
    gateway: GatewaySection,
    control: ControlSection,
    worker: WorkerSection,
    storage: StorageSection,
    metrics: MetricsSection,
    auth: AuthSection,
    logging: LoggingSection,
    runtime: RuntimeSection,
}
```

---

### 8.2 Make service addresses configuration properties

Example:

```toml
version = 1

[node]
role = "all"

[gateway]
listen_addr = "127.0.0.1:5432"

[control]
listen_addr = "127.0.0.1:9200"

[storage]
url = "file://./data"

[metrics]
listen_addr = "127.0.0.1:9090"

[auth]
mode = "off"

[logging]
level = "info"
```

---

### 8.3 Preserve a single override order

Binding precedence:

```text
compiled defaults
    <
configuration file
    <
environment
    <
CLI overrides
```

Expose origins:

```bash
rockstream config print-effective --origins
```

Example:

```text
gateway.listen_addr = "0.0.0.0:5432"
  origin: ROCKSTREAM__GATEWAY__LISTEN_ADDR
```

---

### 8.4 Reject unknown configuration

New configuration versions should fail closed on unknown keys.

Do not silently ignore:

```toml
[gateawy]
listen = "..."
```

The result should identify:

- source file,
- line/column where possible,
- unknown key,
- nearby valid keys.

---

### 8.5 Explicit storage URLs

Replace ambiguous primary storage paths with:

```text
file:///var/lib/rockstream
s3://bucket/prefix
```

Only implemented schemes may validate successfully.

---

### 8.6 Refactor runtime startup

Introduce:

```rust
NodeRuntime
Component
ComponentState
```

Core component lifecycle:

```text
Created
Starting
Recovering
Ready
Draining
Stopping
Stopped
Fatal
```

Components:

```text
ControlComponent
WorkerComponent
GatewayComponent
MetricsComponent
ConnectorSupervisor
```

`role=all` composes the same components as separate-process deployment.

---

### 8.7 Remove production no-op startup

A server role should remain alive until:

- explicit shutdown,
- fatal failure,
- or process termination.

Testing shortcuts such as artificial short sleeps should move into the test harness.

---

## Required Proof

The same configuration must produce equivalent behavior when supplied by:

- file,
- environment,
- CLI override.

The test suite must prove that:

- invalid config fails before opening service ports,
- effective config reports origins,
- generated project config validates cleanly,
- every started service reaches the expected lifecycle state,
- `Ready` is never emitted before mandatory recovery completes.

---

# 9. v0.63 — Durable Catalog

## Focus

Make database metadata as durable as maintained state.

## User outcome

After an ungraceful process death, RockStream still knows which tables and materialized views exist.

---

## Implementation Plan

### 9.1 Introduce `CatalogStore`

The catalog must no longer be fundamentally owned by an in-memory structure.

Define durable records for:

```text
database metadata
namespaces
tables
columns
materialized views
inline views
view dependencies
indexes
workloads
source definitions
sink definitions
roles/grants where applicable
compiled-plan metadata
```

---

### 9.2 Version catalog records

Every durable record includes:

```text
catalog_format_version
record_version
object_id
catalog_revision
```

Never infer compatibility merely from successful deserialization.

---

### 9.3 Durable object IDs

Names may change.

Internal identity must not.

Example:

```text
TableId
ViewId
IndexId
SourceId
WorkloadId
```

Identifiers must survive:

```text
restart
rename
catalog compaction
backup/restore
```

---

### 9.4 Implement catalog transaction records

DDL changes should create an atomic logical catalog transaction:

```text
CatalogTxn {
    revision
    operation_id
    mutations[]
    checksum
}
```

Examples:

```text
CREATE TABLE
CREATE MATERIALIZED VIEW
DROP VIEW
CREATE INDEX
```

The catalog should be replayable to a known revision.

---

### 9.5 Snapshot and compact metadata

Maintain:

```text
catalog/log/...
catalog/snapshots/...
```

Recovery:

```text
latest valid snapshot
        +
subsequent valid log entries
        =
current catalog
```

Old records may be compacted only after a durable snapshot is confirmed.

---

### 9.6 Persist compiled-plan identity

Each materialized view records:

```text
SQL definition
normalized SQL or AST identity
logical plan identity
physical plan/compiler version
state layout version
output schema
dependency IDs
```

Restart must verify that the stored state remains interpretable.

---

### 9.7 Adapt PostgreSQL catalogs

`pg_catalog` and `information_schema` become projections over the durable catalog.

They must not constitute an independent metadata truth.

---

## Required Proof

Black-box crash test:

```text
1. Start RockStream.
2. CREATE TABLE.
3. CREATE MATERIALIZED VIEW.
4. INSERT rows.
5. Verify result.
6. SIGKILL process.
7. Start a completely new process from the same data directory.
8. Query pg_catalog.
9. Query the materialized view.
10. Verify definitions and exact result.
```

Repeat with:

- index,
- workload assignment,
- source metadata where supported.

---

# 10. v0.64 — SQL Execution Integrity

## Focus

Replace RockStream-specific textual DML shortcuts with standard SQL semantics.

## User outcome

Ordinary PostgreSQL clients can issue ordinary PostgreSQL-style `UPDATE` and `DELETE`.

---

## Implementation Plan

### 10.1 One parser path

Every SQL statement must pass through a shared parser/AST layer.

Remove execution dispatch based primarily on patterns such as:

```rust
starts_with("update ")
```

The architecture becomes:

```text
SQL text
  ↓
PostgreSQL AST
  ↓
RockStream typed statement
  ↓
semantic validation
  ↓
logical operation
  ↓
execution
```

---

### 10.2 Typed DML representation

Example:

```rust
enum DmlStatement {
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}
```

Predicates use typed expressions rather than comma-separated key/value strings.

---

### 10.3 Standard predicate behavior

Support:

```sql
UPDATE orders
SET amount = 100
WHERE order_id = 1
  AND store_id = 100;
```

Support a clearly documented subset of general predicates.

Unsupported expressions return an explicit capability error instead of being misparsed.

---

### 10.4 Correct update deltas

An update must resolve to:

```text
-old row
+new row
```

within one logical commit.

Materialized views observe only the resulting committed epoch.

---

### 10.5 Primary key semantics

Add first-class key metadata.

Mutable tables should either:

- have a declared primary key,
- or have explicitly documented heap-row identity semantics.

Prefer requiring keys for the durable mutable-table path.

---

### 10.6 NULL and coercion correctness

Define and test:

- `NULL`,
- `IS NULL`,
- `IS NOT NULL`,
- typed parameters,
- numeric coercion,
- text escaping,
- UUID,
- timestamp types,
- decimal values,
- boolean values.

---

### 10.7 Simple/extended protocol parity

The same SQL must produce equivalent behavior using:

```text
simple protocol
prepared statement
bound parameters
```

---

## Required Proof

Run a PostgreSQL compatibility corpus using `tokio-postgres` and at least one ordinary external ecosystem client.

The test must verify resulting state rather than merely command completion.

Required scenarios include:

```text
multi-row UPDATE
UPDATE RETURNING
DELETE
DELETE RETURNING
no-match UPDATE
NULL predicate
prepared UPDATE
transaction rollback
transaction commit
materialized-view propagation
restart after DML
```

---

# 11. v0.65 — Complete Standalone Recovery, Backup, and Restore

## Focus

Make standalone RockStream a genuinely durable database deployment.

## User outcome

The supported local deployment can be crashed, backed up, restored elsewhere, and verified.

---

## Implementation Plan

### 11.1 Define the standalone recovery contract

Recovery must reconstruct:

```text
catalog
base-table state
operator state
view output
frontier/epoch
indexes
source cursors
idempotency metadata
```

No mandatory state may exist only in process memory.

---

### 11.2 Recovery state machine

Startup:

```text
OpeningStorage
    ↓
RecoveringCatalog
    ↓
RecoveringEpoch
    ↓
RecoveringOperators
    ↓
ValidatingState
    ↓
Ready
```

Management readiness returns false during recovery.

---

### 11.3 Add backup manifest

Example:

```json
{
  "format_version": 1,
  "catalog_revision": 192,
  "checkpoint_id": 81,
  "frontier": 8412,
  "storage_format": 3,
  "files": [],
  "checksum": "..."
}
```

---

### 11.4 Add backup commands

```bash
rockstream admin backup create <destination>
rockstream admin backup inspect <destination>
rockstream admin backup verify <destination>
rockstream admin restore <source>
```

Restore must target an empty or explicitly replaceable destination.

---

### 11.5 Point-in-time consistency

Backup must identify one logically consistent:

```text
catalog revision
+
database checkpoint
+
frontier
```

No backup may mix catalog metadata from one logical point with state from another.

---

### 11.6 Corruption detection

Startup and backup verification should detect:

- missing files,
- invalid checksums,
- incompatible formats,
- incomplete manifests,
- broken catalog references.

Fail closed.

---

## Required Proof

At minimum:

```text
hard kill during ordinary write
hard kill during view maintenance
hard kill during checkpoint
hard kill during catalog DDL
hard kill during backup
restore to new filesystem path
verify exact view multiset
```

The restored instance must have no dependency on files outside the backup.

---

# 12. v0.66 — Real Management Plane

## Focus

Create a real typed operational interface shared by CLI and automation.

## User outcome

`rockstream status` and administrative commands report actual runtime state without speaking private worker protocols directly.

---

## Implementation Plan

### 12.1 Introduce a management service

Prefer a typed RPC interface using the project's existing protobuf/tonic dependencies.

Separate:

```text
Management API
Control-worker protocol
Data plane
PostgreSQL client protocol
```

Do not use one protocol for all four concerns.

---

### 12.2 Initial management API

Read operations:

```text
GetClusterStatus
ListNodes
GetNode
ListShards
GetShard
ListOperations
GetOperation
GetConfigSummary
GetCapabilities
GetHealth
```

Mutation operations:

```text
DrainWorker
MigrateShard
CreateBackup
CancelOperation
```

---

### 12.3 Operation IDs

Long-running administration returns:

```text
OperationId
```

Example:

```bash
$ rockstream admin shard migrate 7 --to worker-3
operation op_01J... accepted
```

Then:

```bash
rockstream admin operation show op_01J...
```

---

### 12.4 Typed operation lifecycle

```text
Pending
Running
Waiting
Succeeded
Failed
Cancelled
```

Status includes:

```text
started_at
updated_at
progress
phase
error_code
next_steps
```

---

### 12.5 Idempotency

Administrative mutation requests must accept an operation/idempotency identifier.

Repeating the same accepted operation must not create a second conflicting migration or backup.

---

### 12.6 CLI becomes an API client

Production CLI paths should contain minimal business logic.

Pattern:

```text
CLI parse
  ↓
management request
  ↓
server-side validation/action
  ↓
structured response
  ↓
render
```

---

## Required Proof

Run the CLI against:

- a real standalone process,
- a real multi-process test cluster.

For every status field shown by the CLI, tests must mutate the actual underlying state and verify the field changes.

---

# 13. v0.67 — Direct Distributed Data Plane

## Focus

Remove row-level workload traffic from the control service.

## User outcome

Distributed execution behaves like an actual distributed data engine rather than a control-plane-mediated execution prototype.

---

## Implementation Plan

### 13.1 Define protocol boundaries

Control plane owns:

```text
membership
leases
placement
deployment metadata
frontiers
operations
```

Data plane owns:

```text
record batches
operator exchange
source delta delivery
result delivery
```

---

### 13.2 Introduce versioned frames

Control envelope:

```protobuf
message ExchangeFrame {
  uint32 protocol_version = 1;
  uint64 workload_id = 2;
  uint64 shard_id = 3;
  uint64 operator_id = 4;
  uint64 epoch = 5;
  uint64 lease_token = 6;
  bytes schema_fingerprint = 7;
  bytes payload = 8;
  bytes checksum = 9;
}
```

---

### 13.3 Arrow IPC payloads

Replace TSV row representation on the distributed hot path with Arrow record batches.

Avoid:

```text
Arrow
→ String
→ TSV
→ JSON
→ TCP
→ JSON
→ TSV split
→ parse
→ Arrow
```

Target:

```text
Arrow
→ IPC frame
→ bounded transport
→ Arrow
```

---

### 13.4 Persistent connections

Do not create a new TCP connection for each data request.

Use long-lived connections with:

- multiplexing,
- heartbeats,
- deadlines,
- reconnection,
- connection generation IDs.

---

### 13.5 Backpressure

Use explicit credit or bounded-channel flow control.

Configuration:

```text
max_inflight_batches
max_inflight_bytes
max_batch_bytes
max_pending_requests
```

Senders must stop when receivers cannot keep up.

---

### 13.6 Durable request identity

Execution identity includes:

```text
request_id
epoch
shard
lease
operator
payload digest
```

Workers must recognize safe replay.

---

### 13.7 Remove control-plane output accumulation

The control plane should record compact execution metadata rather than complete query output histories.

Authoritative query results belong in durable shard state.

---

## Required Proof

Measurements must show that increased row throughput does not produce proportional control-node row-processing CPU.

Fault scenarios:

```text
worker disconnect mid-frame
gateway reconnect
duplicate frame
stale lease frame
out-of-order epoch
oversized frame
backpressure saturation
control-node restart during active data traffic
```

All must have deterministic outcomes.

---

# 14. v0.68 — Durable Distributed Lifecycle

## Focus

Unify migration, worker drain, rebalancing, and failover around one real migration implementation.

## User outcome

When RockStream says a shard migrated, its state actually moved and was verified.

---

## Implementation Plan

### 14.1 One migration saga

Binding states:

```text
PLANNED
SNAPSHOTTING
COPYING
DUAL_WRITING
CATCHING_UP
FENCING_OLD
CUTOVER
VERIFYING
GC_ELIGIBLE
DONE
```

Also:

```text
ABORTED
FAILED
```

---

### 14.2 Persist before side effects

Each phase records durable intent before initiating externally visible work.

Pattern:

```text
persist desired phase
    ↓
perform idempotent side effect
    ↓
persist completion/progress
```

---

### 14.3 Use real copy path

Worker drain must invoke the same checkpoint/copy/verify machinery as manual migration.

No state-machine-only simulation of copying is permitted.

---

### 14.4 Resume after control failure

A new control leader/process must load active migrations and determine:

```text
phase
completed effects
outstanding effects
current leases
current frontiers
```

It then safely resumes.

---

### 14.5 Progress accounting

Expose:

```text
copied_bytes
total_bytes
copied_rows
estimated_rows
donor_frontier
recipient_frontier
lag
current_phase
verification_progress
```

No fabricated duration values.

---

### 14.6 Cutover fencing

The old owner stops authoritatively writing before the new owner becomes the sole authoritative writer.

The safety property is:

```text
at most one authoritative writable lease
```

throughout the process.

---

### 14.7 Verification and rollback

On divergent recipient state:

```text
do not GC donor
return to a safe repair phase
record exact failure
preserve authoritative data
```

---

## Required Proof

A fault-injection matrix kills:

- control process,
- donor worker,
- recipient worker,

at every migration phase.

After recovery:

```text
exact data multiset
one authoritative lease
no lost committed epoch
no duplicated logical write
operation reaches DONE or explicit FAILED
```

Distributed lifecycle protocols remain subject to simulation/formal verification requirements.

---

# 15. v0.69 — PostgreSQL CDC Golden Connector

## Focus

Make one external ingestion mechanism complete.

PostgreSQL CDC is selected first because it strongly exercises:

- snapshots,
- offsets,
- updates,
- deletes,
- schema identity,
- transaction boundaries,
- restart,
- duplicate delivery.

## User outcome

A user can point RockStream at PostgreSQL and maintain a RockStream materialized view continuously.

---

## Implementation Plan

### 15.1 Restore supported project template

```bash
rockstream project new analytics --template postgres-cdc
```

The generated project must be executable.

---

### 15.2 Source DDL/API contract

Define one canonical way to create the source.

Do not expose two competing definitions through config and SQL unless their ownership is explicit.

A source contains:

```text
upstream endpoint
publication
replication slot
table mapping
schema policy
credential reference
snapshot policy
```

---

### 15.3 Snapshot + WAL boundary

Guarantee:

```text
snapshot rows
+
subsequent WAL changes
=
exact upstream history from the chosen logical boundary
```

There may be neither a gap nor double-application at handoff.

---

### 15.4 Transaction-aware CDC

Respect upstream transaction grouping.

One upstream transaction should not become arbitrarily visible as partially applied RockStream state.

---

### 15.5 Persist LSN state

Persist:

```text
received LSN
applied LSN
durable LSN
published frontier
```

Acknowledgement to PostgreSQL may not advance beyond durable RockStream state.

---

### 15.6 Schema change policy

Explicitly classify changes:

```text
compatible
requires rebuild
unsupported
```

Examples:

```text
add nullable column
rename column
type widening
drop referenced column
primary-key change
```

Unsupported changes block the affected relation with a specific recovery procedure.

---

### 15.7 Backpressure

When RockStream cannot keep up, WAL consumption must respect bounded internal buffers.

Expose source lag.

---

## Required Proof

TestContainers scenario:

```text
start Postgres
seed large snapshot
start RockStream
create source
create materialized view
complete snapshot
perform INSERT
perform UPDATE
perform DELETE
perform multi-row transaction
restart RockStream
repeat transactions
disconnect PostgreSQL temporarily
reconnect
apply compatible schema evolution
attempt incompatible evolution
verify exact maintained result
```

The resulting template becomes Supported only after this complete test passes.

---

# 16. v0.70 — Kafka Golden Connector

## Focus

Make Kafka the second complete external ingestion mechanism.

## User outcome

A user can maintain materialized views from real Kafka topics with documented offset and replay semantics.

---

## Implementation Plan

### 16.1 Supported Kafka project template

```bash
rockstream project new events --template kafka
```

It provisions a reproducible Redpanda/Kafka development environment and verifies real records.

---

### 16.2 Partition identity

Persist source identity as:

```text
cluster
topic
partition
offset
```

Topic recreation or incompatible identity changes must not silently reuse old offsets.

---

### 16.3 Commit semantics

Define:

```text
Kafka offset considered committed
    iff
corresponding RockStream epoch is durable
```

A process crash between processing and Kafka acknowledgement must safely replay.

---

### 16.4 Multi-partition epochs

Define how records from multiple partitions join one RockStream epoch.

Avoid global unbounded waiting for idle partitions.

---

### 16.5 Rebalance handling

Consumer-group rebalance must:

- stop processing revoked partitions,
- durably complete or discard incomplete work according to the protocol,
- resume assigned partitions from durable offsets.

---

### 16.6 Poison records

Define bounded dead-letter behavior.

A bad record must not cause:

- infinite retry,
- memory accumulation,
- silent skip.

Expose:

```text
topic
partition
offset
error
schema
payload digest
```

with safe payload redaction.

---

## Required Proof

Test scenarios include:

```text
multiple partitions
duplicate replay
consumer restart
broker interruption
group rebalance
poison record
large record
schema decode error
backpressure
view correctness after restart
```

The Kafka template becomes Supported only after these tests pass.

---

# 17. v0.71 — Operational Observability and Diagnostics

## Focus

Make RockStream explain itself.

## User outcome

An operator should be able to answer:

- Is the system healthy?
- Is my view current?
- Why is it behind?
- What is consuming memory?
- Which worker owns this shard?
- Is a migration blocking progress?
- What should I do next?

without reading implementation code.

---

## Implementation Plan

### 17.1 Define health dimensions

Separate:

```text
liveness
readiness
availability
freshness
durability
capacity
degradation
```

A process being alive does not mean workloads are healthy.

---

### 17.2 Status model

Example:

```text
ViewStatus {
    state,
    published_frontier,
    input_frontier,
    freshness_lag,
    freshness_slo,
    state_bytes,
    memory_bytes,
    assigned_shards,
    degradation_reason,
    blocking_operation,
}
```

---

### 17.3 `rockstream doctor`

Implement checks for:

```text
configuration
filesystem/object-store access
network endpoints
PostgreSQL client connectivity
control connectivity
worker connectivity
connector connectivity
certificate validity
storage-format compatibility
catalog recovery
port conflicts
resource limits
```

Output should distinguish:

```text
PASS
WARN
FAIL
```

---

### 17.4 System SQL surface

Expose durable/runtime state using a small canonical catalog.

For example:

```text
rockstream_catalog.nodes
rockstream_catalog.shards
rockstream_catalog.operations
rockstream_catalog.views
rockstream_catalog.sources
rockstream_catalog.checkpoints
```

Avoid generating a large pseudo-catalog whose fields are not authoritative.

---

### 17.5 Structured logging

Every significant request includes correlated identifiers:

```text
request_id
operation_id
workload_id
view_id
shard_id
worker_id
epoch
```

---

### 17.6 Metrics

Minimum classes:

```text
ingest rows/bytes
execution rows
epoch duration
frontier lag
state size
memory
exchange bytes
exchange backpressure
checkpoint duration
migration progress
connector lag
errors by RS code
```

---

## Required Proof

For a suite of deliberately degraded scenarios, the CLI and system catalog must report the correct reason.

Examples:

```text
source disconnected
worker unavailable
migration active
state budget exceeded
view recovering
connector lagging
storage unavailable
```

---

# 18. v0.72 — Resource Control and Capacity Behavior

## Focus

Make resource limits meaningful and predictable.

## User outcome

A workload cannot silently consume unbounded memory or overwhelm another workload.

---

## Implementation Plan

### 18.1 Memory accounting

Track major classes:

```text
operator state cache
exchange buffers
source buffers
query execution
catalog/cache
checkpoint buffers
migration buffers
```

Avoid double counting.

---

### 18.2 Workload budgets

Apply configured limits to actual execution.

Example:

```text
soft limit
    → throttle/adapt

hard limit
    → bounded rejection/degradation
```

Never respond to a hard budget violation by continuing unboundedly.

---

### 18.3 Execution scheduling

Introduce or harden fair scheduling between workloads.

Inputs:

```text
priority
memory pressure
freshness SLO
available parallelism
```

---

### 18.4 Adaptive epoch sizing

Allow bounded adaptation based on:

```text
ingest rate
commit latency
state pressure
freshness SLO
```

Always enforce configured lower and upper bounds.

---

### 18.5 Capacity benchmark suite

Canonical workloads:

```text
filter/projection
group aggregate
high-cardinality aggregate
two-way join
window
Postgres CDC
Kafka ingestion
distributed exchange
```

Publish:

```text
rows/s
p50/p95/p99 commit latency
state bytes
RSS
CPU
object-store requests
network bytes
```

---

### 18.6 Regression budgets

Establish per-benchmark accepted noise and regression thresholds.

Performance regressions beyond the selected threshold require explicit approval and evidence.

---

## Required Proof

Sustained overload must reach a bounded degraded state rather than:

```text
OOM
unbounded queue growth
runaway retry
control starvation
```

Performance claims must be reproducible from repository commands.

---

# 19. v0.73 — Security Coherence

## Focus

Turn authentication implementations into complete identity systems.

## User outcome

Every advertised auth mode is either correctly configurable or explicitly unavailable.

---

## Implementation Plan

### 19.1 Remove fallback secrets

No production OIDC/JWT path may use built-in default signing secrets.

No password mode may start in an apparently enabled state with no meaningful credential store.

---

### 19.2 Persist roles and grants

Identity metadata belongs in the durable catalog.

Support an intentionally narrow initial model:

```text
login role
admin role
read role
write role
```

Add finer authorization only when justified.

---

### 19.3 Authentication capability validation

Startup should validate selected mode.

Examples:

```text
SCRAM:
  role store available

OIDC:
  issuer configured
  audience configured
  JWKS reachable/cached

mTLS:
  certificate
  private key
  trust roots
```

---

### 19.4 Separate external and internal identity

Client authentication and node authentication are different concerns.

Explicitly distinguish:

```text
gateway TLS/auth
control-worker mTLS
worker-worker mTLS
management API auth
```

---

### 19.5 Secret references

Configuration and source definitions refer to:

```text
secret://name
```

rather than embedding credentials in durable metadata or logs.

---

### 19.6 Audit integrity

Security-sensitive events:

```text
authentication success/failure
authorization denied
role changed
secret rotated
node identity rejected
admin operation started
admin operation completed
```

must be auditable.

---

## Required Proof

Negative tests are mandatory:

```text
expired certificate
wrong CA
wrong node identity
invalid password
invalid SCRAM proof
expired OIDC token
wrong issuer
wrong audience
insufficient role
secret rotation during operation
```

No secret values may appear in ordinary diagnostics or support bundles.

---

# 20. v0.74 — Upgrade and Compatibility

## Focus

Make version evolution a supported technical operation.

## User outcome

Users can determine whether two RockStream versions can safely share state or participate in the same cluster.

---

## Implementation Plan

### 20.1 Publish compatibility dimensions

Version independently:

```text
management protocol
control protocol
data-plane protocol
catalog format
shard/storage format
compiled-plan format
backup format
connector cursor format
```

Do not reuse package version as the only compatibility signal.

---

### 20.2 Compatibility handshake

Node registration advertises:

```text
supported protocol ranges
supported storage format ranges
capability bits
build version
```

Placement rejects incompatible combinations.

---

### 20.3 Catalog migrations

Implement forward migrations:

```text
catalog vN
    →
catalog vN+1
```

Migration steps must be:

- idempotent,
- resumable,
- explicitly versioned.

---

### 20.4 Storage-format policy

Prefer read-old/write-current when practical.

If in-place migration is needed, expose progress through the operation system.

---

### 20.5 Rolling cluster upgrade

Define supported order.

For example:

```text
control followers
control leader handoff
workers
gateways
```

The exact ordering should arise from protocol compatibility rather than documentation guesswork.

---

### 20.6 Downgrade detection

Starting an older binary against unsupported newer state must fail before modifying data.

Example:

```text
RS-xxxx: storage format 7 is newer than this binary's maximum supported format 6
```

---

### 20.7 Backup portability

A supported upgrade must include:

```text
backup with old version
restore/read with new version
```

where compatibility policy promises this behavior.

---

## Required Proof

Upgrade matrix should cover at least:

```text
v0.73 → v0.74 standalone
mixed v0.73/v0.74 workers
control rolling upgrade
gateway rolling upgrade
catalog migration interruption
restart migration
newer-state downgrade rejection
backup from previous version
restore under new version
```

---

# 21. v0.75 — Stable Technical Preview Contract

## Focus

Consolidate everything from v0.60–v0.74 into a stable long-lived 0.x product contract.

This is **not** v1.0 qualification.

## User outcome

RockStream becomes a system that can reasonably be adopted for serious technical evaluation and selected non-critical deployments with clearly understood limitations.

---

## Implementation Plan

### 21.1 Freeze the supported core surface

Define a compact supported contract.

Candidate core:

```text
standalone deployment
PostgreSQL wire protocol
durable tables
durable materialized views
core filter/project/aggregate/join operators
standard DML subset
PostgreSQL CDC
Kafka ingestion
backup/restore
management API
operational CLI
documented distributed experimental profile
```

Capabilities outside this set remain:

```text
Experimental
Maintain
Removed
```

as appropriate.

---

### 21.2 Remove accidental product surface

Review every:

```text
CLI command
config key
SQL statement
system catalog
network port
auth mode
connector
operator
```

Every item receives exactly one classification:

```text
Supported
Experimental
Internal
Deprecated
Removed
```

Internal items should not be presented as ordinary user interfaces.

---

### 21.3 End-to-end qualification scenarios

Maintain a small set of top-level scenarios.

## Q1 — Standalone durability

```text
create schema
load data
maintain views
hard crash
recover
exact state
```

## Q2 — Backup and restore

```text
active workload
consistent backup
restore elsewhere
exact state
```

## Q3 — PostgreSQL CDC

```text
snapshot
live insert/update/delete
restart
schema event
exact state
```

## Q4 — Kafka

```text
multi-partition traffic
rebalance
restart
duplicates
exact state
```

## Q5 — Distributed migration

```text
two workers
active writes
migrate shard
kill donor/control
recover
exact state
single lease
```

## Q6 — Resource pressure

```text
bounded memory
backpressure
recover after pressure
```

## Q7 — Security

```text
authorized access
denied access
rotation
audit
```

## Q8 — Upgrade

```text
previous supported minor
rolling or standalone upgrade
exact state
```

---

### 21.4 Documentation reset

Primary documentation should become small and task-oriented:

```text
README
Quickstart
SQL Guide
Ingestion
Operations
Backup & Restore
Clustering
Security
Compatibility
Known Limitations
Architecture
Contributor Guide
```

Historical implementation plans and qualification evidence should remain available but should not dominate normal navigation.

---

### 21.5 Publish an explicit limitations contract

Example categories:

```text
unsupported SQL semantics
unsupported PostgreSQL extensions
maximum tested cluster size
maximum tested state size
connector limitations
schema-evolution limitations
distributed-profile maturity
upgrade compatibility window
platform support
```

The limitations document is part of the product contract, not an embarrassment to hide.

---

### 21.6 Establish compatibility expectations for future 0.x

Beginning with v0.75:

Within explicitly Supported capabilities:

- patch releases should not intentionally break documented behavior;
- durable format changes require automatic migration or explicit migration tooling;
- removals require deprecation;
- management protocol evolution follows a defined compatibility window;
- SQL deviations require documented error behavior;
- backup compatibility follows a documented support matrix.

Experimental capabilities retain weaker guarantees.

---

## Required Proof

A candidate v0.75 artifact must pass all top-level qualification scenarios using the exact binaries and container images intended for publication.

No scenario may:

- construct internal Rust services directly,
- inject catalog objects through test APIs,
- use mock control clients,
- replace unavailable infrastructure with success,
- skip a required check because an external utility is missing.

The qualification report must distinguish:

```text
PASS
FAIL
NOT SUPPORTED
```

There is no `SKIPPED BUT GREEN`.

---

# 22. Dependency Graph

The intended dependency order is:

```text
v0.60 Product Truth
       │
       ▼
v0.61 Golden Path
       │
       ▼
v0.62 Config + Lifecycle
       │
       ├───────────────┐
       ▼               │
v0.63 Durable Catalog  │
       │               │
       ▼               │
v0.64 SQL Integrity    │
       │               │
       ▼               │
v0.65 Recovery         │
       │               │
       ▼               │
v0.66 Management API ◄─┘
       │
       ▼
v0.67 Data Plane
       │
       ▼
v0.68 Distributed Lifecycle
       │
       ├───────────────┐
       ▼               ▼
v0.69 PostgreSQL CDC   v0.70 Kafka
       │               │
       └───────┬───────┘
               ▼
         v0.71 Observability
               │
               ▼
         v0.72 Resource Control
               │
               ▼
         v0.73 Security
               │
               ▼
         v0.74 Upgrade
               │
               ▼
         v0.75 Stable Preview
```

Some implementation streams may overlap, but sign-off should preserve the logical dependencies.

---

# 23. What Happens to the Previously Deferred Feature Programs?

The repository has previously contemplated substantial post-v0.59 work including richer type semantics, more operators, temporal analytics, recursion, durable-time semantics, stronger transaction models, and multi-region execution.

Those should not be deleted as ideas.

They should be moved into a **Future Research and Admission Queue**.

Candidate areas include:

```text
additional SQL/type completeness
additional operators
session windows
advanced temporal semantics
recursive queries
custom algebra/CRDT semantics
serializable transactions
automatic shard resizing
multi-region execution
additional KMS providers
additional connectors
```

None should automatically receive a roadmap version.

Admission requires answering:

1. What concrete user problem does this solve?
2. Can the existing supported architecture implement it without adding another parallel path?
3. What bounded-state behavior does it require?
4. What durability contract does it introduce?
5. What failure behavior is externally observable?
6. Which public interface exposes it?
7. What black-box test proves it?
8. Will maintaining it materially increase the long-term support burden?
9. Is improving an existing capability more valuable?

A feature that cannot answer these questions stays in research.

---

# 24. Testing Strategy Across the Roadmap

The existing unit, storage, simulation, formal-verification, and integration infrastructure remains valuable, but proof emphasis should change.

The hierarchy becomes:

```text
Layer 1 — algebraic/operator proof
Layer 2 — storage/durability proof
Layer 3 — component/protocol proof
Layer 4 — production-binary black-box proof
Layer 5 — multi-process system proof
```

Higher layers do not replace lower layers.

Lower layers do not substitute for higher layers.

For example:

```text
MigrationCoordinator unit tests
+ FizzBee model
+ SimRuntime test
```

are necessary but do not prove:

```bash
rockstream admin shard migrate ...
```

correctly moves a real live shard between two worker processes.

Both forms of evidence are required.

---

# 25. Repository Architecture Direction

By v0.75, the intended high-level dependency model should resemble:

```text
                    ┌─────────────────────┐
                    │  PostgreSQL Client  │
                    └──────────┬──────────┘
                               │ pgwire
                               ▼
                    ┌─────────────────────┐
                    │      Gateway        │
                    │ SQL / sessions      │
                    └─────┬─────────┬─────┘
                          │         │
               management│         │data
                          │         ▼
                          │   ┌─────────────┐
                          │   │   Workers   │
                          │   │ Operator DAG│
                          │   └──────┬──────┘
                          │          │
                          ▼          ▼
                ┌─────────────┐ ┌─────────────┐
                │   Control   │ │   ShardDb   │
                │ topology    │ │ durable data│
                │ leases      │ └─────────────┘
                │ operations  │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │ Durable     │
                │ Catalog     │
                └─────────────┘
```

The important separation is:

```text
SQL traffic      → gateway
row traffic      → workers
metadata/control → control
persistent data  → shard storage
persistent DDL   → catalog
operations       → management plane
```

The control service should not become the universal transport bus.

---

# 26. Explicit Non-Goals of This Roadmap

Unless required to repair an existing supported contract, v0.60–v0.75 should not prioritize:

- becoming a general-purpose OLTP PostgreSQL replacement,
- complete PostgreSQL syntax compatibility,
- every DataFusion operator,
- arbitrary user-defined code execution,
- a connector marketplace,
- multi-cloud management,
- Kubernetes operators,
- graphical administration,
- multi-region active-active execution,
- globally serializable transactions,
- arbitrary CRDT types,
- custom user-defined merge laws,
- automatic infinite-scale sharding,
- every identity provider,
- every secrets backend,
- every object-storage vendor API.

These may become future programs after admission.

---

# 27. Success Metrics

The roadmap should be judged using product-level measurements.

## First-run success

Target:

```text
clean machine
→ install artifact
→ generate project
→ start
→ apply
→ verify

without source checkout
without Rust toolchain
without psql
without editing generated files
```

---

## Restart integrity

Every core standalone scenario must recover exactly after forced process termination.

---

## CLI truthfulness

Zero production CLI commands should report fixture-derived system state.

---

## Documentation executability

All primary quickstart commands run in automated verification.

---

## Distributed control isolation

Control-plane throughput should be primarily a function of:

```text
nodes
leases
operations
frontier updates
```

not application row throughput.

---

## Boundedness

No known unbounded request/result/migration/exchange accumulator on a supported path.

---

## Connector correctness

For each Supported connector:

```text
source history
==
RockStream committed input history
```

under restart and duplicate delivery.

---

## Operational explainability

For every supported degraded state, the operator receives:

```text
what is wrong
what is affected
why it happened
what RockStream is doing
what the operator should do
```

---

# 28. Recommended Immediate Execution Order

The next engineering work should start with v0.60 rather than attempting to implement several future versions concurrently.

The first concrete sequence is:

```text
1. Inventory every production CLI command.
2. Mark each backing client as live, durable-local, mock, or synthetic.
3. Move mock implementations into rockstream-test-support.
4. Make unimplemented remote commands fail closed.
5. Define the reduced top-level CLI hierarchy.
6. Add black-box CLI tests.
7. Sign off v0.60.
8. Begin the executable local project workflow in v0.61.
```

This work is intentionally mundane compared with factorized joins, Raft, CDC, or incremental window maintenance.

It is also the highest-leverage work in the project.

Once RockStream's public surfaces become trustworthy, every later technical improvement becomes easier to evaluate.

---

# 29. Roadmap Completion State

This roadmap does not end with:

```text
RockStream is finished.
```

It ends with:

> RockStream's supported 0.x surface is coherent enough that future engineering can be chosen based on actual user and operational needs rather than filling gaps between partially connected subsystems.

At v0.75, the correct next action may be:

```text
v0.76 — improve something users are actually struggling with
```

rather than:

```text
v0.76 — add the next item from an old feature wishlist
```

That distinction is intentional.

RockStream should remain at 0.x until changing that designation provides concrete value.

There is no deadline for v1.0.