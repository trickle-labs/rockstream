# RockStream CLI Reference

## Secret handling

Support bundles contain sanitized metadata and audit events only. Secret
payloads are envelope-encrypted in the catalog and are not included in CLI
output, `SHOW SECRETS`, errors, or generated support bundles.

RockStream ships as a **single binary** named `rockstream`. Every node role is a
flag on this one binary — there is no separate server, worker, or gateway
executable. `main` is always runnable through it.

All inspection subcommands support human-readable tabular text output by default
and structured `--json` output for automated tooling.

## Synopsis

```bash
rockstream --help
rockstream --version
rockstream [--json] [--control <url>] [--storage-dir <dir>] [--identity-user <name>] [--identity-role <role>] <command>
```

## Global Options

| Option | Default | Description |
|---|---|---|
| `--json` | `false` | Format output as structured JSON. |
| `--control <url>` | — | Control service URL for cluster and shard inspection. |
| `--storage-dir <dir>` | `.` | Local storage directory for reading checkpoints and audit logs. |
| `--identity-user <name>` | `rockstream` | Principal presented to control-plane and catalog mutations. |
| `--identity-role <role>` | `viewer` | RBAC role presented to mutations: `viewer`, `pipeline-owner`, or `admin`. |
| `--tls-ca-cert-path <path>` | — | Path to CA root certificate bundle for internal mTLS. |
| `--tls-cert-path <path>` | — | Path to client/node X.509 certificate for internal mTLS. |
| `--tls-key-path <path>` | — | Path to client/node private key for internal mTLS. |
| `--internal-tls-ca-cert-path <path>` | — | Alias for `--tls-ca-cert-path`. |
| `--internal-tls-cert-path <path>` | — | Alias for `--tls-cert-path`. |
| `--internal-tls-key-path <path>` | — | Alias for `--tls-key-path`. |

---

## Commands

### `rockstream start`

Starts a RockStream node.

**Options**

| Option | Required | Default | Description |
|---|---|---|---|
| `--storage <dir>` | yes | — | Local storage directory for node state and artifacts. Created if missing. |
| `--role <role>` | no | `all` | Node role. One of `all`, `control`, `worker`, `gateway`, `frontier`. An unrecognised role, or a role requiring `--control` when it is omitted, is rejected with `RS-0002`. |
| `--control <url>` | no (required for `worker`/`frontier`) | — | Control service URL. Required for the `worker` and `frontier` roles; omitting it is rejected with `RS-0002`. |
| `--auth <mode>` | no | `off` | Authentication mode. One of `off`, `oidc`, `mtls`. |
| `--host-id <id>` | no | — | Stable same-host identity advertised during worker registration. |
| `--availability-zone <az>` | no | — | Availability zone advertised during worker registration. |
| `--metrics-addr <addr>` | no | — | Metrics HTTP server listen address. |
| `--listen <addr>` | no | `127.0.0.1:5432` | PostgreSQL wire gateway listen address. Activates the live gateway server for the `gateway` and `all` roles. |
| `--webhook-listen <addr>` | no | — | Independent HTTP listener for webhook ingestion. |
| `--raft-peers <list>` | no | — | Comma-separated list of the other control nodes in this node's Raft group, `id@host:port,id@host:port`. Only meaningful for `--role=control`. |
| `--raft-node-id <id>` | no (required with `--raft-peers`) | — | This node's id within its Raft group. |
| `--raft-bind <addr>` | no (required with `--raft-peers`) | — | Address this node's Raft peer-RPC listener binds to. |
| `--raft-bootstrap` | no | `false` | Start an election immediately on boot rather than waiting out a randomized timeout. Exactly one node in a freshly-bootstrapped Raft group should set this. |
| `--daemon` | no | `false` | Run the `control` role as a real long-lived daemon that blocks on SIGTERM / Ctrl-C. Only meaningful for `--role=control`. |
| `--control-bind <addr>` | no | `127.0.0.1:8000` | Overrides the address the control-plane's worker-facing `ControlService` TCP listener binds to. |
| `--control-shared-storage <dir>` | no | — | Directory for state shared across control nodes in this node's Raft group. |
| `--query-time-shard-dir <dir>` | no | — | Root directory of a non-local shard included in every query-time scatter read. |

---

### `rockstream view`

Inspect and control materialized view lifecycle, query contents, and subscribe to change streams.

- `rockstream view list` — List all registered materialized views and their current execution state.
- `rockstream view show <name>` — Show detailed view metadata, including definition query, assigned workload, and freshness SLO.
- `rockstream view status [<name>]` — Show view lifecycle, freshness SLO, memory limits, per-stage lag breakdown (`source_lag_ms`, `decode_lag_ms`, `compute_lag_ms`, `alignment_lag_ms`, `sink_lag_ms`, `spill_lag_ms`, `storage_pressure_ms`, `total_lag_ms`), and explainability fields (`degradation_reason`, `reason_code`, `dominant_contributor`, `progress_phase`, `bytes_remaining`, `rows_remaining`, `estimated_remaining_ms`).
- `rockstream view pause <name> [--yes]` — Pause view maintenance and execution.
- `rockstream view resume <name>` — Resume execution for a paused view.
- `rockstream view query <name> [--limit <n>]` — Query current view contents with optional row limit.
- `rockstream view subscribe <name> [--from-epoch <n>] [--snapshot]` — Subscribe to real-time view change stream.

---

### `rockstream source`

Inspect and control streaming ingestion sources and connectors.

- `rockstream source list` — List all configured ingestion sources, connector types, and statuses.
- `rockstream source show <name>` — Show detailed source configuration, connector options, offsets, and ingest lag.
- `rockstream source pause <name>` — Pause ingestion on a streaming source connector.
- `rockstream source resume <name>` — Resume ingestion on a paused streaming source connector.
- `rockstream source drop <name> [--yes]` — Drop an ingestion source connector.

---

### `rockstream schema`

Inspect schemas and create or drop schema tables.

- `rockstream schema list` — List all tables and views in the active schema.
- `rockstream schema show <name>` — Show column names, types, and nullability for a table or view.
- `rockstream schema create <name> [--columns <spec>]` — Create a new table schema with optional column specifications.
- `rockstream schema drop <name> [--yes]` — Drop a schema table.

---

### `rockstream workload`

Manage workload definitions, priority scheduling, and resource limits.

- `rockstream workload list` — List all defined workloads, priorities, and assigned view counts.
- `rockstream workload show <name>` — Show workload detail, memory limits, freshness SLOs, and assigned views.
- `rockstream workload create <name> [--priority <n>] [--freshness-slo-ms <ms>] [--memory-limit <bytes>] [--max-parallelism <n>]` — Create a new workload definition.
- `rockstream workload alter <name> [--priority <n>] [--freshness-slo-ms <ms>] [--memory-limit <bytes>] [--max-parallelism <n>]` — Alter workload configuration and resource constraints.
- `rockstream workload drop <name> [--yes]` — Drop a workload (refused if views are assigned).

---

### `rockstream cluster`

Inspect cluster state, quotas, and worker fleet.

- `rockstream cluster status` — Show cluster leadership, active/healthy worker counts, and engine version.
- `rockstream cluster quotas` — Show total memory budgets, used memory, and parallelism limits.
- `rockstream cluster workers list` — List all registered workers, addresses, availability zones, and health.
- `rockstream cluster workers status [<worker_id>]` — Show detailed worker status, capacity headroom, and lifecycle state.
- `rockstream cluster workers drain <worker_id> [--control <addr>] [--yes]` — Signal a worker to drain shard assignments before shutdown.

---

### `rockstream shard`

Inspect shard lease ownership and key ranges, and trigger shard migrations.

- `rockstream shard list` — List all shards, lease tokens, owner workers, and active key ranges.
- `rockstream shard migrate <shard_id> --to <worker_id> [--yes]` — Migrate a shard lease and state to another worker.

---

### `rockstream checkpoint`

Inspect and restore durable cluster checkpoints.

- `rockstream checkpoint list` — List all durable checkpoints, creation timestamps, and shard counts.
- `rockstream checkpoint restore <checkpoint_id> [--storage <dir>] [--yes]` — Restore a committed checkpoint state to storage.

---

### `rockstream support`

Generate on-demand diagnostic support bundles.

- `rockstream support bundle [--view <name>] [--since <duration>] [--out <path>]` — Generate a diagnostic support bundle with secret redaction and bounded size.

---

### `rockstream resource`

Inspect memory, state, and cost across views, workloads, and the cluster.

- `rockstream resource usage [--workload=<name>]` — Show per-view memory, state bytes, and estimated hourly cost (optionally filtered by workload).
- `rockstream resource cluster` — Show aggregate cluster-wide view counts, workload counts, memory, state bytes, and estimated cost.

---

### `rockstream schema-evolution`

Inspect online schema evolution versions and history.

- `rockstream schema-evolution status` — Show current version, synchronization status, and pending schema alterations.
- `rockstream schema-evolution history` — Show the historical ledger of schema alterations and applied timestamps.

---

### `rockstream audit`

Inspect security and lifecycle audit log events.

- `rockstream audit tail [--max=<n>]` — Tail recent audit log events (bounded by default, maximum 1000).
- `rockstream audit query [--filter=<pattern>] [--max=<n>]` — Query audit log events matching a filter substring.

---

### `rockstream explain`

Inspect the compiled incremental operator plan without deploying.

- `rockstream explain <view>` — Print the annotated `PlanNode` operator tree for a registered view.
- `rockstream explain <view> --estimate` — Print static state memory and epoch throughput estimates for a view.
- `rockstream explain <view> --op-ids` — Print stable Operator IDs and addressability for intermediate pipeline state.

---

### `rockstream sql`

Offline SQL compilation and lowering inspection.

- `rockstream sql "<query>"` — Parse and lower a raw SQL query into an incremental execution plan without creating or deploying a view.

---

### `rockstream debug`

Low-level debugging and intermediate arrangement state inspection.

- `rockstream debug arrangement <view> <op_id> <key> [--epoch=N]` — Inspect intermediate arrangement Z-set state, weight, and key bytes for an operator at a current or historical epoch.

---

## Exit codes

| Exit code | Meaning |
|---|---|
| `0` | Command completed successfully. |
| `1` / non-zero | Command failed. An `RS-XXXX` error code and actionable guidance are printed to stderr. |

## Error codes

| Code | Meaning | Next steps |
|---|---|---|
| `RS-0002` | Invalid CLI arguments, unrecognized node role, or missing required flag. | Check `rockstream --help` for expected flags and valid options. |
| `RS-0003` | Storage or I/O error accessing disk or object storage. | Verify storage path permissions and available disk space. |
| `RS-0004` | Unreachable control plane. | Verify the `--control` service URL and ensure the control node is active. |
| `RS-0005` | Destructive command confirmation required. | Pass `--yes` for script execution or answer `y` at the interactive confirmation prompt. |
| `RS-1001` | Entity not found (view, table, schema, namespace, or worker ID). | Verify the entity name with `rockstream view list` or `rockstream cluster workers list`. |
| `RS-1004` | Entity already exists. | Use a unique name or inspect existing definitions. |
| `RS-1005` | Workload not found. | Verify workload name with `rockstream workload list` or create it with `CREATE WORKLOAD`. |
| `RS-1006` | Workload already exists. | Use distinct name or modify existing workload with `rockstream workload alter`. |
| `RS-1007` | View already paused. | Inspect view state with `rockstream view status`. |
| `RS-1008` | View not paused. | Inspect view state with `rockstream view status`. |
| `RS-1012` | SQL syntax or parsing error. | Check SQL query syntax and column references. |
| `RS-1014` | Workload drop rejected because views are currently assigned. | Reassign or drop assigned views before dropping the workload. |
| `RS-1020` | Operator not found in view pipeline. | Run rockstream explain <view> --op-ids to inspect available operator IDs for this view. |
| `RS-1021` | Arrangement key decoding failed or unsupported family. | Check arrangement key syntax or verify if the operator family key codec is supported. |
| `RS-1731` | Control node is not the Raft leader. | Re-resolve control plane leadership and route the request to the active leader. |
| `RS-2006` | Requested subscription epoch is prior to storage retention window. | Subscribe with `--snapshot` or specify a more recent epoch. |
| `RS-2401` | Permission denied due to insufficient RBAC role. | Request elevated RBAC role (PipelineOwner / Admin) or authenticate under an authorized principal. |
| `RS-4009` | Ingestion source not found. | Check source name with `rockstream source list`. |
| `RS-4017` | Removed cold-tier storage configuration was passed. | Remove legacy cold-tier storage flags. |
| `RS-5030` | In-flight shard migration conflict. | Wait for the active migration to complete before re-initiating shard migration. |
