# RockStream CLI Reference

RockStream ships as a **single binary** named `rockstream`. Every node role is a
flag on this one binary — there is no separate server, worker, or gateway
executable. `main` is always runnable through it.

All inspection subcommands support human-readable tabular text output by default
and structured `--json` output for automated tooling.

## Synopsis

```bash
rockstream --help
rockstream --version
rockstream [--json] [--control <url>] [--storage-dir <dir>] <command>
```

## Global Options

| Option | Default | Description |
|---|---|---|
| `--json` | `false` | Format output as structured JSON. |
| `--control <url>` | — | Control service URL for cluster and shard inspection. |
| `--storage-dir <dir>` | `.` | Local storage directory for reading checkpoints and audit logs. |

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

Inspect view metadata, state, and freshness.

- `rockstream view list` — List all registered materialized views and their current execution state.
- `rockstream view show <name>` — Show detailed view metadata, including definition query, assigned workload, and freshness SLO.
- `rockstream view status [<name>]` — Show view lifecycle and freshness status (optionally filtered by view name).

---

### `rockstream source`

Inspect ingested streaming sources and connectors.

- `rockstream source list` — List all configured ingestion sources, connector types, and statuses.
- `rockstream source show <name>` — Show detailed source configuration, connector options, offsets, and ingest lag.

---

### `rockstream schema`

Inspect schemas and entity definitions.

- `rockstream schema list` — List all tables and views in the active schema.
- `rockstream schema show <name>` — Show column names, types, and nullability for a table or view.

---

### `rockstream workload`

Inspect workload priorities, memory limits, and view assignments.

- `rockstream workload list` — List all defined workloads, priorities, and assigned view counts.
- `rockstream workload show <name>` — Show workload detail, memory limits, freshness SLOs, and assigned views.

---

### `rockstream cluster`

Inspect cluster state, quotas, and worker fleet.

- `rockstream cluster status` — Show cluster leadership, active/healthy worker counts, and engine version.
- `rockstream cluster quotas` — Show total memory budgets, used memory, and parallelism limits.
- `rockstream cluster workers list` — List all registered workers, addresses, availability zones, and health.
- `rockstream cluster workers status [<worker_id>]` — Show detailed worker status, capacity headroom, and lifecycle state.
- `rockstream cluster workers drain --control=<addr> <worker_id>` — Signal a worker to drain before shutdown.

---

### `rockstream shard`

Inspect shard lease ownership and key ranges.

- `rockstream shard list` — List all shards, lease tokens, owner workers, and active key ranges.

---

### `rockstream checkpoint`

Inspect durable cluster checkpoints.

- `rockstream checkpoint list` — List all durable checkpoints, creation timestamps, and shard counts.

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

---

### `rockstream sql`

Offline SQL compilation and lowering inspection.

- `rockstream sql "<query>"` — Parse and lower a raw SQL query into an incremental execution plan without creating or deploying a view.

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
| `RS-1001` | Entity not found (view, table, schema, namespace, or worker ID). | Verify the entity name with `rockstream view list` or `rockstream cluster workers list`. |
| `RS-1005` | Workload not found. | Verify workload name with `rockstream workload list` or create it with `CREATE WORKLOAD`. |
| `RS-1012` | SQL syntax or parsing error. | Check SQL query syntax and column references. |
| `RS-1731` | Control node is not the Raft leader. | Re-resolve control plane leadership and route the request to the active leader. |
| `RS-4009` | Ingestion source not found. | Check source name with `rockstream source list`. |
| `RS-4017` | Removed cold-tier storage configuration was passed. | Remove legacy cold-tier storage flags. |
