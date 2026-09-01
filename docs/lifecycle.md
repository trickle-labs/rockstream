# Node Lifecycle, Graceful Shutdown & Health Contract

RockStream nodes implement a deterministic, state-machine-driven lifecycle protocol across all node roles (`gateway`, `worker`, `control`, `frontier`, and standalone `all`).

## 1. Lifecycle State Machine

Each node process progresses through explicit lifecycle states:

```text
[Starting] ───► [Ready] ◄───► [Degraded]
                    │
                    ▼
               [Draining]
                    │
                    ▼
             [ShuttingDown]
                    │
                    ▼
               [Terminated]  (or [Fatal] on watchdog expiry)
```

| Lifecycle State | Description | Readiness Status | Management `/ready` Code |
|---|---|---|---|
| `Starting` | Node process is initializing storage, binding listeners, loading catalog. | Not Ready | HTTP 503 (`starting`) |
| `Ready` | All listeners bound, control plane connected, owned shards assigned and healthy. | Ready | HTTP 200 (`ready`) |
| `Degraded` | Operational but experiencing high replication lag, compaction backlog, or connector backoff. | Ready | HTTP 200 (`ready`) |
| `DependencyLoss`| Lost connection to control plane, lost Raft quorum, or storage backend unreachable. | Not Ready | HTTP 503 (`dependency_loss`) |
| `Draining` | Orderly shutdown triggered via `SIGTERM`, `SIGINT`, or operator drain command. | Not Ready | HTTP 503 (`draining`) |
| `ShuttingDown` | In-flight processing finished; persistent stores and WAL syncing to durable storage. | Not Ready | HTTP 503 (`shutting_down`) |
| `Terminated` | All resources cleanly closed; process exits with code 0. | N/A | Process exits |
| `Fatal` | Shutdown deadline exceeded; self-fenced with `RS-3023` to prevent split-brain. | Fatal | Process forced exit |

---

## 2. Role-Specific Graceful Shutdown Protocols

### Gateway (`--role gateway`)
1. Drops listen socket to reject new incoming TCP client connections immediately.
2. Sends PostgreSQL Notice `57P01` (`admin_shutdown` / `RS-2056`) to all active client sessions.
3. Drains in-flight queries and cursors up to the configured per-query grace window.
4. Cleanly closes live subscriptions and drops client sessions.

### Worker (`--role worker`)
1. Drops readiness probe to HTTP 503 (`draining`) and rejects new shard assignments (`RS-3010`).
2. Completes the currently active micro-batch epoch and flushes SlateDB memtables and WAL to durable storage (LFS / MinIO).
3. Flushes 2PC sink commit batches.
4. Sends explicit shard lease release RPCs to the control plane, making shards immediately acquirable by surviving peer workers without waiting for lease timeouts.
5. Purges and evicts exchange multiplexers and client pools.

### Control Plane (`--role control`)
1. Refuses new worker registrations and DDL operations.
2. If running in a multi-node Raft group and currently leader, cleanly steps down leadership to a follower.
3. Flushes shard lease manager, topology catalog, and migration stores to shared object storage (`--control-shared-storage`).
4. Emits `server.stopping` and `server.stopped` audit records and closes control RPC listeners.

### Frontier Aggregator (`--role frontier`)
1. Computes final watermark snapshot and notifies downstream subscribers.
2. Emits audit events and closes subscriber stream listeners.

### Standalone Profile (`--role all`)
Coordinates ordered shutdown: Gateway client drain -> Worker shard & epoch flush -> Control store persistence -> Metrics listener teardown.

---

## 3. Shutdown Timeout & Watchdog Enforcement

Each node role enforces a configurable shutdown deadline:

- **Configuration**: `cluster.shutdown_timeout_secs` (default: 30s) in `rockstream.toml` or CLI flag `--shutdown-timeout-secs`.
- **Self-Fencing Watchdog**: If in-flight work or slow storage flushes exceed the deadline, the coordinator logs a fatal diagnostic `RS-3023` (`lifecycle.shutdown_deadline_exceeded`), self-fences to prevent split-brain state mutations, and forces process termination.

---

## 4. Structured HTTP Management Endpoints

The management HTTP server (co-located on `--metrics-addr`) exposes structured endpoints:

### Liveness Probe (`GET /live`)
Returns HTTP 200 OK as long as the process runtime is alive:
```json
{"status":"alive"}
```

### Readiness Probe (`GET /ready`)
- Returns `HTTP 200 OK` (`{"status":"ready"}`) when in `Ready` or `Degraded` state.
- Returns `HTTP 503 Service Unavailable` (`{"status":"not_ready","reason":"<reason>"}`) when `Starting`, `Draining`, `ShuttingDown`, or experiencing `DependencyLoss`.

### Structured Health Probe (`GET /health`)
Returns comprehensive JSON detailing node health:
```json
{
  "status": "healthy",
  "role": "worker",
  "version": "0.59.21",
  "commit_sha": "...",
  "uptime_secs": 120,
  "active_shards": 8,
  "dependencies": {
    "lfs_storage": { "status": "ok", "latency_ms": 2 },
    "control_plane": { "status": "ok", "latency_ms": 5 }
  },
  "reasons": []
}
```
