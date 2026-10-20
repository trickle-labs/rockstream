# Operator guide

Use these pages when running RockStream:

- [Configuration](configuration.md) for node and connector settings.
- [Connectors](connectors.md) for source and sink guarantees.
- [SRE operations](sre-operations.md) for metrics, alerts, logs, and support
  bundles.
- [Disaster recovery](disaster-recovery.md) for checkpoint export and restore.
- [Rolling upgrades](rolling-upgrades.md) for upgrade procedure.
- [Known limitations](known-limitations.md) before production rollout.

Generated command, configuration, metric, and error details are in the
[reference index](README.md#reference).

## Operational Observability & Seven Operator Questions

RockStream provides an authoritative operational observation model accessible via the CLI (`rockstream health`, `rockstream status`, `rockstream doctor`) and SQL system tables (`rockstream_catalog.*`).

An operator can answer the seven primary operational questions without inspecting internal code:

1. **Is the system healthy?**
   - Command: `rockstream health` or `GET /health`
   - SQL: `SELECT * FROM rockstream_catalog.nodes;`
   - Evaluation: Assesses 7 independent dimensions (`liveness`, `readiness`, `availability`, `freshness`, `durability`, `capacity`, `degradation`). Liveness alone does not mean workloads are healthy.
2. **Is my view current?**
   - Command: `rockstream status` or `rockstream view status <view>`
   - SQL: `SELECT view_name, state, frontier FROM rockstream_catalog.views;`
   - Evaluation: Compares `input_frontier` with `published_frontier` and checks `freshness_lag` against `freshness_slo`.
3. **Why is it behind?**
   - Command: `rockstream status` or `rockstream view status <view>`
   - Evaluation: Inspects `degradation_reason` (e.g. `waiting_on_source`, `worker_pressure`, `checkpoint_alignment_stalled`).
4. **What is consuming memory?**
   - Command: `rockstream status`
   - SQL: `SELECT * FROM rockstream_catalog.view_resource_usage;` and `SELECT * FROM rockstream_catalog.workload_resource_usage;`
   - Prometheus: `rockstream_memory_bytes{worker="...", category="..."}`
5. **Which worker owns this shard?**
   - Command: `rockstream status`
   - SQL: `SELECT shard_id, worker_id, state FROM rockstream_catalog.shards;`
6. **Is a migration blocking progress?**
   - Command: `rockstream status`
   - SQL: `SELECT operation_id, operation_type, state FROM rockstream_catalog.operations;`
   - Evaluation: Checks `blocking_operation` in view status.
7. **What should I do next?**
   - Command: `rockstream doctor`
   - Evaluation: Runs 12 bounded diagnostic checks distinguishing PASS, WARN, and FAIL, outputting actionable remediation steps with stable RS-XXXX codes.


## Workload policy application (v0.72)

`SHOW WORKLOAD APPLICATION FOR <workload_name>` reports the catalog's
`requested_revision` separately from the gateway's **actual live quota owner**:
`requested_memory_limit_bytes`, `gateway_effective_memory_limit_bytes`, and
`gateway_live_memory_bytes`. SQL NULL for the gateway values means no
local accounting owner has been observed; a NULL requested cap means unlimited.
For workloads that have a locally enforced unlimited cap, the effective-cap
column is also NULL, but the live usage remains observable.

`worker_effective_revision` is **NULL** and
`worker_application_state` is `worker_ack_unavailable` for workloads
with assigned views: the gateway cannot yet attest remote worker acknowledgement.
For workloads without assigned views, the state is `no_assigned_views`.
Do not read the accepted requested revision as proof of cluster-wide enforcement.
The command returns SQLSTATE `42704` for an unknown workload.

The existing `SHOW WORKLOAD REVISION FOR <name>` and `SHOW WORKLOAD STATUS`
commands keep their legacy schemas. A future actual worker-acknowledgement
protocol can populate the effective revision without making up acknowledgements.

