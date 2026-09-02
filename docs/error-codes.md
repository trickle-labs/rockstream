# RockStream Error Code Reference (`RS-XXXX`)

This historical URL is retained for existing links. The canonical manifest-generated reference is [error reference](reference/errors.md).

Contract version: `v0.59.12` — Authoritative Static Error Catalog (`DOC-01`)

Every user-visible, client-returned, or operator-logged error in RockStream carries a registered `RS-XXXX` code.
This document is generated directly from `contracts/errors.toml` with zero manual drift.

---

## Subsystem Index

- [0xxx: Internal & General System](#0xxx-internal--general-system) (5 codes)
- [1xxx: Pipeline, Plan & Optimization](#1xxx-pipeline-plan--optimization) (26 codes)
- [17xx: Lease Management & Raft Leadership](#17xx-lease-management--raft-leadership) (4 codes)
- [2xxx: Gateway, Query Execution & Wire Protocol](#2xxx-gateway-query-execution--wire-protocol) (37 codes)
- [24xx: Authentication, mTLS & Secrets](#24xx-authentication-mtls--secrets) (18 codes)
- [25xx-26xx: Extended Query, Cursors & Transactions](#25xx-26xx-extended-query-cursors--transactions) (9 codes)
- [3xxx: Storage, Execution, Memory & Shuffle](#3xxx-storage-execution-memory--shuffle) (49 codes)
- [4xxx: DDL, Catalog, Ingestion & Removed Connectors](#4xxx-ddl-catalog-ingestion--removed-connectors) (23 codes)
- [5xxx: Cluster, Node Lifecycle & Shard Coordination](#5xxx-cluster-node-lifecycle--shard-coordination) (16 codes)
- [6xxx: Connector Schema Evolution](#6xxx-connector-schema-evolution) (1 codes)
- [8xxx: Frontier Aggregation](#8xxx-frontier-aggregation) (3 codes)
- [9xxx: Admission Control](#9xxx-admission-control) (1 codes)

---

## 0xxx: Internal & General System

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-0001`](#rs-0001) | `internal.error` | Internal error | `Fatal` | `XX000` | `NonRetryable` |
| [`RS-0002`](#rs-0002) | `config.invalid` | Configuration error | `Error` | `22023` | `NonRetryable` |
| [`RS-0003`](#rs-0003) | `storage.unavailable` | Storage unavailable | `Error` | `53100` | `ExponentialBackoff` |
| [`RS-0004`](#rs-0004) | `cluster.unreachable` | Cluster control plane unreachable | `Error` | `08006` | `ExponentialBackoff` |
| [`RS-0005`](#rs-0005) | `cli.confirmation_required` | Destructive command confirmation required | `Error` | `55000` | `NonRetryable` |

### <a id="rs-0001"></a> `RS-0001` — Internal error

- **Key**: `internal.error`
- **Severity**: `Fatal`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Report this bug with the support bundle.

### <a id="rs-0002"></a> `RS-0002` — Configuration error

- **Key**: `config.invalid`
- **Severity**: `Error`
- **SQLSTATE**: `22023`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check configuration file and CLI flags.

### <a id="rs-0003"></a> `RS-0003` — Storage unavailable

- **Key**: `storage.unavailable`
- **Severity**: `Error`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Verify storage directory permissions and disk space.

### <a id="rs-0004"></a> `RS-0004` — Cluster control plane unreachable

- **Key**: `cluster.unreachable`
- **Severity**: `Error`
- **SQLSTATE**: `08006`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Verify the control service URL and ensure the control node is running and reachable.

### <a id="rs-0005"></a> `RS-0005` — Destructive command confirmation required

- **Key**: `cli.confirmation_required`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Pass --yes for script execution or answer y at the prompt.

---

## 1xxx: Pipeline, Plan & Optimization

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-1001`](#rs-1001) | `pipeline.not_found` | Pipeline not found | `Error` | `42P01` | `NonRetryable` |
| [`RS-1002`](#rs-1002) | `schema.incompatible_change` | Incompatible schema change | `Error` | `42804` | `NonRetryable` |
| [`RS-1003`](#rs-1003) | `record.decode_error` | Record decode error | `Error` | `22000` | `NonRetryable` |
| [`RS-1004`](#rs-1004) | `pipeline.already_exists` | Pipeline already exists | `Error` | `42710` | `NonRetryable` |
| [`RS-1005`](#rs-1005) | `workload.not_found` | Workload not found | `Error` | `42P01` | `NonRetryable` |
| [`RS-1006`](#rs-1006) | `workload.already_exists` | Workload already exists | `Error` | `42710` | `NonRetryable` |
| [`RS-1007`](#rs-1007) | `view.already_paused` | View is already paused | `Error` | `55000` | `NonRetryable` |
| [`RS-1008`](#rs-1008) | `view.not_paused` | View is not paused | `Error` | `55000` | `NonRetryable` |
| [`RS-1009`](#rs-1009) | `recursion.non_monotone_delta` | Non-monotone delta rejected in monotone recursion | `Error` | `22000` | `NonRetryable` |
| [`RS-1010`](#rs-1010) | `connector.bootstrap_interrupted` | Bootstrap interrupted; connector position lost | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-1011`](#rs-1011) | `view.dag_cycle_detected` | View-on-view DAG contains a cycle | `Error` | `42P16` | `NonRetryable` |
| [`RS-1012`](#rs-1012) | `sql.parse_error` | SQL statement could not be parsed | `Error` | `42601` | `NonRetryable` |
| [`RS-1013`](#rs-1013) | `sql.unsupported_feature` | Query contains a feature not supported by the incremental planner | `Error` | `0A000` | `NonRetryable` |
| [`RS-1014`](#rs-1014) | `workload.has_assigned_views` | Workload still has assigned views | `Error` | `55000` | `NonRetryable` |
| [`RS-1015`](#rs-1015) | `aggregate.group_commit_full` | Group-commit capacity exceeded; backpressure applied | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-1016`](#rs-1016) | `aggregate.numeric_overflow` | Aggregate running sum overflowed i64 | `Error` | `22003` | `NonRetryable` |
| [`RS-1017`](#rs-1017) | `aggregate.retraction_underflow` | MIN/MAX multiset retraction underflow: value has no positive weight | `Error` | `22000` | `NonRetryable` |
| [`RS-1018`](#rs-1018) | `aggregate.topk_overflow` | TopK buffer overflow: too many unique rows in a single partition | `Error` | `54000` | `ExponentialBackoff` |
| [`RS-1019`](#rs-1019) | `view.compilation_failed` | View query could not be compiled into an executable operator pipeline | `Error` | `0A000` | `NonRetryable` |
| [`RS-1020`](#rs-1020) | `operator.not_found` | Operator not found in pipeline | `Error` | `42P01` | `NonRetryable` |
| [`RS-1021`](#rs-1021) | `arrangement.key_decode_failed` | Arrangement key decoding failed or unsupported | `Error` | `22000` | `NonRetryable` |
| [`RS-1030`](#rs-1030) | `migration.timeout_exceeded` | Migration state exceeded its configured timeout budget | `Error` | `57014` | `ExponentialBackoff` |
| [`RS-1512`](#rs-1512) | `recursion.inner_frontier_stall` | Inner-frontier stall in distributed recursion; per-shard recompute triggered | `Warning` | `55000` | `AfterClusterRecovery` |
| [`RS-1513`](#rs-1513) | `recursion.max_iteration_exceeded` | Distributed recursion max-iteration cap exceeded without convergence | `Error` | `54000` | `NonRetryable` |
| [`RS-1601`](#rs-1601) | `lease.rejected` | Lease acquisition rejected: shard already leased or fence token invalid | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-1603`](#rs-1603) | `recovery.slow_recovery` | Recovery active for > 60s; pipeline freshness behind SLO | `Warning` | `55000` | `AfterClusterRecovery` |

### <a id="rs-1001"></a> `RS-1001` — Pipeline not found

- **Key**: `pipeline.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check pipeline name and ensure it has been created.

### <a id="rs-1002"></a> `RS-1002` — Incompatible schema change

- **Key**: `schema.incompatible_change`
- **Severity**: `Error`
- **SQLSTATE**: `42804`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Review schema evolution rules; a new view may be required.

### <a id="rs-1003"></a> `RS-1003` — Record decode error

- **Key**: `record.decode_error`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect the dead-letter queue for malformed records.

### <a id="rs-1004"></a> `RS-1004` — Pipeline already exists

- **Key**: `pipeline.already_exists`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a different pipeline name or drop the existing one.

### <a id="rs-1005"></a> `RS-1005` — Workload not found

- **Key**: `workload.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check the workload name; ensure it has been created with CREATE WORKLOAD.

### <a id="rs-1006"></a> `RS-1006` — Workload already exists

- **Key**: `workload.already_exists`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a different workload name or drop the existing workload first.

### <a id="rs-1007"></a> `RS-1007` — View is already paused

- **Key**: `view.already_paused`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: The view is already paused; use RESUME MATERIALIZED VIEW to restart it.

### <a id="rs-1008"></a> `RS-1008` — View is not paused

- **Key**: `view.not_paused`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: The view is not paused; only paused views can be resumed.

### <a id="rs-1009"></a> `RS-1009` — Non-monotone delta rejected in monotone recursion

- **Key**: `recursion.non_monotone_delta`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Ensure the recursive query is monotone or restructure it; check EXPLAIN for recursion rules.

### <a id="rs-1010"></a> `RS-1010` — Bootstrap interrupted; connector position lost

- **Key**: `connector.bootstrap_interrupted`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Verify connector positions, reset offsets, or perform a full bootstrap rebuild.

### <a id="rs-1011"></a> `RS-1011` — View-on-view DAG contains a cycle

- **Key**: `view.dag_cycle_detected`
- **Severity**: `Error`
- **SQLSTATE**: `42P16`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Resolve cycle in view dependencies; view-on-view relations must form a DAG.

### <a id="rs-1012"></a> `RS-1012` — SQL statement could not be parsed

- **Key**: `sql.parse_error`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check SQL syntax; see docs/language-features.md for the supported SQL subset.

### <a id="rs-1013"></a> `RS-1013` — Query contains a feature not supported by the incremental planner

- **Key**: `sql.unsupported_feature`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Simplify the query or check docs/language-features.md for the supported incremental SQL subset.

### <a id="rs-1014"></a> `RS-1014` — Workload still has assigned views

- **Key**: `workload.has_assigned_views`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reassign or drop the workload's views before dropping the workload.

### <a id="rs-1015"></a> `RS-1015` — Group-commit capacity exceeded; backpressure applied

- **Key**: `aggregate.group_commit_full`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce epoch rate, increase GROUP_COMMIT_MAX_BATCHES, or add more shards.

### <a id="rs-1016"></a> `RS-1016` — Aggregate running sum overflowed i64

- **Key**: `aggregate.numeric_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `22003`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reduce value magnitudes or switch to a wider numeric type.

### <a id="rs-1017"></a> `RS-1017` — MIN/MAX multiset retraction underflow: value has no positive weight

- **Key**: `aggregate.retraction_underflow`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Ensure every retraction is matched by a prior insertion; check source event ordering and idempotency.

### <a id="rs-1018"></a> `RS-1018` — TopK buffer overflow: too many unique rows in a single partition

- **Key**: `aggregate.topk_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce partition cardinality, increase TOPK_BUFFER_LIMIT, or add more partition columns.

### <a id="rs-1019"></a> `RS-1019` — View query could not be compiled into an executable operator pipeline

- **Key**: `view.compilation_failed`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Simplify the query to a supported shape (see docs/language-features.md), or reference only base tables — views over other views are not yet compiled.

### <a id="rs-1020"></a> `RS-1020` — Operator not found in pipeline

- **Key**: `operator.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Run rockstream explain <view> --op-ids to inspect available operator IDs for this view.

### <a id="rs-1021"></a> `RS-1021` — Arrangement key decoding failed or unsupported

- **Key**: `arrangement.key_decode_failed`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check arrangement key syntax or verify if the operator family key codec is supported.

### <a id="rs-1030"></a> `RS-1030` — Migration state exceeded its configured timeout budget

- **Key**: `migration.timeout_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `57014`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Check donor/recipient shard health, then retry or abort the migration; increase the specific migration timeout only if the cluster is healthy but the workload is larger than expected.

### <a id="rs-1512"></a> `RS-1512` — Inner-frontier stall in distributed recursion; per-shard recompute triggered

- **Key**: `recursion.inner_frontier_stall`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Check the step function for infinite cycles or skewed partitioning; review per-shard recompute logs.

### <a id="rs-1513"></a> `RS-1513` — Distributed recursion max-iteration cap exceeded without convergence

- **Key**: `recursion.max_iteration_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Increase max_iterations or restructure the recursive query to converge faster.

### <a id="rs-1601"></a> `RS-1601` — Lease acquisition rejected: shard already leased or fence token invalid

- **Key**: `lease.rejected`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Inspect shard lease ownership and retry after lease re-acquisition.

### <a id="rs-1603"></a> `RS-1603` — Recovery active for > 60s; pipeline freshness behind SLO

- **Key**: `recovery.slow_recovery`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Monitor worker throughput and checkpoint progress; verify storage health.

---

## 17xx: Lease Management & Raft Leadership

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-1701`](#rs-1701) | `lease.shard_already_leased` | Shard is already leased by a different worker | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-1702`](#rs-1702) | `lease.stale_token` | Stale lease token; worker has been fenced out | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-1703`](#rs-1703) | `lease.no_active_lease` | Shard has no active lease | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-1731`](#rs-1731) | `control.not_leader` | Write rejected: acting control node is not the current Raft leader | `Error` | `08006` | `AfterLeaderElection` |

### <a id="rs-1701"></a> `RS-1701` — Shard is already leased by a different worker

- **Key**: `lease.shard_already_leased`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Check worker assignments; another worker holds the lease. Use force-acquire if the holder is dead.

### <a id="rs-1702"></a> `RS-1702` — Stale lease token; worker has been fenced out

- **Key**: `lease.stale_token`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Worker has been fenced out; acquire a new lease before retrying.

### <a id="rs-1703"></a> `RS-1703` — Shard has no active lease

- **Key**: `lease.no_active_lease`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: No lease exists for this shard; acquire a lease before operating on it.

### <a id="rs-1731"></a> `RS-1731` — Write rejected: acting control node is not the current Raft leader

- **Key**: `control.not_leader`
- **Severity**: `Error`
- **SQLSTATE**: `08006`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Retry the write against the current Raft leader (query cluster status for the elected leader's address); do not retry against this node until it wins a future election.

---

## 2xxx: Gateway, Query Execution & Wire Protocol

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-2000`](#rs-2000) | `sql.malformed_ddl` | Malformed table DDL statement | `Error` | `42601` | `NonRetryable` |
| [`RS-2001`](#rs-2001) | `query.view_not_found` | View not found | `Error` | `42P01` | `NonRetryable` |
| [`RS-2002`](#rs-2002) | `query.timeout` | Query timeout | `Error` | `57014` | `ExponentialBackoff` |
| [`RS-2003`](#rs-2003) | `isolation.unsupported_level` | Unsupported isolation level | `Error` | `25001` | `NonRetryable` |
| [`RS-2004`](#rs-2004) | `view.cannot_drop_inline` | Cannot drop inline view: dependent materialized views still exist | `Error` | `55000` | `NonRetryable` |
| [`RS-2005`](#rs-2005) | `query.rate_limit_exceeded` | Query rate limit exceeded | `Error` | `57014` | `ExponentialBackoff` |
| [`RS-2006`](#rs-2006) | `query.epoch_before_retention` | Historical query beyond checkpoint retention window | `Error` | `22000` | `NonRetryable` |
| [`RS-2007`](#rs-2007) | `write.idempotency_key_required` | Idempotency key required for non-idempotent write | `Error` | `XX000` | `NonRetryable` |
| [`RS-2008`](#rs-2008) | `transaction.optimistic_conflict` | Optimistic transaction conflict: a concurrent write committed to the same key | `Error` | `40001` | `Immediate` |
| [`RS-2012`](#rs-2012) | `session.wait_for_timeout` | Session wait-for deadline exceeded; query proceeded at current frontier | `Warning` | `57014` | `Immediate` |
| [`RS-2013`](#rs-2013) | `transaction.returning_key_not_found` | Transaction RETURNING read-back could not find the row at the current frontier | `Error` | `42P01` | `Immediate` |
| [`RS-2014`](#rs-2014) | `index.is_building` | Index is building | `Warning` | `55000` | `Immediate` |
| [`RS-2015`](#rs-2015) | `index.max_lag_exceeded` | Index frontier lag exceeded limit | `Warning` | `55000` | `Immediate` |
| [`RS-2016`](#rs-2016) | `index.name_conflict` | Index name conflict | `Error` | `42710` | `NonRetryable` |
| [`RS-2017`](#rs-2017) | `shard_stats.too_stale` | Shard statistics are too stale for safe pruning; query fell back to a full scatter scan | `Warning` | `01000` | `NonRetryable` |
| [`RS-2018`](#rs-2018) | `session.max_staleness_exceeded` | Published frontier exceeded the session max_staleness bound; query proceeded | `Warning` | `01000` | `NonRetryable` |
| [`RS-2019`](#rs-2019) | `write.shard_backpressure` | Shard write buffer full; backpressure applied | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-2020`](#rs-2020) | `subscribe.consumer_lag_exceeded` | Subscribe consumer fell behind the change-log retention window | `Error` | `55000` | `Immediate` |
| [`RS-2021`](#rs-2021) | `copy.malformed_statement` | COPY FROM STDIN statement is malformed | `Error` | `42601` | `NonRetryable` |
| [`RS-2022`](#rs-2022) | `write.malformed_returning_clause` | UPDATE/DELETE RETURNING clause is malformed | `Error` | `42601` | `NonRetryable` |
| [`RS-2023`](#rs-2023) | `window.hop_state_overflow` | Hop window state exceeded its configured overlap-aware bound | `Error` | `54000` | `ExponentialBackoff` |
| [`RS-2024`](#rs-2024) | `window.session_state_overflow` | Session window state exceeded its configured open-session bound | `Error` | `54000` | `ExponentialBackoff` |
| [`RS-2025`](#rs-2025) | `query.query_time_result_set_too_large` | Query-time DataFusion source scan exceeded its configured bounded row/byte budget | `Error` | `54000` | `NonRetryable` |
| [`RS-2026`](#rs-2026) | `query.query_time_execution_failed` | Query-time DataFusion planning or execution failed for an ad hoc query | `Error` | `0A000` | `NonRetryable` |
| [`RS-2027`](#rs-2027) | `index.backfill_row_limit_exceeded` | CREATE INDEX automatic backfill scan exceeded its configured bounded row budget | `Error` | `54000` | `NonRetryable` |
| [`RS-2028`](#rs-2028) | `query.query_time_scatter_topology_unavailable` | Late-data side-channel queue reached its configured bound / scatter topology unavailable | `Error` | `55000` | `Immediate` |
| [`RS-2029`](#rs-2029) | `query.scatter_budget_exceeded` | Query-time scatter scan exceeded pathological row/byte budget | `Error` | `54000` | `NonRetryable` |
| [`RS-2030`](#rs-2030) | `ivm.factor_payload_overflow` | Factorized payload exceeded its configured row or byte bound / scatter frontier mismatch | `Error` | `54000` | `NonRetryable` |
| [`RS-2040`](#rs-2040) | `limit.result_set_too_large` | Result set exceeded max_in_flight_rows bound | `Error` | `54000` | `NonRetryable` |
| [`RS-2050`](#rs-2050) | `query.cancelled` | Query was cancelled by a client CancelRequest | `Error` | `57014` | `Immediate` |
| [`RS-2051`](#rs-2051) | `cursor.not_found` | Cursor does not exist | `Error` | `34000` | `NonRetryable` |
| [`RS-2052`](#rs-2052) | `cursor.already_exists` | Cursor already exists or cursor limit exceeded | `Error` | `42P03` | `NonRetryable` |
| [`RS-2053`](#rs-2053) | `limit.memory_limit_exceeded` | Per-connection memory limit exceeded | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-2054`](#rs-2054) | `query.statement_timeout` | Query exceeded the configured statement timeout | `Error` | `57014` | `ExponentialBackoff` |
| [`RS-2055`](#rs-2055) | `limit.connection_limit_exceeded` | Server-wide connection limit reached | `Error` | `53300` | `ExponentialBackoff` |
| [`RS-2056`](#rs-2056) | `write.malformed_values_list` | Malformed INSERT VALUES list or schema mismatch | `Error` | `42601` | `NonRetryable` |
| [`RS-2060`](#rs-2060) | `write.epoch_exhausted` | Commit epoch reached u64::MAX | `Fatal` | `54000` | `NonRetryable` |

### <a id="rs-2000"></a> `RS-2000` — Malformed table DDL statement

- **Key**: `sql.malformed_ddl`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check CREATE/DROP TABLE syntax against documentation.

### <a id="rs-2001"></a> `RS-2001` — View not found

- **Key**: `query.view_not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check view name and ensure the pipeline is running.

### <a id="rs-2002"></a> `RS-2002` — Query timeout

- **Key**: `query.timeout`
- **Severity**: `Error`
- **SQLSTATE**: `57014`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce query scope or increase timeout.

### <a id="rs-2003"></a> `RS-2003` — Unsupported isolation level

- **Key**: `isolation.unsupported_level`
- **Severity**: `Error`
- **SQLSTATE**: `25001`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a supported isolation level (snapshot or eventual).

### <a id="rs-2004"></a> `RS-2004` — Cannot drop inline view: dependent materialized views still exist

- **Key**: `view.cannot_drop_inline`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Drop all dependent materialized views first, or use CASCADE.

### <a id="rs-2005"></a> `RS-2005` — Query rate limit exceeded

- **Key**: `query.rate_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `57014`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce query rate, bundle queries, or increase tenant concurrency limits.

### <a id="rs-2006"></a> `RS-2006` — Historical query beyond checkpoint retention window

- **Key**: `query.epoch_before_retention`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Query a more recent epoch or timestamp, or increase the catalog's checkpoint_retention_duration.

### <a id="rs-2007"></a> `RS-2007` — Idempotency key required for non-idempotent write

- **Key**: `write.idempotency_key_required`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Provide a client-supplied idempotency key or an exactly-once source-epoch envelope.

### <a id="rs-2008"></a> `RS-2008` — Optimistic transaction conflict: a concurrent write committed to the same key

- **Key**: `transaction.optimistic_conflict`
- **Severity**: `Error`
- **SQLSTATE**: `40001`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Retry the transaction; if conflicts persist, reduce write concurrency or switch to a serializable protocol.

### <a id="rs-2012"></a> `RS-2012` — Session wait-for deadline exceeded; query proceeded at current frontier

- **Key**: `session.wait_for_timeout`
- **Severity**: `Warning`
- **SQLSTATE**: `57014`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Increase session_wait_for_timeout or reduce write latency.

### <a id="rs-2013"></a> `RS-2013` — Transaction RETURNING read-back could not find the row at the current frontier

- **Key**: `transaction.returning_key_not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Retry the write; if the row is consistently missing, check that the frontier used for the read-back has advanced past the commit epoch.

### <a id="rs-2014"></a> `RS-2014` — Index is building

- **Key**: `index.is_building`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Wait for index backfill to complete.

### <a id="rs-2015"></a> `RS-2015` — Index frontier lag exceeded limit

- **Key**: `index.max_lag_exceeded`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Index is too far behind view. Wait for synchronization or increase index_max_lag_ms.

### <a id="rs-2016"></a> `RS-2016` — Index name conflict

- **Key**: `index.name_conflict`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: An index with the same name already exists.

### <a id="rs-2017"></a> `RS-2017` — Shard statistics are too stale for safe pruning; query fell back to a full scatter scan

- **Key**: `shard_stats.too_stale`
- **Severity**: `Warning`
- **SQLSTATE**: `01000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Wait for the next checkpoint to publish fresh shard_stats, or increase shard_stats_max_age_checkpoints if this fallback is expected for the workload.

### <a id="rs-2018"></a> `RS-2018` — Published frontier exceeded the session max_staleness bound; query proceeded

- **Key**: `session.max_staleness_exceeded`
- **Severity**: `Warning`
- **SQLSTATE**: `01000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Increase rockstream.max_staleness, reduce publish lag, or switch to session_wait_for mode.

### <a id="rs-2019"></a> `RS-2019` — Shard write buffer full; backpressure applied

- **Key**: `write.shard_backpressure`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Wait for downstream IVM processing to drain, then retry COMMIT.

### <a id="rs-2020"></a> `RS-2020` — Subscribe consumer fell behind the change-log retention window

- **Key**: `subscribe.consumer_lag_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Reconnect with AS OF NOW WITH SNAPSHOT or increase CHANGE_LOG_MAX_ENTRIES.

### <a id="rs-2021"></a> `RS-2021` — COPY FROM STDIN statement is malformed

- **Key**: `copy.malformed_statement`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)].

### <a id="rs-2022"></a> `RS-2022` — UPDATE/DELETE RETURNING clause is malformed

- **Key**: `write.malformed_returning_clause`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check RETURNING syntax; it must be RETURNING * or RETURNING <col>[, <col>...] with no trailing content.

### <a id="rs-2023"></a> `RS-2023` — Hop window state exceeded its configured overlap-aware bound

- **Key**: `window.hop_state_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce hop overlap, increase HOP_WINDOW_STATE_LIMIT, or shard the windowed stream more finely.

### <a id="rs-2024"></a> `RS-2024` — Session window state exceeded its configured open-session bound

- **Key**: `window.session_state_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce session cardinality, increase SESSION_WINDOW_STATE_LIMIT, or shard the windowed stream more finely.

### <a id="rs-2025"></a> `RS-2025` — Query-time DataFusion source scan exceeded its configured bounded row/byte budget

- **Key**: `query.query_time_result_set_too_large`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reduce source-table cardinality, add a LIMIT, or materialize the query into a view.

### <a id="rs-2026"></a> `RS-2026` — Query-time DataFusion planning or execution failed for an ad hoc query

- **Key**: `query.query_time_execution_failed`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Simplify the query, validate referenced table/view schemas, or materialize the query into a view.

### <a id="rs-2027"></a> `RS-2027` — CREATE INDEX automatic backfill scan exceeded its configured bounded row budget

- **Key**: `index.backfill_row_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reduce table cardinality before indexing, or drop and recreate the index once the table is smaller.

### <a id="rs-2028"></a> `RS-2028` — Late-data side-channel queue reached its configured bound / scatter topology unavailable

- **Key**: `query.query_time_scatter_topology_unavailable`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Drain the configured late-data sink, reduce late-event volume, or increase TUMBLE_WINDOW_LATE_ROUTE_LIMIT after verifying available capacity.

### <a id="rs-2029"></a> `RS-2029` — Query-time scatter scan exceeded pathological row/byte budget

- **Key**: `query.scatter_budget_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Narrow the predicate, add a LIMIT, or materialize the query into a view.

### <a id="rs-2030"></a> `RS-2030` — Factorized payload exceeded its configured row or byte bound / scatter frontier mismatch

- **Key**: `ivm.factor_payload_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reduce join fan-out, increase the factor payload bound after capacity review, or use the classic join path.

### <a id="rs-2040"></a> `RS-2040` — Result set exceeded max_in_flight_rows bound

- **Key**: `limit.result_set_too_large`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Add a LIMIT clause or paginate using cursors.

### <a id="rs-2050"></a> `RS-2050` — Query was cancelled by a client CancelRequest

- **Key**: `query.cancelled`
- **Severity**: `Error`
- **SQLSTATE**: `57014`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Retry the query or adjust client timeout settings.

### <a id="rs-2051"></a> `RS-2051` — Cursor does not exist

- **Key**: `cursor.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `34000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use DECLARE to open a cursor before FETCH/MOVE/CLOSE.

### <a id="rs-2052"></a> `RS-2052` — Cursor already exists or cursor limit exceeded

- **Key**: `cursor.already_exists`
- **Severity**: `Error`
- **SQLSTATE**: `42P03`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: CLOSE the existing cursor or use a different name.

### <a id="rs-2053"></a> `RS-2053` — Per-connection memory limit exceeded

- **Key**: `limit.memory_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Close unused cursors, reduce result set sizes, or split the query.

### <a id="rs-2054"></a> `RS-2054` — Query exceeded the configured statement timeout

- **Key**: `query.statement_timeout`
- **Severity**: `Error`
- **SQLSTATE**: `57014`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Increase statement_timeout or optimize the query.

### <a id="rs-2055"></a> `RS-2055` — Server-wide connection limit reached

- **Key**: `limit.connection_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53300`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Close idle connections or increase max_connections.

### <a id="rs-2056"></a> `RS-2056` — Malformed INSERT VALUES list or schema mismatch

- **Key**: `write.malformed_values_list`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Ensure every VALUES row has matching parenthesis and correct column count.

### <a id="rs-2060"></a> `RS-2060` — Commit epoch reached u64::MAX

- **Key**: `write.epoch_exhausted`
- **Severity**: `Fatal`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Create a new shard before retrying.

---

## 24xx: Authentication, mTLS & Secrets

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-2400`](#rs-2400) | `auth.unauthenticated` | Unauthenticated: request missing or carrying invalid credentials | `Error` | `28000` | `NonRetryable` |
| [`RS-2401`](#rs-2401) | `auth.permission_denied` | Permission denied: authenticated principal lacks required RBAC role | `Error` | `28000` | `NonRetryable` |
| [`RS-2402`](#rs-2402) | `auth.namespace_access_denied` | Namespace access denied: cross-namespace access attempt by non-admin principal | `Error` | `28000` | `NonRetryable` |
| [`RS-2403`](#rs-2403) | `auth.mtls_requires_ca_cert` | --auth=mtls configured without tls_ca_cert_path; gateway refused to start | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2404`](#rs-2404) | `auth.mtls_no_verified_cert` | mTLS connection has no verified client certificate CN for its peer address | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2405`](#rs-2405) | `auth.tls_config_invalid` | Gateway TLS certificate/key/CA material failed to load or parse | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2406`](#rs-2406) | `auth.mtls_connection_cap_exceeded` | mTLS handshake rejected: connection identity map is at capacity | `Error` | `53300` | `ExponentialBackoff` |
| [`RS-2410`](#rs-2410) | `auth.internal_mtls_required` | Internal mTLS connection rejected: client certificate required | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2411`](#rs-2411) | `auth.internal_mtls_invalid_cert` | Internal mTLS client certificate invalid, expired, or signed by an untrusted CA | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2412`](#rs-2412) | `auth.internal_mtls_node_identity_mismatch` | Presented client certificate node identity does not match registration payload | `Fatal` | `28000` | `NonRetryable` |
| [`RS-2413`](#rs-2413) | `auth.internal_mtls_rotation_failed` | Internal mTLS certificate rotation or reload failed | `Error` | `28000` | `NonRetryable` |
| [`RS-2420`](#rs-2420) | `secret.not_found` | Secret not found in secret catalog | `Error` | `42P01` | `NonRetryable` |
| [`RS-2421`](#rs-2421) | `secret.already_exists` | Secret already exists in catalog | `Error` | `42710` | `NonRetryable` |
| [`RS-2422`](#rs-2422) | `secret.encryption_failed` | Secret encryption or envelope DEK wrap/unwrap failed | `Fatal` | `XX000` | `NonRetryable` |
| [`RS-2423`](#rs-2423) | `secret.token_invalid` | Secret token is invalid, expired, or failed node-identity verification | `Error` | `28000` | `NonRetryable` |
| [`RS-2424`](#rs-2424) | `secret.ddl_invalid` | Secret DDL syntax or configuration is invalid | `Error` | `42601` | `NonRetryable` |
| [`RS-2425`](#rs-2425) | `secret.rotation_failed` | Secret or KEK rotation failed | `Fatal` | `XX000` | `NonRetryable` |
| [`RS-2426`](#rs-2426) | `secret.in_use_by_source_or_sink` | Secret drop rejected because it is in active use by a source or sink | `Error` | `55000` | `NonRetryable` |

### <a id="rs-2400"></a> `RS-2400` — Unauthenticated: request missing or carrying invalid credentials

- **Key**: `auth.unauthenticated`
- **Severity**: `Error`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Provide valid credentials (Bearer token or mTLS certificate)

### <a id="rs-2401"></a> `RS-2401` — Permission denied: authenticated principal lacks required RBAC role

- **Key**: `auth.permission_denied`
- **Severity**: `Error`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Request elevated RBAC role from an admin or contact the namespace owner

### <a id="rs-2402"></a> `RS-2402` — Namespace access denied: cross-namespace access attempt by non-admin principal

- **Key**: `auth.namespace_access_denied`
- **Severity**: `Error`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Switch to the correct namespace with SET search_path or request cross-namespace admin role

### <a id="rs-2403"></a> `RS-2403` — --auth=mtls configured without tls_ca_cert_path; gateway refused to start

- **Key**: `auth.mtls_requires_ca_cert`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Set --tls-ca-cert-path (or gateway.tls_ca_cert_path in rockstream.toml) to the CA that signs client certificates.

### <a id="rs-2404"></a> `RS-2404` — mTLS connection has no verified client certificate CN for its peer address

- **Key**: `auth.mtls_no_verified_cert`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Connect with a client certificate signed by the configured CA over sslmode=verify-full; a bare TCP or TLS connection without a client cert cannot use --auth=mtls.

### <a id="rs-2405"></a> `RS-2405` — Gateway TLS certificate/key/CA material failed to load or parse

- **Key**: `auth.tls_config_invalid`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the configured paths point to valid PEM-encoded certificate/key files readable by the gateway process.

### <a id="rs-2406"></a> `RS-2406` — mTLS handshake rejected: connection identity map is at capacity

- **Key**: `auth.mtls_connection_cap_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53300`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce concurrent connections or raise MAX_CONNECTIONS. The gateway rejected the handshake to avoid silently dropping the peer identity.

### <a id="rs-2410"></a> `RS-2410` — Internal mTLS connection rejected: client certificate required

- **Key**: `auth.internal_mtls_required`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Configure internal TLS client certificates (--internal-tls-cert-path, --internal-tls-key-path, --internal-tls-ca-cert-path) so the node can authenticate with the cluster.

### <a id="rs-2411"></a> `RS-2411` — Internal mTLS client certificate invalid, expired, or signed by an untrusted CA

- **Key**: `auth.internal_mtls_invalid_cert`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the internal TLS client certificate is valid, not expired, and signed by the trusted cluster CA root certificate.

### <a id="rs-2412"></a> `RS-2412` — Presented client certificate node identity does not match registration payload

- **Key**: `auth.internal_mtls_node_identity_mismatch`
- **Severity**: `Fatal`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Ensure the node ID and role presented in the internal TLS certificate Common Name / SAN match the node registration parameters.

### <a id="rs-2413"></a> `RS-2413` — Internal mTLS certificate rotation or reload failed

- **Key**: `auth.internal_mtls_rotation_failed`
- **Severity**: `Error`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the new certificate and private key files exist, have matching keys, and are signed by a trusted CA before triggering certificate rotation.

### <a id="rs-2420"></a> `RS-2420` — Secret not found in secret catalog

- **Key**: `secret.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the secret name or run CREATE SECRET to define it.

### <a id="rs-2421"></a> `RS-2421` — Secret already exists in catalog

- **Key**: `secret.already_exists`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Choose a distinct secret name or run ALTER SECRET to modify the existing secret.

### <a id="rs-2422"></a> `RS-2422` — Secret encryption or envelope DEK wrap/unwrap failed

- **Key**: `secret.encryption_failed`
- **Severity**: `Fatal`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check KEK provider configuration and key access permissions.

### <a id="rs-2423"></a> `RS-2423` — Secret token is invalid, expired, or failed node-identity verification

- **Key**: `secret.token_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `28000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Request a fresh secret token using valid mTLS node credentials.

### <a id="rs-2424"></a> `RS-2424` — Secret DDL syntax or configuration is invalid

- **Key**: `secret.ddl_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check CREATE/ALTER SECRET syntax and required options (e.g. TYPE).

### <a id="rs-2425"></a> `RS-2425` — Secret or KEK rotation failed

- **Key**: `secret.rotation_failed`
- **Severity**: `Fatal`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the target KEK provider is reachable and active connectors are responsive.

### <a id="rs-2426"></a> `RS-2426` — Secret drop rejected because it is in active use by a source or sink

- **Key**: `secret.in_use_by_source_or_sink`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Drop or alter referencing sources and sinks before dropping the secret.

---

## 25xx-26xx: Extended Query, Cursors & Transactions

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-2500`](#rs-2500) | `copy.table_not_found` | COPY target table does not exist in the catalog | `Error` | `42P01` | `NonRetryable` |
| [`RS-2501`](#rs-2501) | `copy.column_count_mismatch` | COPY row field count does not match declared column count or invalid encoding | `Error` | `22000` | `NonRetryable` |
| [`RS-2560`](#rs-2560) | `transaction.in_failed_sql_transaction` | Query cannot run inside a failed transaction block | `Error` | `25P02` | `NonRetryable` |
| [`RS-2561`](#rs-2561) | `transaction.savepoint_not_found` | Savepoint does not exist | `Error` | `3B001` | `NonRetryable` |
| [`RS-2562`](#rs-2562) | `transaction.two_phase_not_supported` | PREPARE TRANSACTION / XA two-phase commit is not supported | `Error` | `0A000` | `NonRetryable` |
| [`RS-2563`](#rs-2563) | `transaction.savepoint_limit_exceeded` | Per-transaction savepoint limit exceeded | `Error` | `54000` | `NonRetryable` |
| [`RS-2564`](#rs-2564) | `notify.channel_limit_exceeded` | Notify channel limit exceeded | `Error` | `54000` | `NonRetryable` |
| [`RS-2600`](#rs-2600) | `limit.prepared_statements_exceeded` | Prepared statement limit exceeded for this connection | `Error` | `53200` | `Immediate` |
| [`RS-2601`](#rs-2601) | `limit.portals_exceeded` | Portal limit exceeded for this connection | `Error` | `53200` | `Immediate` |

### <a id="rs-2500"></a> `RS-2500` — COPY target table does not exist in the catalog

- **Key**: `copy.table_not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Register the table with CREATE TABLE before using COPY FROM STDIN.

### <a id="rs-2501"></a> `RS-2501` — COPY row field count does not match declared column count or invalid encoding

- **Key**: `copy.column_count_mismatch`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check that the TSV row matches the column count declared in COPY or the catalog.

### <a id="rs-2560"></a> `RS-2560` — Query cannot run inside a failed transaction block

- **Key**: `transaction.in_failed_sql_transaction`
- **Severity**: `Error`
- **SQLSTATE**: `25P02`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Issue ROLLBACK to exit the failed block, then retry.

### <a id="rs-2561"></a> `RS-2561` — Savepoint does not exist

- **Key**: `transaction.savepoint_not_found`
- **Severity**: `Error`
- **SQLSTATE**: `3B001`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use SAVEPOINT <name> to create one before ROLLBACK TO.

### <a id="rs-2562"></a> `RS-2562` — PREPARE TRANSACTION / XA two-phase commit is not supported

- **Key**: `transaction.two_phase_not_supported`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a single-phase COMMIT instead.

### <a id="rs-2563"></a> `RS-2563` — Per-transaction savepoint limit exceeded

- **Key**: `transaction.savepoint_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: RELEASE earlier savepoints before creating new ones.

### <a id="rs-2564"></a> `RS-2564` — Notify channel limit exceeded

- **Key**: `notify.channel_limit_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: UNLISTEN unused channels.

### <a id="rs-2600"></a> `RS-2600` — Prepared statement limit exceeded for this connection

- **Key**: `limit.prepared_statements_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Deallocate unused prepared statements using DEALLOCATE.

### <a id="rs-2601"></a> `RS-2601` — Portal limit exceeded for this connection

- **Key**: `limit.portals_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Close unused portals.

---

## 3xxx: Storage, Execution, Memory & Shuffle

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-3001`](#rs-3001) | `storage.writer_fenced` | Shard writer fenced out: lease lost | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-3003`](#rs-3003) | `storage.object_store_brownout` | Pipeline blocked: object store brownout, local buffer exhausted | `Error` | `53100` | `ExponentialBackoff` |
| [`RS-3005`](#rs-3005) | `config.self_fencing_invalid` | Self-fencing configuration invalid: self_fence_after constraint violated | `Error` | `22023` | `NonRetryable` |
| [`RS-3009`](#rs-3009) | `storage.merge_operand_malformed` | Merge operand malformed | `Error` | `XX000` | `NonRetryable` |
| [`RS-3010`](#rs-3010) | `storage.shuffle_io_retired` | Legacy durable shuffle error (retired, use RS-3011..3016) | `Error` | `XX000` | `ExponentialBackoff` |
| [`RS-3011`](#rs-3011) | `storage.durable_shuffle_rate_limit` | Durable shuffle rate-limit retry budget exhausted | `Error` | `53100` | `ExponentialBackoff` |
| [`RS-3012`](#rs-3012) | `storage.durable_shuffle_io_failure` | Durable shuffle generic object-store I/O failure | `Error` | `53100` | `ExponentialBackoff` |
| [`RS-3013`](#rs-3013) | `storage.durable_shuffle_buffer_full` | Durable shuffle in-memory buffer capacity exceeded | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-3014`](#rs-3014) | `storage.durable_shuffle_footer_serialize_failed` | Durable shuffle footer serialization failed | `Error` | `XX000` | `NonRetryable` |
| [`RS-3015`](#rs-3015) | `storage.durable_shuffle_footer_deserialize_failed` | Durable shuffle footer deserialization failed | `Error` | `XX000` | `NonRetryable` |
| [`RS-3016`](#rs-3016) | `storage.durable_shuffle_footer_corrupt` | Durable shuffle footer is corrupt or undersized | `Error` | `XX000` | `NonRetryable` |
| [`RS-3017`](#rs-3017) | `exchange.ipc_decode_error` | Exchange IPC shuffle decode error | `Error` | `22000` | `NonRetryable` |
| [`RS-3018`](#rs-3018) | `exchange.loopback_target_missing` | Exchange loopback route target shard has no active ShardDb | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-3019`](#rs-3019) | `exchange.shm_segment_unavailable` | Same-host shared-memory segment unavailable; exchange fell back to the direct path | `Warning` | `53200` | `ExponentialBackoff` |
| [`RS-3020`](#rs-3020) | `exchange.shuffle_codec_unknown` | Shuffle payload codec is unknown or decompression failed | `Error` | `22000` | `NonRetryable` |
| [`RS-3021`](#rs-3021) | `exchange.worker_locality_stale` | Worker locality metadata is missing or stale; exchange fell back to the safe route | `Warning` | `55000` | `Immediate` |
| [`RS-3022`](#rs-3022) | `cluster.checkpoint_manifest_codec_error` | Cluster checkpoint manifest codec decode error | `Error` | `XX000` | `NonRetryable` |
| [`RS-3023`](#rs-3023) | `exchange.fast_path_frontier_read_failed` | Fast-path shuffle frontier read failed during replay dedup | `Warning` | `53100` | `Immediate` |
| [`RS-3024`](#rs-3024) | `exchange.row_budget_exceeded` | Shuffle frame row budget exceeded worker.max_rows_per_quantum | `Error` | `54000` | `ExponentialBackoff` |
| [`RS-3025`](#rs-3025) | `platform.unverified_environment_warning` | Unverified platform or compatible backend environment warning | `Warning` | `00000` | `NonRetryable` |
| [`RS-3026`](#rs-3026) | `platform.insecure_container_execution` | Insecure container execution (root user or writable rootfs) | `Warning` | `00000` | `NonRetryable` |
| [`RS-3027`](#rs-3027) | `platform.port_conflict` | Platform port conflict on required listener port | `Error` | `58000` | `NonRetryable` |
| [`RS-3028`](#rs-3028) | `platform.unsupported_environment` | Unsupported host platform, architecture, OS, or filesystem | `Fatal` | `58000` | `NonRetryable` |
| [`RS-3029`](#rs-3029) | `connector.incompatible_version` | Incompatible external database or broker version | `Error` | `0A000` | `NonRetryable` |
| [`RS-3030`](#rs-3030) | `capacity.sample_batch_flush_failure` | Capacity sample batch flush failure | `Error` | `58000` | `NonRetryable` |
| [`RS-3031`](#rs-3031) | `capacity.invalid_estimate_request` | Invalid EXPLAIN INCREMENTAL ESTIMATE query or options | `Error` | `42601` | `NonRetryable` |
| [`RS-3032`](#rs-3032) | `qualification.release_gate_rejection` | Release candidate qualification gate rejected candidate evidence | `Error` | `58000` | `NonRetryable` |
| [`RS-3033`](#rs-3033) | `qualification.harness_invalidation` | Anti-cheat harness mutation or invalid execution detected | `Fatal` | `58000` | `NonRetryable` |
| [`RS-3501`](#rs-3501) | `merge_law.accumulator_decode_error` | Merge-law accumulator wire bytes have the wrong size | `Error` | `22000` | `NonRetryable` |
| [`RS-3601`](#rs-3601) | `checkpoint.alignment_buffer_overflow` | Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-3602`](#rs-3602) | `cluster.checkpoint_recovery_in_progress` | Cluster checkpoint recovery in progress | `Warning` | `55000` | `AfterClusterRecovery` |
| [`RS-3603`](#rs-3603) | `cluster.freshness_recovery_slow` | Pipeline freshness recovery SLO exceeded; RECOVERING_SLOW state | `Warning` | `55000` | `AfterClusterRecovery` |
| [`RS-3604`](#rs-3604) | `worker.drain_in_progress` | Worker drain in progress; new shard assignments rejected | `Error` | `55000` | `Immediate` |
| [`RS-3605`](#rs-3605) | `cluster.skew_threshold_exceeded` | Shard load factor exceeds skew threshold; adaptive re-sharding scheduled | `Warning` | `55000` | `Immediate` |
| [`RS-3606`](#rs-3606) | `worker.drain_deadline_exceeded` | Worker drain deadline exceeded; worker self-fenced | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-3607`](#rs-3607) | `schema.blue_green_required` | Schema change requires blue/green clone; in-place apply rejected | `Error` | `55000` | `NonRetryable` |
| [`RS-3608`](#rs-3608) | `view.clone_already_in_progress` | A blue/green clone operation is already in progress for this view | `Error` | `55000` | `Immediate` |
| [`RS-3609`](#rs-3609) | `view.clone_backfill_lag_exceeded` | Clone backfill lag exceeded the allowed threshold before flip | `Error` | `55000` | `Immediate` |
| [`RS-3610`](#rs-3610) | `worker.drain_target_not_found` | Worker drain target does not exist in the current topology | `Error` | `55000` | `Immediate` |
| [`RS-3611`](#rs-3611) | `worker.no_active_drain_recipient` | Worker drain cannot proceed because no active recipient worker is available | `Error` | `55000` | `Immediate` |
| [`RS-3612`](#rs-3612) | `worker.drain_queue_capacity_reached` | Worker drain queue reached its configured bound; backpressure applied | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-3701`](#rs-3701) | `view.waiting_on_source` | View is waiting on source/frontier progress | `Warning` | `55000` | `Immediate` |
| [`RS-3702`](#rs-3702) | `view.quota_admission_rejected` | View admission rejected by quota controls | `Warning` | `53200` | `ExponentialBackoff` |
| [`RS-3703`](#rs-3703) | `view.spilling` | View lag is dominated by spill delay | `Warning` | `53100` | `ExponentialBackoff` |
| [`RS-3704`](#rs-3704) | `view.over_budget_relaxed` | View is in over-budget relaxed mode | `Warning` | `53200` | `NonRetryable` |
| [`RS-3705`](#rs-3705) | `view.checkpoint_alignment_stalled` | View checkpoint alignment is stalled | `Warning` | `55000` | `Immediate` |
| [`RS-3706`](#rs-3706) | `view.sink_blocked` | View sink commit path is blocked | `Warning` | `55000` | `Immediate` |
| [`RS-3707`](#rs-3707) | `view.topology_transition_in_progress` | View topology transition is in progress | `Warning` | `55000` | `Immediate` |
| [`RS-3708`](#rs-3708) | `view.recovering` | View is recovering from checkpoint/reassignment work | `Warning` | `55000` | `AfterClusterRecovery` |

### <a id="rs-3001"></a> `RS-3001` — Shard writer fenced out: lease lost

- **Key**: `storage.writer_fenced`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Acquire a new lease or refresh cluster leadership before retrying write.

### <a id="rs-3003"></a> `RS-3003` — Pipeline blocked: object store brownout, local buffer exhausted

- **Key**: `storage.object_store_brownout`
- **Severity**: `Error`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce input rate or increase local_buffer_max_epochs; check object store availability.

### <a id="rs-3005"></a> `RS-3005` — Self-fencing configuration invalid: self_fence_after constraint violated

- **Key**: `config.self_fencing_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `22023`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Set self_fence_after so that: dead_after < self_fence_after < 2 × shard_recovery_budget.

### <a id="rs-3009"></a> `RS-3009` — Merge operand malformed

- **Key**: `storage.merge_operand_malformed`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect the stored arrangement value; possible data corruption or law version mismatch.

### <a id="rs-3010"></a> `RS-3010` — Legacy durable shuffle error (retired, use RS-3011..3016)

- **Key**: `storage.shuffle_io_retired`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Upgrade to the granular durable shuffle error codes RS-3011 through RS-3016.

### <a id="rs-3011"></a> `RS-3011` — Durable shuffle rate-limit retry budget exhausted

- **Key**: `storage.durable_shuffle_rate_limit`
- **Severity**: `Error`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Object store is rate-limiting requests; reduce shuffle write concurrency or request a higher rate limit/quota from the object store provider.

### <a id="rs-3012"></a> `RS-3012` — Durable shuffle generic object-store I/O failure

- **Key**: `storage.durable_shuffle_io_failure`
- **Severity**: `Error`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Verify object store connectivity, credentials, and bucket settings.

### <a id="rs-3013"></a> `RS-3013` — Durable shuffle in-memory buffer capacity exceeded

- **Key**: `storage.durable_shuffle_buffer_full`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce per-epoch shuffle frame size or flush more frequently; increase MAX_DURABLE_BUFFER_SIZE_BYTES if the workload legitimately needs a larger buffer.

### <a id="rs-3014"></a> `RS-3014` — Durable shuffle footer serialization failed

- **Key**: `storage.durable_shuffle_footer_serialize_failed`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Report this bug with the support bundle; the index footer failed to serialize to JSON.

### <a id="rs-3015"></a> `RS-3015` — Durable shuffle footer deserialization failed

- **Key**: `storage.durable_shuffle_footer_deserialize_failed`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: The stored footer bytes are not valid JSON; the object may be corrupt or written by an incompatible version. Re-run the shuffle epoch.

### <a id="rs-3016"></a> `RS-3016` — Durable shuffle footer is corrupt or undersized

- **Key**: `storage.durable_shuffle_footer_corrupt`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: The coalesced shuffle object is truncated or its footer-length header is inconsistent with the object size; re-run the shuffle epoch or restore from a prior checkpoint.

### <a id="rs-3017"></a> `RS-3017` — Exchange IPC shuffle decode error

- **Key**: `exchange.ipc_decode_error`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect the Arrow IPC shuffle payload; possible truncation or a version mismatch between the writer and reader.

### <a id="rs-3018"></a> `RS-3018` — Exchange loopback route target shard has no active ShardDb

- **Key**: `exchange.loopback_target_missing`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Verify the target shard is registered and its ShardDb has been attached before routing; check shard assignment and worker startup order.

### <a id="rs-3019"></a> `RS-3019` — Same-host shared-memory segment unavailable; exchange fell back to the direct path

- **Key**: `exchange.shm_segment_unavailable`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Check same_host_shm_segment_bytes, same_host_shm_segments_per_peer, and host-level shared-memory permissions/capacity.

### <a id="rs-3020"></a> `RS-3020` — Shuffle payload codec is unknown or decompression failed

- **Key**: `exchange.shuffle_codec_unknown`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify both peers advertise shuffle_codec_v1, inspect the payload bytes for corruption, and retry after rolling the cluster to a compatible build.

### <a id="rs-3021"></a> `RS-3021` — Worker locality metadata is missing or stale; exchange fell back to the safe route

- **Key**: `exchange.worker_locality_stale`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Check worker host_id/availability_zone registration, wait for topology refresh, or force the durable path during the rollout.

### <a id="rs-3022"></a> `RS-3022` — Cluster checkpoint manifest codec decode error

- **Key**: `cluster.checkpoint_manifest_codec_error`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify the control-plane capability floor, inspect the stored checkpoint manifest bytes for corruption, and finish the rolling upgrade before re-enabling manifest compression.

### <a id="rs-3023"></a> `RS-3023` — Fast-path shuffle frontier read failed during replay dedup

- **Key**: `exchange.fast_path_frontier_read_failed`
- **Severity**: `Warning`
- **SQLSTATE**: `53100`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Inspect target shard storage health and the committed frontier key; the frame was delivered conservatively, so verify downstream idempotency if the shard is unhealthy.

### <a id="rs-3024"></a> `RS-3024` — Shuffle frame row budget exceeded worker.max_rows_per_quantum

- **Key**: `exchange.row_budget_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum only if the worker can safely absorb a larger in-flight row budget.

### <a id="rs-3025"></a> `RS-3025` — Unverified platform or compatible backend environment warning

- **Key**: `platform.unverified_environment_warning`
- **Severity**: `Warning`
- **SQLSTATE**: `00000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Review the platform or backend compatibility matrix in docs/platforms.md; unverified environments are protocol-compatible but not qualified in release gates.

### <a id="rs-3026"></a> `RS-3026` — Insecure container execution (root user or writable rootfs)

- **Key**: `platform.insecure_container_execution`
- **Severity**: `Warning`
- **SQLSTATE**: `00000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Run the container as unprivileged non-root user (UID 10001) with --read-only root filesystem and persistent /data volume mount.

### <a id="rs-3027"></a> `RS-3027` — Platform port conflict on required listener port

- **Key**: `platform.port_conflict`
- **Severity**: `Error`
- **SQLSTATE**: `58000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check for another process binding the requested port (5432, 9090, 9100, 9200) or specify an alternative address via CLI flags.

### <a id="rs-3028"></a> `RS-3028` — Unsupported host platform, architecture, OS, or filesystem

- **Key**: `platform.unsupported_environment`
- **Severity**: `Fatal`
- **SQLSTATE**: `58000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Run RockStream on a supported 64-bit architecture (x86_64, aarch64) with modern Linux (glibc >= 2.31, kernel >= 5.4) or macOS, and ensure /data is on a local POSIX filesystem or supported S3 object store.

### <a id="rs-3029"></a> `RS-3029` — Incompatible external database or broker version

- **Key**: `connector.incompatible_version`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Upgrade external PostgreSQL to version 12+ (14+ recommended) or Kafka broker to version 2.8+ (3.x recommended).

### <a id="rs-3030"></a> `RS-3030` — Capacity sample batch flush failure

- **Key**: `capacity.sample_batch_flush_failure`
- **Severity**: `Error`
- **SQLSTATE**: `58000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check storage permissions and disk space for capacity measurement chunks, or reduce profiling sample rate.

### <a id="rs-3031"></a> `RS-3031` — Invalid EXPLAIN INCREMENTAL ESTIMATE query or options

- **Key**: `capacity.invalid_estimate_request`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check the SQL query syntax, target view name, and cardinality hints supplied to EXPLAIN INCREMENTAL ESTIMATE.

### <a id="rs-3032"></a> `RS-3032` — Release candidate qualification gate rejected candidate evidence

- **Key**: `qualification.release_gate_rejection`
- **Severity**: `Error`
- **SQLSTATE**: `58000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect qualification run failures, verify scaling floors, error ranges, and candidate identity invariants.

### <a id="rs-3033"></a> `RS-3033` — Anti-cheat harness mutation or invalid execution detected

- **Key**: `qualification.harness_invalidation`
- **Severity**: `Fatal`
- **SQLSTATE**: `58000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify multi-process worker isolation, distinct WorkerIds, active shard leases, and untampered timestamps.

### <a id="rs-3501"></a> `RS-3501` — Merge-law accumulator wire bytes have the wrong size

- **Key**: `merge_law.accumulator_decode_error`
- **Severity**: `Error`
- **SQLSTATE**: `22000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect the stored merge-law accumulator bytes; possible data corruption or an accumulator wire-format version mismatch.

### <a id="rs-3601"></a> `RS-3601` — Checkpoint alignment buffer overflowed; bounded buffer capacity exceeded

- **Key**: `checkpoint.alignment_buffer_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce input rate or increase checkpoint alignment buffer capacity; check for slow shards holding up barrier propagation.

### <a id="rs-3602"></a> `RS-3602` — Cluster checkpoint recovery in progress

- **Key**: `cluster.checkpoint_recovery_in_progress`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Wait for recovery to complete; monitor shard reassignment and frontier progress via SHOW VIEW STATUS.

### <a id="rs-3603"></a> `RS-3603` — Pipeline freshness recovery SLO exceeded; RECOVERING_SLOW state

- **Key**: `cluster.freshness_recovery_slow`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Recovery is exceeding SLO; check worker health, storage latency, and frontier progress. Escalate if recovery does not complete within expected bounds.

### <a id="rs-3604"></a> `RS-3604` — Worker drain in progress; new shard assignments rejected

- **Key**: `worker.drain_in_progress`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Wait for worker drain to complete, or target active workers for shard assignment.

### <a id="rs-3605"></a> `RS-3605` — Shard load factor exceeds skew threshold; adaptive re-sharding scheduled

- **Key**: `cluster.skew_threshold_exceeded`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Allow adaptive re-sharding to complete or manually trigger partition splitting.

### <a id="rs-3606"></a> `RS-3606` — Worker drain deadline exceeded; worker self-fenced

- **Key**: `worker.drain_deadline_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Investigate slow network/compaction preventing worker from draining; review worker logs.

### <a id="rs-3607"></a> `RS-3607` — Schema change requires blue/green clone; in-place apply rejected

- **Key**: `schema.blue_green_required`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Perform a zero-downtime view replacement using a blue/green deployment strategy.

### <a id="rs-3608"></a> `RS-3608` — A blue/green clone operation is already in progress for this view

- **Key**: `view.clone_already_in_progress`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Wait for the existing clone backfill to finish before starting a new one.

### <a id="rs-3609"></a> `RS-3609` — Clone backfill lag exceeded the allowed threshold before flip

- **Key**: `view.clone_backfill_lag_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Reduce write load or check worker resource usage to allow backfill to catch up before flip.

### <a id="rs-3610"></a> `RS-3610` — Worker drain target does not exist in the current topology

- **Key**: `worker.drain_target_not_found`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Verify target worker ID exists in active cluster topology before initiating drain.

### <a id="rs-3611"></a> `RS-3611` — Worker drain cannot proceed because no active recipient worker is available

- **Key**: `worker.no_active_drain_recipient`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Ensure at least one active recipient worker is online before initiating drain.

### <a id="rs-3612"></a> `RS-3612` — Worker drain queue reached its configured bound; backpressure applied

- **Key**: `worker.drain_queue_capacity_reached`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Wait for in-flight shard drain tasks to complete before enqueueing additional assignments.

### <a id="rs-3701"></a> `RS-3701` — View is waiting on source/frontier progress

- **Key**: `view.waiting_on_source`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Check source/frontier health and producer lag; verify watermark advancement for the upstream source.

### <a id="rs-3702"></a> `RS-3702` — View admission rejected by quota controls

- **Key**: `view.quota_admission_rejected`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce state pressure, free quota in competing workloads, or adjust admission and memory budgets before retrying.

### <a id="rs-3703"></a> `RS-3703` — View lag is dominated by spill delay

- **Key**: `view.spilling`
- **Severity**: `Warning`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce spill pressure by lowering hot-key skew, increasing memory budget, or reducing ingest burst size.

### <a id="rs-3704"></a> `RS-3704` — View is in over-budget relaxed mode

- **Key**: `view.over_budget_relaxed`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Reduce view memory usage or increase workload memory limit so the view can exit relaxed mode.

### <a id="rs-3705"></a> `RS-3705` — View checkpoint alignment is stalled

- **Key**: `view.checkpoint_alignment_stalled`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Inspect checkpoint barrier holders and slow shards; resolve stalled operators before retrying.

### <a id="rs-3706"></a> `RS-3706` — View sink commit path is blocked

- **Key**: `view.sink_blocked`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Check sink connectivity/commit latency and transactional backpressure; recover sink health before retrying.

### <a id="rs-3707"></a> `RS-3707` — View topology transition is in progress

- **Key**: `view.topology_transition_in_progress`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Wait for migration/drain to complete, or inspect topology transition progress and blocked shard ownership.

### <a id="rs-3708"></a> `RS-3708` — View is recovering from checkpoint/reassignment work

- **Key**: `view.recovering`
- **Severity**: `Warning`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Wait for recovery to complete; monitor checkpoint and shard reassignment progress via SHOW VIEW STATUS.

---

## 4xxx: DDL, Catalog, Ingestion & Removed Connectors

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-4001`](#rs-4001) | `source.connection_failed` | Source connection failed or table already exists | `Error` | `42710` | `NonRetryable` |
| [`RS-4002`](#rs-4002) | `sink.write_failed` | Sink write failed | `Error` | `53100` | `ExponentialBackoff` |
| [`RS-4003`](#rs-4003) | `sink.pre_commit_failed` | Sink 2PC pre-commit failed; epoch not staged | `Error` | `55000` | `ExponentialBackoff` |
| [`RS-4004`](#rs-4004) | `sink.commit_failed` | Sink 2PC commit failed after pre-commit; recovery required | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-4005`](#rs-4005) | `sink.duplicate_delivery` | Sink 2PC duplicate delivery detected and suppressed | `Info` | `00000` | `Immediate` |
| [`RS-4006`](#rs-4006) | `source.epoch_registry_full` | Source-epoch registry full; too many uncommitted epochs in flight | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-4007`](#rs-4007) | `sink.ddl_invalid` | CREATE SINK DDL parse or validation failed | `Error` | `42601` | `NonRetryable` |
| [`RS-4008`](#rs-4008) | `source.ddl_invalid` | CREATE SOURCE DDL parse or validation failed | `Error` | `42601` | `NonRetryable` |
| [`RS-4009`](#rs-4009) | `source.not_found` | Source not found | `Error` | `42P01` | `NonRetryable` |
| [`RS-4010`](#rs-4010) | `source.already_exists` | Source already exists | `Error` | `42710` | `NonRetryable` |
| [`RS-4011`](#rs-4011) | `postgres_cdc.recovery_required` | PostgreSQL CDC replication cannot proceed without recovery | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-4012`](#rs-4012) | `source.owner_recovery_required` | Source owner registration requires checkpoint recovery | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-4013`](#rs-4013) | `postgres_cdc.protocol_error` | PostgreSQL CDC protocol or ownership validation failed | `Error` | `55000` | `NonRetryable` |
| [`RS-4014`](#rs-4014) | `source.bounds_exceeded` | Source bounded in-flight capacity was exceeded | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-4015`](#rs-4015) | `source.fence_mismatch` | Source checkpoint fence did not advance monotonically | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-4016`](#rs-4016) | `source.acknowledgement_failed` | Source checkpoint acknowledgement failed | `Error` | `55000` | `ExponentialBackoff` |
| [`RS-4017`](#rs-4017) | `connector.removed` | Connector has been removed | `Error` | `0A000` | `NonRetryable` |
| [`RS-4018`](#rs-4018) | `source.epoch_exhausted` | Source epoch exhausted | `Error` | `54000` | `NonRetryable` |
| [`RS-4019`](#rs-4019) | `source.backfill_cursor_invalid` | Source backfill cursor or lifecycle is invalid | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-4020`](#rs-4020) | `backfill.live_delta_buffer_full` | Backfill live-delta buffer is full | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-4021`](#rs-4021) | `backfill.admission_rejected` | Backfill admission reservation rejected | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-4022`](#rs-4022) | `backfill.not_published` | Materialized view backfill is not published | `Error` | `55000` | `Immediate` |
| [`RS-4029`](#rs-4029) | `quota.backpressure_refusal` | Quota or backpressure refusal during ingestion burst | `Warning` | `53200` | `ExponentialBackoff` |

### <a id="rs-4001"></a> `RS-4001` — Source connection failed or table already exists

- **Key**: `source.connection_failed`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify source connection settings and network connectivity.

### <a id="rs-4002"></a> `RS-4002` — Sink write failed

- **Key**: `sink.write_failed`
- **Severity**: `Error`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Check sink availability and credentials.

### <a id="rs-4003"></a> `RS-4003` — Sink 2PC pre-commit failed; epoch not staged

- **Key**: `sink.pre_commit_failed`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Retry the epoch; check sink connector health and connectivity.

### <a id="rs-4004"></a> `RS-4004` — Sink 2PC commit failed after pre-commit; recovery required

- **Key**: `sink.commit_failed`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Trigger manual recovery or restart the connector; check sink idempotency profile.

### <a id="rs-4005"></a> `RS-4005` — Sink 2PC duplicate delivery detected and suppressed

- **Key**: `sink.duplicate_delivery`
- **Severity**: `Info`
- **SQLSTATE**: `00000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: This is informational; the duplicate was suppressed. Check the source for duplicate delivery.

### <a id="rs-4006"></a> `RS-4006` — Source-epoch registry full; too many uncommitted epochs in flight

- **Key**: `source.epoch_registry_full`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce source epoch rate or increase max_in_flight_source_epochs.

### <a id="rs-4007"></a> `RS-4007` — CREATE SINK DDL parse or validation failed

- **Key**: `sink.ddl_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check CREATE SINK syntax, referenced view name, and WITH option types; use catalog=filesystem|glue|rest|hive|ducklake.

### <a id="rs-4008"></a> `RS-4008` — CREATE SOURCE DDL parse or validation failed

- **Key**: `source.ddl_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `42601`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check CREATE SOURCE syntax, connector options, and source credentials.

### <a id="rs-4009"></a> `RS-4009` — Source not found

- **Key**: `source.not_found`
- **Severity**: `Error`
- **SQLSTATE**: `42P01`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Check the source name and ensure it has been created.

### <a id="rs-4010"></a> `RS-4010` — Source already exists

- **Key**: `source.already_exists`
- **Severity**: `Error`
- **SQLSTATE**: `42710`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a different source name or drop the existing source first.

### <a id="rs-4011"></a> `RS-4011` — PostgreSQL CDC replication cannot proceed without recovery

- **Key**: `postgres_cdc.recovery_required`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Repair the PostgreSQL slot or publication, then run the bounded resnapshot workflow.

### <a id="rs-4012"></a> `RS-4012` — Source owner registration requires checkpoint recovery

- **Key**: `source.owner_recovery_required`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Run checkpoint recovery before registering the source owner, then retry owner registration.

### <a id="rs-4013"></a> `RS-4013` — PostgreSQL CDC protocol or ownership validation failed

- **Key**: `postgres_cdc.protocol_error`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Validate pgoutput protocol, source identity, slot ownership, and durable routing before retrying.

### <a id="rs-4014"></a> `RS-4014` — Source bounded in-flight capacity was exceeded

- **Key**: `source.bounds_exceeded`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Drain the source or reduce transaction and epoch size before increasing the configured bound.

### <a id="rs-4015"></a> `RS-4015` — Source checkpoint fence did not advance monotonically

- **Key**: `source.fence_mismatch`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Recover the highest committed source checkpoint and retry with the next fenced epoch.

### <a id="rs-4016"></a> `RS-4016` — Source checkpoint acknowledgement failed

- **Key**: `source.acknowledgement_failed`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Retain source ownership, recover the committed checkpoint, and retry upstream acknowledgement.

### <a id="rs-4017"></a> `RS-4017` — Connector has been removed

- **Key**: `connector.removed`
- **Severity**: `Error`
- **SQLSTATE**: `0A000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output.

### <a id="rs-4018"></a> `RS-4018` — Source epoch exhausted

- **Key**: `source.epoch_exhausted`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Create a new connector before retrying.

### <a id="rs-4019"></a> `RS-4019` — Source backfill cursor or lifecycle is invalid

- **Key**: `source.backfill_cursor_invalid`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Recover or recreate the committed backfill cursor or lifecycle, then retry.

### <a id="rs-4020"></a> `RS-4020` — Backfill live-delta buffer is full

- **Key**: `backfill.live_delta_buffer_full`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Wait for snapshot catch-up or reduce live-delta volume before retrying.

### <a id="rs-4021"></a> `RS-4021` — Backfill admission reservation rejected

- **Key**: `backfill.admission_rejected`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Wait for a backfill to finish or reduce BACKFILL_LIVE_DELTA_MAX_BYTES before retrying.

### <a id="rs-4022"></a> `RS-4022` — Materialized view backfill is not published

- **Key**: `backfill.not_published`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Run SHOW BACKFILL STATUS and retry after the materialized view reaches RUNNING, or create it first.

### <a id="rs-4029"></a> `RS-4029` — Quota or backpressure refusal during ingestion burst

- **Key**: `quota.backpressure_refusal`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce ingestion rate or increase quota allocation.

---

## 5xxx: Cluster, Node Lifecycle & Shard Coordination

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-5001`](#rs-5001) | `storage.incompatible_format` | Incompatible storage format | `Fatal` | `55000` | `NonRetryable` |
| [`RS-5002`](#rs-5002) | `arrangement.unknown_merge_law` | Unknown merge law in arrangement header | `Fatal` | `XX000` | `NonRetryable` |
| [`RS-5003`](#rs-5003) | `legacy.validation_failure` | Legacy validation failure | `Error` | `XX000` | `NonRetryable` |
| [`RS-5004`](#rs-5004) | `quota.counter_overflow` | Quota counter overflow detected | `Error` | `54000` | `NonRetryable` |
| [`RS-5018`](#rs-5018) | `resource.budget_warning_80` | Resource usage budget warning (80% threshold reached) | `Warning` | `53200` | `ExponentialBackoff` |
| [`RS-5019`](#rs-5019) | `resource.budget_critical_95` | Resource usage budget critical (95% threshold reached) | `Warning` | `53200` | `ExponentialBackoff` |
| [`RS-5021`](#rs-5021) | `wire.version_not_supported` | Wire protocol version not supported; rolling upgrade version skew | `Fatal` | `08006` | `NonRetryable` |
| [`RS-5022`](#rs-5022) | `storage.latency_budget_breach` | Object store latency or amplification budget breach | `Warning` | `53100` | `ExponentialBackoff` |
| [`RS-5023`](#rs-5023) | `window.partition_too_large` | Window partition size exceeded skew warning threshold | `Warning` | `01000` | `NonRetryable` |
| [`RS-5030`](#rs-5030) | `migration.illegal_state_transition` | Illegal shard-migration state transition rejected | `Error` | `55000` | `NonRetryable` |
| [`RS-5031`](#rs-5031) | `migration.verify_scan_window_full` | Shard-migration verify scan window exceeded its configured bound | `Error` | `54000` | `ExponentialBackoff` |
| [`RS-5032`](#rs-5032) | `migration.bucket_map_version_mismatch` | Shard-migration bucket-map version or watcher acknowledgement mismatch | `Error` | `55000` | `Immediate` |
| [`RS-5033`](#rs-5033) | `migration.reclamation_not_safe` | Donor reclamation is not frontier-safe in current state | `Error` | `55000` | `AfterClusterRecovery` |
| [`RS-5034`](#rs-5034) | `migration.verification_divergence` | Migration verification divergence detected for key | `Error` | `XX000` | `AfterClusterRecovery` |
| [`RS-5035`](#rs-5035) | `skew.slo_cannot_be_met` | Skew-bound SLO cannot be met without composable partial-state splitting | `Error` | `54000` | `NonRetryable` |
| [`RS-5036`](#rs-5036) | `skew.non_composable_hot_key` | Non-composable hot key routed to a single spill shard | `Warning` | `01000` | `NonRetryable` |

### <a id="rs-5001"></a> `RS-5001` — Incompatible storage format

- **Key**: `storage.incompatible_format`
- **Severity**: `Fatal`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Run rockstream migrate --from=N --to=M --storage=<url> before upgrading the binary.

### <a id="rs-5002"></a> `RS-5002` — Unknown merge law in arrangement header

- **Key**: `arrangement.unknown_merge_law`
- **Severity**: `Fatal`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Register the merge law or migrate the arrangement before attaching the shard.

### <a id="rs-5003"></a> `RS-5003` — Legacy validation failure

- **Key**: `legacy.validation_failure`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Inspect the failing component and its version-specific validation guidance.

### <a id="rs-5004"></a> `RS-5004` — Quota counter overflow detected

- **Key**: `quota.counter_overflow`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Release quota or create a new workload before requesting additional capacity.

### <a id="rs-5018"></a> `RS-5018` — Resource usage budget warning (80% threshold reached)

- **Key**: `resource.budget_warning_80`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Examine view resource usage and plan to scale out cluster capacity or adjust memory limits.

### <a id="rs-5019"></a> `RS-5019` — Resource usage budget critical (95% threshold reached)

- **Key**: `resource.budget_critical_95`
- **Severity**: `Warning`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Immediately free unused view resources or scale cluster capacity to prevent pipeline stalls.

### <a id="rs-5021"></a> `RS-5021` — Wire protocol version not supported; rolling upgrade version skew

- **Key**: `wire.version_not_supported`
- **Severity**: `Fatal`
- **SQLSTATE**: `08006`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Use a peer with an overlapping protocol range, or finish the rolling upgrade before retrying.

### <a id="rs-5022"></a> `RS-5022` — Object store latency or amplification budget breach

- **Key**: `storage.latency_budget_breach`
- **Severity**: `Warning`
- **SQLSTATE**: `53100`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Check object store performance, batch sizes, and write amplification.

### <a id="rs-5023"></a> `RS-5023` — Window partition size exceeded skew warning threshold

- **Key**: `window.partition_too_large`
- **Severity**: `Warning`
- **SQLSTATE**: `01000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Add partition keys to distribute window load across shards.

### <a id="rs-5030"></a> `RS-5030` — Illegal shard-migration state transition rejected

- **Key**: `migration.illegal_state_transition`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Drive the migration through the documented next state only, or resume from the persisted record instead of forcing a skipped state.

### <a id="rs-5031"></a> `RS-5031` — Shard-migration verify scan window exceeded its configured bound

- **Key**: `migration.verify_scan_window_full`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce verify_sample_rate, split the migration into fewer buckets, or increase the configured verify scan bound if memory headroom allows.

### <a id="rs-5032"></a> `RS-5032` — Shard-migration bucket-map version or watcher acknowledgement mismatch

- **Key**: `migration.bucket_map_version_mismatch`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `Immediate`
- **Default Next Steps**: Wait for every reader, exchange receiver, and gateway to observe the new bucket_map_version, then retry the migration step under the current version.

### <a id="rs-5033"></a> `RS-5033` — Donor reclamation is not frontier-safe in current state

- **Key**: `migration.reclamation_not_safe`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Wait for migration state machine to advance to a safe reclamation state.

### <a id="rs-5034"></a> `RS-5034` — Migration verification divergence detected for key

- **Key**: `migration.verification_divergence`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `AfterClusterRecovery`
- **Default Next Steps**: Inspect donor and recipient shard state; reconcile divergent keys.

### <a id="rs-5035"></a> `RS-5035` — Skew-bound SLO cannot be met without composable partial-state splitting

- **Key**: `skew.slo_cannot_be_met`
- **Severity**: `Error`
- **SQLSTATE**: `54000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Add composable partial-state semantics for this operator, reduce the hot key's skew at the source, or route the workload to a spill-shard plan that can tolerate the SLO miss.

### <a id="rs-5036"></a> `RS-5036` — Non-composable hot key routed to a single spill shard

- **Key**: `skew.non_composable_hot_key`
- **Severity**: `Warning`
- **SQLSTATE**: `01000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Keep the hot key on a single spill shard, watch that shard's pressure, and switch to a composable law before enabling virtual-bucket splitting for this workload.

---

## 6xxx: Connector Schema Evolution

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-6001`](#rs-6001) | `schema.upstream_evolution_detected` | Incompatible upstream schema evolution detected | `Warning` | `42804` | `NonRetryable` |

### <a id="rs-6001"></a> `RS-6001` — Incompatible upstream schema evolution detected

- **Key**: `schema.upstream_evolution_detected`
- **Severity**: `Warning`
- **SQLSTATE**: `42804`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Apply view replacement or run manual migration to match the new upstream schema.

---

## 8xxx: Frontier Aggregation

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-8001`](#rs-8001) | `frontier.aggregator_registry_full` | Frontier aggregator shard registry is full; new shard reports rejected | `Error` | `53200` | `ExponentialBackoff` |
| [`RS-8002`](#rs-8002) | `frontier.stale_fence_token` | Stale fencing token on frontier-aggregator publisher-lease CAS or publish | `Error` | `55000` | `AfterLeaderElection` |
| [`RS-8003`](#rs-8003) | `frontier.sync_flush_violation` | Sync-flush-before-lease-handoff-read violation on frontier publication | `Error` | `XX000` | `NonRetryable` |

### <a id="rs-8001"></a> `RS-8001` — Frontier aggregator shard registry is full; new shard reports rejected

- **Key**: `frontier.aggregator_registry_full`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Scale out frontier aggregators (add more nodes with --role=frontier) or reduce shard count below the configured limit.

### <a id="rs-8002"></a> `RS-8002` — Stale fencing token on frontier-aggregator publisher-lease CAS or publish

- **Key**: `frontier.stale_fence_token`
- **Severity**: `Error`
- **SQLSTATE**: `55000`
- **Retry Class**: `AfterLeaderElection`
- **Default Next Steps**: Re-acquire the publisher lease under the current fence token before retrying; this aggregator has been fenced out by a newer publisher.

### <a id="rs-8003"></a> `RS-8003` — Sync-flush-before-lease-handoff-read violation on frontier publication

- **Key**: `frontier.sync_flush_violation`
- **Severity**: `Error`
- **SQLSTATE**: `XX000`
- **Retry Class**: `NonRetryable`
- **Default Next Steps**: Verify every publish_frontier write path uses WriteOptions { await_durable: true }; this indicates a durability regression in FrontierLeaseStore.

---

## 9xxx: Admission Control

| Code | Key | Title | Severity | SQLSTATE | Retry Class |
|---|---|---|---|---|---|
| [`RS-9001`](#rs-9001) | `admission.capacity_request_rejected` | Admission control rejected the capacity request | `Error` | `53200` | `ExponentialBackoff` |

### <a id="rs-9001"></a> `RS-9001` — Admission control rejected the capacity request

- **Key**: `admission.capacity_request_rejected`
- **Severity**: `Error`
- **SQLSTATE**: `53200`
- **Retry Class**: `ExponentialBackoff`
- **Default Next Steps**: Reduce the requesting workload's demand, raise the cluster state budget, or lower the priority of contending workloads so admission control can pause them.

---
