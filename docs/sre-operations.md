# Production SRE Operations Guide

This guide covers operational observability for RockStream, details metrics, tracing, logging, alerts, and support bundle diagnostics.

## Prometheus Metrics Mappings

The following metric families are exposed on the HTTP metrics endpoint:

### `merge_law_*` Metrics
- `merge_law_applied_total`: Counter tracking the number of times a merge law is evaluated on a state merge.
- `merge_law_fallback_total`: Counter tracking the number of fallback reads resolved by copying all raw bytes (corrupt operand mitigation).
- `merge_law_compaction_bytes_reclaimed`: Counter of bytes reclaimed during SlateDB compaction filters running laws.
- `merge_law_duplicate_dropped_total`: Counter of duplicated update/insert operands dropped by idempotent laws.
- `merge_law_tombstone_bytes`: Gauge of active tombstone bytes in the arrangement state.
- `merge_law_monotone_partial_lag_ms`: Gauge showing freshness lag of monotone recursive operators.

### Budget & Resource Metrics
- `workload_memory_bytes`: Gauge showing live memory consumption of stateful operators grouped by workload.
- `state_budget_bytes`: Gauge tracking total memory allocations against `state_budget_gb`.
- `freshness_lag_ms`: Gauge showing the lag between input source watermarks and the committed epoch.

---

## OpenTelemetry Tracing

Spans are propagated across cluster services with standard W3C correlation contexts.
- **Traceparent Header**: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
- **Key Spans**:
  - `epoch.flush`: Covers the coordinate-to-commit phase.
  - `shuffle.exchange`: Measures network serialization and transit latency across worker boundaries.

---

## Alert Notification Rules

SREs should establish the following alert rules on Prometheus:

- **SLO Breach**: `freshness_lag_ms > freshness_slo_ms` for > 3 consecutive epochs.
- **State Budget Warning**: `workload_memory_bytes / memory_limit > 0.80` (RS-5018).
- **State Budget Critical**: `workload_memory_bytes / memory_limit > 0.95` (RS-5019).
- **Frontier Stalled**: `rate(merge_law_applied_total[1m]) == 0` for running pipelines.
- **mTLS Handshake Refusal Spikes**: `increase(audit_events_total{action="security.internal_mtls_denied"}[5m]) > 5` (RS-2410 / RS-2411).

---

## Log Filtering Guidelines

For local debugging, filter logs using `RUST_LOG=rockstream=info`.
To debug critical catalog errors, search log files for `RS-XXXX` codes (e.g. `RS-5018`, `RS-5019`, `RS-6001`).

---

## Support Bundle Parsing

Support bundles can be produced on demand using the CLI command:
```bash
rockstream support bundle [--view=<name>] [--since=<duration>] [--out=<path>]
```

Additionally, a support bundle is generated automatically every time a node runs `rockstream start`
(`write_support_bundle()` in `crates/rockstream-cli/src/lib.rs`), written as a
single pretty-printed JSON file directly under the node's `--storage`
directory:

```
<storage>/support-bundle-<generated_at_ms>.json
```

Its top-level shape (field-for-field, no tarball, no separate
`audit.jsonl`/`config.toml`/`metrics.json` files):

```json
{
  "generated_at_ms": 1732000000000,
  "system_info": {
    "version": "0.53.1",
    "os": "linux",
    "arch": "x86_64",
    "role": "all"
  },
  "metrics": {
    "uptime_ms": 1234,
    "audit_events_emitted": 6
  },
  "audit_events": [
    {
      "timestamp_ms": 1732000000000,
      "actor": "system",
      "action": "server.started",
      "resource": "rockstream",
      "error_code": null,
      "detail": null
    }
  ]
}
```

Parse it directly with any JSON tool, e.g.:
```bash
jq '.system_info, .metrics' support-bundle-*.json
```

The on-demand `rockstream support bundle` CLI command produces diagnostic artifacts with secret redaction and bounded size (< 10MB default) for operational support and cluster troubleshooting.

---

## Degradation Reasons, Explainability & Remediation Runbook

When a materialized view is degraded or blocked, `SHOW VIEW STATUS` and `rockstream view status` report an enumerated `degradation_reason`, a stable `reason_code`, a deterministic `dominant_contributor`, and optional live progress.

### Enumerated Degradation Reasons & Error Codes

| Reason | Error Code | Description | Remediation |
|---|---|---|---|
| `waiting_on_source` | `RS-3701` | Upstream source has stalled or stopped emitting watermarks | Inspect upstream source connectors, message brokers, and network connectivity; verify upstream watermarks are advancing. |
| `quota_admission_rejected` | `RS-3702` | Admission control rejected incoming work due to workload quota exhaustion | Increase workload memory budget using `ALTER WORKLOAD` or reduce incoming batch sizes and ingest rate. |
| `spilling` | `RS-3703` | Active disk spilling in progress due to bounded memory pressure | Increase state memory budget (`state_budget_gb`) or scale out cluster workers to distribute arrangement memory. |
| `over_budget_relaxed` | `RS-3704` | Workload exceeded soft memory budget and is running in relaxed mode | Allocate higher memory headroom or trigger proactive compaction to reclaim tombstone memory. |
| `checkpoint_alignment_stalled` | `RS-3705` | Checkpoint barrier alignment is stalled waiting for lagging shards | Identify barrier holder shard/operator with `rockstream checkpoint show <id>` and inspect worker CPU/network. |
| `sink_blocked` | `RS-3706` | Transactional 2PC sink cannot commit batches to external storage | Check downstream sink endpoint health, transaction commit timeouts, and target database locking. |
| `topology_transition_in_progress` | `RS-3707` | Active shard migration or worker drain is transitioning topology | Monitor live migration/drain progress via `rockstream view status` or `SHOW VIEW STATUS`; await cutover. |
| `recovering` | `RS-3708` | Materialized view is backfilling or recovering from a checkpoint | Monitor backfill progress and await catch-up to the current stream frontier. |

### Deterministic Dominant Contributor Priority

When multiple lag components contribute simultaneously, the dominant cause is derived deterministically by maximum value. Ties are broken according to the fixed priority ordering:

```
storage_pressure > source_lag > decode_lag > compute_lag > alignment_lag > sink_lag > spill_lag > healthy
```

### Live Progress & Durability Semantics

- **Phase**: Reflects current lifecycle sub-state (`planned`, `snapshotting`, `copying`, `dual_writing`, `catching_up`, `fencing_old`, `cutover`, `verifying`, `gc_eligible`, `done`, `draining`, `decommissioned`).
- **Remaining work**: `bytes_remaining` and `rows_remaining` are non-increasing and monotonically advance toward zero.
- **Estimates**: `estimated_remaining_ms` is derived only from completed work rate and bounded. On completion, remaining work and estimate report exactly 0.

