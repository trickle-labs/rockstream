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

---

## Log Filtering Guidelines

For local debugging, filter logs using `RUST_LOG=rockstream=info`.
To debug critical catalog errors, search log files for `RS-XXXX` codes (e.g. `RS-5018`, `RS-5019`, `RS-6001`).

---

## Support Bundle Parsing

The support bundle is generated via `rockstream support-bundle` as a tarball:
```bash
tar -xzvf support-bundle.tar.gz
```
It contains:
- `audit.jsonl`: Complete sequence of control plane events and administrative actions.
- `config.toml`: Reloaded node settings and active cluster parameters.
- `metrics.json`: Per-law statistical snapshots from the last 24h.
