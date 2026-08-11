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

There is no `rockstream support-bundle` CLI command today. Instead, a support
bundle is generated automatically every time a node runs `rockstream start`
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
    "version": "0.45.5",
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

A two-word `rockstream support bundle` CLI command that regenerates or
re-exports this bundle on demand is roadmapped for v0.53 and is not yet
available.
