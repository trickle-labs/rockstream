# Auto-Tuning and Resource Sizing Policies

This document details the auto-tuning state machine, hysteresis dampening thresholds, resource allocation priorities, and safety override recovery steps for RockStream.

## Tuning State Machine

The auto-tuner operates on a continuous feedback loop executing at the end of each epoch. It collects metrics from the operators via `snapshot_metrics()` and evaluates performance targets.

```
       ┌───────────────────────────────┐
       │            Healthy            │◀──────────────────┐
       └───────────────────────────────┘                   │
           │                       ▲                       │
           │ Latency > 80% SLO     │ Latency < 20% SLO     │ Recovery
           ▼                       │ (4x K windows)        │
       ┌───────────────────────────────┐                   │
       │         Over-Budget           │───────────────────┤
       └───────────────────────────────┘                   │
           │                                               │
           │ SlateDB Stall detected                        │
           ▼                                               │
       ┌───────────────────────────────┐                   │
       │           Throttled           │───────────────────┘
       └───────────────────────────────┘
```

## Hysteresis Dampening

To prevent oscillation between different parallelism scaling decisions:
- **Scale Up (K consecutive windows)**: Parallelism is scaled up only after `hysteresis_scale_up_windows` (default: 3) consecutive epochs observe over-budget conditions.
- **Scale Down (4x K consecutive windows)**: Parallelism is scaled down only after `hysteresis_scale_down_windows` (default: 12) consecutive epochs observe under-budget conditions.

## Resource Allocation Priority

When resources are constrained, the following allocation priority holds:
1. **Durable SlateDB Persistence**: Ensure WAL writes complete successfully.
2. **Freshness SLO Compliance**: Size epochs and parallelism to meet latency targets.
3. **Ingestion Throttling**: Throttle upstream inputs to match backend storage write rate under write stalls.

## Safety Override Recovery

If the auto-tuner behaves unpredictably, manual overrides can be explicitly pinned via the CLI:
```bash
rockstream tune --override parallelism=8 epoch_size_ms=500
```
This writes overrides to `tune_overrides.json` inside the storage directory, which takes immediate precedence over auto-tuned decisions.
