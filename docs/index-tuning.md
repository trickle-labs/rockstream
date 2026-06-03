# Secondary Index Tuning & Optimizer Deep-Dive

This document details the optimizer's index selection decisions, selectivity calculation, and cost metrics inside `EXPLAIN INCREMENTAL` for secondary indexes.

## Optimizer Selection Decision: index_scan vs shard_scan

The planner automatically evaluates query predicates to determine whether to perform an `index_scan` (reading from a secondary index arrangement) or fall back to a `shard_scan` (scanning all shards).

```
                      ┌──────────────────────┐
                      │    Query Predicate   │
                      └──────────────────────┘
                                  │
                                  ▼
                     /────────────────────────\
                    <   Is Index BUILDING?     > ──── Yes ───┐
                     \────────────────────────/              │
                                  │ No                       │
                                  ▼                          ▼
                     /────────────────────────\      ┌──────────────┐
                    <    Frontier Lag > Max?   > ────│  shard_scan  │
                     \────────────────────────/      └──────────────┘
                                  │ No                       ▲
                                  ▼                          │
                     /────────────────────────\              │
                    < Selectivity < Threshold? > ──── No ────┘
                     \────────────────────────/
                                  │ Yes
                                  ▼
                           ┌──────────────┐
                           │  index_scan  │
                           └──────────────┘
```

The selection process evaluates the following criteria in order:
1. **Index State**: If the index is in the `BUILDING` state (`ViewState::BackfillingFromEpoch`), the planner falls back to `shard_scan`.
2. **Frontier Lag**: If the index's frontier lag exceeds `index_max_lag_ms` (default: 1000 ms), it falls back to `shard_scan`.
3. **Selectivity**: The optimizer computes the query selectivity. If selectivity is lower than `index_prefer_selectivity_threshold` (default: 0.01 or 1%), the index is preferred, selecting `index_scan`.

## Selectivity Calculation Details

Selectivity is calculated as the ratio of estimated rows matching the filter predicate to the total number of rows.
* **Low Selectivity (e.g., < 0.01)**: High-selectivity queries matching very few rows benefit significantly from `index_scan`.
* **High Selectivity (e.g., > 0.01)**: Queries returning a large portion of the table are faster to execute via parallel `shard_scan` rather than index lookups.

## Cost Metrics inside EXPLAIN INCREMENTAL

When querying a view, `EXPLAIN INCREMENTAL ESTIMATE` reports the estimated storage and processing cost of all active arrangements, including index state bytes. Index state bytes are fully charged against the workload `state_budget_gb` limits.

To diagnose index statistics, run:
```sql
EXPLAIN INDEX <index_name>;
```
This renders index-specific metrics:
* **Selectivity**: Calculated selectivity factor.
* **Fragmentation Ratio**: Index arrangement page fragmentation.
* **Cache Hit Metric**: Hot segment cache hit ratio.
* **Statistics**: Number of scans and raw bytes read.
