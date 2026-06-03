# Disaster Recovery and Rolling Upgrade Manual

> **Version**: v0.53  
> **Applies to**: RockStream v0.48 – v0.53

This manual provides concrete walk-through checklists for cluster state backup,
migration scripts, fallback procedures, and emergency data recovery from raw
SlateDB files on S3/MinIO.

---

## Table of Contents

1. [Overview](#overview)
2. [Rolling Upgrade Procedure (N → N+1)](#rolling-upgrade)
3. [Storage Format Compatibility](#storage-format)
4. [Cluster State Backup](#cluster-state-backup)
5. [Disaster Recovery Drill](#disaster-recovery-drill)
6. [Emergency Recovery from Raw SlateDB Files](#emergency-raw-recovery)
7. [MinIO / S3 Recovery Procedures](#minio-s3-recovery)
8. [Shard Column Statistics and Scatter Pruning Recovery](#shard-stats-recovery)
9. [Error Codes and Diagnostics](#error-codes)

---

## 1. Overview <a name="overview"></a>

RockStream stores all durable state in SlateDB files on an object store
(S3/MinIO). Recovery from catastrophic failure requires:

1. A valid checkpoint manifest (`checkpoint.meta`)
2. The corresponding SST data files referenced by the manifest
3. The WAL sequence from the last committed epoch

Recovery does **not** require re-reading the entire event stream — only the
WAL entries since the last committed checkpoint.

### Key invariants

- Every committed epoch is durable before the worker acknowledges it.
- The checkpoint manifest is atomically committed; a partial manifest is never
  visible to readers.
- Storage format version is embedded in every shard header. A version outside
  the `[MIN_COMPATIBLE, CURRENT]` range causes `RS-5001` before any data is
  read.

---

## 2. Rolling Upgrade Procedure (N → N+1) <a name="rolling-upgrade"></a>

A rolling upgrade replaces workers one at a time while the cluster remains
operational. No epoch is lost during a correctly executed rolling upgrade.

### Prerequisites

- The N+1 binary is backward-compatible with format version N (see §3).
- The control service supports mixed-version clusters (asserted by the wire
  protocol version skew contract, `RS-5003`).

### Steps

```
1. Verify current cluster health:
   $ rockstream describe pipeline --all
   $ rockstream inspect stats ./data    # verify no stale stats (RS-2017)

2. Download and stage the N+1 binary on each worker host.

3. For each worker (one at a time):
   a. Drain the worker:
        EXECUTE 'DRAIN WORKER <worker_id>';
      Wait for all shards to be reassigned (SHOW CLUSTER RESOURCE USAGE).
   b. Stop the old binary:
        systemctl stop rockstream-worker
   c. Deploy the new binary:
        cp rockstream-v0.N+1 /usr/local/bin/rockstream
   d. Start the new binary:
        systemctl start rockstream-worker
   e. Verify the worker re-registered:
        rockstream describe pipeline shows worker in topology.

4. Upgrade the control service last:
   a. Perform a control-service handoff (Raft leader transfer if clustered).
   b. Stop old control binary; start new binary.

5. Run verification:
   $ cargo test --workspace     # or CI equivalent
   $ rockstream inspect stats ./data
```

### Rollback

If a worker fails to start with the N+1 binary:

```
1. Stop the failed N+1 worker.
2. Restore the N binary:
     cp rockstream-v0.N /usr/local/bin/rockstream
3. Start the N binary. The N binary can read format version N data.
4. Investigate failure logs before retrying.
```

---

## 3. Storage Format Compatibility <a name="storage-format"></a>

The `StorageFormatVersion` is embedded in every shard header and checkpoint
manifest. Before mounting any shard, the runtime checks:

```
MIN_COMPATIBLE(48) ≤ stored_version ≤ CURRENT(53)
```

| Scenario | Result |
|---|---|
| `stored == CURRENT` | Nominal read |
| `MIN_COMPATIBLE ≤ stored < CURRENT` | Backward-compatible read; no migration needed |
| `stored < MIN_COMPATIBLE` | `RS-5001` — run migration tool before mounting |
| `stored > CURRENT` | `RS-5001` — downgrade binary or upgrade all nodes |

### Migration tool (format too old)

```bash
# Dry run — show what would be migrated.
rockstream migrate --storage ./data --from 47 --to 53 --dry-run

# Execute migration (creates new checkpoint at format version 53).
rockstream migrate --storage ./data --from 47 --to 53
```

Migration is non-destructive: old files are not deleted until the new
checkpoint is committed.

---

## 4. Cluster State Backup <a name="cluster-state-backup"></a>

RockStream state is self-contained in the object store bucket. A consistent
backup is a copy of the bucket at a snapshot point that includes at least one
committed checkpoint manifest.

### Manual backup (MinIO / S3)

```bash
# Mirror the entire RockStream prefix to a backup bucket.
mc mirror --overwrite myminio/rockstream-data myminio/rockstream-backup-$(date +%Y%m%d)

# Verify the latest checkpoint manifest is present.
mc ls myminio/rockstream-backup-$(date +%Y%m%d) | grep checkpoint.meta
```

### Automated backup policy

Configure daily snapshots using MinIO's ILM or S3 lifecycle rules:

```json
{
  "Rules": [{
    "ID": "daily-backup",
    "Status": "Enabled",
    "Filter": { "Prefix": "rockstream/" },
    "Transition": {
      "Days": 1,
      "StorageClass": "GLACIER"
    }
  }]
}
```

---

## 5. Disaster Recovery Drill <a name="disaster-recovery-drill"></a>

Run this drill quarterly to verify that recovery from MinIO state works end to
end without an operational RockStream cluster.

### Drill steps

```
1. Identify the target backup snapshot (choose a checkpoint epoch).

2. Create an isolated MinIO bucket from the backup:
     mc mb myminio/rockstream-dr-test
     mc mirror myminio/rockstream-backup-2026-06-01 myminio/rockstream-dr-test

3. Start a fresh RockStream cluster pointing at the DR bucket:
     rockstream start \
       --storage s3://rockstream-dr-test \
       --role=all

4. Verify recovery:
   - The cluster enters RECOVERING state momentarily.
   - All pipelines return to RUNNING within 60 s.
   - rockstream describe pipeline shows correct epoch number.
   - Run a SELECT query against each view; results match the pre-disaster state.

5. Inject shard column stats staleness:
   - Wait > shard_stats_max_age_ms (default: 300 000 ms / 5 min).
   - Issue a cross-shard query; observe RS-2017 NOTICE in gateway logs.
   - Trigger a checkpoint: EXECUTE 'CHECKPOINT'; stats refresh.
   - Confirm RS-2017 no longer emitted.

6. Destroy the DR test bucket:
     mc rb --force myminio/rockstream-dr-test
```

### Pass criteria

- Pipeline freshness recovery < 60 s.
- No data loss or epoch duplication.
- RS-2017 emitted when stats stale; cleared after checkpoint.

---

## 6. Emergency Recovery from Raw SlateDB Files <a name="emergency-raw-recovery"></a>

If the checkpoint manifest is corrupted or missing, shard data can be recovered
directly from the WAL files and SST data.

### Step 1: List available WAL segments

```bash
rockstream inspect stats ./data    # reveals format version and WAL layout
mc ls myminio/rockstream/shard-0/wal/
```

### Step 2: Replay WAL from the last known good SST

```bash
# Identify the last committed SST epoch from WAL listing.
rockstream debug arrangement --view orders_mv --op-id 1 --key "2026-01-01"
```

### Step 3: Reconstruct checkpoint

```bash
rockstream migrate --storage ./data --from <detected_version> --to 53
```

### Step 4: Verify by starting in read-only mode

```bash
rockstream start --storage ./data --role=gateway
psql -h localhost -p 5432 -U rockstream -c "SELECT COUNT(*) FROM orders_mv;"
```

---

## 7. MinIO / S3 Recovery Procedures <a name="minio-s3-recovery"></a>

### Network partition recovery

If MinIO becomes unavailable mid-pipeline, workers enter
`object_store.brownout` mode (`RS-3003`). Recovery is automatic when the
object store becomes reachable again.

**Monitoring**:
```bash
# Watch for brownout metric.
curl http://localhost:9090/metrics | grep rockstream_pipeline_brownout
```

**Operator action if brownout persists > 5 min**:
1. Check MinIO health: `mc admin info myminio`
2. Check network connectivity: `ping <minio-host>`
3. If storage is permanently unavailable, initiate a cluster checkpoint from
   a backup bucket (see §5).

### Corrupted checkpoint manifest

Symptom: `RS-5001` on worker startup, log message `checkpoint manifest
checksum mismatch`.

```bash
# List all checkpoints; find the last healthy one.
mc ls myminio/rockstream/shard-0/ | grep checkpoint

# Promote an older checkpoint.
mc cp myminio/rockstream/shard-0/checkpoint-epoch-1000.meta \
       myminio/rockstream/shard-0/checkpoint.meta
```

Then restart workers. They will replay WAL from epoch 1001 onward.

---

## 8. Shard Column Statistics and Scatter Pruning Recovery <a name="shard-stats-recovery"></a>

From v0.53, per-shard column statistics (min/max, Bloom filter, HLL) are
embedded in checkpoint manifests. These stats drive OLAP scatter pruning.

### Staleness (RS-2017)

When stats age exceeds `shard_stats_max_age_ms`, the gateway emits:
```
NOTICE RS-2017 [shard_stats.too_stale]: Shard column statistics too stale;
scatter pruning disabled for this query.
```

**Remediation**: Force a checkpoint to refresh stats:
```sql
EXECUTE 'CHECKPOINT';
```
Or increase the freshness window in `rockstream.toml`:
```toml
[gateway]
shard_stats_max_age_ms = 600_000  # 10 minutes
```

### Inspecting stats locally

```bash
rockstream inspect stats ./data
# Output includes:
#   Blocked Bloom Filter: budget, items inserted, membership check
#   HLL Cardinality: distinct value estimate
#   Scatter metrics: shards_total, shards_pruned, bloom_false_positives
```

### Secondary index stat injection

When `CREATE INDEX customer_idx ON orders (customer_id)` completes backfill,
the next checkpoint automatically publishes column stats for `customer_id`.
Verify:
```bash
rockstream inspect stats ./data   # column_name=customer_id entry appears
```

---

## 9. Error Codes and Diagnostics <a name="error-codes"></a>

| Code | Name | Meaning | Action |
|---|---|---|---|
| `RS-5001` | `format.incompatible` | Storage format version outside compatible range | Run migration tool or upgrade binary |
| `RS-5003` | `wire.version_skew` | Rolling upgrade version skew | Ensure N+1 binary is backward-compatible with N |
| `RS-2017` | `shard_stats.too_stale` | Column statistics stale; scatter pruning disabled | Trigger a checkpoint or increase `shard_stats_max_age_ms` |
| `RS-3003` | `object_store.brownout` | Object store unavailable; local buffer exhausted | Check MinIO/S3 health; reduce input rate |
| `RS-3602` | `checkpoint.recovering` | Cluster checkpoint recovery in progress | Wait for recovery; monitor via `SHOW VIEW STATUS` |
| `RS-3603` | `checkpoint.recovering_slow` | Recovery exceeds 60 s SLO | Check worker health, storage latency, frontier progress |

---

*This document is part of the v0.53 release. See also:*
- *`docs/auto-tuning.md` — resource sizing and tuner overrides*
- *`docs/index-tuning.md` — secondary index statistics*
- *`docs/sre-operations.md` — Prometheus metrics and OTEL spans*
- *`docs/configuration.md` — full `rockstream.toml` reference*
