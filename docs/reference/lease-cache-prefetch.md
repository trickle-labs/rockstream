# Lease cache prefetch

Workers observe actual immutable SST range reads and periodically publish a
bounded `ShardHeatReport` under their current shard fence. Heat entries identify
the storage scope, immutable SST object, byte range and observation time. The
control plane accepts only the current owner and scopes belonging to that shard.
The older unassociated `hot_sst_ids` and `hot_block_ranges` fields remain readable
for compatibility and never authorize prefetch reads.

Before migration transfers a lease, the target receives `WarmShardCache`. It
opens shared object-store references and reads the approved ranges into the
worker-wide raw SST cache. Both normal reads and prefetch use `HeatObjectStore`,
so a warm read avoids the same backend range GET that foreground execution would
otherwise issue. No database writer is opened in the preparation stage.

Limits are 64 entries, 1 MiB per range, 4 MiB per preparation, one concurrent
preparation per worker and one range in flight. Hints older than 60 seconds or
with future timestamps are ignored. The worker deadline is two seconds; control
stops waiting after three seconds. Missing objects, malformed hints, unavailable
targets or exhausted prefetch capacity do not change correctness or lease
identity. The canonical fenced handoff still validates the donor and target.

The raw range cache reserves at most 4 MiB (one eighth of the existing decoded
block budget for smaller workers); it is included in worker cache accounting.
Metadata and entry counts are bounded as well as payload bytes. Cache keys use
the full storage scope, object path and range. Mutable manifests, WAL objects,
versioned requests and conditional reads bypass this cache. Actual backend byte
counters, cache hits and recipient preparation logs provide observable evidence.

Shared object-store deployment paths are isolated as
`<storage_root>/workload-<id>/shard-<id>`, independent of the worker holding the
lease. Previous builds could place multiple shards at the same remote prefix.
If an old shared namespace exists and the isolated target is absent, deployment
fails with an explicit migration error instead of silently opening empty state.
Existing local filesystem paths are unchanged.
