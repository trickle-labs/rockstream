# v0.72 local subscription publication ordering

**Scope:** partial #136/#132 implementation and tests. This file is not a capacity verdict, full end-to-end materialized-view proof, or permission to mark any V072 criterion complete.

## Implementation guarantee

The single-gateway PgWire `handle_commit` now serializes local commits under `shard_commit_lock` through base-table write, dependent view refresh, final storage flush, and **only then** bounded per-table change-log publication. The in-memory subscription registry must not expose an epoch before its flush succeeds. Every published table epoch is a complete vector, preserving signed retractions and duplicates. A too-large retained unit invalidates replay and requires resnapshot; it cannot reverse an already durable write.

This is an ordering improvement, not distributed atomic commit. Concurrent independent workers, CDC sources, migrations and the separate materialized-view read-token integration need additional fencing and release-process tests. In particular, downstream compiled-view refresh errors have their own separate handling and must not be presented as successful full view results.

## Regression commands

Run the real-gateway targeted suite:

```sh
rtk cargo test -p rockstream-gateway --test v072_durable_subscription_publication_tests
```

The test exercises: (a) an unacknowledged transaction has no replay entries, then an acknowledged commit publishes **exactly** the committed rows and they are readable; (b) two concurrent committed transactions produce two noninterleaved increasing epochs; and (c) a rejecting object store refuses a committed SST flush, where the transaction must error and the registry must remain empty. The object-store failure injection is an error-path regression, not a production resource benchmark.

Full qualification additionally requires full committed materialized-view tokens, complete snapshot/start and epoch/end frames, all #136 stream and memory bounds, real-process restart/failover/replay and independent committed-result oracles. No release sign-off is asserted.
