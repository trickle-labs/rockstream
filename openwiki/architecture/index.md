# Files

- [Cluster authority, shard placement, and exchange](cluster-and-exchange.md) - How control services assign lease-owned shards, route distributed execution, and connect workers through bounded local, direct, and durable exchange paths. Explains fencing, replay, acknowledgments, migration, and the limits of control-plane HA.
- [SQL compilation and incremental execution](incremental-sql-engine.md) - How SQL becomes logical plans and executable incremental pipelines, with contracts for weighted deltas, arrangements, checked arithmetic, and join strategy selection.
- [System ownership and execution boundaries](system-overview.md) - Maps the Rust workspace to runtime ownership domains and follows compilation, ingestion, durable commits, coordination, and PostgreSQL serving in standalone and distributed execution.
