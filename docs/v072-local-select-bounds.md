# v0.72 gateway query producer limits

## Local ShardDb SELECT producer bound (v0.72)

The ordinary local `SELECT ... FROM <table/view>` storage path now reuses
the existing `scan_relation_rows_bounded` producer, rather than accumulating
an unlimited `scan_prefix()` result before applying `LIMIT`. It enforces
64 MiB of scanned key/value bytes and at most 1,000,000 rows; a truncated
storage scan fails as a complete result, never as silently incomplete rows.
Compiled-view materialization and distributed reads have separate execution
paths and limitations. This is a protective ceiling, **not** a guarantee
that indexed/ordered `LIMIT` queries will always complete on a larger source:
a full-prefix scan can hit the bound before truncation; pagination and pushdown
optimizations remain future work.
