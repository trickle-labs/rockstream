# System limits reference

Authoritative operational, architectural, protocol, and parser limits enforced across RockStream.

| Limit Identifier | Name | Canonical Value | Unit | Enforcement Level | Metric Name | Error Code | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `MAX_RESULT_ROWS` | Result Set Row Limit | 10000 | rows | Gateway query execution | `gateway_result_rows` | `RS-2040` | Maximum in-flight result set size per query execution |
| `MAX_CONN_MEMORY` | Connection Memory Limit | 67108864 | bytes | Gateway per-connection buffer | `gateway_connection_memory_bytes` | `RS-2053` | Maximum memory allocation per client connection |
| `MAX_CONNECTIONS` | Concurrent Connections Limit | 100 | connections | Gateway listener accept loop | `gateway_active_connections` | `RS-2055` | Maximum concurrent active client connections to gateway |
| `MAX_PREPARED_STMTS` | Prepared Statements per Connection | 100 | statements | Gateway session registry | `gateway_prepared_statements_active` | `RS-2600` | Maximum active prepared statements per connection |
| `MAX_PORTALS` | Portals per Connection | 50 | portals | Gateway session registry | `gateway_portals_active` | `RS-2601` | Maximum active portals per connection |
| `MAX_CURSORS` | Cursors per Connection | 64 | cursors | Gateway cursor registry | `gateway_cursors_active` | `RS-2052` | Maximum open cursors per connection |
| `MAX_IDENTIFIER_LEN` | Identifier Length Limit | 63 | bytes | SQL parser / lexer | `sql_parse_errors_total` | `RS-1012` | Maximum byte length of SQL identifiers |
| `MAX_DECIMAL_PRECISION` | Decimal Precision Limit | 38 | digits | SQL type checker | `sql_type_errors_total` | `RS-1016` | Maximum digits of precision for DECIMAL/NUMERIC types |
| `MAX_VIEW_DAG_DEPTH` | View Dependency DAG Depth | 16 | levels | View compiler DAG validator | `view_compilation_errors_total` | `RS-1011` | Maximum depth of materialized view-on-view dependency hierarchy |


## DataFusion ad hoc SELECT fallback (v0.72)

The gateway's standalone DataFusion SELECT fallback (queries without a
production relation scan) now checks producer output **while streaming
record batches**, rather than calling unbounded `collect()`. Limits are
10,000 returned rows, 8 MiB of Arrow array memory held for the response,
a 5-second planning/execution/stream-consumption deadline, and 16 concurrent
fallback queries gateway-wide. Overflow fails the request rather than
truncating results. Cancellation or timeout drops the stream and concurrency
permit. Limits apply to this fallback path only; they do not certify every
DataFusion operator's intermediate allocation or the separate distributed
query-time execution path.
