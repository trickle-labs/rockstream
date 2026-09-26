# System limits reference

Authoritative operational, architectural, protocol, and parser limits enforced across RockStream.

| Limit Identifier | Name | Canonical Value | Unit | Enforcement Level | Metric or status path | Error Code | Description |
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
| `MAX_SOURCE_WAITERS` | In-flight Source Delta Waiters | 1024 | requests | Control-plane source waiter registry | `ControlServiceHandle::source_waiter_status().fill` | `RS-9001` | Rejects a new source delta request when all waiter slots are occupied; status includes the limit and overflow policy |
| `SHARD_ACTOR_MAILBOX` | Per-shard Execution Mailbox | 32 messages / 4194304 bytes | messages and bytes per shard | Worker shard actor registry | `WorkerResourceStatus.shard_mailboxes[]` | `RS-0001` | Waits for mailbox credit for up to 30 seconds; oversized frames and timeouts return a correlated failure; status includes fill, limits, and overflow policy |

## Issue #133 resource inventory

| Resource | Bound | Fill observation | Overflow or owner |
| --- | --- | --- | --- |
| Source request waiters | 1024 | `ControlServiceHandle::source_waiter_status().fill` | Reject new request with `RS-9001`; remove and fail affected requests with `RS-0001` when an owning worker disconnects |
| Shard actor mailbox | 32 messages and 4194304 bytes per shard | `WorkerResourceStatus.shard_mailboxes[]` | Backpressure for 30 seconds; correlated failure on oversized frame or timeout |
| Allocation waiters | 1024 per worker | `WorkerBudgetStatus.allocation_waiter_fill` and `allocation_waiter_capacity` | Reject new waiter with `RS-9001`; timed-out waiter returns `RS-5003` |
| Worker control line buffer | 67108864 bytes per worker | `WorkerResourceStatus.control_line_buffer_fill_bytes` and `control_line_buffer_limit_bytes` | Prospective `SourceBuffers` reservation; disconnect on oversized or invalid input |
| SlateDB unflushed write buffer | 67108864 bytes per shard database | `WorkerBudgetStatus.categories[slatedb_write_buffers]` and owners | Reserve before opening the database; reject when worker memory budget is exhausted with `RS-5003` |
| Worker-to-control command channel | 32 messages per worker | `WorkerResourceStatus.control_message_channel_fill` and `control_message_channel_capacity` | Tokio send backpressure; send fails after receiver closes |
| Worker fence-write acknowledgement waiters | 1024 per worker | `WorkerResourceStatus.fence_write_waiter_fill` and `fence_write_waiter_capacity` | Reject new waiter with `RS-9001`; remove pending entries after acknowledgement or channel close |
| Control-service worker outbound channel | 32 messages per worker connection | `ControlServiceHandle::worker_outbound_channel_status().fill/capacity` | Wait for channel capacity; send fails after worker connection closes |
| Shuffle stream sender queue | 64 frames per target worker stream | `WorkerStreamMultiplexer::sender_queue_status().fill/capacity` | Wait for channel capacity; durable fallback after stream failure |
| Exchange inlet channel | Configured frame-channel capacity per inlet | `ExchangeRegistry::inlet_channel_status().fill/capacity` | Wait for receiver capacity; report receiver closed |
| Exchange acknowledgement channel | 64 acknowledgements per ExchangeStream | `ExchangeRegistry::acknowledgement_channel_status().fill/capacity` | Wait for stream capacity |
| Drain queue | 1024 tasks | Drain response queue fill and capacity | Reject queue growth beyond the limit |
| Management acknowledgement waiters | 64 per waiter registry | Cluster status acknowledgement waiter fill and capacity | Reject insertion when full |
| Backup scan and retry | 1024 objects per scan; 3 retries; 64 MiB pending bytes; 4 concurrent copies | `BackupConcurrencyGovernor::pending_bytes()` and `available_permits()` | Bounded scan/retry/copy; reject over-limit work |
| Checkpoint export | 1 export in flight; 1024 objects per scan; 64 MiB per object | `checkpoint_export_objects_in_flight` and checkpoint export scan/object-buffer fill gauges | Reject oversized objects/scans and concurrent exports |
| Migration copy and verification | 256 rows and 1 MiB per chunk; 1024 scan keys; 1024 active migrations | `MigrationFillLevel` for tracked maps and `MigrationCopyStats` after a copy | Bounded pages; reject over-limit scans or registrations |
| Operator pipeline channels | 16 batches per input or output channel | `OPERATOR_CHANNEL_CAPACITY`; live fill is not exposed | Tokio send waits for downstream capacity; send fails when the receiver closes |
| Branch scheduler | 16 concurrently running branch tasks | `BranchScheduler::active_tasks()` and `peak_active_tasks()` | Additional tasks wait on the semaphore; the pending task set has no separate bound or fill observation |
| Worker secret-token cache | 1024 resolved tokens per worker | `WorkerSecretManager::fill_level()` | Reject a new token with `[RS-3601]` when full; expired entries are removed during lookup |
| Secret store | 100000 secrets per namespace; 1000000 in-memory secrets per process; 10000 references per secret; 1000000 references per process | `SecretStore::metrics()` exposes scan, in-memory, reference, and rotation fill levels | Reject over-limit creates, references, and scans with `secret.capacity_exceeded`; rotation watch retains the latest notification (capacity 1) |
| ACL cache | 10000 entries; 60-second TTL | `AclStore::cache_size()` / `acl_cache_size` | Evict the oldest entry on insertion at capacity; expired entries are treated as misses |
| Management request ingress | 64 concurrent requests | `GetClusterStatusResponse.request_fill/request_capacity` | Reject excess requests with `RESOURCE_EXHAUSTED` |
| Management operation store | 1000 active records; 10000 retained records; 64 transitions per record | `GetClusterStatusResponse.active_operations/retained_operations`; transition history on each operation record | Reject creates/history growth at the limit; operation listing pages are 1–100 records |
| Operations catalog projection | 1000 result rows | `CatalogResponse::Rows.rows.len()` | Return the complete result at the limit; reject larger scans with `RS-9001` / SQLSTATE `54000` |
### Delegated entries — owner attribution only

| Resource family | Owner |
| --- | --- |
| Physical commit queues, retries, and durable write buffers | S2 / issue #134 |
| Interactive-query work, queues, returned results, and waiters | S3 / issue #135 |
| Subscription streams, snapshots, and buffers | S4 / issue #136 |

R5 remains open. Source waiters now expire on an owning worker disconnect, and the control outbound, shuffle sender, exchange inlet, and exchange acknowledgement queues expose live fill. The operator channel fill is not exposed, and branch scheduling retains a pending set without a separate bound or fill. The delegated entries above record owner attribution only; they do not claim S2, S3, or S4 limits or behavior.
