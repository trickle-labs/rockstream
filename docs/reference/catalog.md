# Catalog reference

## `rockstream_catalog`

RockStream system catalog virtual schemas and introspective tables

### `arrangements`

Materialized arrangements maintaining persistent index state  
**Cardinality bound:** Bounded by operator arrangements count

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| arrangement_id | 1 | INT8 | false | Unique 64-bit identifier for the arrangement |
| consumer_count | 2 | INT8 | false | Number of active operators consuming this arrangement |
| shared_state_bytes | 3 | INT8 | false | Total memory and storage footprint in bytes |
| bytes_saved | 4 | INT8 | false | Storage bytes saved via dedup and arrangement sharing |
| compaction_frontier | 5 | TEXT | false | Compaction frontier timestamp / epoch watermark |
| partitioning | 6 | TEXT | false | Key partition routing scheme |

### `capabilities`

System features, connectors, sinks, and storage capabilities  
**Cardinality bound:** Static compile-time registry bounds

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| id | 1 | TEXT | false | Canonical capability identifier |
| kind | 2 | TEXT | false | Capability category (connector, sink, engine, sql) |
| name | 3 | TEXT | false | Human-readable capability name |
| tier | 4 | TEXT | false | Stability tier (Core, Supported, Experimental) |
| description | 5 | TEXT | false | Detailed description and operational notes |

### `checkpoints`

Durable checkpoint manifests and epoch commit ledger  
**Cardinality bound:** Bounded by retention history policy

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| checkpoint_id | 1 | INT8 | false | Monotonically increasing checkpoint identifier |
| committed_at | 2 | TEXT | false | ISO 8601 timestamp of checkpoint commit |
| epoch_number | 3 | INT8 | false | Epoch sequence number associated with checkpoint |
| frontier | 4 | TEXT | false | Watermark frontier captured at checkpoint |
| storage_path | 5 | TEXT | false | Storage directory / URI where manifest is written |
| duration_ms | 6 | INT8 | false | Total wall-clock duration of checkpoint creation in ms |

### `dead_letter_queue`

Poison and malformed ingestion records routed to DLQ  
**Cardinality bound:** Bounded by DLQ buffer capacity limit

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| arrived_at | 1 | TEXT | false | Arrival timestamp of poison event |
| source_name | 2 | TEXT | false | Originating source connector identifier |
| source_offset | 3 | TEXT | false | Source partition and stream offset |
| error_code | 4 | TEXT | false | RS-XXXX error code describing failure |
| error_message | 5 | TEXT | false | Descriptive error message and diagnostic context |
| raw_bytes_hex | 6 | TEXT | false | Hex-encoded raw payload bytes |
| replay_attempt | 7 | INT8 | false | Number of replay attempts executed |

### `nodes`

Active cluster compute and storage worker nodes  
**Cardinality bound:** Bounded by cluster node size

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| node_id | 1 | TEXT | false | Unique node identifier |
| worker_id | 2 | TEXT | false | Worker process identifier |
| role | 3 | TEXT | false | Assigned node role (coordinator, worker, all-in-one) |
| address | 4 | TEXT | false | Network RPC / pgwire address |
| state | 5 | TEXT | false | Node state (active, draining, failed) |
| lease_count | 6 | INT8 | false | Active shard leases held by node |
| memory_budget_bytes | 7 | INT8 | false | Allocated memory budget in bytes |
| last_heartbeat_at | 8 | TEXT | false | Timestamp of last received node heartbeat |

### `operators`

Executable query pipeline operators and state arrangements  
**Cardinality bound:** Bounded by active operators across views

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| operator_id | 1 | INT8 | false | Unique operator identifier |
| view_name | 2 | TEXT | false | Target view owning this operator |
| operator_kind | 3 | TEXT | false | Operator kind (Filter, Project, Aggregate, Join, Window) |
| merge_law_id | 4 | TEXT | false | DBSP merge law used for incremental maintenance |
| dirty_key_count | 5 | INT8 | false | Number of dirty keys in current delta batch |
| logical_write_bytes | 6 | INT8 | false | Total logical bytes processed |

### `sources`

Configured streaming ingestion sources and connector status  
**Cardinality bound:** Bounded by active source configurations

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| name | 1 | TEXT | false | Source connector name |
| type | 2 | TEXT | false | Connector protocol type (kafka, kinesis, file) |
| format | 3 | TEXT | false | Payload data format (json, csv, avro) |
| status | 4 | TEXT | false | Operational status (running, paused, failed) |
| live_offset | 5 | TEXT | false | Latest committed source stream offset |
| live_lag_ms | 6 | INT8 | false | Current ingestion lag in milliseconds |
| buffer_fill | 7 | INT8 | false | Ingestion buffer fill percentage |
| schema_version | 8 | INT8 | false | Registered schema version number |

### `view_resource_usage`

Resource consumption metrics aggregated per view  
**Cardinality bound:** Bounded by materialized views count

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| view_name | 1 | TEXT | false | Target view name |
| workload_name | 2 | TEXT | false | Assigned workload namespace |
| cpu_time_ms | 3 | INT8 | false | Total CPU execution time consumed in ms |
| allocated_bytes | 4 | INT8 | false | Total heap memory allocated in bytes |
| state_bytes | 5 | INT8 | false | Durable state footprint in storage |
| processed_records | 6 | INT8 | false | Cumulative record count processed |

### `views`

Materialized views defined in the incremental IVM engine  
**Cardinality bound:** Bounded by active views count

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| namespace | 1 | TEXT | false | Database namespace / schema name |
| view_name | 2 | TEXT | false | View identifier |
| state | 3 | TEXT | false | View lifecycle state (initializing, ready, error) |
| workload_name | 4 | TEXT | false | Assigned workload namespace |
| workload_source | 5 | TEXT | false | Workload definition source (explicit, default) |
| arrangement_id | 6 | INT8 | false | Output arrangement identifier |
| shared_state_bytes | 7 | INT8 | false | State memory footprint in bytes |
| frontier | 8 | TEXT | false | Current view progress frontier timestamp |

### `workload_resource_usage`

Resource consumption metrics aggregated per workload group  
**Cardinality bound:** Bounded by workload groups count

| Column | Position | Type | Nullable | Description |
| --- | --- | --- | --- | --- |
| workload_name | 1 | TEXT | false | Workload group identifier |
| cpu_time_ms | 2 | INT8 | false | Total CPU execution time consumed in ms |
| allocated_bytes | 3 | INT8 | false | Total heap memory allocated in bytes |
| state_bytes | 4 | INT8 | false | Durable state footprint in storage |
| processed_records | 5 | INT8 | false | Cumulative record count processed |

