//! System Catalog surface contributor (DOC-001).

use crate::manifest::{
    CatalogColumnDescriptor, CatalogSchemaDescriptor, CatalogSurface, CatalogTableDescriptor,
};

pub struct CatalogContributor;

impl CatalogContributor {
    /// Extract system catalog schemas, tables, and columns.
    pub fn extract() -> CatalogSurface {
        let tables = vec![
            CatalogTableDescriptor {
                name: "arrangements".to_string(),
                description: "Materialized arrangements maintaining persistent index state"
                    .to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "arrangement_id".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Unique 64-bit identifier for the arrangement".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "consumer_count".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Number of active operators consuming this arrangement"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "shared_state_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Total memory and storage footprint in bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "bytes_saved".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Storage bytes saved via dedup and arrangement sharing"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "compaction_frontier".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Compaction frontier timestamp / epoch watermark".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "partitioning".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Key partition routing scheme".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by operator arrangements count".to_string()),
            },
            CatalogTableDescriptor {
                name: "capabilities".to_string(),
                description: "System features, connectors, sinks, and storage capabilities"
                    .to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "id".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Canonical capability identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "kind".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Capability category (connector, sink, engine, sql)"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Human-readable capability name".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "tier".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Stability tier (Core, Supported, Experimental)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "description".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Detailed description and operational notes".to_string(),
                    },
                ],
                cardinality_bound: Some("Static compile-time registry bounds".to_string()),
            },
            CatalogTableDescriptor {
                name: "checkpoints".to_string(),
                description: "Durable checkpoint manifests and epoch commit ledger".to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "checkpoint_id".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Monotonically increasing checkpoint identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "committed_at".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "ISO 8601 timestamp of checkpoint commit".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "epoch_number".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Epoch sequence number associated with checkpoint".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "frontier".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Watermark frontier captured at checkpoint".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "storage_path".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Storage directory / URI where manifest is written"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "duration_ms".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Total wall-clock duration of checkpoint creation in ms"
                            .to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by retention history policy".to_string()),
            },
            CatalogTableDescriptor {
                name: "dead_letter_queue".to_string(),
                description: "Poison and malformed ingestion records routed to DLQ".to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "arrived_at".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Arrival timestamp of poison event".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "source_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Originating source connector identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "source_offset".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Source partition and stream offset".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "error_code".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "RS-XXXX error code describing failure".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "error_message".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Descriptive error message and diagnostic context".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "raw_bytes_hex".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Hex-encoded raw payload bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "replay_attempt".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 7,
                        description: "Number of replay attempts executed".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by DLQ buffer capacity limit".to_string()),
            },
            CatalogTableDescriptor {
                name: "nodes".to_string(),
                description: "Active cluster compute and storage worker nodes".to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "node_id".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Unique node identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "worker_id".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Worker process identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "role".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Assigned node role (coordinator, worker, all-in-one)"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "address".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Network RPC / pgwire address".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "state".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Node state (active, draining, failed)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "lease_count".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Active shard leases held by node".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "memory_budget_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 7,
                        description: "Allocated memory budget in bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "last_heartbeat_at".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 8,
                        description: "Timestamp of last received node heartbeat".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by cluster node size".to_string()),
            },
            CatalogTableDescriptor {
                name: "operators".to_string(),
                description: "Executable query pipeline operators and state arrangements"
                    .to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "operator_id".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Unique operator identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "view_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Target view owning this operator".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "operator_kind".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Operator kind (Filter, Project, Aggregate, Join, Window)"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "merge_law_id".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "DBSP merge law used for incremental maintenance".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "dirty_key_count".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Number of dirty keys in current delta batch".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "logical_write_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Total logical bytes processed".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by active operators across views".to_string()),
            },
            CatalogTableDescriptor {
                name: "sources".to_string(),
                description: "Configured streaming ingestion sources and connector status"
                    .to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Source connector name".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "type".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Connector protocol type (kafka, kinesis, file)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "format".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Payload data format (json, csv, avro)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "status".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Operational status (running, paused, failed)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "live_offset".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Latest committed source stream offset".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "live_lag_ms".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Current ingestion lag in milliseconds".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "buffer_fill".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 7,
                        description: "Ingestion buffer fill percentage".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "schema_version".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 8,
                        description: "Registered schema version number".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by active source configurations".to_string()),
            },
            CatalogTableDescriptor {
                name: "view_resource_usage".to_string(),
                description: "Resource consumption metrics aggregated per view".to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "view_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Target view name".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "workload_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Assigned workload namespace".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "cpu_time_ms".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Total CPU execution time consumed in ms".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "allocated_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Total heap memory allocated in bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "state_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Durable state footprint in storage".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "processed_records".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Cumulative record count processed".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by materialized views count".to_string()),
            },
            CatalogTableDescriptor {
                name: "views".to_string(),
                description: "Materialized views defined in the incremental IVM engine".to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "namespace".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Database namespace / schema name".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "view_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "View identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "state".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "View lifecycle state (initializing, ready, error)"
                            .to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "workload_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Assigned workload namespace".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "workload_source".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Workload definition source (explicit, default)".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "arrangement_id".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 6,
                        description: "Output arrangement identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "shared_state_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 7,
                        description: "State memory footprint in bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "frontier".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 8,
                        description: "Current view progress frontier timestamp".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by active views count".to_string()),
            },
            CatalogTableDescriptor {
                name: "workload_resource_usage".to_string(),
                description: "Resource consumption metrics aggregated per workload group"
                    .to_string(),
                columns: vec![
                    CatalogColumnDescriptor {
                        name: "workload_name".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: false,
                        ordinal_position: 1,
                        description: "Workload group identifier".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "cpu_time_ms".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 2,
                        description: "Total CPU execution time consumed in ms".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "allocated_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 3,
                        description: "Total heap memory allocated in bytes".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "state_bytes".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 4,
                        description: "Durable state footprint in storage".to_string(),
                    },
                    CatalogColumnDescriptor {
                        name: "processed_records".to_string(),
                        data_type: "INT8".to_string(),
                        nullable: false,
                        ordinal_position: 5,
                        description: "Cumulative record count processed".to_string(),
                    },
                ],
                cardinality_bound: Some("Bounded by workload groups count".to_string()),
            },
        ];

        let mut schema = CatalogSchemaDescriptor {
            name: "rockstream_catalog".to_string(),
            description: "RockStream system catalog virtual schemas and introspective tables"
                .to_string(),
            tables,
        };
        schema.tables.sort_by(|a, b| a.name.cmp(&b.name));
        for t in &mut schema.tables {
            t.columns.sort_by_key(|c| c.ordinal_position);
        }

        CatalogSurface {
            schemas: vec![schema],
        }
    }
}
