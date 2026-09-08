//! Test common helpers and mock implementations for CLI tests.
//! Isolated strictly within test scope to prevent fixture leakage into production binary.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rockstream_cli::output::*;
use rockstream_cli::transport::*;
use rockstream_cli::CliError;
use rockstream_types::acl::Role;
use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::{RS_1001, RS_2401, RS_5030};

pub struct MockTopologyClient {
    pub workers: Vec<WorkerStatusInfo>,
    pub shards: Vec<ShardInfo>,
    pub quotas: ClusterQuotasInfo,
    pub status: Option<ClusterStatusInfo>,
}

impl Default for MockTopologyClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MockTopologyClient {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
            shards: Vec::new(),
            quotas: ClusterQuotasInfo {
                total_memory_budget_bytes: 64 * 1024 * 1024 * 1024,
                used_memory_bytes: 4 * 1024 * 1024 * 1024,
                max_parallelism: 64,
                active_workloads: 0,
                active_views: 0,
            },
            status: None,
        }
    }

    pub fn with_mock_data(
        mut self,
        workers: Vec<WorkerStatusInfo>,
        shards: Vec<ShardInfo>,
        quotas: ClusterQuotasInfo,
    ) -> Self {
        self.workers = workers;
        self.shards = shards;
        self.quotas = quotas;
        self
    }
}

impl TopologyApi for MockTopologyClient {
    fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        if let Some(ref s) = self.status {
            return Ok(s.clone());
        }
        let active = self.workers.len();
        let healthy = self.workers.iter().filter(|w| w.healthy).count();
        Ok(ClusterStatusInfo {
            node_id: Some(1),
            role: "control".to_string(),
            term: 1,
            active_workers: active,
            healthy_workers: healthy,
            leader_id: Some(1),
            version: env!("CARGO_PKG_VERSION").to_string(),
        })
    }

    fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        Ok(self.quotas.clone())
    }

    fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        Ok(self.workers.clone())
    }

    fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let workers = self.list_workers()?;
        if let Some(id) = worker_id {
            let matched: Vec<_> = workers.into_iter().filter(|w| w.worker_id == id).collect();
            if matched.is_empty() {
                return Err(CliError::new(
                    RS_1001,
                    format!("Worker ID {id} not found"),
                    "Run 'rockstream cluster workers list' to check registered worker IDs.",
                ));
            }
            Ok(matched)
        } else {
            Ok(workers)
        }
    }

    fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        Ok(self.shards.clone())
    }
}

pub struct MockControlClient {
    pub identity: ClientIdentity,
    pub workers: Vec<WorkerStatusInfo>,
    pub shards: Vec<ShardInfo>,
    pub quotas: ClusterQuotasInfo,
    pub status: ClusterStatusInfo,
    pub storage_path: Option<PathBuf>,
}

impl MockControlClient {
    pub fn new(identity: ClientIdentity) -> Self {
        Self {
            identity,
            workers: vec![
                WorkerStatusInfo {
                    worker_id: 1,
                    role: "worker".to_string(),
                    address: "127.0.0.1:8001".to_string(),
                    capacity_headroom: 0.85,
                    host_id: "host-1".to_string(),
                    availability_zone: "us-east-1a".to_string(),
                    healthy: true,
                    lifecycle_state: "active".to_string(),
                    registered_at_ms: 1723620000000,
                },
                WorkerStatusInfo {
                    worker_id: 2,
                    role: "worker".to_string(),
                    address: "127.0.0.1:8002".to_string(),
                    capacity_headroom: 0.90,
                    host_id: "host-2".to_string(),
                    availability_zone: "us-east-1b".to_string(),
                    healthy: true,
                    lifecycle_state: "active".to_string(),
                    registered_at_ms: 1723620001000,
                },
            ],
            shards: vec![
                ShardInfo {
                    shard_id: 1,
                    worker_id: Some(1),
                    lease_token: 101,
                    status: "active".to_string(),
                    key_range: "[00000000..7fffffff]".to_string(),
                },
                ShardInfo {
                    shard_id: 2,
                    worker_id: Some(2),
                    lease_token: 102,
                    status: "active".to_string(),
                    key_range: "[80000000..ffffffff]".to_string(),
                },
            ],
            quotas: ClusterQuotasInfo {
                total_memory_budget_bytes: 64 * 1024 * 1024 * 1024,
                used_memory_bytes: 4 * 1024 * 1024 * 1024,
                max_parallelism: 64,
                active_workloads: 0,
                active_views: 0,
            },
            status: ClusterStatusInfo {
                node_id: Some(1),
                role: "all".to_string(),
                term: 0,
                active_workers: 2,
                healthy_workers: 2,
                leader_id: Some(1),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            storage_path: None,
        }
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    fn record_audit(&self, action: &str, resource: &str, detail: &str, error_code: Option<&str>) {
        let Some(path) = &self.storage_path else {
            return;
        };
        let mut event =
            AuditEvent::now(self.identity.user.clone(), action, resource).with_detail(detail);
        if let Some(code) = error_code {
            event = event.with_error_code(code);
        }
        let file = path.join("audit.jsonl");
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
        {
            let _ = serde_json::to_writer(&mut file, &event);
            let _ = std::io::Write::write_all(&mut file, b"\n");
        }
    }

    pub fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "cluster.workers.drain",
                &worker_id.to_string(),
                "unauthorized role",
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }
        self.record_audit(
            "cluster.workers.drain",
            &worker_id.to_string(),
            "drain initiated",
            None,
        );
        Ok(DrainOutcome {
            worker_id,
            status: "DRAINING".to_string(),
            remaining_shards: 0,
            message: format!("Worker {worker_id} drain initiated successfully"),
        })
    }

    pub fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "shard.migrate",
                &shard_id.to_string(),
                "unauthorized role",
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }
        if shard_id == 999 {
            self.record_audit(
                "shard.migrate",
                &shard_id.to_string(),
                "in-flight migration conflict",
                Some("RS-5030"),
            );
            return Err(CliError::new(
                RS_5030,
                format!("Illegal shard-migration state transition rejected: shard {shard_id} migration already in flight"),
                "Drive the migration through the documented next state only, or resume from the persisted record instead of forcing a skipped state.",
            ));
        }
        self.record_audit(
            "shard.migrate",
            &shard_id.to_string(),
            &format!("to_worker={target_worker}"),
            None,
        );
        Ok(MigrationOutcome {
            shard_id,
            source_worker: 1,
            target_worker,
            status: "COMPLETED".to_string(),
            duration_ms: 42,
        })
    }
}

impl TopologyApi for MockControlClient {
    fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        Ok(self.status.clone())
    }
    fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        Ok(self.quotas.clone())
    }
    fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        Ok(self.workers.clone())
    }
    fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let workers = self.list_workers()?;
        if let Some(id) = worker_id {
            let matched: Vec<_> = workers.into_iter().filter(|w| w.worker_id == id).collect();
            if matched.is_empty() {
                return Err(CliError::new(
                    RS_1001,
                    format!("Worker ID {id} not found"),
                    "Run 'rockstream cluster workers list' to check registered worker IDs.",
                ));
            }
            Ok(matched)
        } else {
            Ok(workers)
        }
    }
    fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        Ok(self.shards.clone())
    }
}

impl OperationApi for MockControlClient {
    fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        self.drain_worker(worker_id)
    }
    fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        self.migrate_shard(shard_id, target_worker)
    }
}

pub struct MockOperationClient {
    pub drained_workers: Arc<Mutex<Vec<u64>>>,
}

impl Default for MockOperationClient {
    fn default() -> Self {
        Self {
            drained_workers: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OperationApi for MockOperationClient {
    fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        self.drained_workers.lock().unwrap().push(worker_id);
        Ok(DrainOutcome {
            worker_id,
            status: "DRAINING".to_string(),
            remaining_shards: 0,
            message: format!("Worker {worker_id} drain initiated successfully"),
        })
    }

    fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        Ok(MigrationOutcome {
            shard_id,
            source_worker: 1,
            target_worker,
            status: "COMPLETED".to_string(),
            duration_ms: 25,
        })
    }
}

pub struct MockCatalogClient {
    pub client: CatalogClient,
}

impl Default for MockCatalogClient {
    fn default() -> Self {
        Self {
            client: catalog_with_defaults(),
        }
    }
}

impl CatalogApi for MockCatalogClient {
    fn list_views(&self) -> Result<Vec<ViewSummary>, CliError> {
        self.client.list_views()
    }
    fn get_view(&self, name: &str) -> Result<ViewDetail, CliError> {
        self.client.get_view(name)
    }
    fn view_status(&self, name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError> {
        self.client.view_status(name)
    }
    fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError> {
        self.client.list_sources()
    }
    fn get_source(&self, name: &str) -> Result<SourceDetail, CliError> {
        self.client.get_source(name)
    }
    fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError> {
        self.client.list_schemas()
    }
    fn get_schema(&self, name: &str) -> Result<SchemaDetail, CliError> {
        self.client.get_schema(name)
    }
    fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError> {
        self.client.list_workloads()
    }
    fn get_workload(&self, name: &str) -> Result<WorkloadDetail, CliError> {
        self.client.get_workload(name)
    }
    fn resource_usage(
        &self,
        workload_name: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError> {
        self.client.resource_usage(workload_name)
    }
    fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError> {
        self.client.resource_cluster()
    }
    fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError> {
        self.client.schema_evolution_status()
    }
    fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError> {
        self.client.schema_evolution_history()
    }
    fn pause_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.pause_view(name)
    }
    fn resume_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.resume_view(name)
    }
    fn query_view(&self, name: &str, limit: Option<usize>) -> Result<QueryResult, CliError> {
        self.client.query_view(name, limit)
    }
    fn pause_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.pause_source(name)
    }
    fn resume_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.resume_source(name)
    }
    fn drop_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.drop_source(name)
    }
    fn create_schema(
        &mut self,
        name: &str,
        columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError> {
        self.client.create_schema(name, columns_spec)
    }
    fn drop_schema(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.drop_schema(name)
    }
    fn create_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        self.client.create_workload(
            name,
            priority,
            freshness_slo_ms,
            memory_limit,
            max_parallelism,
        )
    }
    fn alter_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        self.client.alter_workload(
            name,
            priority,
            freshness_slo_ms,
            memory_limit,
            max_parallelism,
        )
    }
    fn drop_workload(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.client.drop_workload(name)
    }
}

pub struct MockStorageAdminClient {
    pub storage_client: StorageClient,
}

impl Default for MockStorageAdminClient {
    fn default() -> Self {
        Self {
            storage_client: StorageClient::new(),
        }
    }
}

impl StorageAdminApi for MockStorageAdminClient {
    fn export_checkpoint(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<CheckpointExportOutcome, CliError> {
        self.storage_client
            .export_checkpoint(storage_path, destination)
    }
    fn restore_checkpoint(
        &self,
        audit_path: &Path,
        source: &str,
        target: &str,
    ) -> Result<RestoreOutcome, CliError> {
        self.storage_client
            .restore_checkpoint(audit_path, source, target)
    }
    fn list_checkpoints(&self, storage_path: &Path) -> Result<Vec<CheckpointSummary>, CliError> {
        self.storage_client.list_checkpoints(storage_path)
    }
    fn show_checkpoint(
        &self,
        storage_path: &Path,
        checkpoint_id: u64,
    ) -> Result<CheckpointAlignmentInfo, CliError> {
        self.storage_client
            .show_checkpoint(storage_path, checkpoint_id)
    }
    fn generate_support_bundle(
        &self,
        storage_path: &Path,
        bundle_file: &Path,
    ) -> Result<SupportBundleInfo, CliError> {
        self.storage_client
            .generate_support_bundle_with_diagnostics(
                storage_path,
                None,
                None,
                Some(bundle_file),
                &[],
            )
    }
}

pub fn catalog_with_defaults() -> CatalogClient {
    let mut client = CatalogClient::new(ClientIdentity::default());

    // Default workload
    let wl = WorkloadDetail {
        name: "analytics".to_string(),
        priority: 128,
        freshness_slo_ms: Some(5000),
        memory_limit_bytes: Some(1024 * 1024 * 1024),
        max_parallelism: Some(16),
        assigned_views: vec!["active_users".to_string(), "hourly_revenue".to_string()],
    };
    client.workloads.insert("analytics".to_string(), wl);

    // Default views
    let v1 = ViewDetail {
        name: "active_users".to_string(),
        state: "RUNNING".to_string(),
        workload: Some("analytics".to_string()),
        freshness_slo_ms: Some(5000),
        memory_limit_bytes: Some(512 * 1024 * 1024),
        depends_on: vec!["users_source".to_string()],
        query: "SELECT id, count(*) FROM users GROUP BY id".to_string(),
        created_at_ms: 1723620000000,
    };
    let v2 = ViewDetail {
        name: "hourly_revenue".to_string(),
        state: "RUNNING".to_string(),
        workload: Some("analytics".to_string()),
        freshness_slo_ms: Some(5000),
        memory_limit_bytes: Some(512 * 1024 * 1024),
        depends_on: vec!["orders_source".to_string()],
        query: "SELECT hour, sum(amount) FROM orders GROUP BY hour".to_string(),
        created_at_ms: 1723620005000,
    };
    client.views.insert("active_users".to_string(), v1);
    client.views.insert("hourly_revenue".to_string(), v2);

    // Default sources
    let mut src_opts = BTreeMap::new();
    src_opts.insert("topic".to_string(), "users_events".to_string());
    src_opts.insert("group_id".to_string(), "rs_ingest".to_string());
    let s1 = SourceDetail {
        name: "users_source".to_string(),
        connector_type: "kafka".to_string(),
        table: "users".to_string(),
        status: "active".to_string(),
        options: src_opts,
        current_offset: Some("partition_0:482910".to_string()),
        lag_ms: Some(12),
    };
    let mut s2_opts = BTreeMap::new();
    s2_opts.insert("topic".to_string(), "orders_events".to_string());
    s2_opts.insert("group_id".to_string(), "rs_ingest".to_string());
    let s2 = SourceDetail {
        name: "orders_source".to_string(),
        connector_type: "kafka".to_string(),
        table: "orders".to_string(),
        status: "active".to_string(),
        options: s2_opts,
        current_offset: Some("partition_1:109283".to_string()),
        lag_ms: Some(45),
    };
    client.sources.insert("users_source".to_string(), s1);
    client.sources.insert("orders_source".to_string(), s2);

    // Default schemas
    let sch1 = SchemaDetail {
        name: "users".to_string(),
        columns: vec![
            SchemaColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
            SchemaColumn {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
            },
            SchemaColumn {
                name: "created_at".to_string(),
                data_type: "TIMESTAMP".to_string(),
                nullable: false,
            },
        ],
        entity_type: "table".to_string(),
    };
    let sch2 = SchemaDetail {
        name: "orders".to_string(),
        columns: vec![
            SchemaColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
            SchemaColumn {
                name: "amount".to_string(),
                data_type: "FLOAT".to_string(),
                nullable: false,
            },
            SchemaColumn {
                name: "hour".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
        ],
        entity_type: "table".to_string(),
    };
    let sch3 = SchemaDetail {
        name: "active_users".to_string(),
        columns: vec![
            SchemaColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
            SchemaColumn {
                name: "count".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
        ],
        entity_type: "view".to_string(),
    };
    client.schemas.insert("users".to_string(), sch1);
    client.schemas.insert("orders".to_string(), sch2);
    client.schemas.insert("active_users".to_string(), sch3);

    client
}
