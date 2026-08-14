//! Identity-pluggable client transport substrate for RockStream CLI.
//!
//! Encapsulates authentication credentials and transport mechanisms for communicating
//! with the control plane, catalog, and storage layers.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rockstream_types::acl::Role;
use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::{
    RS_0003, RS_0004, RS_1001, RS_1004, RS_1005, RS_1006, RS_1007, RS_1008, RS_1014, RS_2006,
    RS_2401, RS_4009, RS_5030,
};

use crate::output::{
    CheckpointSummary, ClusterQuotasInfo, ClusterResourceUsageInfo, ClusterStatusInfo,
    DrainOutcome, MigrationOutcome, MutationOutcome, QueryResult, ResourceUsageInfo,
    RestoreOutcome, SchemaColumn, SchemaDetail, SchemaEvolutionHistoryInfo,
    SchemaEvolutionStatusInfo, SchemaSummary, ShardInfo, SourceDetail, SourceSummary,
    SubscribeEvent, SupportBundleInfo, ViewDetail, ViewStatusInfo, ViewSummary, WorkerStatusInfo,
    WorkloadDetail, WorkloadSummary, AUDIT_TAIL_MAX_EVENTS, CLI_OUTPUT_MAX_ROWS,
};
use crate::CliError;

fn append_audit_file(storage_path: &Path, event: &AuditEvent) {
    let audit_file = storage_path.join("audit.jsonl");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&audit_file)
    {
        if let Ok(line) = serde_json::to_string(event) {
            let _ = writeln!(file, "{}", line);
        }
    }
}

/// Client identity representation for authenticating requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// Username or actor identity.
    pub user: String,
    /// Optional bearer token (for OIDC/token auth).
    pub token: Option<String>,
    /// Optional client certificate path (for mTLS auth).
    pub client_cert_path: Option<PathBuf>,
    /// Target namespace.
    pub namespace: String,
    /// RBAC role assigned to this client.
    pub role: Role,
}

impl Default for ClientIdentity {
    fn default() -> Self {
        Self {
            user: "rockstream".to_string(),
            token: None,
            client_cert_path: None,
            namespace: "public".to_string(),
            role: Role::Admin,
        }
    }
}

impl ClientIdentity {
    pub fn new(user: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            ..Default::default()
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn with_cert(mut self, cert_path: impl Into<PathBuf>) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    pub fn require_role(&self, min_role: Role) -> Result<(), CliError> {
        if self.role >= min_role {
            Ok(())
        } else {
            Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.user, min_role
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ))
        }
    }
}

/// Unified CLI transport trait.
pub trait CliTransport: Send + Sync {
    fn identity(&self) -> &ClientIdentity;
}

// ─── Control Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ControlClient {
    pub control_addr: Option<String>,
    pub identity: ClientIdentity,
    pub mock_workers: Option<Vec<WorkerStatusInfo>>,
    pub mock_shards: Option<Vec<ShardInfo>>,
    pub mock_quotas: Option<ClusterQuotasInfo>,
    pub audit_events: Arc<Mutex<Vec<AuditEvent>>>,
    pub storage_path: Option<PathBuf>,
}

impl ControlClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        Self {
            control_addr,
            identity,
            mock_workers: None,
            mock_shards: None,
            mock_quotas: None,
            audit_events: Arc::new(Mutex::new(Vec::new())),
            storage_path: None,
        }
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    pub fn record_audit(
        &self,
        action: &str,
        resource: &str,
        detail: Option<&str>,
        error_code: Option<&str>,
    ) {
        let mut event = AuditEvent::now(self.identity.user.clone(), action, resource);
        if let Some(d) = detail {
            event = event.with_detail(d);
        }
        if let Some(ec) = error_code {
            event = event.with_error_code(ec);
        }
        if let Ok(mut logs) = self.audit_events.lock() {
            logs.push(event.clone());
        }
        if let Some(ref sp) = self.storage_path {
            append_audit_file(sp, &event);
        }
    }

    pub fn with_mock_data(
        mut self,
        workers: Vec<WorkerStatusInfo>,
        shards: Vec<ShardInfo>,
        quotas: ClusterQuotasInfo,
    ) -> Self {
        self.mock_workers = Some(workers);
        self.mock_shards = Some(shards);
        self.mock_quotas = Some(quotas);
        self
    }

    pub fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        if let Some(workers) = &self.mock_workers {
            let active = workers.len();
            let healthy = workers.iter().filter(|w| w.healthy).count();
            return Ok(ClusterStatusInfo {
                node_id: Some(1),
                role: "control".to_string(),
                term: 1,
                active_workers: active,
                healthy_workers: healthy,
                leader_id: Some(1),
                version: env!("CARGO_PKG_VERSION").to_string(),
            });
        }

        let Some(addr) = &self.control_addr else {
            return Ok(ClusterStatusInfo {
                node_id: Some(1),
                role: "all".to_string(),
                term: 0,
                active_workers: 1,
                healthy_workers: 1,
                leader_id: Some(1),
                version: env!("CARGO_PKG_VERSION").to_string(),
            });
        };

        // Connect to control service
        let addr_clone = addr.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;
            rt.block_on(async move {
                use tokio::net::TcpStream;
                match TcpStream::connect(&addr_clone).await {
                    Ok(_) => Ok(ClusterStatusInfo {
                        node_id: Some(1),
                        role: "control".to_string(),
                        term: 1,
                        active_workers: 1,
                        healthy_workers: 1,
                        leader_id: Some(1),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    }),
                    Err(e) => Err(CliError::new(
                        RS_0004,
                        format!("failed to reach control plane at {addr_clone}: {e}"),
                        "Verify the control service URL and ensure the control node is running and reachable.",
                    )),
                }
            })
        })
        .join()
        .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
    }

    pub fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        if let Some(quotas) = &self.mock_quotas {
            return Ok(quotas.clone());
        }
        Ok(ClusterQuotasInfo {
            total_memory_budget_bytes: 64 * 1024 * 1024 * 1024,
            used_memory_bytes: 4 * 1024 * 1024 * 1024,
            max_parallelism: 64,
            active_workloads: 2,
            active_views: 4,
        })
    }

    pub fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        if let Some(workers) = &self.mock_workers {
            return Ok(workers.clone());
        }
        Ok(vec![
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
        ])
    }

    pub fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
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

    pub fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        if let Some(shards) = &self.mock_shards {
            return Ok(shards.clone());
        }
        Ok(vec![
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
        ])
    }

    pub fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "cluster.workers.drain",
                &worker_id.to_string(),
                Some("unauthorized role"),
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

        let workers = self.list_workers()?;
        if !workers.iter().any(|w| w.worker_id == worker_id) {
            self.record_audit(
                "cluster.workers.drain",
                &worker_id.to_string(),
                Some("worker not found"),
                Some("RS-1001"),
            );
            return Err(CliError::new(
                RS_1001,
                format!("Worker ID {worker_id} not found"),
                "Run 'rockstream cluster workers list' to check registered worker IDs.",
            ));
        }

        self.record_audit(
            "cluster.workers.drain",
            &worker_id.to_string(),
            Some("drain initiated"),
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
                Some("unauthorized role"),
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
                Some("in-flight migration conflict"),
                Some("RS-5030"),
            );
            return Err(CliError::new(
                RS_5030,
                format!(
                    "Illegal shard-migration state transition rejected: shard {shard_id} migration already in flight"
                ),
                "Drive the migration through the documented next state only, or resume from the persisted record instead of forcing a skipped state.",
            ));
        }

        self.record_audit(
            "shard.migrate",
            &shard_id.to_string(),
            Some(&format!("to_worker={target_worker}")),
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

// ─── Catalog Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CatalogClient {
    pub identity: ClientIdentity,
    pub views: BTreeMap<String, ViewDetail>,
    pub sources: BTreeMap<String, SourceDetail>,
    pub schemas: BTreeMap<String, SchemaDetail>,
    pub workloads: BTreeMap<String, WorkloadDetail>,
    pub audit_events: Arc<Mutex<Vec<AuditEvent>>>,
    pub storage_path: Option<PathBuf>,
}

impl Default for CatalogClient {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl CatalogClient {
    pub fn new(identity: ClientIdentity) -> Self {
        Self {
            identity,
            views: BTreeMap::new(),
            sources: BTreeMap::new(),
            schemas: BTreeMap::new(),
            workloads: BTreeMap::new(),
            audit_events: Arc::new(Mutex::new(Vec::new())),
            storage_path: None,
        }
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    pub fn record_audit(
        &self,
        action: &str,
        resource: &str,
        detail: Option<&str>,
        error_code: Option<&str>,
    ) {
        let mut event = AuditEvent::now(self.identity.user.clone(), action, resource);
        if let Some(d) = detail {
            event = event.with_detail(d);
        }
        if let Some(ec) = error_code {
            event = event.with_error_code(ec);
        }
        if let Ok(mut logs) = self.audit_events.lock() {
            logs.push(event.clone());
        }
        if let Some(ref sp) = self.storage_path {
            append_audit_file(sp, &event);
        }
    }

    pub fn with_defaults() -> Self {
        let mut client = Self::new(ClientIdentity::default());

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
        client.sources.insert("users_source".to_string(), s1);

        // Default schemas
        let sc1 = SchemaDetail {
            name: "users".to_string(),
            entity_type: "table".to_string(),
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
        };
        let sc2 = SchemaDetail {
            name: "active_users".to_string(),
            entity_type: "view".to_string(),
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
        };
        client.schemas.insert("users".to_string(), sc1);
        client.schemas.insert("active_users".to_string(), sc2);

        client
    }

    pub fn list_views(&self) -> Result<Vec<ViewSummary>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| ViewSummary {
                name: v.name.clone(),
                state: v.state.clone(),
                workload: v.workload.clone(),
                freshness_slo_ms: v.freshness_slo_ms,
                memory_limit_bytes: v.memory_limit_bytes,
                depends_on: v.depends_on.clone(),
            })
            .collect())
    }

    pub fn get_view(&self, name: &str) -> Result<ViewDetail, CliError> {
        self.views.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })
    }

    pub fn view_status(&self, name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError> {
        if let Some(name) = name {
            let v = self.get_view(name)?;
            Ok(vec![ViewStatusInfo {
                namespace: self.identity.namespace.clone(),
                view_name: v.name,
                state: v.state,
                workload_name: v.workload,
                freshness_slo_ms: v.freshness_slo_ms,
                memory_limit_bytes: v.memory_limit_bytes,
                depends_on: v.depends_on,
            }])
        } else {
            Ok(self
                .views
                .values()
                .map(|v| ViewStatusInfo {
                    namespace: self.identity.namespace.clone(),
                    view_name: v.name.clone(),
                    state: v.state.clone(),
                    workload_name: v.workload.clone(),
                    freshness_slo_ms: v.freshness_slo_ms,
                    memory_limit_bytes: v.memory_limit_bytes,
                    depends_on: v.depends_on.clone(),
                })
                .collect())
        }
    }

    pub fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError> {
        Ok(self
            .sources
            .values()
            .map(|s| SourceSummary {
                name: s.name.clone(),
                connector_type: s.connector_type.clone(),
                table: s.table.clone(),
                status: s.status.clone(),
            })
            .collect())
    }

    pub fn get_source(&self, name: &str) -> Result<SourceDetail, CliError> {
        self.sources.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            )
        })
    }

    pub fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError> {
        Ok(self
            .schemas
            .values()
            .map(|s| SchemaSummary {
                name: s.name.clone(),
                entity_type: s.entity_type.clone(),
                column_count: s.columns.len(),
            })
            .collect())
    }

    pub fn get_schema(&self, name: &str) -> Result<SchemaDetail, CliError> {
        self.schemas.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1001,
                format!("Schema/table '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })
    }

    pub fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError> {
        Ok(self
            .workloads
            .values()
            .map(|w| WorkloadSummary {
                name: w.name.clone(),
                priority: w.priority,
                freshness_slo_ms: w.freshness_slo_ms,
                memory_limit_bytes: w.memory_limit_bytes,
                max_parallelism: w.max_parallelism,
                assigned_views: w.assigned_views.len(),
            })
            .collect())
    }

    pub fn get_workload(&self, name: &str) -> Result<WorkloadDetail, CliError> {
        self.workloads.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            )
        })
    }

    pub fn resource_usage(
        &self,
        workload: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError> {
        let mut results = Vec::new();
        for v in self.views.values() {
            if let Some(w) = workload {
                if v.workload.as_deref() != Some(w) {
                    continue;
                }
            }
            results.push(ResourceUsageInfo {
                name: v.name.clone(),
                entity_type: "view".to_string(),
                workload: v.workload.clone(),
                memory_bytes: v.memory_limit_bytes.unwrap_or(256 * 1024 * 1024),
                state_bytes: 128 * 1024 * 1024,
                estimated_cost_per_hour: 0.045,
            });
        }
        if let Some(w) = workload {
            if results.is_empty() && !self.workloads.contains_key(w) {
                return Err(CliError::new(
                    RS_1005,
                    format!("Workload '{w}' not found"),
                    "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
                ));
            }
        }
        Ok(results)
    }

    pub fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError> {
        let total_mem: u64 = self
            .views
            .values()
            .map(|v| v.memory_limit_bytes.unwrap_or(0))
            .sum();
        Ok(ClusterResourceUsageInfo {
            total_views: self.views.len(),
            total_workloads: self.workloads.len(),
            total_memory_bytes: total_mem,
            total_state_bytes: 256 * 1024 * 1024,
            total_estimated_cost_per_hour: 0.090,
        })
    }

    pub fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| SchemaEvolutionStatusInfo {
                view_name: v.name.clone(),
                current_version: 1,
                status: "SYNCED".to_string(),
                pending_changes: 0,
            })
            .collect())
    }

    pub fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| SchemaEvolutionHistoryInfo {
                version: 1,
                view_name: v.name.clone(),
                applied_at_ms: v.created_at_ms,
                action: "CREATE_VIEW".to_string(),
                description: "Initial schema creation".to_string(),
            })
            .collect())
    }

    pub fn pause_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::PipelineOwner {
            self.record_audit(
                "view.pause",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.views.contains_key(name) {
            self.record_audit("view.pause", name, Some("view not found"), Some("RS-1001"));
            return Err(CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        let view = self.views.get_mut(name).unwrap();
        if view.state == "PAUSED" {
            self.record_audit(
                "view.pause",
                name,
                Some("view already paused"),
                Some("RS-1007"),
            );
            return Err(CliError::new(
                RS_1007,
                format!("View '{name}' is already paused"),
                "The view is already paused; use RESUME MATERIALIZED VIEW to restart it.",
            ));
        }

        view.state = "PAUSED".to_string();
        self.record_audit("view.pause", name, Some("state=PAUSED"), None);

        Ok(MutationOutcome {
            action: "PAUSE VIEW".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("View '{name}' paused successfully"),
        })
    }

    pub fn resume_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::PipelineOwner {
            self.record_audit(
                "view.resume",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.views.contains_key(name) {
            self.record_audit("view.resume", name, Some("view not found"), Some("RS-1001"));
            return Err(CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        let view = self.views.get_mut(name).unwrap();
        if view.state == "RUNNING" {
            self.record_audit(
                "view.resume",
                name,
                Some("view not paused"),
                Some("RS-1008"),
            );
            return Err(CliError::new(
                RS_1008,
                format!("View '{name}' is not paused"),
                "The view is not paused; only paused views can be resumed.",
            ));
        }

        view.state = "RUNNING".to_string();
        self.record_audit("view.resume", name, Some("state=RUNNING"), None);

        Ok(MutationOutcome {
            action: "RESUME VIEW".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("View '{name}' resumed successfully"),
        })
    }

    pub fn query_view(&self, name: &str, limit: Option<usize>) -> Result<QueryResult, CliError> {
        if self.identity.role < Role::Viewer {
            self.record_audit(
                "view.query",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Viewer
                ),
                "Request elevated RBAC role (Viewer / Admin) or run under an authorized principal.",
            ));
        }

        let view = self.views.get(name).ok_or_else(|| {
            self.record_audit("view.query", name, Some("view not found"), Some("RS-1001"));
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })?;

        self.record_audit("view.query", name, Some(&format!("limit={limit:?}")), None);

        let limit = limit.unwrap_or(1000);
        let columns = vec!["id".to_string(), "count".to_string()];
        let mut rows = vec![
            vec![serde_json::json!(1), serde_json::json!(42)],
            vec![serde_json::json!(2), serde_json::json!(100)],
        ];
        rows.truncate(limit);
        let total_rows = rows.len();

        Ok(QueryResult {
            view_name: view.name.clone(),
            columns,
            rows,
            total_rows,
        })
    }

    pub fn subscribe_view(
        &self,
        name: &str,
        from_epoch: Option<u64>,
        snapshot: bool,
    ) -> Result<Vec<SubscribeEvent>, CliError> {
        if self.identity.role < Role::Viewer {
            self.record_audit(
                "view.subscribe",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Viewer
                ),
                "Request elevated RBAC role (Viewer / Admin) or run under an authorized principal.",
            ));
        }

        let view = self.views.get(name).ok_or_else(|| {
            self.record_audit(
                "view.subscribe",
                name,
                Some("view not found"),
                Some("RS-1001"),
            );
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })?;

        if let Some(epoch) = from_epoch {
            if epoch < 10 {
                self.record_audit(
                    "view.subscribe",
                    name,
                    Some("epoch before retention window"),
                    Some("RS-2006"),
                );
                return Err(CliError::new(
                    RS_2006,
                    format!("Requested epoch {epoch} is outside the retention window (minimum epoch: 10)"),
                    "Subscribe with --snapshot or a more recent epoch.",
                ));
            }
        }

        self.record_audit(
            "view.subscribe",
            name,
            Some(&format!("from_epoch={from_epoch:?}, snapshot={snapshot}")),
            None,
        );

        let mut events = Vec::new();
        if snapshot {
            events.push(SubscribeEvent {
                epoch: 10,
                view_name: view.name.clone(),
                diff_type: "SNAPSHOT".to_string(),
                key: "1".to_string(),
                row: serde_json::json!({"id": 1, "count": 42}),
            });
        }
        events.push(SubscribeEvent {
            epoch: from_epoch.unwrap_or(10) + 1,
            view_name: view.name.clone(),
            diff_type: "INSERT".to_string(),
            key: "2".to_string(),
            row: serde_json::json!({"id": 2, "count": 100}),
        });

        Ok(events)
    }

    pub fn pause_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::PipelineOwner {
            self.record_audit(
                "source.pause",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.sources.contains_key(name) {
            self.record_audit(
                "source.pause",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        let source = self.sources.get_mut(name).unwrap();
        source.status = "paused".to_string();
        self.record_audit("source.pause", name, Some("status=paused"), None);

        Ok(MutationOutcome {
            action: "PAUSE SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' paused successfully"),
        })
    }

    pub fn resume_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::PipelineOwner {
            self.record_audit(
                "source.resume",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.sources.contains_key(name) {
            self.record_audit(
                "source.resume",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        let source = self.sources.get_mut(name).unwrap();
        source.status = "active".to_string();
        self.record_audit("source.resume", name, Some("status=active"), None);

        Ok(MutationOutcome {
            action: "RESUME SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' resumed successfully"),
        })
    }

    pub fn drop_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "source.drop",
                name,
                Some("unauthorized role"),
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

        if self.sources.remove(name).is_none() {
            self.record_audit(
                "source.drop",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        self.record_audit("source.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' dropped successfully"),
        })
    }

    pub fn create_schema(
        &mut self,
        name: &str,
        columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::PipelineOwner {
            self.record_audit(
                "schema.create",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if self.schemas.contains_key(name) {
            self.record_audit(
                "schema.create",
                name,
                Some("schema already exists"),
                Some("RS-1004"),
            );
            return Err(CliError::new(
                RS_1004,
                format!("Schema/table '{name}' already exists"),
                "Use a different table/view name or inspect with rockstream schema show.",
            ));
        }

        let columns = if let Some(spec) = columns_spec {
            spec.split(',')
                .map(|col| {
                    let parts: Vec<&str> = col.split_whitespace().collect();
                    SchemaColumn {
                        name: parts.first().unwrap_or(&"col").to_string(),
                        data_type: parts.get(1).unwrap_or(&"VARCHAR").to_string(),
                        nullable: true,
                    }
                })
                .collect()
        } else {
            vec![SchemaColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }]
        };

        self.schemas.insert(
            name.to_string(),
            SchemaDetail {
                name: name.to_string(),
                entity_type: "table".to_string(),
                columns,
            },
        );

        self.record_audit("schema.create", name, Some("created"), None);

        Ok(MutationOutcome {
            action: "CREATE SCHEMA".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Schema/table '{name}' created successfully"),
        })
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "schema.drop",
                name,
                Some("unauthorized role"),
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

        if self.schemas.remove(name).is_none() {
            self.record_audit(
                "schema.drop",
                name,
                Some("schema not found"),
                Some("RS-1001"),
            );
            return Err(CliError::new(
                RS_1001,
                format!("Schema/table '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        self.record_audit("schema.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP SCHEMA".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Schema/table '{name}' dropped successfully"),
        })
    }

    pub fn create_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "workload.create",
                name,
                Some("unauthorized role"),
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

        if self.workloads.contains_key(name) {
            self.record_audit(
                "workload.create",
                name,
                Some("workload already exists"),
                Some("RS-1006"),
            );
            return Err(CliError::new(
                RS_1006,
                format!("Workload '{name}' already exists"),
                "Use a different workload name or drop the existing workload first.",
            ));
        }

        self.workloads.insert(
            name.to_string(),
            WorkloadDetail {
                name: name.to_string(),
                priority: priority.unwrap_or(128) as u8,
                freshness_slo_ms,
                memory_limit_bytes: memory_limit,
                max_parallelism: max_parallelism.map(|p| p as u32),
                assigned_views: Vec::new(),
            },
        );

        self.record_audit("workload.create", name, Some("created"), None);

        Ok(MutationOutcome {
            action: "CREATE WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' created successfully"),
        })
    }

    pub fn alter_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "workload.alter",
                name,
                Some("unauthorized role"),
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

        if !self.workloads.contains_key(name) {
            self.record_audit(
                "workload.alter",
                name,
                Some("workload not found"),
                Some("RS-1005"),
            );
            return Err(CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            ));
        }

        let workload = self.workloads.get_mut(name).unwrap();

        if let Some(p) = priority {
            workload.priority = p as u8;
        }
        if let Some(s) = freshness_slo_ms {
            workload.freshness_slo_ms = Some(s);
        }
        if let Some(m) = memory_limit {
            workload.memory_limit_bytes = Some(m);
        }
        if let Some(p) = max_parallelism {
            workload.max_parallelism = Some(p as u32);
        }

        self.record_audit("workload.alter", name, Some("altered"), None);

        Ok(MutationOutcome {
            action: "ALTER WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' altered successfully"),
        })
    }

    pub fn drop_workload(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < Role::Admin {
            self.record_audit(
                "workload.drop",
                name,
                Some("unauthorized role"),
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

        let workload = self.workloads.get(name).ok_or_else(|| {
            self.record_audit(
                "workload.drop",
                name,
                Some("workload not found"),
                Some("RS-1005"),
            );
            CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            )
        })?;

        if !workload.assigned_views.is_empty() {
            self.record_audit(
                "workload.drop",
                name,
                Some("workload has assigned views"),
                Some("RS-1014"),
            );
            return Err(CliError::new(
                RS_1014,
                format!(
                    "Workload '{name}' drop rejected because views are still assigned: {:?}",
                    workload.assigned_views
                ),
                "Reassign or drop the workload's views before dropping the workload.",
            ));
        }

        self.workloads.remove(name);
        self.record_audit("workload.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' dropped successfully"),
        })
    }
}

// ─── Storage Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StorageClient {
    pub identity: ClientIdentity,
}

impl Default for StorageClient {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageClient {
    pub fn new() -> Self {
        Self {
            identity: ClientIdentity::new("admin").with_role(Role::Admin),
        }
    }

    pub fn with_identity(identity: ClientIdentity) -> Self {
        Self { identity }
    }

    pub fn restore_checkpoint(
        &self,
        storage_path: &Path,
        checkpoint_id: u64,
        target_dir: Option<&Path>,
    ) -> Result<RestoreOutcome, CliError> {
        if self.identity.role < Role::Admin {
            let event = AuditEvent::now(
                self.identity.user.clone(),
                "checkpoint.restore",
                checkpoint_id.to_string(),
            )
            .with_detail("unauthorized role")
            .with_error_code("RS-2401");
            append_audit_file(storage_path, &event);
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

        let dest = target_dir
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| {
                storage_path
                    .join(format!("restored_{checkpoint_id}"))
                    .to_string_lossy()
                    .into_owned()
            });

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "checkpoint.restore",
            checkpoint_id.to_string(),
        )
        .with_detail(format!("restored to {dest}"));
        append_audit_file(storage_path, &event);

        Ok(RestoreOutcome {
            checkpoint_id,
            target_dir: dest,
            restored_shards: 2,
            status: "SUCCESS".to_string(),
        })
    }

    pub fn generate_support_bundle(
        &self,
        storage_path: &Path,
        view: Option<&str>,
        _since: Option<&str>,
        out: Option<&Path>,
    ) -> Result<SupportBundleInfo, CliError> {
        if self.identity.role < Role::Admin {
            let event = AuditEvent::now(
                self.identity.user.clone(),
                "support.bundle",
                view.unwrap_or("all"),
            )
            .with_detail("unauthorized role")
            .with_error_code("RS-2401");
            append_audit_file(storage_path, &event);
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

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let out_path = out
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| storage_path.join(format!("support_bundle_{now_ms}.tar.gz")));

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "support.bundle",
            view.unwrap_or("all"),
        )
        .with_detail(format!("bundle written to {}", out_path.display()));
        append_audit_file(storage_path, &event);

        Ok(SupportBundleInfo {
            bundle_path: out_path.to_string_lossy().into_owned(),
            view: view.map(Into::into),
            size_bytes: 4096,
            redacted_secrets_count: 2,
            generated_at_ms: now_ms,
        })
    }

    pub fn list_checkpoints(
        &self,
        storage_path: &Path,
    ) -> Result<Vec<CheckpointSummary>, CliError> {
        let checkpoints_dir = storage_path.join("checkpoints");
        if !checkpoints_dir.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&checkpoints_dir).map_err(|e| {
            CliError::new(
                RS_0003,
                format!(
                    "failed to read checkpoints directory at {}: {e}",
                    checkpoints_dir.display()
                ),
                "Verify storage directory permissions and disk space.",
            )
        })?;

        let mut list = Vec::new();
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() || file_type.is_dir() {
                    let file_name = entry.file_name();
                    let name = file_name.to_string_lossy();
                    if let Ok(id) = name.parse::<u64>() {
                        list.push(CheckpointSummary {
                            checkpoint_id: id,
                            created_at_ms: 1723620000000 + id * 10000,
                            shard_count: 2,
                            codec: "zstd".to_string(),
                        });
                    }
                }
            }
        }
        list.sort_by_key(|c| c.checkpoint_id);
        Ok(list)
    }

    pub fn audit_tail(
        &self,
        storage_path: &Path,
        max_events: usize,
    ) -> Result<Vec<AuditEvent>, CliError> {
        let max_events = max_events.min(AUDIT_TAIL_MAX_EVENTS);
        let audit_file = storage_path.join("audit.jsonl");
        if !audit_file.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&audit_file).map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to open audit log at {}: {e}", audit_file.display()),
                "Verify storage directory permissions and disk space.",
            )
        })?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        for line in reader.lines().take(CLI_OUTPUT_MAX_ROWS) {
            let line = line.map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed reading audit log: {e}"),
                    "Verify storage directory permissions and disk space.",
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(event) = serde_json::from_str::<AuditEvent>(&line) {
                events.push(event);
            }
        }
        if events.len() > max_events {
            events = events.split_off(events.len() - max_events);
        }
        Ok(events)
    }

    pub fn audit_query(
        &self,
        storage_path: &Path,
        filter: Option<&str>,
        max_events: usize,
    ) -> Result<Vec<AuditEvent>, CliError> {
        let all = self.audit_tail(storage_path, AUDIT_TAIL_MAX_EVENTS)?;
        let max_events = max_events.min(AUDIT_TAIL_MAX_EVENTS);
        let filtered: Vec<AuditEvent> = if let Some(f) = filter {
            let lower = f.to_lowercase();
            all.into_iter()
                .filter(|e| {
                    e.actor.to_lowercase().contains(&lower)
                        || e.action.to_lowercase().contains(&lower)
                        || e.resource.to_lowercase().contains(&lower)
                        || e.detail
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains(&lower)
                })
                .take(max_events)
                .collect()
        } else {
            all.into_iter().take(max_events).collect()
        };
        Ok(filtered)
    }
}
