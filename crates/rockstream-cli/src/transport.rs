//! Identity-pluggable client transport substrate for RockStream CLI.
//!
//! Encapsulates authentication credentials and transport mechanisms for communicating
//! with the control plane, catalog, and storage layers.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::{RS_0003, RS_0004, RS_1001, RS_1005, RS_4009};

use crate::output::{
    CheckpointSummary, ClusterQuotasInfo, ClusterResourceUsageInfo, ClusterStatusInfo,
    ResourceUsageInfo, SchemaColumn, SchemaDetail, SchemaEvolutionHistoryInfo,
    SchemaEvolutionStatusInfo, SchemaSummary, ShardInfo, SourceDetail, SourceSummary, ViewDetail,
    ViewStatusInfo, ViewSummary, WorkerStatusInfo, WorkloadDetail, WorkloadSummary,
    AUDIT_TAIL_MAX_EVENTS, CLI_OUTPUT_MAX_ROWS,
};
use crate::CliError;

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
}

impl Default for ClientIdentity {
    fn default() -> Self {
        Self {
            user: "rockstream".to_string(),
            token: None,
            client_cert_path: None,
            namespace: "public".to_string(),
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
}

impl ControlClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        Self {
            control_addr,
            identity,
            mock_workers: None,
            mock_shards: None,
            mock_quotas: None,
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
}

// ─── Catalog Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CatalogClient {
    pub identity: ClientIdentity,
    pub views: BTreeMap<String, ViewDetail>,
    pub sources: BTreeMap<String, SourceDetail>,
    pub schemas: BTreeMap<String, SchemaDetail>,
    pub workloads: BTreeMap<String, WorkloadDetail>,
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
}

// ─── Storage Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct StorageClient;

impl StorageClient {
    pub fn new() -> Self {
        Self
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
