//! CLI output formatting and models.
//!
//! Provides human-readable text table rendering and structured `--json` output
//! conforming to stable JSON schemas.

use rockstream_types::audit::AuditEvent;
use serde::{Deserialize, Serialize};

/// Maximum rows formatted or printed in a single CLI command output buffer.
pub const CLI_OUTPUT_MAX_ROWS: usize = 1000;

/// Maximum audit events tailed or queried in a single call.
pub const AUDIT_TAIL_MAX_EVENTS: usize = 1000;

/// Output format mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

impl OutputFormat {
    pub fn from_json_flag(json: bool) -> Self {
        if json {
            Self::Json
        } else {
            Self::Text
        }
    }
}

pub trait Formattable {
    fn to_text(&self) -> String;
}

pub fn render_output<T: Serialize + Formattable>(data: &T, format: OutputFormat) -> String {
    match format {
        OutputFormat::Json => serde_json::to_string_pretty(data)
            .unwrap_or_else(|e| format!("{{\"error\": \"json serialization failed: {}\"}}", e)),
        OutputFormat::Text => data.to_text(),
    }
}

// ─── Catalog Models ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewSummary {
    pub name: String,
    pub state: String,
    pub workload: Option<String>,
    pub freshness_slo_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub depends_on: Vec<String>,
}

impl Formattable for Vec<ViewSummary> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No views found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<20} {:<15} {:<15} {:<15} {:<15} {:<20}",
            "NAME", "STATE", "WORKLOAD", "SLO (MS)", "MEM LIMIT", "DEPENDS ON"
        ));
        lines.push("-".repeat(105));
        for v in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<20} {:<15} {:<15} {:<15} {:<15} {:<20}",
                v.name,
                v.state,
                v.workload.as_deref().unwrap_or("-"),
                v.freshness_slo_ms
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                v.memory_limit_bytes
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                if v.depends_on.is_empty() {
                    "-".to_string()
                } else {
                    v.depends_on.join(",")
                }
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewDetail {
    pub name: String,
    pub state: String,
    pub workload: Option<String>,
    pub freshness_slo_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub depends_on: Vec<String>,
    pub query: String,
    pub created_at_ms: u64,
}

impl Formattable for ViewDetail {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("View: {}", self.name));
        lines.push(format!("State: {}", self.state));
        lines.push(format!(
            "Workload: {}",
            self.workload.as_deref().unwrap_or("-")
        ));
        lines.push(format!(
            "Freshness SLO: {} ms",
            self.freshness_slo_ms
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push(format!(
            "Memory Limit: {} bytes",
            self.memory_limit_bytes
                .map(|m| m.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push(format!(
            "Depends On: {}",
            if self.depends_on.is_empty() {
                "-".to_string()
            } else {
                self.depends_on.join(", ")
            }
        ));
        lines.push(format!("Created At: {} ms", self.created_at_ms));
        lines.push(format!("Query:\n  {}", self.query));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewStatusInfo {
    pub namespace: String,
    pub view_name: String,
    pub state: String,
    pub workload_name: Option<String>,
    pub freshness_slo_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub stage_lag: Option<rockstream_types::metrics::StageLagBreakdown>,
}

impl Formattable for Vec<ViewStatusInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No view status records found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<15} {:<20} {:<15} {:<15} {:<15} {:<15} {:<15} {:<20}",
            "NAMESPACE",
            "VIEW",
            "STATE",
            "WORKLOAD",
            "SLO (MS)",
            "MEM LIMIT",
            "LAG (MS)",
            "DEPENDS ON"
        ));
        lines.push("-".repeat(135));
        for v in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            let lag_str = if let Some(ref lag) = v.stage_lag {
                format!(
                    "{} (src:{} dec:{} cmp:{} aln:{} snk:{} spl:{} stg:{})",
                    lag.total_lag_ms,
                    lag.source_lag_ms,
                    lag.decode_lag_ms,
                    lag.compute_lag_ms,
                    lag.alignment_lag_ms,
                    lag.sink_lag_ms,
                    lag.spill_lag_ms,
                    lag.storage_pressure_ms
                )
            } else {
                "-".to_string()
            };
            lines.push(format!(
                "{:<15} {:<20} {:<15} {:<15} {:<15} {:<15} {:<15} {:<20}",
                v.namespace,
                v.view_name,
                v.state,
                v.workload_name.as_deref().unwrap_or("-"),
                v.freshness_slo_ms
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                v.memory_limit_bytes
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                lag_str,
                if v.depends_on.is_empty() {
                    "-".to_string()
                } else {
                    v.depends_on.join(",")
                }
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceSummary {
    pub name: String,
    pub connector_type: String,
    pub table: String,
    pub status: String,
}

impl Formattable for Vec<SourceSummary> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No sources found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<20} {:<15} {:<20} {:<15}",
            "NAME", "TYPE", "TABLE", "STATUS"
        ));
        lines.push("-".repeat(75));
        for s in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<20} {:<15} {:<20} {:<15}",
                s.name, s.connector_type, s.table, s.status
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceDetail {
    pub name: String,
    pub connector_type: String,
    pub table: String,
    pub status: String,
    pub options: std::collections::BTreeMap<String, String>,
    pub current_offset: Option<String>,
    pub lag_ms: Option<u64>,
}

impl Formattable for SourceDetail {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Source: {}", self.name));
        lines.push(format!("Type: {}", self.connector_type));
        lines.push(format!("Table: {}", self.table));
        lines.push(format!("Status: {}", self.status));
        lines.push(format!(
            "Current Offset: {}",
            self.current_offset.as_deref().unwrap_or("-")
        ));
        lines.push(format!(
            "Lag: {} ms",
            self.lag_ms
                .map(|l| l.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push("Options:".to_string());
        for (k, v) in &self.options {
            lines.push(format!("  {}: {}", k, v));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSummary {
    pub name: String,
    pub entity_type: String,
    pub column_count: usize,
}

impl Formattable for Vec<SchemaSummary> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No schemas/entities found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!("{:<25} {:<15} {:<15}", "NAME", "TYPE", "COLUMNS"));
        lines.push("-".repeat(60));
        for s in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<25} {:<15} {:<15}",
                s.name, s.entity_type, s.column_count
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaDetail {
    pub name: String,
    pub entity_type: String,
    pub columns: Vec<SchemaColumn>,
}

impl Formattable for SchemaDetail {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Schema: {} ({})", self.name, self.entity_type));
        lines.push(format!(
            "{:<20} {:<15} {:<10}",
            "COLUMN", "TYPE", "NULLABLE"
        ));
        lines.push("-".repeat(50));
        for c in &self.columns {
            lines.push(format!(
                "{:<20} {:<15} {:<10}",
                c.name,
                c.data_type,
                if c.nullable { "YES" } else { "NO" }
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadSummary {
    pub name: String,
    pub priority: u8,
    pub freshness_slo_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub max_parallelism: Option<u32>,
    pub assigned_views: usize,
}

impl Formattable for Vec<WorkloadSummary> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No workloads found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<20} {:<10} {:<15} {:<15} {:<15} {:<15}",
            "NAME", "PRIORITY", "SLO (MS)", "MEM LIMIT", "MAX PARALLEL", "VIEWS"
        ));
        lines.push("-".repeat(95));
        for w in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<20} {:<10} {:<15} {:<15} {:<15} {:<15}",
                w.name,
                w.priority,
                w.freshness_slo_ms
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                w.memory_limit_bytes
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                w.max_parallelism
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                w.assigned_views
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadDetail {
    pub name: String,
    pub priority: u8,
    pub freshness_slo_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub max_parallelism: Option<u32>,
    pub assigned_views: Vec<String>,
}

impl Formattable for WorkloadDetail {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Workload: {}", self.name));
        lines.push(format!("Priority: {}", self.priority));
        lines.push(format!(
            "Freshness SLO: {} ms",
            self.freshness_slo_ms
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push(format!(
            "Memory Limit: {} bytes",
            self.memory_limit_bytes
                .map(|m| m.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push(format!(
            "Max Parallelism: {}",
            self.max_parallelism
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        lines.push(format!("Assigned Views ({}):", self.assigned_views.len()));
        if self.assigned_views.is_empty() {
            lines.push("  (none)".to_string());
        } else {
            for v in &self.assigned_views {
                lines.push(format!("  - {}", v));
            }
        }
        lines.join("\n")
    }
}

// ─── Cluster / Worker / Shard / Checkpoint Models ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusInfo {
    pub node_id: Option<u64>,
    pub role: String,
    pub term: u64,
    pub active_workers: usize,
    pub healthy_workers: usize,
    pub leader_id: Option<u64>,
    pub version: String,
}

impl Formattable for ClusterStatusInfo {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push("Cluster Status:".to_string());
        lines.push(format!("  Role: {}", self.role));
        lines.push(format!(
            "  Node ID: {}",
            self.node_id
                .map(|n| n.to_string())
                .unwrap_or_else(|| "none".to_string())
        ));
        lines.push(format!("  Term: {}", self.term));
        lines.push(format!(
            "  Leader ID: {}",
            self.leader_id
                .map(|l| l.to_string())
                .unwrap_or_else(|| "none".to_string())
        ));
        lines.push(format!(
            "  Workers: {} active, {} healthy",
            self.active_workers, self.healthy_workers
        ));
        lines.push(format!("  Version: {}", self.version));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterQuotasInfo {
    pub total_memory_budget_bytes: u64,
    pub used_memory_bytes: u64,
    pub max_parallelism: u32,
    pub active_workloads: usize,
    pub active_views: usize,
}

impl Formattable for ClusterQuotasInfo {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push("Cluster Quotas:".to_string());
        lines.push(format!(
            "  Total Memory Budget: {} bytes",
            self.total_memory_budget_bytes
        ));
        lines.push(format!("  Used Memory: {} bytes", self.used_memory_bytes));
        lines.push(format!("  Max Parallelism: {}", self.max_parallelism));
        lines.push(format!("  Active Workloads: {}", self.active_workloads));
        lines.push(format!("  Active Views: {}", self.active_views));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerStatusInfo {
    pub worker_id: u64,
    pub role: String,
    pub address: String,
    pub capacity_headroom: f64,
    pub host_id: String,
    pub availability_zone: String,
    pub healthy: bool,
    pub lifecycle_state: String,
    pub registered_at_ms: u64,
}

impl Formattable for Vec<WorkerStatusInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No workers found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<10} {:<10} {:<25} {:<10} {:<15} {:<15} {:<10} {:<12}",
            "ID", "ROLE", "ADDRESS", "HEADROOM", "HOST", "AZ", "HEALTHY", "STATE"
        ));
        lines.push("-".repeat(115));
        for w in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<10} {:<10} {:<25} {:<10.2} {:<15} {:<15} {:<10} {:<12}",
                w.worker_id,
                w.role,
                w.address,
                w.capacity_headroom,
                if w.host_id.is_empty() {
                    "-"
                } else {
                    &w.host_id
                },
                if w.availability_zone.is_empty() {
                    "-"
                } else {
                    &w.availability_zone
                },
                if w.healthy { "yes" } else { "no" },
                w.lifecycle_state
            ));
        }
        lines.join("\n")
    }
}

impl Formattable for WorkerStatusInfo {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Worker: {}", self.worker_id));
        lines.push(format!("Role: {}", self.role));
        lines.push(format!("Address: {}", self.address));
        lines.push(format!("Capacity Headroom: {:.2}", self.capacity_headroom));
        lines.push(format!(
            "Host ID: {}",
            if self.host_id.is_empty() {
                "-"
            } else {
                &self.host_id
            }
        ));
        lines.push(format!(
            "Availability Zone: {}",
            if self.availability_zone.is_empty() {
                "-"
            } else {
                &self.availability_zone
            }
        ));
        lines.push(format!(
            "Healthy: {}",
            if self.healthy { "yes" } else { "no" }
        ));
        lines.push(format!("Lifecycle State: {}", self.lifecycle_state));
        lines.push(format!("Registered At: {} ms", self.registered_at_ms));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: u64,
    pub worker_id: Option<u64>,
    pub lease_token: u64,
    pub status: String,
    pub key_range: String,
}

impl Formattable for Vec<ShardInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No shards found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<10} {:<12} {:<15} {:<12} {:<25}",
            "SHARD ID", "WORKER ID", "LEASE TOKEN", "STATUS", "KEY RANGE"
        ));
        lines.push("-".repeat(80));
        for s in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<10} {:<12} {:<15} {:<12} {:<25}",
                s.shard_id,
                s.worker_id
                    .map(|w| w.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                s.lease_token,
                s.status,
                s.key_range
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointSummary {
    pub checkpoint_id: u64,
    pub created_at_ms: u64,
    pub shard_count: usize,
    pub codec: String,
}

impl Formattable for Vec<CheckpointSummary> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No checkpoints found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<15} {:<20} {:<15} {:<10}",
            "CHECKPOINT ID", "CREATED AT (MS)", "SHARDS", "CODEC"
        ));
        lines.push("-".repeat(65));
        for c in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<15} {:<20} {:<15} {:<10}",
                c.checkpoint_id, c.created_at_ms, c.shard_count, c.codec
            ));
        }
        lines.join("\n")
    }
}

// ─── Resource & Schema Evolution Models ─────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceUsageInfo {
    pub name: String,
    pub entity_type: String,
    pub workload: Option<String>,
    pub memory_bytes: u64,
    pub state_bytes: u64,
    pub estimated_cost_per_hour: f64,
}

impl Formattable for Vec<ResourceUsageInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No resource usage records found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<20} {:<12} {:<15} {:<15} {:<15} {:<15}",
            "NAME", "TYPE", "WORKLOAD", "MEMORY (B)", "STATE (B)", "COST/HR ($)"
        ));
        lines.push("-".repeat(95));
        for r in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<20} {:<12} {:<15} {:<15} {:<15} {:<15.4}",
                r.name,
                r.entity_type,
                r.workload.as_deref().unwrap_or("-"),
                r.memory_bytes,
                r.state_bytes,
                r.estimated_cost_per_hour
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterResourceUsageInfo {
    pub total_views: usize,
    pub total_workloads: usize,
    pub total_memory_bytes: u64,
    pub total_state_bytes: u64,
    pub total_estimated_cost_per_hour: f64,
}

impl Formattable for ClusterResourceUsageInfo {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push("Cluster Resource Usage:".to_string());
        lines.push(format!("  Views: {}", self.total_views));
        lines.push(format!("  Workloads: {}", self.total_workloads));
        lines.push(format!("  Total Memory: {} bytes", self.total_memory_bytes));
        lines.push(format!("  Total State: {} bytes", self.total_state_bytes));
        lines.push(format!(
            "  Estimated Cost / Hour: ${:.4}",
            self.total_estimated_cost_per_hour
        ));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaEvolutionStatusInfo {
    pub view_name: String,
    pub current_version: u64,
    pub status: String,
    pub pending_changes: usize,
}

impl Formattable for Vec<SchemaEvolutionStatusInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No schema evolution records found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<25} {:<10} {:<15} {:<15}",
            "VIEW", "VERSION", "STATUS", "PENDING CHANGES"
        ));
        lines.push("-".repeat(70));
        for s in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<25} {:<10} {:<15} {:<15}",
                s.view_name, s.current_version, s.status, s.pending_changes
            ));
        }
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaEvolutionHistoryInfo {
    pub version: u64,
    pub view_name: String,
    pub applied_at_ms: u64,
    pub action: String,
    pub description: String,
}

impl Formattable for Vec<SchemaEvolutionHistoryInfo> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No schema evolution history found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<10} {:<20} {:<20} {:<15} {:<30}",
            "VERSION", "VIEW", "APPLIED AT (MS)", "ACTION", "DESCRIPTION"
        ));
        lines.push("-".repeat(100));
        for h in self.iter().take(CLI_OUTPUT_MAX_ROWS) {
            lines.push(format!(
                "{:<10} {:<20} {:<20} {:<15} {:<30}",
                h.version, h.view_name, h.applied_at_ms, h.action, h.description
            ));
        }
        lines.join("\n")
    }
}

// ─── Audit Log Formatter ───────────────────────────────────────────────────

impl Formattable for Vec<AuditEvent> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No audit events found.".to_string();
        }
        let mut lines = Vec::new();
        lines.push(format!(
            "{:<20} {:<12} {:<25} {:<25} {:<10} {:<25}",
            "TIMESTAMP (MS)", "ACTOR", "ACTION", "RESOURCE", "ERROR", "DETAIL"
        ));
        lines.push("-".repeat(120));
        for e in self.iter().take(AUDIT_TAIL_MAX_EVENTS) {
            lines.push(format!(
                "{:<20} {:<12} {:<25} {:<25} {:<10} {:<25}",
                e.timestamp_ms,
                e.actor,
                e.action,
                e.resource,
                e.error_code.as_deref().unwrap_or("-"),
                e.detail.as_deref().unwrap_or("-")
            ));
        }
        lines.join("\n")
    }
}

// ─── Explain & SQL Output Models ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplainPlanInfo {
    pub view_name: String,
    pub query: String,
    pub plan: String,
}

impl Formattable for ExplainPlanInfo {
    fn to_text(&self) -> String {
        self.plan.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EstimateRowInfo {
    pub operator_kind: String,
    pub predicted_state_bytes: u64,
    pub epoch_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExplainEstimateInfo {
    pub view_name: String,
    pub query: String,
    pub estimates: Vec<EstimateRowInfo>,
    pub formatted_text: String,
}

impl Formattable for ExplainEstimateInfo {
    fn to_text(&self) -> String {
        self.formatted_text.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlCompileInfo {
    pub query: String,
    pub plan: String,
}

impl Formattable for SqlCompileInfo {
    fn to_text(&self) -> String {
        self.plan.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorKindInfo {
    pub op_id: String,
    pub kind: String,
    pub details: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplainOpIdInfo {
    pub view_name: String,
    pub query: String,
    pub operators: Vec<OperatorKindInfo>,
    pub formatted_text: String,
}

impl Formattable for ExplainOpIdInfo {
    fn to_text(&self) -> String {
        self.formatted_text.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArrangementDebugInfo {
    pub view_name: String,
    pub op_id: String,
    pub operator_kind: String,
    pub details: String,
    pub shard: String,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<String>,
    pub user_key: String,
    pub internal_key: String,
    pub state: serde_json::Value,
    pub weight: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delta: Option<String>,
    pub formatted_text: String,
}

impl Formattable for ArrangementDebugInfo {
    fn to_text(&self) -> String {
        self.formatted_text.clone()
    }
}

// ─── Mutating Command Output Models ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationOutcome {
    pub action: String,
    pub resource: String,
    pub status: String,
    pub message: String,
}

impl Formattable for MutationOutcome {
    fn to_text(&self) -> String {
        format!(
            "{}: {} [{}] — {}",
            self.action, self.resource, self.status, self.message
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResult {
    pub view_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_rows: usize,
}

impl Formattable for QueryResult {
    fn to_text(&self) -> String {
        if self.rows.is_empty() {
            return format!("No rows returned for view '{}'.", self.view_name);
        }
        let col_widths: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let col_len = col.len();
                let max_row_len = self
                    .rows
                    .iter()
                    .map(|r| r.get(i).map(|v| v.to_string().len()).unwrap_or(0))
                    .max()
                    .unwrap_or(0);
                col_len.max(max_row_len).max(10)
            })
            .collect();

        let mut lines = Vec::new();
        let header = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<width$}", c, width = col_widths[i]))
            .collect::<Vec<_>>()
            .join(" ");
        let total_w = header.len();
        lines.push(header);
        lines.push("-".repeat(total_w.max(40)));

        for row in self.rows.iter().take(CLI_OUTPUT_MAX_ROWS) {
            let row_str = row
                .iter()
                .enumerate()
                .map(|(i, val)| {
                    let s = match val {
                        serde_json::Value::String(st) => st.clone(),
                        _ => val.to_string(),
                    };
                    format!(
                        "{:<width$}",
                        s,
                        width = col_widths.get(i).copied().unwrap_or(10)
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            lines.push(row_str);
        }
        lines.push(format!("({} rows)", self.total_rows));
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubscribeEvent {
    pub epoch: u64,
    pub view_name: String,
    pub diff_type: String,
    pub key: String,
    pub row: serde_json::Value,
}

impl Formattable for SubscribeEvent {
    fn to_text(&self) -> String {
        format!(
            "[{}] epoch={} view={} key={} row={}",
            self.diff_type, self.epoch, self.view_name, self.key, self.row
        )
    }
}

impl Formattable for Vec<SubscribeEvent> {
    fn to_text(&self) -> String {
        if self.is_empty() {
            return "No stream events received.".to_string();
        }
        self.iter()
            .map(|e| e.to_text())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainOutcome {
    pub worker_id: u64,
    pub status: String,
    pub remaining_shards: usize,
    pub message: String,
}

impl Formattable for DrainOutcome {
    fn to_text(&self) -> String {
        format!(
            "Worker {}: status={} remaining_shards={} ({})",
            self.worker_id, self.status, self.remaining_shards, self.message
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationOutcome {
    pub shard_id: u64,
    pub source_worker: u64,
    pub target_worker: u64,
    pub status: String,
    pub duration_ms: u64,
}

impl Formattable for MigrationOutcome {
    fn to_text(&self) -> String {
        format!(
            "Shard {}: migrated from worker {} to worker {} (status: {}, duration: {}ms)",
            self.shard_id, self.source_worker, self.target_worker, self.status, self.duration_ms
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub checkpoint_id: u64,
    pub target_dir: String,
    pub restored_shards: usize,
    pub status: String,
}

impl Formattable for RestoreOutcome {
    fn to_text(&self) -> String {
        format!(
            "Checkpoint {}: restored to {} (shards: {}, status: {})",
            self.checkpoint_id, self.target_dir, self.restored_shards, self.status
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupportBundleInfo {
    pub bundle_path: String,
    pub view: Option<String>,
    pub size_bytes: u64,
    pub redacted_secrets_count: usize,
    pub generated_at_ms: u64,
}

impl Formattable for SupportBundleInfo {
    fn to_text(&self) -> String {
        format!(
            "Support bundle generated at {}\n  view: {}\n  size: {} bytes\n  redacted secrets: {}\n  timestamp: {}",
            self.bundle_path,
            self.view.as_deref().unwrap_or("all"),
            self.size_bytes,
            self.redacted_secrets_count,
            self.generated_at_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debugger_output_models_and_error_codes() {
        let op_info = OperatorKindInfo {
            op_id: "op-1001".to_string(),
            kind: "Aggregate".to_string(),
            details: "SUM(quantity) GROUP BY product_id".to_string(),
            schema: Some("Int64, Int64".to_string()),
        };
        let explain_info = ExplainOpIdInfo {
            view_name: "orders_mv".to_string(),
            query: "SELECT product_id, SUM(quantity) FROM orders GROUP BY product_id".to_string(),
            operators: vec![op_info],
            formatted_text:
                "VIEW orders_mv\n  op-1001: Aggregate (SUM(quantity) GROUP BY product_id)"
                    .to_string(),
        };
        let rendered_text = render_output(&explain_info, OutputFormat::Text);
        assert!(rendered_text.contains("op-1001"));
        let rendered_json = render_output(&explain_info, OutputFormat::Json);
        assert!(rendered_json.contains("\"op-1001\""));

        let debug_info = ArrangementDebugInfo {
            view_name: "orders_mv".to_string(),
            op_id: "agg_op_3f2a".to_string(),
            operator_kind: "Aggregate".to_string(),
            details: "SUM(quantity) GROUP BY product_id".to_string(),
            shard: "shard-07 (s3://bucket/shards/07/)".to_string(),
            epoch: 1492,
            committed_at: Some("2026-05-28T10:14:23Z".to_string()),
            user_key: "product_id=42".to_string(),
            internal_key: "0100000000000003f2a...".to_string(),
            state: serde_json::json!({"sum_quantity": 1840, "row_count": 23}),
            weight: 1,
            last_delta: Some("epoch 1489  (+120 quantity, +3 rows)".to_string()),
            formatted_text: "op_id: agg_op_3f2a\nkey: product_id=42\nweight: +1".to_string(),
        };
        let debug_text = render_output(&debug_info, OutputFormat::Text);
        assert!(debug_text.contains("product_id=42"));
        let debug_json = render_output(&debug_info, OutputFormat::Json);
        assert!(debug_json.contains("\"agg_op_3f2a\""));
        assert!(debug_json.contains("\"weight\": 1"));
    }
}
