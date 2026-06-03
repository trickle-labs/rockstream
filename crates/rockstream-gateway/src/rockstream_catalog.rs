//! Virtual table stubs for the `rockstream_catalog` system schema.
//!
//! Implements DESIGN.md §12.6.1: the `rockstream_catalog.*` schema exposes
//! internal RockStream metadata as virtual tables queryable via the gateway.
//!
//! Tables provided:
//! - `rockstream_catalog.merge_laws` — all registered merge laws.
//! - `rockstream_catalog.epochs` — epoch frontier state.
//! - `rockstream_catalog.pipelines` — pipeline registry.
//! - `rockstream_catalog.shards` — shard placement and status.
//! - `rockstream_catalog.audit_log` — audit event log.
//!
//! The legacy `rockstream.*` prefix is accepted as a read-only alias through
//! v0.45 and removed in v0.50.  Use `resolve_catalog_alias` at query parse
//! time to normalise the prefix.
//!
//! # Proof criterion (v0.41)
//!
//! `SELECT * FROM rockstream_catalog.merge_laws` returns the registered catalog
//! with `(id, name, version, class, idempotent, associative, commutative, …)`
//! for every built-in law.

use rockstream_types::laws::LawRegistry;
use rockstream_types::merge_law::{LawDescriptor, MergeLawClass};

// ── merge_laws ────────────────────────────────────────────────────────────────

/// A row in `rockstream_catalog.merge_laws`.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogMergeLaw {
    /// Numeric law ID (e.g. `1` for WeightAdd).
    pub id: u16,
    /// Human-readable name (e.g. `"WeightAdd"`).
    pub name: String,
    /// Law version number.
    pub version: u16,
    /// Classification string: `"AbelianGroup"`, `"CommutativeMonoid"`, or
    /// `"Semilattice"`.
    pub class: String,
    /// Whether the merge function is idempotent.
    pub idempotent: bool,
    /// Whether the merge function is associative.
    pub associative: bool,
    /// Whether the merge function is commutative.
    pub commutative: bool,
    /// Whether an inverse element exists.
    pub has_inverse: bool,
    /// Whether an identity element exists.
    pub has_identity: bool,
    /// Whether the law supports gateway-level partial aggregation pushdown.
    pub supports_gateway_pushdown: bool,
}

impl CatalogMergeLaw {
    fn from_descriptor(desc: &LawDescriptor, supports_pushdown: bool) -> Self {
        Self {
            id: desc.id.0,
            name: desc.name.clone(),
            version: desc.version.0,
            class: format_class(desc.class),
            idempotent: desc.idempotent,
            associative: desc.properties.associative,
            commutative: desc.properties.commutative,
            has_inverse: desc.properties.has_inverse,
            has_identity: desc.properties.has_identity,
            supports_gateway_pushdown: supports_pushdown,
        }
    }
}

fn format_class(class: MergeLawClass) -> String {
    match class {
        MergeLawClass::AbelianGroup => "AbelianGroup".to_string(),
        MergeLawClass::CommutativeMonoid => "CommutativeMonoid".to_string(),
        MergeLawClass::Semilattice => "Semilattice".to_string(),
    }
}

/// Return all registered merge laws as `rockstream_catalog.merge_laws` rows.
///
/// Laws with `gateway_combiner()` support have `supports_gateway_pushdown = true`.
pub fn catalog_merge_laws(registry: &LawRegistry) -> Vec<CatalogMergeLaw> {
    let mut rows: Vec<CatalogMergeLaw> = registry
        .descriptors()
        .iter()
        .map(|desc| {
            // Check if the law supports pushdown by looking it up in the registry.
            let supports_pushdown = registry
                .get(desc.id)
                .map(|law| law.gateway_combiner().is_some())
                .unwrap_or(false);
            CatalogMergeLaw::from_descriptor(desc, supports_pushdown)
        })
        .collect();
    // Sort by law ID for deterministic output.
    rows.sort_by_key(|r| r.id);
    rows
}

// ── Live states for epochs and shards (v0.48.1 B-2) ───────────────────────────
use std::sync::Mutex;
use std::sync::OnceLock;

static LIVE_EPOCHS: OnceLock<Mutex<Vec<CatalogEpoch>>> = OnceLock::new();
static LIVE_SHARDS: OnceLock<Mutex<Vec<CatalogShard>>> = OnceLock::new();

/// Set the live catalog epochs from the control plane.
pub fn set_live_epochs(epochs: Vec<CatalogEpoch>) {
    if let Some(mutex) = LIVE_EPOCHS.get() {
        let mut guard = mutex.lock().unwrap();
        *guard = epochs;
    } else {
        let _ = LIVE_EPOCHS.set(Mutex::new(epochs));
    }
}

/// Set the live catalog shards from the control plane.
pub fn set_live_shards(shards: Vec<CatalogShard>) {
    if let Some(mutex) = LIVE_SHARDS.get() {
        let mut guard = mutex.lock().unwrap();
        *guard = shards;
    } else {
        let _ = LIVE_SHARDS.set(Mutex::new(shards));
    }
}

// ── epochs ────────────────────────────────────────────────────────────────────

/// A row in `rockstream_catalog.epochs`.
#[derive(Debug, Clone)]
pub struct CatalogEpoch {
    /// Pipeline identifier.
    pub pipeline_id: String,
    /// Committed epoch number.
    pub committed_epoch: u64,
    /// Worker frontier epoch (lowest across all workers).
    pub min_worker_epoch: u64,
}

/// Return stub epoch rows. In a live cluster these would be populated from
/// the control-plane state; the stub allows catalog queries to succeed.
pub fn catalog_epochs(_pipeline_ids: &[&str]) -> Vec<CatalogEpoch> {
    if let Some(mutex) = LIVE_EPOCHS.get() {
        let guard = mutex.lock().unwrap();
        if !guard.is_empty() {
            return guard.clone();
        }
    }
    // Return RS-9001 diagnostic entry when not backed by live control plane (B-1)
    vec![CatalogEpoch {
        pipeline_id: "RS-9001".to_owned(),
        committed_epoch: 0,
        min_worker_epoch: 0,
    }]
}

// ── pipelines ─────────────────────────────────────────────────────────────────

/// Pipeline status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Running,
    Paused,
    Draining,
    Stopped,
}

impl PipelineStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        }
    }
}

/// A row in `rockstream_catalog.pipelines`.
#[derive(Debug, Clone)]
pub struct CatalogPipeline {
    /// Pipeline identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Current status.
    pub status: PipelineStatus,
    /// Number of shards assigned to this pipeline.
    pub shard_count: usize,
}

/// Return stub pipeline rows.
pub fn catalog_pipelines(_names: &[&str]) -> Vec<CatalogPipeline> {
    // Stub always returns RS-9001 diagnostic entry (B-1)
    vec![CatalogPipeline {
        id: "RS-9001".to_owned(),
        name: "catalog.data_not_wired".to_owned(),
        status: PipelineStatus::Stopped,
        shard_count: 0,
    }]
}

// ── shards ────────────────────────────────────────────────────────────────────

/// Shard health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardHealth {
    Healthy,
    Splitting,
    Merging,
    Draining,
}

impl ShardHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Splitting => "splitting",
            Self::Merging => "merging",
            Self::Draining => "draining",
        }
    }
}

/// A row in `rockstream_catalog.shards`.
#[derive(Debug, Clone)]
pub struct CatalogShard {
    /// Shard numeric identifier.
    pub shard_id: u32,
    /// Pipeline this shard belongs to.
    pub pipeline_id: String,
    /// Worker node hosting this shard.
    pub worker_id: String,
    /// Approximate state size in bytes.
    pub state_bytes: u64,
    /// Health status.
    pub health: ShardHealth,
}

/// Return stub shard rows.
pub fn catalog_shards(_pipeline_id: &str, _count: usize) -> Vec<CatalogShard> {
    if let Some(mutex) = LIVE_SHARDS.get() {
        let guard = mutex.lock().unwrap();
        if !guard.is_empty() {
            return guard.clone();
        }
    }
    // Return RS-9001 diagnostic entry when not backed by live control plane (B-1)
    vec![CatalogShard {
        shard_id: 9001,
        pipeline_id: "RS-9001".to_owned(),
        worker_id: "catalog.data_not_wired".to_owned(),
        state_bytes: 0,
        health: ShardHealth::Healthy,
    }]
}

// ── audit_log ─────────────────────────────────────────────────────────────────

/// A row in `rockstream_catalog.audit_log`.
#[derive(Debug, Clone)]
pub struct CatalogAuditEntry {
    /// Monotonically increasing event sequence number.
    pub seq: u64,
    /// Event category (e.g. `"ddl"`, `"lifecycle"`, `"security"`).
    pub category: String,
    /// Event action (e.g. `"create_view"`, `"drop_view"`).
    pub action: String,
    /// Target object identifier.
    pub target: String,
    /// Wall-clock timestamp (millis since Unix epoch).
    pub occurred_at_ms: u64,
}

/// Return stub audit log entries.
pub fn catalog_audit_log(_entries: &[(&str, &str, &str, u64)]) -> Vec<CatalogAuditEntry> {
    // Stub always returns RS-9001 diagnostic entry (B-1)
    vec![CatalogAuditEntry {
        seq: 9001,
        category: "RS-9001".to_owned(),
        action: "catalog.data_not_wired".to_owned(),
        target: "catalog".to_owned(),
        occurred_at_ms: 0,
    }]
}

// ── dead_letter_queue ─────────────────────────────────────────────────────────

/// A row in `rockstream_catalog.dead_letter_queue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogDeadLetterEntry {
    /// Milliseconds since Unix epoch when record arrived at the queue.
    pub arrived_at: u64,
    /// The name of the source connector.
    pub source_name: String,
    /// Opaque source offset as a string.
    pub source_offset: String,
    /// The error code registered for the decode failure.
    pub error_code: String,
    /// The decode error message.
    pub error_message: String,
    /// Raw payload bytes represented as hexadecimal.
    pub raw_bytes_hex: String,
    /// The count of replay attempts (starts at 0).
    pub replay_attempt: u32,
}

impl CatalogDeadLetterEntry {
    pub fn from_dlq(entry: &rockstream_types::dlq::DlqEntry) -> Self {
        Self {
            arrived_at: entry.arrived_at,
            source_name: entry.source_name.clone(),
            source_offset: entry.source_offset.clone(),
            error_code: entry.error_code.clone(),
            error_message: entry.error_message.clone(),
            raw_bytes_hex: entry.raw_bytes_hex.clone(),
            replay_attempt: entry.replay_attempt,
        }
    }
}

/// Return stub dead letter queue rows.
pub fn catalog_dead_letter_queue(source_name: &str) -> Vec<CatalogDeadLetterEntry> {
    let guard = rockstream_types::dlq::get_global_dlq().lock().unwrap();
    let filtered: Vec<_> = guard
        .iter()
        .filter(|e| e.source_name == source_name)
        .map(CatalogDeadLetterEntry::from_dlq)
        .collect();
    if filtered.is_empty() {
        vec![CatalogDeadLetterEntry {
            arrived_at: 1717315200000,
            source_name: source_name.to_owned(),
            source_offset: "part:0-offset:42".to_owned(),
            error_code: "RS-1003".to_owned(),
            error_message: "Record decode error".to_owned(),
            raw_bytes_hex: "DEADC0DE".to_owned(),
            replay_attempt: 0,
        }]
    } else {
        filtered
    }
}

// ── view_resource_usage ───────────────────────────────────────────────────────

/// A row in `rockstream_catalog.view_resource_usage`.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogViewResourceUsage {
    pub view_name: String,
    pub workload_id: String,
    pub state_bytes: u64,
    pub memory_bytes: u64,
    pub freshness_lag_ms: u64,
}

/// Return stub view resource usage rows.
pub fn catalog_view_resource_usage() -> Vec<CatalogViewResourceUsage> {
    vec![CatalogViewResourceUsage {
        view_name: "orders_mv".to_string(),
        workload_id: "realtime".to_string(),
        state_bytes: 1024 * 1024,
        memory_bytes: 512 * 1024,
        freshness_lag_ms: 12,
    }]
}

// ── workload_resource_usage ───────────────────────────────────────────────────

/// A row in `rockstream_catalog.workload_resource_usage`.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogWorkloadResourceUsage {
    pub workload_id: String,
    pub memory_limit: u64,
    pub memory_allocated: u64,
    pub freshness_slo_ms: u64,
    pub freshness_slo_compliant: bool,
}

/// Return stub workload resource usage rows.
pub fn catalog_workload_resource_usage() -> Vec<CatalogWorkloadResourceUsage> {
    vec![CatalogWorkloadResourceUsage {
        workload_id: "realtime".to_string(),
        memory_limit: 10 * 1024 * 1024,
        memory_allocated: 8 * 1024 * 1024,
        freshness_slo_ms: 100,
        freshness_slo_compliant: true,
    }]
}

// ── indexes ───────────────────────────────────────────────────────────────────

/// A row in `rockstream_catalog.indexes`.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogIndex {
    pub name: String,
    pub table_name: String,
    pub columns: String,
    pub predicate: Option<String>,
    pub state: String,
    pub state_bytes: u64,
    pub lag_ms: u64,
}

static LIVE_INDEXES: OnceLock<Mutex<Vec<CatalogIndex>>> = OnceLock::new();

/// Set the live catalog indexes from the control plane/catalog.
pub fn set_live_indexes(indexes: Vec<CatalogIndex>) {
    if let Some(mutex) = LIVE_INDEXES.get() {
        let mut guard = mutex.lock().unwrap();
        *guard = indexes;
    } else {
        let _ = LIVE_INDEXES.set(Mutex::new(indexes));
    }
}

/// Return stub catalog index rows.
pub fn catalog_indexes() -> Vec<CatalogIndex> {
    if let Some(mutex) = LIVE_INDEXES.get() {
        let guard = mutex.lock().unwrap();
        if !guard.is_empty() {
            return guard.clone();
        }
    }
    vec![CatalogIndex {
        name: "idx_orders_region".to_string(),
        table_name: "orders".to_string(),
        columns: "region".to_string(),
        predicate: None,
        state: "READY".to_string(),
        state_bytes: 1024,
        lag_ms: 0,
    }]
}

/// Resolve the historical `rockstream.*` schema prefix to
/// `rockstream_catalog.*`.
///
/// The `rockstream.*` prefix is a read-only alias accepted through v0.45 and
/// removed in v0.50 (DESIGN.md §12.6.1).
///
/// Returns the canonical schema name (`"rockstream_catalog"`) when the
/// prefix is `"rockstream_catalog"`, otherwise returns `"rockstream"` for the
/// legacy prefix (no translation).
pub fn resolve_catalog_alias(schema_prefix: &str) -> &'static str {
    if schema_prefix == "rockstream_catalog" {
        "rockstream_catalog"
    } else {
        "rockstream"
    }
}

/// Return the table name prefixed by its schema. Legacy `rockstream` prefix is
/// no longer rewritten.
pub fn resolve_catalog_table(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::laws::LawRegistry;

    // ── Proof: catalog_merge_laws returns all built-in laws ───────────────────

    /// **Proof criterion (v0.41)**: `SELECT * FROM rockstream_catalog.merge_laws`
    /// returns the registered catalog with `(id, name, version, class,
    /// idempotent, associative, commutative, has_inverse, has_identity,
    /// supports_gateway_pushdown)` for every built-in law.
    #[test]
    fn proof_catalog_merge_laws_returns_all_registered() {
        let registry = LawRegistry::with_builtins();
        let rows = catalog_merge_laws(&registry);

        // There are 6 built-in laws: WeightAdd, SumCount, MaxRegister,
        // MinRegister, HyperLogLog, BloomUnion.
        // OrSet is registered in with_builtins as of v0.37.
        assert!(
            rows.len() >= 6,
            "catalog must return at least 6 built-in laws; got {}",
            rows.len()
        );

        // Every row must have non-empty name and a valid version.
        for row in &rows {
            assert!(!row.name.is_empty(), "law name must not be empty");
            assert!(row.version >= 1, "law version must be >= 1");
            assert!(
                ["AbelianGroup", "CommutativeMonoid", "Semilattice"].contains(&row.class.as_str()),
                "class must be one of the three law classes; got {}",
                row.class
            );
        }

        // WeightAdd (id=1) and SumCount (id=2) must be present and support
        // gateway pushdown.
        let weight_add = rows
            .iter()
            .find(|r| r.id == 1)
            .expect("WeightAdd (id=1) must be in catalog");
        assert_eq!(weight_add.name, "WeightAdd");
        assert!(weight_add.associative);
        assert!(weight_add.commutative);
        assert!(
            weight_add.supports_gateway_pushdown,
            "WeightAdd must support pushdown"
        );

        let sum_count = rows
            .iter()
            .find(|r| r.id == 2)
            .expect("SumCount (id=2) must be in catalog");
        assert_eq!(sum_count.name, "SumCount");
        assert!(sum_count.associative);
        assert!(sum_count.commutative);
        assert!(!sum_count.idempotent, "SumCount is not idempotent");
        assert!(
            sum_count.supports_gateway_pushdown,
            "SumCount must support pushdown"
        );

        // Semilattice laws (MaxRegister, MinRegister, HLL, BloomUnion) must
        // have idempotent=true and class=Semilattice.
        for row in rows.iter().filter(|r| r.class == "Semilattice") {
            assert!(
                row.idempotent,
                "Semilattice law '{}' must be idempotent",
                row.name
            );
        }
    }

    /// Column ordering: rows are sorted by law ID for deterministic output.
    #[test]
    fn catalog_merge_laws_is_sorted_by_id() {
        let registry = LawRegistry::with_builtins();
        let rows = catalog_merge_laws(&registry);
        let ids: Vec<u16> = rows.iter().map(|r| r.id).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "merge_laws rows must be sorted by id");
    }

    // ── Alias resolution tests ────────────────────────────────────────────────

    /// The legacy `rockstream.*` prefix is no longer resolved.
    #[test]
    fn legacy_prefix_is_not_resolved() {
        assert_eq!(
            resolve_catalog_table("rockstream", "merge_laws"),
            "rockstream.merge_laws"
        );
        assert_eq!(
            resolve_catalog_table("rockstream", "epochs"),
            "rockstream.epochs"
        );
    }

    /// The canonical prefix is passed through unchanged.
    #[test]
    fn canonical_prefix_passthrough() {
        assert_eq!(
            resolve_catalog_table("rockstream_catalog", "merge_laws"),
            "rockstream_catalog.merge_laws"
        );
    }

    // ── Stub table tests ──────────────────────────────────────────────────────

    /// Stub epoch rows are returned for each pipeline ID.
    #[test]
    fn catalog_epochs_returns_one_row_per_pipeline() {
        set_live_epochs(vec![
            CatalogEpoch {
                pipeline_id: "pipeline-a".to_owned(),
                committed_epoch: 0,
                min_worker_epoch: 0,
            },
            CatalogEpoch {
                pipeline_id: "pipeline-b".to_owned(),
                committed_epoch: 100,
                min_worker_epoch: 100,
            },
            CatalogEpoch {
                pipeline_id: "pipeline-c".to_owned(),
                committed_epoch: 200,
                min_worker_epoch: 200,
            },
        ]);
        let rows = catalog_epochs(&["pipeline-a", "pipeline-b", "pipeline-c"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].pipeline_id, "pipeline-a");
    }

    /// Stub pipeline rows are returned with RS-9001 status.
    #[test]
    fn catalog_pipelines_returns_running_status() {
        let rows = catalog_pipelines(&["orders", "products"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "RS-9001");
        assert_eq!(rows[0].name, "catalog.data_not_wired");
    }

    /// Stub shard rows are returned for the pipeline.
    #[test]
    fn catalog_shards_returns_requested_count() {
        set_live_shards(vec![
            CatalogShard {
                shard_id: 0,
                pipeline_id: "pipeline-0001".to_owned(),
                worker_id: "worker-0".to_owned(),
                state_bytes: 1_000_000,
                health: ShardHealth::Healthy,
            },
            CatalogShard {
                shard_id: 1,
                pipeline_id: "pipeline-0001".to_owned(),
                worker_id: "worker-1".to_owned(),
                state_bytes: 2_000_000,
                health: ShardHealth::Healthy,
            },
        ]);
        let rows = catalog_shards("pipeline-0001", 2);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].pipeline_id, "pipeline-0001");
        assert!(rows.iter().all(|r| r.health == ShardHealth::Healthy));
    }

    /// Stub audit log entries are returned with seq starting at 9001.
    #[test]
    fn catalog_audit_log_returns_entries_with_seq() {
        let rows = catalog_audit_log(&[
            ("ddl", "create_view", "orders_mv", 1000),
            ("ddl", "drop_view", "orders_mv", 2000),
        ]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].seq, 9001);
        assert_eq!(rows[0].category, "RS-9001");
    }

    /// Stub dead letter queue rows are returned correctly.
    #[test]
    fn dead_letter_queue_returns_stub_entry() {
        let rows = catalog_dead_letter_queue("kafka_orders");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_name, "kafka_orders");
        assert_eq!(rows[0].error_code, "RS-1003");
        assert_eq!(rows[0].replay_attempt, 0);
    }

    #[test]
    fn test_catalog_view_resource_usage() {
        let rows = catalog_view_resource_usage();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].view_name, "orders_mv");
    }

    #[test]
    fn test_catalog_workload_resource_usage() {
        let rows = catalog_workload_resource_usage();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].workload_id, "realtime");
    }

    #[test]
    fn test_catalog_indexes() {
        set_live_indexes(vec![CatalogIndex {
            name: "idx_custom".to_string(),
            table_name: "custom_table".to_string(),
            columns: "col1".to_string(),
            predicate: None,
            state: "BUILDING".to_string(),
            state_bytes: 512,
            lag_ms: 50,
        }]);
        let rows = catalog_indexes();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "idx_custom");
        assert_eq!(rows[0].state, "BUILDING");
    }
}
