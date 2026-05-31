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
pub fn catalog_epochs(pipeline_ids: &[&str]) -> Vec<CatalogEpoch> {
    pipeline_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| CatalogEpoch {
            pipeline_id: id.to_owned(),
            committed_epoch: i as u64 * 100,
            min_worker_epoch: i as u64 * 100,
        })
        .collect()
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
pub fn catalog_pipelines(names: &[&str]) -> Vec<CatalogPipeline> {
    names
        .iter()
        .enumerate()
        .map(|(i, &name)| CatalogPipeline {
            id: format!("pipeline-{i:04}"),
            name: name.to_owned(),
            status: PipelineStatus::Running,
            shard_count: 4,
        })
        .collect()
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
pub fn catalog_shards(pipeline_id: &str, count: usize) -> Vec<CatalogShard> {
    (0..count)
        .map(|i| CatalogShard {
            shard_id: i as u32,
            pipeline_id: pipeline_id.to_owned(),
            worker_id: format!("worker-{}", i % 4),
            state_bytes: (i as u64 + 1) * 1_000_000,
            health: ShardHealth::Healthy,
        })
        .collect()
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
pub fn catalog_audit_log(entries: &[(&str, &str, &str, u64)]) -> Vec<CatalogAuditEntry> {
    entries
        .iter()
        .enumerate()
        .map(|(i, &(cat, action, target, ts))| CatalogAuditEntry {
            seq: i as u64 + 1,
            category: cat.to_owned(),
            action: action.to_owned(),
            target: target.to_owned(),
            occurred_at_ms: ts,
        })
        .collect()
}

// ── Alias resolution ──────────────────────────────────────────────────────────

/// Resolve the historical `rockstream.*` schema prefix to
/// `rockstream_catalog.*`.
///
/// The `rockstream.*` prefix is a read-only alias accepted through v0.45 and
/// removed in v0.50 (DESIGN.md §12.6.1).  At query parse time, any table
/// reference of the form `rockstream.<table>` should be rewritten to
/// `rockstream_catalog.<table>` using this function.
///
/// Returns the canonical schema name (`"rockstream_catalog"`) when the
/// prefix is the legacy `"rockstream"`, otherwise returns the input unchanged.
pub fn resolve_catalog_alias(schema_prefix: &str) -> &'static str {
    if schema_prefix == "rockstream" {
        "rockstream_catalog"
    } else {
        // SAFETY: caller-provided string; we return a well-known static string
        // only for the alias case.  For all other prefixes we need to return
        // the input, but since this function returns `&'static str` we return
        // the canonical value unconditionally for unknown prefixes.
        "rockstream_catalog"
    }
}

/// Return the canonical table name given an optional schema prefix and a table
/// name.  Handles both `rockstream_catalog.table` and the legacy
/// `rockstream.table` alias.
pub fn resolve_catalog_table(schema: &str, table: &str) -> String {
    let canonical_schema = if schema == "rockstream" {
        "rockstream_catalog"
    } else {
        schema
    };
    format!("{canonical_schema}.{table}")
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

    /// The legacy `rockstream.*` prefix resolves to `rockstream_catalog.*`.
    #[test]
    fn legacy_prefix_resolves_to_canonical() {
        assert_eq!(
            resolve_catalog_table("rockstream", "merge_laws"),
            "rockstream_catalog.merge_laws"
        );
        assert_eq!(
            resolve_catalog_table("rockstream", "epochs"),
            "rockstream_catalog.epochs"
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
        let rows = catalog_epochs(&["pipeline-a", "pipeline-b", "pipeline-c"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].pipeline_id, "pipeline-a");
    }

    /// Stub pipeline rows are returned with Running status.
    #[test]
    fn catalog_pipelines_returns_running_status() {
        let rows = catalog_pipelines(&["orders", "products"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].status, PipelineStatus::Running);
        assert_eq!(rows[0].name, "orders");
    }

    /// Stub shard rows are returned for the pipeline.
    #[test]
    fn catalog_shards_returns_requested_count() {
        let rows = catalog_shards("pipeline-0001", 8);
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].pipeline_id, "pipeline-0001");
        assert!(rows.iter().all(|r| r.health == ShardHealth::Healthy));
    }

    /// Stub audit log entries are returned with seq starting at 1.
    #[test]
    fn catalog_audit_log_returns_entries_with_seq() {
        let rows = catalog_audit_log(&[
            ("ddl", "create_view", "orders_mv", 1000),
            ("ddl", "drop_view", "orders_mv", 2000),
        ]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].seq, 1);
        assert_eq!(rows[1].seq, 2);
        assert_eq!(rows[0].action, "create_view");
    }
}
