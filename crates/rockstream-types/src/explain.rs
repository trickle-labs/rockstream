//! Explain types for `EXPLAIN INCREMENTAL`.
//!
//! Defines the finalized v0.17 contracts for explain output:
//! - `NotMergeSafeReason` — closed enum (moved from `rockstream-plan`).
//! - `ExplainLevel` — three explain levels (Default, Verbose, Analyze).
//! - `ExplainLawAnnotation` — finalized law-annotation contract for every
//!   operator in `EXPLAIN INCREMENTAL` output.
//! - `OperatorStats` — live runtime statistics for `EXPLAIN INCREMENTAL ANALYZE`.
//! - `ShardInfo` — shard/parallelism information for `EXPLAIN INCREMENTAL VERBOSE`.
//! - `ConfidenceLabel` — confidence tier for cost estimates.
//! - `SourceStatsEstimate` — estimated source statistics.
//! - `BackfillCostEstimate` — estimated cost for a `CREATE MATERIALIZED VIEW` backfill,
//!   including the `BACKFILL_CONFIRMATION_THRESHOLD_BYTES` constant.
//! - `ExplainRow` — one row of formatted explain output.

use crate::merge_law::{CompactionPolicy, DuplicatePolicy, MergeLawClass};
use serde::{Deserialize, Serialize};
use std::fmt;

// ─── NotMergeSafeReason ──────────────────────────────────────────────────────

/// Closed enum of reasons an operator does not support merge-safe reads.
///
/// The set is fixed at compile time per the v0.17 finalization contract.
/// New reasons require a crate version bump.
///
/// Appears in `EXPLAIN INCREMENTAL` output as
/// `not_merge_safe_reason=<value>` and via the `LawBundle` trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotMergeSafeReason {
    /// `MIN`/`MAX` require read-modify-write for correctness because
    /// retraction-based delete requires a prefix scan.
    ExtremumRequiresRmw,
    /// `INTERSECT`/`EXCEPT` use weight clamping, not a commutative law.
    ClampNotALaw,
    /// User-defined aggregate with unknown algebraic properties.
    UnknownUdafProperties,
    /// Operator has no arrangement state (stateless linear operator).
    Stateless,
    /// Window ranking functions recompute the entire partition; not merge-safe.
    PartitionRecomputation,
    /// Non-monotone recursive operator: DRed strategy requires read-modify-write.
    /// The escape hatch rejects non-monotone deltas at runtime with RS-1009.
    RecursionDredRequired,
}

impl NotMergeSafeReason {
    /// Convert to the canonical snake_case string used in `EXPLAIN INCREMENTAL`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ExtremumRequiresRmw => "extremum_requires_rmw",
            Self::ClampNotALaw => "clamp_not_a_law",
            Self::UnknownUdafProperties => "unknown_udaf_properties",
            Self::Stateless => "stateless",
            Self::PartitionRecomputation => "partition_recomputation",
            Self::RecursionDredRequired => "recursion_dred_required",
        }
    }

    /// Returns a slice of all registered `NotMergeSafeReason` variants.
    ///
    /// CI-enumerable: the `all_not_merge_safe_reasons_covered` test iterates
    /// this list and verifies every variant has a non-empty canonical string.
    pub fn all() -> &'static [NotMergeSafeReason] {
        &[
            Self::ExtremumRequiresRmw,
            Self::ClampNotALaw,
            Self::UnknownUdafProperties,
            Self::Stateless,
            Self::PartitionRecomputation,
            Self::RecursionDredRequired,
        ]
    }
}

impl fmt::Display for NotMergeSafeReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── ExplainLevel ────────────────────────────────────────────────────────────

/// The detail level requested for an `EXPLAIN INCREMENTAL` statement.
///
/// DESIGN.md §14.8 — three explain levels:
/// - `Default`: human-readable summary with ✓/⚠/✗ merge-safety indicators.
/// - `Verbose`: adds merge-law annotations, shard counts, parallelism,
///   frontier timestamps.
/// - `Analyze`: adds live per-operator runtime statistics (rows/s, state reads,
///   RMW ratio, p99 latency, DLQ entries) requiring a live worker round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ExplainLevel {
    /// Default: human-readable summary with ✓/⚠/✗ indicators.
    #[default]
    Default,
    /// Verbose: adds merge-law annotations, shard counts, parallelism,
    /// and frontier timestamps.
    Verbose,
    /// Analyze: adds live per-operator runtime statistics.
    Analyze,
}

// ─── ExplainLawAnnotation ─────────────────────────────────────────────────────

/// Finalized law-annotation contract for `EXPLAIN INCREMENTAL` output (v0.17).
///
/// Every operator in an explain result carries this annotation. The fields
/// correspond to the finalized contract:
/// `merge_law=<name>/v<n>`, `law_class`, `idempotent`, `duplicate_policy`,
/// `compaction`, `combiner`, `partial_pushdown`, `not_merge_safe_reason`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainLawAnnotation {
    /// Human-readable law name and version, e.g. `"WeightAdd/v1"`.
    pub merge_law: String,
    /// Algebraic classification of the law.
    pub law_class: MergeLawClass,
    /// Whether the merge function is idempotent: `f(a, a) = a`.
    pub idempotent: bool,
    /// Policy for handling duplicate merge operations.
    pub duplicate_policy: DuplicatePolicy,
    /// Compaction policy for arrangement state managed by this law.
    pub compaction: CompactionPolicy,
    /// Whether a shuffle combiner is available for this law.
    /// True for any associative+commutative (non-stateless) law.
    pub combiner: bool,
    /// Whether partial aggregation pushdown to shards is supported.
    /// True when the law is associative+commutative and has an identity.
    pub partial_pushdown: bool,
    /// Reason this operator is not merge-safe, if applicable.
    pub not_merge_safe_reason: Option<NotMergeSafeReason>,
}

impl ExplainLawAnnotation {
    /// Returns `true` if the operator is merge-safe (no forced RMW).
    pub fn is_merge_safe(&self) -> bool {
        self.not_merge_safe_reason.is_none()
    }

    /// Returns the merge-safety indicator character for the default explain
    /// level: `✓` (merge-safe), `⚠` (stateless), `✗` (not merge-safe).
    pub fn merge_safe_indicator(&self) -> char {
        match self.not_merge_safe_reason {
            None => '✓',
            Some(NotMergeSafeReason::Stateless) => '⚠',
            Some(_) => '✗',
        }
    }
}

// ─── ConfidenceLabel ─────────────────────────────────────────────────────────

/// Confidence tier for cost estimates and source statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfidenceLabel {
    /// High confidence — based on actual observed statistics.
    High,
    /// Medium confidence — based on partial statistics or heuristics.
    Medium,
    /// Low confidence — estimate is speculative.
    Low,
}

impl fmt::Display for ConfidenceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::High => write!(f, "high"),
            Self::Medium => write!(f, "medium"),
            Self::Low => write!(f, "low"),
        }
    }
}

// ─── SourceStatsEstimate ─────────────────────────────────────────────────────

/// Estimated statistics for a source, used in `EXPLAIN INCREMENTAL ESTIMATE`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStatsEstimate {
    /// Estimated number of rows in the source.
    pub estimated_rows: u64,
    /// Estimated total state size in bytes.
    pub estimated_state_bytes: u64,
    /// Estimated ingestion rate (rows per second).
    pub estimated_row_rate_per_s: f64,
    /// Confidence label for these estimates.
    pub confidence: ConfidenceLabel,
}

// ─── BackfillCostEstimate ─────────────────────────────────────────────────────

/// Threshold above which a `CREATE MATERIALIZED VIEW` requires explicit
/// confirmation from the user before proceeding.
///
/// DESIGN.md §14.9: set to 1 GB.
pub const BACKFILL_CONFIRMATION_THRESHOLD_BYTES: u64 = 1_000_000_000;

/// Estimated cost for a `CREATE MATERIALIZED VIEW` backfill.
///
/// Produced by `EXPLAIN INCREMENTAL ESTIMATE CREATE MATERIALIZED VIEW ...`
/// and by the backfill confirmation prompt that fires when the estimated
/// state exceeds `BACKFILL_CONFIRMATION_THRESHOLD_BYTES`.
///
/// Pass `WITHOUT CONFIRMATION` to skip the prompt programmatically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackfillCostEstimate {
    /// Estimated resulting arrangement state size in bytes.
    pub estimated_state_bytes: u64,
    /// Estimated number of source rows to process.
    pub estimated_rows: u64,
    /// Estimated backfill duration in milliseconds.
    pub estimated_duration_ms: u64,
    /// Confidence label for these estimates.
    pub confidence: ConfidenceLabel,
    /// Name of the source being backfilled.
    pub source_name: String,
}

impl BackfillCostEstimate {
    /// Returns `true` when the estimated state exceeds the confirmation threshold,
    /// meaning the user must confirm (or pass `WITHOUT CONFIRMATION`) before
    /// `CREATE MATERIALIZED VIEW` proceeds.
    pub fn requires_confirmation(&self) -> bool {
        self.estimated_state_bytes >= BACKFILL_CONFIRMATION_THRESHOLD_BYTES
    }

    /// Returns a human-readable confirmation prompt string.
    pub fn confirmation_prompt(&self) -> String {
        format!(
            "Backfill of '{}' will process ~{} rows and create ~{} of state. \
Proceed? (Use WITHOUT CONFIRMATION to bypass)",
            self.source_name,
            self.estimated_rows,
            format_bytes(self.estimated_state_bytes),
        )
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else {
        format!("{} KB", bytes / 1_000)
    }
}

// ─── OperatorStats ───────────────────────────────────────────────────────────

/// Live per-operator runtime statistics for `EXPLAIN INCREMENTAL ANALYZE`.
///
/// Requires a live worker round-trip to populate — not available for plans
/// that are not currently executing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorStats {
    /// Rows processed per second (throughput at steady state).
    pub rows_per_s: f64,
    /// Total state reads issued by this operator.
    pub state_reads: u64,
    /// Ratio of read-modify-write operations to total state accesses.
    pub rmw_ratio: f64,
    /// 99th-percentile operator processing latency in milliseconds.
    pub p99_latency_ms: f64,
    /// Number of records currently in the dead-letter queue for this operator.
    pub dlq_entries: u64,
}

// ─── ShardInfo ──────────────────────────────────────────────────────────────

/// Shard-level information for `EXPLAIN INCREMENTAL VERBOSE`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Number of shards currently executing this operator.
    pub shard_count: u32,
    /// Parallelism degree (workers × threads per shard).
    pub parallelism: u32,
    /// Frontier epoch last reported by this operator's shards.
    pub frontier_epoch: u64,
}

// ─── ExplainRow ──────────────────────────────────────────────────────────────

/// One row in an `EXPLAIN INCREMENTAL` result.
///
/// The `operator_stats` field is populated only at `ExplainLevel::Analyze`;
/// the `shard_info` field is populated at `ExplainLevel::Verbose` and above.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainRow {
    /// Operator depth in the plan tree (0 = root/sink).
    pub depth: u32,
    /// Human-readable operator kind (e.g. `"Filter"`, `"Aggregate[SUM]"`).
    pub operator_kind: String,
    /// Finalized law annotation for this operator.
    pub annotation: ExplainLawAnnotation,
    /// Live runtime statistics (populated at Analyze level only).
    pub operator_stats: Option<OperatorStats>,
    /// Shard and parallelism information (populated at Verbose level and above).
    pub shard_info: Option<ShardInfo>,
}

impl ExplainRow {
    /// Render a single-line text summary at the requested explain level.
    ///
    /// - **Default**: `"  [✓] Filter  (stateless, merge_safe)"`
    /// - **Verbose**: appends `"  [shards=N parallelism=M frontier=E]"`
    /// - **Analyze**: appends `"  [rows/s=NNN p99=N.Nms rmw_ratio=N.NN]"`
    pub fn format_line(&self, level: ExplainLevel) -> String {
        let indent = "  ".repeat(self.depth as usize);
        let indicator = self.annotation.merge_safe_indicator();
        let law = &self.annotation.merge_law;
        let base = format!("{indent}[{indicator}] {}  ({law})", self.operator_kind);

        match level {
            ExplainLevel::Default => base,
            ExplainLevel::Verbose => {
                if let Some(ref si) = self.shard_info {
                    format!(
                        "{base}  [shards={} parallelism={} frontier={}]",
                        si.shard_count, si.parallelism, si.frontier_epoch
                    )
                } else {
                    base
                }
            }
            ExplainLevel::Analyze => {
                if let Some(ref stats) = self.operator_stats {
                    format!(
                        "{base}  [rows/s={:.0} p99={:.1}ms rmw_ratio={:.2}]",
                        stats.rows_per_s, stats.p99_latency_ms, stats.rmw_ratio
                    )
                } else {
                    base
                }
            }
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_safe_annotation() -> ExplainLawAnnotation {
        ExplainLawAnnotation {
            merge_law: "WeightAdd/v1".to_string(),
            law_class: MergeLawClass::AbelianGroup,
            idempotent: false,
            duplicate_policy: DuplicatePolicy::Merge,
            compaction: CompactionPolicy::TombstoneGc,
            combiner: true,
            partial_pushdown: true,
            not_merge_safe_reason: None,
        }
    }

    fn sample_unsafe_annotation() -> ExplainLawAnnotation {
        ExplainLawAnnotation {
            merge_law: "MaxRegister/v1".to_string(),
            law_class: MergeLawClass::Semilattice,
            idempotent: true,
            duplicate_policy: DuplicatePolicy::Merge,
            compaction: CompactionPolicy::MergeOnCompact,
            combiner: false,
            partial_pushdown: false,
            not_merge_safe_reason: Some(NotMergeSafeReason::ExtremumRequiresRmw),
        }
    }

    fn sample_stateless_annotation() -> ExplainLawAnnotation {
        ExplainLawAnnotation {
            merge_law: "stateless".to_string(),
            law_class: MergeLawClass::CommutativeMonoid,
            idempotent: true,
            duplicate_policy: DuplicatePolicy::Merge,
            compaction: CompactionPolicy::RetainAll,
            combiner: false,
            partial_pushdown: false,
            not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
        }
    }

    // ── NotMergeSafeReason ─────────────────────────────────────────────────

    /// CI-enforced: every NotMergeSafeReason variant must have a non-empty
    /// snake_case canonical string. This test is the registry enumeration
    /// proof for the v0.17 EXPLAIN contract.
    #[test]
    fn all_not_merge_safe_reasons_covered() {
        for reason in NotMergeSafeReason::all() {
            let s = reason.as_str();
            assert!(
                !s.is_empty(),
                "NotMergeSafeReason::{reason:?} has empty string"
            );
            assert!(
                s.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "NotMergeSafeReason::{reason:?} string '{s}' is not snake_case"
            );
            // Display must equal as_str()
            assert_eq!(reason.to_string(), s);
        }
    }

    #[test]
    fn not_merge_safe_reason_strings_are_unique() {
        let strs: Vec<_> = NotMergeSafeReason::all()
            .iter()
            .map(|r| r.as_str())
            .collect();
        let unique: std::collections::HashSet<_> = strs.iter().collect();
        assert_eq!(
            strs.len(),
            unique.len(),
            "duplicate not_merge_safe_reason strings"
        );
    }

    // ── ExplainLawAnnotation ───────────────────────────────────────────────

    #[test]
    fn merge_safe_indicator_for_safe_operator() {
        let ann = sample_safe_annotation();
        assert_eq!(ann.merge_safe_indicator(), '✓');
        assert!(ann.is_merge_safe());
    }

    #[test]
    fn merge_safe_indicator_for_unsafe_operator() {
        let ann = sample_unsafe_annotation();
        assert_eq!(ann.merge_safe_indicator(), '✗');
        assert!(!ann.is_merge_safe());
    }

    #[test]
    fn stateless_operator_shows_warning_indicator() {
        let ann = sample_stateless_annotation();
        assert_eq!(ann.merge_safe_indicator(), '⚠');
        assert!(!ann.is_merge_safe());
    }

    // ── BackfillCostEstimate ───────────────────────────────────────────────

    #[test]
    fn backfill_confirmation_threshold_is_one_gb() {
        assert_eq!(BACKFILL_CONFIRMATION_THRESHOLD_BYTES, 1_000_000_000);
    }

    #[test]
    fn backfill_requires_confirmation_above_threshold() {
        let est = BackfillCostEstimate {
            estimated_state_bytes: 2_000_000_000, // 2 GB
            estimated_rows: 100_000_000,
            estimated_duration_ms: 60_000,
            confidence: ConfidenceLabel::Medium,
            source_name: "orders".to_string(),
        };
        assert!(est.requires_confirmation());
        let prompt = est.confirmation_prompt();
        assert!(prompt.contains("orders"));
        assert!(prompt.contains("WITHOUT CONFIRMATION"));
    }

    #[test]
    fn backfill_no_confirmation_below_threshold() {
        let est = BackfillCostEstimate {
            estimated_state_bytes: 500_000_000, // 0.5 GB
            estimated_rows: 10_000_000,
            estimated_duration_ms: 5_000,
            confidence: ConfidenceLabel::High,
            source_name: "users".to_string(),
        };
        assert!(!est.requires_confirmation());
    }

    #[test]
    fn backfill_at_exactly_threshold_requires_confirmation() {
        let est = BackfillCostEstimate {
            estimated_state_bytes: BACKFILL_CONFIRMATION_THRESHOLD_BYTES,
            estimated_rows: 50_000_000,
            estimated_duration_ms: 30_000,
            confidence: ConfidenceLabel::Low,
            source_name: "events".to_string(),
        };
        assert!(est.requires_confirmation());
    }

    // ── ExplainRow formatting ──────────────────────────────────────────────

    #[test]
    fn explain_row_default_format_shows_checkmark_for_safe() {
        let row = ExplainRow {
            depth: 0,
            operator_kind: "Filter".to_string(),
            annotation: sample_safe_annotation(),
            operator_stats: None,
            shard_info: None,
        };
        let line = row.format_line(ExplainLevel::Default);
        assert!(line.contains('✓'), "line: {line}");
        assert!(line.contains("Filter"), "line: {line}");
        assert!(line.contains("WeightAdd/v1"), "line: {line}");
    }

    #[test]
    fn explain_row_default_format_shows_cross_for_unsafe() {
        let row = ExplainRow {
            depth: 0,
            operator_kind: "Aggregate[MAX]".to_string(),
            annotation: sample_unsafe_annotation(),
            operator_stats: None,
            shard_info: None,
        };
        let line = row.format_line(ExplainLevel::Default);
        assert!(line.contains('✗'), "line: {line}");
    }

    #[test]
    fn explain_row_default_format_shows_warning_for_stateless() {
        let row = ExplainRow {
            depth: 1,
            operator_kind: "Filter".to_string(),
            annotation: sample_stateless_annotation(),
            operator_stats: None,
            shard_info: None,
        };
        let line = row.format_line(ExplainLevel::Default);
        assert!(line.contains('⚠'), "line: {line}");
    }

    #[test]
    fn explain_row_verbose_includes_shard_count_and_parallelism() {
        let row = ExplainRow {
            depth: 0,
            operator_kind: "Aggregate[SUM]".to_string(),
            annotation: sample_safe_annotation(),
            operator_stats: None,
            shard_info: Some(ShardInfo {
                shard_count: 8,
                parallelism: 4,
                frontier_epoch: 1234,
            }),
        };
        let line = row.format_line(ExplainLevel::Verbose);
        assert!(line.contains("shards=8"), "line: {line}");
        assert!(line.contains("parallelism=4"), "line: {line}");
        assert!(line.contains("frontier=1234"), "line: {line}");
    }

    #[test]
    fn explain_row_analyze_includes_p99_latency_and_rows_per_s() {
        let row = ExplainRow {
            depth: 0,
            operator_kind: "Aggregate[COUNT]".to_string(),
            annotation: sample_safe_annotation(),
            operator_stats: Some(OperatorStats {
                rows_per_s: 50_000.0,
                state_reads: 1234,
                rmw_ratio: 0.05,
                p99_latency_ms: 2.5,
                dlq_entries: 0,
            }),
            shard_info: None,
        };
        let line = row.format_line(ExplainLevel::Analyze);
        assert!(line.contains("p99=2.5ms"), "line: {line}");
        assert!(line.contains("rows/s=50000"), "line: {line}");
    }

    #[test]
    fn explain_row_depth_indentation() {
        let ann = sample_safe_annotation();
        let row0 = ExplainRow {
            depth: 0,
            operator_kind: "Source[orders]".to_string(),
            annotation: ann.clone(),
            operator_stats: None,
            shard_info: None,
        };
        let row2 = ExplainRow {
            depth: 2,
            operator_kind: "Filter".to_string(),
            annotation: ann,
            operator_stats: None,
            shard_info: None,
        };
        let line0 = row0.format_line(ExplainLevel::Default);
        let line2 = row2.format_line(ExplainLevel::Default);
        assert!(line0.starts_with('['), "depth=0 should not indent: {line0}");
        assert!(
            line2.starts_with("    "),
            "depth=2 should have 4 spaces: {line2}"
        );
    }

    // ── ConfidenceLabel ───────────────────────────────────────────────────

    #[test]
    fn confidence_label_display() {
        assert_eq!(ConfidenceLabel::High.to_string(), "high");
        assert_eq!(ConfidenceLabel::Medium.to_string(), "medium");
        assert_eq!(ConfidenceLabel::Low.to_string(), "low");
    }

    // ── SourceStatsEstimate ────────────────────────────────────────────────

    #[test]
    fn source_stats_estimate_serializes_round_trip() {
        let stats = SourceStatsEstimate {
            estimated_rows: 1_000_000,
            estimated_state_bytes: 50_000_000,
            estimated_row_rate_per_s: 10_000.0,
            confidence: ConfidenceLabel::High,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: SourceStatsEstimate = serde_json::from_str(&json).unwrap();
        assert_eq!(stats, back);
    }
}
