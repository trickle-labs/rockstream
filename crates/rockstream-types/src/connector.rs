//! Connector contract types for RockStream (v0.48).
//!
//! Defines the shared types for the Tier 2 connector contract additions
//! described in DESIGN.md §13.3:
//!
//! - [`PartitionFilter`] / [`PartitionPredicate`]: opt-in partition push-down
//!   for `start_snapshot` and `poll_delta`. Connectors that do not support
//!   push-down return `partition_filter_support() -> false` and the operator
//!   layer applies the predicate locally.
//!
//! - [`LawSchemaMetadata`]: metadata that connectors return from `discover_schema`
//!   to advertise which columns carry which built-in [`crate::merge_law::MergeLawId`]
//!   and how external writes to those columns should be classified in the
//!   optimistic validation protocol.
//!
//! - [`WriteClassification`]: the per-column write-classification that
//!   participates in the gateway's optimistic transaction protocol
//!   (DESIGN.md §4.4).
//!
//! - [`ExplainTransaction`]: the shape surfaced in `EXPLAIN TRANSACTION` output
//!   so write-classification metadata is operator-observable.
//!
//! - [`ConnectorLifecycleState`]: the pause/resume/delete state machine for
//!   connector lifecycle management.

use crate::merge_law::MergeLawId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── PartitionFilter / PartitionPredicate ────────────────────────────────────

/// A predicate expression identifying which partitions to include when
/// polling a source.
///
/// The expression is kept deliberately opaque at this layer: connector
/// implementations are responsible for interpreting it. The canonical
/// serialised form is a JSON expression tree compatible with DataFusion's
/// physical expressions, but connectors may choose simpler representations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPredicate {
    /// Serialised predicate expression (connector-interpreted).
    pub expression: String,
    /// Column names referenced by the predicate.
    pub referenced_columns: Vec<String>,
}

/// An opt-in partition-level filter passed to `start_snapshot` / `poll_delta`.
///
/// Connectors that support partition push-down return
/// `partition_filter_support() -> true` and honour this filter by limiting
/// the partitions (Kafka partition IDs, S3 prefixes, Postgres table shards,
/// etc.) they read. Connectors that do **not** support push-down return
/// `partition_filter_support() -> false`; the operator layer then applies
/// equivalent filtering itself after receiving the full partition set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionFilter {
    /// The predicate to evaluate against each candidate partition.
    pub predicate: PartitionPredicate,
    /// When `true`, also apply the predicate to individual rows after
    /// partition pruning (double-filtering for safety).
    pub row_level_fallback: bool,
}

impl PartitionFilter {
    /// Construct a simple equality filter on a single column.
    pub fn eq(column: impl Into<String>, value: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            predicate: PartitionPredicate {
                expression: format!("{} = {}", column, value.into()),
                referenced_columns: vec![column],
            },
            row_level_fallback: true,
        }
    }

    /// Construct a range filter (column BETWEEN lo AND hi).
    pub fn between(
        column: impl Into<String>,
        lo: impl Into<String>,
        hi: impl Into<String>,
    ) -> Self {
        let column = column.into();
        Self {
            predicate: PartitionPredicate {
                expression: format!("{} BETWEEN {} AND {}", column, lo.into(), hi.into()),
                referenced_columns: vec![column],
            },
            row_level_fallback: true,
        }
    }
}

// ─── WriteClassification ─────────────────────────────────────────────────────

/// How a single column's writes should be classified in the gateway's
/// optimistic validation protocol (DESIGN.md §4.4).
///
/// These classifications let an external source participate in the optimistic
/// validation protocol without requiring the gateway to invent a gateway-only
/// path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteClassification {
    /// Writes are unconditional deltas (no read-dependency). Safe to apply
    /// without reading the current value first.
    BlindDelta,
    /// Writes depend on the current value of the column (read-modify-write).
    /// The gateway must fence concurrent writers for this column.
    ReadDependentDelta,
    /// Writes are guarded by an exact primary-key predicate. At most one
    /// logical writer per key is active at any time, making conflicts rare.
    ExactKeyGuardedDelta,
    /// The source provides exactly-once guarantees at the transport level;
    /// the gateway may skip additional deduplication for this column.
    SourceExactlyOnceProtected,
}

impl WriteClassification {
    /// Returns the canonical string used in `EXPLAIN TRANSACTION` output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BlindDelta => "blind_delta",
            Self::ReadDependentDelta => "read_dependent_delta",
            Self::ExactKeyGuardedDelta => "exact_key_guarded_delta",
            Self::SourceExactlyOnceProtected => "source_exactly_once_protected",
        }
    }
}

impl std::fmt::Display for WriteClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── LawSchemaMetadata ───────────────────────────────────────────────────────

/// Per-column metadata returned by `discover_schema` to declare which built-in
/// merge law a column uses and how writes to that column should be classified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LawColumnMetadata {
    /// The merge law registered in the global catalog for this column.
    pub law_id: MergeLawId,
    /// The SQL column type name (e.g. `"COUNTER"`, `"MAX_REGISTER"`).
    pub crdt_type: String,
    /// How writes to this column participate in the optimistic validation
    /// protocol. Set by the connector author based on the source's
    /// delivery guarantees.
    pub write_classification: WriteClassification,
}

/// Schema metadata returned by a connector's `discover_schema` to announce
/// which columns are CRDT columns and what merge laws they use.
///
/// This is the v0.48 extension of the v0.47 schema-discovery surface. It lets
/// connectors advertise CRDT column declarations directly from `discover_schema`
/// so they round-trip through `EXPLAIN` and are available to the gateway's
/// optimistic transaction validator.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LawSchemaMetadata {
    /// Map from column name → per-column law metadata.
    pub columns: BTreeMap<String, LawColumnMetadata>,
}

impl LawSchemaMetadata {
    /// Create an empty schema metadata (no CRDT columns).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Register a single column with the given law and write classification.
    pub fn with_column(
        mut self,
        column: impl Into<String>,
        law_id: MergeLawId,
        crdt_type: impl Into<String>,
        write_classification: WriteClassification,
    ) -> Self {
        self.columns.insert(
            column.into(),
            LawColumnMetadata {
                law_id,
                crdt_type: crdt_type.into(),
                write_classification,
            },
        );
        self
    }

    /// Returns `true` if no CRDT columns are declared.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

// ─── ExplainTransaction ──────────────────────────────────────────────────────

/// One column's entry in `EXPLAIN TRANSACTION` output (v0.48).
///
/// Surfaces write-classification metadata so the gateway's optimistic
/// transaction validation logic is operator-observable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainTransactionColumn {
    /// Column name.
    pub column: String,
    /// CRDT type name (e.g. `"COUNTER"`).
    pub crdt_type: String,
    /// The merge law used for this column.
    pub law_id: MergeLawId,
    /// The write-classification for the optimistic validation protocol.
    pub write_classification: WriteClassification,
}

/// The output of `EXPLAIN TRANSACTION` for a write path involving connector
/// columns (v0.48).
///
/// Appears in EXPLAIN output when a write touches one or more CRDT columns
/// declared in [`LawSchemaMetadata`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainTransaction {
    /// Source or sink connector name.
    pub connector_name: String,
    /// Whether the connector supports partition filter push-down.
    pub partition_filter_support: bool,
    /// Per-column write-classification metadata.
    pub columns: Vec<ExplainTransactionColumn>,
}

impl ExplainTransaction {
    /// Render a human-readable string for EXPLAIN output.
    pub fn format_lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "Connector: {}  (partition_filter_support={})",
            self.connector_name, self.partition_filter_support
        )];
        for col in &self.columns {
            lines.push(format!(
                "  column={} crdt_type={} law={} write_classification={}",
                col.column, col.crdt_type, col.law_id, col.write_classification
            ));
        }
        lines
    }

    /// Build an `ExplainTransaction` from a connector name, its
    /// `partition_filter_support` flag, and its `LawSchemaMetadata`.
    pub fn from_schema_metadata(
        connector_name: impl Into<String>,
        partition_filter_support: bool,
        metadata: &LawSchemaMetadata,
    ) -> Self {
        let columns = metadata
            .columns
            .iter()
            .map(|(col, meta)| ExplainTransactionColumn {
                column: col.clone(),
                crdt_type: meta.crdt_type.clone(),
                law_id: meta.law_id,
                write_classification: meta.write_classification,
            })
            .collect();
        Self {
            connector_name: connector_name.into(),
            partition_filter_support,
            columns,
        }
    }
}

// ─── ConnectorLifecycleState ─────────────────────────────────────────────────

/// Lifecycle state for a managed connector (v0.48 connector lifecycle).
///
/// The valid transitions are:
/// ```text
/// Running → Paused → Running
///         ↘
///           Deleted
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorLifecycleState {
    /// Connector is actively producing/consuming records.
    Running,
    /// Connector is paused; no records are produced or consumed.
    /// Epoch commits are suspended. The connector may be resumed.
    Paused,
    /// Connector has been deleted. All resources are released and the
    /// connector cannot be resumed.
    Deleted,
}

impl std::fmt::Display for ConnectorLifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Paused => write!(f, "paused"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::MergeLawId;

    // ── PartitionFilter ────────────────────────────────────────────────────

    #[test]
    fn partition_filter_eq_builds_expression() {
        let f = PartitionFilter::eq("region", "us-east-1");
        assert!(f.predicate.expression.contains("region"));
        assert!(f.predicate.expression.contains("us-east-1"));
        assert_eq!(f.predicate.referenced_columns, vec!["region"]);
        assert!(f.row_level_fallback);
    }

    #[test]
    fn partition_filter_between_builds_expression() {
        let f = PartitionFilter::between("partition_id", "0", "7");
        assert!(f.predicate.expression.contains("BETWEEN"));
        assert!(f
            .predicate
            .referenced_columns
            .contains(&"partition_id".to_string()));
    }

    #[test]
    fn partition_filter_serializes_round_trip() {
        let f = PartitionFilter::eq("ts", "2024-01-01");
        let json = serde_json::to_string(&f).unwrap();
        let back: PartitionFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }

    // ── WriteClassification ────────────────────────────────────────────────

    #[test]
    fn write_classification_as_str_all_variants() {
        assert_eq!(WriteClassification::BlindDelta.as_str(), "blind_delta");
        assert_eq!(
            WriteClassification::ReadDependentDelta.as_str(),
            "read_dependent_delta"
        );
        assert_eq!(
            WriteClassification::ExactKeyGuardedDelta.as_str(),
            "exact_key_guarded_delta"
        );
        assert_eq!(
            WriteClassification::SourceExactlyOnceProtected.as_str(),
            "source_exactly_once_protected"
        );
    }

    #[test]
    fn write_classification_display_matches_as_str() {
        for wc in [
            WriteClassification::BlindDelta,
            WriteClassification::ReadDependentDelta,
            WriteClassification::ExactKeyGuardedDelta,
            WriteClassification::SourceExactlyOnceProtected,
        ] {
            assert_eq!(wc.to_string(), wc.as_str());
        }
    }

    #[test]
    fn write_classification_serializes_round_trip() {
        let wc = WriteClassification::ExactKeyGuardedDelta;
        let json = serde_json::to_string(&wc).unwrap();
        let back: WriteClassification = serde_json::from_str(&json).unwrap();
        assert_eq!(wc, back);
    }

    // ── LawSchemaMetadata ──────────────────────────────────────────────────

    #[test]
    fn law_schema_metadata_empty_by_default() {
        let meta = LawSchemaMetadata::empty();
        assert!(meta.is_empty());
        assert!(meta.columns.is_empty());
    }

    #[test]
    fn law_schema_metadata_with_column_builder() {
        let meta = LawSchemaMetadata::empty()
            .with_column(
                "amount",
                MergeLawId(10), // PNCounter/v1
                "COUNTER",
                WriteClassification::BlindDelta,
            )
            .with_column(
                "last_seen",
                MergeLawId(11), // MaxRegister/v1
                "MAX_REGISTER",
                WriteClassification::ExactKeyGuardedDelta,
            );
        assert!(!meta.is_empty());
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(
            meta.columns["amount"].write_classification,
            WriteClassification::BlindDelta
        );
        assert_eq!(meta.columns["last_seen"].crdt_type, "MAX_REGISTER");
    }

    #[test]
    fn law_schema_metadata_serializes_round_trip() {
        let meta = LawSchemaMetadata::empty().with_column(
            "score",
            MergeLawId(10),
            "COUNTER",
            WriteClassification::BlindDelta,
        );
        let json = serde_json::to_string(&meta).unwrap();
        let back: LawSchemaMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    // ── ExplainTransaction ─────────────────────────────────────────────────

    #[test]
    fn explain_transaction_from_schema_metadata() {
        let meta = LawSchemaMetadata::empty().with_column(
            "count",
            MergeLawId(10),
            "COUNTER",
            WriteClassification::BlindDelta,
        );
        let explain = ExplainTransaction::from_schema_metadata("my-kafka-source", false, &meta);
        assert_eq!(explain.connector_name, "my-kafka-source");
        assert!(!explain.partition_filter_support);
        assert_eq!(explain.columns.len(), 1);
        assert_eq!(explain.columns[0].column, "count");
        assert_eq!(
            explain.columns[0].write_classification,
            WriteClassification::BlindDelta
        );
    }

    #[test]
    fn explain_transaction_format_lines_non_empty() {
        let meta = LawSchemaMetadata::empty().with_column(
            "amount",
            MergeLawId(10),
            "COUNTER",
            WriteClassification::BlindDelta,
        );
        let explain = ExplainTransaction::from_schema_metadata("example-sdk", true, &meta);
        let lines = explain.format_lines();
        assert!(!lines.is_empty());
        assert!(lines[0].contains("example-sdk"));
        assert!(lines[0].contains("partition_filter_support=true"));
        // Second line for the column
        assert!(lines.iter().any(|l| l.contains("amount")));
        assert!(lines.iter().any(|l| l.contains("blind_delta")));
    }

    #[test]
    fn explain_transaction_serializes_round_trip() {
        let meta = LawSchemaMetadata::empty().with_column(
            "val",
            MergeLawId(11),
            "MAX_REGISTER",
            WriteClassification::ReadDependentDelta,
        );
        let explain = ExplainTransaction::from_schema_metadata("pg-cdc", false, &meta);
        let json = serde_json::to_string(&explain).unwrap();
        let back: ExplainTransaction = serde_json::from_str(&json).unwrap();
        assert_eq!(explain, back);
    }

    // ── ConnectorLifecycleState ────────────────────────────────────────────

    #[test]
    fn connector_lifecycle_state_display() {
        assert_eq!(ConnectorLifecycleState::Running.to_string(), "running");
        assert_eq!(ConnectorLifecycleState::Paused.to_string(), "paused");
        assert_eq!(ConnectorLifecycleState::Deleted.to_string(), "deleted");
    }

    #[test]
    fn connector_lifecycle_state_serializes_round_trip() {
        for state in [
            ConnectorLifecycleState::Running,
            ConnectorLifecycleState::Paused,
            ConnectorLifecycleState::Deleted,
        ] {
            let json = serde_json::to_string(&state).unwrap();
            let back: ConnectorLifecycleState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, back);
        }
    }
}
