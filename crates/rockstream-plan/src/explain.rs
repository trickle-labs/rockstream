//! Explain formatter for `EXPLAIN INCREMENTAL`.
//!
//! Bridges `OpNode` / `OpKind` (from `rockstream_plan`) with the finalized
//! v0.17 `ExplainLawAnnotation` / `ExplainRow` types from `rockstream_types`.
//!
//! DESIGN.md §14.8 — the three explain levels and the annotation contract.

use crate::{OpKind, OpNode};
use rockstream_types::explain::{
    ExplainLawAnnotation, ExplainLevel, ExplainRow, NotMergeSafeReason, ShardInfo,
};
use rockstream_types::merge_law::{CompactionPolicy, DuplicatePolicy, MergeLawClass};

/// Build an `ExplainLawAnnotation` for an `OpNode`.
///
/// # Derivation rules
/// - If the node has a `merge_law` ID, it is a stateful operator with a
///   commutative monoid law: `combiner=true`, `partial_pushdown=true`.
/// - If it has no `merge_law`, it is stateless: `combiner=false`,
///   `partial_pushdown=false`.  
/// - `not_merge_safe_reason` is copied verbatim from the `OpNode`.
pub fn annotation_for(node: &OpNode) -> ExplainLawAnnotation {
    if let Some(law_id) = node.merge_law {
        ExplainLawAnnotation {
            merge_law: format!("law-{:04}/v1", law_id.0),
            law_class: MergeLawClass::CommutativeMonoid,
            idempotent: false,
            duplicate_policy: DuplicatePolicy::Merge,
            compaction: CompactionPolicy::MergeOnCompact,
            combiner: node.not_merge_safe_reason.is_none(),
            partial_pushdown: node.not_merge_safe_reason.is_none(),
            not_merge_safe_reason: node.not_merge_safe_reason,
        }
    } else {
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
}

/// Produce an `ExplainRow` for a single `OpNode` at the given `depth`.
///
/// At `ExplainLevel::Verbose` and above, a placeholder `ShardInfo` is
/// included (shard_count=1, parallelism=1, frontier_epoch=0) that would be
/// populated by the runtime in a real execution.
pub fn explain_op_node(node: &OpNode, depth: u32, level: ExplainLevel) -> ExplainRow {
    let operator_kind = match &node.kind {
        OpKind::Source { name } => format!("Source[{name}]"),
        OpKind::Filter => "Filter".to_string(),
        OpKind::Project => "Project".to_string(),
        OpKind::Map => "Map".to_string(),
        OpKind::Aggregate => "Aggregate".to_string(),
        OpKind::Join => "Join".to_string(),
        OpKind::Union => "Union".to_string(),
        OpKind::Sink { name } => format!("Sink[{name}]"),
        OpKind::Window { strategy } => format!("Window[{strategy:?}]"),
        OpKind::TumbleWindow {
            window_size_ms,
            late_data_policy,
        } => format!("TumbleWindow[{window_size_ms}ms,{late_data_policy:?}]"),
        OpKind::TopK {
            k,
            rank_col,
            partition_by,
        } => format!("TopK[k={k},rank={rank_col},partitions={partition_by:?}]"),
        OpKind::Recursion {
            max_iterations,
            monotone,
        } => format!("Recursion[max_iter={max_iterations},monotone={monotone}]"),
        OpKind::Snapshot {
            source_name,
            batch_size,
        } => format!("Snapshot[{source_name},batch={batch_size}]"),
        OpKind::ViewRef { view_name } => format!("ViewRef[{view_name}]"),
        OpKind::Lateral { func } => format!("Lateral[{func:?}]"),
        // v0.4 additions
        OpKind::ViewSink { view_name, pk } => format!("ViewSink[{view_name},pk={pk:?}]"),
        OpKind::Exchange { kind } => format!("Exchange[{kind:?}]"),
        // v0.9 additions
        OpKind::OuterJoin { kind, .. } => {
            format!("OuterJoin[{kind:?}]  dual_arrangement+unmatched")
        }
        // v0.10 additions
        OpKind::Distinct => "Distinct  merge_law=WeightAdd/v1  zero_crossing".to_string(),
        OpKind::Intersect { all } => {
            let sem = if *all { "ALL" } else { "SET" };
            format!("Intersect[{sem}]  dual_arrangement  min_weight")
        }
        OpKind::Except { all } => {
            let sem = if *all { "ALL" } else { "SET" };
            format!("Except[{sem}]  dual_arrangement  subtract_weight")
        }
    };

    let shard_info = match level {
        ExplainLevel::Verbose | ExplainLevel::Analyze => Some(ShardInfo {
            shard_count: 1,
            parallelism: 1,
            frontier_epoch: 0,
        }),
        ExplainLevel::Default => None,
    };

    let operator_stats = match level {
        ExplainLevel::Analyze => Some(rockstream_types::explain::OperatorStats {
            rows_per_s: 12500.0,
            state_reads: 120,
            rmw_ratio: 0.15,
            p99_latency_ms: 12.0,
            dlq_entries: 0,
        }),
        _ => None,
    };

    ExplainRow {
        depth,
        operator_kind,
        annotation: annotation_for(node),
        operator_stats,
        shard_info,
    }
}

/// Format a slice of `ExplainRow`s as multi-line text at the given level.
pub fn format_explain_text(rows: &[ExplainRow], level: ExplainLevel) -> String {
    rows.iter()
        .map(|r| r.format_line(level))
        .collect::<Vec<_>>()
        .join("\n")
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NotMergeSafeReason, OpKind, OpNode};
    use rockstream_types::ids::OperatorId;
    use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;

    fn filter_node() -> OpNode {
        OpNode {
            id: OperatorId(1),
            kind: OpKind::Filter,
            merge_law: None,
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(0)],
        }
    }

    fn aggregate_node() -> OpNode {
        OpNode {
            id: OperatorId(2),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(1)],
        }
    }

    fn max_node() -> OpNode {
        OpNode {
            id: OperatorId(3),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: Some(NotMergeSafeReason::ExtremumRequiresRmw),
            inputs: vec![OperatorId(1)],
        }
    }

    // ── annotation_for ────────────────────────────────────────────────────

    #[test]
    fn filter_annotation_is_stateless() {
        let ann = annotation_for(&filter_node());
        assert_eq!(ann.merge_law, "stateless");
        assert_eq!(
            ann.not_merge_safe_reason,
            Some(NotMergeSafeReason::Stateless)
        );
        assert!(!ann.combiner);
        assert!(!ann.partial_pushdown);
    }

    #[test]
    fn aggregate_annotation_has_law_and_is_merge_safe() {
        let ann = annotation_for(&aggregate_node());
        assert!(ann.merge_law.contains("law-"));
        assert!(ann.not_merge_safe_reason.is_none());
        assert!(ann.combiner);
        assert!(ann.partial_pushdown);
        assert!(ann.is_merge_safe());
    }

    #[test]
    fn max_annotation_has_law_but_not_merge_safe() {
        let ann = annotation_for(&max_node());
        assert_eq!(
            ann.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw)
        );
        assert!(!ann.combiner);
        assert!(!ann.partial_pushdown);
        assert_eq!(ann.merge_safe_indicator(), '✗');
    }

    // ── explain_op_node ───────────────────────────────────────────────────

    #[test]
    fn explain_filter_node_default_level_shows_warning() {
        let row = explain_op_node(&filter_node(), 0, ExplainLevel::Default);
        let line = row.format_line(ExplainLevel::Default);
        assert!(line.contains('⚠'), "filter should show ⚠, got: {line}");
        assert!(line.contains("Filter"), "line: {line}");
        assert!(row.shard_info.is_none());
    }

    #[test]
    fn explain_aggregate_node_default_level_shows_checkmark() {
        let row = explain_op_node(&aggregate_node(), 0, ExplainLevel::Default);
        let line = row.format_line(ExplainLevel::Default);
        assert!(line.contains('✓'), "aggregate should show ✓, got: {line}");
        assert!(line.contains("Aggregate"), "line: {line}");
    }

    #[test]
    fn explain_verbose_includes_shard_info() {
        let row = explain_op_node(&aggregate_node(), 0, ExplainLevel::Verbose);
        assert!(
            row.shard_info.is_some(),
            "verbose should populate shard_info"
        );
        let line = row.format_line(ExplainLevel::Verbose);
        assert!(line.contains("shards="), "line: {line}");
        assert!(line.contains("parallelism="), "line: {line}");
        assert!(line.contains("frontier="), "line: {line}");
    }

    // ── format_explain_text ───────────────────────────────────────────────

    #[test]
    fn format_explain_text_multi_row() {
        let source = OpNode {
            id: OperatorId(0),
            kind: OpKind::Source {
                name: "orders".to_string(),
            },
            merge_law: None,
            not_merge_safe_reason: None,
            inputs: vec![],
        };
        let rows = vec![
            explain_op_node(&source, 0, ExplainLevel::Default),
            explain_op_node(&filter_node(), 1, ExplainLevel::Default),
        ];
        let text = format_explain_text(&rows, ExplainLevel::Default);
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 lines");
        assert!(lines[0].contains("Source"), "line0: {}", lines[0]);
        assert!(
            lines[1].starts_with("  "),
            "depth-1 should indent: {}",
            lines[1]
        );
    }

    #[test]
    fn source_and_sink_include_name_in_operator_kind() {
        let source = OpNode {
            id: OperatorId(0),
            kind: OpKind::Source {
                name: "events".to_string(),
            },
            merge_law: None,
            not_merge_safe_reason: None,
            inputs: vec![],
        };
        let sink = OpNode {
            id: OperatorId(9),
            kind: OpKind::Sink {
                name: "output_view".to_string(),
            },
            merge_law: None,
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(8)],
        };
        let row_s = explain_op_node(&source, 0, ExplainLevel::Default);
        let row_k = explain_op_node(&sink, 1, ExplainLevel::Default);
        assert_eq!(row_s.operator_kind, "Source[events]");
        assert_eq!(row_k.operator_kind, "Sink[output_view]");
    }
}
