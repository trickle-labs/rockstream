//! `EXPLAIN INCREMENTAL` — plan tree formatter (v0.7).
//!
//! Renders a `PlanNode` tree as an annotated, indented text representation
//! suitable for display in the CLI or an SQL client.  Each line shows the
//! operator kind and, for stateful operators, the merge-law annotation that
//! would be applied at runtime.
//!
//! This is the "plan-level" explain.  The "live-stats" explain (showing
//! actual shard frontiers, throughput, and arrangement sizes) is deferred to
//! v0.9 when the runtime has a running pipeline to query.

use rockstream_plan::{AggregateFunc, PlanNode};

// ─── Public API ──────────────────────────────────────────────────────────────

/// Render a `PlanNode` tree as an `EXPLAIN INCREMENTAL` text block.
///
/// Each node is indented by `depth * 2` spaces.  Stateful nodes are annotated
/// with their merge-law class; stateless nodes are annotated with `⚠ stateless`.
pub fn explain_incremental(plan: &PlanNode) -> String {
    let mut lines = Vec::new();
    render_node(plan, 0, &mut lines);
    let mut out = String::from("EXPLAIN INCREMENTAL\n");
    out.push_str("──────────────────────────────────────────────────────\n");
    for line in &lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

// ─── Internal ────────────────────────────────────────────────────────────────

fn render_node(plan: &PlanNode, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    match plan {
        PlanNode::Source { name } => {
            lines.push(format!("{indent}⚠ Source[{name}]  stateless"));
        }

        PlanNode::Filter { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(format!("{indent}⚠ Filter  stateless"));
        }

        PlanNode::Project { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(format!("{indent}⚠ Project  stateless"));
        }

        PlanNode::Map { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(format!("{indent}⚠ Map  stateless"));
        }

        PlanNode::Aggregate {
            input, aggregates, ..
        } => {
            render_node(input, depth + 1, lines);
            let has_minmax = aggregates
                .iter()
                .any(|a| matches!(a.func, AggregateFunc::Min | AggregateFunc::Max));
            if has_minmax {
                lines.push(format!(
                    "{indent}✗ Aggregate[MinMax]  merge_law=IndexedMultiset/v1  extremum_requires_rmw"
                ));
            } else {
                lines.push(format!("{indent}✓ Aggregate  merge_law=WeightAdd/v1"));
            }
        }

        PlanNode::Exchange { child, kind } => {
            render_node(child, depth + 1, lines);
            lines.push(format!(
                "{indent}  Exchange[{kind:?}]  loopback (single-shard)"
            ));
        }

        PlanNode::ViewSink {
            child, view_name, ..
        } => {
            render_node(child, depth + 1, lines);
            lines.push(format!("{indent}  ViewSink[{view_name}]"));
        }

        // v0.9: outer / semi / anti join.
        PlanNode::OuterJoin {
            left, right, kind, ..
        } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            lines.push(format!(
                "{indent}✓ OuterJoin[{kind:?}]  dual_arrangement+unmatched"
            ));
        }

        // v0.8: inner equi-join.
        PlanNode::InnerJoin { left, right, .. } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            lines.push(format!("{indent}✓ InnerJoin  dual_arrangement"));
        }

        // v0.10: Distinct / Intersect / Except
        PlanNode::Distinct { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(format!(
                "{indent}✓ Distinct  merge_law=WeightAdd/v1  zero_crossing"
            ));
        }

        PlanNode::Intersect { left, right, all, .. } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            let sem = if *all { "ALL" } else { "SET" };
            lines.push(format!(
                "{indent}✓ Intersect[{sem}]  dual_arrangement  min_weight"
            ));
        }

        PlanNode::Except { left, right, all, .. } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            let sem = if *all { "ALL" } else { "SET" };
            lines.push(format!(
                "{indent}✓ Except[{sem}]  dual_arrangement  subtract_weight"
            ));
        }

        other => {
            lines.push(format!("{indent}  {other:?}"));
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, ExchangeKind, Expr, PlanNode};

    #[test]
    fn explain_source_shows_stateless() {
        let plan = PlanNode::Source {
            name: "t".to_string(),
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Source[t]"), "text: {text}");
        assert!(text.contains("stateless"), "text: {text}");
        assert!(text.contains("⚠"), "text: {text}");
    }

    #[test]
    fn explain_aggregate_shows_merge_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Aggregate"), "text: {text}");
        assert!(text.contains("merge_law=WeightAdd/v1"), "text: {text}");
        assert!(text.contains("✓"), "text: {text}");
    }

    #[test]
    fn explain_minmax_shows_not_merge_safe() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Min,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("MinMax"), "text: {text}");
        assert!(text.contains("extremum_requires_rmw"), "text: {text}");
        assert!(text.contains("✗"), "text: {text}");
    }

    #[test]
    fn explain_filter_project_indents_child() {
        let plan = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                predicate: Expr::Column(0),
            }),
            columns: vec![Expr::Column(0)],
        };
        let text = explain_incremental(&plan);
        let lines: Vec<&str> = text.lines().collect();
        // Source is deepest, Filter middle, Project outermost.
        let source_line = lines.iter().find(|l| l.contains("Source")).unwrap();
        let filter_line = lines.iter().find(|l| l.contains("Filter")).unwrap();
        let project_line = lines.iter().find(|l| l.contains("Project")).unwrap();
        // Source has 2 more spaces of indent than Filter.
        assert!(
            source_line.starts_with("    "),
            "source indent: |{source_line}|"
        );
        assert!(
            filter_line.starts_with("  "),
            "filter indent: |{filter_line}|"
        );
        assert!(
            project_line.starts_with("⚠")
                || project_line.starts_with("✓")
                || !project_line.starts_with("  "),
            "project should be at depth 0: |{project_line}|"
        );
    }

    #[test]
    fn explain_distinct_shows_zero_crossing() {
        use rockstream_types::ids::OperatorId;
        let plan = PlanNode::Distinct {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            arr_id: OperatorId(0),
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Distinct"), "text: {text}");
        assert!(text.contains("zero_crossing"), "text: {text}");
        assert!(text.contains("WeightAdd/v1"), "text: {text}");
        assert!(text.contains("✓"), "text: {text}");
    }

    #[test]
    fn explain_intersect_set_shows_semantics() {
        use rockstream_types::ids::OperatorId;
        let plan = PlanNode::Intersect {
            left: Box::new(PlanNode::Source { name: "l".to_string() }),
            right: Box::new(PlanNode::Source { name: "r".to_string() }),
            all: false,
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Intersect[SET]"), "text: {text}");
        assert!(text.contains("min_weight"), "text: {text}");
        assert!(text.contains("✓"), "text: {text}");
    }

    #[test]
    fn explain_except_all_shows_semantics() {
        use rockstream_types::ids::OperatorId;
        let plan = PlanNode::Except {
            left: Box::new(PlanNode::Source { name: "l".to_string() }),
            right: Box::new(PlanNode::Source { name: "r".to_string() }),
            all: true,
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Except[ALL]"), "text: {text}");
        assert!(text.contains("subtract_weight"), "text: {text}");
        assert!(text.contains("✓"), "text: {text}");
    }

    #[test]
    fn explain_exchange_shows_loopback() {
        let plan = PlanNode::Exchange {
            kind: ExchangeKind::Loopback,
            child: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("Exchange"), "text: {text}");
        assert!(text.contains("loopback"), "text: {text}");
    }
}
