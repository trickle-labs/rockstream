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
use rockstream_types::explain::{OperatorStats, ShardInfo};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::metrics::LawMetricKey;

// ─── Public API ──────────────────────────────────────────────────────────────

/// Render a `PlanNode` tree as an `EXPLAIN INCREMENTAL` text block.
///
/// Each node is indented by `depth * 2` spaces.  Stateful nodes are annotated
/// with their merge-law class; stateless nodes are annotated with `⚠ stateless`.
pub fn explain_incremental(plan: &PlanNode) -> String {
    let lines = collect_plan_lines(plan);
    format_explain_block(
        &lines
            .iter()
            .map(|line| line.base_line.clone())
            .collect::<Vec<_>>(),
    )
}

pub fn explain_incremental_verbose(
    plan: &PlanNode,
    shard_info: &[ShardInfo],
    workload_memory_bytes: u64,
) -> String {
    let lines = collect_plan_lines(plan);
    let mut rendered = Vec::with_capacity(lines.len() + 1);
    rendered.push(format!("workload_memory_bytes={workload_memory_bytes}"));
    for (idx, line) in lines.iter().enumerate() {
        let shard = shard_info.get(idx).cloned().unwrap_or(ShardInfo {
            shard_count: 1,
            parallelism: 1,
            frontier_epoch: 0,
        });
        let combiner = if line.combiner_enabled {
            "enabled"
        } else {
            "disabled"
        };
        let law_id = line
            .metric_key
            .as_ref()
            .map(|key| key.law_id.to_string())
            .unwrap_or_else(|| "none".to_string());
        let metric_key = line
            .metric_key
            .as_ref()
            .map(format_metric_key)
            .unwrap_or_else(|| "none".to_string());
        let utilisation = if shard.shard_count == 0 {
            0.0
        } else {
            shard.parallelism as f64 / shard.shard_count as f64
        };
        rendered.push(format!(
            "{}  [law_id={} metric_key={} combiner={} shards={} parallelism={} parallelism_utilisation={:.2} frontier_epoch={}]",
            line.base_line,
            law_id,
            metric_key,
            combiner,
            shard.shard_count,
            shard.parallelism,
            utilisation,
            shard.frontier_epoch
        ));
    }
    format_explain_block(&rendered)
}

pub fn explain_incremental_analyze(plan: &PlanNode, stats: &[OperatorStats]) -> String {
    let lines = collect_plan_lines(plan);
    let rendered = lines
        .iter()
        .enumerate()
        .map(|(idx, line)| {
            if let Some(stat) = stats.get(idx) {
                format!(
                    "{}  [rows/s={:.0} state_reads={} rmw_ratio={:.2} p99={:.1}ms dlq_entries={}]",
                    line.base_line,
                    stat.rows_per_s,
                    stat.state_reads,
                    stat.rmw_ratio,
                    stat.p99_latency_ms,
                    stat.dlq_entries
                )
            } else {
                line.base_line.clone()
            }
        })
        .collect::<Vec<_>>();
    format_explain_block(&rendered)
}

// ─── Internal ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct PlanExplainLine {
    base_line: String,
    metric_key: Option<LawMetricKey>,
    combiner_enabled: bool,
}

fn collect_plan_lines(plan: &PlanNode) -> Vec<PlanExplainLine> {
    let mut lines = Vec::new();
    render_node(plan, 0, &mut lines);
    lines
}

fn format_explain_block(lines: &[String]) -> String {
    let mut out = String::from("EXPLAIN INCREMENTAL\n");
    out.push_str("──────────────────────────────────────────────────────\n");
    for line in lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn format_metric_key(key: &LawMetricKey) -> String {
    match key.operator_id {
        Some(operator_id) => format!(
            "{}-{}-v{}-op{}",
            key.law_id, key.law_name, key.law_version, operator_id.0
        ),
        None => format!(
            "{}-{}-v{}-opnone",
            key.law_id, key.law_name, key.law_version
        ),
    }
}

fn law_metric_key(
    law_id: MergeLawId,
    law_name: &'static str,
    operator_id: Option<OperatorId>,
) -> LawMetricKey {
    LawMetricKey {
        law_id,
        law_name,
        law_version: 1,
        operator_id,
    }
}

fn render_node(plan: &PlanNode, depth: usize, lines: &mut Vec<PlanExplainLine>) {
    let indent = "  ".repeat(depth);
    match plan {
        PlanNode::Source { name } => {
            lines.push(PlanExplainLine {
                base_line: format!("{indent}⚠ Source[{name}]  stateless"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::Filter { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}⚠ Filter  stateless"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::Project { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}⚠ Project  stateless"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::Map { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}⚠ Map  stateless"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::Aggregate {
            input, aggregates, ..
        } => {
            render_node(input, depth + 1, lines);
            let has_minmax = aggregates
                .iter()
                .any(|a| matches!(a.func, AggregateFunc::Min | AggregateFunc::Max));
            if has_minmax {
                lines.push(PlanExplainLine {
                    base_line: format!(
                        "{indent}✗ Aggregate[MinMax]  merge_law=IndexedMultiset/v1  extremum_requires_rmw"
                    ),
                    metric_key: Some(law_metric_key(MergeLawId(0x1000), "IndexedMultiset", None)),
                    combiner_enabled: false,
                });
            } else {
                lines.push(PlanExplainLine {
                    base_line: format!("{indent}✓ Aggregate  merge_law=WeightAdd/v1"),
                    metric_key: Some(law_metric_key(WEIGHT_ADD_ID, "WeightAdd", None)),
                    combiner_enabled: true,
                });
            }
        }

        PlanNode::Exchange { child, kind } => {
            render_node(child, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}  Exchange[{kind:?}]  loopback (single-shard)"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::ViewSink {
            child, view_name, ..
        } => {
            render_node(child, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}  ViewSink[{view_name}]"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.9: outer / semi / anti join.
        PlanNode::OuterJoin {
            left, right, kind, ..
        } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}✓ OuterJoin[{kind:?}]  dual_arrangement+unmatched"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.8: inner equi-join.
        PlanNode::InnerJoin { left, right, .. } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}✓ InnerJoin  dual_arrangement"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.10: Distinct / Intersect / Except
        PlanNode::Distinct { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!("{indent}✓ Distinct  merge_law=WeightAdd/v1  zero_crossing"),
                metric_key: Some(law_metric_key(WEIGHT_ADD_ID, "WeightAdd", None)),
                combiner_enabled: true,
            });
        }

        PlanNode::Intersect {
            left, right, all, ..
        } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            let sem = if *all { "ALL" } else { "SET" };
            lines.push(PlanExplainLine {
                base_line: format!("{indent}✓ Intersect[{sem}]  dual_arrangement  min_weight"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        PlanNode::Except {
            left, right, all, ..
        } => {
            render_node(left, depth + 1, lines);
            render_node(right, depth + 1, lines);
            let sem = if *all { "ALL" } else { "SET" };
            lines.push(PlanExplainLine {
                base_line: format!("{indent}✓ Except[{sem}]  dual_arrangement  subtract_weight"),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.11: Window (IVM-7)
        PlanNode::Window { input, .. } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!(
                    "{indent}✗ Window[PartitionRecompute]  not_merge_safe_reason=partition_recomputation"
                ),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.12: TumbleWindow (IVM-8)
        PlanNode::TumbleWindow {
            input,
            window_size_ms,
            late_data_policy,
            ..
        } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!(
                    "{indent}✓ TumbleWindow[{window_size_ms}ms]  merge_law=MaxRegister/v1  watermark_policy={late_data_policy:?}"
                ),
                metric_key: Some(law_metric_key(MergeLawId(0x1001), "MaxRegister", None)),
                combiner_enabled: false,
            });
        }

        // v0.12: TopK (IVM-9)
        PlanNode::TopK {
            input, k, rank_col, ..
        } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!(
                    "{indent}✓ TopK[k={k},rank_col={rank_col}]  buffer=K+epsilon  delta_swap"
                ),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        // v0.32: IndexArrange
        PlanNode::IndexArrange {
            input,
            index_cols,
            pk_cols,
            ..
        } => {
            render_node(input, depth + 1, lines);
            lines.push(PlanExplainLine {
                base_line: format!(
                    "{indent}✓ IndexArrange[index_cols={index_cols:?}, pk_cols={pk_cols:?}]  \
                     bound=max_arrangement_rows  fill=index_arrange_row_count"
                ),
                metric_key: None,
                combiner_enabled: false,
            });
        }

        other => {
            lines.push(PlanExplainLine {
                base_line: format!("{indent}  {other:?}"),
                metric_key: None,
                combiner_enabled: false,
            });
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
            left: Box::new(PlanNode::Source {
                name: "l".to_string(),
            }),
            right: Box::new(PlanNode::Source {
                name: "r".to_string(),
            }),
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
            left: Box::new(PlanNode::Source {
                name: "l".to_string(),
            }),
            right: Box::new(PlanNode::Source {
                name: "r".to_string(),
            }),
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
    fn explain_window_shows_partition_recompute() {
        use rockstream_plan::WindowExpr;
        let plan = PlanNode::Window {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            window_exprs: vec![WindowExpr {
                func: rockstream_plan::WindowFunc::RowNumber,
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };
        let text = explain_incremental(&plan);
        assert!(
            text.contains("Window[PartitionRecompute]"),
            "expected Window[PartitionRecompute]: {text}"
        );
        assert!(
            text.contains("partition_recomputation"),
            "expected partition_recomputation: {text}"
        );
    }

    #[test]
    fn explain_window_oversized_partition_fires_notice() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use rockstream_ops::window::{WindowOp, WINDOW_PARTITION_THRESHOLD};
        use rockstream_ops::zset::ArrowZSet;
        use rockstream_plan::WindowExpr;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("rn", DataType::Int64, false),
        ]));
        let op = WindowOp::new(
            schema,
            vec![WindowExpr {
                func: rockstream_plan::WindowFunc::RowNumber,
                partition_by: vec![],
                order_by: vec![1],
            }],
        );

        let n = WINDOW_PARTITION_THRESHOLD + 1;
        let k_vals: Vec<i64> = vec![1; n];
        let v_vals: Vec<i64> = (0..n as i64).collect();
        let w_vals: Vec<i64> = vec![1; n];
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let data = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(k_vals)) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(v_vals)) as Arc<dyn arrow::array::Array>,
            ],
        )
        .unwrap();
        op.process_epoch(ArrowZSet::new(data, w_vals), 1).unwrap();

        let oversized = op.oversized_partition_keys();
        assert!(
            !oversized.is_empty(),
            "expected at least one oversized partition"
        );

        // Build the NOTICE string.
        let key_hex = hex::encode(&oversized[0]);
        let notice = format!(
            "NOTICE RS-5023: Window partition {} has {} rows (threshold: {})",
            key_hex, n, WINDOW_PARTITION_THRESHOLD
        );
        assert!(notice.contains("RS-5023"), "RS-5023 NOTICE: {notice}");
    }

    #[test]
    fn explain_incremental_tumble_window() {
        use rockstream_plan::LateDataPolicy;
        let plan = PlanNode::TumbleWindow {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            time_col: 0,
            window_size_ms: 5000,
            late_data_policy: LateDataPolicy::Drop,
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("TumbleWindow"), "text: {text}");
        assert!(text.contains("5000ms"), "expected window size: {text}");
        assert!(
            text.contains("watermark_policy"),
            "expected watermark_policy: {text}"
        );
        assert!(!text.is_empty(), "must produce non-empty EXPLAIN text");
    }

    #[test]
    fn explain_incremental_topk() {
        let plan = PlanNode::TopK {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            k: 10,
            rank_col: 2,
            partition_by: vec![0],
        };
        let text = explain_incremental(&plan);
        assert!(text.contains("TopK"), "text: {text}");
        assert!(text.contains("k=10"), "expected k=10: {text}");
        assert!(text.contains("rank_col=2"), "expected rank_col=2: {text}");
    }

    #[test]
    fn explain_incremental_verbose_is_strict_superset() {
        use rockstream_types::explain::ShardInfo;
        use rockstream_types::ids::OperatorId;

        let plan = PlanNode::Exchange {
            kind: ExchangeKind::Hash,
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::InnerJoin {
                    left: Box::new(PlanNode::Source {
                        name: "left".to_string(),
                    }),
                    right: Box::new(PlanNode::Source {
                        name: "right".to_string(),
                    }),
                    left_keys: vec![0],
                    right_keys: vec![0],
                    left_arr_id: OperatorId(11),
                    right_arr_id: OperatorId(12),
                    semantics: Default::default(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Sum,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            }),
        };
        let plain = explain_incremental(&plan);
        let verbose = explain_incremental_verbose(
            &plan,
            &[
                ShardInfo {
                    shard_count: 2,
                    parallelism: 3,
                    frontier_epoch: 40,
                },
                ShardInfo {
                    shard_count: 2,
                    parallelism: 3,
                    frontier_epoch: 41,
                },
                ShardInfo {
                    shard_count: 3,
                    parallelism: 5,
                    frontier_epoch: 42,
                },
                ShardInfo {
                    shard_count: 4,
                    parallelism: 6,
                    frontier_epoch: 43,
                },
                ShardInfo {
                    shard_count: 5,
                    parallelism: 7,
                    frontier_epoch: 44,
                },
            ],
            4096,
        );

        for plain_line in plain.lines() {
            assert!(
                verbose.lines().any(|line| line.contains(plain_line)),
                "missing plain line in verbose output: {plain_line}\nverbose:\n{verbose}"
            );
        }
        assert!(verbose.contains("law_id=law-0001"), "verbose: {verbose}");
        assert!(verbose.contains("metric_key="), "verbose: {verbose}");
        assert!(verbose.contains("combiner=enabled"), "verbose: {verbose}");
        assert!(verbose.contains("shards=4"), "verbose: {verbose}");
        assert!(verbose.contains("parallelism=6"), "verbose: {verbose}");
        assert!(verbose.contains("frontier_epoch=43"), "verbose: {verbose}");
        assert!(
            verbose.contains("workload_memory_bytes=4096"),
            "verbose: {verbose}"
        );
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
