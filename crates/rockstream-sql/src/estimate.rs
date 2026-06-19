//! `EXPLAIN INCREMENTAL ESTIMATE` — static cost model (v0.7).
//!
//! Produces a per-operator cost estimate **without deploying** any operators or
//! accessing storage.  The estimate is purely static analysis of the
//! `PlanNode` tree using throughput floors from the Phase 1 exit criteria.
//!
//! # Throughput floors (from NEW_IMPLEMENTATION_PLAN.md Phase 1 exit criteria)
//!
//! | Operator | In-memory rows/s | epoch_ms per 10k-row batch |
//! |---|---|---|
//! | Filter | 5 000 000 | ~2 ms |
//! | Project | 5 000 000 | ~2 ms |
//! | Map | 5 000 000 | ~2 ms |
//! | Aggregate (SUM/COUNT/AVG) | 1 000 000 | ~10 ms |
//! | Aggregate MIN/MAX | 100 000 | ~100 ms |
//! | Source | ∞ (I/O-bound) | 0 |
//!
//! # State size model
//!
//! | Operator | State bytes |
//! |---|---|
//! | Stateless (Filter, Project, Map) | 0 |
//! | Aggregate with SUM/COUNT/AVG | `cardinality * 24` (key 8B + value 16B) |
//! | Aggregate with MIN/MAX | `cardinality * 32` (key 8B + value 16B + sort_key 8B) |
//!
//! `cardinality_hint` is the caller-supplied estimated number of distinct
//! groups.  If not provided, defaults to `1000`.

use rockstream_plan::{AggregateFunc, PlanNode};

// ─── Public types ─────────────────────────────────────────────────────────────

/// A single row in the `EXPLAIN INCREMENTAL ESTIMATE` output.
#[derive(Debug, Clone, PartialEq)]
pub struct EstimateRow {
    /// Operator kind label (e.g. `"Source[t]"`, `"Filter"`, `"Aggregate"`).
    pub operator_kind: String,
    /// Estimated arrangement state in bytes (0 for stateless operators).
    pub predicted_state_bytes: u64,
    /// Estimated time in milliseconds to process one epoch of `batch_rows` rows.
    pub epoch_ms: f64,
}

/// Compute a static cost estimate for the `plan` tree.
///
/// # Arguments
/// - `plan`: the `PlanNode` tree to estimate.
/// - `cardinality_hint`: estimated number of distinct groups for stateful
///   operators (e.g. number of distinct `k` values in `GROUP BY k`).
/// - `batch_rows`: the number of rows per epoch for throughput estimation.
pub fn explain_incremental_estimate(
    plan: &PlanNode,
    cardinality_hint: u64,
    batch_rows: u64,
) -> Vec<EstimateRow> {
    let mut rows = Vec::new();
    estimate_node(plan, cardinality_hint, batch_rows, &mut rows);
    rows
}

/// Format an estimate as a human-readable multi-line string (for the CLI).
pub fn format_estimate(rows: &[EstimateRow]) -> String {
    let mut out = String::new();
    out.push_str("EXPLAIN INCREMENTAL ESTIMATE\n");
    out.push_str("───────────────────────────────────────────────────\n");
    out.push_str("Operator                        state_bytes   epoch_ms\n");
    out.push_str("───────────────────────────────────────────────────\n");
    for row in rows {
        out.push_str(&format!(
            "{:<32} {:>10}   {:>8.2}\n",
            row.operator_kind, row.predicted_state_bytes, row.epoch_ms
        ));
    }
    out.push_str("───────────────────────────────────────────────────\n");
    out.push_str("(estimates only; no operators deployed)\n");
    out
}

// ─── Internal ────────────────────────────────────────────────────────────────

fn estimate_node(
    plan: &PlanNode,
    cardinality_hint: u64,
    batch_rows: u64,
    out: &mut Vec<EstimateRow>,
) {
    match plan {
        PlanNode::Source { name } => {
            out.push(EstimateRow {
                operator_kind: format!("Source[{name}]"),
                predicted_state_bytes: 0,
                epoch_ms: 0.0,
            });
        }

        PlanNode::Filter { input, .. } => {
            estimate_node(input, cardinality_hint, batch_rows, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Filter".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Project { input, .. } => {
            estimate_node(input, cardinality_hint, batch_rows, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Project".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Map { input, .. } => {
            estimate_node(input, cardinality_hint, batch_rows, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Map".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Aggregate {
            input, aggregates, ..
        } => {
            estimate_node(input, cardinality_hint, batch_rows, out);

            // Determine the heaviest aggregate function to set throughput.
            let has_minmax = aggregates
                .iter()
                .any(|a| matches!(a.func, AggregateFunc::Min | AggregateFunc::Max));
            let (throughput_rows_per_s, state_per_group_bytes) = if has_minmax {
                (100_000.0_f64, 32u64) // multiset key(8) + extremum(16) + sort_key(8)
            } else {
                (1_000_000.0_f64, 24u64) // group key(8) + sum+count(16)
            };

            let kind = if has_minmax {
                "Aggregate[MinMax]"
            } else {
                "Aggregate"
            };
            out.push(EstimateRow {
                operator_kind: kind.to_string(),
                predicted_state_bytes: cardinality_hint * state_per_group_bytes,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        // Exchange (Loopback) is nearly free in single-shard mode.
        PlanNode::Exchange { child, kind } => {
            estimate_node(child, cardinality_hint, batch_rows, out);
            out.push(EstimateRow {
                operator_kind: format!("Exchange[{kind:?}]"),
                predicted_state_bytes: 0,
                epoch_ms: 0.0,
            });
        }

        PlanNode::ViewSink {
            child, view_name, ..
        } => {
            estimate_node(child, cardinality_hint, batch_rows, out);
            out.push(EstimateRow {
                operator_kind: format!("ViewSink[{view_name}]"),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, 2_000_000.0),
            });
        }

        // v0.32: IndexArrange (S9) — arrangement state: cardinality * 24 bytes
        // (index_key:8 + pk:8 + row_ptr:8). Throughput: 1M rows/s (same as ViewSink).
        PlanNode::IndexArrange {
            input,
            index_cols,
            pk_cols,
            ..
        } => {
            estimate_node(input, cardinality_hint, batch_rows, out);
            let state_per_row = 24u64; // index_key(8) + pk(8) + row_ptr(8)
            out.push(EstimateRow {
                operator_kind: format!(
                    "IndexArrange[idx={index_cols:?},pk={pk_cols:?}] estimated_index_state_bytes={}",
                    cardinality_hint * state_per_row
                ),
                predicted_state_bytes: cardinality_hint * state_per_row,
                epoch_ms: epoch_ms(batch_rows, 1_000_000.0),
            });
        }

        // All other nodes — pass through without an estimate row.
        _ => {}
    }
}

/// Convert `batch_rows` at `throughput_rows_per_s` to a millisecond estimate.
fn epoch_ms(batch_rows: u64, throughput_rows_per_s: f64) -> f64 {
    (batch_rows as f64 / throughput_rows_per_s) * 1000.0
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};

    fn source(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
        }
    }

    fn filter_plan() -> PlanNode {
        PlanNode::Filter {
            input: Box::new(source("t")),
            predicate: Expr::Column(0),
        }
    }

    fn agg_sum_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(source("t")),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    fn agg_min_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(source("t")),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Min,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    #[test]
    fn estimate_filter_has_zero_state() {
        let rows = explain_incremental_estimate(&filter_plan(), 1000, 10_000);
        let filter_row = rows.iter().find(|r| r.operator_kind == "Filter").unwrap();
        assert_eq!(filter_row.predicted_state_bytes, 0);
    }

    #[test]
    fn estimate_filter_epoch_ms_positive() {
        let rows = explain_incremental_estimate(&filter_plan(), 1000, 10_000);
        let filter_row = rows.iter().find(|r| r.operator_kind == "Filter").unwrap();
        assert!(filter_row.epoch_ms > 0.0, "epoch_ms should be positive");
    }

    #[test]
    fn estimate_aggregate_sum_has_state_bytes() {
        let rows = explain_incremental_estimate(&agg_sum_plan(), 1000, 10_000);
        let agg_row = rows
            .iter()
            .find(|r| r.operator_kind == "Aggregate")
            .unwrap();
        // 1000 groups × 24 bytes = 24 000
        assert_eq!(agg_row.predicted_state_bytes, 24_000);
    }

    #[test]
    fn estimate_aggregate_minmax_has_larger_state_and_slower_epoch() {
        let sum_rows = explain_incremental_estimate(&agg_sum_plan(), 1000, 10_000);
        let min_rows = explain_incremental_estimate(&agg_min_plan(), 1000, 10_000);

        let sum_agg = sum_rows
            .iter()
            .find(|r| r.operator_kind == "Aggregate")
            .unwrap();
        let min_agg = min_rows
            .iter()
            .find(|r| r.operator_kind.contains("MinMax"))
            .unwrap();

        assert!(
            min_agg.predicted_state_bytes > sum_agg.predicted_state_bytes,
            "MinMax state ({}) should exceed SUM state ({})",
            min_agg.predicted_state_bytes,
            sum_agg.predicted_state_bytes
        );
        assert!(
            min_agg.epoch_ms > sum_agg.epoch_ms,
            "MinMax epoch_ms ({}) should exceed SUM epoch_ms ({})",
            min_agg.epoch_ms,
            sum_agg.epoch_ms
        );
    }

    #[test]
    fn no_operators_are_deployed_during_estimate() {
        // The estimate function must be pure — no I/O, no operator creation.
        // We prove this by calling it without any async runtime or storage.
        let plan = agg_sum_plan();
        let rows = explain_incremental_estimate(&plan, 500, 5_000);
        assert!(!rows.is_empty());
        // If any operator were deployed we'd panic or deadlock in a sync context.
    }

    #[test]
    fn format_estimate_contains_state_and_epoch_headers() {
        let rows = explain_incremental_estimate(&filter_plan(), 100, 1000);
        let text = format_estimate(&rows);
        assert!(text.contains("state_bytes"), "output: {text}");
        assert!(text.contains("epoch_ms"), "output: {text}");
        assert!(text.contains("no operators deployed"), "output: {text}");
    }
}
