//! Distribution pass for RockStream PlanNode trees (v0.7).
//!
//! The distribution pass:
//! 1. Annotates each `PlanNode` with a `partition_key` — the column indices
//!    that the node's output is partitioned by (empty = unpartitioned /
//!    broadcast).
//! 2. Inserts `Exchange { kind: Loopback }` nodes between operators whose
//!    partitioning requirements differ.
//!
//! In the single-shard embedded runtime (v0.7) all exchanges are `Loopback`
//! (zero network calls, zero shuffle objects).  The exchange *is* inserted
//! so that the runtime can later be upgraded to `Hash` or `Direct` without
//! changing the operator graph structure.
//!
//! # Partitioning rules (single-shard)
//!
//! | Operator | Required input partitioning | Output partitioning |
//! |---|---|---|
//! | Source | none | unpartitioned |
//! | Filter | any | same as input |
//! | Project | any | same as input |
//! | Map | any | same as input |
//! | Aggregate | by group-by columns | by group-by columns |
//! | Exchange | n/a | as declared |
//!
//! In v0.7 "partitioned by X" always maps to a single Loopback exchange
//! because there is only one shard.

use rockstream_plan::{ExchangeKind, PlanNode};

/// Annotation produced by the distribution pass for a single node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributionAnnotation {
    /// Column indices the output is partitioned by (empty = unpartitioned).
    pub partition_key: Vec<usize>,
}

impl DistributionAnnotation {
    fn unpartitioned() -> Self {
        Self {
            partition_key: vec![],
        }
    }

    fn by_columns(cols: Vec<usize>) -> Self {
        Self {
            partition_key: cols,
        }
    }
}

/// Apply the distribution pass to a `PlanNode` tree.
///
/// Returns the transformed tree with `Exchange { kind: Loopback }` nodes
/// inserted wherever partitioning requirements change.
pub fn apply_distribution(plan: PlanNode) -> PlanNode {
    let (transformed, _annotation) = distribute(plan);
    transformed
}

/// Internal recursive pass.  Returns (transformed_plan, output_annotation).
fn distribute(plan: PlanNode) -> (PlanNode, DistributionAnnotation) {
    match plan {
        PlanNode::Source { name } => {
            // Sources are unpartitioned at the single-shard level.
            (
                PlanNode::Source { name },
                DistributionAnnotation::unpartitioned(),
            )
        }

        PlanNode::Filter { input, predicate } => {
            let (new_input, ann) = distribute(*input);
            // Filter preserves the input's partitioning.
            (
                PlanNode::Filter {
                    input: Box::new(new_input),
                    predicate,
                },
                ann,
            )
        }

        PlanNode::Project { input, columns } => {
            let (new_input, ann) = distribute(*input);
            // Project preserves partitioning (column indices may shift, but
            // in v0.7 we don't remap them — the pass is single-shard only).
            (
                PlanNode::Project {
                    input: Box::new(new_input),
                    columns,
                },
                ann,
            )
        }

        PlanNode::Map { input, func } => {
            let (new_input, ann) = distribute(*input);
            (
                PlanNode::Map {
                    input: Box::new(new_input),
                    func,
                },
                ann,
            )
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let (new_input, input_ann) = distribute(*input);

            // Aggregate requires data partitioned by the group-by columns.
            // Collect group-by column indices from the lowered expressions.
            let group_cols: Vec<usize> = group_by
                .iter()
                .filter_map(|e| {
                    if let rockstream_plan::Expr::Column(idx) = e {
                        Some(*idx)
                    } else {
                        None
                    }
                })
                .collect();

            // If input is not already partitioned by the group-by columns,
            // insert a Loopback exchange (no-op in single-shard mode).
            let needs_exchange = input_ann.partition_key != group_cols;
            let actual_input = if needs_exchange {
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    child: Box::new(new_input),
                }
            } else {
                new_input
            };

            let out_ann = DistributionAnnotation::by_columns(group_cols);
            (
                PlanNode::Aggregate {
                    input: Box::new(actual_input),
                    group_by,
                    aggregates,
                },
                out_ann,
            )
        }

        // Pass through Exchange nodes unchanged.
        PlanNode::Exchange { kind, child } => {
            let (new_child, _) = distribute(*child);
            (
                PlanNode::Exchange {
                    kind,
                    child: Box::new(new_child),
                },
                DistributionAnnotation::unpartitioned(),
            )
        }

        // ViewSink: preserves input partitioning.
        PlanNode::ViewSink {
            view_name,
            pk,
            child,
        } => {
            let (new_child, ann) = distribute(*child);
            (
                PlanNode::ViewSink {
                    view_name,
                    pk,
                    child: Box::new(new_child),
                },
                ann,
            )
        }

        // OuterJoin (v0.9): distribute children, return unpartitioned annotation.
        PlanNode::OuterJoin {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            unmatched_arr_id,
        } => {
            let (new_left, _) = distribute(*left);
            let (new_right, _) = distribute(*right);
            (
                PlanNode::OuterJoin {
                    kind,
                    left_keys,
                    right_keys,
                    left_arr_id,
                    right_arr_id,
                    unmatched_arr_id,
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                },
                DistributionAnnotation::unpartitioned(),
            )
        }

        // InnerJoin (v0.8): distribute children, return unpartitioned annotation.
        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            semantics,
        } => {
            let (new_left, _) = distribute(*left);
            let (new_right, _) = distribute(*right);
            (
                PlanNode::InnerJoin {
                    left_keys,
                    right_keys,
                    left_arr_id,
                    right_arr_id,
                    semantics,
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                },
                DistributionAnnotation::unpartitioned(),
            )
        }

        // All other nodes pass through unchanged with unpartitioned annotation.
        other => (other, DistributionAnnotation::unpartitioned()),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, ExchangeKind, Expr, PlanNode};

    fn source(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
        }
    }

    fn filter_node(input: PlanNode) -> PlanNode {
        PlanNode::Filter {
            input: Box::new(input),
            predicate: Expr::Column(0),
        }
    }

    fn aggregate_node(input: PlanNode) -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(input),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    #[test]
    fn source_has_no_exchange() {
        let plan = source("t");
        let result = apply_distribution(plan);
        assert!(matches!(result, PlanNode::Source { .. }));
    }

    #[test]
    fn filter_preserves_no_exchange() {
        let plan = filter_node(source("t"));
        let result = apply_distribution(plan.clone());
        // Should be Filter(Source) — no exchange inserted since partitioning
        // is already unpartitioned on both sides.
        assert!(matches!(result, PlanNode::Filter { .. }));
        if let PlanNode::Filter { input, .. } = &result {
            assert!(matches!(input.as_ref(), PlanNode::Source { .. }));
        }
    }

    #[test]
    fn aggregate_inserts_loopback_exchange() {
        // Source is unpartitioned; Aggregate by Column(0) requires hash-partition.
        // In single-shard mode a Loopback exchange is inserted.
        let plan = aggregate_node(source("t"));
        let result = apply_distribution(plan);
        if let PlanNode::Aggregate { input, .. } = &result {
            assert!(
                matches!(
                    input.as_ref(),
                    PlanNode::Exchange {
                        kind: ExchangeKind::Loopback,
                        ..
                    }
                ),
                "expected Loopback exchange before Aggregate, got: {input:?}"
            );
        } else {
            panic!("expected Aggregate, got: {result:?}");
        }
    }

    #[test]
    fn distribution_annotation_by_columns() {
        let ann = DistributionAnnotation::by_columns(vec![0, 1]);
        assert_eq!(ann.partition_key, vec![0, 1]);
    }
}
