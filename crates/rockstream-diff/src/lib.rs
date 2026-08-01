//! DiffCtx differentiation pass for RockStream IVM.
//!
//! Transforms a `PlanNode` logical plan into a `PhysicalPlan` — a flat list of
//! `OpNode` physical operators with input-output edges.
//!
//! ## v0.4 scope
//!
//! - Linear-operator rules for `Filter`, `Project`, `Map`, `Source`,
//!   `ViewSink`, and `Exchange` (stub).
//!
//! ## v0.5 scope
//!
//! - Stateful aggregate rule for `Aggregate` (SUM/COUNT/AVG).
//!
//! ## DBSP linear-operator rule
//!
//! For any linear function `F`, `ΔF(Δx) = F(Δx)`.
//! Filter, Project, and Map are all linear: they are applied unchanged to
//! every incoming delta batch. The differentiation pass therefore emits a
//! physical plan identical in structure to the logical plan for these nodes.
//!
//! ## DBSP aggregate-operator rule
//!
//! `Aggregate` is not linear. Its differentiation requires an arrangement
//! (the `AggState`): for each input delta `(k, v, w)`, the rule is:
//! `Δagg(k) = (new_agg(k), +1) ⊎ (old_agg(k), -1)` when the state changes.

use rockstream_plan::{OpKind, OpNode, PlanNode, WindowFunc, WindowStrategy};
use rockstream_types::{explain::NotMergeSafeReason, ids::OperatorId};

/// Error returned by the differentiation pass.
#[derive(Debug, thiserror::Error)]
pub enum DiffError {
    /// A plan node that is not yet implemented in this version.
    #[error(
        "RS-1014: unsupported plan node '{0}' in v0.5 — this operator arrives in a later version"
    )]
    UnsupportedNode(String),
}

/// The output of the differentiation pass.
#[derive(Debug, Default)]
pub struct PhysicalPlan {
    /// Operators in topological order (sources first, sinks last).
    pub ops: Vec<OpNode>,
}

impl PhysicalPlan {
    /// Return the ID of the last operator in the plan (typically the sink).
    pub fn output_op_id(&self) -> Option<OperatorId> {
        self.ops.last().map(|op| op.id)
    }
}

/// The differentiation pass context.
///
/// Walk a `PlanNode` tree, assign `OperatorId`s, and emit `OpNode`s.
pub struct DiffCtx {
    next_id: u64,
}

impl DiffCtx {
    /// Create a new context, starting operator IDs at 0.
    pub fn new() -> Self {
        DiffCtx { next_id: 0 }
    }

    fn next_op_id(&mut self) -> OperatorId {
        let id = self.next_id;
        self.next_id += 1;
        OperatorId(id)
    }

    /// Differentiate a `PlanNode` tree, returning the `PhysicalPlan`.
    pub fn differentiate(&mut self, plan: &PlanNode) -> Result<PhysicalPlan, DiffError> {
        let mut ops = Vec::new();
        self.diff_node(plan, &mut ops)?;
        Ok(PhysicalPlan { ops })
    }

    /// Recursively differentiate one node and push its `OpNode` to `ops`.
    /// Returns the `OperatorId` of the emitted operator.
    fn diff_node(
        &mut self,
        node: &PlanNode,
        ops: &mut Vec<OpNode>,
    ) -> Result<OperatorId, DiffError> {
        match node {
            // ── Source ────────────────────────────────────────────────────
            PlanNode::Source { name } => {
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Source { name: name.clone() },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![],
                });
                Ok(id)
            }

            // ── Filter (linear) ───────────────────────────────────────────
            // DBSP rule: ΔFilter(Δx) = Filter(Δx)
            PlanNode::Filter { input, .. } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Filter,
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Project (linear) ──────────────────────────────────────────
            // DBSP rule: ΔProject(Δx) = Project(Δx)
            PlanNode::Project { input, .. } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Project,
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Map (linear) ──────────────────────────────────────────────
            // DBSP rule: ΔMap(Δx) = Map(Δx)
            PlanNode::Map { input, .. } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Map,
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── ViewSink ──────────────────────────────────────────────────
            PlanNode::ViewSink {
                view_name,
                pk,
                child,
            } => {
                let input_id = self.diff_node(child, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::ViewSink {
                        view_name: view_name.clone(),
                        pk: pk.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Exchange stub ─────────────────────────────────────────────
            // In v0.4, Exchange is always Loopback: data passes through.
            PlanNode::Exchange { kind, child } => {
                let input_id = self.diff_node(child, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Exchange { kind: *kind },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Aggregate (v0.5) ──────────────────────────────────────────
            // DBSP stateful rule: for each (k, v, w) delta, compute
            // Δagg(k) = (new_state(k), +1) ⊎ (old_state(k), -1) via
            // the AggState arrangement in `AggregateOp`.
            PlanNode::Aggregate { input, .. } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Aggregate,
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── OuterJoin (v0.9 — IVM-5) ─────────────────────────────────
            PlanNode::OuterJoin {
                left,
                right,
                kind,
                left_keys,
                right_keys,
                ..
            } => {
                let left_id = self.diff_node(left, ops)?;
                let right_id = self.diff_node(right, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::OuterJoin {
                        kind: *kind,
                        left_keys: left_keys.clone(),
                        right_keys: right_keys.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![left_id, right_id],
                });
                Ok(id)
            }

            // ── Distinct (v0.10 — IVM-6) ─────────────────────────────────
            PlanNode::Distinct { input, .. } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Distinct,
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Intersect (v0.10 — IVM-6) ────────────────────────────────
            PlanNode::Intersect {
                left, right, all, ..
            } => {
                let left_id = self.diff_node(left, ops)?;
                let right_id = self.diff_node(right, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Intersect { all: *all },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![left_id, right_id],
                });
                Ok(id)
            }

            // ── Except (v0.10 — IVM-6) ───────────────────────────────────
            PlanNode::Except {
                left, right, all, ..
            } => {
                let left_id = self.diff_node(left, ops)?;
                let right_id = self.diff_node(right, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Except { all: *all },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![left_id, right_id],
                });
                Ok(id)
            }

            // ── Window (v0.11 — IVM-7) ────────────────────────────────────────
            PlanNode::Window {
                input,
                window_exprs,
            } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                let strategy = if window_exprs.iter().any(|e| {
                    matches!(
                        e.func,
                        WindowFunc::SlidingSum { .. } | WindowFunc::SlidingAvg { .. }
                    )
                }) {
                    WindowStrategy::SlidingAggregate
                } else {
                    WindowStrategy::PartitionRecompute
                };
                ops.push(OpNode {
                    id,
                    kind: OpKind::Window { strategy },
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::PartitionRecomputation),
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── TumbleWindow (v0.12 — IVM-8) ─────────────────────────────
            PlanNode::TumbleWindow {
                input,
                window_size_ms,
                late_data_policy,
                ..
            } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::TumbleWindow {
                        window_size_ms: *window_size_ms,
                        late_data_policy: late_data_policy.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }
            PlanNode::HopWindow {
                input,
                window_size_ms,
                slide_ms,
                late_data_policy,
                ..
            } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::HopWindow {
                        window_size_ms: *window_size_ms,
                        slide_ms: *slide_ms,
                        late_data_policy: late_data_policy.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }
            PlanNode::SessionWindow {
                input,
                gap_ms,
                late_data_policy,
                ..
            } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::SessionWindow {
                        gap_ms: *gap_ms,
                        late_data_policy: late_data_policy.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── TopK (v0.12 — IVM-9) ─────────────────────────────────────
            PlanNode::TopK {
                input,
                k,
                rank_col,
                partition_by,
            } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::TopK {
                        k: *k,
                        rank_col: *rank_col,
                        partition_by: partition_by.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Lateral (v0.25) ───────────────────────────────────────────
            PlanNode::Lateral { input, func } => {
                let input_id = self.diff_node(input, ops)?;
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Lateral { func: func.clone() },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                Ok(id)
            }

            // ── Snapshot (v0.13) ──────────────────────────────────────────
            PlanNode::Snapshot {
                source_name,
                batch_size,
            } => {
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::Snapshot {
                        source_name: source_name.clone(),
                        batch_size: *batch_size,
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![],
                });
                Ok(id)
            }

            // ── ViewRef (v0.13) ───────────────────────────────────────────
            PlanNode::ViewRef { view_name } => {
                let id = self.next_op_id();
                ops.push(OpNode {
                    id,
                    kind: OpKind::ViewRef {
                        view_name: view_name.clone(),
                    },
                    merge_law: None,
                    not_merge_safe_reason: None,
                    inputs: vec![],
                });
                Ok(id)
            }

            // ── Not yet implemented in v0.5 ───────────────────────────────
            other => Err(DiffError::UnsupportedNode(format!("{other:?}"))),
        }
    }
}

impl Default for DiffCtx {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{BinaryOp, ExchangeKind, Expr, PlanNode};

    fn lit(v: i64) -> Expr {
        Expr::Literal(v.to_be_bytes().to_vec())
    }

    /// Build: Source("t") → Filter(b*2>10) → Project(a, b*2 AS c) → ViewSink("v")
    fn make_plan() -> PlanNode {
        let src = PlanNode::Source { name: "t".into() };
        let filtered = PlanNode::Filter {
            input: Box::new(src),
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(2)),
                }),
                right: Box::new(lit(10)),
            },
        };
        let projected = PlanNode::Project {
            input: Box::new(filtered),
            columns: vec![
                Expr::Column(0),
                Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(2)),
                },
            ],
        };
        PlanNode::ViewSink {
            view_name: "v".into(),
            pk: vec![0],
            child: Box::new(projected),
        }
    }

    #[test]
    fn diff_filter_project_view_sink() {
        let plan = make_plan();
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        // Source(op0) → Filter(op1) → Project(op2) → ViewSink(op3)
        assert_eq!(physical.ops.len(), 4);
        assert!(matches!(physical.ops[0].kind, OpKind::Source { .. }));
        assert!(matches!(physical.ops[1].kind, OpKind::Filter));
        assert!(matches!(physical.ops[2].kind, OpKind::Project));
        assert!(matches!(physical.ops[3].kind, OpKind::ViewSink { .. }));
        // Check input edges
        assert_eq!(physical.ops[1].inputs, vec![OperatorId(0)]);
        assert_eq!(physical.ops[2].inputs, vec![OperatorId(1)]);
        assert_eq!(physical.ops[3].inputs, vec![OperatorId(2)]);
    }

    #[test]
    fn diff_exchange_stub_loopback() {
        let src = PlanNode::Source { name: "t".into() };
        let exchange = PlanNode::Exchange {
            kind: ExchangeKind::Loopback,
            child: Box::new(src),
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&exchange).unwrap();
        assert_eq!(physical.ops.len(), 2);
        assert!(matches!(
            physical.ops[1].kind,
            OpKind::Exchange {
                kind: ExchangeKind::Loopback
            }
        ));
    }

    #[test]
    fn diff_aggregate_emits_aggregate_op() {
        // v0.5: Aggregate is now supported — DiffCtx must emit OpKind::Aggregate.
        use rockstream_plan::AggregateExpr;
        use rockstream_plan::AggregateFunc;
        let aggregate = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source { name: "t".into() }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&aggregate).unwrap();
        // Source(op0) → Aggregate(op1)
        assert_eq!(physical.ops.len(), 2);
        assert!(matches!(physical.ops[0].kind, OpKind::Source { .. }));
        assert!(matches!(physical.ops[1].kind, OpKind::Aggregate));
        assert_eq!(physical.ops[1].inputs, vec![OperatorId(0)]);
    }

    #[test]
    fn diff_tumble_window_emits_tumble_window_op() {
        use rockstream_plan::LateDataPolicy;
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::TumbleWindow {
            input: Box::new(src),
            time_col: 0,
            window_size_ms: 1000,
            late_data_policy: LateDataPolicy::Drop,
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        assert_eq!(physical.ops.len(), 2);
        let tw_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::TumbleWindow { .. }));
        assert!(tw_op.is_some(), "must contain exactly one TumbleWindow op");
        if let OpKind::TumbleWindow { window_size_ms, .. } = &tw_op.unwrap().kind {
            assert_eq!(*window_size_ms, 1000);
        }
    }

    #[test]
    fn diff_hop_window_emits_hop_window_op() {
        use rockstream_plan::LateDataPolicy;
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::HopWindow {
            input: Box::new(src),
            time_col: 0,
            window_size_ms: 1000,
            slide_ms: 250,
            late_data_policy: LateDataPolicy::Drop,
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        let hop_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::HopWindow { .. }));
        assert!(hop_op.is_some(), "must contain exactly one HopWindow op");
        if let OpKind::HopWindow {
            window_size_ms,
            slide_ms,
            ..
        } = &hop_op.unwrap().kind
        {
            assert_eq!((*window_size_ms, *slide_ms), (1000, 250));
        }
    }

    #[test]
    fn diff_session_window_emits_session_window_op() {
        use rockstream_plan::LateDataPolicy;
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::SessionWindow {
            input: Box::new(src),
            time_col: 0,
            gap_ms: 1000,
            late_data_policy: LateDataPolicy::Drop,
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        let session_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::SessionWindow { .. }));
        assert!(
            session_op.is_some(),
            "must contain exactly one SessionWindow op"
        );
        if let OpKind::SessionWindow { gap_ms, .. } = &session_op.unwrap().kind {
            assert_eq!(*gap_ms, 1000);
        }
    }

    #[test]
    fn diff_topk_emits_topk_op() {
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::TopK {
            input: Box::new(src),
            k: 5,
            rank_col: 1,
            partition_by: vec![0],
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        assert_eq!(physical.ops.len(), 2);
        let tk_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::TopK { .. }));
        assert!(tk_op.is_some(), "must contain exactly one TopK op");
        if let OpKind::TopK {
            k,
            rank_col,
            partition_by,
        } = &tk_op.unwrap().kind
        {
            assert_eq!(*k, 5);
            assert_eq!(*rank_col, 1);
            assert_eq!(*partition_by, vec![0]);
        }
    }

    #[test]
    fn diff_lateral_emits_lateral_op() {
        use rockstream_plan::LateralFunc;

        let plan = PlanNode::Lateral {
            input: Box::new(PlanNode::Source {
                name: "docs".into(),
            }),
            func: LateralFunc::Unnest { col: 1 },
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        assert_eq!(physical.ops.len(), 2);
        assert!(matches!(physical.ops[0].kind, OpKind::Source { .. }));
        assert_eq!(
            physical.ops[1].kind,
            OpKind::Lateral {
                func: LateralFunc::Unnest { col: 1 }
            }
        );
        assert_eq!(physical.ops[1].inputs, vec![OperatorId(0)]);
    }

    #[test]
    fn diff_unsupported_node_returns_error() {
        // v0.5: Use a node that is not yet supported (Join).
        let join = PlanNode::Join {
            left: Box::new(PlanNode::Source { name: "a".into() }),
            right: Box::new(PlanNode::Source { name: "b".into() }),
            condition: Expr::Column(0),
        };
        let mut ctx = DiffCtx::new();
        let result = ctx.differentiate(&join);
        assert!(result.is_err());
    }

    #[test]
    fn diff_window_emits_op_with_partition_recompute_reason() {
        use rockstream_plan::{WindowExpr, WindowFunc, WindowStrategy};
        use rockstream_types::explain::NotMergeSafeReason;
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::Window {
            input: Box::new(src),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        let window_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::Window { .. }))
            .expect("Window op must be present");
        assert_eq!(
            window_op.kind,
            OpKind::Window {
                strategy: WindowStrategy::PartitionRecompute
            }
        );
        assert_eq!(
            window_op.not_merge_safe_reason,
            Some(NotMergeSafeReason::PartitionRecomputation)
        );
    }

    #[test]
    fn diff_window_sliding_emits_sliding_aggregate_strategy() {
        use rockstream_plan::{WindowExpr, WindowFunc, WindowStrategy};
        let src = PlanNode::Source { name: "t".into() };
        let plan = PlanNode::Window {
            input: Box::new(src),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::SlidingSum {
                    frame_rows: 3,
                    value_col: 1,
                },
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };
        let mut ctx = DiffCtx::new();
        let physical = ctx.differentiate(&plan).unwrap();
        let window_op = physical
            .ops
            .iter()
            .find(|op| matches!(op.kind, OpKind::Window { .. }))
            .expect("Window op must be present");
        assert_eq!(
            window_op.kind,
            OpKind::Window {
                strategy: WindowStrategy::SlidingAggregate
            }
        );
    }

    #[test]
    fn linear_rule_filter_is_identity_diff() {
        // For a linear operator F, ΔF(Δx) = F(Δx).
        // Verify: the diff of Filter produces exactly one Filter OpNode.
        let src = PlanNode::Source { name: "t".into() };
        let filter = PlanNode::Filter {
            input: Box::new(src),
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(0)),
                right: Box::new(lit(0)),
            },
        };
        let mut ctx = DiffCtx::new();
        let plan = ctx.differentiate(&filter).unwrap();
        let filter_ops: Vec<_> = plan
            .ops
            .iter()
            .filter(|op| matches!(op.kind, OpKind::Filter))
            .collect();
        assert_eq!(filter_ops.len(), 1);
    }
}
