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
//! ## DBSP linear-operator rule
//!
//! For any linear function `F`, `ΔF(Δx) = F(Δx)`.
//! Filter, Project, and Map are all linear: they are applied unchanged to
//! every incoming delta batch. The differentiation pass therefore emits a
//! physical plan identical in structure to the logical plan for these nodes.

use rockstream_plan::{OpKind, OpNode, PlanNode};
use rockstream_types::ids::OperatorId;

/// Error returned by the differentiation pass.
#[derive(Debug, thiserror::Error)]
pub enum DiffError {
    /// A plan node that is not yet implemented in this version.
    #[error("RS-1014: unsupported plan node '{0}' in v0.4 — this operator arrives in a later version")]
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
            PlanNode::ViewSink { view_name, pk, child } => {
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

            // ── Not yet implemented in v0.4 ───────────────────────────────
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
            columns: vec![Expr::Column(0), Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }],
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
        assert!(matches!(physical.ops[1].kind, OpKind::Exchange { kind: ExchangeKind::Loopback }));
    }

    #[test]
    fn diff_unsupported_node_returns_error() {
        let aggregate = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source { name: "t".into() }),
            group_by: vec![],
            aggregates: vec![],
        };
        let mut ctx = DiffCtx::new();
        let result = ctx.differentiate(&aggregate);
        assert!(result.is_err());
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

