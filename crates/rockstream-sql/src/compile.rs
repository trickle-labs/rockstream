//! Query-planning operator matrix validation and budget gate (v0.67.1 V0671-01).
//!
//! Validates that stateful operators in a query plan adhere to the supported
//! beyond-RAM operator matrix. When unsupported operators or operator combinations
//! are estimated to exceed the worker memory budget, they are rejected before
//! allocation with RS-5003 (state_budget_exceeded) or RS-4022 (unsupported_large_state_operator).

use rockstream_plan::{AggregateFunc, PlanNode};

use crate::error::SqlError;

/// Classification of an operator for state beyond RAM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorSupportClassification {
    /// Fully supported for state beyond RAM via demand loading and spillable arrangements.
    SupportedBeyondRam,
    /// In-memory bounded operator that requires state to fit within worker budget.
    BoundedToBudget,
    /// Operator fundamentally unsupported for large-state beyond RAM.
    UnsupportedBeyondRam,
}

/// Validate a `PlanNode` against the state beyond RAM support matrix and worker memory budget.
pub fn validate_operator_support(
    plan: &PlanNode,
    estimated_cardinality: u64,
    memory_budget_bytes: u64,
) -> Result<(), SqlError> {
    validate_node_support(plan, estimated_cardinality, memory_budget_bytes)
}

fn validate_node_support(
    plan: &PlanNode,
    cardinality: u64,
    budget_bytes: u64,
) -> Result<(), SqlError> {
    match plan {
        PlanNode::Source { .. } => Ok(()),

        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Map { input, .. } => validate_node_support(input, cardinality, budget_bytes),

        PlanNode::Aggregate {
            input, aggregates, ..
        } => {
            // First validate child
            validate_node_support(input, cardinality, budget_bytes)?;

            // Check if any aggregate function is non-distributive
            for agg in aggregates {
                match agg.func {
                    AggregateFunc::Median
                    | AggregateFunc::ApproxCountDistinct
                    | AggregateFunc::ApproxMembership => {
                        let estimated_bytes = cardinality * 32;
                        if estimated_bytes > budget_bytes {
                            return Err(SqlError::UnsupportedLargeStateOperator {
                                operator: format!("{:?}", agg.func).to_uppercase(),
                                budget_bytes,
                                next_steps: "use distributive aggregates (SUM, COUNT, MIN, MAX, AVG) or bounded windows; non-distributive aggregates beyond RAM are unsupported".to_string(),
                            });
                        }
                    }
                    AggregateFunc::Sum
                    | AggregateFunc::Count
                    | AggregateFunc::Avg
                    | AggregateFunc::Min
                    | AggregateFunc::Max => {
                        // Distributive aggregates are fully supported for state beyond RAM!
                    }
                }
            }
            Ok(())
        }

        PlanNode::InnerJoin { left, right, .. } | PlanNode::Join { left, right, .. } => {
            validate_node_support(left, cardinality, budget_bytes)?;
            validate_node_support(right, cardinality, budget_bytes)?;

            // Estimated join state requires left + right arrangements + intermediate
            let estimated_bytes = cardinality * 64;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "InnerJoin".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget or use bounded windows/watermarks; unbounded hash joins exceeding budget are rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::OuterJoin { left, right, .. } => {
            validate_node_support(left, cardinality, budget_bytes)?;
            validate_node_support(right, cardinality, budget_bytes)?;

            let estimated_bytes = cardinality * 80;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "OuterJoin".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget or use bounded windows/watermarks; unbounded outer joins exceeding budget are rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::Distinct { input, .. } => {
            validate_node_support(input, cardinality, budget_bytes)?;

            let estimated_bytes = cardinality * 32;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "Distinct".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget or use bounded windows; unbounded DISTINCT exceeding budget is rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::Union { left, right } => {
            validate_node_support(left, cardinality, budget_bytes)?;
            validate_node_support(right, cardinality, budget_bytes)
        }

        PlanNode::Intersect { left, right, .. } => {
            validate_node_support(left, cardinality, budget_bytes)?;
            validate_node_support(right, cardinality, budget_bytes)?;

            let estimated_bytes = cardinality * 64;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "Intersect".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget; Intersect exceeding budget is rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::Except { left, right, .. } => {
            validate_node_support(left, cardinality, budget_bytes)?;
            validate_node_support(right, cardinality, budget_bytes)?;

            let estimated_bytes = cardinality * 64;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "Except".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget; Except exceeding budget is rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::Recursion { base, step, .. } => {
            validate_node_support(base, cardinality, budget_bytes)?;
            validate_node_support(step, cardinality, budget_bytes)?;

            let estimated_bytes = cardinality * 64;
            if estimated_bytes > budget_bytes {
                return Err(SqlError::StateBudgetExceeded {
                    operator: "Recursion".to_string(),
                    estimated_bytes,
                    budget_bytes,
                    next_steps: "increase worker operator memory budget; recursive CTE state beyond RAM is rejected before allocation".to_string(),
                });
            }
            Ok(())
        }

        PlanNode::Window { input, .. } => validate_node_support(input, cardinality, budget_bytes),

        PlanNode::TumbleWindow { input, .. }
        | PlanNode::HopWindow { input, .. }
        | PlanNode::SessionWindow { input, .. } => {
            validate_node_support(input, cardinality, budget_bytes)
        }

        PlanNode::TopK { input, .. } => validate_node_support(input, cardinality, budget_bytes),

        PlanNode::ViewSink { child, .. } => validate_node_support(child, cardinality, budget_bytes),

        PlanNode::IndexArrange { input, .. } => {
            validate_node_support(input, cardinality, budget_bytes)
        }

        PlanNode::Exchange { child, .. } => validate_node_support(child, cardinality, budget_bytes),

        PlanNode::Lateral { input, .. } => validate_node_support(input, cardinality, budget_bytes),

        PlanNode::Snapshot { .. } | PlanNode::ViewRef { .. } => Ok(()),
    }
}
