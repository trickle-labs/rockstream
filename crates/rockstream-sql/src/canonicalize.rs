//! Expression and Query Canonicalization for RockStream Shared Arrangements (v0.59.6).
//!
//! Provides deterministic canonicalization of SQL expressions:
//! - Stripping redundant and no-op type casts
//! - Normalizing commutative operator operand orders (e.g. `a + b` vs `b + a`, `x = 1 AND y = 2` vs `y = 2 AND x = 1`)
//! - Projection alias and whitespace normalization
//! - Generating canonical `ArrangementSpec`s that enable query deduplication and physical arrangement sharing.

use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalLiteral, CollationId,
    SourceIdentity,
};

/// Normalizer for expressions and arrangement specifications.
pub struct ExpressionNormalizer;

impl ExpressionNormalizer {
    /// Normalize a canonical expression into its minimal, deterministic form.
    pub fn normalize(expr: &CanonicalExpr) -> CanonicalExpr {
        match expr {
            CanonicalExpr::Column(name) => {
                // Canonicalize column names (lowercase / trimmed)
                CanonicalExpr::Column(name.trim().to_ascii_lowercase())
            }
            CanonicalExpr::Literal(lit) => CanonicalExpr::Literal(lit.clone()),
            CanonicalExpr::Cast { expr, target_type } => {
                let inner = Self::normalize(expr);
                // Strip redundant nested casts to the same type: CAST(CAST(x AS T) AS T) -> CAST(x AS T)
                if let CanonicalExpr::Cast {
                    expr: nested_inner,
                    target_type: nested_target,
                } = &inner
                {
                    if nested_target == target_type {
                        return CanonicalExpr::Cast {
                            expr: nested_inner.clone(),
                            target_type: target_type.clone(),
                        };
                    }
                }
                CanonicalExpr::Cast {
                    expr: Box::new(inner),
                    target_type: target_type.clone(),
                }
            }
            CanonicalExpr::UnaryOp { op, expr } => CanonicalExpr::UnaryOp {
                op: *op,
                expr: Box::new(Self::normalize(expr)),
            },
            CanonicalExpr::BinaryOp { op, left, right } => {
                let norm_left = Self::normalize(left);
                let norm_right = Self::normalize(right);

                // For associative & commutative operators (AND, OR, ADD, MUL), flatten and sort
                if matches!(op, CanonicalBinaryOp::And | CanonicalBinaryOp::Or) {
                    let mut leaves = Vec::new();
                    Self::collect_associative_leaves(*op, &norm_left, &mut leaves);
                    Self::collect_associative_leaves(*op, &norm_right, &mut leaves);
                    leaves.sort();
                    leaves.dedup(); // A AND A -> A for idempotence

                    // Recombine deterministically
                    Self::build_balanced_binary_tree(*op, leaves)
                } else if op.is_commutative() {
                    // Normalize operand order deterministically
                    if norm_left > norm_right {
                        CanonicalExpr::BinaryOp {
                            op: *op,
                            left: Box::new(norm_right),
                            right: Box::new(norm_left),
                        }
                    } else {
                        CanonicalExpr::BinaryOp {
                            op: *op,
                            left: Box::new(norm_left),
                            right: Box::new(norm_right),
                        }
                    }
                } else {
                    CanonicalExpr::BinaryOp {
                        op: *op,
                        left: Box::new(norm_left),
                        right: Box::new(norm_right),
                    }
                }
            }
            CanonicalExpr::FunctionCall { name, args } => {
                let norm_args = args.iter().map(Self::normalize).collect();
                CanonicalExpr::FunctionCall {
                    name: name.trim().to_ascii_lowercase(),
                    args: norm_args,
                }
            }
        }
    }

    fn collect_associative_leaves(
        target_op: CanonicalBinaryOp,
        expr: &CanonicalExpr,
        leaves: &mut Vec<CanonicalExpr>,
    ) {
        if let CanonicalExpr::BinaryOp { op, left, right } = expr {
            if *op == target_op {
                Self::collect_associative_leaves(target_op, left, leaves);
                Self::collect_associative_leaves(target_op, right, leaves);
                return;
            }
        }
        leaves.push(expr.clone());
    }

    fn build_balanced_binary_tree(
        op: CanonicalBinaryOp,
        mut leaves: Vec<CanonicalExpr>,
    ) -> CanonicalExpr {
        if leaves.is_empty() {
            return CanonicalExpr::Literal(CanonicalLiteral::Bool(true));
        }
        if leaves.len() == 1 {
            return leaves.pop().unwrap();
        }

        let mut current_level = leaves;
        while current_level.len() > 1 {
            let mut next_level = Vec::new();
            let mut iter = current_level.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    let binary = if left > right && op.is_commutative() {
                        CanonicalExpr::BinaryOp {
                            op,
                            left: Box::new(right),
                            right: Box::new(left),
                        }
                    } else {
                        CanonicalExpr::BinaryOp {
                            op,
                            left: Box::new(left),
                            right: Box::new(right),
                        }
                    };
                    next_level.push(binary);
                } else {
                    next_level.push(left);
                }
            }
            current_level = next_level;
        }

        current_level.pop().unwrap()
    }

    /// Canonicalize an entire `ArrangementSpec`, normalizing expressions, keys, projections, and predicates.
    pub fn canonicalize_spec(spec: &ArrangementSpec) -> ArrangementSpec {
        let norm_keys = spec.key_expressions.iter().map(Self::normalize).collect();
        let norm_val = spec.value_projection.iter().map(Self::normalize).collect();
        let norm_pred = spec.predicate.as_ref().map(Self::normalize);

        ArrangementSpec {
            tenant_id: spec.tenant_id,
            security_policy_digest: spec.security_policy_digest,
            source_identity: SourceIdentity(spec.source_identity.0.trim().to_ascii_lowercase()),
            source_schema_generation: spec.source_schema_generation,
            key_expressions: norm_keys,
            key_types: spec.key_types.clone(),
            value_projection: norm_val,
            predicate: norm_pred,
            null_semantics: spec.null_semantics,
            decimal_scale: spec.decimal_scale,
            collation_identifier: CollationId(
                spec.collation_identifier.0.trim().to_ascii_lowercase(),
            ),
            collation_version: spec.collation_version,
            time_domain: spec.time_domain.clone(),
            merge_law_id: spec.merge_law_id,
            merge_law_version: spec.merge_law_version,
            partitioning: spec.partitioning.clone(),
        }
    }
}
