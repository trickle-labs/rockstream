//! Expression evaluator for PlanIR `Expr` nodes over Arrow `RecordBatch`.
//!
//! v0.4 supports only `Int64` column operations. Supported expression forms:
//! - `Column(n)` — column reference by index (must be `Int64`)
//! - `Literal(bytes)` — 8-byte big-endian `i64` literal
//! - `BinaryOp { Gt | Lt | Ge | Le | Eq | Ne | Add | Sub | Mul | Div | And | Or, … }`
//!
//! Returns either a per-row `Vec<i64>` (arithmetic result) or a `Vec<bool>`
//! (comparison / boolean result). Boolean context is used by `FilterOp`;
//! arithmetic context is used by `ProjectOp` and `MapOp`.

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use rockstream_plan::{BinaryOp, Expr};

use crate::error::OpError;

/// Evaluate an expression in integer (i64) context.
///
/// Returns one `i64` value per row. Calling this on a boolean-producing
/// expression (e.g. `b > 10`) returns `1` for true and `0` for false.
pub fn eval_i64(expr: &Expr, batch: &RecordBatch) -> Result<Vec<i64>, OpError> {
    match expr {
        Expr::Column(i) => {
            let n = batch.num_columns();
            if *i >= n {
                return Err(OpError::column_out_of_bounds(*i, n));
            }
            let col = batch.column(*i);
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    OpError::column_type_mismatch("Int64", format!("{:?}", col.data_type()))
                })?;
            Ok((0..arr.len()).map(|r| arr.value(r)).collect())
        }

        Expr::Literal(bytes) => {
            if bytes.len() != 8 {
                return Err(OpError::invalid_literal(format!(
                    "Int64 literal requires 8 bytes, got {}",
                    bytes.len()
                )));
            }
            let val = i64::from_be_bytes(bytes[..8].try_into().unwrap());
            Ok(vec![val; batch.num_rows()])
        }

        Expr::BinaryOp { op, left, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                let l = eval_i64(left, batch)?;
                let r = eval_i64(right, batch)?;
                Ok(l.into_iter()
                    .zip(r)
                    .map(|(a, b)| match op {
                        BinaryOp::Add => a.saturating_add(b),
                        BinaryOp::Sub => a.saturating_sub(b),
                        BinaryOp::Mul => a.saturating_mul(b),
                        BinaryOp::Div => {
                            if b != 0 {
                                a / b
                            } else {
                                0
                            }
                        }
                        _ => unreachable!(),
                    })
                    .collect())
            }
            // Comparison ops in i64 context: 1 = true, 0 = false
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::Ge
            | BinaryOp::Le
            | BinaryOp::Eq
            | BinaryOp::Ne => {
                let bools = eval_bool(expr, batch)?;
                Ok(bools.into_iter().map(|b| if b { 1 } else { 0 }).collect())
            }
            BinaryOp::And | BinaryOp::Or => {
                let bools = eval_bool(expr, batch)?;
                Ok(bools.into_iter().map(|b| if b { 1 } else { 0 }).collect())
            }
        },

        Expr::ScalarUdf { .. } => {
            Err(OpError::unimplemented("ScalarUdf expression evaluation (arrives v0.26+)"))
        }
    }
}

/// Evaluate an expression in boolean context.
///
/// Used by `FilterOp` to build the row-selection mask.
pub fn eval_bool(expr: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, OpError> {
    match expr {
        Expr::BinaryOp { op, left, right } => match op {
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::Ge
            | BinaryOp::Le
            | BinaryOp::Eq
            | BinaryOp::Ne => {
                let l = eval_i64(left, batch)?;
                let r = eval_i64(right, batch)?;
                Ok(l.into_iter()
                    .zip(r)
                    .map(|(a, b)| match op {
                        BinaryOp::Gt => a > b,
                        BinaryOp::Lt => a < b,
                        BinaryOp::Ge => a >= b,
                        BinaryOp::Le => a <= b,
                        BinaryOp::Eq => a == b,
                        BinaryOp::Ne => a != b,
                        _ => unreachable!(),
                    })
                    .collect())
            }
            BinaryOp::And => {
                let l = eval_bool(left, batch)?;
                let r = eval_bool(right, batch)?;
                Ok(l.into_iter().zip(r).map(|(a, b)| a && b).collect())
            }
            BinaryOp::Or => {
                let l = eval_bool(left, batch)?;
                let r = eval_bool(right, batch)?;
                Ok(l.into_iter().zip(r).map(|(a, b)| a || b).collect())
            }
            _ => Err(OpError::expr_type_mismatch(
                "arithmetic operator used in boolean context; wrap in a comparison",
            )),
        },
        _ => Err(OpError::expr_type_mismatch(
            "expected a comparison or boolean operator at the top of the filter predicate",
        )),
    }
}

/// Evaluate an expression to an Arrow `Int64Array` column.
///
/// Convenience wrapper used by `ProjectOp` to build output columns.
pub fn eval_to_array(
    expr: &Expr,
    batch: &RecordBatch,
) -> Result<std::sync::Arc<dyn arrow::array::Array>, OpError> {
    let vals = eval_i64(expr, batch)?;
    Ok(std::sync::Arc::new(Int64Array::from(vals)))
}

/// Construct a `Literal` expression from an `i64` value.
/// Helper used in tests and plan construction.
pub fn lit(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zset::ArrowZSet;
    use rockstream_plan::BinaryOp;

    fn batch_ab(rows: &[(i64, i64)]) -> RecordBatch {
        ArrowZSet::from_ab_rows(rows, 1).data
    }

    #[test]
    fn eval_column_0() {
        let b = batch_ab(&[(10, 20), (30, 40)]);
        let r = eval_i64(&Expr::Column(0), &b).unwrap();
        assert_eq!(r, vec![10, 30]);
    }

    #[test]
    fn eval_column_1() {
        let b = batch_ab(&[(10, 20), (30, 40)]);
        let r = eval_i64(&Expr::Column(1), &b).unwrap();
        assert_eq!(r, vec![20, 40]);
    }

    #[test]
    fn eval_literal() {
        let b = batch_ab(&[(1, 2), (3, 4)]);
        let r = eval_i64(&lit(7), &b).unwrap();
        assert_eq!(r, vec![7, 7]);
    }

    #[test]
    fn eval_mul() {
        let b = batch_ab(&[(1, 3), (2, 4)]);
        // b * 2 → [6, 8]
        let expr = Expr::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(2)),
        };
        let r = eval_i64(&expr, &b).unwrap();
        assert_eq!(r, vec![6, 8]);
    }

    #[test]
    fn eval_gt_bool() {
        let b = batch_ab(&[(1, 3), (2, 6), (3, 2)]);
        // b * 2 > 10 → b > 5 → [false, true, false]
        let expr = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }),
            right: Box::new(lit(10)),
        };
        let r = eval_bool(&expr, &b).unwrap();
        assert_eq!(r, vec![false, true, false]);
    }

    #[test]
    fn eval_column_out_of_bounds() {
        let b = batch_ab(&[(1, 2)]);
        let r = eval_i64(&Expr::Column(5), &b);
        assert!(r.is_err());
    }

    #[test]
    fn eval_literal_wrong_length() {
        let b = batch_ab(&[(1, 2)]);
        let r = eval_i64(&Expr::Literal(vec![1, 2, 3]), &b);
        assert!(r.is_err());
    }
}
