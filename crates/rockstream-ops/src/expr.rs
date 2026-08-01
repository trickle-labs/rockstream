//! Expression evaluator for PlanIR `Expr` nodes over Arrow `RecordBatch`.
//!
//! v0.4 supports only `Int64` column operations. Supported expression forms:
//! - `Column(n)` — column reference by index (must be `Int64`)
//! - `Literal(bytes)` — 8-byte big-endian `i64` literal
//! - `BinaryOp { Gt | Lt | Ge | Le | Eq | Ne | Add | Sub | Mul | Div | And | Or, … }`
//!
//! v0.51.4 (Slice 7) adds `Expr::Case` (searched `CASE WHEN ... THEN ...
//! ELSE ... END`) and three specific `Expr::ScalarUdf` names used by Nexmark
//! q14/q21/q22: `regexp_replace`, `split_part`, `length` (and the plain
//! `replace` function q14's `length(extra) - length(replace(extra, 'a',
//! ''))` lowers to). These can produce `Utf8` output, so they are evaluated
//! through `eval_to_array` (the array-preserving entry point); `length` is
//! additionally wired into `eval_i64` since it is used inside integer
//! arithmetic (`length(x) - length(y)`).
//!
//! Returns either a per-row `Vec<i64>` (arithmetic result) or a `Vec<bool>`
//! (comparison / boolean result). Boolean context is used by `FilterOp`;
//! arithmetic context is used by `ProjectOp` and `MapOp`.

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use regex::Regex;
use rockstream_plan::{BinaryOp, Expr};
use std::sync::Arc;

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
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
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
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
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
                        BinaryOp::Mod => {
                            if b != 0 {
                                a % b
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

        // Only `length` is Int64-producing, so only it is wired into i64
        // arithmetic context (needed for `length(x) - length(y)`, q14).
        // `regexp_replace`/`split_part`/`replace` are Utf8-producing and
        // only reachable through `eval_to_array`.
        Expr::ScalarUdf { name, args } if name == "length" => {
            let arr = eval_scalar_udf(name, args, batch)?;
            let int_arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                OpError::column_type_mismatch("Int64", format!("{:?}", arr.data_type()))
            })?;
            Ok((0..int_arr.len()).map(|r| int_arr.value(r)).collect())
        }

        Expr::ScalarUdf { name, .. } => Err(OpError::unimplemented(format!(
            "ScalarUdf `{name}` evaluation in i64 arithmetic context (arrives in a later version)"
        ))),

        Expr::Case { .. } => Err(OpError::unimplemented(
            "Expr::Case evaluation in i64 arithmetic context; use eval_to_array",
        )),
    }
}

/// Evaluate an expression in boolean context.
///
/// Used by `FilterOp` to build the row-selection mask.
pub fn eval_bool(expr: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, OpError> {
    match expr {
        Expr::BinaryOp { op, left, right } => match op {
            BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le => {
                let l = eval_i64(left, batch)?;
                let r = eval_i64(right, batch)?;
                Ok(l.into_iter()
                    .zip(r)
                    .map(|(a, b)| match op {
                        BinaryOp::Gt => a > b,
                        BinaryOp::Lt => a < b,
                        BinaryOp::Ge => a >= b,
                        BinaryOp::Le => a <= b,
                        _ => unreachable!(),
                    })
                    .collect())
            }
            // Eq/Ne must also support Utf8-producing operands (e.g.
            // `regexp_replace(channel, ...) = 'social'`, q14/q21), so these
            // route through the array-preserving `eval_to_array` and compare
            // element-wise on whichever concrete Arrow type both sides
            // evaluate to, rather than always going through the Int64-only
            // `eval_i64` path.
            BinaryOp::Eq | BinaryOp::Ne => {
                let num_rows = batch.num_rows();
                let dt = resolve_operand_pair_type(left, right, batch)?;
                let l = eval_expr_as_type(left, &dt, num_rows, batch)?;
                let r = eval_expr_as_type(right, &dt, num_rows, batch)?;
                eval_eq_ne_arrays(&l, &r, matches!(op, BinaryOp::Ne))
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

/// Evaluate an expression to an Arrow array column, preserving the
/// underlying Arrow type for plain column references.
///
/// v0.51.3 Slice 3: a bare `Expr::Column(i)` (the common `SELECT col FROM
/// t` pass-through shape every gateway view definition uses) returns the
/// input column's array unchanged, so non-`Int64` columns (`Utf8`,
/// `Boolean`, `Float64`) survive a `Filter → Project → ViewSink` pipeline.
///
/// v0.51.4 (Slice 7): `Expr::Case` and the three Nexmark scalar UDFs
/// (`regexp_replace`, `split_part`, `length`; `replace` too) are also
/// evaluated here since they can produce `Utf8` output. Every other
/// expression shape (`Literal`, `BinaryOp`) is still evaluated through the
/// `Int64`-only arithmetic path (`eval_i64`) — no test in the gateway view
/// corpus computes an arithmetic expression over a non-`Int64` column, so
/// widening that path further is not required by this version's Scope.
pub fn eval_to_array(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, OpError> {
    match expr {
        Expr::Column(i) => {
            let n = batch.num_columns();
            if *i >= n {
                return Err(OpError::column_out_of_bounds(*i, n));
            }
            Ok(batch.column(*i).clone())
        }
        Expr::Case {
            when_then,
            else_expr,
        } => eval_case(when_then, else_expr, batch),
        Expr::ScalarUdf { name, args } => eval_scalar_udf(name, args, batch),
        _ => {
            let vals = eval_i64(expr, batch)?;
            Ok(Arc::new(Int64Array::from(vals)))
        }
    }
}

/// Evaluate a searched `CASE WHEN ... THEN ... ELSE ... END` expression
/// (`Expr::Case`) row-wise, picking the first matching branch's value
/// (falling back to `else_expr`), and returning it as a single Arrow array.
///
/// All branches (every `then` and the `else_expr`) must evaluate to the
/// same concrete Arrow type; a mismatch is a genuine internal-consistency
/// error (`OpError::expr_type_mismatch`), not a silently-corrupted array.
/// In-scope Nexmark usage (q14, q21) always has homogeneous `Utf8` branches.
///
/// A bare `Expr::Literal` branch is type-ambiguous on its own (the IR
/// stores literal bytes with no type tag): the target Arrow type is taken
/// from the first branch that is *not* a bare literal (e.g. a `Column` or
/// `ScalarUdf` result, which do carry a concrete Arrow type), and every
/// literal branch is decoded against that type. If every branch (including
/// `else_expr`) is a bare literal, the target type defaults to `Utf8` —
/// every in-scope Nexmark `CASE` (q14, q21) maps to string labels.
fn eval_case(
    when_then: &[(Expr, Expr)],
    else_expr: &Expr,
    batch: &RecordBatch,
) -> Result<ArrayRef, OpError> {
    let num_rows = batch.num_rows();
    let whens = when_then
        .iter()
        .map(|(w, _)| eval_bool(w, batch))
        .collect::<Result<Vec<_>, _>>()?;

    let branch_exprs: Vec<&Expr> = when_then
        .iter()
        .map(|(_, t)| t)
        .chain(std::iter::once(else_expr))
        .collect();
    let dt = branch_exprs
        .iter()
        .find_map(|e| match e {
            Expr::Literal(_) => None,
            other => eval_to_array(other, batch)
                .ok()
                .map(|a| a.data_type().clone()),
        })
        .unwrap_or(DataType::Utf8);

    let thens = when_then
        .iter()
        .map(|(_, t)| eval_expr_as_type(t, &dt, num_rows, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let else_arr = eval_expr_as_type(else_expr, &dt, num_rows, batch)?;

    for t in &thens {
        if t.data_type() != &dt {
            return Err(OpError::expr_type_mismatch(format!(
                "CASE branches have inconsistent Arrow types: {:?} vs {:?}",
                t.data_type(),
                dt
            )));
        }
    }

    match dt {
        DataType::Utf8 => {
            let else_s = downcast_utf8(&else_arr)?;
            let then_s = thens
                .iter()
                .map(downcast_utf8)
                .collect::<Result<Vec<_>, _>>()?;
            let mut out: Vec<String> = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<&str> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        picked = Some(then_s[branch].value(row));
                        break;
                    }
                }
                out.push(picked.unwrap_or_else(|| else_s.value(row)).to_string());
            }
            Ok(Arc::new(StringArray::from(out)))
        }
        DataType::Int64 => {
            let else_i = downcast_int64(&else_arr)?;
            let then_i = thens
                .iter()
                .map(downcast_int64)
                .collect::<Result<Vec<_>, _>>()?;
            let mut out: Vec<i64> = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<i64> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        picked = Some(then_i[branch].value(row));
                        break;
                    }
                }
                out.push(picked.unwrap_or_else(|| else_i.value(row)));
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        other => Err(OpError::expr_type_mismatch(format!(
            "CASE branch Arrow type {other:?} is not supported (only Utf8 and Int64 are)"
        ))),
    }
}

/// Evaluate an expression against a target Arrow type `dt`.
///
/// A bare `Expr::Literal` is type-ambiguous on its own (the IR stores
/// literal bytes with no type tag), so it is decoded according to `dt` and
/// broadcast to a constant array of `num_rows` values; any other expression
/// is evaluated normally via `eval_to_array`, which already produces a
/// concretely-typed array. Used both by `eval_case` (branch values) and by
/// `eval_bool`'s `Eq`/`Ne` arm (comparison operands) — anywhere a bare
/// literal might sit next to a `Utf8`-producing expression like
/// `regexp_replace(...)`.
fn eval_expr_as_type(
    expr: &Expr,
    dt: &DataType,
    num_rows: usize,
    batch: &RecordBatch,
) -> Result<ArrayRef, OpError> {
    match expr {
        Expr::Literal(_) => match dt {
            DataType::Utf8 => {
                let s = literal_utf8(expr)?;
                Ok(Arc::new(StringArray::from(vec![s; num_rows])))
            }
            DataType::Int64 => {
                let v = literal_i64(expr)?;
                Ok(Arc::new(Int64Array::from(vec![v; num_rows])))
            }
            other => Err(OpError::expr_type_mismatch(format!(
                "literal cannot be decoded as unsupported Arrow type {other:?}"
            ))),
        },
        other => eval_to_array(other, batch),
    }
}

/// Determine the target Arrow type for a `(left, right)` pair where either
/// side may be a bare, type-ambiguous `Expr::Literal` — take the type from
/// whichever side is not a bare literal; if both are bare literals, default
/// to `Int64` (the common case: a plain numeric literal-vs-literal
/// comparison, already handled correctly by the pre-existing `eval_i64`
/// path for every other in-scope operator).
fn resolve_operand_pair_type(
    left: &Expr,
    right: &Expr,
    batch: &RecordBatch,
) -> Result<DataType, OpError> {
    match (left, right) {
        (Expr::Literal(_), Expr::Literal(_)) => Ok(DataType::Int64),
        (Expr::Literal(_), other) | (other, Expr::Literal(_)) => {
            Ok(eval_to_array(other, batch)?.data_type().clone())
        }
        (other, _) => Ok(eval_to_array(other, batch)?.data_type().clone()),
    }
}

/// Evaluate the three Nexmark scalar UDFs (`regexp_replace`, `split_part`,
/// `length`; plus the plain `replace` used by q14's
/// `length(extra) - length(replace(extra, 'a', ''))`).
///
/// Any other UDF name keeps returning `OpError::unimplemented` — no
/// speculative general UDF framework, per ground rules.
fn eval_scalar_udf(name: &str, args: &[Expr], batch: &RecordBatch) -> Result<ArrayRef, OpError> {
    match name {
        "length" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let s = downcast_utf8(&arr)?;
            let out: Int64Array = (0..s.len())
                .map(|i| s.value(i).chars().count() as i64)
                .collect();
            Ok(Arc::new(out))
        }
        "regexp_replace" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let s = downcast_utf8(&arr)?;
            let pattern = literal_utf8(arg(args, 1)?)?;
            let replacement = literal_utf8(arg(args, 2)?)?;
            let re = Regex::new(&pattern).map_err(|e| {
                OpError::invalid_literal(format!("invalid regexp_replace pattern {pattern:?}: {e}"))
            })?;
            let out: StringArray = (0..s.len())
                .map(|i| re.replace(s.value(i), replacement.as_str()).into_owned())
                .collect::<Vec<String>>()
                .into();
            Ok(Arc::new(out))
        }
        "replace" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let s = downcast_utf8(&arr)?;
            let from = literal_utf8(arg(args, 1)?)?;
            let to = literal_utf8(arg(args, 2)?)?;
            let out: StringArray = (0..s.len())
                .map(|i| s.value(i).replace(from.as_str(), to.as_str()))
                .collect::<Vec<String>>()
                .into();
            Ok(Arc::new(out))
        }
        "split_part" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let s = downcast_utf8(&arr)?;
            let delim = literal_utf8(arg(args, 1)?)?;
            let idx = literal_i64(arg(args, 2)?)?;
            let out: StringArray = (0..s.len())
                .map(|i| {
                    if idx < 1 {
                        return String::new();
                    }
                    let value = s.value(i);
                    let part = if delim.is_empty() {
                        if idx == 1 {
                            Some(value)
                        } else {
                            None
                        }
                    } else {
                        value.split(delim.as_str()).nth((idx - 1) as usize)
                    };
                    part.unwrap_or("").to_string()
                })
                .collect::<Vec<String>>()
                .into();
            Ok(Arc::new(out))
        }
        other => Err(OpError::unimplemented(format!(
            "ScalarUdf `{other}` evaluation (arrives in a later version)"
        ))),
    }
}

/// Element-wise `Eq`/`Ne` comparison between two already-evaluated arrays,
/// dispatching on whichever concrete Arrow type both sides share (`Int64`
/// or `Utf8`). Used by `eval_bool`'s `BinaryOp::Eq`/`BinaryOp::Ne` arm so
/// string-producing expressions (`regexp_replace(...) = 'social'`) work
/// alongside the pre-existing Int64 comparison path.
fn eval_eq_ne_arrays(l: &ArrayRef, r: &ArrayRef, negate: bool) -> Result<Vec<bool>, OpError> {
    if let (Some(li), Some(ri)) = (
        l.as_any().downcast_ref::<Int64Array>(),
        r.as_any().downcast_ref::<Int64Array>(),
    ) {
        return Ok((0..li.len())
            .map(|i| (li.value(i) == ri.value(i)) != negate)
            .collect());
    }
    if let (Some(ls), Some(rs)) = (
        l.as_any().downcast_ref::<StringArray>(),
        r.as_any().downcast_ref::<StringArray>(),
    ) {
        return Ok((0..ls.len())
            .map(|i| (ls.value(i) == rs.value(i)) != negate)
            .collect());
    }
    Err(OpError::expr_type_mismatch(format!(
        "Eq/Ne comparison between unsupported or mismatched Arrow types: {:?} vs {:?}",
        l.data_type(),
        r.data_type()
    )))
}

fn arg(args: &[Expr], i: usize) -> Result<&Expr, OpError> {
    args.get(i).ok_or_else(|| {
        OpError::expr_type_mismatch(format!("ScalarUdf call missing expected argument {i}"))
    })
}

fn downcast_utf8(arr: &ArrayRef) -> Result<&StringArray, OpError> {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OpError::column_type_mismatch("Utf8", format!("{:?}", arr.data_type())))
}

fn downcast_int64(arr: &ArrayRef) -> Result<&Int64Array, OpError> {
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| OpError::column_type_mismatch("Int64", format!("{:?}", arr.data_type())))
}

/// Extract a raw UTF-8 string literal from an `Expr::Literal` (per
/// `encode_scalar`'s `Utf8` encoding in `rockstream-sql/src/lower.rs`, the
/// bytes are the literal's raw UTF-8 bytes).
fn literal_utf8(expr: &Expr) -> Result<String, OpError> {
    match expr {
        Expr::Literal(bytes) => String::from_utf8(bytes.clone())
            .map_err(|e| OpError::invalid_literal(format!("literal is not valid UTF-8: {e}"))),
        other => Err(OpError::expr_type_mismatch(format!(
            "expected a string literal argument, got {other:?}"
        ))),
    }
}

/// Extract an `i64` literal from an `Expr::Literal` (8-byte big-endian, per
/// `encode_scalar`'s integer encoding).
fn literal_i64(expr: &Expr) -> Result<i64, OpError> {
    match expr {
        Expr::Literal(bytes) if bytes.len() == 8 => {
            Ok(i64::from_be_bytes(bytes[..8].try_into().unwrap()))
        }
        other => Err(OpError::invalid_literal(format!(
            "expected an 8-byte Int64 literal argument, got {other:?}"
        ))),
    }
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
