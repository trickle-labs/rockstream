//! Expression evaluator for PlanIR `Expr` nodes over Arrow `RecordBatch`.
//!
//! Evaluates expressions in arithmetic (`eval_i64`), boolean (`eval_bool`),
//! and Arrow array (`eval_to_array`) contexts.
//!
//! Supports typed, null-preserving common scalar functions across string,
//! null-handling, and date/time categories (v0.59.11 SQL-04):
//! - String: UPPER, LOWER, LENGTH/CHAR_LENGTH/CHARACTER_LENGTH, SUBSTRING/SUBSTR,
//!   TRIM/LTRIM/RTRIM/BTRIM, CONCAT, CONCAT_WS, REPLACE, REGEXP_REPLACE,
//!   SPLIT_PART, LPAD, RPAD, POSITION/STRPOS.
//! - Null-handling: COALESCE, NULLIF, CASE WHEN ... THEN ... ELSE ... END.
//! - Date/Time: DATE_TRUNC, EXTRACT/DATE_PART, AGE, TO_CHAR, NOW/CURRENT_TIMESTAMP,
//!   CURRENT_DATE, and timestamp interval arithmetic (+, -).

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Float64Array,
    Float64Builder, Int32Array, Int64Array, Int64Builder, NullArray, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use regex::Regex;
use rockstream_plan::{BinaryOp, Expr, CASE_MISSING_ELSE_SENTINEL};
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
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                Ok((0..arr.len())
                    .map(|r| if arr.is_null(r) { 0 } else { arr.value(r) })
                    .collect())
            } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                Ok((0..arr.len())
                    .map(|r| {
                        if arr.is_null(r) {
                            0
                        } else {
                            arr.value(r) as i64
                        }
                    })
                    .collect())
            } else {
                Err(OpError::column_type_mismatch(
                    "Int64",
                    format!("{:?}", col.data_type()),
                ))
            }
        }

        Expr::Literal(bytes) => {
            if bytes.is_empty() {
                Ok(vec![0; batch.num_rows()])
            } else if bytes.len() == 8 {
                let val = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                Ok(vec![val; batch.num_rows()])
            } else if bytes.len() == 4 {
                let val = i32::from_be_bytes(bytes[..4].try_into().unwrap()) as i64;
                Ok(vec![val; batch.num_rows()])
            } else {
                Err(OpError::invalid_literal(format!(
                    "Int64 literal requires 8 bytes, got {}",
                    bytes.len()
                )))
            }
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
                        BinaryOp::Div if b != 0 => a / b,
                        BinaryOp::Mod if b != 0 => a % b,
                        _ => 0,
                    })
                    .collect())
            }
            BinaryOp::Gt
            | BinaryOp::Lt
            | BinaryOp::Ge
            | BinaryOp::Le
            | BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::And
            | BinaryOp::Or => {
                let bools = eval_bool(expr, batch)?;
                Ok(bools.into_iter().map(|b| if b { 1 } else { 0 }).collect())
            }
        },

        Expr::ScalarUdf { name, args } => {
            let arr = eval_scalar_udf(name, args, batch)?;
            if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                Ok((0..int_arr.len())
                    .map(|r| {
                        if int_arr.is_null(r) {
                            0
                        } else {
                            int_arr.value(r)
                        }
                    })
                    .collect())
            } else if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
                Ok((0..int_arr.len())
                    .map(|r| {
                        if int_arr.is_null(r) {
                            0
                        } else {
                            int_arr.value(r) as i64
                        }
                    })
                    .collect())
            } else {
                Err(OpError::column_type_mismatch(
                    "Int64",
                    format!("{:?}", arr.data_type()),
                ))
            }
        }

        Expr::Case { .. } => {
            let arr = eval_to_array(expr, batch)?;
            if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                Ok((0..int_arr.len())
                    .map(|r| {
                        if int_arr.is_null(r) {
                            0
                        } else {
                            int_arr.value(r)
                        }
                    })
                    .collect())
            } else {
                Err(OpError::unimplemented(
                    "Expr::Case evaluation in non-Int64 arithmetic context",
                ))
            }
        }
    }
}

/// Evaluate an expression in boolean context.
///
/// Used by `FilterOp` to build the row-selection mask.
pub fn eval_bool(expr: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, OpError> {
    match expr {
        Expr::BinaryOp { op, left, right } => match op {
            BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le => {
                let num_rows = batch.num_rows();
                let dt = resolve_operand_pair_type(left, right, batch)?;
                let l = eval_expr_as_type(left, &dt, num_rows, batch)?;
                let r = eval_expr_as_type(right, &dt, num_rows, batch)?;
                eval_cmp_arrays(&l, &r, *op)
            }
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
/// underlying Arrow type and null bitmasks.
pub fn eval_to_array(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, OpError> {
    match expr {
        Expr::Column(i) => {
            let n = batch.num_columns();
            if *i >= n {
                return Err(OpError::column_out_of_bounds(*i, n));
            }
            Ok(batch.column(*i).clone())
        }
        Expr::Literal(bytes) => {
            let num_rows = batch.num_rows();
            if bytes.len() == 8 {
                let val = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                Ok(Arc::new(Int64Array::from(vec![val; num_rows])))
            } else if let Ok(s) = String::from_utf8(bytes.clone()) {
                Ok(Arc::new(StringArray::from(vec![s; num_rows])))
            } else if bytes.len() == 1 {
                let val = bytes[0] != 0;
                Ok(Arc::new(BooleanArray::from(vec![val; num_rows])))
            } else {
                Ok(Arc::new(NullArray::new(num_rows)))
            }
        }
        Expr::Case {
            when_then,
            else_expr,
        } => eval_case(when_then, else_expr, batch),
        Expr::ScalarUdf { name, args } => eval_scalar_udf(name, args, batch),
        Expr::BinaryOp { op, left, right } => eval_binary_op_to_array(*op, left, right, batch),
    }
}

/// Evaluate binary arithmetic or comparison operator to Arrow array.
fn eval_binary_op_to_array(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    batch: &RecordBatch,
) -> Result<ArrayRef, OpError> {
    let num_rows = batch.num_rows();
    match op {
        BinaryOp::Gt
        | BinaryOp::Lt
        | BinaryOp::Ge
        | BinaryOp::Le
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::And
        | BinaryOp::Or => {
            let expr = Expr::BinaryOp {
                op,
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            };
            let bools = eval_bool(&expr, batch)?;
            Ok(Arc::new(BooleanArray::from(bools)))
        }
        BinaryOp::Add | BinaryOp::Sub => {
            let l_arr = eval_to_array(left, batch)?;
            let r_arr = eval_to_array(right, batch)?;
            // Check if left or right is Timestamp
            if let Some(ts_arr) = l_arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                for r in 0..num_rows {
                    if ts_arr.is_null(r) || r_arr.is_null(r) {
                        builder.append_null();
                    } else {
                        let base = ts_arr.value(r);
                        let offset_micros = extract_duration_micros(&r_arr, r)?;
                        let val = match op {
                            BinaryOp::Add => base.saturating_add(offset_micros),
                            BinaryOp::Sub => base.saturating_sub(offset_micros),
                            _ => base,
                        };
                        builder.append_value(val);
                    }
                }
                Ok(Arc::new(builder.finish()))
            } else if let Some(ts_arr) = r_arr.as_any().downcast_ref::<TimestampMicrosecondArray>()
            {
                if op == BinaryOp::Add {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        if ts_arr.is_null(r) || l_arr.is_null(r) {
                            builder.append_null();
                        } else {
                            let base = ts_arr.value(r);
                            let offset_micros = extract_duration_micros(&l_arr, r)?;
                            builder.append_value(base.saturating_add(offset_micros));
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                } else {
                    Err(OpError::unimplemented(
                        "interval - timestamp is not supported",
                    ))
                }
            } else if l_arr.data_type() == &DataType::Float64
                || r_arr.data_type() == &DataType::Float64
            {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for r in 0..num_rows {
                    if l_arr.is_null(r) || r_arr.is_null(r) {
                        builder.append_null();
                    } else {
                        let lv = extract_f64(&l_arr, r)?;
                        let rv = extract_f64(&r_arr, r)?;
                        let res = match op {
                            BinaryOp::Add => lv + rv,
                            BinaryOp::Sub => lv - rv,
                            _ => 0.0,
                        };
                        builder.append_value(res);
                    }
                }
                Ok(Arc::new(builder.finish()))
            } else {
                let expr = Expr::BinaryOp {
                    op,
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                };
                let vals = eval_i64(&expr, batch)?;
                Ok(Arc::new(Int64Array::from(vals)))
            }
        }
        BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            let l_arr = eval_to_array(left, batch)?;
            let r_arr = eval_to_array(right, batch)?;
            if l_arr.data_type() == &DataType::Float64 || r_arr.data_type() == &DataType::Float64 {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for r in 0..num_rows {
                    if l_arr.is_null(r) || r_arr.is_null(r) {
                        builder.append_null();
                    } else {
                        let lv = extract_f64(&l_arr, r)?;
                        let rv = extract_f64(&r_arr, r)?;
                        let res = match op {
                            BinaryOp::Mul => lv * rv,
                            BinaryOp::Div if rv != 0.0 => lv / rv,
                            _ => 0.0,
                        };
                        builder.append_value(res);
                    }
                }
                Ok(Arc::new(builder.finish()))
            } else {
                let expr = Expr::BinaryOp {
                    op,
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                };
                let vals = eval_i64(&expr, batch)?;
                Ok(Arc::new(Int64Array::from(vals)))
            }
        }
    }
}

/// Evaluate a searched `CASE WHEN ... THEN ... ELSE ... END` expression
/// (`Expr::Case`) row-wise, picking the first matching branch's value.
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
            Expr::Literal(bytes) if bytes.is_empty() => None,
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

    match dt {
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for row in 0..num_rows {
                let mut picked: Option<Option<&str>> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        if thens[branch].is_null(row) {
                            picked = Some(None);
                        } else {
                            picked = Some(Some(downcast_utf8(&thens[branch])?.value(row)));
                        }
                        break;
                    }
                }
                match picked {
                    Some(Some(s)) => builder.append_value(s),
                    Some(None) => builder.append_null(),
                    None => {
                        if else_arr.is_null(row) {
                            builder.append_null();
                        } else {
                            builder.append_value(downcast_utf8(&else_arr)?.value(row));
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<Option<i64>> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        if thens[branch].is_null(row) {
                            picked = Some(None);
                        } else {
                            picked = Some(Some(downcast_int64(&thens[branch])?.value(row)));
                        }
                        break;
                    }
                }
                match picked {
                    Some(Some(v)) => {
                        if v == CASE_MISSING_ELSE_SENTINEL {
                            builder.append_null();
                        } else {
                            builder.append_value(v);
                        }
                    }
                    Some(None) => builder.append_null(),
                    None => {
                        if else_arr.is_null(row) {
                            builder.append_null();
                        } else {
                            let v = downcast_int64(&else_arr)?.value(row);
                            if v == CASE_MISSING_ELSE_SENTINEL {
                                builder.append_null();
                            } else {
                                builder.append_value(v);
                            }
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<Option<f64>> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        if thens[branch].is_null(row) {
                            picked = Some(None);
                        } else {
                            picked = Some(Some(extract_f64(&thens[branch], row)?));
                        }
                        break;
                    }
                }
                match picked {
                    Some(Some(v)) => builder.append_value(v),
                    Some(None) => builder.append_null(),
                    None => {
                        if else_arr.is_null(row) {
                            builder.append_null();
                        } else {
                            builder.append_value(extract_f64(&else_arr, row)?);
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<Option<bool>> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        if thens[branch].is_null(row) {
                            picked = Some(None);
                        } else {
                            let b_arr = thens[branch]
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap();
                            picked = Some(Some(b_arr.value(row)));
                        }
                        break;
                    }
                }
                match picked {
                    Some(Some(v)) => builder.append_value(v),
                    Some(None) => builder.append_null(),
                    None => {
                        if else_arr.is_null(row) {
                            builder.append_null();
                        } else {
                            let b_arr = else_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                            builder.append_value(b_arr.value(row));
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for row in 0..num_rows {
                let mut picked: Option<Option<i64>> = None;
                for (branch, w) in whens.iter().enumerate() {
                    if w[row] {
                        if thens[branch].is_null(row) {
                            picked = Some(None);
                        } else {
                            picked = Some(Some(
                                extract_timestamp_micros(&thens[branch], row)?.unwrap_or(0),
                            ));
                        }
                        break;
                    }
                }
                match picked {
                    Some(Some(v)) => builder.append_value(v),
                    Some(None) => builder.append_null(),
                    None => {
                        if else_arr.is_null(row) {
                            builder.append_null();
                        } else if let Some(v) = extract_timestamp_micros(&else_arr, row)? {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(OpError::expr_type_mismatch(format!(
            "CASE branch Arrow type {other:?} is not supported"
        ))),
    }
}

/// Evaluate an expression against a target Arrow type `dt`.
fn eval_expr_as_type(
    expr: &Expr,
    dt: &DataType,
    num_rows: usize,
    batch: &RecordBatch,
) -> Result<ArrayRef, OpError> {
    match expr {
        Expr::Literal(bytes) => {
            if bytes.is_empty() {
                match dt {
                    DataType::Utf8 => {
                        let mut b = StringBuilder::with_capacity(num_rows, 0);
                        for _ in 0..num_rows {
                            b.append_null();
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    DataType::Int64 => {
                        let mut b = Int64Builder::with_capacity(num_rows);
                        for _ in 0..num_rows {
                            b.append_null();
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    DataType::Float64 => {
                        let mut b = Float64Builder::with_capacity(num_rows);
                        for _ in 0..num_rows {
                            b.append_null();
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    DataType::Boolean => {
                        let mut b = BooleanBuilder::with_capacity(num_rows);
                        for _ in 0..num_rows {
                            b.append_null();
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        let mut b = TimestampMicrosecondBuilder::with_capacity(num_rows);
                        for _ in 0..num_rows {
                            b.append_null();
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    _ => Ok(Arc::new(NullArray::new(num_rows))),
                }
            } else {
                match dt {
                    DataType::Utf8 => {
                        let s = literal_utf8(expr)?;
                        Ok(Arc::new(StringArray::from(vec![s; num_rows])))
                    }
                    DataType::Int64 => {
                        let v = literal_i64(expr)?;
                        Ok(Arc::new(Int64Array::from(vec![v; num_rows])))
                    }
                    DataType::Float64 => {
                        if bytes.len() == 8 {
                            let bits = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                            let val = f64::from_bits(bits);
                            Ok(Arc::new(Float64Array::from(vec![val; num_rows])))
                        } else {
                            let val = literal_i64(expr)? as f64;
                            Ok(Arc::new(Float64Array::from(vec![val; num_rows])))
                        }
                    }
                    DataType::Boolean => {
                        let val = bytes[0] != 0;
                        Ok(Arc::new(BooleanArray::from(vec![val; num_rows])))
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        let val = literal_i64(expr)?;
                        let mut b = TimestampMicrosecondBuilder::with_capacity(num_rows);
                        for _ in 0..num_rows {
                            b.append_value(val);
                        }
                        Ok(Arc::new(b.finish()))
                    }
                    other => Err(OpError::expr_type_mismatch(format!(
                        "literal cannot be decoded as unsupported Arrow type {other:?}"
                    ))),
                }
            }
        }
        other => eval_to_array(other, batch),
    }
}

/// Determine the target Arrow type for a `(left, right)` pair.
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

/// Evaluate the supported scalar UDFs with null preservation.
fn eval_scalar_udf(name: &str, args: &[Expr], batch: &RecordBatch) -> Result<ArrayRef, OpError> {
    match name {
        "cast_int64" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            arrow::compute::cast(&arr, &DataType::Int64).map_err(OpError::arrow)
        }

        "upper" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                if let Some(s) = extract_string(&arr, r)? {
                    builder.append_value(s.to_uppercase());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "lower" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                if let Some(s) = extract_string(&arr, r)? {
                    builder.append_value(s.to_lowercase());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "length" | "char_length" | "character_length" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = Int64Builder::with_capacity(num_rows);
            for r in 0..num_rows {
                if let Some(s) = extract_string(&arr, r)? {
                    builder.append_value(s.chars().count() as i64);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "substring" | "substr" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let start_arr = eval_to_array(arg(args, 1)?, batch)?;
            let len_arr = if args.len() > 2 {
                Some(eval_to_array(arg(args, 2)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let str_val = extract_string(&str_arr, r)?;
                let start_val = extract_i64(&start_arr, r)?;
                let len_val = match &len_arr {
                    Some(l) => extract_i64(l, r)?,
                    None => None,
                };
                if str_val.is_none()
                    || start_val.is_none()
                    || (len_arr.is_some() && len_val.is_none())
                {
                    builder.append_null();
                    continue;
                }
                let s = str_val.unwrap();
                let start = start_val.unwrap();
                if let Some(count) = len_val {
                    if count < 0 {
                        return Err(OpError::invalid_literal(
                            "negative substring length not allowed",
                        ));
                    }
                    let chars: Vec<char> = s.chars().collect();
                    let end = start.saturating_add(count);
                    let mut res = String::new();
                    for (idx_0, ch) in chars.into_iter().enumerate() {
                        let idx_1 = (idx_0 + 1) as i64;
                        if idx_1 >= start && idx_1 < end {
                            res.push(ch);
                        }
                    }
                    builder.append_value(&res);
                } else {
                    let chars: Vec<char> = s.chars().collect();
                    let mut res = String::new();
                    for (idx_0, ch) in chars.into_iter().enumerate() {
                        let idx_1 = (idx_0 + 1) as i64;
                        if idx_1 >= start {
                            res.push(ch);
                        }
                    }
                    builder.append_value(&res);
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "trim" | "btrim" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let chars_arr = if args.len() > 1 {
                Some(eval_to_array(arg(args, 1)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let chars_opt = match &chars_arr {
                    Some(c) => extract_string(c, r)?,
                    None => None,
                };
                if s_opt.is_none() || (chars_arr.is_some() && chars_opt.is_none()) {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                if let Some(trim_chars) = chars_opt {
                    let trim_set: std::collections::HashSet<char> = trim_chars.chars().collect();
                    let res = s.trim_matches(|c| trim_set.contains(&c));
                    builder.append_value(res);
                } else {
                    builder.append_value(s.trim());
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "ltrim" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let chars_arr = if args.len() > 1 {
                Some(eval_to_array(arg(args, 1)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let chars_opt = match &chars_arr {
                    Some(c) => extract_string(c, r)?,
                    None => None,
                };
                if s_opt.is_none() || (chars_arr.is_some() && chars_opt.is_none()) {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                if let Some(trim_chars) = chars_opt {
                    let trim_set: std::collections::HashSet<char> = trim_chars.chars().collect();
                    let res = s.trim_start_matches(|c| trim_set.contains(&c));
                    builder.append_value(res);
                } else {
                    builder.append_value(s.trim_start());
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "rtrim" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let chars_arr = if args.len() > 1 {
                Some(eval_to_array(arg(args, 1)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let chars_opt = match &chars_arr {
                    Some(c) => extract_string(c, r)?,
                    None => None,
                };
                if s_opt.is_none() || (chars_arr.is_some() && chars_opt.is_none()) {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                if let Some(trim_chars) = chars_opt {
                    let trim_set: std::collections::HashSet<char> = trim_chars.chars().collect();
                    let res = s.trim_end_matches(|c| trim_set.contains(&c));
                    builder.append_value(res);
                } else {
                    builder.append_value(s.trim_end());
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "concat" => {
            let arrs = args
                .iter()
                .map(|a| eval_to_array(a, batch))
                .collect::<Result<Vec<_>, _>>()?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let mut row_res = String::new();
                for arr in &arrs {
                    if let Some(s) = extract_string_any(arr, r)? {
                        row_res.push_str(&s);
                    }
                }
                builder.append_value(&row_res);
            }
            Ok(Arc::new(builder.finish()))
        }

        "concat_ws" => {
            let sep_arr = eval_to_array(arg(args, 0)?, batch)?;
            let item_arrs = args[1..]
                .iter()
                .map(|a| eval_to_array(a, batch))
                .collect::<Result<Vec<_>, _>>()?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                if let Some(sep) = extract_string(&sep_arr, r)? {
                    let mut parts = Vec::new();
                    for arr in &item_arrs {
                        if let Some(s) = extract_string_any(arr, r)? {
                            parts.push(s);
                        }
                    }
                    builder.append_value(parts.join(sep));
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "regexp_replace" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let pattern_arr = eval_to_array(arg(args, 1)?, batch)?;
            let repl_arr = eval_to_array(arg(args, 2)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&arr, r)?;
                let pat_opt = extract_string(&pattern_arr, r)?;
                let rep_opt = extract_string(&repl_arr, r)?;
                if s_opt.is_none() || pat_opt.is_none() || rep_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                let pat = pat_opt.unwrap();
                let rep = rep_opt.unwrap();
                let re = Regex::new(pat).map_err(|e| {
                    OpError::invalid_literal(format!("invalid regexp_replace pattern {pat:?}: {e}"))
                })?;
                builder.append_value(re.replace(s, rep).as_ref());
            }
            Ok(Arc::new(builder.finish()))
        }

        "replace" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let from_arr = eval_to_array(arg(args, 1)?, batch)?;
            let to_arr = eval_to_array(arg(args, 2)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&arr, r)?;
                let from_opt = extract_string(&from_arr, r)?;
                let to_opt = extract_string(&to_arr, r)?;
                if s_opt.is_none() || from_opt.is_none() || to_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                builder.append_value(s_opt.unwrap().replace(from_opt.unwrap(), to_opt.unwrap()));
            }
            Ok(Arc::new(builder.finish()))
        }

        "split_part" => {
            let arr = eval_to_array(arg(args, 0)?, batch)?;
            let delim_arr = eval_to_array(arg(args, 1)?, batch)?;
            let idx_arr = eval_to_array(arg(args, 2)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&arr, r)?;
                let delim_opt = extract_string(&delim_arr, r)?;
                let idx_opt = extract_i64(&idx_arr, r)?;
                if s_opt.is_none() || delim_opt.is_none() || idx_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                let delim = delim_opt.unwrap();
                let idx = idx_opt.unwrap();
                if idx < 1 {
                    builder.append_value("");
                    continue;
                }
                let part = if delim.is_empty() {
                    if idx == 1 {
                        Some(s)
                    } else {
                        None
                    }
                } else {
                    s.split(delim).nth((idx - 1) as usize)
                };
                builder.append_value(part.unwrap_or(""));
            }
            Ok(Arc::new(builder.finish()))
        }

        "lpad" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let len_arr = eval_to_array(arg(args, 1)?, batch)?;
            let pad_arr = if args.len() > 2 {
                Some(eval_to_array(arg(args, 2)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let len_opt = extract_i64(&len_arr, r)?;
                let pad_opt = match &pad_arr {
                    Some(p) => extract_string(p, r)?,
                    None => None,
                };
                if s_opt.is_none() || len_opt.is_none() || (pad_arr.is_some() && pad_opt.is_none())
                {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                let len = len_opt.unwrap();
                if len <= 0 {
                    builder.append_value("");
                    continue;
                }
                let target_len = len as usize;
                let pad_str = pad_opt.unwrap_or(" ");
                if pad_str.is_empty() {
                    let chars: Vec<char> = s.chars().collect();
                    let res: String = chars.into_iter().take(target_len).collect();
                    builder.append_value(&res);
                    continue;
                }
                let chars: Vec<char> = s.chars().collect();
                if chars.len() >= target_len {
                    let res: String = chars.into_iter().take(target_len).collect();
                    builder.append_value(&res);
                } else {
                    let deficit = target_len - chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let mut res = String::with_capacity(target_len);
                    for i in 0..deficit {
                        res.push(pad_chars[i % pad_chars.len()]);
                    }
                    res.extend(chars);
                    builder.append_value(&res);
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "rpad" => {
            let str_arr = eval_to_array(arg(args, 0)?, batch)?;
            let len_arr = eval_to_array(arg(args, 1)?, batch)?;
            let pad_arr = if args.len() > 2 {
                Some(eval_to_array(arg(args, 2)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let len_opt = extract_i64(&len_arr, r)?;
                let pad_opt = match &pad_arr {
                    Some(p) => extract_string(p, r)?,
                    None => None,
                };
                if s_opt.is_none() || len_opt.is_none() || (pad_arr.is_some() && pad_opt.is_none())
                {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                let len = len_opt.unwrap();
                if len <= 0 {
                    builder.append_value("");
                    continue;
                }
                let target_len = len as usize;
                let pad_str = pad_opt.unwrap_or(" ");
                if pad_str.is_empty() {
                    let chars: Vec<char> = s.chars().collect();
                    let res: String = chars.into_iter().take(target_len).collect();
                    builder.append_value(&res);
                    continue;
                }
                let chars: Vec<char> = s.chars().collect();
                if chars.len() >= target_len {
                    let res: String = chars.into_iter().take(target_len).collect();
                    builder.append_value(&res);
                } else {
                    let deficit = target_len - chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let mut res = String::with_capacity(target_len);
                    res.extend(chars);
                    for i in 0..deficit {
                        res.push(pad_chars[i % pad_chars.len()]);
                    }
                    builder.append_value(&res);
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "strpos" | "position" => {
            let (str_arr, substr_arr) = if name == "position" {
                (
                    eval_to_array(arg(args, 1)?, batch)?,
                    eval_to_array(arg(args, 0)?, batch)?,
                )
            } else {
                (
                    eval_to_array(arg(args, 0)?, batch)?,
                    eval_to_array(arg(args, 1)?, batch)?,
                )
            };
            let num_rows = batch.num_rows();
            let mut builder = Int64Builder::with_capacity(num_rows);
            for r in 0..num_rows {
                let s_opt = extract_string(&str_arr, r)?;
                let sub_opt = extract_string(&substr_arr, r)?;
                if s_opt.is_none() || sub_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let s = s_opt.unwrap();
                let sub = sub_opt.unwrap();
                if sub.is_empty() {
                    builder.append_value(1);
                } else if let Some(byte_idx) = s.find(sub) {
                    let char_idx = s[..byte_idx].chars().count() + 1;
                    builder.append_value(char_idx as i64);
                } else {
                    builder.append_value(0);
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "coalesce" => {
            let arrs = args
                .iter()
                .map(|a| eval_to_array(a, batch))
                .collect::<Result<Vec<_>, _>>()?;
            let num_rows = batch.num_rows();
            let target_dt = arrs
                .iter()
                .find(|a| a.data_type() != &DataType::Null)
                .map(|a| a.data_type().clone())
                .unwrap_or(DataType::Utf8);
            match target_dt {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if let Some(s) = extract_string_any(a, r)? {
                                found = Some(s);
                                break;
                            }
                        }
                        if let Some(s) = found {
                            builder.append_value(s);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Int64 | DataType::Int32 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if let Some(v) = extract_i64(a, r)? {
                                found = Some(v);
                                break;
                            }
                        }
                        if let Some(v) = found {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if let Some(v) = extract_f64_opt(a, r)? {
                                found = Some(v);
                                break;
                            }
                        }
                        if let Some(v) = found {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if let Some(v) = extract_timestamp_micros(a, r)? {
                                found = Some(v);
                                break;
                            }
                        }
                        if let Some(v) = found {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if !a.is_null(r) {
                                if let Some(ba) = a.as_any().downcast_ref::<BooleanArray>() {
                                    found = Some(ba.value(r));
                                    break;
                                }
                            }
                        }
                        if let Some(v) = found {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for r in 0..num_rows {
                        let mut found = None;
                        for a in &arrs {
                            if let Some(s) = extract_string_any(a, r)? {
                                found = Some(s);
                                break;
                            }
                        }
                        if let Some(s) = found {
                            builder.append_value(s);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }

        "nullif" => {
            let a_arr = eval_to_array(arg(args, 0)?, batch)?;
            let b_arr = eval_to_array(arg(args, 1)?, batch)?;
            let num_rows = batch.num_rows();
            let dt = a_arr.data_type().clone();
            match dt {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for r in 0..num_rows {
                        let av = extract_string(&a_arr, r)?;
                        let bv = extract_string(&b_arr, r)?;
                        match (av, bv) {
                            (Some(a), Some(b)) if a == b => builder.append_null(),
                            (Some(a), _) => builder.append_value(a),
                            (None, _) => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Int64 | DataType::Int32 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let av = extract_i64(&a_arr, r)?;
                        let bv = extract_i64(&b_arr, r)?;
                        match (av, bv) {
                            (Some(a), Some(b)) if a == b => builder.append_null(),
                            (Some(a), _) => builder.append_value(a),
                            (None, _) => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for r in 0..num_rows {
                        let av = extract_f64_opt(&a_arr, r)?;
                        let bv = extract_f64_opt(&b_arr, r)?;
                        match (av, bv) {
                            (Some(a), Some(b)) if a == b => builder.append_null(),
                            (Some(a), _) => builder.append_value(a),
                            (None, _) => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    for r in 0..num_rows {
                        let av = extract_string_any(&a_arr, r)?;
                        let bv = extract_string_any(&b_arr, r)?;
                        match (av, bv) {
                            (Some(a), Some(b)) if a == b => builder.append_null(),
                            (Some(a), _) => builder.append_value(&a),
                            (None, _) => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }

        "date_trunc" => {
            let unit_arr = eval_to_array(arg(args, 0)?, batch)?;
            let ts_arr = eval_to_array(arg(args, 1)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for r in 0..num_rows {
                let unit_opt = extract_string(&unit_arr, r)?;
                let ts_opt = extract_timestamp_micros(&ts_arr, r)?;
                if unit_opt.is_none() || ts_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let unit = unit_opt.unwrap().to_lowercase();
                let ts_micros = ts_opt.unwrap();
                let dt = Utc.timestamp_micros(ts_micros).single().unwrap_or_else(|| {
                    DateTime::from_timestamp(
                        ts_micros / 1_000_000,
                        ((ts_micros % 1_000_000).unsigned_abs() * 1000) as u32,
                    )
                    .unwrap_or_default()
                });
                let truncated = match unit.as_str() {
                    "year" | "years" => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                        .and_then(|d| d.and_hms_opt(0, 0, 0)),
                    "quarter" => {
                        let m = ((dt.month() - 1) / 3) * 3 + 1;
                        NaiveDate::from_ymd_opt(dt.year(), m, 1)
                            .and_then(|d| d.and_hms_opt(0, 0, 0))
                    }
                    "month" | "months" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                        .and_then(|d| d.and_hms_opt(0, 0, 0)),
                    "week" | "weeks" => {
                        let days_from_mon = dt.weekday().num_days_from_monday();
                        (dt.date_naive() - chrono::Duration::days(days_from_mon as i64))
                            .and_hms_opt(0, 0, 0)
                    }
                    "day" | "days" => dt.date_naive().and_hms_opt(0, 0, 0),
                    "hour" | "hours" => dt.date_naive().and_hms_opt(dt.hour(), 0, 0),
                    "minute" | "minutes" => dt.date_naive().and_hms_opt(dt.hour(), dt.minute(), 0),
                    "second" | "seconds" => {
                        dt.date_naive()
                            .and_hms_opt(dt.hour(), dt.minute(), dt.second())
                    }
                    "millisecond" | "milliseconds" => {
                        let us = (dt.nanosecond() / 1_000_000) * 1000;
                        dt.date_naive()
                            .and_hms_micro_opt(dt.hour(), dt.minute(), dt.second(), us)
                    }
                    "microsecond" | "microseconds" => {
                        let us = dt.nanosecond() / 1000;
                        dt.date_naive()
                            .and_hms_micro_opt(dt.hour(), dt.minute(), dt.second(), us)
                    }
                    _ => dt.date_naive().and_hms_opt(0, 0, 0),
                };
                if let Some(naive) = truncated {
                    builder.append_value(naive.and_utc().timestamp_micros());
                } else {
                    builder.append_value(ts_micros);
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "date_part" | "extract" => {
            let field_arr = eval_to_array(arg(args, 0)?, batch)?;
            let ts_arr = eval_to_array(arg(args, 1)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = Float64Builder::with_capacity(num_rows);
            for r in 0..num_rows {
                let field_opt = extract_string(&field_arr, r)?;
                let ts_opt = extract_timestamp_micros(&ts_arr, r)?;
                if field_opt.is_none() || ts_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let field = field_opt.unwrap().to_lowercase();
                let ts_micros = ts_opt.unwrap();
                let dt = Utc.timestamp_micros(ts_micros).single().unwrap_or_else(|| {
                    DateTime::from_timestamp(
                        ts_micros / 1_000_000,
                        ((ts_micros % 1_000_000).unsigned_abs() * 1000) as u32,
                    )
                    .unwrap_or_default()
                });
                let val = match field.as_str() {
                    "year" | "years" => dt.year() as f64,
                    "quarter" => ((dt.month() - 1) / 3 + 1) as f64,
                    "month" | "months" => dt.month() as f64,
                    "week" | "weeks" => dt.iso_week().week() as f64,
                    "day" | "days" => dt.day() as f64,
                    "dow" | "dayofweek" => dt.weekday().num_days_from_sunday() as f64,
                    "isodow" => dt.weekday().number_from_monday() as f64,
                    "doy" | "dayofyear" => dt.ordinal() as f64,
                    "hour" | "hours" => dt.hour() as f64,
                    "minute" | "minutes" => dt.minute() as f64,
                    "second" | "seconds" => {
                        dt.second() as f64 + (dt.nanosecond() as f64 / 1_000_000_000.0)
                    }
                    "millisecond" | "milliseconds" => {
                        dt.second() as f64 * 1000.0 + (dt.nanosecond() as f64 / 1_000_000.0)
                    }
                    "microsecond" | "microseconds" => {
                        dt.second() as f64 * 1_000_000.0 + (dt.nanosecond() as f64 / 1_000.0)
                    }
                    "epoch" => (ts_micros as f64) / 1_000_000.0,
                    _ => 0.0,
                };
                builder.append_value(val);
            }
            Ok(Arc::new(builder.finish()))
        }

        "to_char" => {
            let ts_arr = eval_to_array(arg(args, 0)?, batch)?;
            let fmt_arr = eval_to_array(arg(args, 1)?, batch)?;
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                let ts_opt = extract_timestamp_micros(&ts_arr, r)?;
                let fmt_opt = extract_string(&fmt_arr, r)?;
                if ts_opt.is_none() || fmt_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let ts_micros = ts_opt.unwrap();
                let fmt = fmt_opt.unwrap();
                let dt = Utc.timestamp_micros(ts_micros).single().unwrap_or_else(|| {
                    DateTime::from_timestamp(
                        ts_micros / 1_000_000,
                        ((ts_micros % 1_000_000).unsigned_abs() * 1000) as u32,
                    )
                    .unwrap_or_default()
                });
                let chrono_fmt = format_pg_to_chrono(fmt);
                builder.append_value(dt.format(&chrono_fmt).to_string());
            }
            Ok(Arc::new(builder.finish()))
        }

        "age" => {
            let ts1_arr = eval_to_array(arg(args, 0)?, batch)?;
            let ts2_arr = if args.len() > 1 {
                Some(eval_to_array(arg(args, 1)?, batch)?)
            } else {
                None
            };
            let num_rows = batch.num_rows();
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            let now_micros = Utc::now().timestamp_micros();
            for r in 0..num_rows {
                let ts1_opt = extract_timestamp_micros(&ts1_arr, r)?;
                let ts2_opt = match &ts2_arr {
                    Some(t) => extract_timestamp_micros(t, r)?,
                    None => Some(now_micros),
                };
                if ts1_opt.is_none() || ts2_opt.is_none() {
                    builder.append_null();
                    continue;
                }
                let ts1 = ts1_opt.unwrap();
                let ts2 = ts2_opt.unwrap();
                let diff = ts1 - ts2;
                let total_secs = diff / 1_000_000;
                let days = total_secs / 86400;
                let rem_secs = (total_secs % 86400).abs();
                let hours = rem_secs / 3600;
                let mins = (rem_secs % 3600) / 60;
                let secs = rem_secs % 60;
                let s = if days != 0 {
                    format!("{days} days {hours:02}:{mins:02}:{secs:02}")
                } else {
                    format!("{hours:02}:{mins:02}:{secs:02}")
                };
                builder.append_value(&s);
            }
            Ok(Arc::new(builder.finish()))
        }

        "now" | "current_timestamp" => {
            let num_rows = batch.num_rows();
            let now_micros = Utc::now().timestamp_micros();
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_value(now_micros);
            }
            Ok(Arc::new(builder.finish()))
        }

        "current_date" => {
            let num_rows = batch.num_rows();
            let now_days = (Utc::now().timestamp() / 86400) as i32;
            let mut builder = Date32Builder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_value(now_days);
            }
            Ok(Arc::new(builder.finish()))
        }

        other => Err(OpError::unimplemented(format!(
            "ScalarUdf `{other}` evaluation (arrives in a later version)"
        ))),
    }
}

/// Convert PostgreSQL datetime format string to chrono format specifiers.
fn format_pg_to_chrono(pg_fmt: &str) -> String {
    let mut f = pg_fmt.to_string();
    f = f.replace("YYYY", "%Y");
    f = f.replace("yyyy", "%Y");
    f = f.replace("YY", "%y");
    f = f.replace("yy", "%y");
    f = f.replace("MM", "%m");
    f = f.replace("DD", "%d");
    f = f.replace("dd", "%d");
    f = f.replace("HH24", "%H");
    f = f.replace("hh24", "%H");
    f = f.replace("HH12", "%I");
    f = f.replace("hh12", "%I");
    f = f.replace("HH", "%H");
    f = f.replace("hh", "%H");
    f = f.replace("MI", "%M");
    f = f.replace("mi", "%M");
    f = f.replace("SS", "%S");
    f = f.replace("ss", "%S");
    f = f.replace("MS", "%3f");
    f = f.replace("ms", "%3f");
    f = f.replace("US", "%6f");
    f = f.replace("us", "%6f");
    f
}

fn extract_string(arr: &ArrayRef, row: usize) -> Result<Option<&str>, OpError> {
    if arr.data_type() == &DataType::Null || arr.is_null(row) {
        return Ok(None);
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        Ok(Some(s.value(row)))
    } else if let Some(s) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        Ok(Some(s.value(row)))
    } else if let Some(s) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        Ok(Some(s.value(row)))
    } else {
        Err(OpError::column_type_mismatch(
            "Utf8",
            format!("{:?}", arr.data_type()),
        ))
    }
}

fn extract_string_any(arr: &ArrayRef, row: usize) -> Result<Option<String>, OpError> {
    if arr.data_type() == &DataType::Null || arr.is_null(row) {
        return Ok(None);
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        Ok(Some(s.value(row).to_string()))
    } else if let Some(s) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        Ok(Some(s.value(row).to_string()))
    } else if let Some(s) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        Ok(Some(s.value(row).to_string()))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(Some(i.value(row).to_string()))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(Some(i.value(row).to_string()))
    } else if let Some(f) = arr.as_any().downcast_ref::<Float64Array>() {
        Ok(Some(f.value(row).to_string()))
    } else if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
        Ok(Some(b.value(row).to_string()))
    } else {
        Ok(Some(format!("{:?}", arr)))
    }
}

fn extract_i64(arr: &ArrayRef, row: usize) -> Result<Option<i64>, OpError> {
    if arr.data_type() == &DataType::Null || arr.is_null(row) {
        return Ok(None);
    }
    if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(Some(i.value(row)))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(Some(i.value(row) as i64))
    } else if let Some(ts) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Ok(Some(ts.value(row)))
    } else {
        Err(OpError::column_type_mismatch(
            "Int64",
            format!("{:?}", arr.data_type()),
        ))
    }
}

fn extract_f64(arr: &ArrayRef, row: usize) -> Result<f64, OpError> {
    if let Some(f) = arr.as_any().downcast_ref::<Float64Array>() {
        Ok(f.value(row))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(i.value(row) as f64)
    } else if let Some(i) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(i.value(row) as f64)
    } else {
        Err(OpError::column_type_mismatch(
            "Float64",
            format!("{:?}", arr.data_type()),
        ))
    }
}

fn extract_f64_opt(arr: &ArrayRef, row: usize) -> Result<Option<f64>, OpError> {
    if arr.data_type() == &DataType::Null || arr.is_null(row) {
        return Ok(None);
    }
    Ok(Some(extract_f64(arr, row)?))
}

fn extract_duration_micros(arr: &ArrayRef, row: usize) -> Result<i64, OpError> {
    if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(i.value(row))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(i.value(row) as i64)
    } else {
        Err(OpError::column_type_mismatch(
            "Interval/Duration (micros)",
            format!("{:?}", arr.data_type()),
        ))
    }
}

fn extract_timestamp_micros(arr: &ArrayRef, row: usize) -> Result<Option<i64>, OpError> {
    if arr.data_type() == &DataType::Null || arr.is_null(row) {
        return Ok(None);
    }
    if let Some(ts) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Ok(Some(ts.value(row)))
    } else if let Some(ts) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        Ok(Some(ts.value(row).saturating_mul(1_000)))
    } else if let Some(ts) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        Ok(Some(ts.value(row).saturating_mul(1_000_000)))
    } else if let Some(ts) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Ok(Some(ts.value(row) / 1_000))
    } else if let Some(d) = arr.as_any().downcast_ref::<Date32Array>() {
        Ok(Some((d.value(row) as i64).saturating_mul(86_400_000_000)))
    } else if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(Some(i.value(row)))
    } else if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        let str_val = s.value(row);
        if let Ok(dt) = DateTime::parse_from_rfc3339(str_val) {
            Ok(Some(dt.timestamp_micros()))
        } else if let Ok(dt) = NaiveDateTime::parse_from_str(str_val, "%Y-%m-%d %H:%M:%S") {
            Ok(Some(dt.and_utc().timestamp_micros()))
        } else if let Ok(d) = NaiveDate::parse_from_str(str_val, "%Y-%m-%d") {
            Ok(Some(
                d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros(),
            ))
        } else {
            Err(OpError::invalid_literal(format!(
                "cannot parse timestamp from string {str_val:?}"
            )))
        }
    } else {
        Err(OpError::column_type_mismatch(
            "Timestamp",
            format!("{:?}", arr.data_type()),
        ))
    }
}

fn eval_cmp_arrays(l: &ArrayRef, r: &ArrayRef, op: BinaryOp) -> Result<Vec<bool>, OpError> {
    let num_rows = l.len();
    if let (Some(li), Some(ri)) = (
        l.as_any().downcast_ref::<Int64Array>(),
        r.as_any().downcast_ref::<Int64Array>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if li.is_null(i) || ri.is_null(i) {
                    false
                } else {
                    let lv = li.value(i);
                    let rv = ri.value(i);
                    match op {
                        BinaryOp::Gt => lv > rv,
                        BinaryOp::Lt => lv < rv,
                        BinaryOp::Ge => lv >= rv,
                        BinaryOp::Le => lv <= rv,
                        _ => false,
                    }
                }
            })
            .collect());
    }
    if let (Some(lf), Some(rf)) = (
        l.as_any().downcast_ref::<Float64Array>(),
        r.as_any().downcast_ref::<Float64Array>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if lf.is_null(i) || rf.is_null(i) {
                    false
                } else {
                    let lv = lf.value(i);
                    let rv = rf.value(i);
                    match op {
                        BinaryOp::Gt => lv > rv,
                        BinaryOp::Lt => lv < rv,
                        BinaryOp::Ge => lv >= rv,
                        BinaryOp::Le => lv <= rv,
                        _ => false,
                    }
                }
            })
            .collect());
    }
    if let (Some(ls), Some(rs)) = (
        l.as_any().downcast_ref::<StringArray>(),
        r.as_any().downcast_ref::<StringArray>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if ls.is_null(i) || rs.is_null(i) {
                    false
                } else {
                    let lv = ls.value(i);
                    let rv = rs.value(i);
                    match op {
                        BinaryOp::Gt => lv > rv,
                        BinaryOp::Lt => lv < rv,
                        BinaryOp::Ge => lv >= rv,
                        BinaryOp::Le => lv <= rv,
                        _ => false,
                    }
                }
            })
            .collect());
    }
    Err(OpError::expr_type_mismatch(format!(
        "Comparison between unsupported or mismatched Arrow types: {:?} vs {:?}",
        l.data_type(),
        r.data_type()
    )))
}

fn eval_eq_ne_arrays(l: &ArrayRef, r: &ArrayRef, negate: bool) -> Result<Vec<bool>, OpError> {
    let num_rows = l.len();
    if let (Some(li), Some(ri)) = (
        l.as_any().downcast_ref::<Int64Array>(),
        r.as_any().downcast_ref::<Int64Array>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if li.is_null(i) || ri.is_null(i) {
                    false
                } else {
                    (li.value(i) == ri.value(i)) != negate
                }
            })
            .collect());
    }
    if let (Some(ls), Some(rs)) = (
        l.as_any().downcast_ref::<StringArray>(),
        r.as_any().downcast_ref::<StringArray>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if ls.is_null(i) || rs.is_null(i) {
                    false
                } else {
                    (ls.value(i) == rs.value(i)) != negate
                }
            })
            .collect());
    }
    if let (Some(lf), Some(rf)) = (
        l.as_any().downcast_ref::<Float64Array>(),
        r.as_any().downcast_ref::<Float64Array>(),
    ) {
        return Ok((0..num_rows)
            .map(|i| {
                if lf.is_null(i) || rf.is_null(i) {
                    false
                } else {
                    (lf.value(i) == rf.value(i)) != negate
                }
            })
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

fn literal_utf8(expr: &Expr) -> Result<String, OpError> {
    match expr {
        Expr::Literal(bytes) => String::from_utf8(bytes.clone())
            .map_err(|e| OpError::invalid_literal(format!("literal is not valid UTF-8: {e}"))),
        other => Err(OpError::expr_type_mismatch(format!(
            "expected a string literal argument, got {other:?}"
        ))),
    }
}

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

    #[test]
    fn cast_int64_truncates_float64_values() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{Field, Schema};

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "avg",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![2887.3913, -4.75]))],
        )
        .unwrap();
        let expr = Expr::ScalarUdf {
            name: "cast_int64".to_string(),
            args: vec![Expr::Column(0)],
        };

        let result = eval_to_array(&expr, &batch).unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.values(), &[2887, -4]);
    }
}
