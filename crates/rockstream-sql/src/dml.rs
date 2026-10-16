//! Typed DML AST representation, parsing, and predicate evaluation (v0.64).
//!
//! Provides:
//! - [`DmlStatement`]: Typed AST for INSERT, UPDATE, and DELETE statements.
//! - [`parse_dml_statement`]: Bounded AST parser using `sqlparser` with `PostgreSqlDialect`.
//! - [`DmlPredicate`]: Standard SQL predicate evaluator supporting three-valued logic (`TriBool`),
//!   multi-column `AND`, `OR`, `NOT`, comparisons, `IS NULL`, and `IS NOT NULL`.

use sqlparser::ast::{
    Assignment as SpAssignment, BinaryOperator, Delete, Expr as SpExpr, FromTable, Insert,
    ObjectName, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use arrow::compute::CastOptions;
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::Operator;

use crate::error::SqlError;

/// Maximum allowed size of raw SQL statement in bytes (1 MiB).
pub const MAX_SQL_STATEMENT_BYTES: usize = 1_048_576;

/// Maximum allowed expression depth in SQL AST (64 levels).
pub const MAX_EXPR_DEPTH: usize = 64;

/// Maximum mutations allowed per DML statement (10,000 rows).
pub const MAX_DML_MUTATIONS_PER_STATEMENT: usize = 10_000;

/// Maximum row scan pagination window during DML execution (1,024 rows).
pub const MAX_DML_SCAN_WINDOW_ROWS: usize = 1_024;

// ─── Typed DML Statement IR ──────────────────────────────────────────────────

/// Strongly typed representation of a DML statement.
#[derive(Debug, Clone, PartialEq)]
pub enum DmlStatement {
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl DmlStatement {
    /// Target table name for this DML statement.
    pub fn table_name(&self) -> &str {
        match self {
            Self::Insert(s) => &s.table,
            Self::Update(s) => &s.table,
            Self::Delete(s) => &s.table,
        }
    }

    /// RETURNING column names, if specified.
    pub fn returning(&self) -> Option<&[String]> {
        match self {
            Self::Insert(s) => s.returning.as_deref(),
            Self::Update(s) => s.returning.as_deref(),
            Self::Delete(s) => s.returning.as_deref(),
        }
    }
}

/// Typed INSERT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<SpExpr>>,
    pub returning: Option<Vec<String>>,
}

/// An assignment clause in an UPDATE statement (`col = expr`).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAssignment {
    pub column: String,
    pub expr: SpExpr,
}

/// Typed UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<UpdateAssignment>,
    pub selection: Option<SpExpr>,
    pub returning: Option<Vec<String>>,
}

/// Typed DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub selection: Option<SpExpr>,
    pub returning: Option<Vec<String>>,
}

// ─── SQL Three-Valued Logic ──────────────────────────────────────────────────

/// Three-valued boolean logic (`TRUE`, `FALSE`, `NULL`) as specified by SQL-92.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriBool {
    True,
    False,
    Null,
}

impl TriBool {
    /// Returns true only if the value is explicitly `True`.
    pub fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    /// Standard SQL three-valued `AND`.
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Null,
        }
    }

    /// Standard SQL three-valued `OR`.
    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Null,
        }
    }

    /// Standard SQL three-valued `NOT`.
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        !self
    }
}

impl std::ops::Not for TriBool {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
        }
    }
}

// ─── Predicate Evaluation Tree ───────────────────────────────────────────────

/// Comparison operators supported in general predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlCompareOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl DmlCompareOp {
    fn compare<T: PartialOrd>(&self, left: T, right: T) -> bool {
        match self {
            Self::Eq => left == right,
            Self::NotEq => left != right,
            Self::Lt => left < right,
            Self::Lte => left <= right,
            Self::Gt => left > right,
            Self::Gte => left >= right,
        }
    }
}

/// Arithmetic operators supported in expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// A literal value inside a predicate or assignment expression.
#[derive(Debug, Clone, PartialEq)]
pub enum DmlLiteral {
    Null,
    Number(String),
    String(String),
    Bool(bool),
}

/// A scalar expression in a predicate or assignment.
#[derive(Debug, Clone, PartialEq)]
pub enum DmlScalarExpr {
    Column(String),
    Literal(DmlLiteral),
    BinaryArith {
        op: DmlArithOp,
        left: Box<DmlScalarExpr>,
        right: Box<DmlScalarExpr>,
    },
    Parameter(usize),
}

fn evaluation_error(code: &str, message: impl std::fmt::Display) -> SqlError {
    SqlError::ParseError {
        message: format!("[{code}] {message}"),
    }
}

fn cast_scalar(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue, SqlError> {
    value
        .cast_to_with_options(
            data_type,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|e| evaluation_error("RS-1016", e))
}

fn parse_scalar(value: &str, data_type: &DataType) -> Result<ScalarValue, SqlError> {
    let data_type = match data_type {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Boolean => data_type,
        // Other SQL types retain their existing textual storage representation.
        _ => &DataType::Utf8,
    };
    cast_scalar(&ScalarValue::Utf8(Some(value.to_string())), data_type)
}

fn scalar_text(value: &ScalarValue) -> Result<Option<String>, SqlError> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(cast_scalar(value, &DataType::Utf8)?.to_string()))
}

impl DmlScalarExpr {
    fn column_type<'a>(&self, types: &'a HashMap<String, DataType>) -> Option<&'a DataType> {
        match self {
            Self::Column(name) => types.get(&name.to_lowercase()),
            Self::BinaryArith { left, right, .. } => {
                left.column_type(types).or_else(|| right.column_type(types))
            }
            _ => None,
        }
    }

    /// Evaluate using declared column types; values stay textual only at the storage boundary.
    pub fn evaluate(
        &self,
        row: &HashMap<String, Option<String>>,
        types: &HashMap<String, DataType>,
    ) -> Result<Option<String>, SqlError> {
        let value = self.evaluate_scalar(row, types, None)?;
        scalar_text(&value)
    }

    /// Validate and format an assignment before any candidate mutations are buffered.
    pub fn evaluate_assignment(
        &self,
        row: &HashMap<String, Option<String>>,
        types: &HashMap<String, DataType>,
        target: &DataType,
    ) -> Result<Option<String>, SqlError> {
        let value = self.evaluate_scalar(row, types, Some(target))?;
        let Some(text) = scalar_text(&value)? else {
            return Ok(None);
        };
        // Reparse even when types match: arithmetic kernels can exceed decimal precision.
        scalar_text(&parse_scalar(&text, target)?)
    }

    fn evaluate_scalar(
        &self,
        row: &HashMap<String, Option<String>>,
        types: &HashMap<String, DataType>,
        expected: Option<&DataType>,
    ) -> Result<ScalarValue, SqlError> {
        match self {
            Self::Literal(DmlLiteral::Null) => Ok(ScalarValue::Null),
            Self::Literal(DmlLiteral::Bool(b)) => Ok(ScalarValue::Boolean(Some(*b))),
            Self::Literal(DmlLiteral::String(s)) => {
                parse_scalar(s, expected.unwrap_or(&DataType::Utf8))
            }
            Self::Literal(DmlLiteral::Number(n)) => {
                if let Some(t @ (DataType::Float32 | DataType::Float64)) = expected {
                    return parse_scalar(n, t);
                }
                if let Ok(i) = n.parse::<i64>() {
                    return Ok(ScalarValue::Int64(Some(i)));
                }
                // SQL numeric literals must reach decimal arithmetic without a binary-float round trip.
                let (mantissa, exponent) = n.split_once(['e', 'E']).unwrap_or((n, "0"));
                let exponent = exponent
                    .parse::<i32>()
                    .map_err(|e| evaluation_error("RS-1012", e))?
                    as i64;
                let fraction = mantissa.split_once('.').map_or(0, |(_, f)| f.len() as i64);
                let scale = (fraction - exponent).max(0);
                let digits = mantissa.bytes().filter(u8::is_ascii_digit).count() as i64;
                let precision = (digits + exponent - fraction + scale).max(scale).max(1);
                if precision > 38 || scale > 38 {
                    return Err(evaluation_error(
                        "RS-1016",
                        "numeric literal exceeds decimal precision",
                    ));
                }
                let coefficient = mantissa
                    .replace('.', "")
                    .parse::<i128>()
                    .map_err(|e| evaluation_error("RS-1016", e))?;
                let value = coefficient
                    .checked_mul(10_i128.pow((exponent - fraction).max(0) as u32))
                    .ok_or_else(|| evaluation_error("RS-1016", "numeric literal overflow"))?;
                Ok(ScalarValue::Decimal128(
                    Some(value),
                    precision as u8,
                    scale as i8,
                ))
            }
            Self::Column(name) => {
                let data_type = self.column_type(types).ok_or_else(|| {
                    evaluation_error("RS-1002", format!("Unknown column: {name}"))
                })?;
                match row
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .and_then(|(_, v)| v.as_deref())
                {
                    Some(value) => parse_scalar(value, data_type),
                    None => Ok(ScalarValue::Null),
                }
            }
            Self::Parameter(_) => Err(evaluation_error(
                "RS-1012",
                "Unbound parameter in scalar evaluation",
            )),
            Self::BinaryArith { op, left, right } => {
                let expected = self.column_type(types).or(expected);
                let left = left.evaluate_scalar(row, types, expected)?;
                let right = right.evaluate_scalar(row, types, expected)?;
                if left.is_null() || right.is_null() {
                    return Ok(ScalarValue::Null);
                }
                let operator = match op {
                    DmlArithOp::Add => Operator::Plus,
                    DmlArithOp::Sub => Operator::Minus,
                    DmlArithOp::Mul => Operator::Multiply,
                    DmlArithOp::Div => Operator::Divide,
                };
                let (lt, rt) =
                    BinaryTypeCoercer::new(&left.data_type(), &operator, &right.data_type())
                        .get_input_types()
                        .map_err(|e| evaluation_error("RS-1002", e))?;
                let left = cast_scalar(&left, &lt)?;
                let right = cast_scalar(&right, &rt)?;
                if matches!(op, DmlArithOp::Div) {
                    let is_zero = match &right {
                        ScalarValue::Float32(Some(v)) => *v == 0.0,
                        ScalarValue::Float64(Some(v)) => *v == 0.0,
                        _ => right == parse_scalar("0", &rt)?,
                    };
                    if is_zero {
                        return Err(evaluation_error("RS-1016", "division by zero"));
                    }
                }
                match op {
                    DmlArithOp::Add => left.add_checked(&right),
                    DmlArithOp::Sub => left.sub_checked(&right),
                    DmlArithOp::Mul => left.mul_checked(&right),
                    DmlArithOp::Div => left.div(&right),
                }
                .map_err(|e| evaluation_error("RS-1016", e))
            }
        }
    }
}

/// A lowered predicate tree for standard SQL evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum DmlPredicate {
    AlwaysTrue,
    AlwaysFalse,
    AlwaysNull,
    And(Box<DmlPredicate>, Box<DmlPredicate>),
    Or(Box<DmlPredicate>, Box<DmlPredicate>),
    Not(Box<DmlPredicate>),
    Comparison {
        op: DmlCompareOp,
        left: DmlScalarExpr,
        right: DmlScalarExpr,
    },
    IsNull(DmlScalarExpr),
    IsNotNull(DmlScalarExpr),
}

impl DmlPredicate {
    /// Evaluate a predicate using declared SQL column types and three-valued logic.
    pub fn evaluate(
        &self,
        row: &HashMap<String, Option<String>>,
        types: &HashMap<String, DataType>,
    ) -> Result<TriBool, SqlError> {
        match self {
            Self::AlwaysTrue => Ok(TriBool::True),
            Self::AlwaysFalse => Ok(TriBool::False),
            Self::AlwaysNull => Ok(TriBool::Null),
            Self::And(l, r) => Ok(l.evaluate(row, types)?.and(r.evaluate(row, types)?)),
            Self::Or(l, r) => Ok(l.evaluate(row, types)?.or(r.evaluate(row, types)?)),
            Self::Not(inner) => Ok(inner.evaluate(row, types)?.not()),
            Self::IsNull(expr) | Self::IsNotNull(expr) => {
                let is_null = expr.evaluate_scalar(row, types, None)?.is_null();
                Ok(if is_null == matches!(self, Self::IsNull(_)) {
                    TriBool::True
                } else {
                    TriBool::False
                })
            }
            Self::Comparison { op, left, right } => {
                let l = left.evaluate_scalar(row, types, right.column_type(types))?;
                let r = right.evaluate_scalar(row, types, left.column_type(types))?;
                if l.is_null() || r.is_null() {
                    return Ok(TriBool::Null);
                }
                let (lt, rt) =
                    BinaryTypeCoercer::new(&l.data_type(), &Operator::Eq, &r.data_type())
                        .get_input_types()
                        .map_err(|e| evaluation_error("RS-1002", e))?;
                let l = cast_scalar(&l, &lt)?;
                let r = cast_scalar(&r, &rt)?;
                if lt == DataType::Boolean && !matches!(op, DmlCompareOp::Eq | DmlCompareOp::NotEq)
                {
                    return Err(evaluation_error(
                        "RS-1002",
                        "Ordered comparisons not supported on booleans",
                    ));
                }
                let cmp = match (&l, &r) {
                    (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
                        op.compare(*l, *r)
                    }
                    (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
                        op.compare(*l, *r)
                    }
                    _ => op.compare(l, r),
                };
                Ok(if cmp { TriBool::True } else { TriBool::False })
            }
        }
    }
}

// ─── Lowering Helpers ────────────────────────────────────────────────────────

/// Lower a `sqlparser::ast::Expr` into a `DmlScalarExpr`.
pub fn lower_scalar_expr(expr: &SpExpr) -> Result<DmlScalarExpr, SqlError> {
    match expr {
        SpExpr::Identifier(ident) => Ok(DmlScalarExpr::Column(ident.value.clone())),
        SpExpr::CompoundIdentifier(idents) => {
            let col = idents.last().map(|i| i.value.clone()).unwrap_or_default();
            Ok(DmlScalarExpr::Column(col))
        }
        SpExpr::Value(v) => match &v.value {
            Value::Null => Ok(DmlScalarExpr::Literal(DmlLiteral::Null)),
            Value::Number(n, _) => Ok(DmlScalarExpr::Literal(DmlLiteral::Number(n.clone()))),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(DmlScalarExpr::Literal(DmlLiteral::String(s.clone())))
            }
            Value::Boolean(b) => Ok(DmlScalarExpr::Literal(DmlLiteral::Bool(*b))),
            other => Err(SqlError::ParseError {
                message: format!(
                    "[RS-1012] Unsupported literal value in DML expression: {other:?}"
                ),
            }),
        },
        SpExpr::BinaryOp { left, op, right } => {
            let a_op = match op {
                BinaryOperator::Plus => DmlArithOp::Add,
                BinaryOperator::Minus => DmlArithOp::Sub,
                BinaryOperator::Multiply => DmlArithOp::Mul,
                BinaryOperator::Divide => DmlArithOp::Div,
                other => {
                    return Err(SqlError::ParseError {
                        message: format!(
                            "[RS-1012] Unsupported operator in scalar expression: {other:?}"
                        ),
                    });
                }
            };
            Ok(DmlScalarExpr::BinaryArith {
                op: a_op,
                left: Box::new(lower_scalar_expr(left)?),
                right: Box::new(lower_scalar_expr(right)?),
            })
        }
        SpExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Ok(DmlScalarExpr::BinaryArith {
            op: DmlArithOp::Sub,
            left: Box::new(DmlScalarExpr::Literal(DmlLiteral::Number("0".to_string()))),
            right: Box::new(lower_scalar_expr(expr)?),
        }),
        SpExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => lower_scalar_expr(expr),
        SpExpr::Nested(expr) => lower_scalar_expr(expr),
        SpExpr::Subquery(_) | SpExpr::Exists { .. } | SpExpr::InSubquery { .. } => {
            Err(SqlError::UnsupportedPlanNode {
                node_type: "[RS-1013] Subqueries in DML expressions are unsupported".to_string(),
            })
        }
        SpExpr::Function(func) => Err(SqlError::UnsupportedPlanNode {
            node_type: format!(
                "[RS-1013] Function '{}' in DML expression is unsupported",
                func.name
            ),
        }),
        other => Err(SqlError::ParseError {
            message: format!("[RS-1012] Unsupported scalar expression: {other:?}"),
        }),
    }
}

/// Lower a `sqlparser::ast::Expr` into a `DmlPredicate`.
pub fn lower_predicate(expr: &SpExpr) -> Result<DmlPredicate, SqlError> {
    match expr {
        SpExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = lower_predicate(left)?;
                let r = lower_predicate(right)?;
                Ok(DmlPredicate::And(Box::new(l), Box::new(r)))
            }
            BinaryOperator::Or => {
                let l = lower_predicate(left)?;
                let r = lower_predicate(right)?;
                Ok(DmlPredicate::Or(Box::new(l), Box::new(r)))
            }
            BinaryOperator::Eq => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Eq,
                    left: l,
                    right: r,
                })
            }
            BinaryOperator::NotEq => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::NotEq,
                    left: l,
                    right: r,
                })
            }
            BinaryOperator::Lt => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Lt,
                    left: l,
                    right: r,
                })
            }
            BinaryOperator::LtEq => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Lte,
                    left: l,
                    right: r,
                })
            }
            BinaryOperator::Gt => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Gt,
                    left: l,
                    right: r,
                })
            }
            BinaryOperator::GtEq => {
                let l = lower_scalar_expr(left)?;
                let r = lower_scalar_expr(right)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Gte,
                    left: l,
                    right: r,
                })
            }
            other => Err(SqlError::ParseError {
                message: format!("[RS-1012] Unsupported binary operator in predicate: {other:?}"),
            }),
        },
        SpExpr::UnaryOp { op: UnaryOperator::Not, expr } => {
            let inner = lower_predicate(expr)?;
            Ok(DmlPredicate::Not(Box::new(inner)))
        }
        SpExpr::IsNull(expr) => {
            let inner = lower_scalar_expr(expr)?;
            Ok(DmlPredicate::IsNull(inner))
        }
        SpExpr::IsNotNull(expr) => {
            let inner = lower_scalar_expr(expr)?;
            Ok(DmlPredicate::IsNotNull(inner))
        }
        SpExpr::Nested(inner) => lower_predicate(inner),
        SpExpr::Value(v) => match &v.value {
            Value::Boolean(true) => Ok(DmlPredicate::AlwaysTrue),
            Value::Boolean(false) => Ok(DmlPredicate::AlwaysFalse),
            Value::Null => Ok(DmlPredicate::AlwaysNull),
            _ => {
                let s = lower_scalar_expr(expr)?;
                Ok(DmlPredicate::Comparison {
                    op: DmlCompareOp::Eq,
                    left: s,
                    right: DmlScalarExpr::Literal(DmlLiteral::Bool(true)),
                })
            }
        },
        SpExpr::Subquery(_) | SpExpr::Exists { .. } | SpExpr::InSubquery { .. } => {
            Err(SqlError::UnsupportedPlanNode {
                node_type: "[RS-1013] Correlated or uncorrelated subqueries in DML predicates are unsupported".to_string(),
            })
        }
        SpExpr::Function(func) => {
            Err(SqlError::UnsupportedPlanNode {
                node_type: format!(
                    "[RS-1013] Function call '{}' in DML predicate is unsupported",
                    func.name
                ),
            })
        }
        other => Err(SqlError::ParseError {
            message: format!("[RS-1012] Unsupported expression in predicate: {other:?}"),
        }),
    }
}

// ─── Depth and Size Boundary Checks ──────────────────────────────────────────

/// Recursively check that expression depth does not exceed `max_depth`.
pub fn check_expr_depth(expr: &SpExpr, depth: usize, max_depth: usize) -> Result<(), SqlError> {
    if depth > max_depth {
        return Err(SqlError::ParseError {
            message: format!(
                "[RS-1012] Expression depth limit exceeded (maximum allowed is {max_depth})"
            ),
        });
    }

    match expr {
        SpExpr::BinaryOp { left, right, .. } => {
            check_expr_depth(left, depth + 1, max_depth)?;
            check_expr_depth(right, depth + 1, max_depth)
        }
        SpExpr::UnaryOp { expr, .. } | SpExpr::IsNull(expr) | SpExpr::IsNotNull(expr) => {
            check_expr_depth(expr, depth + 1, max_depth)
        }
        SpExpr::Nested(inner) => check_expr_depth(inner, depth + 1, max_depth),
        SpExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                check_expr_depth(op, depth + 1, max_depth)?;
            }
            for cw in conditions {
                check_expr_depth(&cw.condition, depth + 1, max_depth)?;
                check_expr_depth(&cw.result, depth + 1, max_depth)?;
            }
            if let Some(el) = else_result {
                check_expr_depth(el, depth + 1, max_depth)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn extract_object_name(name: &ObjectName) -> String {
    name.to_string()
        .split('.')
        .next_back()
        .unwrap_or("")
        .to_string()
}

fn extract_returning(returning: &Option<Vec<SelectItem>>) -> Option<Vec<String>> {
    returning.as_ref().map(|items| {
        items
            .iter()
            .map(|item| match item {
                SelectItem::Wildcard(_) => "*".to_string(),
                SelectItem::UnnamedExpr(expr) => expr.to_string(),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::QualifiedWildcard(prefix, _) => format!("{prefix}.*"),
            })
            .collect()
    })
}

fn check_malformed_returning(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let mut in_quote = false;
    let mut returning_idx = None;
    let bytes = lower.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote && lower[i..].starts_with("returning") {
            let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
            let after_idx = i + "returning".len();
            let after_ok = after_idx == bytes.len()
                || bytes[after_idx].is_ascii_whitespace()
                || bytes[after_idx] == b';'
                || bytes[after_idx] == b'(';
            if before_ok && after_ok {
                returning_idx = Some(after_idx);
            }
        }
        i += 1;
    }

    if let Some(after_idx) = returning_idx {
        let rest = sql[after_idx..].trim().trim_end_matches(';').trim();
        if rest.is_empty() {
            return true;
        }
        if rest == "*" {
            return false;
        }
        if rest.ends_with(',') || rest.starts_with(',') || rest.contains(",,") {
            return true;
        }
        let cols: Vec<&str> = rest.split(',').map(|c| c.trim()).collect();
        if cols.iter().any(|c| {
            c.is_empty()
                || !c
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '*' || ch == '.')
        }) {
            return true;
        }
    }
    false
}

/// Parse a raw SQL query into a typed [`DmlStatement`], enforcing statement size and expression depth bounds.
pub fn parse_dml_statement(sql: &str) -> Result<DmlStatement, SqlError> {
    // 1. Statement length bound check
    if sql.len() > MAX_SQL_STATEMENT_BYTES {
        return Err(SqlError::ParseError {
            message: format!(
                "[RS-1012] Statement size {} exceeds maximum limit of {} bytes",
                sql.len(),
                MAX_SQL_STATEMENT_BYTES
            ),
        });
    }

    // 2. Pre-parse expression nesting depth bound check (protect against stack overflow)
    let mut paren_depth = 0usize;
    let mut in_single_quote = false;
    for ch in sql.chars() {
        match ch {
            '\'' => in_single_quote = !in_single_quote,
            '(' if !in_single_quote => {
                paren_depth += 1;
                if paren_depth > MAX_EXPR_DEPTH {
                    return Err(SqlError::ParseError {
                        message: format!(
                            "[RS-1012] Expression depth limit exceeded (maximum allowed is {MAX_EXPR_DEPTH})"
                        ),
                    });
                }
            }
            ')' if !in_single_quote => {
                paren_depth = paren_depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    // 3. Parse via sqlparser with PostgreSQL dialect
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(e) => {
            if check_malformed_returning(sql) {
                return Err(SqlError::MalformedReturningClause);
            }
            return Err(SqlError::ParseError {
                message: e.to_string(),
            });
        }
    };

    if statements.is_empty() {
        return Err(SqlError::ParseError {
            message: "Empty SQL statement".to_string(),
        });
    }

    let stmt = statements.remove(0);

    match stmt {
        Statement::Insert(Insert {
            table,
            columns,
            source,
            returning,
            ..
        }) => {
            let table_name = table
                .to_string()
                .split('.')
                .next_back()
                .unwrap_or("")
                .to_string();
            let col_names = columns.into_iter().map(|c| c.value).collect();

            let mut values = Vec::new();
            if let Some(query) = source {
                if let SetExpr::Values(v) = *query.body {
                    for row in v.rows {
                        for expr in &row {
                            check_expr_depth(expr, 1, MAX_EXPR_DEPTH)?;
                        }
                        values.push(row);
                    }
                } else {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: "[RS-1013] INSERT from non-VALUES query is unsupported in DML"
                            .to_string(),
                    });
                }
            }

            let ret_cols = extract_returning(&returning);
            Ok(DmlStatement::Insert(InsertStatement {
                table: table_name,
                columns: col_names,
                values,
                returning: ret_cols,
            }))
        }
        Statement::Update(sqlparser::ast::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        }) => {
            let table_name = match &table.relation {
                TableFactor::Table { name, .. } => extract_object_name(name),
                other => other
                    .to_string()
                    .split('.')
                    .next_back()
                    .unwrap_or("")
                    .to_string(),
            };

            let mut typed_assignments = Vec::new();
            for SpAssignment { target, value } in assignments {
                let col = target
                    .to_string()
                    .split('.')
                    .next_back()
                    .unwrap_or("")
                    .to_string();
                check_expr_depth(&value, 1, MAX_EXPR_DEPTH)?;
                typed_assignments.push(UpdateAssignment {
                    column: col,
                    expr: value,
                });
            }

            if let Some(sel) = &selection {
                check_expr_depth(sel, 1, MAX_EXPR_DEPTH)?;
            }

            let ret_cols = extract_returning(&returning);
            Ok(DmlStatement::Update(UpdateStatement {
                table: table_name,
                assignments: typed_assignments,
                selection,
                returning: ret_cols,
            }))
        }
        Statement::Delete(Delete {
            from,
            selection,
            returning,
            ..
        }) => {
            let table_name = match from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                    if let Some(t) = tables.first() {
                        match &t.relation {
                            TableFactor::Table { name, .. } => extract_object_name(name),
                            other => other
                                .to_string()
                                .split('.')
                                .next_back()
                                .unwrap_or("")
                                .to_string(),
                        }
                    } else {
                        return Err(SqlError::ParseError {
                            message: "[RS-1012] Missing table in DELETE statement".to_string(),
                        });
                    }
                }
            };

            if let Some(sel) = &selection {
                check_expr_depth(sel, 1, MAX_EXPR_DEPTH)?;
            }

            let ret_cols = extract_returning(&returning);
            Ok(DmlStatement::Delete(DeleteStatement {
                table: table_name,
                selection,
                returning: ret_cols,
            }))
        }
        other => Err(SqlError::ParseError {
            message: format!("[RS-1012] Not a recognized DML statement: {other:?}"),
        }),
    }
}
