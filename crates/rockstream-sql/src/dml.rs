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
    Int(i64),
    Float(f64),
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

impl DmlScalarExpr {
    /// Evaluate the scalar expression against a row of string fields keyed by column name.
    pub fn evaluate(
        &self,
        row: &HashMap<String, Option<String>>,
    ) -> Result<Option<String>, SqlError> {
        match self {
            Self::Literal(lit) => match lit {
                DmlLiteral::Null => Ok(None),
                DmlLiteral::Int(i) => Ok(Some(i.to_string())),
                DmlLiteral::Float(f) => Ok(Some(f.to_string())),
                DmlLiteral::String(s) => Ok(Some(s.clone())),
                DmlLiteral::Bool(b) => Ok(Some(if *b { "true" } else { "false" }.to_string())),
            },
            Self::Column(col_name) => {
                let lookup = col_name.to_lowercase();
                for (k, v) in row {
                    if k.to_lowercase() == lookup {
                        return Ok(v.clone());
                    }
                }
                // If not found in row map, treat as NULL
                Ok(None)
            }
            Self::Parameter(_) => Err(SqlError::ParseError {
                message: "[RS-1012] Unbound parameter in scalar evaluation".to_string(),
            }),
            Self::BinaryArith { op, left, right } => {
                let l_val = left.evaluate(row)?;
                let r_val = right.evaluate(row)?;
                let (Some(l_str), Some(r_str)) = (l_val, r_val) else {
                    return Ok(None);
                };

                // Check integer arithmetic first
                if let (Ok(l_int), Ok(r_int)) = (l_str.parse::<i64>(), r_str.parse::<i64>()) {
                    let res = match op {
                        DmlArithOp::Add => l_int.checked_add(r_int),
                        DmlArithOp::Sub => l_int.checked_sub(r_int),
                        DmlArithOp::Mul => l_int.checked_mul(r_int),
                        DmlArithOp::Div => {
                            if r_int == 0 {
                                return Err(SqlError::ParseError {
                                    message: "[RS-1016] division by zero".to_string(),
                                });
                            }
                            l_int.checked_div(r_int)
                        }
                    };
                    match res {
                        Some(val) => Ok(Some(val.to_string())),
                        None => Err(SqlError::ParseError {
                            message: "[RS-1016] integer overflow".to_string(),
                        }),
                    }
                } else if let (Ok(l_flt), Ok(r_flt)) = (l_str.parse::<f64>(), r_str.parse::<f64>())
                {
                    let res = match op {
                        DmlArithOp::Add => l_flt + r_flt,
                        DmlArithOp::Sub => l_flt - r_flt,
                        DmlArithOp::Mul => l_flt * r_flt,
                        DmlArithOp::Div => {
                            if r_flt == 0.0 {
                                return Err(SqlError::ParseError {
                                    message: "[RS-1016] division by zero".to_string(),
                                });
                            }
                            l_flt / r_flt
                        }
                    };
                    Ok(Some(res.to_string()))
                } else {
                    Err(SqlError::IncompatibleSchemaChange {
                        reason: "[RS-1002] Incompatible types for arithmetic operation".to_string(),
                    })
                }
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
    /// Evaluate the predicate against a row of string fields, yielding a `TriBool`.
    pub fn evaluate(&self, row: &HashMap<String, Option<String>>) -> Result<TriBool, SqlError> {
        match self {
            Self::AlwaysTrue => Ok(TriBool::True),
            Self::AlwaysFalse => Ok(TriBool::False),
            Self::AlwaysNull => Ok(TriBool::Null),
            Self::And(l, r) => {
                let left_res = l.evaluate(row)?;
                let right_res = r.evaluate(row)?;
                Ok(left_res.and(right_res))
            }
            Self::Or(l, r) => {
                let left_res = l.evaluate(row)?;
                let right_res = r.evaluate(row)?;
                Ok(left_res.or(right_res))
            }
            Self::Not(inner) => {
                let inner_res = inner.evaluate(row)?;
                Ok(inner_res.not())
            }
            Self::IsNull(expr) => {
                let val = expr.evaluate(row)?;
                Ok(if val.is_none() {
                    TriBool::True
                } else {
                    TriBool::False
                })
            }
            Self::IsNotNull(expr) => {
                let val = expr.evaluate(row)?;
                Ok(if val.is_some() {
                    TriBool::True
                } else {
                    TriBool::False
                })
            }
            Self::Comparison { op, left, right } => {
                let l_val = left.evaluate(row)?;
                let r_val = right.evaluate(row)?;

                let (Some(l_str), Some(r_str)) = (l_val, r_val) else {
                    // SQL three-valued logic: NULL comparison yields NULL
                    return Ok(TriBool::Null);
                };

                // Boolean comparisons
                let is_l_bool = l_str.eq_ignore_ascii_case("true")
                    || l_str.eq_ignore_ascii_case("false")
                    || l_str == "t"
                    || l_str == "f";
                let is_r_bool = r_str.eq_ignore_ascii_case("true")
                    || r_str.eq_ignore_ascii_case("false")
                    || r_str == "t"
                    || r_str == "f";
                if is_l_bool && is_r_bool {
                    let b_l = l_str.eq_ignore_ascii_case("true") || l_str == "t";
                    let b_r = r_str.eq_ignore_ascii_case("true") || r_str == "t";
                    return match op {
                        DmlCompareOp::Eq => Ok(if b_l == b_r {
                            TriBool::True
                        } else {
                            TriBool::False
                        }),
                        DmlCompareOp::NotEq => Ok(if b_l != b_r {
                            TriBool::True
                        } else {
                            TriBool::False
                        }),
                        _ => Err(SqlError::IncompatibleSchemaChange {
                            reason: "[RS-1002] Ordered comparisons not supported on booleans"
                                .to_string(),
                        }),
                    };
                }

                // Numeric comparisons (try integer first, then float)
                if let (Ok(l_num), Ok(r_num)) = (l_str.parse::<i64>(), r_str.parse::<i64>()) {
                    let cmp = match op {
                        DmlCompareOp::Eq => l_num == r_num,
                        DmlCompareOp::NotEq => l_num != r_num,
                        DmlCompareOp::Lt => l_num < r_num,
                        DmlCompareOp::Lte => l_num <= r_num,
                        DmlCompareOp::Gt => l_num > r_num,
                        DmlCompareOp::Gte => l_num >= r_num,
                    };
                    return Ok(if cmp { TriBool::True } else { TriBool::False });
                }

                if let (Ok(l_num), Ok(r_num)) = (l_str.parse::<f64>(), r_str.parse::<f64>()) {
                    let cmp = match op {
                        DmlCompareOp::Eq => (l_num - r_num).abs() < f64::EPSILON,
                        DmlCompareOp::NotEq => (l_num - r_num).abs() >= f64::EPSILON,
                        DmlCompareOp::Lt => l_num < r_num,
                        DmlCompareOp::Lte => l_num <= r_num,
                        DmlCompareOp::Gt => l_num > r_num,
                        DmlCompareOp::Gte => l_num >= r_num,
                    };
                    return Ok(if cmp { TriBool::True } else { TriBool::False });
                }

                // String comparisons
                let cmp = match op {
                    DmlCompareOp::Eq => l_str == r_str,
                    DmlCompareOp::NotEq => l_str != r_str,
                    DmlCompareOp::Lt => l_str < r_str,
                    DmlCompareOp::Lte => l_str <= r_str,
                    DmlCompareOp::Gt => l_str > r_str,
                    DmlCompareOp::Gte => l_str >= r_str,
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
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DmlScalarExpr::Literal(DmlLiteral::Int(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DmlScalarExpr::Literal(DmlLiteral::Float(f)))
                } else {
                    Err(SqlError::ParseError {
                        message: format!("[RS-1012] Invalid numeric literal: {n}"),
                    })
                }
            }
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
            left: Box::new(DmlScalarExpr::Literal(DmlLiteral::Int(0))),
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

    // 2. Parse via sqlparser with PostgreSQL dialect
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::ParseError {
        message: e.to_string(),
    })?;

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
