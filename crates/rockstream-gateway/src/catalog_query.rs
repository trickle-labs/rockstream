//! SQL projection and selection for the small, in-memory reflection catalogs.

use crate::catalog_stubs::{CatalogResponse, SessionInfo};
use sqlparser::ast::*;
use std::{cmp::Ordering, collections::HashMap};

type Row = HashMap<String, Option<String>>;
type QueryRows = (Vec<String>, Vec<Vec<Option<String>>>);
type RelationRows = (Vec<(String, String)>, Vec<Row>);
type Result<T> = std::result::Result<T, String>;

fn identifier(id: &Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_lowercase()
    }
}

pub(crate) fn execute(
    query: &Query,
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
    session: &SessionInfo,
) -> Option<CatalogResponse> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &select.from.first()?.relation else {
        return None;
    };
    provider(&name.to_string())?;
    Some(match run(query, provider, session, &Row::new()) {
        Ok((columns, rows)) => CatalogResponse::Rows { columns, rows },
        Err(message) => CatalogResponse::Error { message },
    })
}

fn run(
    query: &Query,
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
    session: &SessionInfo,
    outer: &Row,
) -> Result<QueryRows> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("unsupported catalog query body".into());
    };
    if select.distinct.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty())
    {
        return Err("unsupported catalog aggregation".into());
    }
    let mut rows = vec![outer.clone()];
    let mut fields = Vec::new();
    for source in &select.from {
        let (columns, right) = relation(&source.relation, provider)?;
        rows = join(rows, &right, None, false, provider, session)?;
        fields.extend(columns);
        for item in &source.joins {
            let (columns, right) = relation(&item.relation, provider)?;
            let (constraint, left) = match &item.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (c, false)
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, true),
                _ => return Err("unsupported catalog join".into()),
            };
            let condition = match constraint {
                JoinConstraint::On(expr) => Some(expr),
                JoinConstraint::None => None,
                _ => return Err("unsupported catalog join constraint".into()),
            };
            // Include NULLs for an unmatched LEFT JOIN, including its unqualified columns.
            let nulls: Row = columns
                .iter()
                .flat_map(|(key, name)| [(key.clone(), None), (name.clone(), None)])
                .collect();
            rows = join(rows, &right, condition, left, provider, session)?
                .into_iter()
                .map(|mut row| {
                    for (key, value) in &nulls {
                        row.entry(key.clone()).or_insert(value.clone());
                    }
                    row
                })
                .collect();
            fields.extend(columns);
        }
    }
    let mut projection = Vec::new();
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                columns.push(match expr {
                    Expr::Identifier(id) => identifier(id),
                    Expr::CompoundIdentifier(ids) => identifier(ids.last().unwrap()),
                    Expr::Function(f) => f.name.to_string().rsplit('.').next().unwrap().into(),
                    _ => "?column?".into(),
                });
                projection.push(expr.clone());
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(identifier(alias));
                projection.push(expr.clone());
            }
            SelectItem::Wildcard(_) => {
                for (key, name) in &fields {
                    columns.push(name.clone());
                    projection.push(Expr::Identifier(Ident::with_quote('"', key)));
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                let qualifier = identifier(
                    name.0
                        .last()
                        .and_then(ObjectNamePart::as_ident)
                        .ok_or("invalid wildcard qualifier")?,
                );
                for (key, name) in &fields {
                    if key.starts_with(&format!("{qualifier}.")) {
                        columns.push(name.clone());
                        projection.push(Expr::Identifier(Ident::with_quote('"', key)));
                    }
                }
            }
            _ => return Err("unsupported catalog projection".into()),
        }
    }
    let mut selected = Vec::new();
    for row in rows {
        if let Some(predicate) = &select.selection {
            if eval(predicate, &row, provider, session)?.as_deref() != Some("t") {
                continue;
            }
        }
        let values = projection
            .iter()
            .map(|e| eval(e, &row, provider, session))
            .collect::<Result<Vec<_>>>()?;
        let mut ordering = Vec::new();
        if let Some(order) = &query.order_by {
            let OrderByKind::Expressions(expressions) = &order.kind else {
                return Err("unsupported catalog ordering".into());
            };
            for order in expressions {
                let value = if let Expr::Value(v) = &order.expr {
                    if let Value::Number(n, _) = &v.value {
                        values
                            .get(
                                n.parse::<usize>()
                                    .map_err(|_| "invalid ORDER BY position")?
                                    .saturating_sub(1),
                            )
                            .cloned()
                            .ok_or("invalid ORDER BY position")?
                    } else {
                        eval(&order.expr, &row, provider, session)?
                    }
                } else if let Expr::Identifier(id) = &order.expr {
                    match columns.iter().position(|c| c == &identifier(id)) {
                        Some(i) => values[i].clone(),
                        None => eval(&order.expr, &row, provider, session)?,
                    }
                } else {
                    eval(&order.expr, &row, provider, session)?
                };
                ordering.push((
                    value,
                    order.options.asc.unwrap_or(true),
                    order.options.nulls_first,
                ));
            }
        }
        selected.push((values, ordering));
    }
    selected.sort_by(|a, b| {
        for ((a, asc, nulls_first), (b, _, _)) in a.1.iter().zip(&b.1) {
            let order = match (a, b) {
                (None, None) => Ordering::Equal,
                (None, _) => {
                    if nulls_first.unwrap_or(!asc) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, None) => {
                    if nulls_first.unwrap_or(!asc) {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(a), Some(b)) => {
                    let c = compare(a, b);
                    if *asc {
                        c
                    } else {
                        c.reverse()
                    }
                }
            };
            if order != Ordering::Equal {
                return order;
            }
        }
        Ordering::Equal
    });
    let mut offset = 0;
    let mut limit = usize::MAX;
    if let Some(LimitClause::LimitOffset {
        limit: l,
        offset: o,
        ..
    }) = &query.limit_clause
    {
        if let Some(l) = l {
            limit = eval(l, outer, provider, session)?
                .ok_or("NULL LIMIT")?
                .parse()
                .map_err(|_| "invalid LIMIT")?
        }
        if let Some(o) = o {
            offset = eval(&o.value, outer, provider, session)?
                .ok_or("NULL OFFSET")?
                .parse()
                .map_err(|_| "invalid OFFSET")?
        }
    }
    Ok((
        columns,
        selected
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(values, _)| values)
            .collect(),
    ))
}

fn relation(
    source: &TableFactor,
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
) -> Result<RelationRows> {
    let TableFactor::Table { name, alias, .. } = source else {
        return Err("unsupported catalog relation".into());
    };
    let name = name.to_string();
    let qualifier = alias
        .as_ref()
        .map(|a| identifier(&a.name))
        .unwrap_or_else(|| {
            name.rsplit('.')
                .next()
                .unwrap()
                .trim_matches('"')
                .to_lowercase()
        });
    let Some(CatalogResponse::Rows { columns, rows }) = provider(&name) else {
        return Err(format!("unknown catalog relation: {name}"));
    };
    let fields: Vec<_> = columns
        .iter()
        .map(|c| (format!("{qualifier}.{c}"), c.clone()))
        .collect();
    let rows = rows
        .into_iter()
        .map(|values| {
            fields
                .iter()
                .zip(values)
                .flat_map(|((key, column), value)| {
                    [(key.clone(), value.clone()), (column.clone(), value)]
                })
                .collect()
        })
        .collect();
    Ok((fields, rows))
}

// ponytail: nested-loop joins over tiny reflection catalogs; use the SQL engine if catalog scale requires it.
fn join(
    left: Vec<Row>,
    right: &[Row],
    condition: Option<&Expr>,
    outer: bool,
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
    session: &SessionInfo,
) -> Result<Vec<Row>> {
    let mut result = Vec::new();
    for left in left {
        let mut matched = false;
        for right in right {
            let mut row = left.clone();
            row.extend(right.clone());
            if match condition {
                Some(e) => eval(e, &row, provider, session)?.as_deref() == Some("t"),
                None => true,
            } {
                result.push(row);
                matched = true;
            }
        }
        if outer && !matched {
            result.push(left)
        }
    }
    Ok(result)
}

fn compare(a: &str, b: &str) -> Ordering {
    match (a.parse::<i64>(), b.parse::<i64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

fn boolean(value: bool) -> Option<String> {
    Some(if value { "t" } else { "f" }.into())
}

fn eval(
    expr: &Expr,
    row: &Row,
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
    session: &SessionInfo,
) -> Result<Option<String>> {
    let value = |e| eval(e, row, provider, session);
    Ok(match expr {
        Expr::Identifier(id) => row.get(&identifier(id)).cloned().unwrap_or_default(),
        Expr::CompoundIdentifier(ids) => row
            .get(&ids.iter().map(identifier).collect::<Vec<_>>().join("."))
            .cloned()
            .unwrap_or_default(),
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::EscapedStringLiteral(s) | Value::Number(s, _) => {
                Some(s.clone())
            }
            Value::Boolean(b) => boolean(*b),
            Value::Null => None,
            _ => return Err(format!("unsupported catalog literal: {expr}")),
        },
        Expr::Nested(e) | Expr::Collate { expr: e, .. } => value(e)?,
        Expr::Cast {
            expr: e, data_type, ..
        } => {
            let input = value(e)?;
            let ty = data_type.to_string();
            match ty.rsplit('.').next().unwrap_or(&ty).to_lowercase().as_str() {
                "regclass" => lookup(provider, "pg_class", "oid", input.as_deref(), "relname"),
                "regnamespace" => {
                    lookup(provider, "pg_namespace", "oid", input.as_deref(), "nspname")
                }
                "regtype" => lookup(provider, "pg_type", "oid", input.as_deref(), "format_type"),
                _ => input,
            }
        }
        Expr::IsNull(e) => boolean(value(e)?.is_none()),
        Expr::IsNotNull(e) => boolean(value(e)?.is_some()),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: e,
        } => value(e)?.map(|v| if v == "t" { "f" } else { "t" }.into()),
        Expr::BinaryOp { left, op, right } => {
            let a = value(left)?;
            let b = value(right)?;
            match op {
                BinaryOperator::And => {
                    if a.as_deref() == Some("f") || b.as_deref() == Some("f") {
                        boolean(false)
                    } else if a.is_none() || b.is_none() {
                        None
                    } else {
                        boolean(true)
                    }
                }
                BinaryOperator::Or => {
                    if a.as_deref() == Some("t") || b.as_deref() == Some("t") {
                        boolean(true)
                    } else if a.is_none() || b.is_none() {
                        None
                    } else {
                        boolean(false)
                    }
                }
                _ => match (a, b) {
                    (Some(a), Some(b)) => match op {
                        BinaryOperator::Eq => boolean(a == b),
                        BinaryOperator::NotEq => boolean(a != b),
                        BinaryOperator::Gt => boolean(compare(&a, &b).is_gt()),
                        BinaryOperator::GtEq => boolean(!compare(&a, &b).is_lt()),
                        BinaryOperator::Lt => boolean(compare(&a, &b).is_lt()),
                        BinaryOperator::LtEq => boolean(!compare(&a, &b).is_gt()),
                        BinaryOperator::StringConcat => Some(a + &b),
                        BinaryOperator::PGRegexMatch
                        | BinaryOperator::PGRegexNotMatch
                        | BinaryOperator::PGCustomBinaryOperator(_) => {
                            if let BinaryOperator::PGCustomBinaryOperator(parts) = op {
                                if !matches!(parts.last().map(String::as_str), Some("~" | "!~")) {
                                    return Err(format!("unsupported catalog operator: {op}"));
                                }
                            }
                            let matched = regex::Regex::new(&b)
                                .map_err(|e| format!("invalid catalog regex: {e}"))?
                                .is_match(&a);
                            boolean(
                                if matches!(op, BinaryOperator::PGRegexNotMatch)
                                    || matches!(op, BinaryOperator::PGCustomBinaryOperator(parts) if parts.last().map(String::as_str) == Some("!~"))
                                {
                                    !matched
                                } else {
                                    matched
                                },
                            )
                        }
                        _ => return Err(format!("unsupported catalog operator: {op}")),
                    },
                    _ => None,
                },
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let input = value(expr)?;
            let values = list.iter().map(value).collect::<Result<Vec<_>>>()?;
            if input.is_none() {
                None
            } else if values.contains(&input) {
                boolean(!negated)
            } else if values.contains(&None) {
                None
            } else {
                boolean(*negated)
            }
        }
        Expr::Like {
            expr: input_expr,
            pattern,
            negated,
            ..
        }
        | Expr::ILike {
            expr: input_expr,
            pattern,
            negated,
            ..
        } => match (value(input_expr)?, value(pattern)?) {
            (Some(input), Some(pattern)) => {
                let regex = format!(
                    "{}^{}$",
                    if matches!(expr, Expr::ILike { .. }) {
                        "(?i)"
                    } else {
                        ""
                    },
                    regex::escape(&pattern).replace('%', ".*").replace('_', ".")
                );
                boolean(
                    regex::Regex::new(&regex)
                        .map_err(|e| e.to_string())?
                        .is_match(&input)
                        != *negated,
                )
            }
            _ => None,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand = operand.as_ref().map(|e| value(e)).transpose()?;
            let mut found = None;
            for condition in conditions {
                let test = value(&condition.condition)?;
                if match &operand {
                    Some(op) => op.is_some() && op == &test,
                    None => test.as_deref() == Some("t"),
                } {
                    found = Some(value(&condition.result)?);
                    break;
                }
            }
            match found {
                Some(v) => v,
                None => else_result
                    .as_ref()
                    .map(|e| value(e))
                    .transpose()?
                    .flatten(),
            }
        }
        Expr::Subquery(query) => run(query, provider, session, row)?
            .1
            .first()
            .and_then(|r| r.first())
            .cloned()
            .flatten(),
        Expr::Exists { subquery, negated } => {
            boolean(run(subquery, provider, session, row)?.1.is_empty() == *negated)
        }
        Expr::Function(function) => {
            let name = function.name.to_string().to_lowercase();
            let name = name.rsplit('.').next().unwrap();
            let args = match &function.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => value(e),
                        _ => Err("unsupported catalog function argument".into()),
                    })
                    .collect::<Result<Vec<_>>>()?,
                FunctionArguments::Subquery(query) if name == "array" => {
                    let (_, rows) = run(query, provider, session, row)?;
                    vec![Some(format!(
                        "{{{}}}",
                        rows.iter()
                            .filter_map(|r| r.first()?.as_deref())
                            .collect::<Vec<_>>()
                            .join(",")
                    ))]
                }
                FunctionArguments::None => vec![],
                _ => return Err(format!("unsupported catalog function: {name}")),
            };
            let arg = args.first().and_then(|v| v.as_deref());
            match name {
                "pg_get_userbyid" => arg.map(|_| "rockstream".into()),
                "pg_encoding_to_char" => arg.map(|_| "UTF8".into()),
                "current_database" => Some("rockstream".into()),
                "current_schema" => Some("public".into()),
                "format_type" => lookup(provider, "pg_type", "oid", arg, "format_type"),
                "pg_get_function_result" => {
                    let oid = lookup(provider, "pg_proc", "oid", arg, "prorettype");
                    lookup(provider, "pg_type", "oid", oid.as_deref(), "format_type")
                }
                "pg_get_function_arguments" | "pg_get_function_identity_arguments" => {
                    lookup(provider, "pg_proc", "oid", arg, "proargtypes")
                }
                "pg_get_viewdef" => lookup(provider, "pg_class", "oid", arg, "definition"),
                "pg_get_indexdef" => lookup(provider, "pg_index", "indexrelid", arg, "indexdef"),
                "pg_table_is_visible" | "pg_function_is_visible" | "pg_type_is_visible" => {
                    let (table, column) = match name {
                        "pg_table_is_visible" => ("pg_class", "relnamespace"),
                        "pg_function_is_visible" => ("pg_proc", "pronamespace"),
                        _ => ("pg_type", "typnamespace"),
                    };
                    let oid = lookup(provider, table, "oid", arg, column);
                    let schema = lookup(provider, "pg_namespace", "oid", oid.as_deref(), "nspname");
                    boolean(
                        schema.as_deref() == Some("pg_catalog")
                            || schema.as_ref().is_some_and(|schema| {
                                session
                                    .search_path
                                    .split(',')
                                    .any(|s| s.trim().trim_matches('"') == schema)
                            }),
                    )
                }
                "obj_description"
                | "col_description"
                | "shobj_description"
                | "pg_get_expr"
                | "pg_get_constraintdef"
                | "array_length" => None,
                "array" => args.first().cloned().flatten(),
                "array_to_string" => arg.map(|a| {
                    a.trim_matches(['{', '}'])
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>()
                        .join(args.get(1).and_then(|v| v.as_deref()).unwrap_or(","))
                }),
                _ => return Err(format!("unsupported catalog function: {name}")),
            }
        }
        _ => return Err(format!("unsupported catalog expression: {expr}")),
    })
}

fn lookup(
    provider: &impl Fn(&str) -> Option<CatalogResponse>,
    table: &str,
    key: &str,
    input: Option<&str>,
    column: &str,
) -> Option<String> {
    let input = input?;
    let CatalogResponse::Rows { columns, rows } = provider(table)? else {
        return None;
    };
    let key = columns.iter().position(|c| c == key)?;
    let column = columns.iter().position(|c| c == column)?;
    rows.into_iter()
        .find(|row| row[key].as_deref() == Some(input))?[column]
        .clone()
}
