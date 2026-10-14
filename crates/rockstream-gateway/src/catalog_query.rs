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
        return Err("[RS-2026] unsupported catalog query body".into());
    };
    if select.distinct.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty())
    {
        return Err("[RS-2026] unsupported catalog aggregation".into());
    }
    let mut rows = vec![outer.clone()];
    let mut fields = Vec::new();
    for source in &select.from {
        let (columns, right) = relation(&source.relation, provider, session, outer)?;
        rows = join(rows, &right, None, false, provider, session)?;
        fields.extend(columns);
        for item in &source.joins {
            let (columns, right) = relation(&item.relation, provider, session, outer)?;
            let (constraint, left) = match &item.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (c, false)
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, true),
                _ => return Err("[RS-2026] unsupported catalog join".into()),
            };
            let condition = match constraint {
                JoinConstraint::On(expr) => Some(expr),
                JoinConstraint::None => None,
                _ => return Err("[RS-2026] unsupported catalog join constraint".into()),
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
                        .ok_or("[RS-2026] invalid wildcard qualifier")?,
                );
                for (key, name) in &fields {
                    if key.starts_with(&format!("{qualifier}.")) {
                        columns.push(name.clone());
                        projection.push(Expr::Identifier(Ident::with_quote('"', key)));
                    }
                }
            }
            _ => return Err("[RS-2026] unsupported catalog projection".into()),
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
                return Err("[RS-2026] unsupported catalog ordering".into());
            };
            for order in expressions {
                let value = if let Expr::Value(v) = &order.expr {
                    if let Value::Number(n, _) = &v.value {
                        values
                            .get(
                                n.parse::<usize>()
                                    .map_err(|_| "[RS-2026] invalid ORDER BY position")?
                                    .saturating_sub(1),
                            )
                            .cloned()
                            .ok_or("[RS-2026] invalid ORDER BY position")?
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
                .ok_or("[RS-2026] NULL LIMIT")?
                .parse()
                .map_err(|_| "[RS-2026] invalid LIMIT")?
        }
        if let Some(o) = o {
            offset = eval(&o.value, outer, provider, session)?
                .ok_or("[RS-2026] NULL OFFSET")?
                .parse()
                .map_err(|_| "[RS-2026] invalid OFFSET")?
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
    session: &SessionInfo,
    outer: &Row,
) -> Result<RelationRows> {
    let (qualifier, columns, rows) = match source {
        TableFactor::Table { name, alias, .. } => {
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
                return Err(format!("[RS-2026] unknown catalog relation: {name}"));
            };
            (qualifier, columns, rows)
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(alias),
            ..
        } => {
            // PostgreSQL clients such as Prisma join a projected catalog
            // subquery (SELECT pg_class.oid, ...) as a relation. Use the same
            // evaluator and preserve its actual rows rather than assuming
            // that a selected catalog binary establishes reflection.
            let (columns, rows) = run(subquery, provider, session, outer)?;
            (identifier(&alias.name), columns, rows)
        }
        _ => return Err("[RS-2026] unsupported catalog relation".into()),
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
        Expr::CompoundIdentifier(ids) => {
            let parts = ids.iter().map(identifier).collect::<Vec<_>>();
            // Relation maps are keyed by their SQL alias or unqualified table
            // name (pg_class.oid). PostgreSQL clients use fully qualified
            // pg_catalog.pg_class.oid identifiers; resolve those to the same
            // column without accidentally matching unrelated bare "oid".
            let key = parts.join(".");
            let schema_qualified = if parts.len() >= 3 {
                row.get(&parts[parts.len() - 2..].join("."))
            } else {
                None
            };
            row.get(&key)
                .or(schema_qualified)
                .cloned()
                .unwrap_or_default()
        }
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::EscapedStringLiteral(s) | Value::Number(s, _) => {
                Some(s.clone())
            }
            Value::Boolean(b) => boolean(*b),
            Value::Null => None,
            _ => return Err(format!("[RS-2026] unsupported catalog literal: {expr}")),
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
                                    return Err(format!(
                                        "[RS-2026] unsupported catalog operator: {op}"
                                    ));
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
                        _ => return Err(format!("[RS-2026] unsupported catalog operator: {op}")),
                    },
                    _ => None,
                },
            }
        }
        // PostgreSQL's reflection queries (including SQLAlchemy get_columns)
        // filter relkinds using `relkind = ANY (ARRAY['r', 'v', ...])`.
        // Evaluate array membership against the actual catalog row; do not
        // silently turn an unsupported predicate into an empty result.
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => {
            let input = value(left)?;
            let members = match right.as_ref() {
                Expr::Array(array) => array.elem.iter().map(value).collect::<Result<Vec<_>>>()?,
                Expr::Nested(e) => match e.as_ref() {
                    Expr::Array(array) => {
                        array.elem.iter().map(value).collect::<Result<Vec<_>>>()?
                    }
                    _ => return Err("[RS-2026] unsupported ANY array expression".into()),
                },
                _ => return Err("[RS-2026] unsupported ANY operand".into()),
            };
            let matched = |a: &str, b: &str| -> Result<bool> {
                Ok(match compare_op {
                    BinaryOperator::Eq => a == b,
                    BinaryOperator::NotEq => a != b,
                    BinaryOperator::Gt => compare(a, b).is_gt(),
                    BinaryOperator::GtEq => !compare(a, b).is_lt(),
                    BinaryOperator::Lt => compare(a, b).is_lt(),
                    BinaryOperator::LtEq => !compare(a, b).is_gt(),
                    _ => return Err("[RS-2026] unsupported ANY comparison".into()),
                })
            };
            match input {
                None => None,
                Some(input) => {
                    let mut has_null = false;
                    let mut any_match = false;
                    for member in members {
                        match member {
                            Some(member) => {
                                if matched(&input, &member)? {
                                    any_match = true;
                                    break;
                                }
                            }
                            None => has_null = true,
                        }
                    }
                    if any_match {
                        boolean(true)
                    } else if has_null {
                        None
                    } else {
                        boolean(false)
                    }
                }
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
                        _ => Err("[RS-2026] unsupported catalog function argument".into()),
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
                _ => return Err(format!("[RS-2026] unsupported catalog function: {name}")),
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
                _ => return Err(format!("[RS-2026] unsupported catalog function: {name}")),
            }
        }
        _ => return Err(format!("[RS-2026] unsupported catalog expression: {expr}")),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_any_array_filters_real_relation_kinds() {
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

        let provider = |name: &str| -> Option<CatalogResponse> {
            if name != "pg_catalog.pg_class" {
                return None;
            }
            Some(CatalogResponse::Rows {
                columns: vec!["relname".to_string(), "relkind".to_string()],
                rows: vec![
                    vec![Some("numeric_view".into()), Some("v".into())],
                    vec![Some("unrelated_table".into()), Some("r".into())],
                ],
            })
        };
        let statement = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT relname FROM pg_catalog.pg_class WHERE relkind = ANY (ARRAY['v', 'm'])",
        )
        .expect("valid PostgreSQL ANY expression")
        .remove(0);
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT query");
        };
        let result = execute(&query, &provider, &SessionInfo::default())
            .expect("catalog query was recognized");
        let CatalogResponse::Rows { columns, rows } = result else {
            panic!("catalog ANY expression must execute successfully");
        };
        assert_eq!(columns, vec!["relname"]);
        assert_eq!(rows, vec![vec![Some("numeric_view".into())]]);
    }

    #[test]
    fn sqlalchemy_style_column_reflection_reads_registered_view() {
        use crate::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};

        let catalog = CatalogStubs::new();
        catalog.add_view(CatalogView {
            name: "numeric_view".into(),
            sql: "SELECT 1 AS c_int2".into(),
            columns: vec![CatalogColumn {
                name: "c_int2".into(),
                data_type: "Int16".into(),
            }],
            namespace: "public".into(),
            op_id: None,
        });

        // Real PostgreSQL reflection shape: pg_class, outer-joined attributes,
        // empty identity/default catalogs, relation-kind ANY and schema scope.
        let query = r#"
            SELECT pg_catalog.pg_attribute.attname AS name,
                   pg_catalog.format_type(pg_catalog.pg_attribute.atttypid,
                                          pg_catalog.pg_attribute.atttypmod) AS format_type,
                   (SELECT pg_catalog.pg_get_expr(pg_catalog.pg_attrdef.adbin,
                                                   pg_catalog.pg_attrdef.adrelid)
                      FROM pg_catalog.pg_attrdef
                     WHERE pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid
                       AND pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum
                       AND pg_catalog.pg_attribute.atthasdef) AS "default",
                   pg_catalog.pg_attribute.attnotnull AS not_null,
                   pg_catalog.pg_class.relname AS table_name,
                   pg_catalog.pg_description.description AS comment,
                   pg_catalog.pg_attribute.attgenerated AS generated,
                   (SELECT pg_catalog.pg_sequence.seqstart
                      FROM pg_catalog.pg_sequence
                     WHERE pg_catalog.pg_attribute.attidentity != '') AS identity_options
              FROM pg_catalog.pg_class
              LEFT OUTER JOIN pg_catalog.pg_attribute
                ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid
               AND pg_catalog.pg_attribute.attnum > 0
               AND NOT pg_catalog.pg_attribute.attisdropped
              LEFT OUTER JOIN pg_catalog.pg_description
                ON pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid
               AND pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum
              JOIN pg_catalog.pg_namespace
                ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
             WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm'])
               AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)
               AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
               AND pg_catalog.pg_class.relname IN ('numeric_view')
             ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum
        "#;
        let Some(CatalogResponse::Rows { columns, rows }) =
            catalog.handle_query(query, &SessionInfo::default())
        else {
            panic!("SQLAlchemy-style reflection must resolve to catalog rows");
        };
        assert_eq!(columns[0], "name");
        assert_eq!(columns[1], "format_type");
        assert_eq!(columns[4], "table_name");
        assert_eq!(rows.len(), 1, "a registered view must be reflected");
        assert_eq!(rows[0][0].as_deref(), Some("c_int2"));
        assert_eq!(rows[0][1].as_deref(), Some("smallint"));
        assert_eq!(rows[0][4].as_deref(), Some("numeric_view"));
    }

    #[test]
    fn prisma_derived_catalog_join_returns_existing_view_columns() {
        use crate::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};

        let catalog = CatalogStubs::new();
        catalog.add_view(CatalogView {
            name: "numeric_view".into(),
            sql: "SELECT 1 AS c_int2".into(),
            columns: vec![CatalogColumn {
                name: "c_int2".into(),
                data_type: "Int16".into(),
            }],
            namespace: "public".into(),
            op_id: None,
        });
        // Mirrors Prisma's nested pg_class/pg_namespace catalog join. The
        // array is the already-bound value of Prisma's $1 schema parameter.
        let query = r#"
            SELECT oid.namespace, info.table_name, info.column_name,
                   format_type(att.atttypid, att.atttypmod) AS formatted_type,
                   info.data_type, info.is_nullable
            FROM information_schema.columns info
            JOIN pg_attribute att ON att.attname = info.column_name
            JOIN (
                SELECT pg_class.oid, relname, pg_namespace.nspname AS namespace
                FROM pg_class
                JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                    AND pg_namespace.nspname = ANY (ARRAY['public'])
                WHERE reltype > 0
            ) AS oid ON oid.oid = att.attrelid
                AND relname = info.table_name
                AND namespace = info.table_schema
            WHERE table_schema = ANY (ARRAY['public'])
            ORDER BY namespace, table_name, ordinal_position
        "#;
        let Some(CatalogResponse::Rows { columns, rows }) =
            catalog.handle_query(query, &SessionInfo::default())
        else {
            panic!("Prisma derived catalog reflection must return rows");
        };
        assert_eq!(
            columns,
            vec![
                "namespace",
                "table_name",
                "column_name",
                "formatted_type",
                "data_type",
                "is_nullable",
            ]
        );
        assert_eq!(rows.len(), 1, "registered view must be discoverable");
        assert_eq!(rows[0][0].as_deref(), Some("public"));
        assert_eq!(rows[0][1].as_deref(), Some("numeric_view"));
        assert_eq!(rows[0][2].as_deref(), Some("c_int2"));
        assert_eq!(rows[0][3].as_deref(), Some("smallint"));
    }

    #[test]
    fn catalog_any_array_rejects_unsupported_rhs() {
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

        let statement = Parser::parse_sql(
            &PostgreSqlDialect {},
            "SELECT relname FROM pg_class WHERE relkind = ANY (subquery_value)",
        )
        .expect("valid expression")
        .remove(0);
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected SELECT query");
        };
        let provider = |name: &str| {
            (name == "pg_class").then(|| CatalogResponse::Rows {
                columns: vec!["relname".into(), "relkind".into()],
                rows: vec![vec![Some("v".into()), Some("v".into())]],
            })
        };
        assert!(matches!(
            execute(&query, &provider, &SessionInfo::default()),
            Some(CatalogResponse::Error { .. })
        ));
    }
}
