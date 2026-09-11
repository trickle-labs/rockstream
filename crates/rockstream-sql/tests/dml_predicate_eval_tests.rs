//! Tests for SQL three-valued logic, predicate evaluation, and capability error rejection (v0.64 Slice 2).

use sqlparser::ast::Expr as SpExpr;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

use rockstream_sql::dml::{lower_predicate, parse_dml_statement, DmlPredicate, TriBool};

fn parse_expr(sql_expr: &str) -> SpExpr {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql_expr)
        .expect("parser creation");
    parser.parse_expr().expect("parse expr")
}

fn row(pairs: &[(&str, Option<&str>)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
        .collect()
}

#[test]
fn test_predicate_and_three_valued_logic() {
    assert_eq!(TriBool::True.and(TriBool::True), TriBool::True);
    assert_eq!(TriBool::True.and(TriBool::False), TriBool::False);
    assert_eq!(TriBool::False.and(TriBool::True), TriBool::False);
    assert_eq!(TriBool::False.and(TriBool::False), TriBool::False);
    assert_eq!(TriBool::True.and(TriBool::Null), TriBool::Null);
    assert_eq!(TriBool::Null.and(TriBool::True), TriBool::Null);
    assert_eq!(TriBool::False.and(TriBool::Null), TriBool::False);
    assert_eq!(TriBool::Null.and(TriBool::False), TriBool::False);
    assert_eq!(TriBool::Null.and(TriBool::Null), TriBool::Null);

    // Evaluate via SQL AST
    let pred = lower_predicate(&parse_expr("a = 1 AND b = 2")).unwrap();

    let r_both = row(&[("a", Some("1")), ("b", Some("2"))]);
    assert_eq!(pred.evaluate(&r_both).unwrap(), TriBool::True);

    let r_one_false = row(&[("a", Some("1")), ("b", Some("3"))]);
    assert_eq!(pred.evaluate(&r_one_false).unwrap(), TriBool::False);

    let r_one_null = row(&[("a", Some("1")), ("b", None)]);
    assert_eq!(pred.evaluate(&r_one_null).unwrap(), TriBool::Null);

    let r_false_and_null = row(&[("a", Some("0")), ("b", None)]);
    assert_eq!(pred.evaluate(&r_false_and_null).unwrap(), TriBool::False);
}

#[test]
fn test_predicate_or_three_valued_logic() {
    assert_eq!(TriBool::True.or(TriBool::True), TriBool::True);
    assert_eq!(TriBool::True.or(TriBool::False), TriBool::True);
    assert_eq!(TriBool::False.or(TriBool::True), TriBool::True);
    assert_eq!(TriBool::False.or(TriBool::False), TriBool::False);
    assert_eq!(TriBool::True.or(TriBool::Null), TriBool::True);
    assert_eq!(TriBool::Null.or(TriBool::True), TriBool::True);
    assert_eq!(TriBool::False.or(TriBool::Null), TriBool::Null);
    assert_eq!(TriBool::Null.or(TriBool::False), TriBool::Null);
    assert_eq!(TriBool::Null.or(TriBool::Null), TriBool::Null);

    let pred = lower_predicate(&parse_expr("a = 1 OR b = 2")).unwrap();

    let r_one_true_one_null = row(&[("a", Some("1")), ("b", None)]);
    assert_eq!(pred.evaluate(&r_one_true_one_null).unwrap(), TriBool::True);

    let r_false_and_null = row(&[("a", Some("0")), ("b", None)]);
    assert_eq!(pred.evaluate(&r_false_and_null).unwrap(), TriBool::Null);

    let r_both_false = row(&[("a", Some("0")), ("b", Some("0"))]);
    assert_eq!(pred.evaluate(&r_both_false).unwrap(), TriBool::False);
}

#[test]
fn test_predicate_not_three_valued_logic() {
    assert_eq!(TriBool::True.not(), TriBool::False);
    assert_eq!(TriBool::False.not(), TriBool::True);
    assert_eq!(TriBool::Null.not(), TriBool::Null);

    let pred = lower_predicate(&parse_expr("NOT (a = 1)")).unwrap();
    assert_eq!(
        pred.evaluate(&row(&[("a", Some("1"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        pred.evaluate(&row(&[("a", Some("2"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(pred.evaluate(&row(&[("a", None)])).unwrap(), TriBool::Null);
}

#[test]
fn test_predicate_equality_across_types() {
    // Integer equality
    let p_int = lower_predicate(&parse_expr("id = 42")).unwrap();
    assert_eq!(
        p_int.evaluate(&row(&[("id", Some("42"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_int.evaluate(&row(&[("id", Some("43"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_int.evaluate(&row(&[("id", None)])).unwrap(),
        TriBool::Null
    );

    // Text equality
    let p_text = lower_predicate(&parse_expr("name = 'alice'")).unwrap();
    assert_eq!(
        p_text.evaluate(&row(&[("name", Some("alice"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_text.evaluate(&row(&[("name", Some("bob"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_text.evaluate(&row(&[("name", None)])).unwrap(),
        TriBool::Null
    );

    // Boolean equality
    let p_bool = lower_predicate(&parse_expr("active = true")).unwrap();
    assert_eq!(
        p_bool.evaluate(&row(&[("active", Some("true"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_bool.evaluate(&row(&[("active", Some("false"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_bool.evaluate(&row(&[("active", None)])).unwrap(),
        TriBool::Null
    );

    // NULL = NULL evaluates to NULL in standard SQL
    let p_null_eq = lower_predicate(&parse_expr("val = NULL")).unwrap();
    assert_eq!(
        p_null_eq.evaluate(&row(&[("val", None)])).unwrap(),
        TriBool::Null
    );
    assert_eq!(
        p_null_eq.evaluate(&row(&[("val", Some("hello"))])).unwrap(),
        TriBool::Null
    );
}

#[test]
fn test_predicate_inequality_across_types() {
    let p_neq = lower_predicate(&parse_expr("status <> 'deleted'")).unwrap();
    assert_eq!(
        p_neq.evaluate(&row(&[("status", Some("active"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_neq
            .evaluate(&row(&[("status", Some("deleted"))]))
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_neq.evaluate(&row(&[("status", None)])).unwrap(),
        TriBool::Null
    );

    let p_bang = lower_predicate(&parse_expr("id != 10")).unwrap();
    assert_eq!(
        p_bang.evaluate(&row(&[("id", Some("20"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_bang.evaluate(&row(&[("id", Some("10"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_bang.evaluate(&row(&[("id", None)])).unwrap(),
        TriBool::Null
    );
}

#[test]
fn test_predicate_less_than_comparison() {
    let p_lt = lower_predicate(&parse_expr("age < 18")).unwrap();
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("17"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("18"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("25"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", None)])).unwrap(),
        TriBool::Null
    );

    let p_lte = lower_predicate(&parse_expr("age <= 18")).unwrap();
    assert_eq!(
        p_lte.evaluate(&row(&[("age", Some("18"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_lte.evaluate(&row(&[("age", Some("19"))])).unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_greater_than_comparison() {
    let p_gt = lower_predicate(&parse_expr("score > 100")).unwrap();
    assert_eq!(
        p_gt.evaluate(&row(&[("score", Some("101"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_gt.evaluate(&row(&[("score", Some("100"))])).unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_gt.evaluate(&row(&[("score", None)])).unwrap(),
        TriBool::Null
    );

    let p_gte = lower_predicate(&parse_expr("score >= 100")).unwrap();
    assert_eq!(
        p_gte.evaluate(&row(&[("score", Some("100"))])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_gte.evaluate(&row(&[("score", Some("99"))])).unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_is_null() {
    let p_null = lower_predicate(&parse_expr("address IS NULL")).unwrap();
    assert_eq!(
        p_null.evaluate(&row(&[("address", None)])).unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_null
            .evaluate(&row(&[("address", Some("Main St"))]))
            .unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_is_not_null() {
    let p_not_null = lower_predicate(&parse_expr("address IS NOT NULL")).unwrap();
    assert_eq!(
        p_not_null
            .evaluate(&row(&[("address", Some("Main St"))]))
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_not_null.evaluate(&row(&[("address", None)])).unwrap(),
        TriBool::False
    );
}

#[test]
fn test_multi_column_and_predicate() {
    let p_multi = lower_predicate(&parse_expr("order_id = 1 AND store_id = 100")).unwrap();

    let r_match = row(&[("order_id", Some("1")), ("store_id", Some("100"))]);
    assert_eq!(p_multi.evaluate(&r_match).unwrap(), TriBool::True);

    let r_diff_store = row(&[("order_id", Some("1")), ("store_id", Some("101"))]);
    assert_eq!(p_multi.evaluate(&r_diff_store).unwrap(), TriBool::False);

    let r_diff_order = row(&[("order_id", Some("2")), ("store_id", Some("100"))]);
    assert_eq!(p_multi.evaluate(&r_diff_order).unwrap(), TriBool::False);
}

#[test]
fn test_dml_without_where_clause() {
    let p_none = DmlPredicate::AlwaysTrue;
    assert_eq!(
        p_none
            .evaluate(&row(&[("any_col", Some("value"))]))
            .unwrap(),
        TriBool::True
    );
    assert_eq!(p_none.evaluate(&row(&[])).unwrap(), TriBool::True);
}

#[test]
fn test_unsupported_subquery_predicate_rejection() {
    let subquery_expr = parse_expr("id IN (SELECT id FROM other)");
    let err = lower_predicate(&subquery_expr).expect_err("subquery must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1013"),
        "expected RS-1013 error code, got: {msg}"
    );
}

#[test]
fn test_unsupported_udf_predicate_rejection() {
    let udf_expr = parse_expr("non_deterministic_func(id) > 10");
    let err = lower_predicate(&udf_expr).expect_err("UDF in predicate must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1013"),
        "expected RS-1013 error code, got: {msg}"
    );
}

#[test]
fn test_predicate_evaluator_general_subset_and_capability_errors() {
    // End-to-end integration of full predicate capability
    let sql = "UPDATE orders SET status = 'shipped' WHERE customer_id = 50 AND amount >= 100.0 AND notes IS NOT NULL;";
    let dml = parse_dml_statement(sql).expect("parse UPDATE");
    let sel = match dml {
        rockstream_sql::dml::DmlStatement::Update(u) => u.selection.expect("selection exists"),
        _ => panic!("expected Update"),
    };
    let pred = lower_predicate(&sel).expect("lower predicate");

    let matching_row = row(&[
        ("customer_id", Some("50")),
        ("amount", Some("150.0")),
        ("notes", Some("Leave at front porch")),
    ]);
    assert_eq!(pred.evaluate(&matching_row).unwrap(), TriBool::True);

    let non_matching_row = row(&[
        ("customer_id", Some("50")),
        ("amount", Some("50.0")),
        ("notes", Some("Standard")),
    ]);
    assert_eq!(pred.evaluate(&non_matching_row).unwrap(), TriBool::False);
}
