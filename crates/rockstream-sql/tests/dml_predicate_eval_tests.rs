//! Tests for SQL three-valued logic, predicate evaluation, and capability error rejection (v0.64 Slice 2).

use arrow::datatypes::DataType;
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

fn types() -> HashMap<String, DataType> {
    [
        ("a", DataType::Int64),
        ("b", DataType::Int64),
        ("id", DataType::Int64),
        ("age", DataType::Int64),
        ("score", DataType::Int64),
        ("order_id", DataType::Int64),
        ("store_id", DataType::Int64),
        ("customer_id", DataType::Int64),
        ("amount", DataType::Float64),
        ("name", DataType::Utf8),
        ("status", DataType::Utf8),
        ("notes", DataType::Utf8),
        ("address", DataType::Utf8),
        ("val", DataType::Utf8),
        ("any_col", DataType::Utf8),
        ("active", DataType::Boolean),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

#[test]
fn declared_types_preserve_ambiguous_text_and_numeric_boundaries() {
    let cases = [
        (
            DataType::Utf8,
            "v = '1'",
            vec![Some("01"), Some("1"), Some("true"), Some("t"), None],
            vec![
                TriBool::False,
                TriBool::True,
                TriBool::False,
                TriBool::False,
                TriBool::Null,
            ],
        ),
        (
            DataType::Utf8,
            "v = 'true'",
            vec![Some("true"), Some("t"), Some("TRUE"), None],
            vec![TriBool::True, TriBool::False, TriBool::False, TriBool::Null],
        ),
        (
            DataType::Boolean,
            "v = true",
            vec![Some("true"), Some("t"), Some("false"), None],
            vec![TriBool::True, TriBool::True, TriBool::False, TriBool::Null],
        ),
        (
            DataType::Float64,
            "v = 0",
            vec![Some("0"), Some("-0"), Some("1e-17"), None],
            vec![TriBool::True, TriBool::True, TriBool::False, TriBool::Null],
        ),
        (
            DataType::Float64,
            "v <> 0",
            vec![Some("0"), Some("1e-17"), None],
            vec![TriBool::False, TriBool::True, TriBool::Null],
        ),
        (
            DataType::Decimal128(20, 2),
            "v = 9007199254740993.00",
            vec![
                Some("9007199254740992.00"),
                Some("9007199254740993.00"),
                None,
            ],
            vec![TriBool::False, TriBool::True, TriBool::Null],
        ),
    ];
    for (data_type, expression, values, expected) in cases {
        let types = HashMap::from([("v".to_string(), data_type)]);
        let predicate = lower_predicate(&parse_expr(expression)).unwrap();
        let actual: Vec<_> = values
            .into_iter()
            .map(|v| predicate.evaluate(&row(&[("v", v)]), &types).unwrap())
            .collect();
        assert_eq!(actual, expected, "{expression}");
    }
}

#[test]
fn decimal_assignments_are_exact_and_checked_at_declared_precision() {
    use rockstream_sql::dml::lower_scalar_expr;
    for (sql, expected) in [
        ("9007199254740993.00 + 1", "9007199254740994.00"),
        ("0.1 + 0.2", "0.3"),
        ("1e-17", "0.00000000000000001"),
    ] {
        assert_eq!(
            lower_scalar_expr(&parse_expr(sql))
                .unwrap()
                .evaluate(&HashMap::new(), &HashMap::new())
                .unwrap(),
            Some(expected.to_string())
        );
    }
    assert_eq!(
        lower_scalar_expr(&parse_expr("1e-2147483648"))
            .unwrap()
            .evaluate(&HashMap::new(), &HashMap::new())
            .unwrap_err()
            .to_string(),
        "[RS-1012] SQL parse error: [RS-1016] numeric literal exceeds decimal precision"
    );
    let types = HashMap::from([("v".to_string(), DataType::Decimal128(38, 2))]);
    let expression = lower_scalar_expr(&parse_expr("v + 1")).unwrap();
    assert_eq!(
        expression
            .evaluate_assignment(
                &row(&[("v", Some("9007199254740993.00"))]),
                &types,
                &DataType::Decimal128(20, 2)
            )
            .unwrap(),
        Some("9007199254740994.00".to_string())
    );
    assert_eq!(
        expression
            .evaluate_assignment(
                &row(&[("v", Some("99999999999999999999999999999999999.00"))]),
                &types,
                &DataType::Decimal128(38, 2)
            )
            .unwrap(),
        Some("100000000000000000000000000000000000.00".to_string())
    );
    let error = expression
        .evaluate_assignment(
            &row(&[("v", Some("999.00"))]),
            &types,
            &DataType::Decimal128(5, 2),
        )
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[RS-1012] SQL parse error: [RS-1016] Arrow error: Invalid argument error: 1000.00 is too large to store in a Decimal128 of precision 5. Max is 999.99"
    );
    assert_eq!(
        expression
            .evaluate_assignment(&row(&[("v", None)]), &types, &DataType::Decimal128(20, 2))
            .unwrap(),
        None
    );
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
    assert_eq!(pred.evaluate(&r_both, &types()).unwrap(), TriBool::True);

    let r_one_false = row(&[("a", Some("1")), ("b", Some("3"))]);
    assert_eq!(
        pred.evaluate(&r_one_false, &types()).unwrap(),
        TriBool::False
    );

    let r_one_null = row(&[("a", Some("1")), ("b", None)]);
    assert_eq!(pred.evaluate(&r_one_null, &types()).unwrap(), TriBool::Null);

    let r_false_and_null = row(&[("a", Some("0")), ("b", None)]);
    assert_eq!(
        pred.evaluate(&r_false_and_null, &types()).unwrap(),
        TriBool::False
    );
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
    assert_eq!(
        pred.evaluate(&r_one_true_one_null, &types()).unwrap(),
        TriBool::True
    );

    let r_false_and_null = row(&[("a", Some("0")), ("b", None)]);
    assert_eq!(
        pred.evaluate(&r_false_and_null, &types()).unwrap(),
        TriBool::Null
    );

    let r_both_false = row(&[("a", Some("0")), ("b", Some("0"))]);
    assert_eq!(
        pred.evaluate(&r_both_false, &types()).unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_not_three_valued_logic() {
    assert_eq!(TriBool::True.not(), TriBool::False);
    assert_eq!(TriBool::False.not(), TriBool::True);
    assert_eq!(TriBool::Null.not(), TriBool::Null);

    let pred = lower_predicate(&parse_expr("NOT (a = 1)")).unwrap();
    assert_eq!(
        pred.evaluate(&row(&[("a", Some("1"))]), &types()).unwrap(),
        TriBool::False
    );
    assert_eq!(
        pred.evaluate(&row(&[("a", Some("2"))]), &types()).unwrap(),
        TriBool::True
    );
    assert_eq!(
        pred.evaluate(&row(&[("a", None)]), &types()).unwrap(),
        TriBool::Null
    );
}

#[test]
fn test_predicate_equality_across_types() {
    // Integer equality
    let p_int = lower_predicate(&parse_expr("id = 42")).unwrap();
    assert_eq!(
        p_int
            .evaluate(&row(&[("id", Some("42"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_int
            .evaluate(&row(&[("id", Some("43"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_int.evaluate(&row(&[("id", None)]), &types()).unwrap(),
        TriBool::Null
    );

    // Text equality
    let p_text = lower_predicate(&parse_expr("name = 'alice'")).unwrap();
    assert_eq!(
        p_text
            .evaluate(&row(&[("name", Some("alice"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_text
            .evaluate(&row(&[("name", Some("bob"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_text.evaluate(&row(&[("name", None)]), &types()).unwrap(),
        TriBool::Null
    );

    // Boolean equality
    let p_bool = lower_predicate(&parse_expr("active = true")).unwrap();
    assert_eq!(
        p_bool
            .evaluate(&row(&[("active", Some("true"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_bool
            .evaluate(&row(&[("active", Some("false"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_bool
            .evaluate(&row(&[("active", None)]), &types())
            .unwrap(),
        TriBool::Null
    );

    // NULL = NULL evaluates to NULL in standard SQL
    let p_null_eq = lower_predicate(&parse_expr("val = NULL")).unwrap();
    assert_eq!(
        p_null_eq
            .evaluate(&row(&[("val", None)]), &types())
            .unwrap(),
        TriBool::Null
    );
    assert_eq!(
        p_null_eq
            .evaluate(&row(&[("val", Some("hello"))]), &types())
            .unwrap(),
        TriBool::Null
    );
}

#[test]
fn test_predicate_inequality_across_types() {
    let p_neq = lower_predicate(&parse_expr("status <> 'deleted'")).unwrap();
    assert_eq!(
        p_neq
            .evaluate(&row(&[("status", Some("active"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_neq
            .evaluate(&row(&[("status", Some("deleted"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_neq.evaluate(&row(&[("status", None)]), &types()).unwrap(),
        TriBool::Null
    );

    let p_bang = lower_predicate(&parse_expr("id != 10")).unwrap();
    assert_eq!(
        p_bang
            .evaluate(&row(&[("id", Some("20"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_bang
            .evaluate(&row(&[("id", Some("10"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_bang.evaluate(&row(&[("id", None)]), &types()).unwrap(),
        TriBool::Null
    );
}

#[test]
fn test_predicate_less_than_comparison() {
    let p_lt = lower_predicate(&parse_expr("age < 18")).unwrap();
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("17"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("18"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", Some("25"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_lt.evaluate(&row(&[("age", None)]), &types()).unwrap(),
        TriBool::Null
    );

    let p_lte = lower_predicate(&parse_expr("age <= 18")).unwrap();
    assert_eq!(
        p_lte
            .evaluate(&row(&[("age", Some("18"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_lte
            .evaluate(&row(&[("age", Some("19"))]), &types())
            .unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_greater_than_comparison() {
    let p_gt = lower_predicate(&parse_expr("score > 100")).unwrap();
    assert_eq!(
        p_gt.evaluate(&row(&[("score", Some("101"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_gt.evaluate(&row(&[("score", Some("100"))]), &types())
            .unwrap(),
        TriBool::False
    );
    assert_eq!(
        p_gt.evaluate(&row(&[("score", None)]), &types()).unwrap(),
        TriBool::Null
    );

    let p_gte = lower_predicate(&parse_expr("score >= 100")).unwrap();
    assert_eq!(
        p_gte
            .evaluate(&row(&[("score", Some("100"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_gte
            .evaluate(&row(&[("score", Some("99"))]), &types())
            .unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_is_null() {
    let p_null = lower_predicate(&parse_expr("address IS NULL")).unwrap();
    assert_eq!(
        p_null
            .evaluate(&row(&[("address", None)]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_null
            .evaluate(&row(&[("address", Some("Main St"))]), &types())
            .unwrap(),
        TriBool::False
    );
}

#[test]
fn test_predicate_is_not_null() {
    let p_not_null = lower_predicate(&parse_expr("address IS NOT NULL")).unwrap();
    assert_eq!(
        p_not_null
            .evaluate(&row(&[("address", Some("Main St"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(
        p_not_null
            .evaluate(&row(&[("address", None)]), &types())
            .unwrap(),
        TriBool::False
    );
}

#[test]
fn test_multi_column_and_predicate() {
    let p_multi = lower_predicate(&parse_expr("order_id = 1 AND store_id = 100")).unwrap();

    let r_match = row(&[("order_id", Some("1")), ("store_id", Some("100"))]);
    assert_eq!(p_multi.evaluate(&r_match, &types()).unwrap(), TriBool::True);

    let r_diff_store = row(&[("order_id", Some("1")), ("store_id", Some("101"))]);
    assert_eq!(
        p_multi.evaluate(&r_diff_store, &types()).unwrap(),
        TriBool::False
    );

    let r_diff_order = row(&[("order_id", Some("2")), ("store_id", Some("100"))]);
    assert_eq!(
        p_multi.evaluate(&r_diff_order, &types()).unwrap(),
        TriBool::False
    );
}

#[test]
fn test_dml_without_where_clause() {
    let p_none = DmlPredicate::AlwaysTrue;
    assert_eq!(
        p_none
            .evaluate(&row(&[("any_col", Some("value"))]), &types())
            .unwrap(),
        TriBool::True
    );
    assert_eq!(p_none.evaluate(&row(&[]), &types()).unwrap(), TriBool::True);
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
    assert_eq!(
        pred.evaluate(&matching_row, &types()).unwrap(),
        TriBool::True
    );

    let non_matching_row = row(&[
        ("customer_id", Some("50")),
        ("amount", Some("50.0")),
        ("notes", Some("Standard")),
    ]);
    assert_eq!(
        pred.evaluate(&non_matching_row, &types()).unwrap(),
        TriBool::False
    );
}
