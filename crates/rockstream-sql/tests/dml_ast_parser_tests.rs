//! Unit and reachability tests for the typed DML AST parser and bounded parser guards (v0.64 Slice 1).

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_sql::dml::{
    parse_dml_statement, DmlStatement, MAX_EXPR_DEPTH, MAX_SQL_STATEMENT_BYTES,
};
use rockstream_sql::frontend::SqlFrontend;
use std::sync::Arc;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("store_id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ]))
}

#[tokio::test]
async fn test_dml_ast_parser_recognizes_insert_update_delete() {
    let frontend = SqlFrontend::new();
    frontend.register_table("t", test_schema()).unwrap();

    // 1. INSERT statement
    let insert_sql = "INSERT INTO t (id, store_id, val) VALUES (1, 100, 'test') RETURNING id, val;";
    let dml = frontend.parse_dml(insert_sql).expect("parse INSERT");
    assert_eq!(dml.table_name(), "t");
    assert_eq!(
        dml.returning(),
        Some(&["id".to_string(), "val".to_string()][..])
    );
    if let DmlStatement::Insert(ins) = &dml {
        assert_eq!(ins.columns, vec!["id", "store_id", "val"]);
        assert_eq!(ins.values.len(), 1);
        assert_eq!(ins.values[0].len(), 3);
    } else {
        panic!("expected InsertStatement");
    }
    frontend.validate_dml(&dml).await.expect("validate INSERT");

    // 2. UPDATE statement
    let update_sql = "UPDATE t SET val = 'updated' WHERE id = 1 AND store_id = 100 RETURNING *;";
    let dml = frontend.parse_dml(update_sql).expect("parse UPDATE");
    assert_eq!(dml.table_name(), "t");
    assert_eq!(dml.returning(), Some(&["*".to_string()][..]));
    if let DmlStatement::Update(upd) = &dml {
        assert_eq!(upd.assignments.len(), 1);
        assert_eq!(upd.assignments[0].column, "val");
        assert!(upd.selection.is_some());
    } else {
        panic!("expected UpdateStatement");
    }
    frontend.validate_dml(&dml).await.expect("validate UPDATE");

    // 3. DELETE statement
    let delete_sql = "DELETE FROM t WHERE id = 1 RETURNING id;";
    let dml = frontend.parse_dml(delete_sql).expect("parse DELETE");
    assert_eq!(dml.table_name(), "t");
    assert_eq!(dml.returning(), Some(&["id".to_string()][..]));
    if let DmlStatement::Delete(del) = &dml {
        assert!(del.selection.is_some());
    } else {
        panic!("expected DeleteStatement");
    }
    frontend.validate_dml(&dml).await.expect("validate DELETE");
}

#[tokio::test]
async fn test_dml_ast_parser_multi_row_insert() {
    let sql = "INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c');";
    let dml = parse_dml_statement(sql).expect("parse multi-row INSERT");
    if let DmlStatement::Insert(ins) = dml {
        assert_eq!(ins.table, "t");
        assert_eq!(ins.columns, vec!["id", "val"]);
        assert_eq!(ins.values.len(), 3);
    } else {
        panic!("expected InsertStatement");
    }
}

#[tokio::test]
async fn test_dml_ast_parser_rejects_oversized_sql() {
    // Generate statement exceeding MAX_SQL_STATEMENT_BYTES (1 MiB)
    let padding = " ".repeat(MAX_SQL_STATEMENT_BYTES + 10);
    let oversized_sql = format!("DELETE FROM t WHERE id = 1{padding};");
    let err = parse_dml_statement(&oversized_sql).expect_err("should reject oversized statement");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1012"),
        "expected RS-1012 error code, got: {msg}"
    );
    assert!(msg.contains("exceeds maximum limit"), "got: {msg}");
}

#[tokio::test]
async fn test_dml_ast_parser_rejects_excessive_expression_depth() {
    // Generate an expression nested deeper than MAX_EXPR_DEPTH (64)
    let mut deeply_nested = "1".to_string();
    for _ in 0..(MAX_EXPR_DEPTH + 10) {
        deeply_nested = format!("({deeply_nested} + 1)");
    }
    let sql = format!("UPDATE t SET val = 'x' WHERE id = {deeply_nested};");
    let err = parse_dml_statement(&sql).expect_err("should reject excessive depth");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1012"),
        "expected RS-1012 error code, got: {msg}"
    );
    assert!(msg.contains("limit exceeded"), "got: {msg}");
}

#[tokio::test]
async fn test_dml_ast_parser_rejects_malformed_syntax() {
    let malformed = "UPDATE t SET;";
    let err = parse_dml_statement(malformed).expect_err("should reject malformed syntax");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1012"),
        "expected RS-1012 error code, got: {msg}"
    );
}

#[tokio::test]
async fn test_dml_ast_parser_rejects_non_dml() {
    let select_sql = "SELECT * FROM t;";
    let err = parse_dml_statement(select_sql).expect_err("should reject SELECT in parse_dml");
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1012"),
        "expected RS-1012 error code, got: {msg}"
    );
}

#[tokio::test]
async fn test_dml_schema_validation_unknown_table_and_column() {
    let frontend = SqlFrontend::new();
    frontend.register_table("t", test_schema()).unwrap();

    // Unknown table
    let dml_unknown_table =
        parse_dml_statement("UPDATE nonexistent SET val = 'x' WHERE id = 1;").unwrap();
    let err = frontend
        .validate_dml(&dml_unknown_table)
        .await
        .expect_err("unknown table");
    assert!(err.to_string().contains("RS-1012"));
    assert!(err.to_string().contains("nonexistent"));

    // Unknown column in UPDATE
    let dml_unknown_col = parse_dml_statement("UPDATE t SET bad_col = 'x' WHERE id = 1;").unwrap();
    let err = frontend
        .validate_dml(&dml_unknown_col)
        .await
        .expect_err("unknown column");
    assert!(err.to_string().contains("RS-1012"));
    assert!(err.to_string().contains("bad_col"));

    // Unknown column in INSERT
    let dml_unknown_insert =
        parse_dml_statement("INSERT INTO t (id, missing_col) VALUES (1, 'x');").unwrap();
    let err = frontend
        .validate_dml(&dml_unknown_insert)
        .await
        .expect_err("unknown insert column");
    assert!(err.to_string().contains("RS-1012"));
    assert!(err.to_string().contains("missing_col"));
}
