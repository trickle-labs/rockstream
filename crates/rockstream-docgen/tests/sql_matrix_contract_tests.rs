//! Tests for SQL Type Compatibility Contract (DOC-001, Matrix B).

use rockstream_docgen::sql_matrix::SqlMatrixDocument;

#[test]
fn test_sql_matrix_toml_parsing_and_validity() {
    let doc =
        SqlMatrixDocument::load_canonical().expect("contracts/sql-type-matrix.toml must parse");
    assert_eq!(doc.contract.version, "0.59.19");
    assert_eq!(doc.contract.roadmap, "DOC-001");
    assert!(!doc.types.is_empty(), "Must contain SQL types");
}

#[test]
fn test_contract_pinned_pg18_digest() {
    let doc =
        SqlMatrixDocument::load_canonical().expect("contracts/sql-type-matrix.toml must parse");
    let ref_db = doc
        .reference_database
        .as_ref()
        .expect("Reference database contract must be present");
    assert_eq!(ref_db.engine, "postgresql");
    assert_eq!(ref_db.version, "18.0");
    assert_eq!(
        ref_db.canonical_image,
        "postgres:18.0@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c"
    );
    assert!(!ref_db.amd64_digest.is_empty());
    assert!(!ref_db.arm64_digest.is_empty());
}

#[test]
fn test_contract_collation_and_limits() {
    let doc =
        SqlMatrixDocument::load_canonical().expect("contracts/sql-type-matrix.toml must parse");
    let collation = doc
        .collation
        .as_ref()
        .expect("Collation contract must be present");
    assert_eq!(collation.name, "rockstream_binary_v1");
    assert_eq!(collation.rejection_code, "RS-1013");

    let num_bounds = doc
        .numeric_bounds
        .as_ref()
        .expect("Numeric bounds contract must be present");
    assert_eq!(num_bounds.max_precision, 38);
    assert_eq!(num_bounds.overflow_code, "RS-1016");

    let limits = doc
        .limits
        .as_ref()
        .expect("Limits contract must be present");
    assert_eq!(limits.max_result_rows, 10000);
    assert_eq!(limits.max_conn_memory_bytes, 64 * 1024 * 1024);
    assert_eq!(limits.max_connections, 100);
    assert_eq!(limits.max_prepared_stmts, 100);
    assert_eq!(limits.max_portals, 50);
    assert_eq!(limits.max_cursors, 64);
    assert_eq!(limits.max_identifier_len, 63);
    assert_eq!(limits.max_decimal_precision, 38);
    assert_eq!(limits.max_view_dag_depth, 16);
}

#[test]
fn test_unsupported_cells_declare_rejection_codes() {
    let doc =
        SqlMatrixDocument::load_canonical().expect("contracts/sql-type-matrix.toml must parse");
    for ty in &doc.types {
        for op in &ty.operations {
            if op.status == "Unsupported" {
                assert!(
                    op.rejection_code.is_some(),
                    "Type {} op {} marked Unsupported must declare rejection_code",
                    ty.name,
                    op.operation
                );
                let code = op.rejection_code.as_ref().unwrap();
                assert!(
                    code.starts_with("RS-"),
                    "Rejection code must follow RS-XXXX format: {}",
                    code
                );
            }
        }
    }
}

#[test]
fn test_integer_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    for name in &["INT2", "INT4", "INT8"] {
        let ty = doc
            .types
            .iter()
            .find(|t| t.name == *name)
            .expect("Integer type must exist");
        assert_eq!(ty.family, "exact_integer");
        for op in &ty.operations {
            assert!(
                op.status == "Core" || op.status == "Supported",
                "Integer {} operation {} must be Core or Supported, got {}",
                name,
                op.operation,
                op.status
            );
        }
    }
}

#[test]
fn test_float_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    for name in &["FLOAT4", "FLOAT8"] {
        let ty = doc
            .types
            .iter()
            .find(|t| t.name == *name)
            .expect("Float type must exist");
        assert_eq!(ty.family, "floating_point");
        let join_op = ty
            .operations
            .iter()
            .find(|o| o.operation == "joins")
            .unwrap();
        assert_eq!(join_op.status, "Unsupported");
        assert_eq!(join_op.rejection_code.as_deref(), Some("RS-1019"));
    }
}

#[test]
fn test_decimal_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    for name in &["NUMERIC", "DECIMAL"] {
        let ty = doc
            .types
            .iter()
            .find(|t| t.name == *name)
            .expect("Decimal type must exist");
        assert_eq!(ty.family, "decimal");
        let arith_op = ty
            .operations
            .iter()
            .find(|o| o.operation == "arithmetic")
            .unwrap();
        assert_eq!(arith_op.status, "Supported");
    }
}

#[test]
fn test_boolean_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    let ty = doc
        .types
        .iter()
        .find(|t| t.name == "BOOLEAN")
        .expect("BOOLEAN must exist");
    assert_eq!(ty.family, "boolean");
    let arith_op = ty
        .operations
        .iter()
        .find(|o| o.operation == "arithmetic")
        .unwrap();
    assert_eq!(arith_op.status, "Unsupported");
    assert_eq!(arith_op.rejection_code.as_deref(), Some("RS-1012"));
}

#[test]
fn test_string_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    for name in &["TEXT", "VARCHAR"] {
        let ty = doc
            .types
            .iter()
            .find(|t| t.name == *name)
            .expect("String type must exist");
        assert_eq!(ty.family, "character_string");
        let cmp_op = ty
            .operations
            .iter()
            .find(|o| o.operation == "comparison")
            .unwrap();
        assert_eq!(cmp_op.status, "Supported");
    }
}

#[test]
fn test_bytea_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    let ty = doc
        .types
        .iter()
        .find(|t| t.name == "BYTEA")
        .expect("BYTEA must exist");
    assert_eq!(ty.family, "binary");
    let arith_op = ty
        .operations
        .iter()
        .find(|o| o.operation == "arithmetic")
        .unwrap();
    assert_eq!(arith_op.status, "Unsupported");
    assert_eq!(arith_op.rejection_code.as_deref(), Some("RS-1012"));
}

#[test]
fn test_temporal_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    for name in &["DATE", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL"] {
        let ty = doc
            .types
            .iter()
            .find(|t| t.name == *name)
            .expect("Temporal type must exist");
        assert_eq!(ty.family, "temporal");
        let cmp_op = ty
            .operations
            .iter()
            .find(|o| o.operation == "comparison")
            .unwrap();
        assert_eq!(cmp_op.status, "Supported");
    }
}

#[test]
fn test_uuid_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    let ty = doc
        .types
        .iter()
        .find(|t| t.name == "UUID")
        .expect("UUID must exist");
    assert_eq!(ty.family, "uuid");
    let cmp_op = ty
        .operations
        .iter()
        .find(|o| o.operation == "comparison")
        .unwrap();
    assert_eq!(cmp_op.status, "Supported");
    let arith_op = ty
        .operations
        .iter()
        .find(|o| o.operation == "arithmetic")
        .unwrap();
    assert_eq!(arith_op.status, "Unsupported");
    assert_eq!(arith_op.rejection_code.as_deref(), Some("RS-1012"));
}

#[test]
fn test_array_type_matrix_conformance() {
    let doc = SqlMatrixDocument::load_canonical().unwrap();
    let ty = doc
        .types
        .iter()
        .find(|t| t.name == "ARRAY")
        .expect("ARRAY must exist");
    assert_eq!(ty.family, "array");
    let binding_op = ty
        .operations
        .iter()
        .find(|o| o.operation == "parameter_binding")
        .unwrap();
    assert_eq!(binding_op.status, "Supported");
}
