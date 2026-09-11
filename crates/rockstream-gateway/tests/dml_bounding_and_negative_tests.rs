//! Matrix F & Slice 8: Negative Boundary, Resource Limits & Malformed Statement Containment.
//!
//! Asserts that:
//! 1. Oversized SQL statements exceeding MAX_SQL_STATEMENT_BYTES (1 MiB) fail with RS-1012 (42601).
//! 2. Excessive AST expression depth exceeding MAX_EXPR_DEPTH (64) fails with RS-1012 (42601).
//! 3. Mutation limit exceeding MAX_DML_MUTATIONS_PER_STATEMENT fails with RS-2002 (57014).
//! 4. Write buffer memory limit (WRITE_BUFFER_LIMIT_BYTES = 64 MiB) enforces RS-2019 backpressure.
//! 5. DML scan pagination with MAX_DML_SCAN_WINDOW_ROWS continues to completion across pages.
//! 6. Negative error containment leaves zero partial mutations across failures.

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    write_buffer::{DmlOp, WriteBuffer, WRITE_BUFFER_LIMIT_BYTES},
    GatewayError, GatewayServer,
};
use rockstream_sql::dml::{
    MAX_DML_MUTATIONS_PER_STATEMENT, MAX_EXPR_DEPTH, MAX_SQL_STATEMENT_BYTES,
};
use rockstream_storage::ShardDb;

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
    Arc<InMemory>,
) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, shard_db, store)
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let msgs = client.simple_query(query).await.expect("query failed");
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let count = r.columns().len();
                let mut v = Vec::with_capacity(count);
                for i in 0..count {
                    v.push(r.get(i).unwrap_or("").to_string());
                }
                Some(v)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_oversized_sql_statement_rejected() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("limit-oversized-sql").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_size (id INT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    // Create a statement exceeding MAX_SQL_STATEMENT_BYTES (1 MiB)
    let padding = " ".repeat(MAX_SQL_STATEMENT_BYTES + 64);
    let oversized_query = format!("UPDATE t_size SET val = 'x'{padding} WHERE id = 1;");

    let err = client
        .simple_query(&oversized_query)
        .await
        .expect_err("oversized SQL statement must be rejected");

    let db_err = err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert_eq!(code, "42601", "expected SQLSTATE 42601, got: {code}");
    assert!(
        msg.contains("RS-1012"),
        "expected RS-1012 in error message, got: {msg}"
    );
}

#[tokio::test]
async fn test_excessive_expression_depth_rejected() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("limit-expr-depth").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_depth (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();

    // Build expression nested deeper than MAX_EXPR_DEPTH (64)
    let mut deep_expr = "1".to_string();
    for _ in 0..=(MAX_EXPR_DEPTH + 1) {
        deep_expr = format!("({deep_expr} + 0)");
    }
    let query = format!("UPDATE t_depth SET val = 'x' WHERE id = {deep_expr};");

    let err = client
        .simple_query(&query)
        .await
        .expect_err("excessive expression depth must be rejected");

    let db_err = err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert_eq!(code, "42601", "expected SQLSTATE 42601, got: {code}");
    assert!(
        msg.contains("RS-1012") || msg.contains("MAX_EXPR_DEPTH"),
        "expected RS-1012 or MAX_EXPR_DEPTH in error message, got: {msg}"
    );
}

#[tokio::test]
async fn test_mutation_limit_per_statement_enforced() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("limit-mutations").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_limit (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();

    // Seed MAX_DML_MUTATIONS_PER_STATEMENT + 1 rows (10,001 rows)
    let total_rows = MAX_DML_MUTATIONS_PER_STATEMENT + 1;
    let batch_size = 2000;
    for chunk_start in (1..=total_rows).step_by(batch_size) {
        let chunk_end = std::cmp::min(chunk_start + batch_size - 1, total_rows);
        let mut values_str = String::new();
        for id in chunk_start..=chunk_end {
            if !values_str.is_empty() {
                values_str.push_str(", ");
            }
            values_str.push_str(&format!("({id}, {id})"));
        }
        client
            .simple_query(&format!(
                "INSERT INTO t_limit (id, val) VALUES {values_str};"
            ))
            .await
            .unwrap();
    }

    // Attempt to UPDATE all 10,001 rows in a single statement
    let err = client
        .simple_query("UPDATE t_limit SET val = val + 1 WHERE id >= 1;")
        .await
        .expect_err("mutation count exceeding MAX_DML_MUTATIONS_PER_STATEMENT must fail");

    let db_err = err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert_eq!(code, "57014", "expected SQLSTATE 57014, got: {code}");
    assert!(
        msg.contains("RS-2002"),
        "expected RS-2002 in error message, got: {msg}"
    );

    // Verify atomic failure: all rows remain at original value
    let check_row1 = rows(&client, "SELECT id, val FROM t_limit WHERE id = 1;").await;
    assert_eq!(check_row1, vec![vec!["1".to_string(), "1".to_string()]]);
}

#[tokio::test]
async fn test_write_buffer_memory_limit_enforced() {
    // Unit test write buffer bounded limit RS-2019
    let buf = WriteBuffer::new();
    assert_eq!(buf.byte_count(), 0);

    // A buffer with tiny limit fails immediately
    let mut tiny_buf = WriteBuffer::with_limit_bytes(16);

    let op = DmlOp::Insert {
        table: "test_t".to_string(),
        cols: vec!["c1".to_string(), "c2".to_string()],
        values_tsv: "value1\tvalue2".to_string(),
        row_key: "k1".to_string(),
    };

    let res = tiny_buf.push(op);
    assert!(res.is_err(), "pushing beyond limit must return error");
    let err = res.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2019") || err_str.contains("shard_backpressure"),
        "error message must contain RS-2019: {err_str}"
    );
    match err {
        GatewayError::ShardBackpressure {
            current_bytes: _,
            limit_bytes,
        } => {
            assert_eq!(limit_bytes, 16);
        }
        other => panic!("expected ShardBackpressure, got: {other:?}"),
    }

    // Verify standard buffer has named constant limit
    assert_eq!(WRITE_BUFFER_LIMIT_BYTES, 64 * 1024 * 1024);
}

#[tokio::test]
async fn test_dml_scan_page_window_continues_to_completion() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("limit-scan-window").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_scan (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();

    // Insert 1500 rows (crossing the 1024 page window boundary)
    let total = 1500;
    let batch_size = 500;
    for chunk_start in (1..=total).step_by(batch_size) {
        let chunk_end = chunk_start + batch_size - 1;
        let mut values_str = String::new();
        for id in chunk_start..=chunk_end {
            if !values_str.is_empty() {
                values_str.push_str(", ");
            }
            values_str.push_str(&format!("({id}, {id})"));
        }
        client
            .simple_query(&format!(
                "INSERT INTO t_scan (id, val) VALUES {values_str};"
            ))
            .await
            .unwrap();
    }

    // Update across all 1500 rows
    let msgs = client
        .simple_query("UPDATE t_scan SET val = 999 WHERE id >= 1;")
        .await
        .unwrap();

    let mut updated_count = 0;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = msg {
            updated_count = n;
        }
    }
    assert_eq!(
        updated_count, 1500,
        "all 1500 rows across page windows must be updated"
    );

    // Check count of updated rows
    let r = rows(&client, "SELECT COUNT(*) FROM t_scan WHERE val = 999;").await;
    assert_eq!(r, vec![vec!["1500".to_string()]]);
}

#[tokio::test]
async fn test_dml_limits_and_negative_error_containment() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("limit-containment").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_contain (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_contain (id, val) VALUES (1, 10), (2, 20);")
        .await
        .unwrap();

    // 1. Malformed values list without column list on unknown table (RS-2056)
    let err_val = client
        .simple_query("INSERT INTO unknown_tbl VALUES (1, 2, 3);")
        .await
        .expect_err("malformed INSERT on unknown table must fail");
    let db_err1 = err_val.as_db_error().expect("must be DB error");
    assert!(
        db_err1.message().contains("RS-2056"),
        "expected RS-2056: {}",
        db_err1.message()
    );

    // 2. Division by zero in arithmetic (RS-1016)
    let err_div = client
        .simple_query("UPDATE t_contain SET val = 1 / 0 WHERE id = 1;")
        .await
        .expect_err("division by zero must fail");
    let db_err2 = err_div.as_db_error().expect("must be DB error");
    assert!(
        db_err2.code().code() == "22012" || db_err2.message().contains("RS-1016"),
        "expected 22012 / RS-1016: {}",
        db_err2.message()
    );

    // 3. Duplicate primary key insertion (RS-2057)
    let err_dup = client
        .simple_query("INSERT INTO t_contain (id, val) VALUES (1, 99);")
        .await
        .expect_err("duplicate PK must fail");
    let db_err3 = err_dup.as_db_error().expect("must be DB error");
    assert_eq!(db_err3.code().code(), "23505");
    assert!(
        db_err3.message().contains("RS-2057"),
        "expected RS-2057: {}",
        db_err3.message()
    );

    // 4. Verify containment: t_contain has original untouched state (1, 10), (2, 20)
    let mut check_rows = rows(&client, "SELECT id, val FROM t_contain;").await;
    check_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        check_rows,
        vec![
            vec!["1".to_string(), "10".to_string()],
            vec!["2".to_string(), "20".to_string()],
        ],
        "Negative failure containment must leave all table data untouched"
    );
}
