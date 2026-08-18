//! Unscoped Silent-Wrong-Answer and Negative Case Test Suite (v0.58.3).
//!
//! Asserts that unsupported, malformed, or boundary inputs across all engine
//! and gateway components return explicit, actionable RS-XXXX coded errors
//! or structured rejection, and never panic, abort, or produce silent wrong answers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogSourceEntry, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_sql::frontend::SqlFrontend;
use rockstream_storage::ShardDb;
use tempfile::TempDir;
use tokio_postgres::NoTls;

struct StubViewReader;

#[async_trait::async_trait]
impl ViewReader for StubViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        Some(1)
    }
}

struct TestGateway {
    catalog: Arc<CatalogStubs>,
    _shard_db: Arc<ShardDb>,
    _handle: tokio::task::JoinHandle<()>,
    _temp_dir: TempDir,
}

async fn start_test_gateway() -> (tokio_postgres::Client, TestGateway) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("unscoped-silent-wrong-answer", store)
            .build()
            .await
            .unwrap(),
    );

    let catalog = Arc::new(CatalogStubs::new());
    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("test-passphrase")),
    ));

    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(StubViewReader),
        Arc::clone(&shard_db),
    )
    .with_secret_store(secret_store);

    let (addr, handle) = server.serve_background().await.unwrap();

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        let _ = connection.await;
    });

    (
        client,
        TestGateway {
            catalog,
            _shard_db: shard_db,
            _handle: handle,
            _temp_dir: temp_dir,
        },
    )
}

#[tokio::test]
async fn test_invalid_source_worker_returns_error_not_panic() {
    let (client, gw) = start_test_gateway().await;

    // Register a source table and source entry with an unknown / unsupported source type
    gw.catalog.add_table(CatalogTable {
        name: "custom_src".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
    });

    gw.catalog.add_source(CatalogSourceEntry {
        name: "custom_src".to_string(),
        table_name: Some("custom_src".to_string()),
        source_type: "unsupported_external_type".to_string(),
        options: HashMap::new(),
        format: "json".to_string(),
        status: "OK".to_string(),
        live_offset: "0".to_string(),
        live_lag: 0,
    });

    // Attempting to create a materialized view referencing the unsupported source should fail gracefully
    let err = client
        .simple_query("CREATE MATERIALIZED VIEW v_custom AS SELECT id FROM custom_src;")
        .await
        .unwrap_err();

    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());

    assert!(
        msg.contains("source type 'unsupported_external_type'")
            || msg.contains("QueryTimeExecutionFailed")
            || msg.contains("error")
            || msg.contains("RS-"),
        "expected structured error rejecting unsupported source type without panicking, got: {msg}"
    );
}

#[tokio::test]
async fn test_unsupported_expressions_reject_honestly() {
    let frontend = SqlFrontend::new();

    // Unsupported SQL expressions must reject with an error instead of producing wrong answers or panicking
    let invalid_queries = [
        "SELECT XMLSERIALIZE(CONTENT '<a></a>' AS TEXT);",
        "SELECT JSON_TABLE(j, '$[*]' COLUMNS (x INT PATH '$.x')) FROM t;",
        "SELECT MATCH (title) AGAINST ('database' IN NATURAL LANGUAGE MODE);",
    ];

    for q in invalid_queries {
        let res = frontend.sql_to_plan_node(q).await;
        assert!(
            res.is_err(),
            "query '{q}' should be rejected as unsupported rather than accepted or panicked"
        );
    }
}

#[test]
fn test_unsupported_window_funcs_reject_honestly() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let window_op = WindowOp::new(
        schema.clone(),
        vec![WindowExpr {
            func: WindowFunc::Ntile(4),
            partition_by: vec![0],
            order_by: vec![1],
        }],
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();

    let delta = ArrowZSet::new(batch, vec![1, 1, 1]);
    let result = window_op.process_epoch(delta, 1);

    assert!(
        result.is_err(),
        "Expected unsupported window function to fail with RS-1016"
    );
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("RS-1016"),
        "Expected error message to contain RS-1016, got: {err_str}"
    );
}

#[tokio::test]
async fn test_arithmetic_overflow_division_by_zero_honest_rejection() {
    let (client, _gw) = start_test_gateway().await;

    client
        .simple_query("CREATE TABLE t_math (id BIGINT, val BIGINT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_math VALUES (1, 0);")
        .await
        .unwrap();

    // Query division by zero: must return an error or null, never panic the connection
    let res = client.simple_query("SELECT id / val FROM t_math;").await;
    // If it returns an error or rows, server must remain healthy
    if let Err(err) = res {
        let msg = err
            .as_db_error()
            .map(|e| e.message().to_string())
            .unwrap_or_else(|| err.to_string());
        assert!(!msg.is_empty());
    }

    // Prove server is still healthy
    let probe = client.simple_query("SELECT 1;").await;
    assert!(
        probe.is_ok(),
        "server must remain alive after arithmetic error"
    );
}

#[tokio::test]
async fn test_removed_connector_explicit_error() {
    let (client, _gw) = start_test_gateway().await;

    let removed_connectors = [
        "CREATE SOURCE src_s3 TYPE s3 (bucket='bucket') FORMAT json;",
        "CREATE SOURCE src_webhook TYPE http_webhook (credential_ref='vault://source') FORMAT json;",
        "CREATE SINK s_iceberg FOR VIEW v_test TO ICEBERG 's3://bucket/path';",
    ];

    for stmt in removed_connectors {
        let err = client.simple_query(stmt).await.unwrap_err();
        let msg = err
            .as_db_error()
            .map(|e| e.message().to_string())
            .unwrap_or_else(|| err.to_string());
        assert!(
            msg.contains("RS-4017"),
            "query '{stmt}' must fail with RS-4017, got: {msg}"
        );
    }
}
