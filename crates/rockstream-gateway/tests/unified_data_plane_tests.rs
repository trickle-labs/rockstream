//! v0.51.3 Slice 4 exit test: `CREATE VIEW` for a simple, directly
//! compilable SELECT registers a real `op_id` on the catalog view and
//! populates the in-process compiled-view registry — the "one data plane"
//! fast path (`rockstream_ops::compile_plan`) rather than falling back to
//! `view_materializer.rs`'s standalone DataFusion path.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    server::COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_ops::read_view_output;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;
use tokio_postgres::NoTls;

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

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

#[tokio::test]
async fn create_view_registers_op_id_and_no_longer_uses_datafusion_materializer() {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "orders".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "amount".to_string(),
                data_type: "Int64".to_string(),
            },
        ],
    });

    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("unified-data-plane-v4", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();

    let client = connect_port(addr.port()).await;
    client
        .simple_query("CREATE VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100")
        .await
        .expect("CREATE VIEW should succeed");

    let view = catalog
        .get_view("big_orders")
        .expect("view should be registered in the catalog");
    assert!(
        view.op_id.is_some(),
        "CatalogView.op_id should be Some(_) for a directly-compilable view, got None"
    );

    assert!(
        handler.has_compiled_view("big_orders"),
        "the in-process compiled-view registry should contain 'big_orders'"
    );
}

#[tokio::test]
async fn insert_commit_select_round_trip_serviced_by_real_operator_dag() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("unified-data-plane-v5", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("big_orders"));

    let before = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (1, 150)")
        .await
        .expect("INSERT should succeed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT should succeed");

    let msgs = client
        .simple_query("SELECT * FROM big_orders ORDER BY id")
        .await
        .expect("SELECT should succeed");
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("amount"), Some("150"));
    assert!(
        COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed) > before,
        "compiled COMMIT path should increment COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL"
    );

    let op_id = catalog
        .get_view("big_orders")
        .expect("view should be registered")
        .op_id
        .expect("compiled view should persist an op_id");
    let stored = read_view_output(&shard_db, OperatorId(op_id), 2)
        .await
        .expect("read_view_output should succeed");
    assert!(
        stored.iter().any(|(_, _, cols, weight)| {
            *weight > 0 && cols[0].as_i64() == Some(1) && cols[1].as_i64() == Some(150)
        }),
        "compiled sink output should contain the inserted row"
    );
}

/// v0.51.4 Slice 0 exit test: the compiled-view COMMIT refresh path costs
/// work proportional to the commit's own delta, not the source table's full
/// size. Populates 10,000 rows in one commit (expect the delta counter to
/// jump by ~10,000), then commits a single additional row and asserts the
/// counter increases by exactly 1 — not by ~10,001, which is what the
/// retired full-table-rescan path would have done.
#[tokio::test]
async fn commit_refresh_is_proportional_to_delta_not_table_size() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("unified-data-plane-proportionality", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 100",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(handler.has_compiled_view("big_orders"));

    const N: i64 = 10_000;
    let values: Vec<String> = (1..=N).map(|i| format!("({i}, 150)")).collect();
    let insert_sql = format!(
        "INSERT INTO orders (id, amount) VALUES {}",
        values.join(",")
    );

    let before_bulk = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);
    client
        .simple_query(&insert_sql)
        .await
        .expect("bulk INSERT should succeed");
    let after_bulk = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);
    let bulk_delta = after_bulk - before_bulk;
    assert!(
        (N as u64..=N as u64 + 5).contains(&bulk_delta),
        "expected the bulk commit's refresh delta to be ~{N} rows, got {bulk_delta}"
    );

    let before_single = after_bulk;
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (999999, 150)")
        .await
        .expect("single-row INSERT should succeed");
    let after_single = COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.load(Ordering::Relaxed);
    let single_delta = after_single - before_single;
    assert_eq!(
        single_delta, 1,
        "a single-row commit against a 10,000-row table must cost exactly 1 delta row, \
         not ~{N} (that would mean the refresh is still rescanning the full table)"
    );
}
