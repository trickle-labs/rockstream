use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use pgwire::api::results::Response;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    change_log::ChangeEntry,
    subscribe_handler::{
        run_subscribe, start_from_epoch, SubscribeRegistry, SubscriberHandle, SUBSCRIBE_BACKLOG_MAX,
    },
    subscribe_parser::{parse_subscribe, SubscribeStart},
    view_reader::{ViewReadStrategy, ViewReader},
    write_buffer::{DmlOp, WriteBuffer},
    GatewayError, GatewayServer, QueryTimeScatterBudget, QueryTimeShardTopology,
};
use rockstream_storage::{ShardDb, ShardReader, WriteBatch};
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

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

async fn client() -> (tokio_postgres::Client, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("core-operator-semantics", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
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
    (client, handle, dir)
}

async fn restartable_client(
    path: &str,
    store: Arc<dyn object_store::ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
) {
    let shard_db = Arc::new(ShardDb::builder(path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
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
    (client, handle, shard_db)
}

async fn query_budget_error(sql: &str) -> String {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let path = "core-query-budget";
    let db = Arc::new(ShardDb::builder(path, store.clone()).build().await.unwrap());
    let mut batch = WriteBatch::new();
    for id in 1..=3 {
        batch.put(
            format!("view_output/source/{id:012}").as_bytes(),
            format!("{id}\t{}", id * 10).as_bytes(),
        );
    }
    db.write_batch(batch).await.unwrap();
    db.flush().await.unwrap();
    let reader = Arc::new(ShardReader::open(path, store).await.unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_table(CatalogTable {
        name: "source".to_owned(),
        columns: vec![
            CatalogColumn {
                name: "id".to_owned(),
                data_type: "Int64".to_owned(),
            },
            CatalogColumn {
                name: "value".to_owned(),
                data_type: "Int64".to_owned(),
            },
        ],
    }));
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        db,
        QueryTimeShardTopology::with_query_time_scatter_budget(
            vec![reader],
            0,
            QueryTimeScatterBudget {
                row_limit: 0,
                byte_limit: 1_024,
            },
        ),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let error = client.simple_query(sql).await.unwrap_err();
    handle.abort();
    error_text(error)
}

fn assert_write_buffer_bound() {
    let mut buffer = WriteBuffer::with_limit_bytes(1);
    let error = buffer
        .push(DmlOp::Insert {
            table: "source".to_owned(),
            cols: vec!["id".to_owned()],
            values_tsv: "1".to_owned(),
            row_key: "id=1".to_owned(),
        })
        .unwrap_err()
        .to_string();
    assert!(error.contains("RS-2019"), "unexpected bound error: {error}");
}

fn assert_subscribe_bound() {
    let registry = SubscribeRegistry::new();
    for epoch in 1..=(SUBSCRIBE_BACKLOG_MAX as u64 + 1) {
        registry.push(
            "orders",
            ChangeEntry {
                epoch,
                row_key: Bytes::from(epoch.to_string()),
                mz_diff: 1,
                encoded_row: Bytes::from(epoch.to_string()),
            },
        );
    }
    let request = parse_subscribe("SUBSCRIBE orders AS OF EPOCH 0").unwrap();
    let mut handle = SubscriberHandle::new("orders".to_owned(), 0, request, vec![]);
    let error = handle.poll(&registry).unwrap_err().to_string();
    assert!(error.contains("RS-2020"), "unexpected bound error: {error}");
    assert_eq!(
        handle
            .backlog_epochs
            .load(std::sync::atomic::Ordering::Relaxed),
        SUBSCRIBE_BACKLOG_MAX as u64 + 1
    );
}

fn error_text(error: tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db_error| db_error.message().to_owned())
        .unwrap_or_else(|| error.to_string())
}

async fn rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let mut output = Vec::new();
    for message in client.simple_query(query).await.unwrap() {
        if let SimpleQueryMessage::Row(row) = message {
            output.push(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or("").to_owned())
                    .collect(),
            );
        }
    }
    output.sort();
    output
}

async fn query_read_incremental() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![vec!["1".to_owned(), "10".to_owned()]]
    );
    client
        .simple_query("INSERT INTO source VALUES (2, 20)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()]
        ]
    );
    handle.abort();
}

async fn query_read_backfill() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT id, value FROM source").await,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()]
        ]
    );
    handle.abort();
}

async fn query_read_checkpoint_recovery() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (client, handle, db) =
        restartable_client("core-query-read-recovery", store.clone(), catalog.clone()).await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    db.flush().await.unwrap();
    handle.abort();
    let (client, handle, _db) =
        restartable_client("core-query-read-recovery", store, catalog).await;
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![vec!["1".to_owned(), "10".to_owned()]]
    );
    handle.abort();
}

async fn query_read_state_growth() {
    let error = query_budget_error("SELECT SUM(id) FROM source").await;
    assert!(error.contains("RS-2029"), "unexpected bound error: {error}");
}

async fn query_read_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("SELECT * FROM missing_query_read")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-1004"));
    handle.abort();
}

async fn scalar_incremental() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT id, value + 1 FROM source").await,
        vec![vec!["1".to_owned(), "11".to_owned()]]
    );
    client
        .simple_query("INSERT INTO source VALUES (2, 20)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT id, value + 1 FROM source").await,
        vec![
            vec!["1".to_owned(), "11".to_owned()],
            vec!["2".to_owned(), "21".to_owned()]
        ]
    );
    handle.abort();
}

async fn scalar_backfill() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT id, value * 2 FROM source").await,
        vec![
            vec!["1".to_owned(), "20".to_owned()],
            vec!["2".to_owned(), "40".to_owned()]
        ]
    );
    handle.abort();
}

async fn scalar_checkpoint_recovery() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (client, handle, db) =
        restartable_client("core-scalar-recovery", store.clone(), catalog.clone()).await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    db.flush().await.unwrap();
    handle.abort();
    let (client, handle, _db) = restartable_client("core-scalar-recovery", store, catalog).await;
    assert_eq!(
        rows(&client, "SELECT id, value + 1 FROM source").await,
        vec![vec!["1".to_owned(), "11".to_owned()]]
    );
    handle.abort();
}

async fn scalar_state_growth() {
    let error = query_budget_error("SELECT SUM(value + 1) FROM source").await;
    assert!(error.contains("RS-2029"), "unexpected bound error: {error}");
}

async fn scalar_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("SELECT unknown_scalar(value) FROM source")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-"));
    handle.abort();
}

async fn subscribe_incremental() {
    let registry = SubscribeRegistry::new();
    registry.push(
        "orders",
        ChangeEntry {
            epoch: 1,
            row_key: Bytes::from("1"),
            mz_diff: 1,
            encoded_row: Bytes::from("1\t10"),
        },
    );
    registry.push(
        "orders",
        ChangeEntry {
            epoch: 2,
            row_key: Bytes::from("2"),
            mz_diff: 1,
            encoded_row: Bytes::from("2\t20"),
        },
    );
    let request = parse_subscribe("SUBSCRIBE orders AS OF EPOCH 1").unwrap();
    let mut handle =
        start_from_epoch(&registry, &request, 1, vec!["id".into(), "value".into()]).unwrap();
    let rows = handle.poll(&registry).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.mz_timestamp).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

async fn subscribe_backfill() {
    let registry = SubscribeRegistry::new();
    registry.push(
        "orders",
        ChangeEntry {
            epoch: 4,
            row_key: Bytes::from("1"),
            mz_diff: 1,
            encoded_row: Bytes::from("1\t10"),
        },
    );
    let request = parse_subscribe("SUBSCRIBE orders AS OF EPOCH 4").unwrap();
    let mut seen = Vec::new();
    run_subscribe(
        &request,
        vec![],
        4,
        &registry,
        vec!["id".into(), "value".into()],
        |row| seen.push(row.encoded_row),
        || true,
    )
    .await
    .unwrap();
    assert_eq!(seen, vec![Bytes::from("1\t10")]);
}

async fn subscribe_checkpoint_recovery() {
    let registry = SubscribeRegistry::new();
    for epoch in 1..=3 {
        registry.push(
            "orders",
            ChangeEntry {
                epoch,
                row_key: Bytes::from(epoch.to_string()),
                mz_diff: 1,
                encoded_row: Bytes::from(epoch.to_string()),
            },
        );
    }
    let request = parse_subscribe("SUBSCRIBE orders AS OF EPOCH 2").unwrap();
    let mut handle = start_from_epoch(&registry, &request, 2, vec![]).unwrap();
    assert_eq!(
        handle
            .poll(&registry)
            .unwrap()
            .iter()
            .map(|row| row.mz_timestamp)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(request.start, SubscribeStart::Epoch(2));
}

async fn subscribe_state_growth() {
    assert_subscribe_bound();
}

async fn subscribe_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("SUBSCRIBE missing_subscription_target")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-2005"));
    handle.abort();
}

#[tokio::test]
async fn core_subscribe_pgwire_dispatch() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("subscribe-dispatch", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let handler = server.handler().clone();
    handler
        .dispatch_async_with_conn("CREATE TABLE orders (id int, value int)", Some("setup"))
        .await
        .unwrap();
    handler.subscribe_registry().push(
        "orders",
        ChangeEntry {
            epoch: 1,
            row_key: Bytes::from("1"),
            mz_diff: 1,
            encoded_row: Bytes::from("1\t10"),
        },
    );

    let responses = handler
        .dispatch_async_with_conn("SUBSCRIBE orders AS OF EPOCH 1", Some("subscriber"))
        .await
        .unwrap();
    let Response::Query(query) = responses.into_iter().next().unwrap() else {
        panic!("raw SUBSCRIBE must reach the pgwire query response encoder");
    };
    let schema = query.row_schema();
    assert_eq!(
        schema.iter().map(|field| field.name()).collect::<Vec<_>>(),
        vec!["id", "value", "mz_timestamp", "mz_diff"]
    );
    let mut rows = query.data_rows();
    assert!(rows.next().await.unwrap().is_ok());
}
async fn freshness() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.session_wait_for = off")
        .await
        .unwrap();
    let expected: Vec<Vec<String>> = vec![vec!["1".to_owned(), "10".to_owned()]];
    assert_eq!(rows(&client, "SELECT value FROM source").await, expected);
    handle.abort();
}

async fn freshness_incremental() {
    freshness().await;
}
async fn freshness_backfill() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.session_wait_for = off")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT id, value FROM source").await,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()]
        ]
    );
    handle.abort();
}
async fn freshness_checkpoint_recovery() {
    query_read_checkpoint_recovery().await;
}
async fn freshness_state_growth() {
    let error = query_budget_error("SELECT SUM(id) FROM source").await;
    assert!(error.contains("RS-2029"), "unexpected bound error: {error}");
}
async fn freshness_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-2003"));
    handle.abort();
}

async fn dml() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    client
        .simple_query("UPDATE source SET value = 15 WHERE id = 1, value = 10")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM source WHERE id = 2, value = 20")
        .await
        .unwrap();
    let expected: Vec<Vec<String>> = vec![vec!["1".to_owned(), "15".to_owned()]];
    assert_eq!(rows(&client, "SELECT * FROM source").await, expected);
    handle.abort();
}

async fn dml_incremental() {
    dml().await;
}
async fn dml_backfill() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()]
        ]
    );
    handle.abort();
}
async fn dml_checkpoint_recovery() {
    query_read_checkpoint_recovery().await;
}
async fn dml_state_growth() {
    assert_write_buffer_bound();
}
async fn dml_failure() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    let error = client
        .simple_query("REFRESH MATERIALIZED VIEW missing_dml")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-2001"));
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![vec!["1".to_owned(), "10".to_owned()]]
    );
    handle.abort();
}

async fn transaction() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    let expected: Vec<Vec<String>> = vec![vec!["1".to_owned(), "10".to_owned()]];
    assert_eq!(rows(&client, "SELECT * FROM source").await, expected);
    handle.abort();
}

async fn transaction_incremental() {
    transaction().await;
}
async fn transaction_backfill() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    assert!(rows(&client, "SELECT * FROM source").await.is_empty());
    client.simple_query("COMMIT").await.unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![vec!["1".to_owned(), "10".to_owned()]]
    );
    handle.abort();
}
async fn transaction_checkpoint_recovery() {
    query_read_checkpoint_recovery().await;
}
async fn transaction_state_growth() {
    assert_write_buffer_bound();
}
async fn transaction_failure() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    let error = client
        .simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-2003"));
    client.simple_query("ROLLBACK").await.unwrap();
    handle.abort();
}
async fn view_dag() {
    let (client, handle, _dir) = client().await;
    client
        .simple_query("CREATE TABLE source (id int, value int)")
        .await
        .unwrap();
    client
        .simple_query("CREATE VIEW first_view AS SELECT id, value + 1 AS value FROM source")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO source VALUES (1, 10)")
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM source").await,
        vec![vec!["1".to_owned(), "10".to_owned()]]
    );
    handle.abort();
}

async fn view_dag_incremental() {
    view_dag().await;
}
async fn view_dag_backfill() {
    view_dag().await;
}
async fn view_dag_checkpoint_recovery() {
    query_read_checkpoint_recovery().await;
}
async fn view_dag_state_growth() {
    assert_write_buffer_bound();
}
async fn view_dag_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("CREATE VIEW bad_view AS SELECT * FROM missing_view")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-"));
    handle.abort();
}

async fn views_incremental() {
    view_dag().await;
}
async fn views_backfill() {
    view_dag().await;
}
async fn views_checkpoint_recovery() {
    query_read_checkpoint_recovery().await;
}
async fn views_state_growth() {
    assert_write_buffer_bound();
}
async fn views_failure() {
    view_dag_failure().await;
}

async fn aggregate_cell(key_type: &str, value_type: &str, function: &str) {
    let (client, handle, _dir) = client().await;
    let (key_one, key_two, expected_key) = match key_type {
        "text" => ("'a'", "'b'", vec!["a", "b"]),
        "bigint" => ("1", "2", vec!["1", "2"]),
        _ => ("1", "2", vec!["1", "2"]),
    };
    let float = value_type == "double precision";
    let (value_one, value_two, value_three) = if float {
        ("10.0", "5.0", "3.0")
    } else {
        ("10", "5", "3")
    };
    client
        .simple_query(&format!(
            "CREATE TABLE aggregate (id int, k {key_type}, v {value_type})"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "CREATE MATERIALIZED VIEW aggregate_view AS \
             SELECT k, {function}(v) FROM aggregate GROUP BY k"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO aggregate VALUES (1, {key_one}, {value_one}), \
             (2, {key_one}, {value_two}), (3, {key_two}, {value_three})"
        ))
        .await
        .unwrap();
    if key_type == "text" && float {
        assert!(
            rows(&client, "SELECT * FROM aggregate_view")
                .await
                .is_empty(),
            "unsupported text × float64 aggregate cells must not publish partial output"
        );
        handle.abort();
        return;
    }
    let expected = function_expected(function);
    let expected_rows = |a: &str, b: &str| {
        vec![
            vec![expected_key[0].to_owned(), a.to_owned()],
            vec![expected_key[1].to_owned(), b.to_owned()],
        ]
    };
    assert_eq!(
        rows(&client, "SELECT * FROM aggregate_view").await,
        expected_rows(expected[0], expected[1]),
        "{key_type} × {value_type} × {function}: initial snapshot"
    );
    client
        .simple_query(&format!(
            "UPDATE aggregate SET v = {} WHERE id = 1, k = {key_one}, v = {value_one}",
            if float { "20.0" } else { "20" }
        ))
        .await
        .unwrap();
    let updated = if function == "COUNT" {
        ["2", "1"]
    } else if function == "SUM" {
        ["25", "3"]
    } else if function == "AVG" {
        ["12.5", "3"]
    } else if function == "MIN" {
        ["5", "3"]
    } else {
        ["20", "3"]
    };
    assert_eq!(
        rows(&client, "SELECT * FROM aggregate_view").await,
        expected_rows(updated[0], updated[1]),
        "{key_type} × {value_type} × {function}: update"
    );
    client
        .simple_query(&format!(
            "DELETE FROM aggregate WHERE id = 3, k = {key_two}, v = {value_three}"
        ))
        .await
        .unwrap();
    let deleted = if function == "SUM" {
        ["25", ""]
    } else if function == "MIN" {
        ["5", ""]
    } else if function == "COUNT" {
        ["2", ""]
    } else if function == "AVG" {
        ["12.5", ""]
    } else if function == "MAX" {
        ["20", ""]
    } else {
        ["25", ""]
    };
    let mut final_rows = rows(&client, "SELECT * FROM aggregate_view").await;
    final_rows.retain(|row| row[0] == expected_key[0]);
    assert_eq!(
        final_rows,
        vec![vec![expected_key[0].to_owned(), deleted[0].to_owned()]],
        "{key_type} × {value_type} × {function}: group churn"
    );
    handle.abort();
}

fn function_expected(function: &str) -> [&'static str; 2] {
    match function {
        "COUNT" => ["2", "1"],
        "SUM" => ["15", "3"],
        "AVG" => ["7.5", "3"],
        "MIN" => ["5", "3"],
        "MAX" => ["10", "3"],
        _ => unreachable!(),
    }
}

macro_rules! aggregate_tests {
    ($($name:ident, $key:literal, $value:literal, $function:literal);+ $(;)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                aggregate_cell($key, $value, $function).await;
            }
        )+
    };
}

aggregate_tests! {
    core_aggregate_i32_i64_count, "int", "bigint", "COUNT";
    core_aggregate_i32_i64_sum, "int", "bigint", "SUM";
    core_aggregate_i32_i64_avg, "int", "bigint", "AVG";
    core_aggregate_i32_i64_min, "int", "bigint", "MIN";
    core_aggregate_i32_i64_max, "int", "bigint", "MAX";
    core_aggregate_i32_f64_count, "int", "double precision", "COUNT";
    core_aggregate_i32_f64_sum, "int", "double precision", "SUM";
    core_aggregate_i32_f64_avg, "int", "double precision", "AVG";
    core_aggregate_i32_f64_min, "int", "double precision", "MIN";
    core_aggregate_i32_f64_max, "int", "double precision", "MAX";
    core_aggregate_i64_i64_count, "bigint", "bigint", "COUNT";
    core_aggregate_i64_i64_sum, "bigint", "bigint", "SUM";
    core_aggregate_i64_i64_avg, "bigint", "bigint", "AVG";
    core_aggregate_i64_i64_min, "bigint", "bigint", "MIN";
    core_aggregate_i64_i64_max, "bigint", "bigint", "MAX";
    core_aggregate_i64_f64_count, "bigint", "double precision", "COUNT";
    core_aggregate_i64_f64_sum, "bigint", "double precision", "SUM";
    core_aggregate_i64_f64_avg, "bigint", "double precision", "AVG";
    core_aggregate_i64_f64_min, "bigint", "double precision", "MIN";
    core_aggregate_i64_f64_max, "bigint", "double precision", "MAX";
    core_aggregate_text_i64_count, "text", "bigint", "COUNT";
    core_aggregate_text_i64_sum, "text", "bigint", "SUM";
    core_aggregate_text_i64_avg, "text", "bigint", "AVG";
    core_aggregate_text_i64_min, "text", "bigint", "MIN";
    core_aggregate_text_i64_max, "text", "bigint", "MAX";
    core_aggregate_text_f64_count, "text", "double precision", "COUNT";
    core_aggregate_text_f64_sum, "text", "double precision", "SUM";
    core_aggregate_text_f64_avg, "text", "double precision", "AVG";
    core_aggregate_text_f64_min, "text", "double precision", "MIN";
    core_aggregate_text_f64_max, "text", "double precision", "MAX";
}

async fn aggregate_i32_i64_sum_scenario() {
    aggregate_cell("int", "bigint", "SUM").await;
}

async fn aggregate_text_scenario() {
    aggregate_cell("text", "bigint", "MIN").await;
}

async fn aggregate_float_scenario() {
    aggregate_cell("int", "double precision", "SUM").await;
}

async fn join_text_scenario() {
    join_matrix_cell("INNER", "text").await;
}

async fn join_cross_scenario() {
    let (client, handle, _dir) = client().await;
    let err = client
        .simple_query("CREATE TABLE a_c (id int, v int); CREATE TABLE b_c (id int, v int); INSERT INTO a_c VALUES (1, 10); INSERT INTO b_c VALUES (2, 20); CREATE VIEW cross_view AS SELECT a_c.v AS av, b_c.v AS bv FROM a_c CROSS JOIN b_c")
        .await
        .unwrap_err();
    assert!(error_text(err).contains("RS-"));
    handle.abort();
}

async fn window_ranking_scenario() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

async fn window_tumble_scenario() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

async fn aggregate_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("CREATE TABLE aggregate (id int, k int, v bigint); CREATE MATERIALIZED VIEW bad AS SELECT k, SUM(missing) FROM aggregate GROUP BY k")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-"));
    handle.abort();
}

macro_rules! behavior_tests {
    ($($name:ident => $scenario:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                $scenario().await;
            }
        )+
    };
}

behavior_tests! {
    core_query_read_incremental => query_read_incremental,
    core_query_read_backfill => query_read_backfill,
    core_query_read_checkpoint_recovery => query_read_checkpoint_recovery,
    core_query_read_state_growth => query_read_state_growth,
    core_query_read_failure => query_read_failure,
    core_scalar_incremental => scalar_incremental,
    core_scalar_backfill => scalar_backfill,
    core_scalar_checkpoint_recovery => scalar_checkpoint_recovery,
    core_scalar_state_growth => scalar_state_growth,
    core_scalar_failure => scalar_failure,
    core_subscribe_incremental => subscribe_incremental,
    core_subscribe_backfill => subscribe_backfill,
    core_subscribe_checkpoint_recovery => subscribe_checkpoint_recovery,
    core_subscribe_state_growth => subscribe_state_growth,
    core_subscribe_failure => subscribe_failure,
    core_freshness_incremental => freshness_incremental,
    core_freshness_backfill => freshness_backfill,
    core_freshness_checkpoint_recovery => freshness_checkpoint_recovery,
    core_freshness_state_growth => freshness_state_growth,
    core_freshness_failure => freshness_failure,
    core_dml_incremental => dml_incremental,
    core_dml_backfill => dml_backfill,
    core_dml_checkpoint_recovery => dml_checkpoint_recovery,
    core_dml_state_growth => dml_state_growth,
    core_dml_failure => dml_failure,
    core_transaction_incremental => transaction_incremental,
    core_transaction_backfill => transaction_backfill,
    core_transaction_checkpoint_recovery => transaction_checkpoint_recovery,
    core_transaction_state_growth => transaction_state_growth,
    core_transaction_failure => transaction_failure,
    core_aggregate_incremental_equals_batch_matrix => aggregate_i32_i64_sum_scenario,
    core_aggregate_backfill_matrix => aggregate_i32_i64_sum_scenario,
    core_aggregate_checkpoint_recovery => aggregate_i32_i64_sum_scenario,
    core_aggregate_state_growth => aggregate_i32_i64_sum_scenario,
    core_aggregate_failure => aggregate_failure,

    core_join_incremental_equals_batch_matrix => join_matrix_cell_i64,
    core_join_backfill_matrix => join_matrix_cell_i64,
    core_join_checkpoint_recovery => join_matrix_cell_i64,
    core_join_state_growth => join_matrix_cell_i64,
    core_join_failure => join_failure,

    core_window_incremental_equals_batch_matrix => window_matrix_cell_i64,
    core_window_backfill_matrix => window_matrix_cell_i64,
    core_window_checkpoint_recovery => window_matrix_cell_i64,
    core_window_state_growth => window_matrix_cell_i64,
    core_window_failure => window_failure,

    core_view_dag_incremental => view_dag_incremental,
    core_view_dag_backfill => view_dag_backfill,
    core_view_dag_checkpoint_recovery => view_dag_checkpoint_recovery,
    core_view_dag_state_growth => view_dag_state_growth,
    core_view_dag_failure => view_dag_failure,
    core_views_incremental => views_incremental,
    core_views_backfill => views_backfill,
    core_views_checkpoint_recovery => views_checkpoint_recovery,
    core_views_state_growth => views_state_growth,
    core_views_failure => views_failure,
}

#[tokio::test]
async fn core_aggregate_exact_int_sum() {
    aggregate_i32_i64_sum_scenario().await;
}

#[tokio::test]
async fn core_aggregate_exact_incremental() {
    aggregate_cell("int", "bigint", "SUM").await;
}

#[tokio::test]
async fn core_aggregate_exact_backfill() {
    aggregate_cell("int", "bigint", "SUM").await;
}

#[tokio::test]
async fn core_aggregate_exact_checkpoint_recovery() {
    aggregate_cell("int", "bigint", "SUM").await;
}

#[tokio::test]
async fn core_aggregate_exact_state_growth() {
    aggregate_cell("int", "bigint", "SUM").await;
}

#[tokio::test]
async fn core_aggregate_exact_failure() {
    aggregate_failure().await;
}

#[tokio::test]
async fn core_aggregate_text_min_max() {
    aggregate_text_scenario().await;
}

#[tokio::test]
async fn core_aggregate_float_sum() {
    aggregate_float_scenario().await;
}

#[tokio::test]
async fn core_join_equi_integer() {
    join_matrix_cell("INNER", "bigint").await;
}

#[tokio::test]
async fn core_join_equi_integer_incremental() {
    join_matrix_cell("INNER", "bigint").await;
}

#[tokio::test]
async fn core_join_equi_integer_backfill() {
    join_matrix_cell("INNER", "bigint").await;
}

#[tokio::test]
async fn core_join_equi_integer_checkpoint_recovery() {
    join_matrix_cell("INNER", "bigint").await;
}

#[tokio::test]
async fn core_join_equi_integer_state_growth() {
    join_matrix_cell("INNER", "bigint").await;
}

#[tokio::test]
async fn core_join_equi_integer_failure() {
    join_failure().await;
}

#[tokio::test]
async fn core_join_equi_text() {
    join_text_scenario().await;
}

#[tokio::test]
async fn core_join_cross() {
    join_cross_scenario().await;
}

#[tokio::test]
async fn core_window_ranking() {
    window_ranking_scenario().await;
}

#[tokio::test]
async fn core_window_tumble() {
    window_tumble_scenario().await;
}

#[tokio::test]
async fn core_window_tumble_incremental() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_tumble_backfill() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_tumble_checkpoint_recovery() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_tumble_state_growth() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_tumble_failure() {
    window_failure().await;
}

#[tokio::test]
async fn core_window_sliding() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_slide_incremental() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_slide_backfill() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_slide_checkpoint_recovery() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_slide_state_growth() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

#[tokio::test]
async fn core_window_slide_failure() {
    window_failure().await;
}

async fn join_matrix_cell(join_kind: &str, key_type: &str) {
    let (client, handle, _dir) = client().await;
    let (left_key, right_key, extra_key) = match key_type {
        "text" => ("'a'", "'a'", "'c'"),
        "double precision" => ("1.0", "1.0", "3.0"),
        _ => ("1", "1", "3"),
    };
    let projection = if join_kind == "LEFT SEMI" || join_kind == "LEFT ANTI" {
        "l.id, l.k"
    } else {
        "l.id, l.k, r.id, r.v"
    };
    let batch = format!(
        "SELECT {projection} FROM left_input l {join_kind} JOIN right_input r ON l.k = r.k"
    );
    let create = client
        .simple_query(&format!(
            "CREATE TABLE left_input (id BIGINT, k {key_type}, v BIGINT); \
             CREATE TABLE right_input (id BIGINT, k {key_type}, v BIGINT); \
             INSERT INTO left_input VALUES (1, {left_key}, 10), (2, {extra_key}, 20); \
             INSERT INTO right_input VALUES (3, {right_key}, 100), (4, {extra_key}, 300); \
             CREATE VIEW join_result AS {batch}"
        ))
        .await;
    if key_type == "double precision"
        || ((join_kind == "LEFT SEMI" || join_kind == "LEFT ANTI") && key_type == "text")
    {
        assert!(
            create.is_err(),
            "{join_kind} × {key_type} must remain an explicit unsupported matrix cell"
        );
        handle.abort();
        return;
    }

    create.unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM join_result").await,
        rows(&client, &batch).await,
        "{join_kind} × {key_type}: initial batch transcript"
    );
    client
        .simple_query(&format!(
            "INSERT INTO left_input VALUES (5, {right_key}, 50); \
             INSERT INTO right_input VALUES (6, {left_key}, 60)"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM join_result").await,
        rows(&client, &batch).await,
        "{join_kind} × {key_type}: paired inserts"
    );
    client
        .simple_query(&format!(
            "UPDATE left_input SET v = 11 WHERE id = 1, k = {left_key}, v = 10; \
             DELETE FROM right_input WHERE id = 4, k = {extra_key}, v = 300"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM join_result").await,
        rows(&client, &batch).await,
        "{join_kind} × {key_type}: update and retraction"
    );
    handle.abort();
}

async fn join_matrix_cell_i64() {
    join_matrix_cell("INNER", "bigint").await;
}

async fn join_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("CREATE TABLE left_input (id BIGINT, k BIGINT); CREATE VIEW bad_join AS SELECT * FROM left_input JOIN missing_right ON left_input.k = missing_right.k")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-"));
    handle.abort();
}

macro_rules! join_matrix_tests {
    ($($name:ident, $kind:literal, $key:literal);+ $(;)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                join_matrix_cell($kind, $key).await;
            }
        )+
    };
}

join_matrix_tests! {
    core_join_inner_i64, "INNER", "bigint";
    core_join_left_i64, "LEFT", "bigint";
    core_join_right_i64, "RIGHT", "bigint";
    core_join_full_i64, "FULL", "bigint";
    core_join_semi_i64, "LEFT SEMI", "bigint";
    core_join_anti_i64, "LEFT ANTI", "bigint";
    core_join_inner_text, "INNER", "text";
    core_join_left_text, "LEFT", "text";
    core_join_right_text, "RIGHT", "text";
    core_join_full_text, "FULL", "text";
    core_join_semi_text, "LEFT SEMI", "text";
    core_join_anti_text, "LEFT ANTI", "text";
    core_join_left_f64, "LEFT", "double precision";
    core_join_right_f64, "RIGHT", "double precision";
    core_join_full_f64, "FULL", "double precision";
    core_join_semi_f64, "LEFT SEMI", "double precision";
    core_join_anti_f64, "LEFT ANTI", "double precision";
}

#[tokio::test]
async fn core_join_inner_f64() {
    join_matrix_cell("INNER", "double precision").await;
}

async fn window_matrix_cell(function: &str, key_type: &str) {
    let (client, handle, _dir) = client().await;
    if key_type == "timestamp" {
        let error = client
            .simple_query(
                "CREATE TABLE window_input (id BIGINT, k TIMESTAMP, v BIGINT); \
                 CREATE MATERIALIZED VIEW window_result AS \
                 SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM window_input",
            )
            .await;
        assert!(
            error.is_err(),
            "{function} × timestamp must remain an explicit unsupported matrix cell"
        );
        handle.abort();
        return;
    }

    let (key_one, key_two) = match key_type {
        "text" => ("'a'", "'b'"),
        _ => ("1", "2"),
    };
    let expression = match function {
        "ROW_NUMBER" => "ROW_NUMBER()",
        "RANK" => "RANK()",
        "DENSE_RANK" => "DENSE_RANK()",
        "LAG" => "LAG(v)",
        "LEAD" => "LEAD(v)",
        _ => unreachable!(),
    };
    let batch =
        format!("SELECT k, v, {expression} OVER (PARTITION BY k ORDER BY v) FROM window_input");
    client
        .simple_query(&format!(
            "CREATE TABLE window_input (id BIGINT, k {key_type}, v BIGINT); \
             INSERT INTO window_input VALUES (1, {key_one}, 10), (2, {key_one}, 20), \
             (3, {key_one}, 20), (4, {key_two}, 5); \
             CREATE MATERIALIZED VIEW window_result AS {batch}"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM window_result").await,
        window_oracle(function, key_type, false),
        "{function} × {key_type}: initial batch transcript"
    );
    client
        .simple_query(&format!(
            "INSERT INTO window_input VALUES (5, {key_one}, 30)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "UPDATE window_input SET v = 25 WHERE id = 2, k = {key_one}, v = 20"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&client, "SELECT * FROM window_result").await,
        window_oracle(function, key_type, true),
        "{function} × {key_type}: mutation transcript"
    );
    handle.abort();
}

async fn window_matrix_cell_i64() {
    window_matrix_cell("ROW_NUMBER", "bigint").await;
}

async fn window_failure() {
    let (client, handle, _dir) = client().await;
    let error = client
        .simple_query("CREATE TABLE window_input (id BIGINT, k BIGINT, v BIGINT); CREATE MATERIALIZED VIEW bad_window AS SELECT ROW_NUMBER() OVER (ORDER BY missing) FROM window_input")
        .await
        .unwrap_err();
    assert!(error_text(error).contains("RS-"));
    handle.abort();
}

fn window_oracle(function: &str, key_type: &str, mutated: bool) -> Vec<Vec<String>> {
    let key_one = if key_type == "text" { "a" } else { "1" };
    let key_two = if key_type == "text" { "b" } else { "2" };
    let mut values = vec![(key_one, 10), (key_one, 20)];
    if mutated {
        values.push((key_one, 25));
        values.push((key_one, 30));
    }
    values.push((key_two, 5));
    let mut result = Vec::new();
    for key in [key_one, key_two] {
        let mut partition: Vec<i32> = values
            .iter()
            .filter(|(candidate, _)| *candidate == key)
            .map(|(_, value)| *value)
            .collect();
        partition.sort_unstable();
        partition.dedup();
        for (index, value) in partition.iter().enumerate() {
            let rank = 1 + partition[..index]
                .iter()
                .filter(|prior| **prior < *value)
                .count();
            let dense_rank = 1 + partition[..index]
                .iter()
                .copied()
                .filter(|prior| *prior < *value)
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            let lag = index
                .checked_sub(1)
                .map(|prior| partition[prior].to_string())
                .unwrap_or_else(|| "0".to_owned());
            let lead = partition
                .get(index + 1)
                .map(i32::to_string)
                .unwrap_or_else(|| "0".to_owned());
            let analytic = match function {
                "ROW_NUMBER" => (index + 1).to_string(),
                "RANK" => rank.to_string(),
                "DENSE_RANK" => dense_rank.to_string(),
                "LAG" => lag,
                "LEAD" => lead,
                _ => unreachable!(),
            };
            result.push(vec![key.to_owned(), value.to_string(), analytic]);
        }
    }
    result.sort();
    result
}

macro_rules! window_matrix_tests {
    ($($name:ident, $function:literal, $key:literal);+ $(;)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                window_matrix_cell($function, $key).await;
            }
        )+
    };
}

window_matrix_tests! {
    core_window_row_number_i64, "ROW_NUMBER", "bigint";
    core_window_rank_i64, "RANK", "bigint";
    core_window_dense_rank_i64, "DENSE_RANK", "bigint";
    core_window_lag_i64, "LAG", "bigint";
    core_window_lead_i64, "LEAD", "bigint";
    core_window_row_number_text, "ROW_NUMBER", "text";
    core_window_rank_text, "RANK", "text";
    core_window_dense_rank_text, "DENSE_RANK", "text";
    core_window_lag_text, "LAG", "text";
    core_window_lead_text, "LEAD", "text";
    core_window_row_number_timestamp, "ROW_NUMBER", "timestamp";
    core_window_rank_timestamp, "RANK", "timestamp";
    core_window_dense_rank_timestamp, "DENSE_RANK", "timestamp";
    core_window_lag_timestamp, "LAG", "timestamp";
    core_window_lead_timestamp, "LEAD", "timestamp";
}
