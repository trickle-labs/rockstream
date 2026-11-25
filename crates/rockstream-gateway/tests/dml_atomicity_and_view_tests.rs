//! Matrix E & Slice 4 tests: Transactional Atomicity, Rollback & Materialized View Propagation.

use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{catalog::DurableCatalogStore, ShardDb, ShardKeyEncoder};

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

async fn connect_port_with_task(
    port: u16,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    let task = tokio::spawn(async move {
        let _ = conn.await;
    });
    (client, task)
}

async fn start_durable_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog_prefix: &str,
    recover: bool,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
    Arc<rockstream_gateway::subscribe_handler::SubscribeRegistry>,
) {
    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let durable = if recover {
        DurableCatalogStore::recover(store.clone(), catalog_prefix)
            .await
            .expect("catalog recovery should succeed")
    } else {
        DurableCatalogStore::new(store.clone(), catalog_prefix)
    };
    let catalog = Arc::new(CatalogStubs::new());
    catalog.set_durable_store(Arc::new(durable));
    if recover {
        catalog.sync_from_durable_store().await.unwrap();
    }
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
    );
    let registry = server.handler().subscribe_registry().clone();
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, shard_db, registry)
}

async fn stop_durable_gateway(handle: tokio::task::JoinHandle<()>, shard_db: Arc<ShardDb>) {
    handle.abort();
    let _ = handle.await;
    shard_db.flush().await.unwrap();
}

async fn read_rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let messages = client
        .simple_query(query)
        .await
        .expect("SELECT should succeed");
    let mut rows = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.columns().len())
                    .map(|index| row.get(index).unwrap_or("").to_owned())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn command_counts(messages: Vec<tokio_postgres::SimpleQueryMessage>) -> Vec<u64> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(tag),
            _ => None,
        })
        .collect()
}

async fn committed_frontier(shard_db: &ShardDb) -> Option<u64> {
    shard_db
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .unwrap()
        .map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap()))
}

fn idempotency_key_hash(value: &str) -> [u8; 16] {
    Sha256::digest(value.as_bytes())[..16].try_into().unwrap()
}

async fn assert_no_idempotency_record(shard_db: &ShardDb, key: &str) {
    assert_eq!(
        shard_db
            .get_idempotency_epoch(0, idempotency_key_hash(key))
            .await
            .unwrap(),
        None
    );
}

fn subscription_transcript(
    registry: &rockstream_gateway::subscribe_handler::SubscribeRegistry,
    table: &str,
) -> Vec<(u64, Vec<u8>, i8, Vec<u8>)> {
    registry
        .since_epoch(table, 0)
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            (
                entry.epoch,
                entry.row_key.to_vec(),
                entry.mz_diff,
                entry.encoded_row.to_vec(),
            )
        })
        .collect()
}

async fn read_view_map(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT view should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let k: i64 = row.get(0).unwrap().parse().unwrap();
            let v: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(k, v);
        }
    }
    state
}

async fn read_table_map(client: &tokio_postgres::Client, table: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT id, v FROM {table}"))
        .await
        .expect("SELECT table should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let k: i64 = row.get(0).unwrap().parse().unwrap();
            let v: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(k, v);
        }
    }
    state
}

const DML_CONFLICT_MESSAGE: &str = "transaction conflict: a row changed or a primary key became occupied after this transaction staged its writes; no writes were committed. Re-read current rows, resolve any key conflicts, and retry the transaction.";

async fn create_conflict_relation(client: &tokio_postgres::Client, table: &str, view: &str) {
    client
        .simple_query(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "CREATE MATERIALIZED VIEW {view} AS SELECT id, SUM(v) FROM {table} GROUP BY id"
        ))
        .await
        .unwrap();
}

async fn read_table_and_view(
    client: &tokio_postgres::Client,
    table: &str,
    view: &str,
) -> (Vec<Vec<String>>, Vec<Vec<String>>) {
    (
        read_rows(client, &format!("SELECT * FROM {table}")).await,
        read_rows(client, &format!("SELECT * FROM {view}")).await,
    )
}

fn pair_rows(rows: &[(i64, i64)]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|(id, value)| vec![id.to_string(), value.to_string()])
        .collect()
}

async fn assert_dml_conflict(client: &tokio_postgres::Client) {
    let error = client
        .simple_query("COMMIT")
        .await
        .expect_err("conflicting transaction must not be acknowledged");
    let db_error = error
        .as_db_error()
        .expect("commit conflict must be a DB error");
    assert_eq!(db_error.code().code(), "40001");
    assert_eq!(db_error.message(), DML_CONFLICT_MESSAGE);
    assert_eq!(
        command_counts(client.simple_query("ROLLBACK").await.unwrap()),
        vec![0]
    );
}

#[tokio::test]
async fn stale_update_and_delete_conflicts_abort_every_write_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "dml-stale-preimage-conflict";
    let catalog_prefix = "dml-stale-preimage-conflict-catalog";
    let (port, server, shard_db, registry) =
        start_durable_gateway(shard_path, store.clone(), catalog_prefix, false).await;
    let (a, a_connection) = connect_port_with_task(port).await;
    let (b, b_connection) = connect_port_with_task(port).await;
    create_conflict_relation(&b, "t_stale", "mv_stale").await;
    assert_eq!(
        command_counts(
            b.simple_query("INSERT INTO t_stale VALUES (1, 10)")
                .await
                .unwrap()
        ),
        vec![1]
    );

    a.simple_query("SET rockstream.idempotency_key = 'stale-update'")
        .await
        .unwrap();
    a.simple_query("BEGIN").await.unwrap();
    a.simple_query("UPDATE t_stale SET v = 11 WHERE id = 1")
        .await
        .unwrap();
    a.simple_query("INSERT INTO t_stale VALUES (3, 30)")
        .await
        .unwrap();
    // The awaited autocommit response synchronizes B's change before A commits.
    assert_eq!(
        command_counts(
            b.simple_query("UPDATE t_stale SET v = 99 WHERE id = 1")
                .await
                .unwrap()
        ),
        vec![1]
    );
    let before = read_table_and_view(&b, "t_stale", "mv_stale").await;
    assert_eq!(before.0, pair_rows(&[(1, 99)]));
    assert_eq!(before.1, pair_rows(&[(1, 99)]));
    let frontier = committed_frontier(&shard_db).await;
    let transcript = subscription_transcript(&registry, "t_stale");
    assert_dml_conflict(&a).await;
    assert_no_idempotency_record(&shard_db, "stale-update").await;
    assert_eq!(read_table_and_view(&b, "t_stale", "mv_stale").await, before);
    assert_eq!(committed_frontier(&shard_db).await, frontier);
    assert_eq!(subscription_transcript(&registry, "t_stale"), transcript);

    drop(a);
    drop(b);
    let _ = a_connection.await;
    let _ = b_connection.await;
    stop_durable_gateway(server, shard_db).await;

    let (port, server, shard_db, registry) =
        start_durable_gateway(shard_path, store.clone(), catalog_prefix, true).await;
    let (a, a_connection) = connect_port_with_task(port).await;
    let (b, b_connection) = connect_port_with_task(port).await;
    let expected = pair_rows(&[(1, 99)]);
    assert_eq!(
        read_table_and_view(&b, "t_stale", "mv_stale").await,
        (expected.clone(), expected)
    );

    a.simple_query("SET rockstream.idempotency_key = 'stale-delete'")
        .await
        .unwrap();
    a.simple_query("BEGIN").await.unwrap();
    a.simple_query("INSERT INTO t_stale VALUES (4, 40)")
        .await
        .unwrap();
    a.simple_query("DELETE FROM t_stale WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        command_counts(
            b.simple_query("UPDATE t_stale SET v = 100 WHERE id = 1")
                .await
                .unwrap()
        ),
        vec![1]
    );
    let before = read_table_and_view(&b, "t_stale", "mv_stale").await;
    assert_eq!(before.0, pair_rows(&[(1, 100)]));
    assert_eq!(before.1, pair_rows(&[(1, 100)]));
    let frontier = committed_frontier(&shard_db).await;
    let transcript = subscription_transcript(&registry, "t_stale");
    assert_dml_conflict(&a).await;
    assert_no_idempotency_record(&shard_db, "stale-delete").await;
    assert_eq!(read_table_and_view(&b, "t_stale", "mv_stale").await, before);
    assert_eq!(committed_frontier(&shard_db).await, frontier);
    assert_eq!(subscription_transcript(&registry, "t_stale"), transcript);

    drop(a);
    drop(b);
    let _ = a_connection.await;
    let _ = b_connection.await;
    stop_durable_gateway(server, shard_db).await;

    let (port, server, shard_db, _) =
        start_durable_gateway(shard_path, store, catalog_prefix, true).await;
    let (client, connection) = connect_port_with_task(port).await;
    let expected = pair_rows(&[(1, 100)]);
    assert_eq!(
        read_table_and_view(&client, "t_stale", "mv_stale").await,
        (expected.clone(), expected)
    );
    drop(client);
    let _ = connection.await;
    stop_durable_gateway(server, shard_db).await;
}

#[tokio::test]
async fn occupied_update_and_insert_keys_conflict_atomically_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "dml-occupied-key-conflict";
    let catalog_prefix = "dml-occupied-key-conflict-catalog";
    let (port, server, shard_db, registry) =
        start_durable_gateway(shard_path, store.clone(), catalog_prefix, false).await;
    let (a, a_connection) = connect_port_with_task(port).await;
    let (b, b_connection) = connect_port_with_task(port).await;
    create_conflict_relation(&b, "t_occupied", "mv_occupied").await;
    b.simple_query("INSERT INTO t_occupied VALUES (1, 10)")
        .await
        .unwrap();
    a.simple_query("SET rockstream.idempotency_key = 'occupied-update'")
        .await
        .unwrap();
    a.simple_query("BEGIN").await.unwrap();
    a.simple_query("UPDATE t_occupied SET id = 2 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        command_counts(
            b.simple_query("INSERT INTO t_occupied VALUES (2, 20)")
                .await
                .unwrap()
        ),
        vec![1]
    );
    let before = read_table_and_view(&b, "t_occupied", "mv_occupied").await;
    let expected = pair_rows(&[(1, 10), (2, 20)]);
    assert_eq!(before, (expected.clone(), expected));
    let frontier = committed_frontier(&shard_db).await;
    let transcript = subscription_transcript(&registry, "t_occupied");
    assert_dml_conflict(&a).await;
    assert_no_idempotency_record(&shard_db, "occupied-update").await;
    assert_eq!(
        read_table_and_view(&b, "t_occupied", "mv_occupied").await,
        before
    );
    assert_eq!(committed_frontier(&shard_db).await, frontier);
    assert_eq!(subscription_transcript(&registry, "t_occupied"), transcript);

    drop(a);
    drop(b);
    let _ = a_connection.await;
    let _ = b_connection.await;
    stop_durable_gateway(server, shard_db).await;

    let (port, server, shard_db, registry) =
        start_durable_gateway(shard_path, store.clone(), catalog_prefix, true).await;
    let (a, a_connection) = connect_port_with_task(port).await;
    let (b, b_connection) = connect_port_with_task(port).await;
    let expected = pair_rows(&[(1, 10), (2, 20)]);
    assert_eq!(
        read_table_and_view(&b, "t_occupied", "mv_occupied").await,
        (expected.clone(), expected)
    );

    a.simple_query("SET rockstream.idempotency_key = 'occupied-insert'")
        .await
        .unwrap();
    a.simple_query("BEGIN").await.unwrap();
    a.simple_query("INSERT INTO t_occupied VALUES (3, 30)")
        .await
        .unwrap();
    a.simple_query("INSERT INTO t_occupied VALUES (4, 40)")
        .await
        .unwrap();
    assert_eq!(
        command_counts(
            b.simple_query("INSERT INTO t_occupied VALUES (3, 33)")
                .await
                .unwrap()
        ),
        vec![1]
    );
    let before = read_table_and_view(&b, "t_occupied", "mv_occupied").await;
    let expected = pair_rows(&[(1, 10), (2, 20), (3, 33)]);
    assert_eq!(before, (expected.clone(), expected));
    let frontier = committed_frontier(&shard_db).await;
    let transcript = subscription_transcript(&registry, "t_occupied");
    assert_dml_conflict(&a).await;
    assert_no_idempotency_record(&shard_db, "occupied-insert").await;
    assert_eq!(
        read_table_and_view(&b, "t_occupied", "mv_occupied").await,
        before
    );
    assert_eq!(committed_frontier(&shard_db).await, frontier);
    assert_eq!(subscription_transcript(&registry, "t_occupied"), transcript);

    drop(a);
    drop(b);
    let _ = a_connection.await;
    let _ = b_connection.await;
    stop_durable_gateway(server, shard_db).await;

    let (port, server, shard_db, _) =
        start_durable_gateway(shard_path, store, catalog_prefix, true).await;
    let (client, connection) = connect_port_with_task(port).await;
    let expected = pair_rows(&[(1, 10), (2, 20), (3, 33)]);
    assert_eq!(
        read_table_and_view(&client, "t_occupied", "mv_occupied").await,
        (expected.clone(), expected)
    );
    drop(client);
    let _ = connection.await;
    stop_durable_gateway(server, shard_db).await;
}

#[tokio::test]
async fn sequential_transaction_changes_validate_against_staged_rows() {
    let (port, _server, _shard_db, _store) = start_gateway_with_shard("test_dml_staged_rows").await;
    let client = connect_port(port).await;
    create_conflict_relation(&client, "t_staged", "mv_staged").await;
    client
        .simple_query("INSERT INTO t_staged VALUES (1, 10)")
        .await
        .unwrap();

    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE t_staged SET id = 2 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("UPDATE t_staged SET v = 30 WHERE id = 2")
        .await
        .unwrap();
    client
        .simple_query("UPDATE t_staged SET id = 3 WHERE id = 2")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t_staged VALUES (4, 40)")
        .await
        .unwrap();
    client
        .simple_query("UPDATE t_staged SET v = 41 WHERE id = 4")
        .await
        .unwrap();
    assert_eq!(
        command_counts(client.simple_query("COMMIT").await.unwrap()),
        vec![5]
    );
    let expected = pair_rows(&[(3, 30), (4, 41)]);
    assert_eq!(
        read_table_and_view(&client, "t_staged", "mv_staged").await,
        (expected.clone(), expected)
    );
}

#[tokio::test]
async fn test_atomic_update_deltas_and_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_delta_prop").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_delta (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_delta AS SELECT id, SUM(v) FROM t_delta GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_delta (id, v) VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 100);
    expected.insert(2, 200);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);

    // Single row update emits -old/+new
    client
        .simple_query("UPDATE t_delta SET v = 150 WHERE id = 1")
        .await
        .unwrap();
    expected.insert(1, 150);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);

    // Multi-row update with expression
    client
        .simple_query("UPDATE t_delta SET v = v + 10 WHERE id >= 1")
        .await
        .unwrap();
    expected.insert(1, 160);
    expected.insert(2, 210);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);
}

#[tokio::test]
async fn test_atomic_single_row_update_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_single_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_single (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_single AS SELECT id, SUM(v) FROM t_single GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_single (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    assert_eq!(read_view_map(&client, "mv_single").await, expected);

    client
        .simple_query("UPDATE t_single SET v = 20 WHERE id = 1")
        .await
        .unwrap();

    expected.insert(1, 20);
    assert_eq!(read_table_map(&client, "t_single").await, expected);
    assert_eq!(read_view_map(&client, "mv_single").await, expected);
}

#[tokio::test]
async fn test_atomic_multi_row_update_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_multi_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_multi (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_multi AS SELECT id, SUM(v) FROM t_multi GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_multi (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t_multi SET v = v + 5 WHERE id > 1")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    expected.insert(2, 25);
    expected.insert(3, 35);
    assert_eq!(read_table_map(&client, "t_multi").await, expected);
    assert_eq!(read_view_map(&client, "mv_multi").await, expected);
}

#[tokio::test]
async fn test_multi_row_update_failure_rolls_back_completely() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_fail_roll").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_fail (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_fail AS SELECT id, SUM(v) FROM t_fail GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_fail (id, v) VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let mut orig = HashMap::new();
    orig.insert(1, 10);
    orig.insert(2, 20);
    assert_eq!(read_table_map(&client, "t_fail").await, orig);
    assert_eq!(read_view_map(&client, "mv_fail").await, orig);

    // Row 1 (v=10): 100 / (10 - 20) = -10
    // Row 2 (v=20): 100 / (20 - 20) -> division by zero
    // Whole statement must fail without altering any state.
    let err = client
        .simple_query("UPDATE t_fail SET v = 100 / (v - 20) WHERE id >= 1")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("must be DB error");
    let err_msg = db_err.message();
    let code = db_err.code().code();
    assert!(
        code == "22012" || err_msg.contains("RS-1016") || err_msg.contains("division by zero"),
        "error should indicate division by zero, got code {code}: {err_msg}"
    );

    // Verify all rows remain completely unchanged
    assert_eq!(read_table_map(&client, "t_fail").await, orig);
    assert_eq!(read_view_map(&client, "mv_fail").await, orig);
}

#[tokio::test]
async fn test_pk_update_retraction_and_insertion() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_pk_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_pk (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv_pk AS SELECT id, SUM(v) FROM t_pk GROUP BY id")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_pk (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    // Update the primary key from 1 to 2
    client
        .simple_query("UPDATE t_pk SET id = 2 WHERE id = 1")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(2, 10);
    assert_eq!(read_table_map(&client, "t_pk").await, expected);
    assert_eq!(read_view_map(&client, "mv_pk").await, expected);
}

#[tokio::test]
async fn test_transaction_rollback_discards_all_dml() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_tx_roll").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_tx_roll (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_tx_roll AS SELECT id, SUM(v) FROM t_tx_roll GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_tx_roll (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut orig = HashMap::new();
    orig.insert(1, 10);
    assert_eq!(read_table_map(&client, "t_tx_roll").await, orig);
    assert_eq!(read_view_map(&client, "mv_tx_roll").await, orig);

    // Client executes in transaction block then rolls back
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE t_tx_roll SET v = 99 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM t_tx_roll WHERE id = 1")
        .await
        .unwrap();
    client.simple_query("ROLLBACK").await.unwrap();

    // Original state must be preserved
    assert_eq!(read_table_map(&client, "t_tx_roll").await, orig);
    assert_eq!(read_view_map(&client, "mv_tx_roll").await, orig);
}

#[tokio::test]
async fn test_transaction_commit_publishes_atomic_epoch() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_tx_commit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_tx_commit (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_tx_commit AS SELECT id, SUM(v) FROM t_tx_commit GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_tx_commit (id, v) VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE t_tx_commit SET v = 15 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t_tx_commit (id, v) VALUES (3, 30)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 15);
    expected.insert(2, 20);
    expected.insert(3, 30);
    assert_eq!(read_table_map(&client, "t_tx_commit").await, expected);
    assert_eq!(read_view_map(&client, "mv_tx_commit").await, expected);
}

#[tokio::test]
async fn test_view_reader_isolation_during_dml() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_view_iso").await;
    let client1 = connect_port(port).await;
    let client2 = connect_port(port).await;

    client1
        .simple_query("CREATE TABLE t_iso (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client1
        .simple_query("CREATE MATERIALIZED VIEW mv_iso AS SELECT id, SUM(v) FROM t_iso GROUP BY id")
        .await
        .unwrap();

    client1
        .simple_query("INSERT INTO t_iso (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);

    // Client 1 begins transaction and mutates data
    client1.simple_query("BEGIN").await.unwrap();
    client1
        .simple_query("UPDATE t_iso SET v = 999 WHERE id = 1")
        .await
        .unwrap();

    // Client 2 queries view concurrently: reader isolation ensures it sees committed data (10), not 999
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);

    // Client 1 rolls back
    client1.simple_query("ROLLBACK").await.unwrap();

    // Client 2 still sees 10
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);
}
