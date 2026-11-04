//! v0.51.6 Slice 4 Durability (LFS): the compiled `AVG` view's row encoding
//! changed from `Int64` (truncating) to `Float64` (true division) — this
//! new `TAG_FLOAT64` row encoding (see `sink.rs`) must survive a gateway
//! process restart/reload against the same SlateDB-on-LocalFileSystem
//! backend, and post-restart commits continue producing the correct
//! fractional average on top of the restored `(sum, count)` state — not
//! reinterpreted as a stale `Int64` tag, and not recomputed from scratch.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
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

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

async fn read_avg_state(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, f64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let category: i64 = row.get(0).unwrap().parse().unwrap();
            let avg_qty: f64 = row.get(1).unwrap().parse().unwrap();
            state.insert(category, avg_qty);
        }
    }
    state
}

async fn read_pairs(client: &tokio_postgres::Client, relation: &str) -> Vec<(i64, i64)> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {relation}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().parse().unwrap(),
                row.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

async fn read_triples(client: &tokio_postgres::Client, relation: &str) -> Vec<(i64, i64, i64)> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {relation}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().parse().unwrap(),
                row.get(1).unwrap().parse().unwrap(),
                row.get(2).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

async fn read_text_pairs(client: &tokio_postgres::Client, relation: &str) -> Vec<(String, i64)> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {relation}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().to_string(),
                row.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

async fn read_text_triples(
    client: &tokio_postgres::Client,
    relation: &str,
) -> Vec<(String, i64, i64)> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {relation}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().to_string(),
                row.get(1).unwrap().parse().unwrap(),
                row.get(2).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn legacy_mixed_group_checkpoint_survives_restart() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("legacy-group-keys", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE legacy_t (label TEXT, id BIGINT, value BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO legacy_t VALUES ('a', 1, 10), ('b', 2, 20)")
        .await
        .unwrap();
    client.simple_query("CREATE MATERIALIZED VIEW legacy_mv AS SELECT label, id, SUM(value) FROM legacy_t GROUP BY label, id").await.unwrap();
    client.simple_query("CREATE MATERIALIZED VIEW legacy_fn AS SELECT upper(label), id, SUM(value) FROM legacy_t GROUP BY upper(label), id").await.unwrap();
    let records = db.scan_prefix(&[0x01, 0x4b, 0x50]).await.unwrap();
    let mut labels = records
        .iter()
        .filter_map(|(key, _)| {
            key.windows(6)
                .find(|bytes| bytes[..5] == [1, 0, 0, 0, 1] && b"ABab".contains(&bytes[5]))
                .map(|bytes| bytes[5] as char)
        })
        .collect::<Vec<_>>();
    labels.sort_unstable();
    assert_eq!(labels, vec!['A', 'B', 'a', 'b']);
    let mut batch = rockstream_storage::WriteBatch::new();
    for (key, value) in records {
        batch.put(&key, &value[..8]);
    }
    db.write_batch(batch).await.unwrap();
    db.flush().await.unwrap();
    handle.abort();
    let (port, _handle, _db) = start_gateway("legacy-group-keys", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("INSERT INTO legacy_t VALUES ('a', 1, 5), ('c', 3, 30)")
        .await
        .unwrap();
    assert_eq!(
        read_text_triples(&client, "legacy_mv").await,
        vec![
            ("a".to_string(), 1, 15),
            ("b".to_string(), 2, 20),
            ("c".to_string(), 3, 30)
        ]
    );
    assert_eq!(
        read_text_triples(&client, "legacy_fn").await,
        vec![
            ("A".to_string(), 1, 15),
            ("B".to_string(), 2, 20),
            ("C".to_string(), 3, 30)
        ]
    );
}

#[tokio::test]
async fn copy_from_stdin_updates_compiled_views() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) = start_gateway("copy-views", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE copy_source (id BIGINT, value BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW copy_view AS SELECT id, value FROM copy_source")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let sink: tokio_postgres::CopyInSink<bytes::Bytes> =
            client.copy_in("COPY copy_source FROM STDIN").await.unwrap();
        tokio::pin!(sink);
        futures::SinkExt::send(&mut sink, bytes::Bytes::from_static(b"1\t10\n2\t20\n"))
            .await
            .unwrap();
        assert_eq!(sink.finish().await.unwrap(), 2);
    })
    .await
    .expect("COPY did not finish");
    let reader = connect_port(port).await;
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            read_pairs(&reader, "copy_source")
        )
        .await
        .expect("base read timed out"),
        vec![(1, 10), (2, 20)]
    );
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            read_pairs(&reader, "copy_view")
        )
        .await
        .expect("view read timed out"),
        vec![(1, 10), (2, 20)]
    );
    db.fail_next_view_output_write();
    let failing_client = connect_port(port).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let sink: tokio_postgres::CopyInSink<bytes::Bytes> = failing_client
            .copy_in("COPY copy_source FROM STDIN")
            .await
            .unwrap();
        tokio::pin!(sink);
        futures::SinkExt::send(&mut sink, bytes::Bytes::from_static(b"3\t30\n"))
            .await
            .unwrap();
        assert!(sink.finish().await.is_err());
    })
    .await
    .expect("failed COPY did not finish");
    assert_eq!(
        read_pairs(&reader, "copy_source").await,
        vec![(1, 10), (2, 20)]
    );
    assert_eq!(
        read_pairs(&reader, "copy_view").await,
        vec![(1, 10), (2, 20)]
    );
    reader
        .simple_query("INSERT INTO copy_source VALUES (4, 40)")
        .await
        .unwrap();
    let expected = vec![(1, 10), (2, 20), (4, 40)];
    assert_eq!(read_pairs(&reader, "copy_view").await, expected);
    handle.abort();
    let (port, _handle, _db) = start_gateway("copy-views", store, catalog).await;
    let reopened = connect_port(port).await;
    assert_eq!(read_pairs(&reopened, "copy_source").await, expected);
    assert_eq!(read_pairs(&reopened, "copy_view").await, expected);
}

#[tokio::test]
async fn failed_atomic_commit_restores_window_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("window-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client.simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t").await.unwrap();
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 10, 1)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 10, 1)]);
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(
        read_triples(&client, "mv").await,
        vec![(1, 10, 1), (3, 30, 2)]
    );
    handle.abort();
    let (port, _handle, _db) = start_gateway("window-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(
        read_triples(&client, "mv").await,
        vec![(1, 10, 1), (3, 30, 2)]
    );
}

#[tokio::test]
async fn failed_atomic_commit_restores_topk_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("topk-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    let select = "SELECT id, v FROM (SELECT id, v, ROW_NUMBER() OVER (ORDER BY v DESC) AS rn FROM t) WHERE rn <= 1";
    let explain = client
        .simple_query(&format!("EXPLAIN INCREMENTAL {select}"))
        .await
        .unwrap();
    assert!(
        explain.iter().any(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0).is_some_and(|value| value.contains("TopK")))),
        "{explain:?}"
    );
    client
        .simple_query(&format!("CREATE MATERIALIZED VIEW mv AS {select}"))
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(3, 30)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("topk-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(read_pairs(&client, "mv").await, vec![(3, 30)]);
    client
        .simple_query("INSERT INTO t VALUES (4, 5)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(3, 30)]);
}

#[tokio::test]
async fn view_output_failure_must_not_acknowledge_incomplete_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) = start_gateway("view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    let before = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .unwrap();
    db.fail_next_view_output_write();
    let result = client.simple_query("COMMIT").await;
    let frontier = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    let table = read_pairs(&client, "t").await;
    let view = read_pairs(&client, "mv").await;
    handle.abort();
    let (port2, _handle2, _db2) = start_gateway("view-failure", store, catalog).await;
    let client2 = connect_port(port2).await;
    let reopened_table = read_pairs(&client2, "t").await;
    let reopened_view = read_pairs(&client2, "mv").await;
    client2
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    let final_table = read_pairs(&client2, "t").await;
    let final_view = read_pairs(&client2, "mv").await;
    let initial = vec![(1, 10)];
    let committed = vec![(1, 10), (2, 20)];
    let valid_outcome = if result.is_ok() {
        frontier > before && table == committed
    } else {
        (table == initial && frontier == before) || (table == committed && frontier > before)
    };
    let mut expected_final = table.clone();
    expected_final.push((3, 30));
    assert!(
        valid_outcome && view == table && reopened_table == table && reopened_view == table
            && final_table == expected_final && final_view == expected_final,
        "commit={result:?}, frontier={before}->{frontier}, table={table:?}, view={view:?}, reopened table={reopened_table:?}, reopened view={reopened_view:?}, final table={final_table:?}, final view={final_view:?}"
    );
}

#[tokio::test]
async fn failed_atomic_commit_restores_aggregate_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("aggregate-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    let before = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(
        db.last_epoch().load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (3, 30)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("aggregate-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (3, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (3, 30)]);
}

#[tokio::test]
async fn failed_atomic_commit_restores_minmax_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("minmax-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, MIN(v) FROM t GROUP BY id")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (2, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 30)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("minmax-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 30)]);
}

#[tokio::test]
async fn failed_commit_does_not_reuse_composite_group_surrogates() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("composite-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 1, 1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT a, b, SUM(v) FROM t GROUP BY a, b")
        .await
        .unwrap();
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 1, 10)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 2, 2, 20)")
        .await
        .is_err());
    client
        .simple_query("INSERT INTO t VALUES (3, 3, 3, 30)")
        .await
        .unwrap();
    assert_eq!(
        read_triples(&client, "mv").await,
        vec![(1, 1, 10), (3, 3, 30)]
    );
    client
        .simple_query("INSERT INTO t VALUES (4, 2, 2, 40)")
        .await
        .unwrap();
    assert_eq!(
        read_triples(&client, "mv").await,
        vec![(1, 1, 10), (2, 2, 40), (3, 3, 30)]
    );
    handle.abort();
    let (port, _handle, _db) = start_gateway("composite-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("INSERT INTO t VALUES (5, 4, 4, 50)")
        .await
        .unwrap();
    assert_eq!(
        read_triples(&client, "mv").await,
        vec![(1, 1, 10), (2, 2, 40), (3, 3, 30), (4, 4, 50)]
    );
}

#[tokio::test]
async fn failed_commit_does_not_reuse_text_group_surrogates() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("text-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, k TEXT, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 'a', 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) FROM t GROUP BY k")
        .await
        .unwrap();
    assert_eq!(read_text_pairs(&client, "mv").await, vec![("a".into(), 10)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 'b', 20)")
        .await
        .is_err());
    client
        .simple_query("INSERT INTO t VALUES (3, 'c', 30)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (4, 'b', 40)")
        .await
        .unwrap();
    assert_eq!(
        read_text_pairs(&client, "mv").await,
        vec![("a".into(), 10), ("b".into(), 40), ("c".into(), 30)]
    );
    handle.abort();
    let (port, _handle, _db) = start_gateway("text-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("INSERT INTO t VALUES (5, 'd', 50)")
        .await
        .unwrap();
    assert_eq!(
        read_text_pairs(&client, "mv").await,
        vec![
            ("a".into(), 10),
            ("b".into(), 40),
            ("c".into(), 30),
            ("d".into(), 50)
        ]
    );
}

#[tokio::test]
async fn failed_atomic_commit_restores_join_before_next_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("join-view-failure", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client.simple_query("CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT); CREATE TABLE b (id BIGINT PRIMARY KEY, v BIGINT)").await.unwrap();
    client
        .simple_query("INSERT INTO a VALUES (1, 10); INSERT INTO b VALUES (1, 100)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv AS SELECT a.id, a.v, b.v FROM a JOIN b ON a.id = b.id",
        )
        .await
        .unwrap();
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 10, 100)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("UPDATE a SET v = 20 WHERE id = 1")
        .await
        .is_err());
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 10, 100)]);
    client
        .simple_query("UPDATE a SET v = 30 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "a").await, vec![(1, 30)]);
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 30, 100)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("join-view-failure", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 30, 100)]);
    client
        .simple_query("UPDATE b SET v = 200 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "a").await, vec![(1, 30)]);
    assert_eq!(read_pairs(&client, "b").await, vec![(1, 200)]);
    assert_eq!(read_triples(&client, "mv").await, vec![(1, 30, 200)]);
}

#[tokio::test]
async fn committed_rows_reach_transitive_materialized_views() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("transitive-view-commit", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW first AS SELECT id, v FROM t")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW second AS SELECT id, v FROM first WHERE v > 0")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "first").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "second").await, vec![(1, 10)]);
    db.fail_next_view_output_write();
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "first").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "second").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 20)]);
    assert_eq!(read_pairs(&client, "first").await, vec![(1, 10), (2, 20)]);
    assert_eq!(read_pairs(&client, "second").await, vec![(1, 10), (2, 20)]);
    handle.abort();
    let (port, _handle, _db) = start_gateway("transitive-view-commit", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&client, "t").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
    assert_eq!(
        read_pairs(&client, "first").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
    assert_eq!(
        read_pairs(&client, "second").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
}

#[tokio::test]
async fn failed_rollback_cannot_corrupt_a_later_commit() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, db) = start_gateway("failed-aggregate-rollback", store, catalog).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    db.fail_next_view_output_write();
    db.set_fail_scans(true);
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert!(client.simple_query("SELECT * FROM mv").await.is_err());
    db.set_fail_scans(false);
    client
        .simple_query("INSERT INTO t VALUES (2, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 30)]);
}

#[tokio::test]
async fn failed_flush_blocks_reads_until_complete_epoch_is_visible() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, db) =
        start_gateway("failed-view-flush", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    db.set_fail_flushes(true);
    assert!(client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .is_err());
    assert!(client.simple_query("SELECT * FROM mv").await.is_err());
    db.set_fail_flushes(false);
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 20)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 20)]);
    client
        .simple_query("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    assert_eq!(
        read_pairs(&client, "mv").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
    handle.abort();
    let (port, _handle, _db) = start_gateway("failed-view-flush", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(
        read_pairs(&client, "t").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
    assert_eq!(
        read_pairs(&client, "mv").await,
        vec![(1, 10), (2, 20), (3, 30)]
    );
}

#[tokio::test]
async fn pgwire_cancel_during_commit_preserves_complete_view() {
    for phase in 0..3 {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let catalog = Arc::new(CatalogStubs::new());
        let db = Arc::new(
            ShardDb::builder("pgwire-cancel-commit", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let server = GatewayServer::with_shard_db(
            "127.0.0.1:0".parse().unwrap(),
            catalog.clone(),
            Arc::new(NoopViewReader),
            db.clone(),
        );
        let handler = server.handler().clone();
        let (addr, handle) = server.serve_background().await.unwrap();
        let port = addr.port();
        let client = connect_port(port).await;
        client
            .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO t VALUES (1, 10)")
            .await
            .unwrap();
        client
            .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
            .await
            .unwrap();
        let before = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        match phase {
            0 => db.pause_next_view_output_write(entered.clone(), release.clone()),
            1 => db.pause_after_next_view_write(entered.clone(), release.clone()),
            _ => db.pause_next_flush(entered.clone(), release.clone()),
        }
        let token = client.cancel_token();
        let mut cancellation = handler
            .cancellation_registry
            .iter()
            .next()
            .unwrap()
            .value()
            .clone();
        let commit =
            tokio::spawn(async move { client.simple_query("INSERT INTO t VALUES (2, 20)").await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        token.cancel_query(NoTls).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), cancellation.cancelled())
            .await
            .unwrap();
        release.notify_one();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), commit)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                result.as_deref(),
                Ok([tokio_postgres::SimpleQueryMessage::CommandComplete(1)])
            ),
            "unexpected commit response: {result:?}"
        );
        assert!(db.last_epoch().load(std::sync::atomic::Ordering::SeqCst) > before);
        let observer = connect_port(port).await;
        assert_eq!(read_pairs(&observer, "t").await, vec![(1, 10), (2, 20)]);
        assert_eq!(read_pairs(&observer, "mv").await, vec![(1, 10), (2, 20)]);
        observer
            .simple_query("INSERT INTO t VALUES (3, 30)")
            .await
            .unwrap();
        assert_eq!(
            read_pairs(&observer, "t").await,
            vec![(1, 10), (2, 20), (3, 30)]
        );
        assert_eq!(
            read_pairs(&observer, "mv").await,
            vec![(1, 10), (2, 20), (3, 30)]
        );
        handle.abort();
        let (port, _handle, _db) = start_gateway("pgwire-cancel-commit", store, catalog).await;
        let reopened = connect_port(port).await;
        assert_eq!(
            read_pairs(&reopened, "t").await,
            vec![(1, 10), (2, 20), (3, 30)]
        );
        assert_eq!(
            read_pairs(&reopened, "mv").await,
            vec![(1, 10), (2, 20), (3, 30)]
        );
    }
}

#[tokio::test]
async fn cancelled_commit_restores_staged_view_before_next_query() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let db = Arc::new(
        ShardDb::builder("cancelled-view-commit", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        db.clone(),
    );
    let handler = server.handler().clone();
    let (addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, SUM(v) FROM t GROUP BY id")
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    db.pause_next_view_output_write(entered.clone(), release);
    let first_handler = handler.clone();
    let commit = tokio::spawn(async move {
        first_handler
            .dispatch_async_with_conn("INSERT INTO t VALUES (2, 20)", Some("cancelled-test"))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    commit.abort();
    assert!(matches!(commit.await, Err(error) if error.is_cancelled()));
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10)]);
    client
        .simple_query("INSERT INTO t VALUES (2, 30)")
        .await
        .unwrap();
    assert_eq!(read_pairs(&client, "t").await, vec![(1, 10), (2, 30)]);
    assert_eq!(read_pairs(&client, "mv").await, vec![(1, 10), (2, 30)]);
    let previous_epoch = db.last_epoch().load(std::sync::atomic::Ordering::SeqCst);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    db.pause_after_next_view_write(entered.clone(), release);
    let second_handler = handler.clone();
    let commit = tokio::spawn(async move {
        second_handler
            .dispatch_async_with_conn(
                "INSERT INTO t VALUES (3, 40)",
                Some("cancelled-after-write"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        db.last_epoch().load(std::sync::atomic::Ordering::SeqCst),
        previous_epoch
    );
    let observer = connect_port(addr.port()).await;
    let mut read = tokio::spawn(async move { read_pairs(&observer, "mv").await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut read)
            .await
            .is_err()
    );
    commit.abort();
    assert!(matches!(commit.await, Err(error) if error.is_cancelled()));
    assert_eq!(read.await.unwrap(), vec![(1, 10), (2, 30), (3, 40)]);
    assert!(db.last_epoch().load(std::sync::atomic::Ordering::SeqCst) > previous_epoch);
    assert_eq!(
        read_pairs(&client, "t").await,
        vec![(1, 10), (2, 30), (3, 40)]
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    db.pause_next_flush(entered.clone(), release);
    let commit = tokio::spawn(async move {
        handler
            .dispatch_async_with_conn(
                "INSERT INTO t VALUES (4, 50)",
                Some("cancelled-before-flush"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let observer = connect_port(addr.port()).await;
    let mut read = tokio::spawn(async move { read_pairs(&observer, "mv").await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut read)
            .await
            .is_err()
    );
    commit.abort();
    assert!(matches!(commit.await, Err(error) if error.is_cancelled()));
    assert_eq!(
        read.await.unwrap(),
        vec![(1, 10), (2, 30), (3, 40), (4, 50)]
    );
    handle.abort();
    let (port, _handle, _db) = start_gateway("cancelled-view-commit", store, catalog).await;
    let client = connect_port(port).await;
    assert_eq!(
        read_pairs(&client, "mv").await,
        vec![(1, 10), (2, 30), (3, 40), (4, 50)]
    );
}

#[tokio::test]
async fn avg_aggregate_fractional_mean_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "avg-aggregate-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cat_avg AS SELECT category, AVG(price) as avg_price FROM bid GROUP BY category",
        )
        .await
        .unwrap();
    // category 10: {100, 200} -> mean 150.0 (exact).
    // category 20: {50, 51} -> mean 50.5 (genuinely fractional).
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50), (4, 20, 51)",
        )
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart: reopen the same on-disk shard, no in-memory state carried
    // over except the (unpersisted-by-design) catalog handle.
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let restored = read_avg_state(&client2, "cat_avg").await;
    assert_eq!(
        restored,
        HashMap::from([(10, 150.0), (20, 50.5)]),
        "AVG's Float64 row encoding should survive restart with its pre-restart values, \
         not be reinterpreted as a stale Int64 tag"
    );

    // Post-restart commit must accumulate on top of the restored (sum,
    // count) state, not recompute from scratch: adding price=52 to category
    // 20 moves its true mean from 50.5 to (50+51+52)/3 = 51.0.
    client2
        .simple_query("INSERT INTO bid (id, category, price) VALUES (5, 20, 52)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_avg_state(&client2, "cat_avg").await;
    assert_eq!(
        after,
        HashMap::from([(10, 150.0), (20, 51.0)]),
        "post-restart commit should accumulate on top of the persisted pre-restart state \
         and still produce a true floating-point average"
    );
}
