//! Type Completeness: Text, Temporal & UUID Keys, Joins and Windows Tests (v0.59.20 Slice 4 / Phase 3a).

use std::sync::Arc;

use object_store::local::LocalFileSystem;
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

async fn client() -> (tokio_postgres::Client, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("type-completeness-keys", store)
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

#[tokio::test]
async fn test_text_keys_join_and_aggregates() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_text_left (name TEXT, department TEXT)")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE t_text_right (department TEXT, budget BIGINT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_text_left VALUES ('Alice', 'Engineering'), ('Bob', 'Marketing'), ('Carol', 'Engineering')")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_text_right VALUES ('Engineering', 500000), ('Marketing', 200000)",
        )
        .await
        .unwrap();

    // Equi-join on TEXT key
    let res = client
        .simple_query("SELECT l.name, r.department, r.budget FROM t_text_left l JOIN t_text_right r ON l.department = r.department ORDER BY l.name")
        .await
        .unwrap();

    let mut rows = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            rows.push((
                row.get(0).unwrap().to_string(),
                row.get(1).unwrap().to_string(),
                row.get(2).unwrap().to_string(),
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            (
                "Alice".to_string(),
                "Engineering".to_string(),
                "500000".to_string()
            ),
            (
                "Bob".to_string(),
                "Marketing".to_string(),
                "200000".to_string()
            ),
            (
                "Carol".to_string(),
                "Engineering".to_string(),
                "500000".to_string()
            ),
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn test_temporal_keys_join_and_grouping() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_events (id INT, event_date DATE, amount BIGINT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_events VALUES (1, '2026-09-01', 100), (2, '2026-09-01', 200), (3, '2026-09-02', 300)")
        .await
        .unwrap();

    // Grouping by DATE key
    let res = client
        .simple_query("SELECT event_date, SUM(amount) as total FROM t_events GROUP BY event_date ORDER BY event_date")
        .await
        .unwrap();

    let mut groups = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            groups.push((
                row.get(0).unwrap().to_string(),
                row.get(1).unwrap().to_string(),
            ));
        }
    }

    assert_eq!(
        groups,
        vec![
            ("2026-09-01".to_string(), "300".to_string()),
            ("2026-09-02".to_string(), "300".to_string()),
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn test_uuid_keys_join() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_uuid_left (id UUID, label TEXT)")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE t_uuid_right (id UUID, value INT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_uuid_left VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'item_1'), ('b1ffcd00-1c0b-4ef8-bb6d-6bb9bd380a22', 'item_2')")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_uuid_right VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 42)",
        )
        .await
        .unwrap();

    let res = client
        .simple_query(
            "SELECT l.label, r.value FROM t_uuid_left l JOIN t_uuid_right r ON l.id = r.id",
        )
        .await
        .unwrap();

    let mut rows = Vec::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            rows.push((
                row.get(0).unwrap().to_string(),
                row.get(1).unwrap().to_string(),
            ));
        }
    }

    assert_eq!(rows, vec![("item_1".to_string(), "42".to_string())]);

    handle.abort();
}
