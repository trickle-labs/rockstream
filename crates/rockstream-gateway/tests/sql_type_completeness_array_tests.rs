//! Type Completeness: Array Parameter Binding and ANY($1) Tests (v0.59.20 Slice 5 / Phase 3b).

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
        ShardDb::builder("type-completeness-array", store)
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
async fn test_array_parameter_binding_and_any() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_arr (id BIGINT, tag TEXT, val DOUBLE PRECISION)")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_arr VALUES \
             (1, 'apple', 10.5), \
             (2, 'banana', 20.0), \
             (3, 'cherry', 30.25), \
             (4, 'date', 40.0)",
        )
        .await
        .unwrap();

    // 1. Extended query protocol: INT8[] parameter binding with ANY($1)
    let rows_int = client
        .query(
            "SELECT id, tag FROM t_arr WHERE id = ANY($1) ORDER BY id",
            &[&vec![1i64, 3i64]],
        )
        .await
        .unwrap();

    let int_ids: Vec<i64> = rows_int.iter().map(|r| r.get(0)).collect();
    let int_tags: Vec<String> = rows_int.iter().map(|r| r.get(1)).collect();
    assert_eq!(int_ids, vec![1, 3]);
    assert_eq!(int_tags, vec!["apple", "cherry"]);

    // 2. Extended query protocol: TEXT[] parameter binding with ANY($1)
    let rows_str = client
        .query(
            "SELECT id, tag FROM t_arr WHERE tag = ANY($1) ORDER BY id",
            &[&vec!["banana".to_string(), "date".to_string()]],
        )
        .await
        .unwrap();

    let str_ids: Vec<i64> = rows_str.iter().map(|r| r.get(0)).collect();
    assert_eq!(str_ids, vec![2, 4]);

    // 3. Extended query protocol: Empty array parameter binding returns 0 rows
    let empty_vec: Vec<i64> = vec![];
    let rows_empty = client
        .query(
            "SELECT id FROM t_arr WHERE id = ANY($1) ORDER BY id",
            &[&empty_vec],
        )
        .await
        .unwrap();
    assert_eq!(rows_empty.len(), 0);

    // 4. Simple query protocol: array literal ANY(ARRAY['apple', 'cherry'])
    let rows_simple = client
        .simple_query("SELECT id FROM t_arr WHERE tag = ANY(ARRAY['apple', 'cherry']) ORDER BY id")
        .await
        .unwrap();

    let mut simple_ids = Vec::new();
    for msg in rows_simple {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            simple_ids.push(row.get(0).unwrap().to_string());
        }
    }
    assert_eq!(simple_ids, vec!["1", "3"]);

    handle.abort();
}

#[tokio::test]
async fn test_uuid_and_float_array_parameter_binding() {
    let (client, handle, _dir) = client().await;

    client
        .simple_query("CREATE TABLE t_items (id INT, uid UUID, score DOUBLE PRECISION)")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO t_items VALUES \
             (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1.5), \
             (2, 'b1ffcd00-1c0b-4ef8-bb6d-6bb9bd380a22', 2.5), \
             (3, 'c2eedd11-2d0c-4fa9-cc7e-7cc0ce490b33', 3.5)",
        )
        .await
        .unwrap();

    // FLOAT8[] parameter binding with ANY($1)
    let rows_float = client
        .query(
            "SELECT id FROM t_items WHERE score = ANY($1) ORDER BY id",
            &[&vec![1.5f64, 3.5f64]],
        )
        .await
        .unwrap();
    let float_ids: Vec<i32> = rows_float.iter().map(|r| r.get(0)).collect();
    assert_eq!(float_ids, vec![1, 3]);

    handle.abort();
}
