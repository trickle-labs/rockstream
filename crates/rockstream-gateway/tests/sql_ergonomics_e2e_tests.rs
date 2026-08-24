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
async fn test_sql_ergonomics_e2e_workflow() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());

    let (port, _handle, _shard_db) =
        start_gateway("e2e-workflow", store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;

    // 1. DDL IF NOT EXISTS workflow for Table
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS items (id INT, title TEXT, category TEXT, price INT);",
        )
        .await
        .unwrap();

    // Idempotent duplicate CREATE TABLE IF NOT EXISTS
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS items (id INT, title TEXT, category TEXT, price INT);",
        )
        .await
        .unwrap();

    // 2. Insert initial records
    client
        .batch_execute(
            "INSERT INTO items VALUES (1, 'keyboard', 'electronics', 100), (2, 'mouse', 'electronics', 50), (3, 'desk', 'furniture', 300);",
        )
        .await
        .unwrap();

    // 3. UPDATE ... RETURNING with simple query protocol and projection
    let rows = client
        .simple_query("UPDATE items SET price = 120 WHERE id = 1 RETURNING id, title, price;")
        .await
        .unwrap();
    let row_messages: Vec<_> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(row_messages.len(), 1);
    assert_eq!(row_messages[0].get("id"), Some("1"));
    assert_eq!(row_messages[0].get("title"), Some("keyboard"));
    assert_eq!(row_messages[0].get("price"), Some("120"));

    // 4. Extended Query protocol UPDATE ... RETURNING with parameters
    let stmt = client
        .prepare("UPDATE items SET price = $1 WHERE id = $2 RETURNING id, price;")
        .await
        .unwrap();
    let updated_rows = client.query(&stmt, &[&60i32, &2i32]).await.unwrap();
    assert_eq!(updated_rows.len(), 1);
    let id_val: i32 = updated_rows[0].get(0);
    let price_val: i32 = updated_rows[0].get(1);
    assert_eq!(id_val, 2);
    assert_eq!(price_val, 60);

    // 5. DELETE ... RETURNING with simple query protocol
    let del_rows = client
        .simple_query(
            "DELETE FROM items WHERE category = 'furniture' RETURNING id, title, category;",
        )
        .await
        .unwrap();
    let del_messages: Vec<_> = del_rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(del_messages.len(), 1);
    assert_eq!(del_messages[0].get("id"), Some("3"));
    assert_eq!(del_messages[0].get("title"), Some("desk"));
    assert_eq!(del_messages[0].get("category"), Some("furniture"));

    // 6. Extended Query protocol DELETE ... RETURNING with parameter
    let del_stmt = client
        .prepare("DELETE FROM items WHERE id = $1 RETURNING id, title;")
        .await
        .unwrap();
    let deleted_extended = client.query(&del_stmt, &[&1i32]).await.unwrap();
    assert_eq!(deleted_extended.len(), 1);
    let del_id: i32 = deleted_extended[0].get(0);
    let del_title: String = deleted_extended[0].get(1);
    assert_eq!(del_id, 1);
    assert_eq!(del_title, "keyboard");

    // 7. Harmonized DDL IF [NOT] EXISTS across other families
    client
        .batch_execute("CREATE VIEW IF NOT EXISTS v_items AS SELECT id FROM items;")
        .await
        .unwrap();
    client
        .batch_execute("CREATE VIEW IF NOT EXISTS v_items AS SELECT id FROM items;")
        .await
        .unwrap();
    client
        .batch_execute("DROP VIEW IF EXISTS v_items;")
        .await
        .unwrap();
    client
        .batch_execute("DROP VIEW IF EXISTS v_items;")
        .await
        .unwrap();

    client
        .batch_execute("CREATE INDEX IF NOT EXISTS idx_items_title ON items(title);")
        .await
        .unwrap();
    client
        .batch_execute("CREATE INDEX IF NOT EXISTS idx_items_title ON items(title);")
        .await
        .unwrap();
    client
        .batch_execute("DROP INDEX IF EXISTS idx_items_title;")
        .await
        .unwrap();
    client
        .batch_execute("DROP INDEX IF EXISTS idx_items_title;")
        .await
        .unwrap();

    client
        .batch_execute("CREATE WORKLOAD IF NOT EXISTS wl_analytics WITH (MEMORY_LIMIT = 1048576);")
        .await
        .unwrap();
    client
        .batch_execute("DROP WORKLOAD IF EXISTS wl_analytics;")
        .await
        .unwrap();

    client
        .batch_execute("CREATE SECRET IF NOT EXISTS sec_api_key (TYPE = 'postgres_role', PASSWORD = 'secret_123');")
        .await
        .unwrap();
    client
        .batch_execute("DROP SECRET IF EXISTS sec_api_key;")
        .await
        .unwrap();

    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS reporting;")
        .await
        .unwrap();
    client
        .batch_execute("DROP SCHEMA IF EXISTS reporting;")
        .await
        .unwrap();

    // 8. Clean drop of items table
    client
        .batch_execute("DROP TABLE IF EXISTS items;")
        .await
        .unwrap();
    client
        .batch_execute("DROP TABLE IF EXISTS items;")
        .await
        .unwrap();
}
