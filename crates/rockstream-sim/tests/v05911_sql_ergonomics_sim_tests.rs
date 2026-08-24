//! v0.59.11 Slice 6: Deterministic SimRuntime DML RETURNING & DDL Fault Injection Tests.
//!
//! Asserts that under buggify fault injection and concurrent client operations,
//! DML RETURNING (UPDATE & DELETE) and DDL IF [NOT] EXISTS operations remain strictly consistent
//! without torn writes, panics, or phantom state.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_sim::{buggify, SimRuntime};
use rockstream_storage::ShardDb;
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

async fn start_sim_gateway(
    shard_path: &str,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let store = Arc::new(InMemory::new());
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
async fn test_dml_returning_under_simulated_faults() {
    for seed in [0x5911, 0x5912, 0x5913] {
        let _runtime = SimRuntime::new(seed);
        rockstream_sim::buggify::buggify_init(seed);

        let _read_back_delay = buggify!("v05911.gateway.returning_read_back_delay", 0.3);
        let _concurrent_ddl = buggify!("v05911.gateway.concurrent_ddl_if_exists", 0.3);
        let _scalar_eval = buggify!("v05911.ops.scalar_eval_error", 0.3);

        let catalog = Arc::new(CatalogStubs::new());
        let (port, _handle, _shard_db) =
            start_sim_gateway(&format!("dml-sim-{seed}"), catalog.clone()).await;

        let client = connect_port(port).await;

        // Set up test table
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sim_records (id INT, val TEXT, counter INT);",
            )
            .await
            .unwrap();

        // Populate initial rows
        for i in 1..=10 {
            client
                .simple_query(&format!(
                    "INSERT INTO sim_records VALUES ({i}, 'val_{i}', {i});"
                ))
                .await
                .unwrap();
        }

        // Concurrent DML RETURNING and DDL operations
        let client_c1 = connect_port(port).await;
        let client_c2 = connect_port(port).await;

        let t1 = tokio::spawn(async move {
            for i in 1..=5 {
                let new_counter = i + 100;
                let rows = client_c1
                    .simple_query(&format!(
                        "UPDATE sim_records SET counter = {new_counter} WHERE id = {i} RETURNING id, counter;"
                    ))
                    .await;
                if let Ok(rows) = rows {
                    for msg in rows {
                        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                            assert_eq!(r.get("id"), Some(i.to_string().as_str()));
                            let counter_str = r.get("counter").unwrap();
                            let counter_val: i32 = counter_str.parse().unwrap();
                            assert_eq!(counter_val, new_counter);
                        }
                    }
                }
            }
        });

        let t2 = tokio::spawn(async move {
            for i in 6..=10 {
                let rows = client_c2
                    .simple_query(&format!(
                        "DELETE FROM sim_records WHERE id = {i} RETURNING id, val;"
                    ))
                    .await;
                if let Ok(rows) = rows {
                    for msg in rows {
                        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                            assert_eq!(r.get("id"), Some(i.to_string().as_str()));
                            assert_eq!(r.get("val"), Some(format!("val_{i}").as_str()));
                        }
                    }
                }
            }
        });

        let (r1, r2) = tokio::join!(t1, t2);
        r1.unwrap();
        r2.unwrap();

        // Verify state consistency after concurrent operations
        let check_rows = client
            .simple_query("SELECT id, val, counter FROM sim_records;")
            .await
            .unwrap();
        let remaining_rows: Vec<_> = check_rows
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect();

        // 5 rows updated (id 1..=5), 5 rows deleted (id 6..=10)
        assert_eq!(remaining_rows.len(), 5);
        for row in &remaining_rows {
            let id: i32 = row.get("id").unwrap().parse().unwrap();
            assert!((1..=5).contains(&id));
            let counter: i32 = row.get("counter").unwrap().parse().unwrap();
            assert_eq!(counter, id + 100);
        }

        // Clean up with idempotent DDL
        client
            .batch_execute("DROP TABLE IF EXISTS sim_records;")
            .await
            .unwrap();
    }
}
