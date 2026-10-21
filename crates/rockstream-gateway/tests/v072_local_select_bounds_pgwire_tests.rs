//! PgWire regression: ordinary SELECT uses bounded storage producer and
//! does not turn a storage fault into an apparently valid empty answer.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct NoopReader;

#[async_trait::async_trait]
impl ViewReader for NoopReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn local_select_surfaces_scan_failures_and_recovers_without_partial_rows() {
    let storage = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-bounded-select-normal-client", storage)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopReader),
        Arc::clone(&shard),
    );
    let (addr, task) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query("CREATE TABLE v072_bounded_select (id BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO v072_bounded_select (id) VALUES (1), (2)")
        .await
        .unwrap();
    let sql = "SELECT * FROM v072_bounded_select ORDER BY id";
    let rows = client.simple_query(sql).await.unwrap();
    let values: Vec<_> = rows
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["1".to_string(), "2".to_string()]);

    shard.set_fail_scans(true);
    let denied = client.simple_query(sql).await;
    assert!(
        denied.is_err(),
        "a failed scan must not return partial/empty success"
    );

    shard.set_fail_scans(false);
    let recovered = client.simple_query(sql).await.unwrap();
    let recovered_values: Vec<_> = recovered
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(recovered_values, vec!["1".to_string(), "2".to_string()]);

    connection_task.abort();
    task.abort();
}
