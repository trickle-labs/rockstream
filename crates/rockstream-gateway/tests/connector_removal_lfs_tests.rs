use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogSinkEntry, CatalogSourceEntry, CatalogStubs, V0522ConnectorCatalog},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

struct NoopViewReader;
#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _: &str,
        _: Option<usize>,
        _: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

const REMEDIATION: &str = "[RS-4017] connector.removed: Use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output.";

#[tokio::test]
async fn v0522_removed_connector_catalog_loads_as_removed_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("connector-removal", store)
            .build()
            .await
            .unwrap(),
    );
    CatalogStubs::seed_v0522_connector_catalog(
        &shard_db,
        &V0522ConnectorCatalog {
            sinks: vec![
                CatalogSinkEntry {
                    name: "iceberg_sink".into(),
                    view: "orders".into(),
                    format: "ICEBERG".into(),
                    path: "s3://bucket/orders".into(),
                    snapshot_interval_epochs: None,
                    snapshot_interval_ms: None,
                    parquet_row_group_bytes: None,
                    format_version: None,
                    partition_by: vec![],
                    catalog: "glue".into(),
                    last_snapshot_epoch: None,
                    state: "OK".into(),
                },
                CatalogSinkEntry {
                    name: "object_store_sink".into(),
                    view: "orders".into(),
                    format: "PARQUET".into(),
                    path: "s3://bucket/archive".into(),
                    snapshot_interval_epochs: None,
                    snapshot_interval_ms: None,
                    parquet_row_group_bytes: None,
                    format_version: None,
                    partition_by: vec![],
                    catalog: "filesystem".into(),
                    last_snapshot_epoch: None,
                    state: "OK".into(),
                },
            ],
            sources: vec![
                CatalogSourceEntry {
                    name: "s3_source".into(),
                    table_name: None,
                    source_type: "s3".into(),
                    options: Default::default(),
                    format: "json".into(),
                    status: "OK".into(),
                    live_offset: "0".into(),
                    live_lag: 0,
                },
                CatalogSourceEntry {
                    name: "webhook_source".into(),
                    table_name: None,
                    source_type: "http_webhook".into(),
                    options: Default::default(),
                    format: "json".into(),
                    status: "OK".into(),
                    live_offset: "0".into(),
                    live_lag: 0,
                },
            ],
        },
    )
    .await
    .unwrap();
    shard_db.flush().await.unwrap();
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (addr, _server) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sources = client.simple_query("SHOW SOURCES").await.unwrap();
    let source_rows: Vec<_> = sources
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0), row.get(1), row.get(2), row.get(3), row.get(6)))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        source_rows,
        vec![
            (
                Some("s3_source"),
                Some("s3"),
                Some("json"),
                Some("REMOVED"),
                Some(REMEDIATION)
            ),
            (
                Some("webhook_source"),
                Some("http_webhook"),
                Some("json"),
                Some("REMOVED"),
                Some(REMEDIATION)
            )
        ]
    );
    let sinks = client.simple_query("SHOW SINKS").await.unwrap();
    let sink_rows: Vec<_> = sinks
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        sink_rows,
        vec![
            (
                Some("iceberg_sink"),
                Some("ICEBERG"),
                Some("s3://bucket/orders"),
                Some("glue"),
                Some("REMOVED"),
                Some(REMEDIATION)
            ),
            (
                Some("object_store_sink"),
                Some("PARQUET"),
                Some("s3://bucket/archive"),
                Some("filesystem"),
                Some("REMOVED"),
                Some(REMEDIATION)
            )
        ]
    );
}
