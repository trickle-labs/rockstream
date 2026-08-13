use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
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

const REMOVED: &str = "[RS-4017] connector.removed: This connector has been removed. Next steps: use an external loader through pgwire or Kafka for S3 input, an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter for webhooks, or RockStream to Kafka to a downstream writer for sink output.";

async fn client() -> tokio_postgres::Client {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
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
    client
}

async fn assert_removed_over_pgwire(sql: &str) {
    let client = client().await;
    assert_eq!(
        client
            .simple_query(sql)
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .message(),
        REMOVED
    );
    assert_eq!(
        client
            .execute(sql, &[])
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .message(),
        REMOVED
    );
}

#[tokio::test]
async fn iceberg_sink_ddl_options_fail_closed_over_pgwire() {
    assert_removed_over_pgwire("CREATE SINK sink FOR VIEW view TO ICEBERG 's3://bucket/path' WITH (snapshot_interval_epochs=1, snapshot_interval_ms=2, parquet_row_group_bytes=3, format_version=2, partition_by=ARRAY['region'], catalog='glue')").await;
}

#[tokio::test]
async fn delta_sink_ddl_options_fail_closed_over_pgwire() {
    assert_removed_over_pgwire("CREATE SINK sink FOR VIEW view TO DELTA 's3://bucket/path' WITH (snapshot_interval_epochs=1, snapshot_interval_ms=2, parquet_row_group_bytes=3, format_version=2, partition_by=ARRAY['region'], catalog='ducklake')").await;
}

#[tokio::test]
async fn object_store_sink_ddl_catalog_variants_fail_closed_over_pgwire() {
    for catalog in ["filesystem", "glue", "hive", "rest", "ducklake"] {
        assert_removed_over_pgwire(&format!("CREATE SINK sink FOR VIEW view TO PARQUET 's3://bucket/path' WITH (catalog='{catalog}')")).await;
    }
}

#[tokio::test]
async fn s3_source_ddl_fails_closed_over_pgwire() {
    assert_removed_over_pgwire("CREATE SOURCE source TYPE s3 (bucket='bucket') FORMAT json").await;
}

#[tokio::test]
async fn webhook_source_ddl_fails_closed_over_pgwire() {
    assert_removed_over_pgwire(
        "CREATE SOURCE source TYPE http_webhook (credential_ref='vault://source') FORMAT json",
    )
    .await;
}

#[tokio::test]
async fn webhook_endpoint_returns_rs4017_and_accepts_no_delivery() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (addr, _server) = server
        .serve_webhook_background("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/webhook/removed"))
        .body("delivery")
        .send()
        .await
        .unwrap();
    assert_eq!(
        (response.status().as_u16(), response.text().await.unwrap()),
        (410, "[RS-4017] connector.removed: HTTP/webhook sources have been removed. Next steps: use an external HTTP-to-Kafka (or HTTP-to-PostgreSQL) adapter outside RockStream.\n".to_string())
    );
}

#[test]
fn connector_removal_docs_have_exact_replacements_and_no_live_references() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let migration = std::fs::read_to_string(root.join("docs/connector-migration.md")).unwrap();
    assert_eq!(
        migration,
        "# Connector migration\n\nThe Iceberg, Delta, object-store, S3, and HTTP webhook connector frontends\nwere removed in v0.52.4. RockStream rejects their DDL and webhook ingress with\n`RS-4017 connector.removed`.\n\n| Removed surface | Replacement |\n| --- | --- |\n| S3 source | Use an external loader through pgwire or Kafka. |\n| HTTP webhook source | Use an external HTTP-to-Kafka or HTTP-to-PostgreSQL adapter. |\n| Iceberg, Delta, and object-store sink | Use RockStream to Kafka and a downstream writer. |\n| Cold-tier configuration | Use RockStream to Kafka and a downstream writer. |\n"
    );
    for path in [
        "README.md",
        "docs/concepts.md",
        "docs/language-features.md",
        "docs/configuration.md",
    ] {
        let content = std::fs::read_to_string(root.join(path)).unwrap();
        assert!(
            !content.contains("CREATE SINK ... TO ICEBERG"),
            "live connector reference in {path}"
        );
        assert!(
            !content.contains("CREATE SINK ... TO DELTA"),
            "live connector reference in {path}"
        );
        assert!(
            !content.contains("TYPE s3"),
            "live connector reference in {path}"
        );
        assert!(
            !content.contains("TYPE http_webhook"),
            "live connector reference in {path}"
        );
    }
}
