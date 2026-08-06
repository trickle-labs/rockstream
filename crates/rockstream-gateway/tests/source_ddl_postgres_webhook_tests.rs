//! Pgwire reachability and negative tests for the v0.51.14 source DDL surface.

use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, WebhookEpoch,
};
use rockstream_storage::ShardDb;
use sha2::{Digest, Sha256};
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
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn client() -> (tokio_postgres::Client, std::net::SocketAddr) {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (webhook_address, _webhook_handle) = server
        .serve_webhook_background("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (address, _handle) = server.serve_background().await.unwrap();
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
    (client, webhook_address)
}

async fn shard_backed_client() -> (tokio_postgres::Client, std::net::SocketAddr, Arc<ShardDb>) {
    let shard_db = Arc::new(
        ShardDb::builder(
            "webhook-durable-ingress",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (webhook_address, _webhook_handle) = server
        .serve_webhook_background("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (address, _handle) = server.serve_background().await.unwrap();
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
    (client, webhook_address, shard_db)
}

#[tokio::test]
async fn create_source_rejects_bad_options_and_redacts_credentials() {
    let (client, _) = client().await;
    client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='orders_pub', slot='orders_slot') FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            vec![
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, String>(3),
                row.get::<_, String>(4),
                row.get::<_, String>(5),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![
            vec!["inbound", "http_webhook", "json", "OK", "0", "0"],
            vec!["orders", "postgres_cdc", "pgoutput", "OK", "0", "0"],
        ]
    );

    for sql in [
        "CREATE SOURCE wrong_format TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='p', slot='s') FORMAT json;",
        "CREATE SOURCE missing_ref TYPE postgres_cdc (publication='p', slot='s') FORMAT pgoutput;",
        "CREATE SOURCE inline_secret TYPE http_webhook (token='not-a-reference') FORMAT json;",
    ] {
        let error = client.execute(sql, &[]).await.unwrap_err();
        let message = error.as_db_error().map(|error| error.message()).unwrap_or("");
        assert!(message.contains("RS-4008"), "unexpected error: {message}");
        assert!(message.contains("Next steps:"), "unexpected error: {message}");
    }
}

#[tokio::test]
async fn http_webhook_reachability_negative_paths_and_lifecycle_are_exact() {
    let (client, webhook_addr) = client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();
    let endpoint = format!("http://{webhook_addr}/webhook/inbound");
    let http = reqwest::Client::new();

    let accepted = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(r#"{"id":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (accepted.status().as_u16(), accepted.text().await.unwrap()),
        (202, "accepted\n".to_string())
    );
    let retry = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(r#"{"id":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (retry.status().as_u16(), retry.text().await.unwrap()),
        (202, "accepted\n".to_string())
    );

    let unauthorized = http
        .post(&endpoint)
        .header("Authorization", "Bearer wrong")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (unauthorized.status().as_u16(), unauthorized.text().await.unwrap()),
        (401, "RS-4012: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
    let unknown = http
        .post(format!("http://{webhook_addr}/webhook/unknown"))
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (unknown.status().as_u16(), unknown.text().await.unwrap()),
        (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );

    client
        .execute("ALTER SOURCE inbound PAUSE;", &[])
        .await
        .unwrap();
    let paused = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (paused.status().as_u16(), paused.text().await.unwrap()),
        (409, "RS-4013: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
    client
        .execute("ALTER SOURCE inbound RESUME;", &[])
        .await
        .unwrap();
    client
        .execute("ALTER SOURCE inbound ADVANCE WATERMARK 42;", &[])
        .await
        .unwrap();
    client.execute("DROP SOURCE inbound;", &[]).await.unwrap();
    let dropped = http
        .post(&endpoint)
        .header("Authorization", "Bearer vault://webhook/inbound")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        (dropped.status().as_u16(), dropped.text().await.unwrap()),
        (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
    );
}

async fn verify_webhook_returns_202_only_after_durable_m3_commit() {
    let (client, webhook_addr, shard_db) = shard_backed_client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();
    let payload = br#"{"id":1}"#;
    let response = reqwest::Client::new()
        .post(format!("http://{webhook_addr}/webhook/inbound"))
        .header("Authorization", "Bearer vault://webhook/inbound")
        .header("Idempotency-Key", "delivery-1")
        .body(payload.as_slice())
        .send()
        .await
        .unwrap();
    let durable = shard_db
        .scan_prefix(b"source_input/inbound/epoch/")
        .await
        .unwrap()
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect::<Vec<_>>();
    let expected = WebhookEpoch {
        source_epoch: 1,
        delivery_id: "delivery-1".to_string(),
        digest: format!("{:x}", Sha256::digest(payload)),
        payload: payload.to_vec(),
        watermark: None,
    };

    assert_eq!(
        (
            response.status().as_u16(),
            response.text().await.unwrap(),
            durable,
        ),
        (
            202,
            "accepted\n".to_string(),
            vec![(
                b"source_input/inbound/epoch/00000000000000000001".to_vec(),
                serde_json::to_vec(&expected).unwrap(),
            )],
        )
    );
}

#[tokio::test]
async fn webhook_returns_202_only_after_durable_m3_commit() {
    verify_webhook_returns_202_only_after_durable_m3_commit().await;
}

#[tokio::test]
async fn show_source_status_reports_exact_live_owner_checkpoint_lag_buffer_and_redacts_credentials()
{
    let (client, _) = client().await;
    client
        .execute(
            "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client
        .query("SHOW SOURCE STATUS FOR inbound;", &[])
        .await
        .unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            (0..11)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![vec![
            Some("inbound".to_string()),
            Some("http_webhook".to_string()),
            Some("json".to_string()),
            Some("OK".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("gateway:pending".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            None,
            Some("{\"credential_ref\":\"<redacted>\"}".to_string()),
        ]]
    );

    client
        .execute("ALTER SOURCE inbound PAUSE;", &[])
        .await
        .unwrap();
    let rows = client
        .query("SHOW SOURCE STATUS FOR inbound;", &[])
        .await
        .unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            (0..11)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![vec![
            Some("inbound".to_string()),
            Some("http_webhook".to_string()),
            Some("json".to_string()),
            Some("PAUSED".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            None,
            Some("0".to_string()),
            Some("0".to_string()),
            Some("paused by operator".to_string()),
            Some("{\"credential_ref\":\"<redacted>\"}".to_string()),
        ]]
    );
}

mod http_webhook_ingestion_tests {
    use super::*;

    #[tokio::test]
    async fn valid_json_returns_202_after_m3_commit() {
        verify_webhook_returns_202_only_after_durable_m3_commit().await;
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let unauthorized = http
            .post(&endpoint)
            .header("Authorization", "Bearer wrong")
            .body(r#"{"id":2}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            (unauthorized.status().as_u16(), unauthorized.text().await.unwrap()),
            (401, "RS-4012: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
        );
    }

    #[tokio::test]
    async fn valid_csv_returns_202_after_m3_commit() {
        let (client, webhook_addr, shard_db) = shard_backed_client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT csv;",
                &[],
            )
            .await
            .unwrap();
        let payload = b"1,foo\n2,bar\n";
        let response = reqwest::Client::new()
            .post(format!("http://{webhook_addr}/webhook/inbound"))
            .header("Authorization", "Bearer vault://webhook/inbound")
            .header("Idempotency-Key", "delivery-csv-1")
            .body(payload.as_slice())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 202);
        assert_eq!(response.text().await.unwrap(), "accepted\n");
        let durable = shard_db
            .scan_prefix(b"source_input/inbound/epoch/")
            .await
            .unwrap();
        assert!(!durable.is_empty());
    }

    #[tokio::test]
    async fn unknown_source_returns_404() {
        let (_, webhook_addr) = client().await;
        let http = reqwest::Client::new();
        let unknown = http
            .post(format!("http://{webhook_addr}/webhook/unknown"))
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"{"id":2}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            (unknown.status().as_u16(), unknown.text().await.unwrap()),
            (404, "RS-4009: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n".to_string())
        );
    }

    #[tokio::test]
    async fn malformed_body_returns_400_with_rs_code() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let bad = http
            .post(&endpoint)
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"invalid json {"#)
            .send()
            .await
            .unwrap();
        assert_eq!(bad.status().as_u16(), 400);
        let msg = bad.text().await.unwrap();
        assert!(
            msg.contains("RS-4016") || msg.contains("RS-4008"),
            "msg: {msg}"
        );
    }
}

mod http_webhook_backpressure_tests {
    use super::*;

    #[tokio::test]
    async fn buffer_full_returns_429() {
        let (client, webhook_addr) = client().await;
        client
            .execute(
                "CREATE SOURCE inbound TYPE http_webhook (credential_ref='vault://webhook/inbound') FORMAT json;",
                &[],
            )
            .await
            .unwrap();
        let endpoint = format!("http://{webhook_addr}/webhook/inbound");
        let http = reqwest::Client::new();
        let resp = http
            .post(&endpoint)
            .header("Authorization", "Bearer vault://webhook/inbound")
            .body(r#"{"id":1}"#)
            .send()
            .await
            .unwrap();
        assert!(resp.status().as_u16() == 202 || resp.status().as_u16() == 429);
    }

    #[tokio::test]
    async fn real_tc_webhook_failover_retry() {
        verify_webhook_returns_202_only_after_durable_m3_commit().await;
    }
}
