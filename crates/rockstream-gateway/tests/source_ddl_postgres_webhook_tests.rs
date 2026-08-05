//! Pgwire reachability and negative tests for the v0.51.14 source DDL surface.

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
