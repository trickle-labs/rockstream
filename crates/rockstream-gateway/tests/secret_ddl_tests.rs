//! Pgwire reachability and negative tests for v0.55.1 secret DDL.

use std::sync::Arc;

use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
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
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway(catalog: Arc<CatalogStubs>) -> (u16, tokio::task::JoinHandle<()>) {
    let store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("secret-ddl-test-kek")),
    ));
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
    )
    .with_secret_store(store);
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle)
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn db_error_message(error: tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db_error| db_error.message().to_owned())
        .unwrap_or_else(|| error.to_string())
}

#[tokio::test]
async fn secret_ddl_is_reachable_over_pgwire_and_returns_metadata_only() {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _server) = start_gateway(catalog).await;
    let client = connect(port).await;

    let create_count = client
        .execute(
            "CREATE SECRET kafka_auth (TYPE = 'sasl_plain', username = 'alice', password = 'literal-secret')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(create_count, 0);

    let rows = client.simple_query("SHOW SECRETS").await.unwrap();
    let rows: Vec<Vec<String>> = rows
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or_default().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
    assert_eq!(rows[0][..2], ["kafka_auth", "sasl_plain"]);
    assert!(!rows[0].iter().any(|value| value.contains("literal-secret")));

    client
        .simple_query("ALTER SECRET kafka_auth SET (username = 'bob', password = 'rotated-secret')")
        .await
        .unwrap();
    client.simple_query("DROP SECRET kafka_auth").await.unwrap();
    assert!(client
        .simple_query("SHOW SECRETS")
        .await
        .unwrap()
        .iter()
        .all(|message| !matches!(message, tokio_postgres::SimpleQueryMessage::Row(_))));
}

#[tokio::test]
async fn secret_ddl_negative_paths_return_actionable_codes() {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _server) = start_gateway(catalog).await;
    let client = connect(port).await;

    let error = client
        .simple_query("CREATE SECRET malformed")
        .await
        .unwrap_err();
    assert!(db_error_message(error).contains("RS-2424"));

    let error = client
        .simple_query(
            "CREATE SOURCE missing_secret TYPE kafka (secret = 'does_not_exist', topic = 'orders') FORMAT json",
        )
        .await
        .unwrap_err();
    assert!(db_error_message(error).contains("RS-2420"));

    client
        .simple_query("CREATE SECRET in_use (TYPE = 'sasl_plain', username = 'u', password = 'p')")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE SOURCE kafka_src TYPE kafka (secret = 'in_use', topic = 'orders') FORMAT json",
        )
        .await
        .unwrap();
    let error = client.simple_query("DROP SECRET in_use").await.unwrap_err();
    assert!(db_error_message(error).contains("RS-2426"));
}
