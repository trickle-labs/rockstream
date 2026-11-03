//! Pgwire reachability and negative tests for v0.55.1 secret DDL.

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_types::ids::{PrincipalId, SinkId, SourceId};
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
    let (port, _server) = start_gateway(Arc::clone(&catalog)).await;
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
    let (port, _server) = start_gateway(Arc::clone(&catalog)).await;
    let client = connect(port).await;

    let error = client
        .simple_query("CREATE SECRET malformed")
        .await
        .unwrap_err();
    assert!(db_error_message(error).contains("RS-2424"));

    let error = client
        .simple_query(
            "CREATE SOURCE unsupported_secret TYPE kafka (bootstrap_servers = 'localhost:9092', secret = 'vault://orders', topic = 'orders') FORMAT json",
        )
        .await
        .unwrap_err();
    assert_eq!(
        db_error_message(error),
        "[RS-4008] CREATE SOURCE secret must be a name or secret:// reference. Next steps: use the documented secret = '<name>' option."
    );

    let error = client
        .simple_query(
            "CREATE SOURCE missing_secret TYPE kafka (bootstrap_servers = 'localhost:9092', secret = 'does_not_exist', topic = 'orders') FORMAT json",
        )
        .await
        .unwrap_err();
    assert_eq!(
        db_error_message(error),
        "[RS-2401] auth.permission_denied: secret reference use is not authorized. Next steps: request an authorized scoped secret-use grant."
    );

    client
        .simple_query("CREATE SECRET in_use (TYPE = 'sasl_plain', username = 'u', password = 'p')")
        .await
        .unwrap();
    let error = client
        .simple_query(
            "CREATE SOURCE kafka_src TYPE kafka (bootstrap_servers = 'localhost:9092', secret = 'in_use', topic = 'orders') FORMAT json",
        )
        .await
        .unwrap_err();
    assert_eq!(
        db_error_message(error),
        "[RS-2401] auth.permission_denied: secret reference use is not authorized. Next steps: request an authorized scoped secret-use grant."
    );
    assert!(catalog.get_source("kafka_src").is_none());
    client.simple_query("DROP SECRET in_use").await.unwrap();
}

#[tokio::test]
async fn legacy_source_and_sink_secret_names_migrate_before_gateway_use() {
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "secret-reference-migration",
    ));
    let mut source_options = HashMap::new();
    source_options.insert("secret".to_string(), "legacy_pg_password".to_string());
    let mut sink_options = HashMap::new();
    sink_options.insert("secret".to_string(), "legacy_sink_password".to_string());
    durable
        .commit_txn(
            CatalogTxn::new(
                1,
                1,
                vec![
                    CatalogMutation::PutSource(rockstream_storage::catalog::CatalogSourceEntry {
                        id: SourceId(11),
                        name: "legacy_pg_source".to_string(),
                        connector_type: "postgres_cdc".to_string(),
                        table_name: Some("orders".to_string()),
                        options: source_options,
                        secret_consumer: Some(PrincipalId(21)),
                        secret_namespace: Some("sales".to_string()),
                    }),
                    CatalogMutation::PutSink(rockstream_storage::catalog::CatalogSinkEntry {
                        id: SinkId(12),
                        name: "legacy_sink".to_string(),
                        sink_type: "kafka".to_string(),
                        target: "orders".to_string(),
                        options: sink_options,
                        status: "ACTIVE".to_string(),
                        secret_reference: None,
                        secret_consumer: Some(PrincipalId(22)),
                        secret_namespace: Some("sales".to_string()),
                    }),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let catalog = CatalogStubs::new();
    catalog.set_durable_store(durable.clone());
    catalog.sync_from_durable_store().await.unwrap();

    assert_eq!(
        catalog
            .get_source("legacy_pg_source")
            .unwrap()
            .options
            .get("secret")
            .map(String::as_str),
        Some("secret://legacy_pg_password")
    );
    let sink = catalog.get_sink("legacy_sink").unwrap();
    assert_eq!(
        sink.secret_name.as_deref(),
        Some("secret://legacy_sink_password")
    );
    assert_eq!(sink.secret_consumer, Some(PrincipalId(22)));
    assert_eq!(sink.secret_namespace.as_deref(), Some("sales"));

    assert_eq!(durable.get_revision().await, 2);
    assert_eq!(
        durable
            .get_source(SourceId(11))
            .await
            .unwrap()
            .unwrap()
            .options["secret"],
        "secret://legacy_pg_password"
    );
    let sink = durable.get_sink(SinkId(12)).await.unwrap().unwrap();
    assert_eq!(
        sink.secret_reference.as_deref(),
        Some("secret://legacy_sink_password")
    );
    assert_eq!(sink.options["secret"], "secret://legacy_sink_password");
}
