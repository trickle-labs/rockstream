use rockstream_connectors::{PgOutputConfig, PostgresCdcSource};
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_types::secret::SecretType;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn alter_secret_resolution_updates_pgoutput_credential_version() {
    let store = SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("rotation-test-kek")),
    );
    store
        .create_secret(
            0,
            "pg",
            SecretType::PostgresPassword,
            HashMap::from([(String::from("password"), String::from("before"))]),
            "test",
        )
        .await
        .unwrap();
    store
        .alter_secret(
            0,
            "pg",
            HashMap::from([(String::from("password"), String::from("after"))]),
            "test",
        )
        .await
        .unwrap();
    let secret = store.get_secret(0, "pg").await.unwrap();
    assert_eq!(secret.metadata.version, 2);
    assert_eq!(
        secret.payload.get("password").map(String::as_str),
        Some("after")
    );
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source = PostgresCdcSource::configured_pgoutput(
        rockstream_types::ids::ConnectorId(1),
        schema,
        PgOutputConfig {
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: "app".to_string(),
            user: "app".to_string(),
            password: Some("before".to_string()),
            slot: "source_slot".to_string(),
            publication: "source_pub".to_string(),
            table: "public.orders".to_string(),
        },
    )
    .unwrap();
    source.bind_secret_version("pg", 1);
    let rotated = source
        .rotate_pgoutput_secret_at_epoch(
            "pg",
            secret.metadata.version,
            secret.payload.get("password").cloned().unwrap_or_default(),
            false,
        )
        .await
        .unwrap();
    assert!(rotated);
    assert_eq!(source.secret_binding(), (Some("pg"), 2));
    assert_eq!(source.pipeline_restarts(), 0);
    assert_eq!(source.failed_batches(), 0);
}
