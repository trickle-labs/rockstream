use rockstream_connectors::PostgresCdcSource;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_types::secret::SecretType;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn alter_secret_notifies_and_connector_applies_next_epoch_token() {
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
    let mut rotation_rx = store.subscribe_rotation();
    store
        .alter_secret(
            0,
            "pg",
            HashMap::from([(String::from("password"), String::from("after"))]),
            "test",
        )
        .await
        .unwrap();
    rotation_rx.changed().await.unwrap();
    assert_eq!(rotation_rx.borrow().as_ref().unwrap().secret_name, "pg");
    assert_eq!(rotation_rx.borrow().as_ref().unwrap().version, 2);

    let token = store
        .issue_worker_token(0, "pg", "worker-1", 300, "test")
        .await
        .unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source = PostgresCdcSource::new(
        rockstream_types::ids::ConnectorId(1),
        schema,
        rockstream_connectors::CdcWireFormat::Wal2Json,
    );
    source.bind_secret("pg");
    source.set_secret_token(token.clone()).unwrap();
    source.apply_secret_token_at_epoch();
    assert_eq!(
        source.active_secret_token_id(),
        Some(token.token_id.as_str())
    );
    assert_eq!(source.secret_rotations_applied(), 1);
    assert_eq!(source.pipeline_restarts(), 0);
    assert_eq!(source.failed_batches(), 0);
}
