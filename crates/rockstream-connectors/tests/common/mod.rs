// Shared test helper module included via `mod common;` by several separate
// test binaries (LFS, MinIO, TC variants). Each binary only uses a subset of
// these helpers, so per-binary dead-code lints are false positives here.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio_postgres::{Client, NoTls};

pub const RNG_SEED: u64 = 0x4400_0044;

pub struct ConnectorFixture {
    pub _postgres: ContainerAsync<GenericImage>,
    pub _kafka: ContainerAsync<apache::Kafka>,
    pub postgres: Client,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub kafka_bootstrap: String,
}

pub async fn connector_fixture(label: &str) -> ConnectorFixture {
    assert!(
        docker_available(),
        "Docker is required for connector guarantees"
    );
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let postgres_host = postgres.get_host().await.unwrap().to_string();
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!(
        "host={postgres_host} port={postgres_port} user=postgres password=postgres dbname=postgres"
    );
    let (postgres_client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    postgres_client
        .batch_execute(&format!(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY); ALTER TABLE orders REPLICA IDENTITY FULL; CREATE PUBLICATION orders_pub FOR TABLE orders; CREATE TABLE health_{label} (id BIGINT);"
        ))
        .await
        .unwrap();
    postgres_client.query_one("SELECT 1", &[]).await.unwrap();

    let kafka = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .unwrap();
    let kafka_bootstrap = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let mut metadata_res = Err(rdkafka::error::KafkaError::ClientCreation("initial".into()));
    for _ in 0..30 {
        if let Ok(health) = ClientConfig::new()
            .set("bootstrap.servers", &kafka_bootstrap)
            .create::<BaseConsumer>()
        {
            if health.fetch_metadata(None, Duration::from_secs(2)).is_ok() {
                metadata_res = Ok(());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    metadata_res.expect("kafka broker must become reachable");

    ConnectorFixture {
        _postgres: postgres,
        _kafka: kafka,
        postgres: postgres_client,
        postgres_host,
        postgres_port,
        kafka_bootstrap,
    }
}

pub fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

pub fn make_cumulative_batch(last_id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from((1..=last_id).collect::<Vec<_>>()));
    let names: ArrayRef = Arc::new(StringArray::from(
        (1..=last_id)
            .map(|id| format!("row-{id}"))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![ids, names]).unwrap()
}

pub fn render_batches(batches: &[RecordBatch]) -> String {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row_idx in 0..batch.num_rows() {
            rows.push(format!("{}|{}", ids.value(row_idx), names.value(row_idx)));
        }
    }
    rows.join("\n")
}

pub use rockstream_test_support::minio::{
    build_minio_store, create_minio_bucket, minio_object_store, start_minio, MinIO2024, MINIO_PASS,
    MINIO_USER,
};
