mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use rockstream_connectors::{
    KafkaDlqDiagnostic, OffsetToken, SourceCheckpoint, SourceCheckpointStore,
};
use rockstream_storage::{
    keys::{ShardKeyEncoder, ShardPrefix},
    ShardDb, WriteBatch,
};
use rockstream_types::ids::ConnectorId;

const BUCKET: &str = "kafka-source-v070";

#[tokio::test]
async fn test_kafka_partition_offsets_survive_process_restart_minio() {
    if !common::docker_available() {
        eprintln!(
            "SKIP test_kafka_partition_offsets_survive_process_restart_minio: Docker not available"
        );
        return;
    }
    let (_container, port) = match common::start_minio(BUCKET).await {
        Some(res) => res,
        None => {
            eprintln!("SKIP test_kafka_partition_offsets_survive_process_restart_minio: MinIO container unavailable");
            return;
        }
    };

    let connector_id = ConnectorId(70201);
    let store_backend = common::build_minio_store(port, BUCKET);

    // Initial run: persist multi-partition offsets into ShardDb on MinIO
    {
        let db = Arc::new(
            ShardDb::builder("kafka-durability-minio", store_backend.clone())
                .build()
                .await
                .unwrap(),
        );

        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
        let offsets = BTreeMap::from([(0u64, 100u64), (1, 250), (2, 500)]);
        let token_bytes = serde_json::to_vec(&offsets).unwrap();
        let token = OffsetToken::new(token_bytes);

        let checkpoint = SourceCheckpoint::prepared(connector_id, 1, token.clone());
        store.prepare(&checkpoint).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"kafka_state_minio");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output_minio");
        batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        db.flush().await.unwrap();
        drop(store);
        drop(db);
    }

    // Process restart: reopen ShardDb from MinIO S3 store
    {
        let reopened = Arc::new(
            ShardDb::builder("kafka-durability-minio", store_backend)
                .build()
                .await
                .unwrap(),
        );

        let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 0, connector_id);
        let highest = recovered.highest_committed().await.unwrap();
        assert!(highest.is_some());
        let committed = highest.unwrap();
        assert_eq!(committed.source_epoch, 1);

        let recovered_offsets: BTreeMap<u64, u64> =
            serde_json::from_slice(committed.token.as_bytes()).unwrap();
        assert_eq!(recovered_offsets.get(&0), Some(&100));
        assert_eq!(recovered_offsets.get(&1), Some(&250));
        assert_eq!(recovered_offsets.get(&2), Some(&500));
    }
}

#[tokio::test]
async fn test_kafka_dlq_entries_survive_restart_minio() {
    if !common::docker_available() {
        eprintln!("SKIP test_kafka_dlq_entries_survive_restart_minio: Docker not available");
        return;
    }
    let (_container, port) = match common::start_minio(BUCKET).await {
        Some(res) => res,
        None => {
            eprintln!(
                "SKIP test_kafka_dlq_entries_survive_restart_minio: MinIO container unavailable"
            );
            return;
        }
    };

    let connector_id = ConnectorId(70202);
    let store_backend = common::build_minio_store(port, BUCKET);

    let malformed_payload = b"{\"secret_token\": \"minio_secret_token_12345\"}";
    let diag = KafkaDlqDiagnostic::new(
        "events_topic_minio",
        2,
        84,
        "RS-1003",
        "events_schema_minio",
        malformed_payload,
    );

    // Initial run: persist DLQ entry in ShardDb on MinIO
    {
        let db = Arc::new(
            ShardDb::builder("kafka-dlq-minio", store_backend.clone())
                .build()
                .await
                .unwrap(),
        );

        let mut batch = WriteBatch::new();
        let dlq_key = format!("dlq/{}/p2/84", connector_id.0);
        let diag_bytes = serde_json::to_vec(&diag).unwrap();
        batch.put(dlq_key.as_bytes(), &diag_bytes);
        db.write_batch(batch).await.unwrap();
        db.flush().await.unwrap();
        drop(db);
    }

    // Process restart: verify DLQ entry survives bit-identically on MinIO
    {
        let reopened = Arc::new(
            ShardDb::builder("kafka-dlq-minio", store_backend)
                .build()
                .await
                .unwrap(),
        );

        let dlq_key = format!("dlq/{}/p2/84", connector_id.0);
        let raw = reopened.get(dlq_key.as_bytes()).await.unwrap();
        assert!(raw.is_some());
        let recovered_diag: KafkaDlqDiagnostic = serde_json::from_slice(&raw.unwrap()).unwrap();

        assert_eq!(recovered_diag.topic, "events_topic_minio");
        assert_eq!(recovered_diag.partition, 2);
        assert_eq!(recovered_diag.offset, 84);
        assert_eq!(recovered_diag.error, "RS-1003");
        assert_eq!(recovered_diag.schema, "events_schema_minio");
        assert_eq!(recovered_diag.payload_digest, diag.payload_digest);
        assert!(
            recovered_diag
                .redacted_payload
                .as_ref()
                .unwrap()
                .contains("[REDACTED]"),
            "Secret token must be safely redacted in persistent DLQ record on MinIO"
        );
    }
}

#[tokio::test]
async fn test_kafka_epoch_commit_barrier_minio() {
    if !common::docker_available() {
        eprintln!("SKIP test_kafka_epoch_commit_barrier_minio: Docker not available");
        return;
    }
    let (_container, port) = match common::start_minio(BUCKET).await {
        Some(res) => res,
        None => {
            eprintln!("SKIP test_kafka_epoch_commit_barrier_minio: MinIO container unavailable");
            return;
        }
    };

    let connector_id = ConnectorId(70203);
    let store_backend = common::build_minio_store(port, BUCKET);

    // Initial run: commit epoch 25 to MinIO
    {
        let db = Arc::new(
            ShardDb::builder("kafka-epoch-barrier-minio", store_backend.clone())
                .build()
                .await
                .unwrap(),
        );

        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
        let offsets = BTreeMap::from([(0u64, 750u64), (1, 850)]);
        let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());

        let checkpoint = SourceCheckpoint::prepared(connector_id, 25, token);
        store.prepare(&checkpoint).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_epoch25");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output_epoch25");
        batch.put(&ShardKeyEncoder::frontier_key(), &25_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        db.flush().await.unwrap();
        drop(store);
        drop(db);
    }

    // Reopen and assert epoch barrier 25 is preserved exactly
    {
        let reopened = Arc::new(
            ShardDb::builder("kafka-epoch-barrier-minio", store_backend)
                .build()
                .await
                .unwrap(),
        );

        let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 0, connector_id);
        let highest = recovered.highest_committed().await.unwrap().unwrap();
        assert_eq!(highest.source_epoch, 25);

        let frontier_bytes = reopened
            .get(&ShardKeyEncoder::frontier_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(frontier_bytes.as_ref().try_into().unwrap()),
            25
        );
    }
}

#[tokio::test]
async fn test_kafka_source_durability_on_object_store_minio() {
    if !common::docker_available() {
        eprintln!("SKIP test_kafka_source_durability_on_object_store_minio: Docker not available");
        return;
    }
    let (_container, port) = match common::start_minio(BUCKET).await {
        Some(res) => res,
        None => {
            eprintln!("SKIP test_kafka_source_durability_on_object_store_minio: MinIO container unavailable");
            return;
        }
    };

    let connector_id = ConnectorId(70204);
    let store_backend = common::build_minio_store(port, BUCKET);

    let db = Arc::new(
        ShardDb::builder("kafka-object-store-minio", store_backend.clone())
            .build()
            .await
            .unwrap(),
    );

    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let offsets = BTreeMap::from([(0u64, 1_000u64), (1, 2_000), (2, 3_000)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());

    let checkpoint = SourceCheckpoint::prepared(connector_id, 99, token);
    store.prepare(&checkpoint).await.unwrap();

    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_s3");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output_s3");
    batch.put(&ShardKeyEncoder::frontier_key(), &99_u64.to_be_bytes());
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store.commit_m3(batch).await.unwrap();
    db.flush().await.unwrap();
    drop(store);
    drop(db);

    let reopened = Arc::new(
        ShardDb::builder("kafka-object-store-minio", store_backend)
            .build()
            .await
            .unwrap(),
    );
    let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 0, connector_id);
    let highest = recovered.highest_committed().await.unwrap().unwrap();
    assert_eq!(highest.source_epoch, 99);
}
