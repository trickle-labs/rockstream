//! Kafka Source Durability Tests on Local File System (LFS) (§6.1, V070-02, V070-03, V070-06).

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_connectors::{
    KafkaDlqDiagnostic, OffsetToken, SourceCheckpoint, SourceCheckpointStore,
};
use rockstream_storage::{
    keys::{ShardKeyEncoder, ShardPrefix},
    ShardDb, WriteBatch,
};
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;

#[tokio::test]
async fn test_kafka_partition_offsets_survive_process_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(70101);

    // Initial run: persist multi-partition offsets into ShardDb
    {
        let db = Arc::new(
            ShardDb::builder(
                "kafka-durability-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
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
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output");
        batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        db.flush().await.unwrap();

        drop(store);
        let db = match Arc::try_unwrap(db) {
            Ok(db) => db,
            Err(_) => panic!("expected single shard db owner"),
        };
        db.close().await.unwrap();
    }

    // Process restart: reopen ShardDb from LFS
    {
        let reopened = Arc::new(
            ShardDb::builder(
                "kafka-durability-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
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
async fn test_kafka_dlq_entries_survive_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(70102);

    let malformed_payload = b"{\"secret_token\": \"top_secret_value_12345\"}";
    let diag = KafkaDlqDiagnostic::new(
        "events_topic",
        1,
        42,
        "RS-1003",
        "events_schema_v1",
        malformed_payload,
    );

    // Initial run: persist DLQ entry in ShardDb
    {
        let db = Arc::new(
            ShardDb::builder(
                "kafka-dlq-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
            .build()
            .await
            .unwrap(),
        );

        let mut batch = WriteBatch::new();
        let dlq_key = format!("dlq/{}/p1/42", connector_id.0);
        let diag_bytes = serde_json::to_vec(&diag).unwrap();
        batch.put(dlq_key.as_bytes(), &diag_bytes);
        db.write_batch(batch).await.unwrap();
        db.flush().await.unwrap();

        let db = match Arc::try_unwrap(db) {
            Ok(db) => db,
            Err(_) => panic!("expected single shard db owner"),
        };
        db.close().await.unwrap();
    }

    // Process restart: verify DLQ entry survives bit-identically with safe redaction
    {
        let reopened = Arc::new(
            ShardDb::builder(
                "kafka-dlq-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
            .build()
            .await
            .unwrap(),
        );

        let dlq_key = format!("dlq/{}/p1/42", connector_id.0);
        let raw = reopened.get(dlq_key.as_bytes()).await.unwrap();
        assert!(raw.is_some());
        let recovered_diag: KafkaDlqDiagnostic = serde_json::from_slice(&raw.unwrap()).unwrap();

        assert_eq!(recovered_diag.topic, "events_topic");
        assert_eq!(recovered_diag.partition, 1);
        assert_eq!(recovered_diag.offset, 42);
        assert_eq!(recovered_diag.error, "RS-1003");
        assert_eq!(recovered_diag.schema, "events_schema_v1");
        assert_eq!(recovered_diag.payload_digest, diag.payload_digest);
        assert!(
            recovered_diag
                .redacted_payload
                .as_ref()
                .unwrap()
                .contains("[REDACTED]"),
            "Secret token must be safely redacted in persistent DLQ record"
        );
    }
}

#[tokio::test]
async fn test_kafka_epoch_commit_barrier_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(70103);

    // Initial run: commit epoch 15
    {
        let db = Arc::new(
            ShardDb::builder(
                "kafka-epoch-barrier-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
            .build()
            .await
            .unwrap(),
        );

        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
        let offsets = BTreeMap::from([(0u64, 500u64), (1, 600)]);
        let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());

        let checkpoint = SourceCheckpoint::prepared(connector_id, 15, token);
        store.prepare(&checkpoint).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"output");
        batch.put(&ShardKeyEncoder::frontier_key(), &15_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        db.flush().await.unwrap();

        drop(store);
        let db = match Arc::try_unwrap(db) {
            Ok(db) => db,
            Err(_) => panic!("expected single shard db owner"),
        };
        db.close().await.unwrap();
    }

    // Reopen and assert epoch barrier is preserved exactly
    {
        let reopened = Arc::new(
            ShardDb::builder(
                "kafka-epoch-barrier-lfs",
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            )
            .build()
            .await
            .unwrap(),
        );

        let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 0, connector_id);
        let highest = recovered.highest_committed().await.unwrap().unwrap();
        assert_eq!(highest.source_epoch, 15);

        let frontier_bytes = reopened
            .get(&ShardKeyEncoder::frontier_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(frontier_bytes.as_ref().try_into().unwrap()),
            15
        );
    }
}

#[test]
fn test_source_cleanup_uses_no_range_delete_lfs() {
    let source_code = include_str!("../src/kafka_source.rs");
    let epoch_code = include_str!("../src/source_epoch.rs");
    assert!(
        !source_code.contains("delete_range"),
        "kafka_source.rs must not use SlateDB delete_range"
    );
    assert!(
        !epoch_code.contains("delete_range"),
        "source_epoch.rs must not use SlateDB delete_range"
    );
}
