//! v0.67 Section 6.1 MinIO (S3 via TestContainers) Durability Commitments.
//!
//! Verifies:
//! 1. replayed_request_deduplicates_without_duplicate_write_minio
//! 2. kill_pre_ack_recovers_committed_frontier_minio
//! 3. shared_arrangement_restores_from_minio_post_restart

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rockstream_runtime::exchange::persistence::{
    committed_frontier, execute_durable_request, RequestIdentity,
};
use rockstream_storage::arrangement_catalog::ArrangementCatalog;
use rockstream_storage::shard_db::ShardDbBuilder;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::ids::{ArrangementId, TenantId, ViewId};

const MINIO_BUCKET: &str = "rockstream-test-v067-replay";

async fn setup_minio() -> Option<(
    testcontainers::ContainerAsync<rockstream_test_support::minio::MinIO2024>,
    u16,
    Arc<dyn ObjectStore>,
)> {
    let (container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(cp) => cp,
        None => return None,
    };
    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));
    Some((container, port, store))
}

#[tokio::test]
async fn test_replayed_request_deduplicates_without_duplicate_write_minio() {
    let (_container, _port, store) = match setup_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_replayed_request_deduplicates_without_duplicate_write_minio: MinIO container unavailable");
            return;
        }
    };

    let identity = RequestIdentity::new(101, 1, 100, 1, 1);
    let payload = b"minio-arrow-batch";
    let executions = Arc::new(AtomicUsize::new(0));

    // Phase 1: Execute on MinIO
    {
        let db = ShardDbBuilder::new("shard-minio-1", store.clone())
            .build()
            .await
            .unwrap();

        let ops = executions.clone();
        let (res, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        assert!(!res.is_replayed());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    // Phase 2: Reopen from MinIO post restart
    {
        let db = ShardDbBuilder::new("shard-minio-1", store.clone())
            .build()
            .await
            .unwrap();

        let ops = executions.clone();
        let (res, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        assert!(res.is_replayed());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            1,
            "zero duplicate logical writes on MinIO"
        );
    }
}

#[tokio::test]
async fn test_kill_pre_ack_recovers_committed_frontier_minio() {
    let (_container, _port, store) = match setup_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_kill_pre_ack_recovers_committed_frontier_minio: MinIO container unavailable");
            return;
        }
    };

    let identity = RequestIdentity::new(202, 2, 200, 77, 12);
    let payload = b"minio-kill-pre-ack";

    // Process commits to MinIO then is dropped
    {
        let db = ShardDbBuilder::new("shard-minio-kill", store.clone())
            .build()
            .await
            .unwrap();

        execute_durable_request(&db, &identity, payload, 1, 1, || async { Ok(()) })
            .await
            .unwrap();
    }

    // Recover from MinIO: committed frontier is 77 and replayed request returns cached ACK
    {
        let db = ShardDbBuilder::new("shard-minio-kill", store.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(committed_frontier(&db).await.unwrap(), 77);

        let (res, _): (_, Option<()>) =
            execute_durable_request(&db, &identity, payload, 1, 1, || async {
                panic!("must not execute op on replayed committed request");
            })
            .await
            .unwrap();

        assert!(res.is_replayed());
        assert_eq!(res.outcome().committed_epoch, 77);
    }
}

#[tokio::test]
async fn test_shared_arrangement_restores_from_minio_post_restart() {
    let (_container, _port, store) = match setup_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_shared_arrangement_restores_from_minio_post_restart: MinIO container unavailable");
            return;
        }
    };

    let checkpoint_path = Path::from("catalog/arrangement_catalog.json");

    // Phase 1: Create catalog, register arrangements and snapshot to MinIO
    let arrangement_id: ArrangementId;
    {
        let catalog = ArrangementCatalog::new();
        let spec = ArrangementSpec::default_for_source(TenantId(1), "shared_source_table");
        let (id, is_new) = catalog.register_consumer(ViewId(101), spec.clone()).await;
        assert!(is_new);
        arrangement_id = id;

        // Register second consumer
        let (id2, is_new2) = catalog.register_consumer(ViewId(102), spec.clone()).await;
        assert_eq!(id, id2);
        assert!(!is_new2, "second view must reuse existing arrangement");
        assert_eq!(catalog.consumer_count(id).await, 2);

        // Snapshot to MinIO
        let snapshot = catalog.snapshot().await;
        let bytes = serde_json::to_vec(&snapshot).expect("serialize catalog snapshot");
        store
            .put(&checkpoint_path, Bytes::from(bytes).into())
            .await
            .expect("persist catalog to MinIO");
    }

    // Phase 2: Restart worker process and restore arrangement catalog from MinIO
    {
        let catalog = ArrangementCatalog::new();
        let get_res = store
            .get(&checkpoint_path)
            .await
            .expect("read catalog from MinIO");
        let bytes = get_res.bytes().await.expect("read bytes");
        let snapshot: Vec<_> =
            serde_json::from_slice(&bytes).expect("deserialize catalog snapshot");
        catalog.restore(snapshot).await;

        assert_eq!(
            catalog.consumer_count(arrangement_id).await,
            2,
            "restored arrangement must retain 2 consumers"
        );
        let entry = catalog
            .lookup(arrangement_id)
            .await
            .expect("arrangement must exist in restored catalog");
        assert_eq!(entry.spec.source_identity.as_str(), "shared_source_table");
        assert_eq!(entry.consumers.len(), 2);
    }
}
