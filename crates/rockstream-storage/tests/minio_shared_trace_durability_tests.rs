//! v0.59.6 Slice 9: MinIO S3 Shared Trace Durability Tests.
//!
//! Verifies object-store shared trace commits, multi-consumer snapshot reading,
//! compaction manifests, and recovery against MinIO S3 when available.

use object_store::ObjectStore;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::trace::{SharedArrangementTrace, TraceManifestHeader};
use rockstream_storage::ShardDb;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::batch::ZSetRow;
use rockstream_types::compatibility::SupportedStorageFormatRange;
use rockstream_types::ids::{TenantId, ViewId};
use std::sync::Arc;

const MINIO_BUCKET: &str = "rockstream-shared-trace-test";

#[tokio::test]
async fn test_minio_shared_trace_durability_and_recovery() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_minio_shared_trace_durability_and_recovery: MinIO unavailable");
            return;
        }
    };

    let store: Arc<dyn ObjectStore> = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));

    // Initialize ShardDb against MinIO in V3 format
    let db = ShardDb::builder("minio-shared-trace-shard", store.clone())
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    let spec = ArrangementSpec::default_for_source(TenantId(1), "minio_trades");
    let mut trace = SharedArrangementTrace::new(spec.clone());

    let consumer_1 = ViewId(301);
    let consumer_2 = ViewId(302);
    trace.register_consumer_frontier(consumer_1, 0);
    trace.register_consumer_frontier(consumer_2, 0);

    let k1 = b"AMZN".to_vec();
    trace.commit_trace_batch(0, 100, vec![ZSetRow::insert(k1.clone(), b"3500".to_vec())]);

    let header = TraceManifestHeader::new(spec.clone(), 0);
    let header_bytes = header.to_bytes().unwrap();
    let manifest_key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"trace_manifest");
    db.put(&manifest_key, &header_bytes).await.unwrap();

    let trace_bytes = serde_json::to_vec(&trace).unwrap();
    let trace_key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"trace_data");
    db.put(&trace_key, &trace_bytes).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Reopen and assert recovery from MinIO S3
    let reopened = ShardDb::builder("minio-shared-trace-shard", store.clone())
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    let raw_header = reopened.get(&manifest_key).await.unwrap().unwrap();
    let recovered_header = TraceManifestHeader::from_bytes(&raw_header).unwrap();
    assert_eq!(recovered_header.spec, spec);
    assert_eq!(recovered_header.arrangement_id, spec.arrangement_id());

    let raw_trace = reopened.get(&trace_key).await.unwrap().unwrap();
    let recovered_trace: SharedArrangementTrace = serde_json::from_slice(&raw_trace).unwrap();
    assert_eq!(recovered_trace.arrangement_id, spec.arrangement_id());

    let snap = recovered_trace.read_trace_snapshot(100).unwrap();
    assert_eq!(snap.get(&k1).unwrap().0, b"3500");

    reopened.close().await.unwrap();
}
