//! v0.59.6 Slice 9: Local Filesystem (LFS) Shared Trace Durability Tests.
//!
//! Verifies shared trace batch commits, multi-consumer snapshot reading,
//! compaction safety derived from slowest consumer, and recovery on LFS.

use object_store::local::LocalFileSystem;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::trace::{SharedArrangementTrace, TraceManifestHeader};
use rockstream_storage::ShardDb;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::batch::ZSetRow;
use rockstream_types::compatibility::SupportedStorageFormatRange;
use rockstream_types::ids::{TenantId, ViewId};
use std::sync::Arc;
use tempfile::TempDir;

fn fast_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(std::time::Duration::from_millis(10)),
        manifest_poll_interval: std::time::Duration::from_millis(10),
        ..slatedb::config::Settings::default()
    }
}

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_lfs_shared_trace_durability_and_recovery() {
    let dir = TempDir::new().unwrap();
    let db = ShardDb::builder("shard-lfs-shared-trace", store(&dir))
        .with_settings(fast_settings())
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    let spec = ArrangementSpec::default_for_source(TenantId(1), "lfs_trades");
    let mut trace = SharedArrangementTrace::new(spec.clone());

    let consumer_1 = ViewId(101);
    let consumer_2 = ViewId(102);
    trace.register_consumer_frontier(consumer_1, 0);
    trace.register_consumer_frontier(consumer_2, 0);

    // Commit batches
    let k1 = b"AAPL".to_vec();
    let k2 = b"GOOG".to_vec();
    trace.commit_trace_batch(
        0,
        50,
        vec![
            ZSetRow::insert(k1.clone(), b"150".to_vec()),
            ZSetRow::insert(k2.clone(), b"2800".to_vec()),
        ],
    );
    trace.commit_trace_batch(50, 100, vec![ZSetRow::insert(k1.clone(), b"155".to_vec())]);

    // Write trace manifest header and trace state to ShardDb
    let header = TraceManifestHeader::new(spec.clone(), 0);
    let header_bytes = header.to_bytes().unwrap();
    let manifest_key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"trace_manifest");
    db.put(&manifest_key, &header_bytes).await.unwrap();

    let trace_bytes = serde_json::to_vec(&trace).unwrap();
    let trace_key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"trace_data");
    db.put(&trace_key, &trace_bytes).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Reopen from LFS and verify persistence
    let reopened = ShardDb::builder("shard-lfs-shared-trace", store(&dir))
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
    assert_eq!(recovered_trace.delta_batches.len(), 2);

    let snap_50 = recovered_trace.read_trace_snapshot(50).unwrap();
    assert_eq!(snap_50.get(&k1).unwrap().0, b"150");
    assert_eq!(snap_50.get(&k2).unwrap().0, b"2800");

    let snap_100 = recovered_trace.read_trace_snapshot(100).unwrap();
    assert_eq!(snap_100.get(&k1).unwrap().0, b"155");
    assert_eq!(snap_100.get(&k2).unwrap().0, b"2800");

    reopened.close().await.unwrap();
}

#[tokio::test]
async fn test_lfs_shared_trace_compaction_and_slowest_consumer() {
    let spec = ArrangementSpec::default_for_source(TenantId(1), "lfs_compaction");
    let mut trace = SharedArrangementTrace::new(spec);

    let consumer_fast = ViewId(201);
    let consumer_slow = ViewId(202);
    trace.register_consumer_frontier(consumer_fast, 0);
    trace.register_consumer_frontier(consumer_slow, 0);

    let k = b"key_x".to_vec();
    trace.commit_trace_batch(0, 10, vec![ZSetRow::insert(k.clone(), b"v1".to_vec())]);
    trace.commit_trace_batch(10, 20, vec![ZSetRow::insert(k.clone(), b"v2".to_vec())]);
    trace.commit_trace_batch(20, 30, vec![ZSetRow::insert(k.clone(), b"v3".to_vec())]);

    trace.advance_consumer_frontier(consumer_fast, 30);
    trace.advance_consumer_frontier(consumer_slow, 10);

    // Compaction bounded by slowest consumer (10)
    let comp_1 = trace.compact_trace();
    assert_eq!(comp_1, 10);
    assert_eq!(trace.base_frontier, 10);
    assert_eq!(trace.delta_batches.len(), 2); // batches (10, 20] and (20, 30]

    // Slow consumer catches up to 30
    trace.advance_consumer_frontier(consumer_slow, 30);
    let comp_2 = trace.compact_trace();
    assert_eq!(comp_2, 30);
    assert_eq!(trace.base_frontier, 30);
    assert_eq!(trace.delta_batches.len(), 0);

    // Snapshot at 30 remains accurate
    let snap = trace.read_trace_snapshot(30).unwrap();
    assert_eq!(snap.get(&k).unwrap().0, b"v3");
}
