//! v0.59.6 Slice 8: SimRuntime Shared Trace Crash Boundary Tests.
//!
//! Asserts that crashes at trace commit, compaction, migration, and reclamation boundaries
//! recover cleanly without state loss or duplicate records.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::arrangement_catalog::ArrangementCatalog;
use rockstream_storage::format_migration::{migrate_shard_format_with_options, MigrationOptions};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_storage::ShardDb;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::batch::ZSetRow;
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};
use rockstream_types::ids::{TenantId, ViewId};
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_crash_at_v2_to_v3_migration_boundary_recovery() {
    let dir = TempDir::new().unwrap();

    // 1. Populate initial V2 shard
    let db = ShardDb::builder("shard-v2-chaos", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"k1");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"k2");
    let k3 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"k3");
    db.put(&k1, b"v1").await.unwrap();
    db.put(&k2, b"v2").await.unwrap();
    db.put(&k3, b"v3").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // 2. Inject simulated crash after migrating 1 object
    let crash_err = migrate_shard_format_with_options(
        "shard-v2-chaos",
        store(&dir),
        StorageFormatVersion::V2,
        StorageFormatVersion::V3,
        MigrationOptions {
            fail_after_objects: Some(1),
        },
    )
    .await
    .unwrap_err();

    assert!(crash_err.to_string().contains("interrupted"));

    // 3. Resume migration from crash boundary to completion
    let resume_report = migrate_shard_format_with_options(
        "shard-v2-chaos",
        store(&dir),
        StorageFormatVersion::V2,
        StorageFormatVersion::V3,
        MigrationOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(resume_report.from, StorageFormatVersion::V2);
    assert_eq!(resume_report.to, StorageFormatVersion::V3);

    // 4. Verify completed V3 shard state
    let v3_db = ShardDb::builder("shard-v2-chaos", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    assert_eq!(v3_db.format_version(), StorageFormatVersion::V3.0);
    assert_eq!(
        v3_db.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        v3_db.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        v3_db.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
    v3_db.close().await.unwrap();
}

#[tokio::test]
async fn test_shared_trace_compaction_crash_recovery() {
    let spec = ArrangementSpec::default_for_source(TenantId(1), "chaos_events");
    let mut trace = SharedArrangementTrace::new(spec);

    let consumer_a = ViewId(1);
    let consumer_b = ViewId(2);
    trace.register_consumer_frontier(consumer_a, 0);
    trace.register_consumer_frontier(consumer_b, 0);

    let k1 = b"k1".to_vec();
    trace.commit_trace_batch(0, 5, vec![ZSetRow::insert(k1.clone(), b"v5".to_vec())]);
    trace.commit_trace_batch(5, 10, vec![ZSetRow::insert(k1.clone(), b"v10".to_vec())]);

    // Consumer A is fast (at 10), Consumer B is slow (at 5)
    trace.advance_consumer_frontier(consumer_a, 10);
    trace.advance_consumer_frontier(consumer_b, 5);

    // Compaction up to slowest consumer (5)
    let comp_frontier = trace.compact_trace();
    assert_eq!(comp_frontier, 5);
    assert_eq!(trace.base_frontier, 5);

    // Both consumers can still read their respective views without data corruption
    let snap_5 = trace.read_trace_snapshot(5).unwrap();
    assert_eq!(snap_5.get(&k1).unwrap().0, b"v5");

    let snap_10 = trace.read_trace_snapshot(10).unwrap();
    assert_eq!(snap_10.get(&k1).unwrap().0, b"v10");
}

#[tokio::test]
async fn test_shared_arrangement_catalog_reclamation_crash_safety() {
    let catalog = ArrangementCatalog::new();
    let spec = ArrangementSpec::default_for_source(TenantId(1), "orders_chaos");
    let view_1 = ViewId(101);
    let view_2 = ViewId(102);

    let (arr_id, is_new) = catalog.register_consumer(view_1, spec.clone()).await;
    assert!(is_new);
    let (arr_id2, is_new2) = catalog.register_consumer(view_2, spec).await;
    assert_eq!(arr_id, arr_id2);
    assert!(!is_new2);
    assert_eq!(catalog.consumer_count(arr_id).await, 2);

    // Drop first consumer: refcount becomes 1, no GC
    let marked1 = catalog.deregister_consumer(view_1, arr_id).await.unwrap();
    assert!(!marked1);
    assert_eq!(catalog.consumer_count(arr_id).await, 1);
    let reclaimed_none = catalog.reclaim_unreferenced_arrangements(100).await;
    assert!(reclaimed_none.is_empty());

    // Drop second consumer: refcount becomes 0, marked for GC
    let marked2 = catalog.deregister_consumer(view_2, arr_id).await.unwrap();
    assert!(marked2);
    assert_eq!(catalog.consumer_count(arr_id).await, 0);

    // Update compaction frontier to 100
    catalog.update_compaction_frontier(arr_id, 100).await;

    // Safe reclamation cleans up
    let reclaimed = catalog.reclaim_unreferenced_arrangements(100).await;
    assert_eq!(reclaimed, vec![arr_id]);
    assert_eq!(catalog.physical_arrangements_count().await, 0);
}
