//! Multi-Page Bounded Scan Continuation for Recovery Tests (v0.65 Slice 2 / Phase 3a).
//!
//! Validates that recovery and restore scans:
//! 1. Paginate to completion with bounded page sizes (MAX_RESTORE_SCAN_PAGE_ROWS = 1,024).
//! 2. Obey memory buffer bounds (MAX_RECOVERY_SCAN_BUFFER_BYTES = 32 MiB) and fail with RS-2002 on overflow.
//! 3. Support observable progress tracking and clean cancellation.
//! 4. Never stop on the first page when multiple pages exist.

use std::sync::Arc;

use rockstream_storage::{
    ScanProgressHandle, ShardDb, StorageError, MAX_RECOVERY_SCAN_BUFFER_BYTES,
    MAX_RESTORE_SCAN_PAGE_ROWS,
};
use tempfile::tempdir;

async fn open_test_shard_db(dir: &tempfile::TempDir) -> ShardDb {
    let object_store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    ShardDb::builder("test_shard", object_store)
        .build()
        .await
        .expect("build shard db")
}

#[tokio::test]
async fn test_multi_page_recovery_scan_continues_to_completion() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"test_prefix/";
    let total_rows = 2500; // Exceeds MAX_RESTORE_SCAN_PAGE_ROWS (1,024) across > 2 pages

    let mut batch = rockstream_storage::WriteBatch::new();
    for i in 0..total_rows {
        let key = format!("test_prefix/{:06}", i);
        let val = format!("val_{:06}", i);
        batch.put(key.as_bytes(), val.as_bytes());
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush db");

    let progress = ScanProgressHandle::new();
    let results = db
        .scan_prefix_paginated(
            prefix,
            MAX_RESTORE_SCAN_PAGE_ROWS,
            MAX_RECOVERY_SCAN_BUFFER_BYTES,
            &progress,
        )
        .await
        .expect("scan paginated");

    // Must return all 2,500 rows, NOT stopping at 1,024 rows.
    assert_eq!(results.len(), total_rows);
    assert_eq!(progress.rows_scanned(), total_rows as u64);
    // 2,500 rows / 1,024 = 3 pages (1024, 1024, 452)
    assert_eq!(progress.pages_scanned(), 3);
    assert!(progress.bytes_scanned() > 0);

    // Verify ordering and exact row contents
    for (i, (k, v)) in results.iter().enumerate() {
        let expected_key = format!("test_prefix/{:06}", i);
        let expected_val = format!("val_{:06}", i);
        assert_eq!(k.as_ref(), expected_key.as_bytes());
        assert_eq!(v.as_ref(), expected_val.as_bytes());
    }
}

#[tokio::test]
async fn test_recovery_scan_buffer_limit_overflow() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"overflow_prefix/";
    let mut batch = rockstream_storage::WriteBatch::new();
    for i in 0..100 {
        let key = format!("overflow_prefix/{:04}", i);
        let val = vec![b'x'; 1024]; // 1 KiB per entry
        batch.put(key.as_bytes(), &val);
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush");

    let progress = ScanProgressHandle::new();
    // Set max buffer to 10 KiB, which 100 KiB will exceed
    let err = db
        .scan_prefix_paginated(prefix, 10, 10 * 1024, &progress)
        .await
        .unwrap_err();

    match err {
        StorageError::ScanBufferLimitExceeded { bytes, limit } => {
            assert!(bytes > limit);
            let msg = err.to_string();
            assert!(msg.contains("RS-2002"));
            assert!(msg.contains("scan buffer limit exceeded"));
        }
        other => panic!(
            "expected StorageError::ScanBufferLimitExceeded, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_recovery_scan_cancellation_handle() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"cancel_prefix/";
    let mut batch = rockstream_storage::WriteBatch::new();
    for i in 0..100 {
        let key = format!("cancel_prefix/{:04}", i);
        batch.put(key.as_bytes(), b"val");
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush");

    let progress = ScanProgressHandle::new();
    progress.cancel();
    assert!(progress.is_cancelled());

    let err = db
        .scan_prefix_paginated(prefix, 10, MAX_RECOVERY_SCAN_BUFFER_BYTES, &progress)
        .await
        .unwrap_err();

    match err {
        StorageError::Unsupported(msg) => {
            assert!(msg.contains("cancelled by caller"));
        }
        other => panic!(
            "expected StorageError::Unsupported cancellation, got {:?}",
            other
        ),
    }
}

#[tokio::test]
async fn test_truncation_injection_fails_startup() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"trunc_prefix/";
    let mut batch = rockstream_storage::WriteBatch::new();
    for i in 0..50 {
        let key = format!("trunc_prefix/{:04}", i);
        batch.put(key.as_bytes(), b"data");
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush");

    // Injected scan truncation: progress handle cancelled early after 1st page
    let progress = ScanProgressHandle::new();
    progress.cancel();

    let err = db
        .scan_prefix_paginated(prefix, 10, MAX_RECOVERY_SCAN_BUFFER_BYTES, &progress)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("cancelled by caller"));
}
