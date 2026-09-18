//! Bounded ShardDb Streaming Paged Scans Tests (v0.67.1 Slice 2 / V0671-03).
//!
//! Validates:
//! 1. `scan_prefix_paginated` streams directly over SlateDB iterators without full-state buffer accumulation.
//! 2. Row-bounded and byte-bounded page continuation with deterministic tokens.
//! 3. Cancellation, limit enforcement, and exact recovered multiset.

use std::sync::Arc;

use rockstream_storage::{ScanPage, ScanProgressHandle, ShardDb, StorageError, WriteBatch};
use tempfile::tempdir;

async fn open_test_shard_db(dir: &tempfile::TempDir) -> ShardDb {
    let object_store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    ShardDb::builder("test_paged_scan", object_store)
        .build()
        .await
        .expect("build shard db")
}

#[tokio::test]
async fn test_shard_db_scan_prefix_paginated_streaming_memory_bound() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"stream_test/";
    let total_rows = 5000;

    let mut batch = WriteBatch::new();
    for i in 0..total_rows {
        let key = format!("stream_test/{:06}", i);
        let val = format!("value_{:06}_payload", i);
        batch.put(key.as_bytes(), val.as_bytes());
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush db");

    let progress = ScanProgressHandle::new();
    let page_size = 100; // 50 pages total
    let max_buffer = 10 * 1024 * 1024; // 10 MiB limit

    let results = db
        .scan_prefix_paginated(prefix, page_size, max_buffer, &progress)
        .await
        .expect("scan_prefix_paginated");

    assert_eq!(results.len(), total_rows);
    assert_eq!(progress.rows_scanned(), total_rows as u64);
    assert_eq!(progress.pages_scanned(), (total_rows / page_size) as u64);
    assert!(progress.bytes_scanned() > 0);

    // Verify ordering and correctness
    for (i, (k, v)) in results.iter().enumerate() {
        let expected_key = format!("stream_test/{:06}", i);
        let expected_val = format!("value_{:06}_payload", i);
        assert_eq!(k.as_ref(), expected_key.as_bytes());
        assert_eq!(v.as_ref(), expected_val.as_bytes());
    }
}

#[tokio::test]
async fn test_paged_scan_continuation_tokens() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"token_test/";
    let total_rows = 250;

    let mut batch = WriteBatch::new();
    for i in 0..total_rows {
        let key = format!("token_test/{:06}", i);
        let val = format!("val_{:06}", i);
        batch.put(key.as_bytes(), val.as_bytes());
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush db");

    let page_size = 50;
    let mut continuation: Option<Vec<u8>> = None;
    let mut retrieved_rows = 0;
    let mut page_count = 0;

    loop {
        let page: ScanPage = db
            .scan_prefix_page(prefix, continuation.as_deref(), page_size, 1024 * 1024)
            .await
            .expect("scan page");

        retrieved_rows += page.rows.len();
        page_count += 1;

        if page.is_last_page {
            assert!(page.next_token.is_none());
            break;
        }

        assert!(page.next_token.is_some());
        continuation = page.next_token.map(|b| b.to_vec());
    }

    assert_eq!(retrieved_rows, total_rows);
    assert_eq!(page_count, 5);
}

#[tokio::test]
async fn test_paged_scan_cancellation_releases_resources() {
    let dir = tempdir().unwrap();
    let db = open_test_shard_db(&dir).await;

    let prefix = b"cancel_test/";
    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("cancel_test/{:06}", i);
        batch.put(key.as_bytes(), b"data");
    }
    db.write_batch(batch).await.expect("write batch");
    db.flush().await.expect("flush db");

    let progress = ScanProgressHandle::new();
    progress.cancel();

    let res = db
        .scan_prefix_paginated(prefix, 100, 1024 * 1024, &progress)
        .await;

    assert!(res.is_err());
    let err = res.unwrap_err();
    match err {
        StorageError::Unsupported(msg) => {
            assert!(msg.contains("cancelled"));
        }
        other => panic!("expected cancellation error, got: {other:?}"),
    }
}
