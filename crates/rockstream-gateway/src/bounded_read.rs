//! Bounded producer-side legacy view prefix scans for v0.72 (#135).
//!
//! Unlike eager scan_prefix(), this does not retain an entire keyspace for
//! a read with a declared row limit. Producer cancellation drops a guard that
//! aborts the scanner. Compiled-view materialization remains a separate path.

use std::sync::Arc;

use rockstream_storage::ShardReader;
use tokio::task::JoinHandle;

use crate::error::GatewayError;

/// Returned-state ceiling in the frozen v0.72 interactive profile.
pub const LIMITED_SCAN_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Bounded per-page, per-channel memory independent of the total keyspace.
const PAGE_ROWS: usize = 128;
const PAGE_BYTES: usize = 8 * 1024 * 1024 + 8192;

struct AbortProducer(JoinHandle<()>);

impl Drop for AbortProducer {
    fn drop(&mut self) {
        // Releasing a bounded request must not leave its storage iterator
        // running to exhaustion after client disconnect or early LIMIT.
        self.0.abort();
    }
}

fn failed(detail: impl Into<String>) -> GatewayError {
    GatewayError::QueryTimeExecutionFailed {
        detail: detail.into(),
    }
}

/// Read only the first `limit` rows, with bounded storage pages and a
/// producer-side materialization byte ceiling. This never streams a partial
/// response on a limit error: the caller gets an error before any rows.
pub async fn scan_bounded_prefix(
    reader: Arc<ShardReader>,
    prefix: &[u8],
    limit: usize,
    max_output_bytes: usize,
) -> Result<Vec<Vec<u8>>, GatewayError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if max_output_bytes == 0 || max_output_bytes > LIMITED_SCAN_MAX_BYTES {
        return Err(failed("invalid limited-scan byte budget"));
    }
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let scan_prefix = prefix.to_vec();
    let producer = AbortProducer(tokio::spawn(async move {
        reader
            .scan_prefix_pages(&scan_prefix, PAGE_ROWS.min(limit), PAGE_BYTES, sender)
            .await;
    }));
    let mut rows = Vec::with_capacity(limit.min(128));
    let mut output_bytes = 0_usize;
    while let Some(page) = receiver.recv().await {
        let page = page?;
        for (_, value) in page {
            output_bytes = output_bytes
                .checked_add(value.len())
                .ok_or_else(|| failed("limited-scan materialization byte counter overflow"))?;
            if output_bytes > max_output_bytes {
                return Err(failed(format!(
                    "limited-scan materialization exceeds {max_output_bytes} bytes"
                )));
            }
            rows.push(value.to_vec());
            if rows.len() == limit {
                return Ok(rows);
            }
        }
    }
    // Intentional: a completed producer guard is dropped, so cancellations
    // and errors take the same cleanup path.
    drop(producer);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_storage::{ShardDb, ShardReader};

    async fn seeded_reader(values: &[&[u8]]) -> Arc<ShardReader> {
        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("bounded-prefix-test", store.clone())
            .build()
            .await
            .unwrap();
        for (index, value) in values.iter().enumerate() {
            let key = format!("view_output/test/{index:04}");
            db.put(key.as_bytes(), value).await.unwrap();
        }
        db.flush().await.unwrap();
        Arc::new(
            ShardReader::open("bounded-prefix-test", store)
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn v072_scan_limited_prefix_returns_only_first_ordered_rows() {
        let reader = seeded_reader(&[b"a", b"b", b"c", b"d", b"e"]).await;
        assert_eq!(
            scan_bounded_prefix(reader.clone(), b"view_output/test/", 2, 64)
                .await
                .unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            scan_bounded_prefix(reader.clone(), b"view_output/test/", 5, 64)
                .await
                .unwrap(),
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );
        assert_eq!(
            scan_bounded_prefix(reader, b"view_output/absent/", 4, 64)
                .await
                .unwrap(),
            Vec::<Vec<u8>>::new()
        );
    }

    #[tokio::test]
    async fn v072_scan_rejects_over_bytes_without_returning_partial_rows() {
        let reader = seeded_reader(&[b"a", b"bbbb", b"cc"]).await;
        let error = scan_bounded_prefix(reader.clone(), b"view_output/test/", 3, 4)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("materialization exceeds 4 bytes"),
            "unexpected error: {error}"
        );
        assert_eq!(
            scan_bounded_prefix(reader, b"view_output/test/", 2, 5)
                .await
                .unwrap(),
            vec![b"a".to_vec(), b"bbbb".to_vec()]
        );
    }

    #[tokio::test]
    async fn v072_scan_rejects_invalid_budget_and_zero_rows_without_scanning() {
        let reader = seeded_reader(&[b"a"]).await;
        assert_eq!(
            scan_bounded_prefix(reader.clone(), b"view_output/test/", 0, 64)
                .await
                .unwrap(),
            Vec::<Vec<u8>>::new()
        );
        for invalid in [0, LIMITED_SCAN_MAX_BYTES + 1] {
            assert!(
                scan_bounded_prefix(reader.clone(), b"view_output/test/", 1, invalid)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("invalid limited-scan byte budget")
            );
        }
    }
}
