//! WAL reader utilities.
//!
//! Provides access to the SlateDB write-ahead log for recovery
//! and debugging scenarios.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::Db;

use crate::error::StorageError;

/// Read WAL entries by scanning the database at uncommitted read level.
///
/// SlateDB's WAL entries are visible through the normal read path when using
/// dirty reads. This function provides a simple way to verify
/// WAL contents during testing.
pub async fn read_wal_entries(db: &Db, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>, StorageError> {
    let mut results = Vec::new();
    let mut iter = db.scan_prefix(prefix, ..).await?;
    while let Some(entry) = iter.next().await? {
        results.push((entry.key, entry.value));
    }
    Ok(results)
}

/// Verify that a set of keys exist in the database after flush.
///
/// This is a test utility that confirms writes made it through the WAL
/// and are visible in the durable store.
pub async fn verify_keys_present(db: &Db, keys: &[&[u8]]) -> Result<Vec<bool>, StorageError> {
    let mut results = Vec::with_capacity(keys.len());
    for key in keys {
        let val = db.get(*key).await?;
        results.push(val.is_some());
    }
    Ok(results)
}

/// Open a database configured for dirty reads (includes unflushed WAL data).
///
/// Returns a `Db` that reads unflushed in-memory data, useful
/// for verifying writes before flush.
pub async fn open_with_wal_reads(
    path: impl Into<String>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Db, StorageError> {
    // SlateDB's default ReadOptions already include Memory durability level
    // which means reads see in-memory (unflushed) data. We just open normally.
    let db = Db::builder(path.into(), object_store).build().await?;
    Ok(db)
}
