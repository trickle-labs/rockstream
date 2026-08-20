//! Offline, resumable storage-format migration.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use object_store::ObjectStore;
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};

use crate::error::StorageError;
use crate::keys::{ShardKeyEncoder, ShardPrefix};
use crate::ShardDb;

/// Migration intentionally processes one key/object at a time.
pub const MAX_MIGRATION_OBJECTS_IN_FLIGHT: usize = 1;
pub const MAX_MIGRATION_SHARDS: usize = 4096;

/// Current migration work fill level.
pub static MIGRATION_OBJECTS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Maximum observed migration work fill level.
pub static MIGRATION_MAX_OBJECTS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

pub fn migration_fill_level() -> (usize, usize) {
    (
        MIGRATION_OBJECTS_IN_FLIGHT.load(Ordering::Relaxed),
        MIGRATION_MAX_OBJECTS_IN_FLIGHT.load(Ordering::Relaxed),
    )
}

struct InFlightObject;

impl Drop for InFlightObject {
    fn drop(&mut self) {
        MIGRATION_OBJECTS_IN_FLIGHT.store(0, Ordering::Relaxed);
    }
}

const V2_KEY_MAGIC: &[u8] = b"\xffV2";

/// Testable interruption point for proving restart safety.
#[derive(Debug, Clone, Copy, Default)]
pub struct MigrationOptions {
    pub fail_after_objects: Option<usize>,
}

/// Exact outcome of migrating one shard.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FormatMigrationReport {
    pub path: String,
    pub from: StorageFormatVersion,
    pub to: StorageFormatVersion,
    pub objects_migrated: usize,
    pub already_complete: bool,
    pub max_objects_in_flight: usize,
}

/// Migrate every shard below a bounded storage URL prefix.
pub async fn migrate_storage_format(
    storage_url: &str,
    from: StorageFormatVersion,
    to: StorageFormatVersion,
) -> Result<Vec<FormatMigrationReport>, StorageError> {
    if (from, to) != (StorageFormatVersion::V1, StorageFormatVersion::V2) {
        return Err(StorageError::Unsupported(
            "RS-0002: only storage format migration 1→2 is supported".to_string(),
        ));
    }
    let store =
        crate::build_migration_object_store(storage_url).map_err(StorageError::Unsupported)?;
    let mut shards = std::collections::HashSet::new();
    let mut objects = store.list(None);
    while let Some(meta) = objects.next().await {
        let meta = meta.map_err(|error| StorageError::Unsupported(error.to_string()))?;
        let mut parts = meta.location.as_ref().split('/');
        if parts.next() != Some("shards") {
            continue;
        }
        let Some(shard_id) = parts.next() else {
            continue;
        };
        let path = format!("shards/{shard_id}/db");
        if shards.insert(path.clone()) && shards.len() > MAX_MIGRATION_SHARDS {
            return Err(StorageError::Unsupported(format!(
                "RS-0002: migration shard discovery exceeded MAX_MIGRATION_SHARDS={MAX_MIGRATION_SHARDS}"
            )));
        }
    }
    if shards.is_empty() {
        return Err(StorageError::Unsupported(format!(
            "RS-0002: no shard objects found below migration storage `{storage_url}`"
        )));
    }

    let mut reports = Vec::with_capacity(shards.len());
    for path in shards {
        reports.push(migrate_shard_format(&path, store.clone(), from, to).await?);
    }
    reports.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(reports)
}

pub(crate) fn format_v2_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(V2_KEY_MAGIC.len() + key.len());
    encoded.extend_from_slice(V2_KEY_MAGIC);
    encoded.extend_from_slice(key);
    encoded
}

pub(crate) fn is_format_v2_key(key: &[u8]) -> bool {
    key.starts_with(V2_KEY_MAGIC)
}

pub(crate) fn format_v2_prefix(prefix: &[u8]) -> Vec<u8> {
    format_v2_key(prefix)
}

pub(crate) fn logical_key_from_format_v2(key: &[u8]) -> Option<&[u8]> {
    key.strip_prefix(V2_KEY_MAGIC)
}

pub(crate) fn migration_progress_key(
    from: StorageFormatVersion,
    to: StorageFormatVersion,
) -> Vec<u8> {
    ShardKeyEncoder::meta_key(format!("format_migration/progress/{from}-{to}").as_bytes())
}

pub(crate) fn migration_complete_key(
    from: StorageFormatVersion,
    to: StorageFormatVersion,
) -> Vec<u8> {
    ShardKeyEncoder::meta_key(format!("format_migration/complete/{from}-{to}").as_bytes())
}

pub(crate) fn is_data_key(key: &[u8]) -> bool {
    key.first().copied() != Some(ShardPrefix::ShardMeta.as_byte())
}

/// Migrate one shard using the synthetic, real 1→2 layout transform.
pub async fn migrate_shard_format<F, T>(
    path: impl Into<String>,
    object_store: Arc<dyn ObjectStore>,
    from: F,
    to: T,
) -> Result<FormatMigrationReport, StorageError>
where
    F: Into<StorageFormatVersion>,
    T: Into<StorageFormatVersion>,
{
    migrate_shard_format_with_options(path, object_store, from, to, MigrationOptions::default())
        .await
}

/// Migrate one shard with a deterministic interruption hook for tests.
pub async fn migrate_shard_format_with_options<F, T>(
    path: impl Into<String>,
    object_store: Arc<dyn ObjectStore>,
    from: F,
    to: T,
    options: MigrationOptions,
) -> Result<FormatMigrationReport, StorageError>
where
    F: Into<StorageFormatVersion>,
    T: Into<StorageFormatVersion>,
{
    let path = path.into();
    let from = from.into();
    let to = to.into();
    if (from, to) != (StorageFormatVersion::V1, StorageFormatVersion::V2) {
        return Err(StorageError::Unsupported(format!(
            "RS-0002: only storage format migration 1→2 is supported (requested {from}→{to})"
        )));
    }

    let db = ShardDb::builder(path.clone(), object_store)
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await?;
    let raw = db.raw_db();
    let complete_key = migration_complete_key(from, to);
    if raw.get(&complete_key).await?.is_some() {
        db.close().await?;
        return Ok(FormatMigrationReport {
            path,
            from,
            to,
            objects_migrated: 0,
            already_complete: true,
            max_objects_in_flight: 0,
        });
    }
    if db.format_version() != from.0 {
        let result = Err(StorageError::IncompatibleFormat {
            stored: db.format_version(),
            min: from.0,
            max: from.0,
        });
        let _ = db.close().await;
        return result;
    }

    let progress_key = migration_progress_key(from, to);
    let mut processed = 0usize;
    let mut iter = raw.scan::<&[u8], _>(..).await?;
    while let Some(entry) = iter.next().await? {
        if !is_data_key(&entry.key) || is_format_v2_key(&entry.key) {
            continue;
        }
        if options
            .fail_after_objects
            .is_some_and(|limit| processed >= limit)
        {
            let _ = db.close().await;
            return Err(StorageError::MigrationInterrupted { processed });
        }

        MIGRATION_OBJECTS_IN_FLIGHT.store(1, Ordering::Relaxed);
        MIGRATION_MAX_OBJECTS_IN_FLIGHT.fetch_max(1, Ordering::Relaxed);
        let _in_flight = InFlightObject;

        let new_key = format_v2_key(&entry.key);
        raw.put(&new_key, &entry.value).await?;
        raw.flush().await?;
        if raw.get(&new_key).await?.as_deref() != Some(entry.value.as_ref()) {
            let _ = db.close().await;
            return Err(StorageError::Unsupported(format!(
                "RS-5001: format migration verification failed for key {:?}",
                entry.key
            )));
        }
        raw.delete(&entry.key).await?;
        raw.flush().await?;
        processed += 1;
        raw.put(&progress_key, &processed.to_be_bytes()).await?;
        raw.flush().await?;
    }

    // INVARIANT-BY-CONSTRUCTION: M1-S7 — shard format version is atomically updated to V2 upon successful migration completion.
    let mut batch = slatedb::WriteBatch::new();
    batch.put(ShardKeyEncoder::format_version_key(), [to.0]);
    batch.delete(&progress_key);
    batch.put(&complete_key, b"complete");
    raw.write(batch).await?;
    raw.flush().await?;
    db.close().await?;

    Ok(FormatMigrationReport {
        path,
        from,
        to,
        objects_migrated: processed,
        already_complete: false,
        max_objects_in_flight: MIGRATION_MAX_OBJECTS_IN_FLIGHT.load(Ordering::Relaxed),
    })
}
