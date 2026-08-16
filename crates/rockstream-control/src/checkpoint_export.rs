use std::collections::BTreeSet;
use std::sync::Arc;

use futures::StreamExt;
use object_store::{path::Path, ObjectStore};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use uuid::Uuid;

/// The export copy path is deliberately sequential: there is no queue behind
/// the single permit.
pub const MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT: usize = 1;
/// Maximum number of object names retained while scanning a source prefix.
pub const MAX_CHECKPOINT_EXPORT_SCAN_WINDOW: usize = 1024;
/// Maximum size of one object held by the object-store put API.
pub const MAX_CHECKPOINT_EXPORT_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHECKPOINT_EXPORT_RECORD_BYTES: u64 = 1024 * 1024;
const EXPORT_PREFIX: &str = "checkpoint-exports";

#[derive(Debug, Error)]
pub enum CheckpointExportError {
    #[error("RS-5035: checkpoint export is already in progress; next_steps: wait for the active export to finish and retry")]
    InFlight,
    #[error("RS-5035: checkpoint export integrity validation failed: {0}; next_steps: discard the incomplete generation and retry from the same committed checkpoint")]
    Integrity(String),
    #[error("RS-5035: checkpoint export object-store operation failed: {0}; next_steps: verify source and destination access, then retry the same generation")]
    ObjectStore(String),
    #[error("RS-5035: checkpoint export was interrupted at {0}; next_steps: retry the same generation so validated objects can be resumed")]
    Interrupted(&'static str),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointExportOutcome {
    pub checkpoint_id: u64,
    pub generation: String,
    pub object_count: u64,
    pub byte_count: u64,
    pub inventory_digest: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRestoreOutcome {
    pub checkpoint_id: u64,
    pub generation: String,
    pub object_count: u64,
    pub byte_count: u64,
    pub restored_shards: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GenerationRecord {
    checkpoint_id: CheckpointId,
    generation: String,
    checkpoint: ClusterCheckpoint,
    object_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct InventoryRecord {
    source: String,
    destination: String,
    byte_len: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CommitMarker {
    outcome: CheckpointExportOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BootstrapPointer {
    checkpoint_id: CheckpointId,
    generation: String,
}

#[derive(Clone, Debug)]
pub struct CheckpointExportService {
    permit: Arc<Semaphore>,
}

impl Default for CheckpointExportService {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointExportService {
    pub fn new() -> Self {
        Self {
            permit: Arc::new(Semaphore::new(MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT)),
        }
    }

    /// Export an exact, already-selected object list for one committed checkpoint.
    /// The iterator stays owned by the caller and is consumed one object at a time.
    pub async fn export_objects<I>(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        objects: I,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError>
    where
        I: IntoIterator<Item = Path>,
        I::IntoIter: ExactSizeIterator,
    {
        let generation = generation.into();
        validate_generation(&generation)?;
        let _permit = self
            .permit
            .clone()
            .try_acquire_owned()
            .map_err(|_| CheckpointExportError::InFlight)?;
        rockstream_types::metrics::set_checkpoint_export_objects_in_flight(1);
        let result = self
            .export_objects_inner(source, destination, checkpoint, generation, objects)
            .await;
        rockstream_types::metrics::set_checkpoint_export_objects_in_flight(0);
        result
    }

    /// Select the durable latest manifest before invoking the exact snapshot
    /// inventory callback. The callback therefore cannot select objects from a
    /// later in-progress checkpoint.
    pub async fn export_latest<F, I>(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        manifests: &crate::checkpoint_store::CheckpointManifestStore,
        generation: impl Into<String>,
        select_objects: F,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError>
    where
        F: FnOnce(&ClusterCheckpoint) -> I,
        I: IntoIterator<Item = Path>,
        I::IntoIter: ExactSizeIterator,
    {
        let checkpoint = manifests
            .load_latest_manifest()
            .await
            .map_err(CheckpointExportError::Integrity)?
            .ok_or_else(|| {
                CheckpointExportError::Integrity(
                    "no committed checkpoint manifest exists".to_string(),
                )
            })?;
        if rockstream_sim::buggify!("dr.export.after_m3_selection", 1.0) {
            return Err(CheckpointExportError::Interrupted(
                "dr.export.after_m3_selection",
            ));
        }
        self.export_objects(
            source,
            destination,
            checkpoint.clone(),
            generation,
            select_objects(&checkpoint),
        )
        .await
    }

    pub async fn export_latest_prefix(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        manifests: &crate::checkpoint_store::CheckpointManifestStore,
        generation: impl Into<String>,
        prefix: &Path,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        let checkpoint = manifests
            .load_latest_manifest()
            .await
            .map_err(CheckpointExportError::Integrity)?
            .ok_or_else(|| {
                CheckpointExportError::Integrity(
                    "no committed checkpoint manifest exists".to_string(),
                )
            })?;
        if rockstream_sim::buggify!("dr.export.after_m3_selection", 1.0) {
            return Err(CheckpointExportError::Interrupted(
                "dr.export.after_m3_selection",
            ));
        }
        let objects = Self::list_export_objects(&source, prefix).await?;
        self.export_objects(source, destination, checkpoint, generation, objects)
            .await
    }

    /// Export all objects currently visible below an immutable snapshot prefix.
    /// A snapshot prefix must be selected from the committed checkpoint before
    /// this method is called; a live mutable database prefix is not a snapshot.
    pub async fn export_prefix(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        prefix: &Path,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        let objects = Self::list_checkpoint_objects(&source, &checkpoint, prefix).await?;
        self.export_objects(source, destination, checkpoint, generation, objects)
            .await
    }

    async fn list_checkpoint_objects(
        source: &Arc<dyn ObjectStore>,
        checkpoint: &ClusterCheckpoint,
        prefix: &Path,
    ) -> Result<Vec<Path>, CheckpointExportError> {
        let mut objects = Self::list_export_objects(source, prefix).await?;
        objects.retain(|path| !is_shard_path(path));
        for (shard_id, shard_checkpoint) in &checkpoint.shards {
            let shard_path = format!("shards/{}", shard_id.0);
            let shard_objects = match shard_checkpoint.snapshot_id.as_deref() {
                Some(snapshot_id) => {
                    snapshot_object_paths(source.clone(), &shard_path, snapshot_id).await?
                }
                None => {
                    manifest_object_paths(
                        source.clone(),
                        &shard_path,
                        shard_checkpoint.shard_checkpoint_id,
                    )
                    .await?
                }
            };
            objects.extend(shard_objects);
            if objects.len() > MAX_CHECKPOINT_EXPORT_SCAN_WINDOW {
                return Err(CheckpointExportError::Integrity(format!(
                    "checkpoint object inventory exceeded MAX_CHECKPOINT_EXPORT_SCAN_WINDOW={MAX_CHECKPOINT_EXPORT_SCAN_WINDOW}"
                )));
            }
        }
        objects.sort();
        objects.dedup();
        Ok(objects)
    }

    async fn list_export_objects(
        source: &Arc<dyn ObjectStore>,
        prefix: &Path,
    ) -> Result<Vec<Path>, CheckpointExportError> {
        let mut listing = source.list(Some(prefix));
        let mut objects = Vec::new();
        while let Some(entry) = listing.next().await {
            let meta = match entry {
                Ok(meta) => meta,
                Err(error) => {
                    rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
                    return Err(CheckpointExportError::ObjectStore(error.to_string()));
                }
            };
            let name = meta.location.as_ref();
            if name.starts_with("checkpoint-exports/")
                || name.starts_with("restore-generations/")
                || name == "control/bootstrap/active-generation"
            {
                continue;
            }
            if objects.len() == MAX_CHECKPOINT_EXPORT_SCAN_WINDOW {
                rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
                return Err(CheckpointExportError::Integrity(format!(
                    "source prefix scan exceeded MAX_CHECKPOINT_EXPORT_SCAN_WINDOW={MAX_CHECKPOINT_EXPORT_SCAN_WINDOW}"
                )));
            }
            objects.push(meta.location);
            rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(
                objects.len() as u64
            );
        }
        rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
        objects.sort();
        Ok(objects)
    }

    /// Validate a committed generation without consulting the source control plane.
    pub async fn validate_generation(
        &self,
        destination: Arc<dyn ObjectStore>,
        generation: &str,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        validate_generation(generation)?;
        let marker = self
            .read_json::<CommitMarker>(&destination, &commit_path(generation))
            .await?
            .ok_or_else(|| {
                CheckpointExportError::Integrity("terminal commit marker is missing".to_string())
            })?;
        let outcome = marker.outcome;
        let generation_record = self
            .read_json::<GenerationRecord>(&destination, &generation_path(generation))
            .await?
            .ok_or_else(|| {
                CheckpointExportError::Integrity("generation record is missing".to_string())
            })?;
        if generation_record.generation != generation
            || outcome.generation != generation
            || generation_record.checkpoint_id.0 != outcome.checkpoint_id
            || generation_record.checkpoint.checkpoint_id != generation_record.checkpoint_id
            || generation_record.object_count != outcome.object_count
            || outcome.status != "SUCCESS"
        {
            return Err(CheckpointExportError::Integrity(
                "generation and terminal marker identify different checkpoints".to_string(),
            ));
        }
        if generation_record
            .checkpoint
            .shards
            .values()
            .any(|shard| shard.checkpoint_id != generation_record.checkpoint_id)
        {
            return Err(CheckpointExportError::Integrity(
                "shard and control checkpoint epochs do not match".to_string(),
            ));
        }

        let mut digest = Sha256::new();
        let mut byte_count = 0_u64;
        for index in 0..outcome.object_count {
            let record = self
                .read_json::<InventoryRecord>(&destination, &inventory_path(generation, index))
                .await?
                .ok_or_else(|| {
                    CheckpointExportError::Integrity(format!("inventory record {index} is missing"))
                })?;
            if record.destination != object_path(generation, index).to_string() {
                return Err(CheckpointExportError::Integrity(format!(
                    "inventory record {index} points outside the generation"
                )));
            }
            let bytes = self
                .read_bytes(&destination, &Path::from(record.destination.clone()))
                .await?;
            let actual_digest = digest_bytes(&bytes);
            if bytes.len() as u64 != record.byte_len || actual_digest != record.sha256 {
                return Err(CheckpointExportError::Integrity(format!(
                    "destination object {} failed validation",
                    record.destination
                )));
            }
            byte_count = byte_count.checked_add(record.byte_len).ok_or_else(|| {
                CheckpointExportError::Integrity("export byte count overflowed".to_string())
            })?;
            update_digest(&mut digest, &record)?;
        }
        if byte_count != outcome.byte_count
            || hex::encode(digest.finalize()) != outcome.inventory_digest
        {
            return Err(CheckpointExportError::Integrity(
                "inventory digest or byte total does not match the terminal marker".to_string(),
            ));
        }
        Ok(outcome)
    }

    pub async fn latest_committed_generation(
        &self,
        source: Arc<dyn ObjectStore>,
    ) -> Result<String, CheckpointExportError> {
        let mut listing = source.list(Some(&Path::from(EXPORT_PREFIX)));
        let mut latest: Option<(u64, String)> = None;
        let mut scanned = 0_usize;
        while let Some(entry) = listing.next().await {
            let meta =
                entry.map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
            scanned += 1;
            if scanned > MAX_CHECKPOINT_EXPORT_SCAN_WINDOW {
                return Err(CheckpointExportError::Integrity(format!(
                    "export generation scan exceeded MAX_CHECKPOINT_EXPORT_SCAN_WINDOW={MAX_CHECKPOINT_EXPORT_SCAN_WINDOW}"
                )));
            }
            let name = meta.location.as_ref();
            let Some(generation) = name
                .strip_prefix("checkpoint-exports/")
                .and_then(|rest| rest.strip_suffix("/commit"))
            else {
                continue;
            };
            let marker = self
                .read_json::<CommitMarker>(&source, &meta.location)
                .await?
                .ok_or_else(|| {
                    CheckpointExportError::Integrity(format!(
                        "terminal marker {} disappeared during selection",
                        meta.location
                    ))
                })?;
            if latest
                .as_ref()
                .is_none_or(|(checkpoint_id, _)| marker.outcome.checkpoint_id > *checkpoint_id)
            {
                latest = Some((marker.outcome.checkpoint_id, generation.to_string()));
            }
        }
        latest.map(|(_, generation)| generation).ok_or_else(|| {
            CheckpointExportError::Integrity("no committed export generation exists".to_string())
        })
    }

    pub async fn restore_generation(
        &self,
        source: Arc<dyn ObjectStore>,
        target: Arc<dyn ObjectStore>,
        generation: &str,
    ) -> Result<CheckpointRestoreOutcome, CheckpointExportError> {
        let _permit = self
            .permit
            .clone()
            .try_acquire_owned()
            .map_err(|_| CheckpointExportError::InFlight)?;
        rockstream_types::metrics::set_checkpoint_export_objects_in_flight(1);
        let result = self
            .restore_generation_inner(source, target, generation)
            .await;
        rockstream_types::metrics::set_checkpoint_export_objects_in_flight(0);
        result
    }

    async fn restore_generation_inner(
        &self,
        source: Arc<dyn ObjectStore>,
        target: Arc<dyn ObjectStore>,
        generation: &str,
    ) -> Result<CheckpointRestoreOutcome, CheckpointExportError> {
        let outcome = self.validate_generation(source.clone(), generation).await?;
        let generation_record = self
            .read_json::<GenerationRecord>(&source, &generation_path(generation))
            .await?
            .ok_or_else(|| {
                CheckpointExportError::Integrity("generation record is missing".to_string())
            })?;
        let pointer_path = Path::from("control/bootstrap/active-generation");
        if let Some(existing) = self
            .read_json::<BootstrapPointer>(&target, &pointer_path)
            .await?
        {
            if existing.checkpoint_id != generation_record.checkpoint_id
                || existing.generation != generation
            {
                return Err(CheckpointExportError::Integrity(
                    "target already has a different active generation".to_string(),
                ));
            }
            return Ok(CheckpointRestoreOutcome {
                checkpoint_id: outcome.checkpoint_id,
                generation: generation.to_string(),
                object_count: outcome.object_count,
                byte_count: outcome.byte_count,
                restored_shards: generation_record.checkpoint.shards.len(),
                status: "SUCCESS".to_string(),
            });
        }

        for index in 0..outcome.object_count {
            let record = self
                .read_json::<InventoryRecord>(&source, &inventory_path(generation, index))
                .await?
                .ok_or_else(|| {
                    CheckpointExportError::Integrity(format!("inventory record {index} is missing"))
                })?;
            let bytes = self
                .read_bytes(&source, &Path::from(record.destination.clone()))
                .await?;
            let staged = restore_object_path(generation, index);
            target
                .put(&staged, bytes.clone().into())
                .await
                .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
            let staged_bytes = self.read_bytes(&target, &staged).await?;
            if staged_bytes.len() as u64 != record.byte_len
                || digest_bytes(&staged_bytes) != record.sha256
            {
                return Err(CheckpointExportError::Integrity(format!(
                    "staged restore object {index} failed validation"
                )));
            }
        }

        for index in 0..outcome.object_count {
            let record = self
                .read_json::<InventoryRecord>(&source, &inventory_path(generation, index))
                .await?
                .ok_or_else(|| {
                    CheckpointExportError::Integrity(format!("inventory record {index} is missing"))
                })?;
            let staged = restore_object_path(generation, index);
            let bytes = self.read_bytes(&target, &staged).await?;
            target
                .put(&Path::from(record.source.clone()), bytes.into())
                .await
                .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
            let restored = self
                .read_bytes(&target, &Path::from(record.source.clone()))
                .await?;
            if restored.len() as u64 != record.byte_len || digest_bytes(&restored) != record.sha256
            {
                return Err(CheckpointExportError::Integrity(format!(
                    "restored target object {} failed validation",
                    record.source
                )));
            }
        }

        self.put_json(
            &target,
            &pointer_path,
            &BootstrapPointer {
                checkpoint_id: generation_record.checkpoint_id,
                generation: generation.to_string(),
            },
        )
        .await?;
        Ok(CheckpointRestoreOutcome {
            checkpoint_id: outcome.checkpoint_id,
            generation: generation.to_string(),
            object_count: outcome.object_count,
            byte_count: outcome.byte_count,
            restored_shards: generation_record.checkpoint.shards.len(),
            status: "SUCCESS".to_string(),
        })
    }

    async fn export_objects_inner<I>(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: String,
        objects: I,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError>
    where
        I: IntoIterator<Item = Path>,
        I::IntoIter: ExactSizeIterator,
    {
        match destination.head(&commit_path(&generation)).await {
            Ok(_) => {
                let outcome = self
                    .validate_generation(destination.clone(), &generation)
                    .await?;
                let existing = self
                    .read_json::<GenerationRecord>(&destination, &generation_path(&generation))
                    .await?
                    .ok_or_else(|| {
                        CheckpointExportError::Integrity("generation record is missing".to_string())
                    })?;
                if existing.checkpoint != checkpoint {
                    return Err(CheckpointExportError::Integrity(
                        "existing generation belongs to a different checkpoint".to_string(),
                    ));
                }
                return Ok(outcome);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(CheckpointExportError::ObjectStore(error.to_string())),
        }

        let objects = objects.into_iter();
        let total = objects.len() as u64;
        let generation_record = GenerationRecord {
            checkpoint_id: checkpoint.checkpoint_id,
            generation: generation.clone(),
            checkpoint,
            object_count: total,
        };
        let generation_location = generation_path(&generation);
        match self
            .read_json::<GenerationRecord>(&destination, &generation_location)
            .await?
        {
            Some(existing) if existing != generation_record => {
                return Err(CheckpointExportError::Integrity(
                    "existing generation record belongs to a different checkpoint".to_string(),
                ));
            }
            Some(_) => {}
            None => {
                self.put_json(&destination, &generation_location, &generation_record)
                    .await?
            }
        }

        let mut copied = 0_u64;
        let mut byte_count = 0_u64;
        let mut inventory_digest = Sha256::new();
        rockstream_types::metrics::set_checkpoint_export_copy_progress(0, total);
        for (index, source_path) in objects.enumerate() {
            let source_bytes = self.read_bytes(&source, &source_path).await?;
            let source_name = source_path.to_string();
            let destination_name = object_path(&generation, index as u64).to_string();
            let record = InventoryRecord {
                source: source_name,
                destination: destination_name,
                byte_len: source_bytes.len() as u64,
                sha256: digest_bytes(&source_bytes),
            };
            let record_path = inventory_path(&generation, index as u64);

            if let Some(existing) = self
                .read_json::<InventoryRecord>(&destination, &record_path)
                .await?
            {
                if existing != record {
                    return Err(CheckpointExportError::Integrity(format!(
                        "inventory record {index} does not match the selected checkpoint"
                    )));
                }
                let existing_bytes = self
                    .read_bytes(&destination, &Path::from(existing.destination.clone()))
                    .await?;
                if existing_bytes.len() as u64 != existing.byte_len
                    || digest_bytes(&existing_bytes) != existing.sha256
                {
                    return Err(CheckpointExportError::Integrity(format!(
                        "destination object {} failed validation during resume",
                        existing.destination
                    )));
                }
            } else {
                destination
                    .put(&Path::from(record.destination.clone()), source_bytes.into())
                    .await
                    .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
                let written = self
                    .read_bytes(&destination, &Path::from(record.destination.clone()))
                    .await?;
                if written.len() as u64 != record.byte_len
                    || digest_bytes(&written) != record.sha256
                {
                    return Err(CheckpointExportError::Integrity(format!(
                        "destination object {} failed post-copy validation",
                        record.destination
                    )));
                }
                self.put_json(&destination, &record_path, &record).await?;
            }
            byte_count = byte_count.checked_add(record.byte_len).ok_or_else(|| {
                CheckpointExportError::Integrity("export byte count overflowed".to_string())
            })?;
            copied += 1;
            update_digest(&mut inventory_digest, &record)?;
            rockstream_types::metrics::set_checkpoint_export_copy_progress(copied, total);
        }

        if rockstream_sim::buggify!("dr.export.before_terminal_marker", 1.0) {
            return Err(CheckpointExportError::Interrupted(
                "dr.export.before_terminal_marker",
            ));
        }
        let outcome = CheckpointExportOutcome {
            checkpoint_id: generation_record.checkpoint_id.0,
            generation,
            object_count: copied,
            byte_count,
            inventory_digest: hex::encode(inventory_digest.finalize()),
            status: "SUCCESS".to_string(),
        };
        self.put_json(
            &destination,
            &commit_path(&outcome.generation),
            &CommitMarker {
                outcome: outcome.clone(),
            },
        )
        .await?;
        Ok(outcome)
    }

    async fn read_bytes(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Result<bytes::Bytes, CheckpointExportError> {
        let result = store
            .get(path)
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
        if result.meta.size > MAX_CHECKPOINT_EXPORT_OBJECT_BYTES {
            return Err(CheckpointExportError::Integrity(format!(
                "object {path} exceeds MAX_CHECKPOINT_EXPORT_OBJECT_BYTES={MAX_CHECKPOINT_EXPORT_OBJECT_BYTES}"
            )));
        }
        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(result.meta.size);
        let bytes = result
            .bytes()
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()));
        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
        bytes
    }

    async fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Result<Option<T>, CheckpointExportError> {
        let Some(bytes) = (match store.get(path).await {
            Ok(result) => {
                if result.meta.size > MAX_CHECKPOINT_EXPORT_RECORD_BYTES {
                    return Err(CheckpointExportError::Integrity(format!(
                        "record {path} exceeds MAX_CHECKPOINT_EXPORT_RECORD_BYTES={MAX_CHECKPOINT_EXPORT_RECORD_BYTES}"
                    )));
                }
                rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(
                    result.meta.size,
                );
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()));
                rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                Some(bytes?)
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => return Err(CheckpointExportError::ObjectStore(error.to_string())),
        }) else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            CheckpointExportError::Integrity(format!("malformed {}: {error}", path))
        })
    }

    async fn put_json<T: Serialize>(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        value: &T,
    ) -> Result<(), CheckpointExportError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| CheckpointExportError::Integrity(error.to_string()))?;
        store
            .put(path, bytes.into())
            .await
            .map(|_| ())
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))
    }
}

fn generation_path(generation: &str) -> Path {
    Path::from(format!("{EXPORT_PREFIX}/{generation}/generation"))
}

fn is_shard_path(path: &Path) -> bool {
    let mut parts = path.as_ref().split('/');
    parts.next() == Some("shards") && parts.next().is_some()
}

async fn snapshot_object_paths(
    source: Arc<dyn ObjectStore>,
    shard_path: &str,
    snapshot_id: &str,
) -> Result<Vec<Path>, CheckpointExportError> {
    let checkpoint_id = Uuid::parse_str(snapshot_id).map_err(|error| {
        CheckpointExportError::Integrity(format!(
            "invalid SlateDB snapshot id `{snapshot_id}`: {error}"
        ))
    })?;
    let admin = slatedb::admin::AdminBuilder::new(Path::from(shard_path), source.clone()).build();
    let checkpoint = admin
        .list_checkpoints(None)
        .await
        .map_err(|error| CheckpointExportError::Integrity(error.to_string()))?
        .into_iter()
        .find(|checkpoint| checkpoint.id == checkpoint_id)
        .ok_or_else(|| {
            CheckpointExportError::Integrity(format!(
                "SlateDB snapshot `{snapshot_id}` is not present in shard `{shard_path}`"
            ))
        })?;
    manifest_object_paths(source, shard_path, checkpoint.manifest_id).await
}

async fn manifest_object_paths(
    source: Arc<dyn ObjectStore>,
    shard_path: &str,
    manifest_id: u64,
) -> Result<Vec<Path>, CheckpointExportError> {
    let admin = slatedb::admin::AdminBuilder::new(Path::from(shard_path), source).build();
    let manifest = admin
        .read_manifest(Some(manifest_id))
        .await
        .map_err(|error| CheckpointExportError::Integrity(error.to_string()))?
        .ok_or_else(|| {
            CheckpointExportError::Integrity(format!(
                "SlateDB manifest {manifest_id} for shard `{shard_path}` is missing"
            ))
        })?;

    let mut objects = BTreeSet::new();
    objects.insert(Path::from(format!(
        "{shard_path}/manifest/{:020}.manifest",
        manifest_id
    )));
    let mut add_view = |view: &slatedb::manifest::SsTableView, root: &str| {
        let path = match view.sst.id {
            slatedb::manifest::SsTableId::Wal(id) => {
                format!("{root}/wal/{id:020}.sst")
            }
            slatedb::manifest::SsTableId::Compacted(id) => {
                format!("{root}/compacted/{id}.sst")
            }
        };
        objects.insert(Path::from(path));
    };
    for view in manifest.l0() {
        add_view(view, shard_path);
    }
    for run in manifest.compacted() {
        for view in &run.sst_views {
            add_view(view, shard_path);
        }
    }
    for segment in manifest.segments() {
        for view in segment.l0() {
            add_view(view, shard_path);
        }
        for run in segment.compacted() {
            for view in &run.sst_views {
                add_view(view, shard_path);
            }
        }
    }
    for external in manifest.external_dbs() {
        for id in &external.sst_ids {
            let path = match id {
                slatedb::manifest::SsTableId::Wal(id) => {
                    format!("{}/wal/{id:020}.sst", external.path)
                }
                slatedb::manifest::SsTableId::Compacted(id) => {
                    format!("{}/compacted/{id}.sst", external.path)
                }
            };
            objects.insert(Path::from(path));
        }
    }
    Ok(objects.into_iter().collect())
}

fn validate_generation(generation: &str) -> Result<(), CheckpointExportError> {
    if generation.is_empty()
        || generation == "."
        || generation == ".."
        || generation.contains('/')
        || generation.contains('\\')
    {
        return Err(CheckpointExportError::Integrity(
            "generation must be a non-empty path segment".to_string(),
        ));
    }
    Ok(())
}

fn inventory_path(generation: &str, index: u64) -> Path {
    Path::from(format!(
        "{EXPORT_PREFIX}/{generation}/inventory/{index:020}"
    ))
}

fn object_path(generation: &str, index: u64) -> Path {
    Path::from(format!("{EXPORT_PREFIX}/{generation}/objects/{index:020}"))
}

fn restore_object_path(generation: &str, index: u64) -> Path {
    Path::from(format!(
        "restore-generations/{generation}/objects/{index:020}"
    ))
}

fn commit_path(generation: &str) -> Path {
    Path::from(format!("{EXPORT_PREFIX}/{generation}/commit"))
}

fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn update_digest(
    digest: &mut Sha256,
    record: &InventoryRecord,
) -> Result<(), CheckpointExportError> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| CheckpointExportError::Integrity(error.to_string()))?;
    digest.update(bytes);
    digest.update(*b"\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};

    use super::*;

    fn checkpoint() -> ClusterCheckpoint {
        ClusterCheckpoint::new(CheckpointId(7))
    }

    #[tokio::test]
    async fn checkpoint_export_copies_sequentially_and_publishes_marker_last() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        source
            .put(
                &Path::from("shards/1/manifest"),
                bytes::Bytes::from_static(b"m1").into(),
            )
            .await
            .unwrap();
        source
            .put(
                &Path::from("shards/1/sst/0"),
                bytes::Bytes::from_static(b"sst").into(),
            )
            .await
            .unwrap();

        let outcome = CheckpointExportService::new()
            .export_objects(
                source,
                destination.clone(),
                checkpoint(),
                "generation-a",
                vec![
                    Path::from("shards/1/manifest"),
                    Path::from("shards/1/sst/0"),
                ],
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            CheckpointExportOutcome {
                checkpoint_id: 7,
                generation: "generation-a".to_string(),
                object_count: 2,
                byte_count: 5,
                inventory_digest:
                    "316eca1bfe63598110dc7a119b2a000d99766ba59f2359d3a1cf3147d27cfc34".to_string(),
                status: "SUCCESS".to_string(),
            }
        );
        assert!(destination
            .get(&Path::from("checkpoint-exports/generation-a/commit"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn checkpoint_export_resume_rejects_corrupt_existing_object() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("shards/1/manifest");
        source
            .put(&path, bytes::Bytes::from_static(b"manifest").into())
            .await
            .unwrap();
        let service = CheckpointExportService::new();
        service
            .export_objects(
                source.clone(),
                destination.clone(),
                checkpoint(),
                "generation-b",
                vec![path.clone()],
            )
            .await
            .unwrap();
        destination
            .put(
                &Path::from("checkpoint-exports/generation-b/objects/00000000000000000000"),
                bytes::Bytes::from_static(b"corrupt").into(),
            )
            .await
            .unwrap();

        let error = service
            .export_objects(
                source,
                destination,
                checkpoint(),
                "generation-b",
                vec![path],
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-5035: checkpoint export integrity validation failed: destination object checkpoint-exports/generation-b/objects/00000000000000000000 failed validation; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
        );
    }

    #[tokio::test]
    async fn checkpoint_export_prefix_rejects_an_unbounded_scan() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for index in 0..=MAX_CHECKPOINT_EXPORT_SCAN_WINDOW {
            source
                .put(
                    &Path::from(format!("shards/1/{index}")),
                    bytes::Bytes::from_static(b"x").into(),
                )
                .await
                .unwrap();
        }

        let error = CheckpointExportService::new()
            .export_prefix(
                source,
                destination,
                checkpoint(),
                "generation-c",
                &Path::from("shards/1"),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-5035: checkpoint export integrity validation failed: source prefix scan exceeded MAX_CHECKPOINT_EXPORT_SCAN_WINDOW=1024; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
        );
        assert_eq!(
            rockstream_types::metrics::read_checkpoint_export_scan_window_fill_level(),
            0
        );
    }

    #[test]
    fn checkpoint_export_limits_are_named_and_exact() {
        assert_eq!(MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT, 1);
        assert_eq!(MAX_CHECKPOINT_EXPORT_SCAN_WINDOW, 1024);
        assert_eq!(MAX_CHECKPOINT_EXPORT_OBJECT_BYTES, 64 * 1024 * 1024);
    }

    #[tokio::test]
    async fn checkpoint_export_rejects_path_traversal_generation() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let error = CheckpointExportService::new()
            .export_objects(
                source,
                destination,
                checkpoint(),
                "../generation",
                Vec::<Path>::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-5035: checkpoint export integrity validation failed: generation must be a non-empty path segment; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
        );
    }

    #[test]
    fn checkpoint_export_source_has_no_range_delete_path() {
        let source = std::fs::read_to_string(format!(
            "{}/src/checkpoint_export.rs",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(!production.contains("range_delete"));
        assert!(!production.contains("delete_range"));
    }
}
