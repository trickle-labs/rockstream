use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::StreamExt;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use uuid::Uuid;

/// The export copy path is deliberately sequential: there is no queue behind
/// the single permit.
pub const MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT: usize = 1;
/// Per-page scan window size used to bound resident scan progress metrics while
/// paginating across arbitrarily large object inventories.
pub const MAX_CHECKPOINT_EXPORT_SCAN_WINDOW: usize = 1024;
/// Streaming chunk size (8 MiB) used for bounded-memory multipart upload/copy
/// and streaming SHA-256 verification. Satisfies S3's 5 MiB minimum part size
/// while supporting arbitrarily large SSTables beyond 64 MiB.
pub const CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Legacy single-buffer object threshold retained for API compatibility; objects
/// larger than [`CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES`] are streamed in bounded
/// chunks rather than rejected.
pub const MAX_CHECKPOINT_EXPORT_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHECKPOINT_EXPORT_RECORD_BYTES: u64 = 1024 * 1024;
const EXPORT_PREFIX: &str = "checkpoint-exports";

#[derive(Debug, Error)]
pub enum CheckpointExportError {
    #[error("RS-3613: checkpoint export is already in progress; next_steps: wait for the active export to finish and retry")]
    InFlight,
    #[error("RS-3614: checkpoint export integrity validation failed: {0}; next_steps: discard the incomplete generation and retry from the same committed checkpoint")]
    Integrity(String),
    #[error("RS-3614: checkpoint export object-store operation failed: {0}; next_steps: verify source and destination access, then retry the same generation")]
    ObjectStore(String),
    #[error("RS-3614: checkpoint export was interrupted at {0}; next_steps: retry the same generation so validated objects can be resumed")]
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
    /// Legacy hand-picked exports need not contain a durable control manifest.
    /// Prefix exports must contain exactly the selected durable manifest.
    #[serde(default)]
    requires_checkpoint_manifest: bool,
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

#[derive(Default)]
struct ExportGuard {
    require_latest: bool,
    require_manifest: bool,
    metadata_baseline: Option<(Path, BTreeMap<Path, String>)>,
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

    /// Export an exact, caller-selected object list. Unlike prefix exports,
    /// this low-level API does not discover or require a control manifest.
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
        self.export_objects_with_guard(
            source,
            destination,
            checkpoint,
            generation,
            objects,
            ExportGuard::default(),
        )
        .await
    }

    async fn export_objects_with_guard<I>(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        objects: I,
        guard: ExportGuard,
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
            .export_objects_inner(source, destination, checkpoint, generation, objects, guard)
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
        self.export_objects_with_guard(
            source,
            destination,
            checkpoint.clone(),
            generation,
            select_objects(&checkpoint),
            ExportGuard {
                require_latest: true,
                ..Default::default()
            },
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
        self.export_checkpoint_prefix(source, destination, checkpoint, generation, prefix)
            .await
    }

    /// Export a checkpoint already selected by the caller (e.g. the CLI when
    /// constructing its checkpoint-named generation). Do not reselect here:
    /// the name, generation record, manifest and shard snapshot must agree.
    ///
    /// Live, unversioned control metadata is only safe while this exact
    /// committed checkpoint stays latest and the copied metadata stays
    /// unchanged. A concurrent publication fails before the commit marker.
    pub async fn export_checkpoint_prefix(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        prefix: &Path,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        self.export_checkpoint_prefix_inner(
            source,
            destination,
            checkpoint,
            generation,
            prefix,
            true,
        )
        .await
    }

    async fn export_checkpoint_prefix_inner(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        prefix: &Path,
        require_latest: bool,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        if require_latest {
            let latest = crate::checkpoint_store::CheckpointManifestStore::new(source.clone())
                .load_latest_manifest()
                .await
                .map_err(CheckpointExportError::Integrity)?;
            if latest.as_ref() != Some(&checkpoint) {
                return Err(CheckpointExportError::Integrity(
                    "source committed checkpoint changed before inventory; retry with the newly selected checkpoint"
                        .to_string(),
                ));
            }
        }
        // Pin the actual manifest and shard objects to the selected epoch.
        // Live control manifests (and changelog contributions) of other
        // epochs must never become independently discoverable after restore.
        // Snapshot unversioned catalog/source metadata *before* the live root
        // scan. Membership and bytes must remain stable through the export,
        // otherwise a concurrent writer could smuggle new control state into A.
        let metadata_baseline = self.snapshot_live_metadata(&source, prefix).await?;
        let objects = Self::list_checkpoint_objects(&source, &checkpoint, prefix).await?;
        let inventoried_metadata: BTreeSet<&Path> = objects
            .iter()
            .filter(|path| is_unversioned_export_metadata(path))
            .collect();
        let selected_metadata: BTreeSet<&Path> = metadata_baseline.keys().collect();
        if inventoried_metadata != selected_metadata {
            return Err(CheckpointExportError::Integrity(
                "unversioned control/catalog/source metadata changed between checkpoint selection and inventory"
                    .to_string(),
            ));
        }
        self.export_objects_with_guard(
            source,
            destination,
            checkpoint,
            generation,
            objects,
            ExportGuard {
                require_latest,
                require_manifest: true,
                metadata_baseline: Some((prefix.clone(), metadata_baseline)),
            },
        )
        .await
    }

    /// Export a checkpoint selected from an immutable snapshot prefix. A
    /// prefix export also requires the matching committed control manifest,
    /// so a live prefix cannot accidentally mix checkpoint generations.
    pub async fn export_prefix(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: impl Into<String>,
        prefix: &Path,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError> {
        self.export_checkpoint_prefix_inner(
            source,
            destination,
            checkpoint,
            generation,
            prefix,
            false,
        )
        .await
    }

    /// Capture mutable control/catalog/source names and bytes before the root
    /// inventory can observe a subsequently committed checkpoint. An immutable
    /// checkpoint manifest is authoritative, but the remaining legacy control
    /// paths have no epoch qualifier, so instability must fail closed.
    async fn snapshot_live_metadata(
        &self,
        source: &Arc<dyn ObjectStore>,
        prefix: &Path,
    ) -> Result<BTreeMap<Path, String>, CheckpointExportError> {
        let path = prefix.as_ref();
        let roots = if path.is_empty() {
            vec![
                Path::from("control"),
                Path::from("topology"),
                Path::from("catalog"),
                Path::from("sources"),
            ]
        } else if ["control", "topology", "catalog", "sources"]
            .iter()
            .any(|root| path == *root || path.starts_with(&format!("{root}/")))
        {
            vec![prefix.clone()]
        } else {
            Vec::new()
        };
        let mut snapshot = BTreeMap::new();
        let mut page_fill = 0_usize;
        for root in roots {
            let mut listing = source.list(Some(&root));
            while let Some(entry) = listing.next().await {
                let meta = match entry {
                    Ok(meta) => meta,
                    Err(error) => {
                        rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
                        return Err(CheckpointExportError::ObjectStore(error.to_string()));
                    }
                };
                page_fill = (page_fill % MAX_CHECKPOINT_EXPORT_SCAN_WINDOW) + 1;
                rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(
                    page_fill as u64,
                );
                if !is_unversioned_export_metadata(&meta.location)
                    || is_uncheckpointed_operational_path(&meta.location)
                    || meta.location.as_ref() == "control/bootstrap/active-generation"
                {
                    continue;
                }
                let (_, digest) = match self.stream_hash_and_size(source, &meta.location).await {
                    Ok(result) => result,
                    Err(error) => {
                        rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
                        return Err(error);
                    }
                };
                snapshot.insert(meta.location, digest);
            }
        }
        rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
        Ok(snapshot)
    }

    async fn list_checkpoint_objects(
        source: &Arc<dyn ObjectStore>,
        checkpoint: &ClusterCheckpoint,
        prefix: &Path,
    ) -> Result<Vec<Path>, CheckpointExportError> {
        let mut objects = Self::list_export_objects(source, prefix).await?;
        objects.retain(|path| {
            !is_shard_path(path)
                && !is_workload_shard_path(path)
                && !is_uncheckpointed_operational_path(path)
                && is_selected_checkpoint_metadata(path, checkpoint)
        });
        // Always include the *selected* control manifest, even if prefix was
        // scoped to a shard. The root listing must not determine its identity.
        objects.push(Path::from(format!(
            "control/checkpoints/{}",
            checkpoint.checkpoint_id.0
        )));
        for (shard_id, shard_checkpoint) in &checkpoint.shards {
            let shard_path = shard_checkpoint
                .shard_path
                .clone()
                .unwrap_or_else(|| format!("shards/{}", shard_id.0));
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
        let mut page_fill = 0_usize;
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
            objects.push(meta.location);
            page_fill = (page_fill % MAX_CHECKPOINT_EXPORT_SCAN_WINDOW) + 1;
            rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(
                page_fill as u64,
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
        let mut seen_sources = BTreeSet::new();
        let mut selected_manifest_present = false;
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
            if !seen_sources.insert(record.source.clone()) {
                return Err(CheckpointExportError::Integrity(format!(
                    "duplicate restored source path {}",
                    record.source
                )));
            }
            if record.source.starts_with("control/checkpoints/")
                || record.source.starts_with("control/changelog-checkpoints/")
            {
                let bytes = self
                    .read_bytes(&destination, &Path::from(record.destination.clone()))
                    .await?;
                selected_manifest_present |= validate_checkpoint_metadata_record(
                    &record.source,
                    &bytes,
                    &generation_record.checkpoint,
                )?;
            }
            let (actual_len, actual_digest) = self
                .stream_hash_and_size(&destination, &Path::from(record.destination.clone()))
                .await?;
            if actual_len != record.byte_len || actual_digest != record.sha256 {
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
        if generation_record.requires_checkpoint_manifest && !selected_manifest_present {
            return Err(CheckpointExportError::Integrity(
                "selected control checkpoint manifest is missing from export inventory".to_string(),
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
        while let Some(entry) = listing.next().await {
            let meta =
                entry.map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
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
        // A target may be partially populated even without an active pointer.
        // Publishing A over a pre-existing B would make recovery select B.
        self.reject_other_target_manifests(&target, &generation_record.checkpoint)
            .await?;
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
            self.assert_restored_checkpoint(&target, &generation_record.checkpoint)
                .await?;
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
            let staged = restore_object_path(generation, index);
            let (copied_len, copied_digest) = self
                .stream_copy_and_hash(
                    &source,
                    &Path::from(record.destination.clone()),
                    &target,
                    &staged,
                )
                .await?;
            if copied_len != record.byte_len || copied_digest != record.sha256 {
                return Err(CheckpointExportError::Integrity(format!(
                    "source object {} failed validation during restore staging",
                    record.destination
                )));
            }
            let (staged_len, staged_digest) = self.stream_hash_and_size(&target, &staged).await?;
            if staged_len != record.byte_len || staged_digest != record.sha256 {
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
            let target_path = Path::from(record.source.clone());
            self.stream_copy_and_hash(&target, &staged, &target, &target_path)
                .await?;
            let (restored_len, restored_digest) =
                self.stream_hash_and_size(&target, &target_path).await?;
            if restored_len != record.byte_len || restored_digest != record.sha256 {
                return Err(CheckpointExportError::Integrity(format!(
                    "restored target object {} failed validation",
                    record.source
                )));
            }
        }

        // Check the exact durable discovery path a fresh recovery will use
        // before publishing the bootstrap pointer and SUCCESS.
        self.assert_restored_checkpoint(&target, &generation_record.checkpoint)
            .await?;
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

    async fn reject_other_target_manifests(
        &self,
        target: &Arc<dyn ObjectStore>,
        checkpoint: &ClusterCheckpoint,
    ) -> Result<(), CheckpointExportError> {
        let expected_path = format!("control/checkpoints/{}", checkpoint.checkpoint_id.0);
        let mut listing = target.list(Some(&Path::from("control/checkpoints")));
        let mut scanned = 0_usize;
        while let Some(entry) = listing.next().await {
            let meta =
                entry.map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
            scanned += 1;
            if scanned > MAX_CHECKPOINT_EXPORT_SCAN_WINDOW {
                return Err(CheckpointExportError::Integrity(format!(
                    "restore target manifest scan exceeded MAX_CHECKPOINT_EXPORT_SCAN_WINDOW={MAX_CHECKPOINT_EXPORT_SCAN_WINDOW}"
                )));
            }
            if meta.location.as_ref() != expected_path {
                return Err(CheckpointExportError::Integrity(format!(
                    "restore target contains another discoverable checkpoint manifest: {}",
                    meta.location
                )));
            }
        }
        Ok(())
    }

    async fn assert_restored_checkpoint(
        &self,
        target: &Arc<dyn ObjectStore>,
        checkpoint: &ClusterCheckpoint,
    ) -> Result<(), CheckpointExportError> {
        let discovered = crate::checkpoint_store::CheckpointManifestStore::new(target.clone())
            .load_latest_manifest()
            .await
            .map_err(CheckpointExportError::Integrity)?;
        if discovered.as_ref() != Some(checkpoint) {
            return Err(CheckpointExportError::Integrity(
                "restored latest control checkpoint does not match export generation".to_string(),
            ));
        }
        Ok(())
    }

    async fn export_objects_inner<I>(
        &self,
        source: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        checkpoint: ClusterCheckpoint,
        generation: String,
        objects: I,
        guard: ExportGuard,
    ) -> Result<CheckpointExportOutcome, CheckpointExportError>
    where
        I: IntoIterator<Item = Path>,
        I::IntoIter: ExactSizeIterator,
    {
        let ExportGuard {
            require_latest,
            require_manifest,
            metadata_baseline,
        } = guard;
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
            requires_checkpoint_manifest: require_manifest,
        };
        let generation_location = generation_path(&generation);
        match self
            .read_json::<GenerationRecord>(&destination, &generation_location)
            .await?
        {
            Some(existing) if existing != generation_record => {
                if existing.checkpoint_id == generation_record.checkpoint_id
                    && existing.object_count == generation_record.object_count
                {
                    return Err(CheckpointExportError::InFlight);
                }
                return Err(CheckpointExportError::Integrity(
                    format!(
                        "existing generation record conflicts with retry: checkpoint_equal={}, checkpoint_ids={}/{}, object_counts={}/{}",
                        existing.checkpoint == generation_record.checkpoint,
                        existing.checkpoint_id.0,
                        generation_record.checkpoint_id.0,
                        existing.object_count,
                        generation_record.object_count,
                    ),
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
        let mut selected_manifest_present = false;
        let mut mutable_metadata = Vec::new();
        rockstream_types::metrics::set_checkpoint_export_copy_progress(0, total);
        for (index, source_path) in objects.enumerate() {
            if require_manifest
                && (source_path.as_ref().starts_with("control/checkpoints/")
                    || source_path
                        .as_ref()
                        .starts_with("control/changelog-checkpoints/"))
            {
                let source_bytes = self.read_bytes(&source, &source_path).await?;
                selected_manifest_present |= validate_checkpoint_metadata_record(
                    source_path.as_ref(),
                    &source_bytes,
                    &generation_record.checkpoint,
                )?;
            }
            if require_latest && is_unversioned_export_metadata(&source_path) {
                let (_, source_digest) = self.stream_hash_and_size(&source, &source_path).await?;
                if let Some((_, baseline)) = &metadata_baseline {
                    if baseline.get(&source_path) != Some(&source_digest) {
                        return Err(CheckpointExportError::Integrity(format!(
                            "unversioned checkpoint metadata {source_path} changed since checkpoint selection"
                        )));
                    }
                }
                mutable_metadata.push((source_path.clone(), source_digest));
            }
            let source_name = source_path.to_string();
            let dest_path = object_path(&generation, index as u64);
            let destination_name = dest_path.to_string();
            let record_path = inventory_path(&generation, index as u64);

            let record = if let Some(existing) = self
                .read_json::<InventoryRecord>(&destination, &record_path)
                .await?
            {
                let (source_len, source_sha) =
                    self.stream_hash_and_size(&source, &source_path).await?;
                let expected = InventoryRecord {
                    source: source_name,
                    destination: destination_name,
                    byte_len: source_len,
                    sha256: source_sha,
                };
                if existing != expected {
                    return Err(CheckpointExportError::Integrity(format!(
                        "inventory record {index} for source {} does not match the selected checkpoint",
                        expected.source
                    )));
                }
                let (existing_len, existing_sha) = self
                    .stream_hash_and_size(&destination, &Path::from(existing.destination.clone()))
                    .await?;
                if existing_len != existing.byte_len || existing_sha != existing.sha256 {
                    return Err(CheckpointExportError::Integrity(format!(
                        "destination object {} failed validation during resume",
                        existing.destination
                    )));
                }
                existing
            } else {
                let (copied_len, copied_sha) = self
                    .stream_copy_and_hash(&source, &source_path, &destination, &dest_path)
                    .await?;
                let record = InventoryRecord {
                    source: source_name,
                    destination: destination_name,
                    byte_len: copied_len,
                    sha256: copied_sha,
                };
                let (written_len, written_sha) =
                    self.stream_hash_and_size(&destination, &dest_path).await?;
                if written_len != record.byte_len || written_sha != record.sha256 {
                    return Err(CheckpointExportError::Integrity(format!(
                        "destination object {} failed post-copy validation",
                        record.destination
                    )));
                }
                self.put_json(&destination, &record_path, &record).await?;
                record
            };
            byte_count = byte_count.checked_add(record.byte_len).ok_or_else(|| {
                CheckpointExportError::Integrity("export byte count overflowed".to_string())
            })?;
            copied += 1;
            update_digest(&mut inventory_digest, &record)?;
            rockstream_types::metrics::set_checkpoint_export_copy_progress(copied, total);
        }

        if require_manifest && !selected_manifest_present {
            return Err(CheckpointExportError::Integrity(
                "selected control checkpoint manifest is missing from export inventory".to_string(),
            ));
        }
        if let Some((prefix, baseline)) = &metadata_baseline {
            let current = self.snapshot_live_metadata(&source, prefix).await?;
            if &current != baseline {
                return Err(CheckpointExportError::Integrity(
                    "unversioned control/catalog/source metadata changed during export".to_string(),
                ));
            }
        } else {
            for (path, expected_digest) in mutable_metadata {
                let (_, current_digest) = self.stream_hash_and_size(&source, &path).await?;
                if current_digest != expected_digest {
                    return Err(CheckpointExportError::Integrity(format!(
                        "unversioned checkpoint metadata {path} changed during export"
                    )));
                }
            }
        }
        if require_latest {
            // Re-scan membership *and bytes* of unversioned metadata. Merely
            // checking the copied paths would miss newly added source/catalog
            // records after inventory. The durable checkpoint must also still
            // be the one selected before the scan.
            let latest = crate::checkpoint_store::CheckpointManifestStore::new(source.clone())
                .load_latest_manifest()
                .await
                .map_err(CheckpointExportError::Integrity)?;
            if latest.as_ref() != Some(&generation_record.checkpoint) {
                return Err(CheckpointExportError::Integrity(
                    "source committed checkpoint changed during export; no terminal marker published"
                        .to_string(),
                ));
            }
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

    async fn stream_hash_and_size(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Result<(u64, String), CheckpointExportError> {
        let result = store
            .get(path)
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
        let mut stream = result.into_stream();
        let mut hasher = Sha256::new();
        let mut total_bytes = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                    return Err(CheckpointExportError::ObjectStore(error.to_string()));
                }
            };
            let chunk_len = chunk.len() as u64;
            rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(chunk_len);
            hasher.update(&chunk);
            total_bytes = total_bytes.checked_add(chunk_len).ok_or_else(|| {
                rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                CheckpointExportError::Integrity("object byte length overflowed".to_string())
            })?;
        }
        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
        Ok((total_bytes, hex::encode(hasher.finalize())))
    }

    async fn stream_copy_and_hash(
        &self,
        source: &Arc<dyn ObjectStore>,
        source_path: &Path,
        destination: &Arc<dyn ObjectStore>,
        dest_path: &Path,
    ) -> Result<(u64, String), CheckpointExportError> {
        let result = source
            .get(source_path)
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
        if result.meta.size <= CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES as u64 {
            rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(
                result.meta.size,
            );
            let bytes = match result.bytes().await {
                Ok(bytes) => bytes,
                Err(error) => {
                    rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                    return Err(CheckpointExportError::ObjectStore(error.to_string()));
                }
            };
            let byte_len = bytes.len() as u64;
            let sha = digest_bytes(&bytes);
            let put_result = destination
                .put(dest_path, bytes.into())
                .await
                .map(|_| ())
                .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()));
            rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
            put_result?;
            return Ok((byte_len, sha));
        }

        let mut upload = destination
            .put_multipart(dest_path)
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
        let mut stream = result.into_stream();
        let mut hasher = Sha256::new();
        let mut total_bytes = 0_u64;
        let mut buffer = Vec::with_capacity(CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES);

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = upload.abort().await;
                    rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                    return Err(CheckpointExportError::ObjectStore(error.to_string()));
                }
            };
            let mut slice = chunk.as_ref();
            while !slice.is_empty() {
                let remaining = CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES - buffer.len();
                let take = remaining.min(slice.len());
                buffer.extend_from_slice(&slice[..take]);
                hasher.update(&slice[..take]);
                total_bytes = match total_bytes.checked_add(take as u64) {
                    Some(total) => total,
                    None => {
                        let _ = upload.abort().await;
                        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(
                            0,
                        );
                        return Err(CheckpointExportError::Integrity(
                            "object byte length overflowed".to_string(),
                        ));
                    }
                };
                slice = &slice[take..];
                rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(
                    buffer.len() as u64,
                );
                if buffer.len() == CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES {
                    let part = std::mem::replace(
                        &mut buffer,
                        Vec::with_capacity(CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES),
                    );
                    if let Err(error) = upload.put_part(part.into()).await {
                        let _ = upload.abort().await;
                        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(
                            0,
                        );
                        return Err(CheckpointExportError::ObjectStore(error.to_string()));
                    }
                    rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                }
            }
        }
        if !buffer.is_empty() {
            if let Err(error) = upload.put_part(buffer.into()).await {
                let _ = upload.abort().await;
                rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
                return Err(CheckpointExportError::ObjectStore(error.to_string()));
            }
        }
        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(0);
        if let Err(error) = upload.complete().await {
            let _ = upload.abort().await;
            return Err(CheckpointExportError::ObjectStore(error.to_string()));
        }
        Ok((total_bytes, hex::encode(hasher.finalize())))
    }

    async fn read_bytes(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Result<Vec<u8>, CheckpointExportError> {
        let result = store
            .get(path)
            .await
            .map_err(|error| CheckpointExportError::ObjectStore(error.to_string()))?;
        if result.meta.size > MAX_CHECKPOINT_EXPORT_RECORD_BYTES {
            return Err(CheckpointExportError::Integrity(format!(
                "record {path} exceeds MAX_CHECKPOINT_EXPORT_RECORD_BYTES={MAX_CHECKPOINT_EXPORT_RECORD_BYTES}"
            )));
        }
        rockstream_types::metrics::set_checkpoint_export_object_buffer_fill_level(result.meta.size);
        let bytes = result
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
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

fn is_workload_shard_path(path: &Path) -> bool {
    let mut parts = path.as_ref().split('/');
    matches!(
        (parts.next(), parts.next()),
        (Some(workload), Some(shard))
            if workload.starts_with("workload-") && shard.starts_with("shard-")
    )
}

fn is_uncheckpointed_operational_path(path: &Path) -> bool {
    let path = path.as_ref();
    path == "audit.jsonl"
        || path.starts_with("control/management-operations/")
        || path.starts_with("control/gc-pins/")
        || path.starts_with("topology/workers/")
}

fn is_selected_checkpoint_metadata(path: &Path, checkpoint: &ClusterCheckpoint) -> bool {
    let name = path.as_ref();
    if name.starts_with("control/checkpoints/") {
        return name == format!("control/checkpoints/{}", checkpoint.checkpoint_id.0);
    }
    if let Some(suffix) = name.strip_prefix("control/changelog-checkpoints/") {
        return suffix.split_once('-').is_some_and(|(id, shard)| {
            id.parse::<u64>() == Ok(checkpoint.checkpoint_id.0)
                && shard.parse::<u64>().ok().is_some_and(|shard| {
                    checkpoint
                        .shards
                        .contains_key(&rockstream_types::ids::ShardId(shard))
                })
        });
    }
    true
}

fn is_unversioned_export_metadata(path: &Path) -> bool {
    let name = path.as_ref();
    (name.starts_with("control/")
        && !name.starts_with("control/checkpoints/")
        && !name.starts_with("control/changelog-checkpoints/"))
        || name.starts_with("catalog/")
        || name.starts_with("sources/")
        || name.starts_with("topology/")
}

/// Check both the restore-selected control manifest and epoch-qualified
/// changelog records. This is called on source bytes before committing and
/// again on exported bytes during offline validation/restore.
fn validate_checkpoint_metadata_record(
    path: &str,
    bytes: &[u8],
    checkpoint: &ClusterCheckpoint,
) -> Result<bool, CheckpointExportError> {
    if path.starts_with("control/checkpoints/") {
        if path != format!("control/checkpoints/{}", checkpoint.checkpoint_id.0) {
            return Err(CheckpointExportError::Integrity(format!(
                "export contains a control manifest other than selected checkpoint {}: {path}",
                checkpoint.checkpoint_id.0
            )));
        }
        let restored = crate::checkpoint_store::decode_manifest(bytes).map_err(|error| {
            CheckpointExportError::Integrity(format!(
                "exported control checkpoint manifest is malformed: {error}"
            ))
        })?;
        if restored != *checkpoint {
            return Err(CheckpointExportError::Integrity(
                "exported control checkpoint manifest differs from the selected checkpoint"
                    .to_string(),
            ));
        }
        return Ok(true);
    }
    if let Some(suffix) = path.strip_prefix("control/changelog-checkpoints/") {
        if !is_selected_checkpoint_metadata(&Path::from(path), checkpoint) {
            return Err(CheckpointExportError::Integrity(format!(
                "export contains a changelog contribution outside selected checkpoint {}: {path}",
                checkpoint.checkpoint_id.0
            )));
        }
        let (epoch, _) = suffix.split_once('-').ok_or_else(|| {
            CheckpointExportError::Integrity(format!(
                "invalid checkpoint changelog contribution path: {path}"
            ))
        })?;
        let contribution: crate::checkpoint::ChangelogCheckpointContribution =
            serde_json::from_slice(bytes).map_err(|error| {
                CheckpointExportError::Integrity(format!(
                    "malformed checkpoint changelog contribution {path}: {error}"
                ))
            })?;
        if contribution.checkpoint_id.0.to_string() != epoch
            || contribution.checkpoint_id != checkpoint.checkpoint_id
        {
            return Err(CheckpointExportError::Integrity(format!(
                "changelog contribution {path} identifies a different checkpoint epoch"
            )));
        }
    }
    Ok(false)
}

async fn snapshot_object_paths(
    source: Arc<dyn ObjectStore>,
    shard_path: &str,
    snapshot_id: &str,
) -> Result<Vec<Path>, CheckpointExportError> {
    let shard_path = resolve_shard_db_path(&source, shard_path).await?;
    let checkpoint_id = Uuid::parse_str(snapshot_id).map_err(|error| {
        CheckpointExportError::Integrity(format!(
            "invalid SlateDB snapshot id `{snapshot_id}`: {error}"
        ))
    })?;
    let admin =
        slatedb::admin::AdminBuilder::new(Path::from(shard_path.as_str()), source.clone()).build();
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
    manifest_object_paths_at(source, &shard_path, checkpoint.manifest_id).await
}

async fn manifest_object_paths(
    source: Arc<dyn ObjectStore>,
    shard_path: &str,
    manifest_id: u64,
) -> Result<Vec<Path>, CheckpointExportError> {
    let shard_path = resolve_shard_db_path(&source, shard_path).await?;
    manifest_object_paths_at(source, &shard_path, manifest_id).await
}

async fn resolve_shard_db_path(
    source: &Arc<dyn ObjectStore>,
    shard_path: &str,
) -> Result<String, CheckpointExportError> {
    let current = format!("{shard_path}/db");
    let mut listing = source.list(Some(&Path::from(current.clone())));
    match listing.next().await {
        Some(Ok(_)) => return Ok(current),
        Some(Err(error)) => return Err(CheckpointExportError::ObjectStore(error.to_string())),
        None => {}
    }
    let mut listing = source.list(Some(&Path::from(shard_path)));
    match listing.next().await {
        Some(Ok(_)) => Ok(shard_path.to_owned()),
        Some(Err(error)) => Err(CheckpointExportError::ObjectStore(error.to_string())),
        None => Err(CheckpointExportError::Integrity(format!(
            "SlateDB database for shard `{shard_path}` is missing"
        ))),
    }
}

async fn manifest_object_paths_at(
    source: Arc<dyn ObjectStore>,
    shard_path: &str,
    manifest_id: u64,
) -> Result<Vec<Path>, CheckpointExportError> {
    let admin = slatedb::admin::AdminBuilder::new(Path::from(shard_path), source.clone()).build();
    let manifest = admin
        .read_manifest(Some(manifest_id))
        .await
        .map_err(|error| CheckpointExportError::Integrity(error.to_string()))?
        .ok_or_else(|| {
            CheckpointExportError::Integrity(format!(
                "SlateDB manifest {manifest_id} for shard `{shard_path}` is missing"
            ))
        })?;
    if manifest.wal_object_store_uri().is_some() {
        return Err(CheckpointExportError::Integrity(
            "checkpoint uses a separate WAL object store that is not attached for export".into(),
        ));
    }

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
        for view in run.sst_views() {
            add_view(view, shard_path);
        }
    }
    for segment in manifest.segments() {
        for view in segment.l0() {
            add_view(view, shard_path);
        }
        for run in segment.compacted() {
            for view in run.sst_views() {
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
    let wal_prefix = Path::from(format!("{shard_path}/wal"));
    let mut wal_objects = source.list(Some(&wal_prefix));
    let first_wal_id = manifest.replay_after_wal_id().saturating_add(1);
    let next_wal_id = manifest.next_wal_sst_id();
    let mut page_fill = 0_usize;
    while let Some(meta) = wal_objects.next().await {
        let meta = match meta {
            Ok(meta) => meta,
            Err(error) => {
                rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
                return Err(CheckpointExportError::ObjectStore(error.to_string()));
            }
        };
        page_fill = (page_fill % MAX_CHECKPOINT_EXPORT_SCAN_WINDOW) + 1;
        rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(page_fill as u64);
        let Some(wal_id) = meta
            .location
            .filename()
            .and_then(|filename| filename.strip_suffix(".sst"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if wal_id >= first_wal_id && wal_id < next_wal_id {
            objects.insert(meta.location);
        }
    }
    rockstream_types::metrics::set_checkpoint_export_scan_window_fill_level(0);
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
    use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
    use rockstream_types::ids::ShardId;

    use super::*;

    fn checkpoint() -> ClusterCheckpoint {
        ClusterCheckpoint::new(CheckpointId(7))
    }

    #[tokio::test]
    async fn checkpoint_export_selects_runtime_db_namespace_and_legacy_namespace() {
        for (name, db_path, expected_root) in [
            ("runtime", "db", "shards/1/db/"),
            ("legacy", "shards/1", "shards/1/"),
        ] {
            let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let db_store: Arc<dyn ObjectStore> = if name == "runtime" {
                Arc::new(object_store::prefix::PrefixStore::new(
                    source.clone(),
                    "shards/1",
                ))
            } else {
                source.clone()
            };
            let db = rockstream_storage::ShardDb::builder(db_path, db_store)
                .build()
                .await
                .unwrap();
            db.put(b"namespace-key", b"namespace-value").await.unwrap();
            db.flush().await.unwrap();
            let handle = db.create_checkpoint().await.unwrap();
            let source_reader = rockstream_storage::ShardReader::open_with_snapshot_id(
                db_path,
                if name == "runtime" {
                    Arc::new(object_store::prefix::PrefixStore::new(
                        source.clone(),
                        "shards/1",
                    ))
                } else {
                    source.clone()
                },
                &handle.snapshot_id,
            )
            .await
            .unwrap();
            assert_eq!(
                source_reader
                    .get(b"namespace-key")
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"namespace-value"
            );
            db.close().await.unwrap();
            let mut cluster = checkpoint();
            cluster.record_shard(
                ShardId(1),
                PerShardCheckpoint::new(CheckpointId(7), handle.shard_checkpoint_id)
                    .with_snapshot_id(handle.snapshot_id),
            );
            crate::checkpoint_store::CheckpointManifestStore::new(source.clone())
                .save_manifest(&cluster, false, None)
                .await
                .unwrap();
            let snapshot_id = cluster.shards[&ShardId(1)]
                .snapshot_id
                .as_deref()
                .unwrap()
                .to_owned();

            let objects = CheckpointExportService::list_checkpoint_objects(
                &source,
                &cluster,
                &Path::from(""),
            )
            .await
            .unwrap();
            assert!(!objects.is_empty());
            assert!(objects.contains(&Path::from("control/checkpoints/7")));
            assert!(objects
                .iter()
                .filter(|path| is_shard_path(path))
                .all(|path| path.as_ref().starts_with(expected_root)));
            assert!(objects.iter().any(|path| {
                path.as_ref().ends_with(&format!(
                    "manifest/{:020}.manifest",
                    handle.shard_checkpoint_id
                ))
            }));

            let outcome = CheckpointExportService::new()
                .export_prefix(
                    source.clone(),
                    destination.clone(),
                    cluster,
                    format!("namespace-{name}"),
                    &Path::from(""),
                )
                .await
                .unwrap();
            assert_eq!(outcome.checkpoint_id, 7);
            assert_eq!(outcome.generation, format!("namespace-{name}"));
            assert_eq!(outcome.object_count as usize, objects.len());
            assert_eq!(outcome.status, "SUCCESS");
            let committed = CheckpointExportService::new()
                .validate_generation(destination.clone(), &format!("namespace-{name}"))
                .await
                .unwrap();
            assert_eq!(committed, outcome);
            let restored: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            CheckpointExportService::new()
                .restore_generation(destination, restored.clone(), &format!("namespace-{name}"))
                .await
                .unwrap();
            let restored_db_store: Arc<dyn ObjectStore> = if name == "runtime" {
                Arc::new(object_store::prefix::PrefixStore::new(restored, "shards/1"))
            } else {
                restored
            };
            let restored_reader = rockstream_storage::ShardReader::open_with_snapshot_id(
                db_path,
                restored_db_store,
                &snapshot_id,
            )
            .await
            .unwrap();
            assert_eq!(
                restored_reader
                    .get(b"namespace-key")
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"namespace-value"
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_inventory_pins_only_selected_control_manifest() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifests = crate::checkpoint_store::CheckpointManifestStore::new(source.clone());
        let checkpoint_a = ClusterCheckpoint::new(CheckpointId(7));
        let checkpoint_b = ClusterCheckpoint::new(CheckpointId(8));
        manifests
            .save_manifest(&checkpoint_a, false, None)
            .await
            .unwrap();
        manifests
            .save_manifest(&checkpoint_b, false, None)
            .await
            .unwrap();

        // Even when the live root lists B, inventory of an explicitly
        // selected A must contain exactly A's discoverable control manifest.
        let objects = CheckpointExportService::list_checkpoint_objects(
            &source,
            &checkpoint_a,
            &Path::from(""),
        )
        .await
        .unwrap();
        assert_eq!(objects, vec![Path::from("control/checkpoints/7")]);
    }

    #[tokio::test]
    async fn checkpoint_validator_rejects_newer_manifest_with_valid_hashes() {
        let export: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let service = CheckpointExportService::new();
        let generation = "tampered-manifest";
        let selected = ClusterCheckpoint::new(CheckpointId(7));
        let newer = ClusterCheckpoint::new(CheckpointId(8));
        let bytes = serde_json::to_vec(&newer).unwrap();
        let record = InventoryRecord {
            source: "control/checkpoints/8".to_string(),
            destination: object_path(generation, 0).to_string(),
            byte_len: bytes.len() as u64,
            sha256: digest_bytes(&bytes),
        };
        export
            .put(&object_path(generation, 0), bytes.into())
            .await
            .unwrap();
        service
            .put_json(
                &export,
                &generation_path(generation),
                &GenerationRecord {
                    checkpoint_id: selected.checkpoint_id,
                    generation: generation.to_string(),
                    checkpoint: selected,
                    object_count: 1,
                    requires_checkpoint_manifest: true,
                },
            )
            .await
            .unwrap();
        service
            .put_json(&export, &inventory_path(generation, 0), &record)
            .await
            .unwrap();
        let mut digest = Sha256::new();
        update_digest(&mut digest, &record).unwrap();
        let outcome = CheckpointExportOutcome {
            checkpoint_id: 7,
            generation: generation.to_string(),
            object_count: 1,
            byte_count: record.byte_len,
            inventory_digest: hex::encode(digest.finalize()),
            status: "SUCCESS".to_string(),
        };
        service
            .put_json(&export, &commit_path(generation), &CommitMarker { outcome })
            .await
            .unwrap();

        let error = service
            .validate_generation(export.clone(), generation)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("other than selected checkpoint"));
        assert!(service
            .restore_generation(export, target.clone(), generation)
            .await
            .is_err());
        assert!(matches!(
            target
                .head(&Path::from("control/bootstrap/active-generation"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn restore_rejects_preexisting_newer_checkpoint_without_success_pointer() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let export: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let selected = ClusterCheckpoint::new(CheckpointId(7));
        crate::checkpoint_store::CheckpointManifestStore::new(source.clone())
            .save_manifest(&selected, false, None)
            .await
            .unwrap();
        let service = CheckpointExportService::new();
        service
            .export_latest_prefix(
                source.clone(),
                export.clone(),
                &crate::checkpoint_store::CheckpointManifestStore::new(source),
                "target-has-newer",
                &Path::from(""),
            )
            .await
            .unwrap();
        crate::checkpoint_store::CheckpointManifestStore::new(target.clone())
            .save_manifest(&ClusterCheckpoint::new(CheckpointId(8)), false, None)
            .await
            .unwrap();
        let error = service
            .restore_generation(export, target.clone(), "target-has-newer")
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("another discoverable checkpoint"));
        assert!(matches!(
            target
                .head(&Path::from("control/bootstrap/active-generation"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
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
            "RS-3614: checkpoint export integrity validation failed: destination object checkpoint-exports/generation-b/objects/00000000000000000000 failed validation; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
        );
    }

    #[tokio::test]
    async fn checkpoint_export_prefix_paginates_beyond_legacy_scan_window() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let total = MAX_CHECKPOINT_EXPORT_SCAN_WINDOW + 128;
        for index in 0..total {
            source
                .put(
                    &Path::from(format!("control/meta/{index:05}")),
                    bytes::Bytes::from_static(b"x").into(),
                )
                .await
                .unwrap();
        }

        crate::checkpoint_store::CheckpointManifestStore::new(source.clone())
            .save_manifest(&checkpoint(), false, None)
            .await
            .unwrap();
        let manifest_bytes = serde_json::to_vec(&checkpoint()).unwrap();
        let outcome = CheckpointExportService::new()
            .export_prefix(
                source,
                destination.clone(),
                checkpoint(),
                "generation-c",
                &Path::from("control/meta"),
            )
            .await
            .unwrap();
        assert_eq!(outcome.checkpoint_id, 7);
        assert_eq!(outcome.generation, "generation-c");
        assert_eq!(outcome.object_count, total as u64 + 1);
        assert_eq!(
            outcome.byte_count,
            total as u64 + manifest_bytes.len() as u64
        );
        assert_eq!(outcome.status, "SUCCESS");
        assert_eq!(
            rockstream_types::metrics::read_checkpoint_export_scan_window_fill_level(),
            0
        );
        let validated = CheckpointExportService::new()
            .validate_generation(destination, "generation-c")
            .await
            .unwrap();
        assert_eq!(validated, outcome);
    }

    #[test]
    fn checkpoint_export_limits_are_named_and_exact() {
        assert_eq!(MAX_CHECKPOINT_EXPORT_OBJECTS_IN_FLIGHT, 1);
        assert_eq!(MAX_CHECKPOINT_EXPORT_SCAN_WINDOW, 1024);
        assert_eq!(CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES, 8 * 1024 * 1024);
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
            "RS-3614: checkpoint export integrity validation failed: generation must be a non-empty path segment; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
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
