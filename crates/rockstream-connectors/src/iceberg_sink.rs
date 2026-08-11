//! Fallback filesystem-only Iceberg sink.
//!
//! FALLBACK: `iceberg` 0.9.x compiles, but its public transaction surface in
//! this release is currently missing the replace/overwrite-files action
//! RockStream needs here. RockStream sink epochs are full-state snapshots, so a
//! real integration must atomically replace the table's active data file set on
//! each commit. `iceberg::transaction::Transaction` exposes `fast_append()` and
//! metadata-only actions, but not a public overwrite/delete-files transaction
//! action in 0.9.x. This module therefore still writes Parquet snapshot files
//! plus a simplified RockStream-owned `metadata.json` manifest instead of
//! spec-complete Iceberg manifests/manifest lists. That preserves durability and
//! exact row read-back in tests, but it does **not** fully prove P1 ("external
//! engines can query with zero data corruption").

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;
use serde::{Deserialize, Serialize};

use crate::fault_injecting_store::FaultInjectingObjectStore;
use crate::partition_spec::split_batch_by_partition;
use crate::sink_connector::{
    assert_epoch_committed_only_after_cluster_checkpoint, assert_no_duplicate_delivery,
    assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};

pub const ICEBERG_SINK_MAX_PENDING_EPOCHS: usize = 8;

/// One partitioned (or, if `partition` is empty, unpartitioned) data file
/// belonging to a committed snapshot (v0.44 slice 7 — `partition_by`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergDataFile {
    path: String,
    partition: String,
    row_count: usize,
    file_size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergSnapshotMetadata {
    epoch: Epoch,
    /// Wall-clock commit time in ms, injectable via `set_now_ms` for
    /// deterministic tests — used by cold-snapshot GC's
    /// `cold_snapshot_retention_duration` bound (v0.44 slice 11).
    committed_at_ms: u64,
    data_files: Vec<IcebergDataFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct IcebergMetadataFile {
    format_version: u64,
    last_snapshot_epoch: Option<Epoch>,
    snapshots: Vec<IcebergSnapshotMetadata>,
}

/// One partition's pending/final path pair staged during `pre_commit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingFile {
    pending_path: String,
    final_path: String,
    partition: String,
    row_count: usize,
}

#[derive(Debug, Clone)]
struct PendingSnapshot {
    files: Vec<PendingFile>,
}

#[derive(Debug)]
pub struct IcebergSink {
    connector_id: ConnectorId,
    store: Arc<FaultInjectingObjectStore>,
    base_path: String,
    cluster_committed: Epoch,
    parquet_row_group_bytes: usize,
    format_version: u64,
    partition_by: Vec<String>,
    pending: BTreeMap<Epoch, PendingSnapshot>,
    pending_epochs_count: usize,
    max_pending_epochs: usize,
    delivered_epochs: BTreeSet<Epoch>,
    staged_batch: Option<RecordBatch>,
    /// Injectable wall-clock time (ms), used to stamp snapshot commits for
    /// GC's `cold_snapshot_retention_duration` bound (v0.44 slice 11).
    /// Defaults to real wall-clock time via `SystemTime::now()`.
    now_ms: Option<u64>,
}

impl IcebergSink {
    pub fn new(
        connector_id: ConnectorId,
        store: Arc<FaultInjectingObjectStore>,
        base_path: impl Into<String>,
    ) -> Self {
        Self {
            connector_id,
            store,
            base_path: base_path.into().trim_end_matches('/').to_string(),
            cluster_committed: 0,
            parquet_row_group_bytes: 1024 * 1024,
            format_version: 2,
            partition_by: Vec::new(),
            pending: BTreeMap::new(),
            pending_epochs_count: 0,
            max_pending_epochs: ICEBERG_SINK_MAX_PENDING_EPOCHS,
            delivered_epochs: BTreeSet::new(),
            staged_batch: None,
            now_ms: None,
        }
    }

    pub fn set_cluster_committed(&mut self, epoch: Epoch) {
        self.cluster_committed = epoch;
    }

    pub fn set_staged_batch(&mut self, batch: RecordBatch) {
        self.staged_batch = Some(batch);
    }

    pub fn set_parquet_row_group_bytes(&mut self, bytes: usize) {
        self.parquet_row_group_bytes = bytes.max(1);
    }

    pub fn set_partition_by(&mut self, partition_by: Vec<String>) {
        self.partition_by = partition_by;
    }

    /// Inject a deterministic wall-clock time (ms) for snapshot commit
    /// timestamps, used by cold-snapshot GC tests. Production code leaves
    /// this unset and falls back to `SystemTime::now()`.
    pub fn set_now_ms(&mut self, now_ms: u64) {
        self.now_ms = Some(now_ms);
    }

    fn current_now_ms(&self) -> u64 {
        self.now_ms.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0)
        })
    }

    pub fn iceberg_pending_epochs_count(&self) -> usize {
        self.pending_epochs_count
    }

    pub fn backpressure_active(&self) -> bool {
        self.pending_epochs_count >= self.max_pending_epochs
    }

    pub async fn read_snapshot(&self, epoch: Epoch) -> Result<Vec<RecordBatch>, SinkError> {
        let metadata = self.read_metadata().await?;
        let snapshot = metadata
            .snapshots
            .iter()
            .find(|snapshot| snapshot.epoch == epoch)
            .ok_or_else(|| SinkError::Io(format!("snapshot {epoch} not found")))?;
        let mut batches = Vec::new();
        for file in &snapshot.data_files {
            batches.extend(self.read_batches_from_path(&file.path).await?);
        }
        Ok(batches)
    }

    pub async fn read_latest_snapshot(&self) -> Result<Vec<RecordBatch>, SinkError> {
        let metadata = self.read_metadata().await?;
        let latest = metadata
            .last_snapshot_epoch
            .ok_or_else(|| SinkError::Io("no committed snapshots".to_string()))?;
        self.read_snapshot(latest).await
    }

    /// Returns `(partition, path)` for every data file in `epoch`'s snapshot,
    /// sorted by partition — used to assert partition-directory layout
    /// (v0.44 slice 7 green test).
    pub async fn snapshot_partition_files(
        &self,
        epoch: Epoch,
    ) -> Result<Vec<(String, String)>, SinkError> {
        let metadata = self.read_metadata().await?;
        let snapshot = metadata
            .snapshots
            .iter()
            .find(|snapshot| snapshot.epoch == epoch)
            .ok_or_else(|| SinkError::Io(format!("snapshot {epoch} not found")))?;
        Ok(snapshot
            .data_files
            .iter()
            .map(|file| (file.partition.clone(), file.path.clone()))
            .collect())
    }

    fn metadata_path(&self) -> Path {
        Path::from(format!("{}/metadata.json", self.base_path))
    }

    fn pending_data_path(&self, epoch: Epoch, partition: &str) -> Path {
        if partition.is_empty() {
            Path::from(format!("{}/_pending/{epoch}/data.parquet", self.base_path))
        } else {
            Path::from(format!(
                "{}/_pending/{epoch}/{partition}/data.parquet",
                self.base_path
            ))
        }
    }

    fn final_data_path(&self, epoch: Epoch, partition: &str) -> Path {
        if partition.is_empty() {
            Path::from(format!("{}/data/epoch-{epoch}.parquet", self.base_path))
        } else {
            Path::from(format!(
                "{}/data/{partition}/epoch-{epoch}.parquet",
                self.base_path
            ))
        }
    }

    async fn read_metadata(&self) -> Result<IcebergMetadataFile, SinkError> {
        match self.store.inner().get(&self.metadata_path()).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| SinkError::Io(error.to_string()))?;
                serde_json::from_slice(&bytes).map_err(|error| SinkError::Io(error.to_string()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(IcebergMetadataFile {
                format_version: self.format_version,
                ..IcebergMetadataFile::default()
            }),
            Err(error) => Err(SinkError::Io(error.to_string())),
        }
    }

    async fn write_metadata(&self, metadata: &IcebergMetadataFile) -> Result<(), SinkError> {
        let payload = serde_json::to_vec_pretty(metadata)
            .map_err(|error| SinkError::Io(error.to_string()))?;
        self.store
            .inner()
            .put(&self.metadata_path(), payload.into())
            .await
            .map_err(|error| SinkError::Io(error.to_string()))?;
        Ok(())
    }

    async fn read_batches_from_path(&self, path: &str) -> Result<Vec<RecordBatch>, SinkError> {
        let bytes = self
            .store
            .inner()
            .get(&Path::from(path))
            .await
            .map_err(|error| SinkError::Io(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| SinkError::Io(error.to_string()))?;
        decode_record_batches_from_parquet(bytes)
    }

    async fn finalize_commit(
        &mut self,
        epoch: Epoch,
        files: Vec<PendingFile>,
    ) -> Result<(), SinkError> {
        let mut data_files = Vec::with_capacity(files.len());
        for file in &files {
            let pending_path = Path::from(file.pending_path.clone());
            let final_path = Path::from(file.final_path.clone());

            let pending_bytes = self
                .store
                .inner()
                .get(&pending_path)
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?
                .bytes()
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?;
            let expected_size_bytes = pending_bytes.len();

            self.store
                .put(&final_path, pending_bytes.clone().into())
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?;

            let observed_size_bytes = self
                .store
                .inner()
                .head(&final_path)
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?
                .size;

            if observed_size_bytes as usize != expected_size_bytes {
                return Err(SinkError::CommitFailed {
                    epoch,
                    reason: format!(
                        "RS-4002: sink write failed: final snapshot size {} != expected {}",
                        observed_size_bytes, expected_size_bytes
                    ),
                });
            }

            data_files.push(IcebergDataFile {
                path: final_path.to_string(),
                partition: file.partition.clone(),
                row_count: file.row_count,
                file_size_bytes: expected_size_bytes,
            });
        }

        let mut metadata = self.read_metadata().await?;
        if !metadata
            .snapshots
            .iter()
            .any(|snapshot| snapshot.epoch == epoch)
        {
            metadata.snapshots.push(IcebergSnapshotMetadata {
                epoch,
                committed_at_ms: self.current_now_ms(),
                data_files,
            });
            metadata.last_snapshot_epoch = Some(epoch);
            self.write_metadata(&metadata).await?;
        }

        self.cleanup_pending(epoch, &files).await?;
        self.delivered_epochs.insert(epoch);
        Ok(())
    }

    async fn cleanup_pending(
        &mut self,
        epoch: Epoch,
        files: &[PendingFile],
    ) -> Result<(), SinkError> {
        for file in files {
            match self
                .store
                .inner()
                .delete(&Path::from(file.pending_path.clone()))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(SinkError::Io(error.to_string())),
            }
        }
        self.pending.remove(&epoch);
        if self.pending_epochs_count > 0 {
            self.pending_epochs_count -= 1;
        }
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::NativeIdempotent
    }

    async fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
        if self.backpressure_active() {
            return Err(SinkError::PreCommitFailed {
                epoch,
                reason: format!(
                    "backpressure: pending_epochs={} >= max={}",
                    self.pending_epochs_count, self.max_pending_epochs
                ),
            });
        }
        let batch = self
            .staged_batch
            .clone()
            .ok_or_else(|| SinkError::PreCommitFailed {
                epoch,
                reason: "no staged batch prepared before pre_commit".to_string(),
            })?;
        let groups = split_batch_by_partition(&batch, &self.partition_by).map_err(|error| {
            SinkError::PreCommitFailed {
                epoch,
                reason: error.to_string(),
            }
        })?;
        let store = Arc::clone(&self.store);
        let parquet_row_group_bytes = self.parquet_row_group_bytes;

        let mut files = Vec::with_capacity(groups.len());
        for (partition, sub_batch) in &groups {
            let pending_path = self.pending_data_path(epoch, partition);
            let final_path = self.final_data_path(epoch, partition);
            let bytes = encode_record_batch_as_parquet(sub_batch, parquet_row_group_bytes)?;
            store
                .inner()
                .put(&pending_path, bytes.into())
                .await
                .map_err(|error| SinkError::Io(error.to_string()))?;
            files.push(PendingFile {
                pending_path: pending_path.to_string(),
                final_path: final_path.to_string(),
                partition: partition.clone(),
                row_count: sub_batch.num_rows(),
            });
        }

        let pending_handle =
            serde_json::to_vec(&files).map_err(|error| SinkError::PreCommitFailed {
                epoch,
                reason: error.to_string(),
            })?;
        self.pending.insert(epoch, PendingSnapshot { files });
        self.pending_epochs_count += 1;
        Ok(SinkState::PreCommitted {
            staged_rows: row_count,
            pending_handle,
        })
    }

    async fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError> {
        assert_epoch_committed_only_after_cluster_checkpoint(
            self.connector_id,
            epoch,
            self.cluster_committed,
        );
        assert_no_duplicate_delivery(self.connector_id, epoch, &self.delivered_epochs);

        let files: Vec<PendingFile> = match state {
            SinkState::PreCommitted { pending_handle, .. } => {
                serde_json::from_slice(pending_handle).map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?
            }
            SinkState::Committed => return Ok(()),
            SinkState::Idle => {
                return Err(SinkError::CommitFailed {
                    epoch,
                    reason: "commit called on Idle sink state".to_string(),
                });
            }
        };

        self.finalize_commit(epoch, files).await
    }

    async fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        if let Some(pending) = self.pending.get(&epoch).cloned() {
            self.cleanup_pending(epoch, &pending.files).await?;
        }
        Ok(())
    }

    async fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        match action {
            RecoveryAction::Noop => Ok(()),
            RecoveryAction::RerunCommit {
                epoch,
                ref pending_handle,
                ..
            } => {
                let files: Vec<PendingFile> = serde_json::from_slice(pending_handle)
                    .map_err(|error| SinkError::Io(error.to_string()))?;

                for file in &files {
                    let pending_path = Path::from(file.pending_path.clone());
                    let final_path = Path::from(file.final_path.clone());
                    let expected_size_bytes = self
                        .store
                        .inner()
                        .head(&pending_path)
                        .await
                        .map_err(|error| SinkError::CommitFailed {
                            epoch,
                            reason: error.to_string(),
                        })?
                        .size;

                    if let Ok(meta) = self.store.inner().head(&final_path).await {
                        if meta.size != expected_size_bytes {
                            let _ = self.store.inner().delete(&final_path).await;
                        }
                    }
                }

                self.finalize_commit(epoch, files).await?;
                let final_state = SinkState::Committed;
                assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                Ok(())
            }
        }
    }
}

// ─── ColdGc adapter (v0.44 slice 11) ───────────────────────────────────────

/// Path used to durably record a scan-and-delete GC pass's target file list
/// before deleting the expired snapshot metadata, so a crash mid-delete can
/// resume without re-computing (and without ever deleting a file twice —
/// `delete_file` tolerates `NotFound`).
const GC_PENDING_DELETES_FILE: &str = "_gc_pending_deletes.json";

#[async_trait]
impl crate::cold_gc::ColdGcCatalog for IcebergSink {
    async fn list_snapshots(&self) -> Result<Vec<crate::cold_gc::RetainedSnapshot>, SinkError> {
        let metadata = self.read_metadata().await?;
        Ok(metadata
            .snapshots
            .into_iter()
            .map(|snapshot| crate::cold_gc::RetainedSnapshot {
                epoch: snapshot.epoch,
                committed_at_ms: snapshot.committed_at_ms,
                files: snapshot
                    .data_files
                    .into_iter()
                    .map(|file| file.path)
                    .collect(),
            })
            .collect())
    }

    async fn remove_snapshots(&mut self, epochs: &[Epoch]) -> Result<(), SinkError> {
        let mut metadata = self.read_metadata().await?;
        metadata
            .snapshots
            .retain(|snapshot| !epochs.contains(&snapshot.epoch));
        self.write_metadata(&metadata).await
    }

    async fn delete_file(&mut self, path: &str) -> Result<u64, SinkError> {
        let object_path = Path::from(path);
        let size_before = match self.store.inner().head(&object_path).await {
            Ok(meta) => meta.size,
            Err(object_store::Error::NotFound { .. }) => return Ok(0),
            Err(error) => return Err(SinkError::Io(error.to_string())),
        };
        match self.store.inner().delete(&object_path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(size_before),
            Err(error) => Err(SinkError::Io(error.to_string())),
        }
    }

    async fn read_pending_deletes(&self) -> Result<Vec<String>, SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        match self.store.inner().get(&path).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| SinkError::Io(error.to_string()))?;
                serde_json::from_slice(&bytes).map_err(|error| SinkError::Io(error.to_string()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(error) => Err(SinkError::Io(error.to_string())),
        }
    }

    async fn write_pending_deletes(&mut self, paths: &[String]) -> Result<(), SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        let payload =
            serde_json::to_vec(paths).map_err(|error| SinkError::Io(error.to_string()))?;
        self.store
            .inner()
            .put(&path, payload.into())
            .await
            .map_err(|error| SinkError::Io(error.to_string()))?;
        Ok(())
    }

    async fn clear_pending_deletes(&mut self) -> Result<(), SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        match self.store.inner().delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(SinkError::Io(error.to_string())),
        }
    }
}

fn encode_record_batch_as_parquet(
    batch: &RecordBatch,
    parquet_row_group_bytes: usize,
) -> Result<Vec<u8>, SinkError> {
    let props = WriterProperties::builder()
        .set_max_row_group_bytes(Some(parquet_row_group_bytes))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
        .map_err(|error| SinkError::Io(error.to_string()))?;
    writer
        .write(batch)
        .map_err(|error| SinkError::Io(error.to_string()))?;
    writer
        .close()
        .map_err(|error| SinkError::Io(error.to_string()))?;
    Ok(buffer)
}

fn decode_record_batches_from_parquet(bytes: Bytes) -> Result<Vec<RecordBatch>, SinkError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| SinkError::Io(error.to_string()))?
        .build()
        .map_err(|error| SinkError::Io(error.to_string()))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SinkError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;

    fn make_batch(last_id: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from((1..=last_id).collect::<Vec<_>>()));
        let names: ArrayRef = Arc::new(StringArray::from(
            (1..=last_id)
                .map(|id| format!("row-{id}"))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![ids, names]).unwrap()
    }

    fn render_batches(batches: &[RecordBatch]) -> String {
        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let rendered = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                            values.value(row_idx).to_string()
                        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
                            values.value(row_idx).to_string()
                        } else {
                            panic!(
                                "unsupported array type in test row renderer: {:?}",
                                column.data_type()
                            );
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                rows.push(rendered);
            }
        }
        rows.join("\n")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_five_snapshots_and_reads_them_back() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultInjectingObjectStore::new(inner));
        let mut sink = IcebergSink::new(ConnectorId(44), store, "iceberg-test");
        sink.set_cluster_committed(10);
        sink.set_parquet_row_group_bytes(256);

        let mut expected_batches = Vec::new();
        for epoch in 1..=5 {
            let batch = make_batch(epoch as i64);
            expected_batches.push(batch.clone());
            sink.set_staged_batch(batch.clone());
            let state = sink.pre_commit(epoch, batch.num_rows()).await.unwrap();
            sink.set_cluster_committed(epoch);
            sink.commit(epoch, &state).await.unwrap();
        }

        for (index, expected) in expected_batches.iter().enumerate() {
            let observed = sink.read_snapshot((index + 1) as u64).await.unwrap();
            assert_eq!(
                render_batches(&observed),
                render_batches(std::slice::from_ref(expected))
            );
        }
    }

    fn make_partitioned_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));
        let regions: ArrayRef = Arc::new(StringArray::from(vec![
            "eu", "us", "eu", "apac", "us", "apac",
        ]));
        RecordBatch::try_new(schema, vec![ids, regions]).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_partitioned_snapshot_across_three_partitions() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultInjectingObjectStore::new(inner));
        let mut sink = IcebergSink::new(ConnectorId(45), store, "iceberg-partitioned-test");
        sink.set_cluster_committed(10);
        sink.set_partition_by(vec!["region".to_string()]);

        let batch = make_partitioned_batch();
        sink.set_staged_batch(batch.clone());
        let state = sink.pre_commit(1, batch.num_rows()).await.unwrap();
        sink.set_cluster_committed(1);
        sink.commit(1, &state).await.unwrap();

        let partition_files = sink.snapshot_partition_files(1).await.unwrap();
        let mut suffixes: Vec<&str> = partition_files
            .iter()
            .map(|(suffix, _)| suffix.as_str())
            .collect();
        suffixes.sort();
        assert_eq!(suffixes, vec!["region=apac", "region=eu", "region=us"]);
        for (suffix, path) in &partition_files {
            assert!(
                path.contains(suffix.as_str()),
                "expected data file path {path} to embed partition suffix {suffix}"
            );
        }

        let observed = sink.read_snapshot(1).await.unwrap();
        let mut rendered_rows: Vec<String> = render_batches(&observed)
            .lines()
            .map(str::to_string)
            .collect();
        rendered_rows.sort();
        let mut expected_rows: Vec<String> = render_batches(std::slice::from_ref(&batch))
            .lines()
            .map(str::to_string)
            .collect();
        expected_rows.sort();
        assert_eq!(rendered_rows, expected_rows);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cold_gc_reclaims_expired_snapshots_via_real_sink() {
        use crate::cold_gc::{ColdGc, ColdGcCatalog, ColdGcConfig};
        use tokio::sync::Mutex;

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultInjectingObjectStore::new(inner));
        let mut sink = IcebergSink::new(ConnectorId(46), store, "iceberg-gc-test");
        sink.set_cluster_committed(10);

        for epoch in 1..=4u64 {
            let batch = make_batch(epoch as i64);
            sink.set_staged_batch(batch.clone());
            let state = sink.pre_commit(epoch, batch.num_rows()).await.unwrap();
            sink.set_cluster_committed(epoch);
            sink.commit(epoch, &state).await.unwrap();
        }

        let snapshots_before = ColdGcCatalog::list_snapshots(&sink).await.unwrap();
        assert_eq!(snapshots_before.len(), 4);

        let catalog = Arc::new(Mutex::new(sink));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 2,
                retention_duration_ms: u64::MAX,
            },
        );
        let result = gc.run(0).await.unwrap();
        assert_eq!(result.expired_epochs, vec![1, 2]);
        assert_eq!(result.deleted_files.len(), 2);
        drop(gc);

        let remaining_epochs = {
            let sink = catalog.lock().await;
            let snapshots_after = ColdGcCatalog::list_snapshots(&*sink).await.unwrap();
            let mut remaining_epochs: Vec<Epoch> =
                snapshots_after.iter().map(|s| s.epoch).collect();
            remaining_epochs.sort();
            remaining_epochs
        };
        assert_eq!(remaining_epochs, vec![3, 4]);

        let sink = Arc::try_unwrap(catalog)
            .unwrap_or_else(|_| panic!("catalog still shared"))
            .into_inner();
        assert!(sink.read_snapshot(3).await.is_ok());
        assert!(sink.read_snapshot(4).await.is_ok());
    }
}
