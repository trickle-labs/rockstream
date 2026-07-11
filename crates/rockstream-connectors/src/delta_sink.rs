//! Filesystem-only Delta sink fallback.
//!
//! FALLBACK: `deltalake` 0.32.x compiles, but the real-crate integration is
//! currently blocked by a concrete storage-API mismatch with RockStream's
//! existing fault-injection harness. `FaultInjectingObjectStore` implements
//! `object_store` 0.12, while `deltalake` 0.32.x's
//! `DeltaTableBuilder::with_storage_backend()` expects `object_store` 0.13.
//! The attempted integration failed with:
//! `expected trait deltalake::ObjectStore, found trait object_store::ObjectStore`
//! when passing the wrapped store into the Delta builder. Without injecting the
//! wrapped store, the existing LFS + MinIO recovery tests would lose their
//! deterministic partial-write coverage.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
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

pub const DELTA_SINK_MAX_PENDING_EPOCHS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaAddAction {
    path: String,
    epoch: Epoch,
    size: usize,
    rows: usize,
    partition: String,
    /// Wall-clock commit time in ms — used by cold-snapshot GC's
    /// `cold_snapshot_retention_duration` bound (v0.44 slice 11).
    committed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaLogEntry {
    version: u64,
    adds: Vec<DeltaAddAction>,
}

/// One partitioned (or, if `partition` is empty, unpartitioned) pending/final
/// path pair staged during `pre_commit` (v0.44 slice 7 — `partition_by`).
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
pub struct DeltaSink {
    connector_id: ConnectorId,
    store: Arc<FaultInjectingObjectStore>,
    base_path: String,
    cluster_committed: Epoch,
    parquet_row_group_bytes: usize,
    partition_by: Vec<String>,
    pending: BTreeMap<Epoch, PendingSnapshot>,
    pending_epochs_count: usize,
    max_pending_epochs: usize,
    delivered_epochs: BTreeSet<Epoch>,
    staged_batch: Option<RecordBatch>,
    /// Injectable wall-clock time (ms), used by cold-snapshot GC tests
    /// (v0.44 slice 11). Defaults to real wall-clock time.
    now_ms: Option<u64>,
}

impl DeltaSink {
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
            partition_by: Vec::new(),
            pending: BTreeMap::new(),
            pending_epochs_count: 0,
            max_pending_epochs: DELTA_SINK_MAX_PENDING_EPOCHS,
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

    /// Inject a deterministic wall-clock time (ms) for commit timestamps,
    /// used by cold-snapshot GC tests.
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

    pub fn delta_pending_epochs_count(&self) -> usize {
        self.pending_epochs_count
    }

    pub fn backpressure_active(&self) -> bool {
        self.pending_epochs_count >= self.max_pending_epochs
    }

    pub async fn read_snapshot(&self, epoch: Epoch) -> Result<Vec<RecordBatch>, SinkError> {
        let logs = self.read_all_logs().await?;
        let mut batches = Vec::new();
        for entry in &logs {
            for add in entry.adds.iter().filter(|add| add.epoch == epoch) {
                batches.extend(self.read_batches_from_path(&add.path).await?);
            }
        }
        if batches.is_empty() {
            return Err(SinkError::Io(format!("snapshot {epoch} not found")));
        }
        Ok(batches)
    }

    /// Returns `(partition, path)` for every data file committed at `epoch`,
    /// sorted by partition — used to assert partition-directory layout
    /// (v0.44 slice 7 green test).
    pub async fn snapshot_partition_files(
        &self,
        epoch: Epoch,
    ) -> Result<Vec<(String, String)>, SinkError> {
        let logs = self.read_all_logs().await?;
        let mut files: Vec<(String, String)> = logs
            .into_iter()
            .flat_map(|entry| entry.adds)
            .filter(|add| add.epoch == epoch)
            .map(|add| (add.partition, add.path))
            .collect();
        files.sort();
        Ok(files)
    }

    pub async fn read_latest_snapshot(&self) -> Result<Vec<RecordBatch>, SinkError> {
        let logs = self.read_all_logs().await?;
        let latest_version = logs
            .iter()
            .map(|entry| entry.version)
            .max()
            .ok_or_else(|| SinkError::Io("no committed snapshots".to_string()))?;
        let mut batches = Vec::new();
        for entry in logs.iter().filter(|entry| entry.version == latest_version) {
            for add in &entry.adds {
                batches.extend(self.read_batches_from_path(&add.path).await?);
            }
        }
        Ok(batches)
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

    fn log_path(&self, epoch: Epoch) -> Path {
        Path::from(format!(
            "{}/_delta_log/{:020}.json",
            self.base_path,
            epoch.saturating_sub(1)
        ))
    }

    async fn read_all_logs(&self) -> Result<Vec<DeltaLogEntry>, SinkError> {
        let prefix = Path::from(format!("{}/_delta_log/", self.base_path));
        let mut entries = Vec::new();
        let mut listing = self.store.inner().list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|error| SinkError::Io(error.to_string()))?;
            let bytes = self
                .store
                .inner()
                .get(&meta.location)
                .await
                .map_err(|error| SinkError::Io(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| SinkError::Io(error.to_string()))?;
            entries.push(
                serde_json::from_slice::<DeltaLogEntry>(&bytes)
                    .map_err(|error| SinkError::Io(error.to_string()))?,
            );
        }
        entries.sort_by_key(|entry| entry.version);
        Ok(entries)
    }

    async fn write_log_entry(
        &self,
        epoch: Epoch,
        adds: Vec<DeltaAddAction>,
    ) -> Result<(), SinkError> {
        let entry = DeltaLogEntry {
            version: epoch.saturating_sub(1),
            adds,
        };
        let payload =
            serde_json::to_vec_pretty(&entry).map_err(|error| SinkError::Io(error.to_string()))?;
        self.store
            .inner()
            .put(&self.log_path(epoch), payload.into())
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
        let mut adds = Vec::with_capacity(files.len());
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
            let expected_size = pending_bytes.len();

            self.store
                .put(&final_path, pending_bytes.into())
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?;

            let observed_size = self
                .store
                .inner()
                .head(&final_path)
                .await
                .map_err(|error| SinkError::CommitFailed {
                    epoch,
                    reason: error.to_string(),
                })?
                .size;
            if observed_size as usize != expected_size {
                return Err(SinkError::CommitFailed {
                    epoch,
                    reason: format!(
                        "RS-4002: sink write failed: final snapshot size {} != expected {}",
                        observed_size, expected_size
                    ),
                });
            }

            adds.push(DeltaAddAction {
                path: final_path.to_string(),
                epoch,
                size: expected_size,
                rows: file.row_count,
                partition: file.partition.clone(),
                committed_at_ms: self.current_now_ms(),
            });
        }

        let logs = self.read_all_logs().await?;
        if !logs
            .iter()
            .any(|entry| entry.adds.iter().any(|add| add.epoch == epoch))
        {
            self.write_log_entry(epoch, adds).await?;
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

impl SinkConnector for DeltaSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::NativeIdempotent
    }

    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
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
            let store = Arc::clone(&store);
            let sub_batch_owned = sub_batch.clone();
            let pending_path_for_task = pending_path.clone();
            run_async(async move {
                let bytes =
                    encode_record_batch_as_parquet(&sub_batch_owned, parquet_row_group_bytes)?;
                store
                    .inner()
                    .put(&pending_path_for_task, bytes.into())
                    .await
                    .map_err(|error| SinkError::Io(error.to_string()))
            })?;
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

    fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError> {
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

        run_async(self.finalize_commit(epoch, files))
    }

    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        if let Some(pending) = self.pending.get(&epoch).cloned() {
            run_async(self.cleanup_pending(epoch, &pending.files))?;
        }
        Ok(())
    }

    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        match action {
            RecoveryAction::Noop => Ok(()),
            RecoveryAction::RerunCommit {
                epoch,
                ref pending_handle,
                ..
            } => {
                let files: Vec<PendingFile> = serde_json::from_slice(pending_handle)
                    .map_err(|error| SinkError::Io(error.to_string()))?;

                run_async(async {
                    for file in &files {
                        let pending_path = Path::from(file.pending_path.clone());
                        let final_path = Path::from(file.final_path.clone());
                        let expected_size = self
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
                            if meta.size != expected_size {
                                let _ = self.store.inner().delete(&final_path).await;
                            }
                        }
                    }

                    self.finalize_commit(epoch, files).await?;
                    let final_state = SinkState::Committed;
                    assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                    Ok(())
                })
            }
        }
    }
}

// ─── ColdGc adapter (v0.44 slice 11) ───────────────────────────────────────

const GC_PENDING_DELETES_FILE: &str = "_gc_pending_deletes.json";

impl crate::cold_gc::ColdGcCatalog for DeltaSink {
    fn list_snapshots(&self) -> Result<Vec<crate::cold_gc::RetainedSnapshot>, SinkError> {
        let logs = run_async(self.read_all_logs())?;
        let mut by_epoch: BTreeMap<Epoch, crate::cold_gc::RetainedSnapshot> = BTreeMap::new();
        for entry in logs {
            for add in entry.adds {
                let snapshot =
                    by_epoch
                        .entry(add.epoch)
                        .or_insert_with(|| crate::cold_gc::RetainedSnapshot {
                            epoch: add.epoch,
                            committed_at_ms: add.committed_at_ms,
                            files: Vec::new(),
                        });
                snapshot.files.push(add.path);
            }
        }
        Ok(by_epoch.into_values().collect())
    }

    fn remove_snapshots(&mut self, epochs: &[Epoch]) -> Result<(), SinkError> {
        run_async(async {
            let logs = self.read_all_logs().await?;
            for entry in logs {
                if entry.adds.iter().any(|add| epochs.contains(&add.epoch)) {
                    let remaining: Vec<DeltaAddAction> = entry
                        .adds
                        .into_iter()
                        .filter(|add| !epochs.contains(&add.epoch))
                        .collect();
                    let log_path = Path::from(format!(
                        "{}/_delta_log/{:020}.json",
                        self.base_path, entry.version
                    ));
                    if remaining.is_empty() {
                        match self.store.inner().delete(&log_path).await {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(error) => return Err(SinkError::Io(error.to_string())),
                        }
                    } else {
                        let payload = serde_json::to_vec_pretty(&DeltaLogEntry {
                            version: entry.version,
                            adds: remaining,
                        })
                        .map_err(|error| SinkError::Io(error.to_string()))?;
                        self.store
                            .inner()
                            .put(&log_path, payload.into())
                            .await
                            .map_err(|error| SinkError::Io(error.to_string()))?;
                    }
                }
            }
            Ok(())
        })
    }

    fn delete_file(&mut self, path: &str) -> Result<u64, SinkError> {
        run_async(async {
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
        })
    }

    fn read_pending_deletes(&self) -> Result<Vec<String>, SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        run_async(async {
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
        })
    }

    fn write_pending_deletes(&mut self, paths: &[String]) -> Result<(), SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        let payload =
            serde_json::to_vec(paths).map_err(|error| SinkError::Io(error.to_string()))?;
        run_async(async {
            self.store
                .inner()
                .put(&path, payload.into())
                .await
                .map_err(|error| SinkError::Io(error.to_string()))?;
            Ok(())
        })
    }

    fn clear_pending_deletes(&mut self) -> Result<(), SinkError> {
        let path = Path::from(format!("{}/{}", self.base_path, GC_PENDING_DELETES_FILE));
        run_async(async {
            match self.store.inner().delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(SinkError::Io(error.to_string())),
            }
        })
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

fn run_async<F, T>(future: F) -> Result<T, SinkError>
where
    F: std::future::Future<Output = Result<T, SinkError>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SinkError::Io(error.to_string()))?
            .block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
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
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row_idx in 0..batch.num_rows() {
                rows.push(format!("{}|{}", ids.value(row_idx), names.value(row_idx)));
            }
        }
        rows.join("\n")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_five_snapshots_and_reads_them_back() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultInjectingObjectStore::new(inner));
        let mut sink = DeltaSink::new(ConnectorId(45), store, "delta-test");
        sink.set_parquet_row_group_bytes(256);

        let mut expected_batches = Vec::new();
        for epoch in 1..=5 {
            let batch = make_batch(epoch as i64);
            expected_batches.push(batch.clone());
            sink.set_staged_batch(batch.clone());
            let state = sink.pre_commit(epoch, batch.num_rows()).unwrap();
            sink.set_cluster_committed(epoch);
            sink.commit(epoch, &state).unwrap();
        }

        for (index, expected) in expected_batches.iter().enumerate() {
            let observed = sink.read_snapshot((index + 1) as u64).await.unwrap();
            assert_eq!(
                render_batches(&observed),
                render_batches(std::slice::from_ref(expected))
            );
        }

        let logs = sink.read_all_logs().await.unwrap();
        assert_eq!(logs.len(), 5);
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
        let mut sink = DeltaSink::new(ConnectorId(46), store, "delta-partitioned-test");
        sink.set_partition_by(vec!["region".to_string()]);

        let batch = make_partitioned_batch();
        sink.set_staged_batch(batch.clone());
        let state = sink.pre_commit(1, batch.num_rows()).unwrap();
        sink.set_cluster_committed(1);
        sink.commit(1, &state).unwrap();

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
        use std::sync::Mutex;

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultInjectingObjectStore::new(inner));
        let mut sink = DeltaSink::new(ConnectorId(47), store, "delta-gc-test");
        sink.set_cluster_committed(10);

        for epoch in 1..=4u64 {
            let batch = make_batch(epoch as i64);
            sink.set_staged_batch(batch.clone());
            let state = sink.pre_commit(epoch, batch.num_rows()).unwrap();
            sink.set_cluster_committed(epoch);
            sink.commit(epoch, &state).unwrap();
        }

        let snapshots_before = ColdGcCatalog::list_snapshots(&sink).unwrap();
        assert_eq!(snapshots_before.len(), 4);

        let catalog = Arc::new(Mutex::new(sink));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 2,
                retention_duration_ms: u64::MAX,
            },
        );
        let result = gc.run(0).unwrap();
        assert_eq!(result.expired_epochs, vec![1, 2]);
        assert_eq!(result.deleted_files.len(), 2);
        drop(gc);

        let remaining_epochs = {
            let sink = catalog.lock().unwrap();
            let snapshots_after = ColdGcCatalog::list_snapshots(&*sink).unwrap();
            let mut remaining_epochs: Vec<Epoch> =
                snapshots_after.iter().map(|s| s.epoch).collect();
            remaining_epochs.sort();
            remaining_epochs
        };
        assert_eq!(remaining_epochs, vec![3, 4]);

        // Drop the mutex lock before the `.await` points below (holding a
        // std::sync::MutexGuard across an await is flagged by clippy and is
        // a genuine footgun on a multi-threaded runtime), by taking the sink
        // out of the Arc<Mutex> now that GC mutation is complete.
        let sink = Arc::try_unwrap(catalog)
            .unwrap_or_else(|_| panic!("catalog still shared"))
            .into_inner()
            .unwrap();
        assert!(sink.read_snapshot(3).await.is_ok());
        assert!(sink.read_snapshot(4).await.is_ok());
    }
}
