//! Unified whole-process worker resource budget controller (Issue #402 R4).
//!
//! Coordinates every memory and disk pool in one worker process:
//! - Operator state & transactional overlays ([`MemoryCategory::OperatorState`])
//! - SlateDB write buffers ([`MemoryCategory::SlateDbWriteBuffers`])
//! - Worker-wide decoded block & metadata caches vs. raw object-store cache
//!   ([`WorkerStorageContext`] + [`WorkerCacheBudget`], charged ONCE across all shards)
//! - Source decoding buffers ([`MemoryCategory::SourceBuffers`])
//! - Exchange & shard-actor queues ([`MemoryCategory::ExchangeBuffers`])
//! - Foreground query & subscription buffers ([`MemoryCategory::QueryWorkMemory`])
//! - Background checkpoint/spill staging ([`MemoryCategory::CheckpointStaging`])
//! - Real OS process RSS and disk occupancy calibration ([`RssCalibrationReport`])

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_storage::{ShardDb, StorageError, WorkerStorageContext};
use rockstream_types::state_budget::{
    measure_directory_bytes, sample_process_rss_bytes, MemoryCategory, MemoryPermit,
    RssCalibrationReport, StateBudgetError, WorkerBudgetLedger, WorkerCacheBudget,
};

use crate::source_pressure::SourcePressureController;
use crate::spill_governor::{SpillGovernor, SpillGovernorConfig};

/// Configuration for a whole-process [`WorkerResourceController`].
#[derive(Debug, Clone)]
pub struct WorkerResourceConfig {
    /// Worker identifier used in telemetry & calibration reports.
    pub worker_id: String,
    /// Hard whole-process memory ceiling across all categories (bytes).
    pub total_memory_budget_bytes: usize,
    /// Memory reserved exclusively for foreground queries/subscriptions (bytes).
    pub foreground_reservation_bytes: usize,
    /// Worker-wide cache budget separating decoded RAM caches from raw disk cache.
    pub cache_budget: WorkerCacheBudget,
    /// Spill staging, concurrency, and disk occupancy ceilings.
    pub spill_config: SpillGovernorConfig,
    /// Base CDC source ingestion poll credits.
    pub source_base_credits: u32,
    /// Optional local storage root directory used for disk occupancy calibration.
    pub disk_root: Option<PathBuf>,
}

impl Default for WorkerResourceConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-0".to_string(),
            total_memory_budget_bytes: 512 * 1024 * 1024,
            foreground_reservation_bytes: 32 * 1024 * 1024,
            cache_budget: WorkerCacheBudget::new(
                64 * 1024 * 1024,
                16 * 1024 * 1024,
                256 * 1024 * 1024,
            ),
            spill_config: SpillGovernorConfig::default(),
            source_base_credits: 64,
            disk_root: None,
        }
    }
}

/// Whole-process resource controller unifying memory ledger, worker-wide storage
/// caches, spill governance, source pressure, and OS RSS/disk calibration.
#[derive(Debug)]
pub struct WorkerResourceController {
    worker_id: String,
    ledger: Arc<WorkerBudgetLedger>,
    storage_context: Arc<WorkerStorageContext>,
    spill_governor: Arc<SpillGovernor>,
    source_pressure: Arc<SourcePressureController>,
    disk_root: Option<PathBuf>,
}

impl WorkerResourceController {
    /// Initialize a unified worker resource controller, capture the initial
    /// process RSS baseline, and charge the shared worker-wide cache budget
    /// exactly once to [`MemoryCategory::BlockAndMetadataCaches`].
    pub fn new(config: WorkerResourceConfig) -> Result<Self, StateBudgetError> {
        let ledger = Arc::new(WorkerBudgetLedger::new(
            config.total_memory_budget_bytes,
            config.foreground_reservation_bytes,
        ));
        if let Ok(baseline_rss) = sample_process_rss_bytes() {
            ledger.set_rss_calibration_baseline(baseline_rss);
        }

        let storage_context = Arc::new(WorkerStorageContext::from_cache_budget(
            &config.worker_id,
            config.cache_budget,
        ));
        storage_context.charge_to_budget_ledger(&ledger)?;

        let spill_governor = Arc::new(SpillGovernor::new(ledger.clone(), config.spill_config));
        let source_pressure = Arc::new(SourcePressureController::new(
            ledger.clone(),
            config.source_base_credits,
        ));

        Ok(Self {
            worker_id: config.worker_id,
            ledger,
            storage_context,
            spill_governor,
            source_pressure,
            disk_root: config.disk_root,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn ledger(&self) -> &Arc<WorkerBudgetLedger> {
        &self.ledger
    }

    pub fn storage_context(&self) -> &Arc<WorkerStorageContext> {
        &self.storage_context
    }

    pub fn cache_budget(&self) -> WorkerCacheBudget {
        self.storage_context.cache_budget()
    }

    pub fn spill_governor(&self) -> &Arc<SpillGovernor> {
        &self.spill_governor
    }

    pub fn source_pressure(&self) -> &Arc<SourcePressureController> {
        &self.source_pressure
    }

    /// Open a [`ShardDb`] bound to this worker's shared [`WorkerStorageContext`]
    /// so opening N shards shares one worker-wide cache pool rather than
    /// multiplying decoded or raw cache budgets per shard.
    pub async fn open_shard_db(
        &self,
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<ShardDb>, StorageError> {
        let db = ShardDb::builder(db_path, object_store)
            .with_storage_context(self.storage_context.clone())
            .build()
            .await?;
        Ok(Arc::new(db))
    }

    /// Charge operator state or transactional overlay memory.
    pub fn acquire_operator_state(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::OperatorState, bytes, false)
    }

    /// Charge SlateDB write buffer memory.
    pub fn acquire_write_buffer(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::SlateDbWriteBuffers, bytes, false)
    }

    /// Charge source decoding buffer memory.
    pub fn acquire_source_decode(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::SourceBuffers, bytes, false)
    }

    /// Charge exchange / shard-actor queue memory.
    pub fn acquire_exchange_buffer(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::ExchangeBuffers, bytes, false)
    }

    /// Charge foreground query / subscription buffer memory (honors foreground reservation).
    pub fn acquire_foreground_query(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::QueryWorkMemory, bytes, true)
    }

    /// Charge background compaction/checkpoint/spill staging memory.
    pub fn acquire_background_work(&self, bytes: u64) -> Result<MemoryPermit, StateBudgetError> {
        self.ledger
            .try_acquire(MemoryCategory::CheckpointStaging, bytes, false)
    }

    /// Sample real OS process RSS and local storage disk occupancy, synchronize
    /// them into the budget ledger, and return an exact calibration report.
    pub fn calibrate_resources(
        &self,
        override_disk_root: Option<&Path>,
    ) -> Result<RssCalibrationReport, String> {
        let disk_path = override_disk_root.or(self.disk_root.as_deref());
        let disk_used_bytes = match disk_path {
            Some(path) => measure_directory_bytes(path).map_err(|e| e.to_string())?,
            None => self.spill_governor.current_disk_occupancy(),
        };
        let disk_budget_bytes = self.spill_governor.config().max_disk_occupancy_bytes;
        self.ledger
            .calibrate_against_rss(&self.worker_id, disk_used_bytes, disk_budget_bytes)
    }

    /// Return exact allocated bytes for every canonical memory category.
    pub fn category_breakdown(&self) -> [(MemoryCategory, u64); 8] {
        let cats = MemoryCategory::all();
        [
            (cats[0], self.ledger.category_bytes(cats[0])),
            (cats[1], self.ledger.category_bytes(cats[1])),
            (cats[2], self.ledger.category_bytes(cats[2])),
            (cats[3], self.ledger.category_bytes(cats[3])),
            (cats[4], self.ledger.category_bytes(cats[4])),
            (cats[5], self.ledger.category_bytes(cats[5])),
            (cats[6], self.ledger.category_bytes(cats[6])),
            (cats[7], self.ledger.category_bytes(cats[7])),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    #[tokio::test]
    async fn shared_worker_cache_budget_charged_once_across_many_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = WorkerCacheBudget::new(4 * 1024 * 1024, 1024 * 1024, 16 * 1024 * 1024);
        let config = WorkerResourceConfig {
            worker_id: "test-worker".to_string(),
            total_memory_budget_bytes: 64 * 1024 * 1024,
            foreground_reservation_bytes: 8 * 1024 * 1024,
            cache_budget: cache,
            spill_config: SpillGovernorConfig {
                max_disk_occupancy_bytes: 64 * 1024 * 1024,
                ..SpillGovernorConfig::default()
            },
            source_base_credits: 32,
            disk_root: Some(tmp.path().to_path_buf()),
        };

        let controller = WorkerResourceController::new(config).unwrap();
        let expected_cache_ram = cache.total_ram_bytes() as u64;
        assert_eq!(
            controller
                .ledger()
                .category_bytes(MemoryCategory::BlockAndMetadataCaches),
            expected_cache_ram
        );

        let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
        let mut dbs = Vec::new();
        for shard_idx in 0..8 {
            let db = controller
                .open_shard_db(format!("shard-{shard_idx}"), store.clone())
                .await
                .unwrap();
            dbs.push(db);
        }

        // Opening 8 shards must NOT multiply BlockAndMetadataCaches by 8.
        assert_eq!(controller.storage_context().registered_shards(), 8);
        assert_eq!(
            controller
                .ledger()
                .category_bytes(MemoryCategory::BlockAndMetadataCaches),
            expected_cache_ram
        );
        assert_eq!(controller.cache_budget(), cache);

        let report = controller.calibrate_resources(None).unwrap();
        assert!(report.observed_rss_bytes > 0);
        assert!(report.within_disk_budget);
    }
}
