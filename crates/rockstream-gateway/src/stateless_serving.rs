//! Stateless read-serving gateway router, freshness contract, and admission
//! protection (Issue #401: R1, R2, R3).
//!
//! - [`StatelessReadRouter`] dynamically discovers active shards from the
//!   control plane (`DataPlaneClient::discover_shards`), opens `ShardReader`s
//!   with sub-second manifest polling, shared RAM + optional NVMe disk caches,
//!   and `ManagedCheckpoint` horizon protection against shared object storage.
//! - Enforces explicit snapshot/freshness visibility: tracks per-shard and
//!   cluster serving frontiers, waits up to a bounded timeout for a requested
//!   freshness epoch, and fails explicitly on timeout instead of returning
//!   silently stale data.
//! - [`ReadAdmissionController`] bounds concurrency, queue depth, queue wait,
//!   estimated memory, and estimated scan rows under overload.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use object_store::ObjectStore;
use rockstream_runtime::DataPlaneClient;
use rockstream_storage::reader::{ServingReaderOptions, SharedServingCache};
use rockstream_storage::{build_runtime_object_store, ShardKeyEncoder, ShardReader};
use rockstream_types::config::ServingConfig;
use rockstream_types::ids::ShardId;
use rockstream_types::topology::ShardServingInfo;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::error::GatewayError;
use crate::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};

/// Source of active shard locations and published frontiers.
#[async_trait]
pub trait ShardDiscovery: Send + Sync {
    async fn discover(&self) -> Result<(Vec<ShardServingInfo>, Option<u64>), GatewayError>;
}

/// Control-plane discovery backed by [`DataPlaneClient`].
pub struct ControlShardDiscovery {
    client: Arc<DataPlaneClient>,
}

impl ControlShardDiscovery {
    pub fn new(client: Arc<DataPlaneClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ShardDiscovery for ControlShardDiscovery {
    async fn discover(&self) -> Result<(Vec<ShardServingInfo>, Option<u64>), GatewayError> {
        self.client
            .discover_shards()
            .await
            .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                detail: format!("shard discovery failed: {e}"),
            })
    }
}

/// Resolves a discovered shard into its `ObjectStore`.
#[async_trait]
pub trait ShardStoreResolver: Send + Sync {
    async fn resolve(&self, shard: &ShardServingInfo)
        -> Result<Arc<dyn ObjectStore>, GatewayError>;
}

/// Default runtime resolver using [`build_runtime_object_store`].
pub struct RuntimeShardStoreResolver {
    storage_root: PathBuf,
}

impl RuntimeShardStoreResolver {
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }
}

#[async_trait]
impl ShardStoreResolver for RuntimeShardStoreResolver {
    async fn resolve(
        &self,
        shard: &ShardServingInfo,
    ) -> Result<Arc<dyn ObjectStore>, GatewayError> {
        let local_path = self.storage_root.join(&shard.remote_prefix);
        build_runtime_object_store(&local_path, &shard.remote_prefix).map_err(|e| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("failed to open shard {} store: {e}", shard.shard_id.0),
            }
        })
    }
}

/// Snapshot of admission controller fill levels for metrics/observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadAdmissionSnapshot {
    pub active_reads: usize,
    pub queued_reads: usize,
    pub reserved_memory_bytes: usize,
    pub rejected_total: u64,
}

/// Enforces queue depth, queue wait, concurrency, memory, and query-estimate
/// bounds for stateless read serving (R3).
pub struct ReadAdmissionController {
    semaphore: Arc<Semaphore>,
    max_queue_depth: usize,
    max_queue_wait: Duration,
    max_memory_bytes: usize,
    max_estimated_scan_rows: usize,
    active_reads: Arc<AtomicUsize>,
    queued_reads: Arc<AtomicUsize>,
    reserved_memory_bytes: Arc<AtomicUsize>,
    rejected_total: Arc<AtomicU64>,
}

impl ReadAdmissionController {
    pub fn from_config(cfg: &ServingConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(cfg.max_concurrent_reads.max(1))),
            max_queue_depth: cfg.max_read_queue_depth,
            max_queue_wait: Duration::from_millis(cfg.max_read_queue_wait_ms.max(1)),
            max_memory_bytes: cfg.max_read_memory_bytes.max(1024),
            max_estimated_scan_rows: cfg.max_estimated_scan_rows.max(1),
            active_reads: Arc::new(AtomicUsize::new(0)),
            queued_reads: Arc::new(AtomicUsize::new(0)),
            reserved_memory_bytes: Arc::new(AtomicUsize::new(0)),
            rejected_total: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Admit a read query or reject cleanly under overload.
    pub async fn admit(
        &self,
        estimated_rows: Option<usize>,
        estimated_bytes: usize,
    ) -> Result<ReadAdmissionPermit, GatewayError> {
        if let Some(rows) = estimated_rows {
            if rows > self.max_estimated_scan_rows {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                return Err(GatewayError::ReadOverloaded {
                    reason: format!(
                        "estimated scan rows {rows} exceeds bound {}",
                        self.max_estimated_scan_rows
                    ),
                });
            }
        }

        // Check & reserve queue slot atomically.
        let prev_queued = self.queued_reads.fetch_add(1, Ordering::AcqRel);
        if prev_queued >= self.max_queue_depth {
            self.queued_reads.fetch_sub(1, Ordering::AcqRel);
            self.rejected_total.fetch_add(1, Ordering::Relaxed);
            return Err(GatewayError::ReadOverloaded {
                reason: format!(
                    "read admission queue full ({prev_queued} >= {})",
                    self.max_queue_depth
                ),
            });
        }

        let permit_res = tokio::time::timeout(
            self.max_queue_wait,
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await;
        self.queued_reads.fetch_sub(1, Ordering::AcqRel);

        let permit = match permit_res {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                return Err(GatewayError::ReadOverloaded {
                    reason: "read admission semaphore closed".to_owned(),
                });
            }
            Err(_) => {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                return Err(GatewayError::ReadOverloaded {
                    reason: format!(
                        "read admission queue wait exceeded {}ms",
                        self.max_queue_wait.as_millis()
                    ),
                });
            }
        };

        // Reserve memory budget with CAS loop.
        loop {
            let current = self.reserved_memory_bytes.load(Ordering::Acquire);
            let next = current.saturating_add(estimated_bytes);
            if next > self.max_memory_bytes {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                return Err(GatewayError::MemoryLimitExceeded);
            }
            if self
                .reserved_memory_bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        self.active_reads.fetch_add(1, Ordering::AcqRel);
        Ok(ReadAdmissionPermit {
            _permit: permit,
            reserved_bytes: estimated_bytes,
            active_reads: Arc::clone(&self.active_reads),
            reserved_memory_bytes: Arc::clone(&self.reserved_memory_bytes),
        })
    }

    pub fn snapshot(&self) -> ReadAdmissionSnapshot {
        ReadAdmissionSnapshot {
            active_reads: self.active_reads.load(Ordering::Acquire),
            queued_reads: self.queued_reads.load(Ordering::Acquire),
            reserved_memory_bytes: self.reserved_memory_bytes.load(Ordering::Acquire),
            rejected_total: self.rejected_total.load(Ordering::Acquire),
        }
    }
}

impl Default for ReadAdmissionController {
    fn default() -> Self {
        Self::from_config(&ServingConfig::default())
    }
}

/// RAII permit releasing concurrency and memory reservations on drop.
pub struct ReadAdmissionPermit {
    _permit: OwnedSemaphorePermit,
    reserved_bytes: usize,
    active_reads: Arc<AtomicUsize>,
    reserved_memory_bytes: Arc<AtomicUsize>,
}

impl Drop for ReadAdmissionPermit {
    fn drop(&mut self) {
        self.active_reads.fetch_sub(1, Ordering::AcqRel);
        self.reserved_memory_bytes
            .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
    }
}

struct DiscoveredShardReader {
    _info: ShardServingInfo,
    reader: Arc<ShardReader>,
    last_frontier: u64,
}

/// Stateless read-serving router that dynamically discovers shards from control,
/// maintains pinned/polling `ShardReader`s over shared object storage, publishes
/// the servable frontier, and enforces explicit freshness waits (R1, R2).
pub struct StatelessReadRouter {
    discovery: Arc<dyn ShardDiscovery>,
    store_resolver: Arc<dyn ShardStoreResolver>,
    serving_opts: ServingReaderOptions,
    shared_cache: SharedServingCache,
    shards: RwLock<BTreeMap<ShardId, DiscoveredShardReader>>,
    /// Published serving frontier (minimum committed epoch visible across all
    /// active shards). `u64::MAX` means no shards discovered yet.
    serving_frontier: AtomicU64,
    default_freshness_timeout: Duration,
}

impl StatelessReadRouter {
    pub fn new(
        discovery: Arc<dyn ShardDiscovery>,
        store_resolver: Arc<dyn ShardStoreResolver>,
        cfg: &ServingConfig,
    ) -> Self {
        let manifest_poll = Duration::from_millis(cfg.manifest_poll_interval_ms.max(20));
        let serving_opts = ServingReaderOptions {
            manifest_poll_interval: manifest_poll,
            checkpoint_lifetime: Duration::from_secs(60).max(manifest_poll * 4),
            nvme_cache_dir: cfg.nvme_cache_dir.clone(),
            nvme_cache_bytes: cfg.nvme_cache_bytes,
            filter_bits_per_key: Some(10),
            use_managed_checkpoint: true,
        };
        let shared_cache = SharedServingCache::new("stateless-gateway", cfg.ram_cache_bytes as u64);
        Self {
            discovery,
            store_resolver,
            serving_opts,
            shared_cache,
            shards: RwLock::new(BTreeMap::new()),
            serving_frontier: AtomicU64::new(u64::MAX),
            default_freshness_timeout: Duration::from_millis(cfg.freshness_wait_timeout_ms.max(50)),
        }
    }

    /// Bootstrap from the current cluster topology and pin initial readers.
    pub async fn bootstrap(&self) -> Result<Option<u64>, GatewayError> {
        self.refresh().await
    }

    /// Refresh shard discovery and read the latest committed frontier from each
    /// active `ShardReader`, returning the servable cluster frontier.
    pub async fn refresh(&self) -> Result<Option<u64>, GatewayError> {
        let (discovered, _control_frontier) = self.discovery.discover().await?;
        let discovered_ids: std::collections::BTreeSet<ShardId> =
            discovered.iter().map(|s| s.shard_id).collect();

        // Open readers for any newly discovered shards outside the write lock.
        let mut new_entries = Vec::new();
        {
            let existing = self.shards.read().await;
            for info in &discovered {
                if !existing.contains_key(&info.shard_id) {
                    let store = self.store_resolver.resolve(info).await?;
                    let reader = match ShardReader::open_for_serving(
                        info.db_path.clone(),
                        store,
                        self.serving_opts.clone(),
                        Some(&self.shared_cache),
                    )
                    .await
                    {
                        Ok(r) => Arc::new(r),
                        Err(e) => {
                            tracing::debug!(
                                shard = info.shard_id.0,
                                error = %e,
                                "stateless gateway: shard not yet readable"
                            );
                            continue;
                        }
                    };
                    let frontier = read_shard_frontier(&reader).await.unwrap_or(0);
                    new_entries.push((
                        info.shard_id,
                        DiscoveredShardReader {
                            _info: info.clone(),
                            reader,
                            last_frontier: frontier,
                        },
                    ));
                }
            }
        }

        let mut guard = self.shards.write().await;
        guard.retain(|id, _| discovered_ids.contains(id));
        for (id, entry) in new_entries {
            guard.insert(id, entry);
        }

        // Refresh each active shard's visible frontier from its ShardReader.
        let mut min_frontier: Option<u64> = None;
        for entry in guard.values_mut() {
            if let Ok(frontier) = read_shard_frontier(&entry.reader).await {
                entry.last_frontier = frontier;
            }
            min_frontier = Some(match min_frontier {
                Some(cur) => cur.min(entry.last_frontier),
                None => entry.last_frontier,
            });
        }

        self.serving_frontier
            .store(min_frontier.unwrap_or(u64::MAX), Ordering::Release);
        Ok(min_frontier)
    }

    /// The frontier epoch this stateless gateway can currently serve.
    pub fn published_serving_frontier(&self) -> Option<u64> {
        let raw = self.serving_frontier.load(Ordering::Acquire);
        (raw != u64::MAX).then_some(raw)
    }

    /// Number of active discovered shards.
    pub async fn shard_count(&self) -> usize {
        self.shards.read().await.len()
    }

    /// Wait until `published_serving_frontier() >= required_epoch`, or fail
    /// explicitly with [`GatewayError::FreshnessTimeout`] instead of returning
    /// silently stale data (R2).
    pub async fn wait_for_freshness(
        &self,
        required_epoch: u64,
        timeout: Option<Duration>,
    ) -> Result<u64, GatewayError> {
        let timeout = timeout.unwrap_or(self.default_freshness_timeout);
        if let Some(current) = self.published_serving_frontier() {
            if current >= required_epoch {
                return Ok(current);
            }
        }
        let deadline = Instant::now() + timeout;
        let poll_step = self
            .serving_opts
            .manifest_poll_interval
            .min(Duration::from_millis(50))
            .max(Duration::from_millis(10));
        loop {
            if let Ok(Some(current)) = self.refresh().await {
                if current >= required_epoch {
                    return Ok(current);
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(GatewayError::FreshnessTimeout {
                    required_epoch,
                    serving_frontier: self.published_serving_frontier(),
                    timeout_ms: timeout.as_millis() as u64,
                });
            }
            tokio::time::sleep((deadline - now).min(poll_step)).await;
        }
    }

    /// Read rows for `view_name` directly from all discovered `ShardReader`s
    /// over shared object storage, optionally waiting for `required_epoch`.
    pub async fn read_view(
        &self,
        view_name: &str,
        limit: Option<usize>,
        required_epoch: Option<u64>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        if let Some(epoch) = required_epoch {
            self.wait_for_freshness(epoch, timeout).await?;
        } else {
            let _ = self.refresh().await;
        }

        let readers: Vec<(Arc<ShardReader>, u64)> = {
            let guard = self.shards.read().await;
            guard
                .values()
                .map(|entry| (Arc::clone(&entry.reader), entry.last_frontier))
                .collect()
        };

        if readers.is_empty() {
            return Err(GatewayError::QueryTimeScatterTopologyUnavailable);
        }

        let mut merged = Vec::new();
        for (reader, frontier) in readers {
            let view_reader = HotOnlyViewReader {
                shard_reader: reader,
                frontier_epoch: Some(frontier),
            };
            let rows = view_reader
                .read_view(view_name, limit, ViewReadStrategy::HotOnly)
                .await?;
            merged.extend(rows);
            if let Some(max) = limit {
                if merged.len() >= max {
                    merged.truncate(max);
                    break;
                }
            }
        }
        Ok(merged)
    }
}

async fn read_shard_frontier(reader: &ShardReader) -> Result<u64, GatewayError> {
    let key = ShardKeyEncoder::frontier_key();
    match reader.get(&key).await? {
        Some(bytes) if bytes.len() == 8 => {
            let arr: [u8; 8] = bytes[..8]
                .try_into()
                .map_err(|_| GatewayError::QueryTimeScatterTopologyUnavailable)?;
            Ok(u64::from_be_bytes(arr))
        }
        Some(_) => Err(GatewayError::QueryTimeScatterTopologyUnavailable),
        None => Ok(0),
    }
}
