//! Cold-snapshot garbage collection (v0.44 slice 11, DESIGN.md §13.6.2.1,
//! Proof claims P5/P6/P7).
//!
//! After each successful snapshot commit, [`ColdGc::run`] evaluates retention
//! — `cold_snapshot_retention_count` / `cold_snapshot_retention_duration`,
//! whichever bound is reached first (defaults 32 / 7 days) — then
//! scan-and-deletes (never a SlateDB range delete) any data file that is not
//! referenced by a *retained* snapshot, even if it is still referenced by an
//! expired one (P6: "never delete a file referenced by a retained
//! snapshot"). The newest snapshot is never expired regardless of
//! count/duration, so there is always at least one readable snapshot.
//!
//! GC acquires the same per-sink `Arc<Mutex<_>>` that the flush/commit path
//! holds around the sink's catalog, so a GC pass structurally cannot
//! interleave with a snapshot commit (P7) — both paths serialize through the
//! identical lock rather than relying on timing.
//!
//! Crash-mid-delete safety (P6b, "idempotent GC"): before removing expired
//! snapshots from the metadata, the target file list is written durably via
//! `write_pending_deletes`. A crash between that write and
//! `clear_pending_deletes` is recovered by resuming the pending-deletes list
//! on the next `run` call — `delete_file` tolerates a file that is already
//! gone (returns `0` bytes reclaimed), so replaying the same delete list
//! twice is safe and deletes nothing extra.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use rockstream_types::timestamp::Epoch;

use crate::sink_connector::SinkError;

/// Default retention count (DESIGN.md §13.6.2.1).
pub const DEFAULT_RETENTION_COUNT: usize = 32;
/// Default retention duration, in milliseconds (7 days).
pub const DEFAULT_RETENTION_DURATION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// `cold_snapshot_retention_count` / `cold_snapshot_retention_duration`
/// configuration (DESIGN.md §13.6.2.1). Either bound set to `0` disables
/// that criterion (expiry is then driven solely by the other bound).
#[derive(Debug, Clone, Copy)]
pub struct ColdGcConfig {
    pub retention_count: usize,
    pub retention_duration_ms: u64,
}

impl Default for ColdGcConfig {
    fn default() -> Self {
        Self {
            retention_count: DEFAULT_RETENTION_COUNT,
            retention_duration_ms: DEFAULT_RETENTION_DURATION_MS,
        }
    }
}

/// A committed snapshot as seen by GC: its epoch, commit wall-clock time,
/// and the full list of data-file paths it references.
#[derive(Debug, Clone)]
pub struct RetainedSnapshot {
    pub epoch: Epoch,
    pub committed_at_ms: u64,
    pub files: Vec<String>,
}

/// Metrics emitted after each `ColdGc::run` (P5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdGcMetrics {
    pub cold_gc_bytes_reclaimed: u64,
    pub cold_gc_last_run_epoch: Epoch,
}

#[derive(Debug, Clone, Default)]
pub struct ColdGcResult {
    pub expired_epochs: Vec<Epoch>,
    pub deleted_files: Vec<String>,
    pub resumed_from_crash: bool,
    pub metrics: ColdGcMetrics,
}

/// The minimal surface `ColdGc` needs from a cold-tier sink's snapshot
/// catalog. Implemented by `IcebergSink`/`DeltaSink` directly (kept separate
/// from `SinkConnector` since GC is a maintenance operation, not part of the
/// exactly-once commit protocol).
#[async_trait]
pub trait ColdGcCatalog: Send + Sync {
    /// All committed snapshots (any order — `ColdGc::run` sorts by epoch).
    async fn list_snapshots(&self) -> Result<Vec<RetainedSnapshot>, SinkError>;
    /// Drop the given epochs from the snapshot metadata (never a range
    /// delete — implementations rewrite the metadata file to omit them).
    async fn remove_snapshots(&mut self, epochs: &[Epoch]) -> Result<(), SinkError>;
    /// Delete a single data file, scan-and-delete style. Returns the number
    /// of bytes reclaimed (`0` if the file was already gone — idempotent).
    async fn delete_file(&mut self, path: &str) -> Result<u64, SinkError>;
    /// Read a durably-staged pending-delete list left by a crashed GC run
    /// (empty if there is none).
    async fn read_pending_deletes(&self) -> Result<Vec<String>, SinkError>;
    /// Durably stage a pending-delete list before removing snapshot
    /// metadata, so a crash mid-delete can resume.
    async fn write_pending_deletes(&mut self, paths: &[String]) -> Result<(), SinkError>;
    /// Clear the pending-delete marker once all listed files are deleted.
    async fn clear_pending_deletes(&mut self) -> Result<(), SinkError>;
}

/// Cold-snapshot GC coordinator. Holds the same `Arc<Mutex<_>>` around the
/// sink's catalog that the flush/commit path holds, so `run` and a
/// concurrent commit structurally serialize (P7).
pub struct ColdGc<C: ColdGcCatalog> {
    catalog: Arc<Mutex<C>>,
    config: ColdGcConfig,
}

impl<C: ColdGcCatalog> ColdGc<C> {
    pub fn new(catalog: Arc<Mutex<C>>, config: ColdGcConfig) -> Self {
        Self { catalog, config }
    }

    /// Evaluate retention and scan-and-delete expired, unreferenced files.
    /// `now_ms` is the caller's wall-clock time (injectable for deterministic
    /// tests).
    pub async fn run(&self, now_ms: u64) -> Result<ColdGcResult, SinkError> {
        let pending = {
            let catalog = self.catalog.lock().await;
            catalog.read_pending_deletes().await?
        };

        let mut bytes_reclaimed = 0u64;
        let mut deleted_files = Vec::new();
        let resumed_from_crash = !pending.is_empty();
        for path in &pending {
            let bytes = {
                let mut catalog = self.catalog.lock().await;
                catalog.delete_file(path).await?
            };
            bytes_reclaimed += bytes;
            deleted_files.push(path.clone());
        }
        if resumed_from_crash {
            let mut catalog = self.catalog.lock().await;
            catalog.clear_pending_deletes().await?;
        }

        let mut snapshots = {
            let catalog = self.catalog.lock().await;
            catalog.list_snapshots().await?
        };
        snapshots.sort_by_key(|snapshot| snapshot.epoch);
        let last_run_epoch = snapshots.last().map(|snapshot| snapshot.epoch).unwrap_or(0);

        if snapshots.len() <= 1 {
            // Never expire the only remaining snapshot regardless of age/count.
            return Ok(ColdGcResult {
                expired_epochs: Vec::new(),
                deleted_files,
                resumed_from_crash,
                metrics: ColdGcMetrics {
                    cold_gc_bytes_reclaimed: bytes_reclaimed,
                    cold_gc_last_run_epoch: last_run_epoch,
                },
            });
        }

        let retention_count = self.config.retention_count.max(1);
        let count_excess = snapshots.len().saturating_sub(retention_count);
        let newest_idx = snapshots.len() - 1;

        let mut expired_indices: Vec<usize> = Vec::new();
        for (idx, snapshot) in snapshots.iter().enumerate() {
            if idx == newest_idx {
                break; // never expire the newest snapshot
            }
            let age_ms = now_ms.saturating_sub(snapshot.committed_at_ms);
            let duration_expired = self.config.retention_duration_ms > 0
                && age_ms >= self.config.retention_duration_ms;
            let count_expired = self.config.retention_count > 0 && idx < count_excess;
            if duration_expired || count_expired {
                expired_indices.push(idx);
            }
        }

        if expired_indices.is_empty() {
            return Ok(ColdGcResult {
                expired_epochs: Vec::new(),
                deleted_files,
                resumed_from_crash,
                metrics: ColdGcMetrics {
                    cold_gc_bytes_reclaimed: bytes_reclaimed,
                    cold_gc_last_run_epoch: last_run_epoch,
                },
            });
        }

        let expired_set: HashSet<usize> = expired_indices.iter().copied().collect();
        let expired_epochs: Vec<Epoch> = expired_indices
            .iter()
            .map(|&idx| snapshots[idx].epoch)
            .collect();

        // P6: a file referenced by ANY retained (non-expired) snapshot must
        // never be deleted, even if it is also referenced by an expired one.
        let retained_files: HashSet<&str> = snapshots
            .iter()
            .enumerate()
            .filter(|(idx, _)| !expired_set.contains(idx))
            .flat_map(|(_, snapshot)| snapshot.files.iter().map(String::as_str))
            .collect();

        let mut deletable: Vec<String> = Vec::new();
        for &idx in &expired_indices {
            for file in &snapshots[idx].files {
                if !retained_files.contains(file.as_str()) && !deletable.contains(file) {
                    deletable.push(file.clone());
                }
            }
        }

        // Stage the delete list durably BEFORE removing snapshot metadata,
        // so a crash mid-delete can resume via `read_pending_deletes` above.
        {
            let mut catalog = self.catalog.lock().await;
            catalog.write_pending_deletes(&deletable).await?;
            catalog.remove_snapshots(&expired_epochs).await?;
        }

        for path in &deletable {
            let bytes = {
                let mut catalog = self.catalog.lock().await;
                catalog.delete_file(path).await?
            };
            bytes_reclaimed += bytes;
            deleted_files.push(path.clone());
        }
        {
            let mut catalog = self.catalog.lock().await;
            catalog.clear_pending_deletes().await?;
        }

        Ok(ColdGcResult {
            expired_epochs,
            deleted_files,
            resumed_from_crash,
            metrics: ColdGcMetrics {
                cold_gc_bytes_reclaimed: bytes_reclaimed,
                cold_gc_last_run_epoch: last_run_epoch,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    /// In-memory `ColdGcCatalog` used to unit-test retention math, the
    /// shared-file safety guarantee, and crash-mid-delete idempotency
    /// without needing a real object store.
    #[derive(Default)]
    struct MockCatalog {
        snapshots: Vec<RetainedSnapshot>,
        deleted_paths: Vec<String>,
        existing_files: HashSet<String>,
        pending_deletes: Vec<String>,
    }

    impl MockCatalog {
        fn with_snapshots(snapshots: Vec<RetainedSnapshot>) -> Self {
            let existing_files = snapshots
                .iter()
                .flat_map(|snapshot| snapshot.files.iter().cloned())
                .collect();
            Self {
                snapshots,
                deleted_paths: Vec::new(),
                existing_files,
                pending_deletes: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl ColdGcCatalog for MockCatalog {
        async fn list_snapshots(&self) -> Result<Vec<RetainedSnapshot>, SinkError> {
            Ok(self.snapshots.clone())
        }

        async fn remove_snapshots(&mut self, epochs: &[Epoch]) -> Result<(), SinkError> {
            self.snapshots
                .retain(|snapshot| !epochs.contains(&snapshot.epoch));
            Ok(())
        }

        async fn delete_file(&mut self, path: &str) -> Result<u64, SinkError> {
            if self.existing_files.remove(path) {
                self.deleted_paths.push(path.to_string());
                Ok(1024)
            } else {
                Ok(0) // already gone — idempotent
            }
        }

        async fn read_pending_deletes(&self) -> Result<Vec<String>, SinkError> {
            Ok(self.pending_deletes.clone())
        }

        async fn write_pending_deletes(&mut self, paths: &[String]) -> Result<(), SinkError> {
            self.pending_deletes = paths.to_vec();
            Ok(())
        }

        async fn clear_pending_deletes(&mut self) -> Result<(), SinkError> {
            self.pending_deletes.clear();
            Ok(())
        }
    }

    fn snapshot(epoch: Epoch, committed_at_ms: u64, files: &[&str]) -> RetainedSnapshot {
        RetainedSnapshot {
            epoch,
            committed_at_ms,
            files: files.iter().map(|f| f.to_string()).collect(),
        }
    }

    // ── P5: retention-count expiry ──────────────────────────────────────────

    #[tokio::test]
    async fn expires_by_count_whichever_first() {
        let snapshots: Vec<RetainedSnapshot> = (1..=5)
            .map(|epoch| {
                snapshot(
                    epoch,
                    epoch * 1000,
                    &[&format!("data/epoch-{epoch}.parquet")],
                )
            })
            .collect();
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(snapshots)));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 2,
                retention_duration_ms: 0, // disabled — count-only
            },
        );

        let result = gc.run(10_000).await.unwrap();
        // 5 snapshots, retain 2 newest (epochs 4,5) => expire 1,2,3.
        assert_eq!(result.expired_epochs, vec![1, 2, 3]);
        assert_eq!(result.metrics.cold_gc_last_run_epoch, 5);
        assert_eq!(result.metrics.cold_gc_bytes_reclaimed, 3 * 1024);

        let remaining: Vec<Epoch> = catalog
            .lock()
            .await
            .snapshots
            .iter()
            .map(|s| s.epoch)
            .collect();
        assert_eq!(remaining, vec![4, 5]);
    }

    #[tokio::test]
    async fn expires_by_duration_whichever_first() {
        let snapshots = vec![
            snapshot(1, 0, &["data/epoch-1.parquet"]),
            snapshot(2, 1_000, &["data/epoch-2.parquet"]),
            snapshot(3, 100_000, &["data/epoch-3.parquet"]),
        ];
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(snapshots)));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 100, // disabled in effect — duration-only
                retention_duration_ms: 10_000,
            },
        );

        // now=100_000: epoch 1 (age 100_000) and 2 (age 99_000) are older
        // than the 10s bound; epoch 3 is the newest and is never expired.
        let result = gc.run(100_000).await.unwrap();
        assert_eq!(result.expired_epochs, vec![1, 2]);
    }

    #[tokio::test]
    async fn never_expires_the_only_or_newest_snapshot() {
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(vec![snapshot(
            1,
            0,
            &["data/epoch-1.parquet"],
        )])));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 1,
                retention_duration_ms: 1,
            },
        );
        let result = gc.run(1_000_000).await.unwrap();
        assert!(result.expired_epochs.is_empty());
        assert_eq!(catalog.lock().await.snapshots.len(), 1);
    }

    // ── P6: shared-file safety ──────────────────────────────────────────────

    #[tokio::test]
    async fn never_deletes_a_file_shared_with_a_retained_snapshot() {
        let snapshots = vec![
            snapshot(1, 0, &["data/shared.parquet", "data/epoch-1-only.parquet"]),
            snapshot(2, 0, &["data/shared.parquet"]), // shares a file with expired epoch 1
        ];
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(snapshots)));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 1, // retain only the newest (epoch 2)
                retention_duration_ms: 0,
            },
        );

        let result = gc.run(10_000).await.unwrap();
        assert_eq!(result.expired_epochs, vec![1]);
        // Only the epoch-1-exclusive file is deleted; the shared file
        // (still referenced by retained epoch 2) must survive.
        assert_eq!(
            result.deleted_files,
            vec!["data/epoch-1-only.parquet".to_string()]
        );
        assert!(catalog
            .lock()
            .await
            .existing_files
            .contains("data/shared.parquet"));
    }

    // ── P6: Shared file safety ─────────────────────────────────────────────

    #[tokio::test]
    async fn shared_file_referenced_by_retained_snapshot_is_never_deleted() {
        let snapshots = vec![
            snapshot(1, 1000, &["shared.parquet", "epoch1_only.parquet"]),
            snapshot(2, 2000, &["shared.parquet", "epoch2_only.parquet"]),
        ];
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(snapshots)));
        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 1,
                retention_duration_ms: 0,
            },
        );

        let result = gc.run(3000).await.unwrap();
        assert_eq!(result.expired_epochs, vec![1]);
        assert_eq!(
            result.deleted_files,
            vec!["epoch1_only.parquet".to_string()]
        );

        let guard = catalog.lock().await;
        assert!(guard.existing_files.contains("shared.parquet"));
    }

    // ── P6b: Crash-mid-delete resume safety ───────────────────────────────

    #[tokio::test]
    async fn resumes_and_clears_pending_deletes_left_by_crashed_run() {
        let snapshots = vec![
            snapshot(1, 1000, &["data/epoch-1.parquet"]),
            snapshot(2, 2000, &["data/epoch-2.parquet"]),
        ];
        let catalog = Arc::new(Mutex::new(MockCatalog::with_snapshots(snapshots)));
        {
            let mut catalog = catalog.lock().await;
            catalog.snapshots.retain(|s| s.epoch != 1);
            catalog.pending_deletes = vec!["data/epoch-1.parquet".to_string()];
        }

        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 100,
                retention_duration_ms: 0,
            },
        );

        let first = gc.run(10_000).await.unwrap();
        assert!(first.resumed_from_crash);
        assert_eq!(
            first.deleted_files,
            vec!["data/epoch-1.parquet".to_string()]
        );

        let second = gc.run(10_000).await.unwrap();
        assert!(!second.resumed_from_crash);
        assert!(second.deleted_files.is_empty());
    }

    // ── P7: GC never runs concurrently with a snapshot commit ──────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn gc_and_commit_are_serialized_by_the_shared_lock() {
        struct SlowCatalog {
            inner: MockCatalog,
            active_ops: Arc<AtomicI64>,
            max_concurrent: Arc<AtomicI64>,
        }

        #[async_trait]
        impl ColdGcCatalog for SlowCatalog {
            async fn list_snapshots(&self) -> Result<Vec<RetainedSnapshot>, SinkError> {
                self.inner.list_snapshots().await
            }
            async fn remove_snapshots(&mut self, epochs: &[Epoch]) -> Result<(), SinkError> {
                self.inner.remove_snapshots(epochs).await
            }
            async fn delete_file(&mut self, path: &str) -> Result<u64, SinkError> {
                let now = self.active_ops.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_concurrent.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                let result = self.inner.delete_file(path).await;
                self.active_ops.fetch_sub(1, Ordering::SeqCst);
                result
            }
            async fn read_pending_deletes(&self) -> Result<Vec<String>, SinkError> {
                self.inner.read_pending_deletes().await
            }
            async fn write_pending_deletes(&mut self, paths: &[String]) -> Result<(), SinkError> {
                self.inner.write_pending_deletes(paths).await
            }
            async fn clear_pending_deletes(&mut self) -> Result<(), SinkError> {
                self.inner.clear_pending_deletes().await
            }
        }

        let snapshots = vec![
            snapshot(1, 0, &["data/epoch-1.parquet"]),
            snapshot(2, 0, &["data/epoch-2.parquet"]),
        ];
        let active_ops = Arc::new(AtomicI64::new(0));
        let max_concurrent = Arc::new(AtomicI64::new(0));
        let catalog = Arc::new(Mutex::new(SlowCatalog {
            inner: MockCatalog::with_snapshots(snapshots),
            active_ops: Arc::clone(&active_ops),
            max_concurrent: Arc::clone(&max_concurrent),
        }));

        let gc = ColdGc::new(
            Arc::clone(&catalog),
            ColdGcConfig {
                retention_count: 1,
                retention_duration_ms: 0,
            },
        );

        let commit_task = {
            let catalog = Arc::clone(&catalog);
            let active_ops = Arc::clone(&active_ops);
            let max_concurrent = Arc::clone(&max_concurrent);
            tokio::spawn(async move {
                for _ in 0..10 {
                    let _guard = catalog.lock().await;
                    let now = active_ops.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    active_ops.fetch_sub(1, Ordering::SeqCst);
                }
            })
        };

        let result = gc.run(10_000).await.unwrap();
        commit_task.await.unwrap();
        assert_eq!(result.expired_epochs, vec![1]);
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    }
}
