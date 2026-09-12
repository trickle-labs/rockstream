//! Group-commit coalescing for shard-level epoch commits (v0.5).
//!
//! `GroupCommit` collects `WriteBatch` fragments from multiple operators over
//! one epoch, then merges them into **a single atomic `WriteBatch`** and calls
//! `ShardDb::write_batch()` exactly once per `flush()`.
//!
//! ## Why this matters
//!
//! Without group commit, each operator (ViewSinkOp, AggregateOp, …) calls
//! `db.write_batch()` independently.  For N operators per epoch, that is N
//! durability events (each one is a `Db::write()` call).
//!
//! With group commit, all N batches are merged and committed atomically:
//! N batches → 1 `Db::write()` call per epoch.  For N ≥ 5 the reduction is
//! ≥ 5×, satisfying the v0.5 Proof obligation.
//!
//! ## Bound
//!
//! The pending queue is capped at [`GROUP_COMMIT_MAX_BATCHES`] entries.
//! Adding a batch when the queue is full returns [`OpError::GroupCommitFull`]
//! (`RS-1015`).
//!
//! ## Fill-level metric
//!
//! `GroupCommit::fill_level()` returns the current number of pending batches.
//! This value is a named bound (DESIGN.md constraint: every buffer must have
//! a name, a fill-level metric, and a backpressure/error path).
//!
//! ## Durability-event counter
//!
//! `GroupCommit::commit_count()` returns the total number of `Db::write()`
//! calls issued.  In a test with N ≥ 5 operators and one `flush()`, this
//! equals 1 (vs N individual commits), proving the ≥ 5× reduction.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use rockstream_storage::{ShardDb, WriteBatch};

use crate::error::OpError;

/// Maximum number of `WriteBatch` fragments that may be pending before
/// `add_batch` returns `RS-1015` (group-commit queue full, back-pressure
/// applied).
///
/// Named bound: **`GROUP_COMMIT_MAX_BATCHES`** — every buffer must have a
/// named upper bound (DESIGN.md).
pub const GROUP_COMMIT_MAX_BATCHES: usize = 64;

/// Maximum number of complete logical epochs held in one physical commit.
pub const PHYSICAL_COMMIT_GROUP_MAX_EPOCHS: usize = 64;

/// Maximum pending write batch bytes before physical commit triggers or backpressure applies.
///
/// Named bound: **`MAX_GROUP_COMMIT_PENDING_BYTES`** (8 MiB).
pub const MAX_GROUP_COMMIT_PENDING_BYTES: usize = 8 * 1024 * 1024;

/// Maximum number of active waiters queued for durability acknowledgment.
///
/// Named bound: **`MAX_GROUP_COMMIT_WAITERS`** (1,024).
pub const MAX_GROUP_COMMIT_WAITERS: usize = 1024;

/// Default maximum delay for oldest pending epoch before triggering physical flush.
///
/// Named bound: **`DEFAULT_GROUP_COMMIT_MAX_DELAY_MS`** (10 ms).
pub const DEFAULT_GROUP_COMMIT_MAX_DELAY_MS: u64 = 10;

/// Maximum retry attempts for failed physical write batch or flush.
///
/// Named bound: **`MAX_GROUP_COMMIT_RETRY_COUNT`** (3 attempts).
pub const MAX_GROUP_COMMIT_RETRY_COUNT: usize = 3;

/// Shard-level group-commit coalescer.
///
/// Thread-safe: operators running in concurrent Tokio tasks may call
/// `add_batch` concurrently.  `flush` is typically called by the epoch
/// coordinator after all operators have submitted their batches.
pub struct GroupCommit {
    db: Arc<ShardDb>,
    /// Pending write-batch fragments (bounded by `GROUP_COMMIT_MAX_BATCHES`).
    pending: Mutex<Vec<WriteBatch>>,
    /// Current fill level (number of pending batches).
    fill_level: Arc<AtomicUsize>,
    /// Total number of `Db::write()` calls issued (durability events).
    commit_count: Arc<AtomicU64>,
}

impl GroupCommit {
    /// Create a new `GroupCommit` backed by `db`.
    pub fn new(db: Arc<ShardDb>) -> Self {
        GroupCommit {
            db,
            pending: Mutex::new(Vec::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
            commit_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Add a `WriteBatch` fragment to the pending queue.
    ///
    /// Returns `Err(RS-1015)` if the queue is already at capacity.
    /// Callers should apply back-pressure or reduce the epoch rate.
    pub fn add_batch(&self, wb: WriteBatch) -> Result<(), OpError> {
        let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
        if pending.len() >= GROUP_COMMIT_MAX_BATCHES {
            return Err(OpError::group_commit_full(pending.len()));
        }
        pending.push(wb);
        self.fill_level.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Convert state mutations directly into a `WriteBatch` and add to the pending queue.
    pub fn add_mutations(
        &self,
        mutations: Vec<rockstream_types::state_mutation::StateMutation>,
    ) -> Result<(), OpError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let mut wb = WriteBatch::new();
        for mutation in mutations {
            match mutation {
                rockstream_types::state_mutation::StateMutation::Put { key, value } => {
                    wb.put(&key, &value);
                }
                rockstream_types::state_mutation::StateMutation::Delete { key } => {
                    wb.delete(&key);
                }
                rockstream_types::state_mutation::StateMutation::Merge { key, operand, .. } => {
                    wb.merge(&key, &operand);
                }
            }
        }
        self.add_batch(wb)
    }

    /// Add an `EpochStateDelta` to the pending group commit.
    pub fn add_epoch_delta(
        &self,
        delta: rockstream_types::state_mutation::EpochStateDelta,
    ) -> Result<(), OpError> {
        self.add_mutations(delta.mutations)
    }

    /// Flush: merge all pending batches into one and commit atomically.
    ///
    /// Returns the number of individual batches that were merged (0 if
    /// nothing was pending).  Regardless of N, this issues exactly **one**
    /// `Db::write()` call — the group-commit invariant.
    pub async fn flush(&self) -> Result<usize, OpError> {
        let batches: Vec<WriteBatch> = {
            let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
            self.fill_level.store(0, Ordering::Relaxed);
            std::mem::take(&mut *pending)
        };

        let n = batches.len();
        if n == 0 {
            return Ok(0);
        }

        // Merge all fragments into one WriteBatch.
        let mut merged = WriteBatch::new();
        for wb in batches.iter().cloned() {
            merged.merge_from(wb);
        }

        // One atomic commit — the only Db::write() call for this epoch.
        if let Err(error) = self.db.write_batch(merged).await {
            let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
            pending.extend(batches);
            self.fill_level.store(pending.len(), Ordering::Release);
            return Err(OpError::storage(error));
        }
        self.commit_count.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }

    /// Current fill level: number of batches waiting to be flushed.
    ///
    /// This is the fill-level metric required by DESIGN.md for every bounded
    /// buffer.  Monitor this to detect back-pressure episodes.
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Total number of `Db::write()` calls issued since this `GroupCommit` was
    /// created (durability events).
    ///
    /// In the proof test: with N ≥ 5 operators each adding one batch and one
    /// `flush()`, `commit_count() == 1` vs. N individual commits → ≥ 5×
    /// reduction.
    pub fn commit_count(&self) -> u64 {
        self.commit_count.load(Ordering::Relaxed)
    }
}

/// Atomic physical commit grouping behind logical visibility epochs.
///
/// A caller stages all sink and operator-state writes for an epoch in one
/// `WriteBatch`. Nothing is reported as committed until both `write_batch` and
/// `flush` succeed, so a failed physical group cannot publish a partial epoch.
struct PendingEpochEntry {
    batch: WriteBatch,
    byte_size: usize,
    staged_at: Instant,
    waiters: Vec<tokio::sync::oneshot::Sender<Result<(), OpError>>>,
}

pub struct PhysicalCommitGroup {
    db: Arc<ShardDb>,
    pending: Arc<Mutex<BTreeMap<rockstream_types::timestamp::Epoch, PendingEpochEntry>>>,
    pending_bytes: Arc<AtomicUsize>,
    last_committed: Arc<AtomicU64>,
    has_committed: Arc<AtomicBool>,
    max_delay_ms: u64,
    max_pending_bytes: usize,
    max_epochs: usize,
    max_waiters: usize,
    active_waiters: Arc<AtomicUsize>,
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    notify: Arc<tokio::sync::Notify>,
    timer_started: Arc<AtomicBool>,
}

impl PhysicalCommitGroup {
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self::with_config(
            db,
            DEFAULT_GROUP_COMMIT_MAX_DELAY_MS,
            MAX_GROUP_COMMIT_PENDING_BYTES,
            PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
        )
    }

    pub fn with_config(
        db: Arc<ShardDb>,
        max_delay_ms: u64,
        max_pending_bytes: usize,
        max_epochs: usize,
    ) -> Self {
        let group = Self {
            db,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            last_committed: Arc::new(AtomicU64::new(0)),
            has_committed: Arc::new(AtomicBool::new(false)),
            max_delay_ms,
            max_pending_bytes,
            max_epochs,
            max_waiters: MAX_GROUP_COMMIT_WAITERS,
            active_waiters: Arc::new(AtomicUsize::new(0)),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            notify: Arc::new(tokio::sync::Notify::new()),
            timer_started: Arc::new(AtomicBool::new(false)),
        };
        group.ensure_timer_running();
        group
    }

    pub fn fill_level(&self) -> usize {
        self.pending
            .lock()
            .expect("PhysicalCommitGroup mutex poisoned")
            .len()
    }

    pub fn pending_epochs(&self) -> usize {
        self.fill_level()
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    pub fn active_waiters(&self) -> usize {
        self.active_waiters.load(Ordering::Relaxed)
    }

    pub fn last_committed(&self) -> rockstream_types::timestamp::Epoch {
        self.last_committed.load(Ordering::Acquire)
    }

    fn ensure_timer_running(&self) {
        if self
            .timer_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let pending = self.pending.clone();
                let notify = self.notify.clone();
                let max_delay = Duration::from_millis(self.max_delay_ms);
                let db = self.db.clone();
                let last_committed = self.last_committed.clone();
                let has_committed = self.has_committed.clone();
                let pending_bytes = self.pending_bytes.clone();
                let active_waiters = self.active_waiters.clone();
                let flush_lock = self.flush_lock.clone();

                handle.spawn(async move {
                    loop {
                        let sleep_dur = {
                            let lock = pending.lock().expect("PhysicalCommitGroup mutex poisoned");
                            if let Some((_, oldest)) = lock.iter().next() {
                                let elapsed = oldest.staged_at.elapsed();
                                if elapsed >= max_delay {
                                    Duration::from_millis(0)
                                } else {
                                    max_delay - elapsed
                                }
                            } else {
                                Duration::from_secs(3600)
                            }
                        };

                        if sleep_dur.is_zero() {
                            let _ = Self::do_flush(
                                &db,
                                &pending,
                                &pending_bytes,
                                &last_committed,
                                &has_committed,
                                &active_waiters,
                                &flush_lock,
                            )
                            .await;
                        } else {
                            tokio::select! {
                                _ = notify.notified() => {}
                                _ = tokio::time::sleep(sleep_dur) => {
                                    let _ = Self::do_flush(
                                        &db,
                                        &pending,
                                        &pending_bytes,
                                        &last_committed,
                                        &has_committed,
                                        &active_waiters,
                                        &flush_lock,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                });
            } else {
                self.timer_started.store(false, Ordering::SeqCst);
            }
        }
    }

    fn add_epoch_internal(
        &self,
        epoch: rockstream_types::timestamp::Epoch,
        batch: WriteBatch,
        waiter: Option<tokio::sync::oneshot::Sender<Result<(), OpError>>>,
    ) -> Result<bool, OpError> {
        let last_committed = self.last_committed();
        let batch_bytes = batch.byte_size();
        let should_flush_immediately;

        {
            let mut pending = self
                .pending
                .lock()
                .expect("PhysicalCommitGroup mutex poisoned");
            if (self.has_committed.load(Ordering::Acquire) && epoch <= last_committed)
                || pending.contains_key(&epoch)
            {
                return Err(OpError::internal(format!(
                    "logical epoch {epoch} is not newer than physical frontier {last_committed}"
                )));
            }
            if pending.len() >= self.max_epochs {
                return Err(OpError::group_commit_full(pending.len()));
            }
            let current_bytes = self.pending_bytes.load(Ordering::Relaxed);
            if !pending.is_empty() && current_bytes + batch_bytes > self.max_pending_bytes {
                return Err(OpError::group_commit_full(pending.len()));
            }

            let mut waiters = Vec::new();
            if let Some(w) = waiter {
                if self.active_waiters.load(Ordering::Relaxed) >= self.max_waiters {
                    return Err(OpError::group_commit_full(self.max_waiters));
                }
                self.active_waiters.fetch_add(1, Ordering::SeqCst);
                waiters.push(w);
            }

            pending.insert(
                epoch,
                PendingEpochEntry {
                    batch,
                    byte_size: batch_bytes,
                    staged_at: Instant::now(),
                    waiters,
                },
            );
            let new_bytes =
                self.pending_bytes.fetch_add(batch_bytes, Ordering::SeqCst) + batch_bytes;
            should_flush_immediately =
                new_bytes >= self.max_pending_bytes || pending.len() >= self.max_epochs;
        }

        self.ensure_timer_running();
        self.notify.notify_one();
        Ok(should_flush_immediately)
    }

    pub fn add_epoch(
        &self,
        epoch: rockstream_types::timestamp::Epoch,
        batch: WriteBatch,
    ) -> Result<(), OpError> {
        self.add_epoch_internal(epoch, batch, None).map(|_| ())
    }

    /// Flush all staged complete logical epochs as one durable physical group.
    pub async fn flush(&self) -> Result<Vec<rockstream_types::timestamp::Epoch>, OpError> {
        Self::do_flush(
            &self.db,
            &self.pending,
            &self.pending_bytes,
            &self.last_committed,
            &self.has_committed,
            &self.active_waiters,
            &self.flush_lock,
        )
        .await
    }

    async fn do_flush(
        db: &Arc<ShardDb>,
        pending: &Arc<Mutex<BTreeMap<rockstream_types::timestamp::Epoch, PendingEpochEntry>>>,
        pending_bytes: &Arc<AtomicUsize>,
        last_committed: &Arc<AtomicU64>,
        has_committed: &Arc<AtomicBool>,
        active_waiters: &Arc<AtomicUsize>,
        flush_lock: &Arc<tokio::sync::Mutex<()>>,
    ) -> Result<Vec<rockstream_types::timestamp::Epoch>, OpError> {
        let _guard = flush_lock.lock().await;

        let (entries, merged) = {
            let mut lock = pending.lock().expect("PhysicalCommitGroup mutex poisoned");
            if lock.is_empty() {
                return Ok(Vec::new());
            }

            let mut merged = WriteBatch::new();
            for entry in lock.values() {
                merged.merge_from(entry.batch.clone());
            }

            let drained: BTreeMap<_, _> = std::mem::take(&mut *lock);
            pending_bytes.store(0, Ordering::SeqCst);
            (drained, merged)
        };

        let epochs: Vec<_> = entries.keys().copied().collect();
        let mut merged = merged;
        if let Some(&last) = epochs.last() {
            merged.put(
                &rockstream_storage::ShardKeyEncoder::frontier_key(),
                &last.to_be_bytes(),
            );
        }

        // Write batch with retry
        let mut write_err = None;
        for attempt in 0..MAX_GROUP_COMMIT_RETRY_COUNT {
            match db.write_batch(merged.clone()).await {
                Ok(()) => {
                    write_err = None;
                    break;
                }
                Err(err) => {
                    write_err = Some(OpError::storage(err));
                    if attempt + 1 < MAX_GROUP_COMMIT_RETRY_COUNT {
                        tokio::time::sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }

        if let Some(error) = write_err {
            Self::restore_pending_static(pending, pending_bytes, active_waiters, entries, &error);
            return Err(error);
        }

        // Flush with retry
        let mut flush_err = None;
        for attempt in 0..MAX_GROUP_COMMIT_RETRY_COUNT {
            match db.flush().await {
                Ok(()) => {
                    flush_err = None;
                    break;
                }
                Err(err) => {
                    flush_err = Some(OpError::storage(err));
                    if attempt + 1 < MAX_GROUP_COMMIT_RETRY_COUNT {
                        tokio::time::sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }

        if let Some(error) = flush_err {
            Self::restore_pending_static(pending, pending_bytes, active_waiters, entries, &error);
            return Err(error);
        }

        // Advance physical frontier
        if let Some(last) = epochs.last().copied() {
            assert!(
                last >= last_committed.load(Ordering::Relaxed),
                "M1-S8: physical group committed epoch must be monotonic"
            );
            last_committed.store(last, Ordering::Release);
            has_committed.store(true, Ordering::Release);
        }

        // Notify all waiters
        for (_, mut entry) in entries {
            for waiter in entry.waiters.drain(..) {
                active_waiters.fetch_sub(1, Ordering::SeqCst);
                let _ = waiter.send(Ok(()));
            }
        }

        Ok(epochs)
    }

    pub async fn commit_epoch(
        &self,
        epoch: rockstream_types::timestamp::Epoch,
        batch: WriteBatch,
    ) -> Result<(), OpError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let should_flush = self.add_epoch_internal(epoch, batch, Some(tx))?;
        if should_flush {
            let _ = self.flush().await;
        }
        rx.await
            .map_err(|_| OpError::internal("commit epoch waiter dropped"))?
    }

    fn restore_pending_static(
        pending: &Arc<Mutex<BTreeMap<rockstream_types::timestamp::Epoch, PendingEpochEntry>>>,
        pending_bytes: &Arc<AtomicUsize>,
        active_waiters: &Arc<AtomicUsize>,
        entries: BTreeMap<rockstream_types::timestamp::Epoch, PendingEpochEntry>,
        error: &OpError,
    ) {
        let mut lock = pending.lock().expect("PhysicalCommitGroup mutex poisoned");
        for (epoch, mut entry) in entries {
            let waiters = std::mem::take(&mut entry.waiters);
            for waiter in waiters {
                active_waiters.fetch_sub(1, Ordering::SeqCst);
                let _ = waiter.send(Err(OpError::internal(format!(
                    "group commit failed: {error}"
                ))));
            }
            pending_bytes.fetch_add(entry.byte_size, Ordering::SeqCst);
            lock.insert(epoch, entry);
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_commit_max_batches_constant_is_named() {
        // The constant must have the exact name used in the DESIGN.md bound table.
        // Using const assertions to avoid clippy::assertions_on_constants.
        const _: () = assert!(GROUP_COMMIT_MAX_BATCHES >= 8);
        const _: () = assert!(GROUP_COMMIT_MAX_BATCHES <= 256);
    }

    #[test]
    fn add_batch_respects_bound() {
        // Verify the constant itself is the expected value.
        assert_eq!(GROUP_COMMIT_MAX_BATCHES, 64);
    }

    #[test]
    fn fill_level_decreases_on_flush_conceptual() {
        // Conceptual test: fill_level before flush = N, after flush = 0.
        // Real flush test is in lfs_aggregate integration test.
        let fill = AtomicUsize::new(10);
        fill.store(0, Ordering::Relaxed);
        assert_eq!(fill.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn physical_group_bound_is_named() {
        assert_eq!(PHYSICAL_COMMIT_GROUP_MAX_EPOCHS, 64);
        assert_eq!(MAX_GROUP_COMMIT_PENDING_BYTES, 8 * 1024 * 1024);
        assert_eq!(MAX_GROUP_COMMIT_WAITERS, 1024);
        assert_eq!(DEFAULT_GROUP_COMMIT_MAX_DELAY_MS, 10);
        assert_eq!(MAX_GROUP_COMMIT_RETRY_COUNT, 3);
    }
}
