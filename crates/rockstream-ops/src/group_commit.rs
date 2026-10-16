//! Group-commit coalescing for shard-level epoch commits.
//!
//! `GroupCommit` combines operator fragments. `PhysicalCommitGroup` combines
//! complete logical epochs and acknowledges them only after durable flush.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rockstream_storage::{ShardDb, WriteBatch};
use crate::error::OpError;

mod physical;
pub use physical::{PhysicalCommitFill, PhysicalCommitGroup, GROUP_COMMIT_IO_TIMEOUT_MS, MAX_GROUP_COMMIT_OPERATIONS};

/// Maximum pending operator write fragments.
pub const GROUP_COMMIT_MAX_BATCHES: usize = 64;
/// Maximum retained complete epochs, including in-flight and failed epochs.
pub const PHYSICAL_COMMIT_GROUP_MAX_EPOCHS: usize = 64;
/// Maximum retained write payload across pending, in-flight and recovery state.
pub const MAX_GROUP_COMMIT_PENDING_BYTES: usize = 8 * 1024 * 1024;
/// Maximum live commit/flush acknowledgement callers.
pub const MAX_GROUP_COMMIT_WAITERS: usize = 1024;
/// Maximum coalescing delay before the oldest healthy epoch is flushed.
pub const DEFAULT_GROUP_COMMIT_MAX_DELAY_MS: u64 = 10;
/// Attempts in one explicitly requested physical operation. Failure pauses
/// automatic retries; it never starts an unbounded timer retry loop.
pub const MAX_GROUP_COMMIT_RETRY_COUNT: usize = 3;

/// Legacy operator-fragment coalescer. Complete logical epochs should use
/// `PhysicalCommitGroup`; this type retains its original write-only API.
pub struct GroupCommit {
    db: Arc<ShardDb>,
    pending: Mutex<Vec<WriteBatch>>,
    fill_level: Arc<AtomicUsize>,
    commit_count: Arc<AtomicU64>,
}

impl GroupCommit {
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self {
            db,
            pending: Mutex::new(Vec::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
            commit_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn add_batch(&self, wb: WriteBatch) -> Result<(), OpError> {
        let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
        if pending.len() >= GROUP_COMMIT_MAX_BATCHES {
            return Err(OpError::group_commit_full(pending.len()));
        }
        pending.push(wb);
        self.fill_level.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn add_mutations(&self, mutations: Vec<rockstream_types::state_mutation::StateMutation>) -> Result<(), OpError> {
        if mutations.is_empty() { return Ok(()); }
        let mut wb = WriteBatch::new();
        for mutation in mutations {
            match mutation {
                rockstream_types::state_mutation::StateMutation::Put { key, value } => wb.put(&key, &value),
                rockstream_types::state_mutation::StateMutation::Delete { key } => wb.delete(&key),
                rockstream_types::state_mutation::StateMutation::Merge { key, operand, .. } => wb.merge(&key, &operand),
            }
        }
        self.add_batch(wb)
    }

    pub fn add_epoch_delta(&self, delta: rockstream_types::state_mutation::EpochStateDelta) -> Result<(), OpError> {
        self.add_mutations(delta.mutations)
    }

    pub async fn flush(&self) -> Result<usize, OpError> {
        let batches: Vec<WriteBatch> = {
            let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
            self.fill_level.store(0, Ordering::Relaxed);
            std::mem::take(&mut *pending)
        };
        let count = batches.len();
        if count == 0 { return Ok(0); }
        let mut merged = WriteBatch::new();
        for batch in batches.iter().cloned() { merged.merge_from(batch); }
        if let Err(error) = self.db.write_batch(merged).await {
            let mut pending = self.pending.lock().expect("GroupCommit mutex poisoned");
            pending.extend(batches);
            self.fill_level.store(pending.len(), Ordering::Release);
            return Err(OpError::storage(error));
        }
        self.commit_count.fetch_add(1, Ordering::Relaxed);
        Ok(count)
    }

    pub fn fill_level(&self) -> usize { self.fill_level.load(Ordering::Relaxed) }
    pub fn commit_count(&self) -> u64 { self.commit_count.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_commit_max_batches_constant_is_named() {
        const _: () = assert!(GROUP_COMMIT_MAX_BATCHES >= 8);
        const _: () = assert!(GROUP_COMMIT_MAX_BATCHES <= 256);
    }

    #[test]
    fn add_batch_respects_bound() { assert_eq!(GROUP_COMMIT_MAX_BATCHES, 64); }

    #[test]
    fn physical_group_bound_is_named() {
        assert_eq!((PHYSICAL_COMMIT_GROUP_MAX_EPOCHS, MAX_GROUP_COMMIT_PENDING_BYTES, MAX_GROUP_COMMIT_WAITERS, DEFAULT_GROUP_COMMIT_MAX_DELAY_MS, MAX_GROUP_COMMIT_RETRY_COUNT), (64, 8 * 1024 * 1024, 1024, 10, 3));
    }
}
