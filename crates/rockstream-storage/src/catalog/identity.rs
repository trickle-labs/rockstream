//! Monotonic durable ID allocator and ID invariance guarantees (Slice 2).
//!
//! Restored IDs never collide with new allocations because restored IDs update
//! the high-water mark via `observe()`. Domain IDs (`TableId`, `ViewId`, `IndexId`,
//! `SourceId`, `WorkloadId`) remain immutable across restart, rename, compaction,
//! and backup/restore.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic ID allocator tracking high-water mark.
#[derive(Debug)]
pub struct IdAllocator {
    high_water_mark: AtomicU64,
}

impl IdAllocator {
    /// Create a new allocator starting at the given high-water mark.
    pub fn new(initial: u64) -> Self {
        Self {
            high_water_mark: AtomicU64::new(initial),
        }
    }

    /// Allocate the next monotonic ID.
    pub fn allocate(&self) -> u64 {
        self.high_water_mark.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Observe an existing or restored ID.
    ///
    /// Guarantees that future allocations will be strictly greater than any observed ID.
    pub fn observe(&self, id: u64) {
        let mut current = self.high_water_mark.load(Ordering::SeqCst);
        while id > current {
            match self.high_water_mark.compare_exchange_weak(
                current,
                id,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Read the current high-water mark.
    pub fn high_water_mark(&self) -> u64 {
        self.high_water_mark.load(Ordering::SeqCst)
    }
}

impl Default for IdAllocator {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Compute a deterministic 64-bit FNV-1a identifier from object kind and name.
pub fn stable_name_id(kind: &str, name: &str) -> u64 {
    kind.bytes()
        .chain([0])
        .chain(name.bytes())
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}
