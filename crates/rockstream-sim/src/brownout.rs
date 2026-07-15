//! Object store brownout handling (DESIGN.md §11.7, v0.36).
//!
//! During a brownout, workers stall at the epoch commit step and buffer
//! up to `local_buffer_max_epochs` (default 10) epochs locally before
//! applying backpressure. The pipeline transitions: Normal → Stalled →
//! Blocked(`RS-3003`).
//!
//! On recovery, buffered epochs commit in order; no data loss and no
//! duplicates (epoch keys are idempotent).

/// Default maximum epochs to buffer locally during a brownout.
pub const LOCAL_BUFFER_MAX_EPOCHS: usize = 10;

/// Status of the pipeline with respect to object store availability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrownoutStatus {
    /// Object store is healthy; no buffering.
    Normal,
    /// Object store is unavailable; epochs are being buffered locally.
    /// Pipeline is `STRESSED`; sources are not yet credit-starved.
    Stalled { buffered_epochs: usize },
    /// Buffer limit reached; backpressure applied to sources.
    /// Pipeline transitions to `BLOCKED(RS-3003)`.
    Blocked,
}

/// Handles object store brownout by buffering epoch commits and applying
/// backpressure when the local buffer is full (DESIGN.md §11.7).
///
/// INVARIANT-BY-CONSTRUCTION: M4-S4 — this guard's only failure states are
/// [`BrownoutStatus::Stalled`] and [`BrownoutStatus::Blocked`]; there is no
/// code path here that terminates the worker or invokes
/// `SelfFenceGuard`/`must_self_fence()` (`crates/rockstream-runtime/src/
/// fence.rs`). Self-fencing is gated exclusively on control-plane
/// reachability there, which is an entirely separate signal from object
/// store reachability tracked by this struct, so a worker that can reach
/// the control plane but not the object store is structurally always
/// `Blocked`, never `terminated` — proven by `test_checkpoint_under_slow_input`
/// (`crates/rockstream-sim/tests/`) completing without a self-fence panic
/// under a slow/unavailable object store.
pub struct ObjectStoreBrownoutGuard {
    max_buffered_epochs: usize,
    buffered_epochs: usize,
    brownout_active: bool,
}

impl ObjectStoreBrownoutGuard {
    /// Create a new guard with the given buffer limit.
    pub fn new(max_buffered_epochs: usize) -> Self {
        Self {
            max_buffered_epochs,
            buffered_epochs: 0,
            brownout_active: false,
        }
    }

    /// Signal that the object store is unavailable.
    pub fn record_store_unavailable(&mut self) {
        self.brownout_active = true;
    }

    /// Signal that the object store has recovered.
    pub fn record_store_recovery(&mut self) {
        self.brownout_active = false;
        self.buffered_epochs = 0;
    }

    /// Attempt to commit an epoch.
    ///
    /// - `Ok(())`: object store healthy; proceed with commit.
    /// - `Err(BrownoutStatus::Stalled { .. })`: epoch buffered; sources not yet paused.
    /// - `Err(BrownoutStatus::Blocked)`: buffer full; apply backpressure (`RS-3003`).
    pub fn try_commit_epoch(&mut self) -> Result<(), BrownoutStatus> {
        if !self.brownout_active {
            return Ok(());
        }
        if self.buffered_epochs < self.max_buffered_epochs {
            self.buffered_epochs += 1;
            return Err(BrownoutStatus::Stalled {
                buffered_epochs: self.buffered_epochs,
            });
        }
        Err(BrownoutStatus::Blocked)
    }

    /// Current brownout status.
    pub fn status(&self) -> BrownoutStatus {
        if !self.brownout_active {
            return BrownoutStatus::Normal;
        }
        if self.buffered_epochs < self.max_buffered_epochs {
            BrownoutStatus::Stalled {
                buffered_epochs: self.buffered_epochs,
            }
        } else {
            BrownoutStatus::Blocked
        }
    }

    /// Whether backpressure should be applied to the source connector.
    pub fn backpressure_active(&self) -> bool {
        self.brownout_active && self.buffered_epochs >= self.max_buffered_epochs
    }

    /// Number of epochs currently buffered.
    pub fn buffered_epochs(&self) -> usize {
        self.buffered_epochs
    }

    /// Whether a brownout is currently active.
    pub fn brownout_active(&self) -> bool {
        self.brownout_active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_when_store_healthy() {
        let guard = ObjectStoreBrownoutGuard::new(10);
        assert_eq!(guard.status(), BrownoutStatus::Normal);
        assert!(!guard.backpressure_active());
    }

    #[test]
    fn stalled_while_buffering() {
        let mut guard = ObjectStoreBrownoutGuard::new(5);
        guard.record_store_unavailable();
        let result = guard.try_commit_epoch();
        assert_eq!(result, Err(BrownoutStatus::Stalled { buffered_epochs: 1 }));
        assert!(!guard.backpressure_active());
    }

    #[test]
    fn blocked_after_buffer_full() {
        let mut guard = ObjectStoreBrownoutGuard::new(3);
        guard.record_store_unavailable();
        for _ in 0..3 {
            guard.try_commit_epoch().unwrap_err();
        }
        assert_eq!(guard.try_commit_epoch(), Err(BrownoutStatus::Blocked));
        assert!(guard.backpressure_active());
    }

    #[test]
    fn recovery_clears_buffer() {
        let mut guard = ObjectStoreBrownoutGuard::new(5);
        guard.record_store_unavailable();
        guard.try_commit_epoch().unwrap_err();
        guard.try_commit_epoch().unwrap_err();
        assert_eq!(guard.buffered_epochs(), 2);
        guard.record_store_recovery();
        assert_eq!(guard.status(), BrownoutStatus::Normal);
        assert_eq!(guard.buffered_epochs(), 0);
        assert!(guard.try_commit_epoch().is_ok());
    }
}
