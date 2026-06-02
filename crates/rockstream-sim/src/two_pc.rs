//! Two-phase commit protocol for exactly-once sinks (DESIGN.md §11.4, v0.36).
//!
//! Sink connectors implement the standard 2PC protocol:
//!
//! ```text
//! Pre-commit (during epoch):
//!   - Stage outgoing rows in a sink-specific transactional buffer.
//!   - Stage atomically committed in the shard's WriteBatch via a
//!     sink_state/ entry recording the pending position.
//!
//! Commit (after cluster checkpoint succeeds):
//!   - Finalize the staged transaction (Kafka flush, S3 rename, Postgres COMMIT).
//!   - Update sink_state/ to mark epoch as committed.
//! ```
//!
//! On crash recovery:
//! - `PreCommitted` but not `Committed`: re-run commit (idempotent).
//! - `Idle`: epoch data reproduced from source; nothing to do.

use rockstream_types::timestamp::Epoch;

/// Phase of the 2PC protocol for a sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPcPhase {
    /// No active transaction.
    Idle,
    /// Pre-commit staged; waiting for cluster checkpoint before finalizing.
    PreCommitted {
        epoch: Epoch,
        /// Number of rows staged.
        staged_rows: usize,
    },
    /// Transaction committed and finalized.
    Committed { epoch: Epoch },
}

/// State machine for a 2PC sink (DESIGN.md §11.4).
///
/// Pure synchronous data structure. The actual I/O (Kafka flush, S3 rename,
/// Postgres COMMIT) is performed by the concrete sink implementation.
#[derive(Debug)]
pub struct TwoPcSinkState {
    phase: TwoPcPhase,
    committed_epochs: Vec<Epoch>,
}

impl TwoPcSinkState {
    pub fn new() -> Self {
        Self {
            phase: TwoPcPhase::Idle,
            committed_epochs: Vec::new(),
        }
    }

    /// Stage rows for the given epoch (pre-commit phase).
    ///
    /// Returns `Err` if a pre-commit is already in progress for a different epoch.
    pub fn pre_commit(&mut self, epoch: Epoch, rows: usize) -> Result<(), &'static str> {
        if !matches!(self.phase, TwoPcPhase::Idle) {
            return Err("pre_commit: already in a transaction; abort or commit first");
        }
        self.phase = TwoPcPhase::PreCommitted {
            epoch,
            staged_rows: rows,
        };
        Ok(())
    }

    /// Finalize commit after the cluster checkpoint succeeds.
    ///
    /// Idempotent if called again for the same epoch after a crash.
    /// Returns `Err` if not in `PreCommitted` state.
    pub fn commit(&mut self) -> Result<Epoch, &'static str> {
        match &self.phase {
            TwoPcPhase::PreCommitted { epoch, .. } => {
                let epoch = *epoch;
                self.phase = TwoPcPhase::Committed { epoch };
                self.committed_epochs.push(epoch);
                if self.committed_epochs.len() > 1024 {
                    self.committed_epochs.remove(0);
                }
                Ok(epoch)
            }
            TwoPcPhase::Committed { epoch } => Ok(*epoch),
            TwoPcPhase::Idle => Err("commit: not in pre-committed state"),
        }
    }

    /// Abort the current transaction (checkpoint aborted or source reset).
    pub fn abort(&mut self) {
        self.phase = TwoPcPhase::Idle;
    }

    /// Finalize a committed epoch and return to `Idle`.
    pub fn finalize(&mut self) {
        if matches!(self.phase, TwoPcPhase::Committed { .. }) {
            self.phase = TwoPcPhase::Idle;
        }
    }

    /// Recover from crash.
    ///
    /// - `true`: was in `PreCommitted` state; caller must re-run commit (idempotent).
    /// - `false`: was `Idle`; epoch data will be reproduced from source.
    pub fn recover(&self) -> bool {
        matches!(self.phase, TwoPcPhase::PreCommitted { .. })
    }

    /// Current phase.
    pub fn phase(&self) -> &TwoPcPhase {
        &self.phase
    }

    /// All epochs committed through this state machine in order.
    pub fn committed_epochs(&self) -> &[Epoch] {
        &self.committed_epochs
    }
}

impl Default for TwoPcSinkState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_pre_commit_then_commit() {
        let mut state = TwoPcSinkState::new();
        assert_eq!(state.phase(), &TwoPcPhase::Idle);
        state.pre_commit(1, 100).unwrap();
        assert!(matches!(
            state.phase(),
            TwoPcPhase::PreCommitted { epoch: 1, .. }
        ));
        let committed = state.commit().unwrap();
        assert_eq!(committed, 1);
        state.finalize();
        assert_eq!(state.phase(), &TwoPcPhase::Idle);
        assert_eq!(state.committed_epochs(), &[1]);
    }

    #[test]
    fn abort_returns_to_idle() {
        let mut state = TwoPcSinkState::new();
        state.pre_commit(5, 50).unwrap();
        state.abort();
        assert_eq!(state.phase(), &TwoPcPhase::Idle);
    }

    #[test]
    fn recovery_true_when_pre_committed() {
        let mut state = TwoPcSinkState::new();
        state.pre_commit(3, 10).unwrap();
        assert!(
            state.recover(),
            "must re-run commit after crash in pre-committed state"
        );
    }

    #[test]
    fn recovery_false_when_idle() {
        let state = TwoPcSinkState::new();
        assert!(!state.recover(), "no action needed if idle at crash");
    }

    #[test]
    fn double_pre_commit_fails() {
        let mut state = TwoPcSinkState::new();
        state.pre_commit(1, 10).unwrap();
        assert!(state.pre_commit(2, 20).is_err());
    }

    #[test]
    fn commit_is_idempotent_after_crash() {
        let mut state = TwoPcSinkState::new();
        state.pre_commit(7, 5).unwrap();
        let e1 = state.commit().unwrap();
        // Simulate: crash without finalize, then recover and re-commit.
        let e2 = state.commit().unwrap();
        assert_eq!(e1, e2);
    }
}
