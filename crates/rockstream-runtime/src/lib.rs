//! RockStream worker runtime.
//!
//! v0.4 re-exports the key runtime primitives from `rockstream-ops`:
//! the `CreditScheduler`, `EmbeddedRuntime`, and related types.
//!
//! Later versions add the epoch-commit coordinator (v0.5), exchange (v0.16),
//! and the control-plane service (v0.15).

pub use rockstream_ops::embedded::{EmbeddedCounters, EmbeddedRuntime};
pub use rockstream_ops::scheduler::CreditScheduler;
pub use rockstream_ops::task::{OperatorTask, OPERATOR_CHANNEL_CAPACITY};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_crate_compiles() {
        let rt = EmbeddedRuntime::new();
        assert_eq!(rt.counters.grpc_call_count(), 0);
        assert_eq!(rt.counters.shuffle_write_count(), 0);
    }
}

