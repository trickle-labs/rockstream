//! Deterministic simulation harness for RockStream.
//!
//! This crate provides the [`Runtime`] trait that abstracts time, task spawning,
//! sleep, object storage, and network I/O. Two implementations exist:
//!
//! - [`TokioRuntime`]: Production runtime backed by Tokio.
//! - [`SimRuntime`]: Deterministic, seeded-RNG simulation runtime for testing.
//!
//! The [`buggify!`] macro injects faults during simulation builds (feature
//! `simulation`) and compiles to a no-op in production builds.
//!
//! Every operator, scheduler, and storage call site in RockStream is
//! parameterized on the `Runtime` trait so that tests can deterministically
//! reproduce failures.

pub mod auto_tuner;
pub mod brownout;
pub mod buggify;
pub mod chaos;
pub mod clock;
pub mod compaction;
pub mod coord_faults;
pub mod fault_model;
pub mod law_faults;
pub mod liveness;
pub mod network;
pub mod nexmark;
pub mod object_store;
pub mod paired_assert;
pub mod recovery_soak;
pub mod runtime;
pub mod shard_map;
pub mod sim;
pub mod soak;
pub mod spike;
pub mod tokio_rt;
pub mod two_pc;
pub mod wire_version;

pub use nexmark::{Auction, Bid, NexmarkEvent, NexmarkGenerator, Person};

pub use auto_tuner::{
    AutoTuner, EPOCH_CEILING_MS, EPOCH_FLOOR_MS, LAG_TRIGGER_EPOCHS, MAX_THROTTLE_BYTES,
    MIN_THROTTLE_BYTES, PARALLELISM_P95_SCALE_DOWN_MS, PARALLELISM_P95_SCALE_UP_MS, SLO_TARGET,
    WRITE_RATE_QUOTA,
};
pub use brownout::{BrownoutStatus, ObjectStoreBrownoutGuard, LOCAL_BUFFER_MAX_EPOCHS};
pub use buggify::buggify_enabled;
pub use chaos::{
    run_chaos_reference, run_chaos_scenario, ChaosConfig, ChaosResult, RecoveryTimings,
};
pub use clock::{Clock, SimClock, TokioClock};
pub use compaction::{
    apply_tombstone_gc, simulate_donor_cleanup, simulate_split_migration, SimEntry,
};
pub use coord_faults::{register_coord_faults, COORD_FAULT_IDS};
pub use fault_model::{FaultEntry, FaultModel};
pub use law_faults::{register_law_faults, LAW_FAULT_IDS};
pub use liveness::{DegradedState, LivenessChecker, LivenessStatus};
pub use network::{SimNetwork, SimNetworkHandle};
pub use object_store::{SimObjectStore, SimObjectStoreHandle};
pub use paired_assert::paired_assert;
pub use recovery_soak::{
    run_brownout_recovery_scenario, run_partition_recovery_scenario, KafkaLagTimings,
    KafkaRecoverySoakResult, RecoverySoakConfig,
};
pub use runtime::{Runtime, Spawner};
pub use shard_map::{ShardOwnership, ShardRange, SimShardMap};
pub use sim::SimRuntime;
pub use soak::{
    build_initial_corpus, LawSeed, RegressionSeed, SeedCorpus, SeedOutcome, SoakRunner,
};
pub use spike::{OscillationDetector, SpikeResult, SpikeScenario};
pub use tokio_rt::TokioRuntime;
pub use two_pc::{TwoPcPhase, TwoPcSinkState};
pub use wire_version::{
    negotiate_version, NegotiationResult, ProtocolVersion, SupportedVersionRange,
};

#[cfg(test)]
mod tests;
