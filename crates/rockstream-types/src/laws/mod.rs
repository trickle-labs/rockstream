//! Built-in merge law implementations.
//!
//! Each law implements `LawBundle` and is registered in the global law registry.
//! v0.5 ships `WeightAdd/v1` — the fundamental Z-set weight addition law.
//! v0.7 adds `SumCount/v1` — the abelian-group aggregate law (SUM, COUNT, AVG).
//! v0.8 adds `MaxRegister/v1` and `MinRegister/v1` — semilattice cached-slot
//!      laws backing the retraction-aware `MinMaxOp` operator.
//! v0.21 adds `HyperLogLog/v1` — semilattice sketch law for planner NDV
//!       estimation.
//! v0.25 adds `BloomUnion/v1` — semilattice sketch law for `APPROX_MEMBERSHIP`.
//! v0.37 adds `OrSet/v1` — semilattice CRDT set law for split/merge proof tests;
//!       full user-visible OR-Set column types ship in v0.44.

pub mod registry;
pub mod sum_count;
pub mod weight_add;

pub use registry::LawRegistry;
pub use sum_count::SumCountV1;
pub use weight_add::WeightAddV1;

// Re-export well-known law IDs for use in tests and exchange combiners.
pub use sum_count::SUM_COUNT_ID;
pub use weight_add::WEIGHT_ADD_ID;
