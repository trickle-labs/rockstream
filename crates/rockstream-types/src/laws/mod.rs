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

pub mod bloom_union;
pub mod hyper_log_log;
pub mod lww_register;
pub mod max_register;
pub mod min_register;
pub mod mv_register;
pub mod or_set;
pub mod pn_counter;
pub mod registry;
pub mod sum_count;
pub mod weight_add;

pub use bloom_union::BloomUnionV1;
pub use hyper_log_log::HyperLogLogV1;
pub use lww_register::LWWRegisterV1;
pub use max_register::MaxRegisterV1;
pub use min_register::MinRegisterV1;
pub use mv_register::MVRegisterV1;
pub use or_set::OrSetV1;
pub use pn_counter::PNCounterV1;
pub use registry::LawRegistry;
pub use sum_count::SumCountV1;
pub use weight_add::WeightAddV1;

// Re-export well-known law IDs for use in tests and exchange combiners.
pub use bloom_union::BLOOM_UNION_ID;
pub use hyper_log_log::HLL_ID;
pub use lww_register::LWW_REGISTER_ID;
pub use max_register::MAX_REGISTER_ID;
pub use min_register::MIN_REGISTER_ID;
pub use mv_register::MV_REGISTER_ID;
pub use or_set::OR_SET_ID;
pub use pn_counter::PN_COUNTER_ID;
pub use sum_count::SUM_COUNT_ID;
pub use weight_add::WEIGHT_ADD_ID;

