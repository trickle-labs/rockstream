//! Batch reference engine and property-test harness for RockStream correctness.
//!
//! This crate provides the **oracle** that validates the DBSP soundness property:
//!
//! > `incremental(query, Δ) == batch(query, accumulated)`
//!
//! For every operator and every query, the incremental result over a sequence
//! of Z-set deltas must equal the DataFusion batch result over the same
//! accumulated data. This property is checked continuously, using `proptest`
//! for randomized sequences.
//!
//! ## v0.2 scope
//!
//! - [`zset`]: Z-set accumulation (`TestRow`, `ZSetDelta`, `accumulate`,
//!   `present_rows`).
//! - [`batch`]: DataFusion batch reference oracle (`run_noop_batch_query`).
//! - [`harness`]: `assert_oracle_noop` and the proptest harness that proves
//!   `incremental == batch` on the trivial no-op pipeline.
//!
//! Later versions will add per-operator oracle tests (filter, project, map,
//! aggregate, join, etc.) as those operators are implemented.

pub mod batch;
pub mod harness;
pub mod zset;
