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
//! ## v0.4 additions
//!
//! - [`filter_oracle`]: Oracle property tests for `Filter`, `Project`, `Map`
//!   — the `SELECT a, b*2 AS c FROM t WHERE b*2 > 10` query over ≥100k
//!   random insert/delete sequences.
//!
//! ## v0.5 additions
//!
//! - [`aggregate_oracle`]: Oracle property tests for `Aggregate`
//!   (SUM/COUNT/AVG) — the `SELECT k, SUM(v), COUNT(*), AVG(v) FROM t
//!   GROUP BY k` query over ≥100k random insert/update/delete sequences
//!   with group churn.
//!
//! ## v0.6 additions
//!
//! - [`minmax_oracle`]: Oracle property tests for `MinMaxOp` (MIN/MAX) —
//!   `SELECT k, MIN(v) FROM t GROUP BY k` and `SELECT k, MAX(v) FROM t
//!   GROUP BY k` over ≥100k random sequences with group churn and
//!   extremum transitions.  Also asserts that the cached extremum equals
//!   the true multiset extremum after every batch.

pub mod aggregate_oracle;
pub mod distinct_oracle;
pub mod batch;
pub mod filter_oracle;
pub mod harness;
pub mod minmax_oracle;
pub mod outer_join_oracle;
pub mod zset;
