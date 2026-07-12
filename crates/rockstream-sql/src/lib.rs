//! SQL frontend for RockStream, built on DataFusion (v0.7).
//!
//! This crate implements the DataFusion-based SQL frontend, including:
//!
//! - **`SqlFrontend`** — parse/bind/optimize SQL into a `LogicalPlan`, then
//!   lower to a RockStream `PlanNode`.
//! - **Extension nodes** — `IncAggregate` / `IncJoin` / `IncDistinct` custom
//!   DataFusion plan nodes marking operations as incrementally maintained.
//! - **Lowering pass** — `LogicalPlan → PlanNode` for the Phase 1 operator set.
//! - **Distribution pass** — annotate `partition_key` and insert `Exchange`
//!   no-ops (single-shard: all exchanges are `Loopback`).
//! - **`SchemaCatalog`** — schema-version catalog backed by `ShardDb`;
//!   compatible changes accepted online, breaking changes return `RS-1002`.
//! - **`CREATE VIEW`** — parse, lower, and persist a view definition.
//! - **`EXPLAIN INCREMENTAL`** — format the annotated operator tree.
//! - **`EXPLAIN INCREMENTAL ESTIMATE`** — static cost model reporting predicted
//!   state size and per-operator `epoch_ms` without deploying.

pub mod catalog;
pub mod distribution;
pub mod error;
pub mod estimate;
pub mod explain_incremental;
pub mod extension;
pub mod frontend;
pub mod lower;

pub use catalog::{ColumnDef, SchemaCatalog, ViewEntry};
pub use distribution::apply_distribution;
pub use error::SqlError;
pub use estimate::{explain_incremental_estimate, format_estimate, EstimateRow};
pub use explain_incremental::{
    explain_incremental, explain_incremental_analyze, explain_incremental_verbose,
};
pub use extension::{IncAggregate, IncDistinct, IncJoin};
pub use frontend::SqlFrontend;
pub use lower::lower;

#[cfg(test)]
mod tests {
    #[test]
    fn sql_crate_compiles() {}
}
