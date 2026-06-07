//! SQL frontend for RockStream, built on DataFusion.
//!
//! This crate will hold the DataFusion-based parse/bind/optimize frontend, the
//! custom incremental extension nodes (`IncAggregate`/`IncJoin`/`IncDistinct`),
//! the `LogicalPlan -> PlanNode` lowering pass, the schema-version catalog,
//! `CREATE VIEW`, and `EXPLAIN INCREMENTAL`.
//!
//! Per the focused roadmap, the SQL frontend is implemented test-first in
//! **v0.7**. The crate is intentionally an empty scaffold at v0.1
//! ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn sql_crate_compiles() {}
}
