//! Operator trait and per-operator implementations for RockStream.
//!
//! This crate will hold the `Operator` trait, the `EpochOutput` type, the
//! `OperatorTask` event loop, and the per-operator IVM implementations.
//!
//! Per the focused roadmap, operators are implemented test-first one slice at a
//! time starting in **v0.4** (filter / project / map), followed by algebraic
//! aggregates (v0.5), MIN/MAX (v0.6), joins (v0.8–v0.9), and the remaining
//! operators in later versions. Each operator ships with an oracle property
//! test (`incremental == batch`) before it is considered complete. The crate is
//! intentionally an empty scaffold at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn ops_crate_compiles() {}
}
