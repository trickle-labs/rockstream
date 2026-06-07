//! Batch reference engine and property-test harness for RockStream.
//!
//! This crate will hold the DataFusion batch reference engine and the
//! `proptest` harness that asserts the DBSP soundness property
//! `incremental(query, deltas) == batch(query, accumulated)` for every operator
//! and query.
//!
//! Per the focused roadmap, the oracle harness is implemented in **v0.2**
//! (runtime abstraction, simulation, and oracle) and then exercised by every
//! later version's operator work. The crate is intentionally an empty scaffold
//! at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn oracle_crate_compiles() {}
}
