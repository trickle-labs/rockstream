//! DiffCtx differentiation pass for RockStream IVM.
//!
//! This crate will hold the `DiffCtx` differentiation pass that transforms a
//! logical `PlanNode` graph into a physical `OpNode` execution graph with
//! merge-law annotations (IVM.md §6–7).
//!
//! Per the focused roadmap, the linear-operator differentiation rules are
//! implemented test-first starting in **v0.4** (filter / project / map), with
//! aggregate, join, and set-operator rules following in later versions. The
//! crate is intentionally an empty scaffold at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn diff_crate_compiles() {}
}
