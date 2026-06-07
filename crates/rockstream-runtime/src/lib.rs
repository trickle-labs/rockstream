//! Worker process, scheduler, epoch-commit coordinator, and exchange for
//! RockStream.
//!
//! This crate will hold the per-worker runtime: the async ownership-free
//! scheduler with credit backpressure, the epoch-commit coordinator, and the
//! exchange (shuffle) paths.
//!
//! Per the focused roadmap, the worker scheduler and embedded runtime profile
//! arrive in **v0.4**, group commit and persisted frontier in **v0.5**, and the
//! distributed exchange paths in the v0.16–v0.17 range. The crate is
//! intentionally an empty scaffold at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_crate_compiles() {}
}
