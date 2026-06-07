//! PostgreSQL wire gateway service for RockStream.
//!
//! This crate will hold the pgwire protocol gateway that serves reads of
//! maintained views to Postgres-compatible clients (psql, SQLAlchemy, JDBC),
//! plus the freshness, subscribe, and DML surfaces.
//!
//! Per the focused roadmap, the Postgres wire access layer is the project's
//! second pillar and is built after the IVM engine is correct and distributed
//! (the Postgres phase, v0.40+). The crate is intentionally an empty scaffold
//! at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_crate_compiles() {}
}
