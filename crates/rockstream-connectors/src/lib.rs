//! Source and sink connector implementations for RockStream.
//!
//! This crate will hold the source and sink connector contracts and the
//! built-in connectors.
//!
//! Per the focused roadmap, the built-in `GENERATE ROWS` source and a
//! `Vec<RecordBatch>` delta source arrive in **v0.4**; external connectors
//! (Kafka, Postgres CDC) follow in the connector phase. The crate is
//! intentionally an empty scaffold at v0.1 ("workspace and CI").

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}
