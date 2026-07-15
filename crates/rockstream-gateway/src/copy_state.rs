//! COPY IN session state — bounded buffer for `COPY FROM STDIN` ingestion.
//!
//! Named upper bounds:
//! - `MAX_COPY_IN_BATCH_ROWS = 10_000 rows`
//! - `COPY_IN_FLUSH_BYTES = 64 MiB`
//!
//! Fill-level metric: `COPY_IN_BUFFER_ROWS` (AtomicU64 gauge).

use std::sync::atomic::AtomicU64;

use crate::write_buffer::DmlOp;

/// Named upper bound: auto-flush row count (10 000 rows).
pub const MAX_COPY_IN_BATCH_ROWS: usize = 10_000;

/// Named upper bound: auto-flush byte size (64 MiB).
pub const COPY_IN_FLUSH_BYTES: usize = 64 * 1024 * 1024;

/// Fill-level metric: current rows buffered across all active COPY IN connections.
///
/// Incremented once per row accepted into any buffer; decremented on flush.
pub static COPY_IN_BUFFER_ROWS: AtomicU64 = AtomicU64::new(0);

/// Per-connection COPY IN state.
pub struct CopyState {
    /// Target table name (lowercased).
    pub table: String,
    /// Declared column names (empty → infer from catalog or use positional).
    pub columns: Vec<String>,
    /// Buffered DML ops waiting for the next flush.
    pub buf_rows: Vec<DmlOp>,
    /// Approximate byte size of `buf_rows`.
    pub buf_bytes: usize,
    /// Partial line from the last `CopyData` message that did not end with `\n`.
    pub partial_line: String,
    /// Total rows flushed to the shard across all batches (for `CommandComplete`).
    pub total_rows_flushed: usize,
}

impl CopyState {
    pub fn new(table: String, columns: Vec<String>) -> Self {
        CopyState {
            table,
            columns,
            buf_rows: Vec::new(),
            buf_bytes: 0,
            partial_line: String::new(),
            total_rows_flushed: 0,
        }
    }
}

/// Parse `COPY <table> [(<col1>, <col2>, ...)] FROM STDIN [WITH (...)]`.
///
/// Returns `(table_name, columns)` on success.  Column names are lowercased.
/// A `WITH (...)` suffix is accepted and ignored.
pub fn parse_copy_from_stmt(q: &str) -> Result<(String, Vec<String>), String> {
    let q = q.trim().trim_end_matches(';');
    let ql = q.to_lowercase();

    if !ql.starts_with("copy ") {
        return Err(format!(
            "[RS-2021] not a COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
        ));
    }

    // Find " from stdin" boundary
    let from_pos = ql.find(" from stdin").ok_or_else(|| {
        format!(
            "[RS-2021] missing FROM STDIN in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
        )
    })?;

    // "copy " is 5 bytes; if from_pos <= 5 there is no table name.
    if from_pos <= 5 {
        return Err(format!(
            "[RS-2021] missing table name in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
        ));
    }

    // Middle part: between "copy " and " from stdin"
    let middle = q[5..from_pos].trim();

    if let Some(paren_open) = middle.find('(') {
        // Has explicit column list
        let table_name = middle[..paren_open].trim().to_lowercase();
        if table_name.is_empty() {
            return Err(format!(
                "[RS-2021] missing table name in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
            ));
        }
        let paren_close = middle.rfind(')').ok_or_else(|| {
            format!(
                "[RS-2021] unmatched '(' in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
            )
        })?;
        let cols_str = &middle[paren_open + 1..paren_close];
        let columns: Vec<String> = cols_str
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        if columns.is_empty() {
            return Err(format!(
                "[RS-2021] empty column list in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
            ));
        }
        Ok((table_name, columns))
    } else {
        // No column list
        let table_name = middle.to_lowercase();
        if table_name.is_empty() {
            return Err(format!(
                "[RS-2021] missing table name in COPY statement: {q}. Next steps: check COPY syntax; the statement must be COPY <table> [(<col>, ...)] FROM STDIN [WITH (...)]."
            ));
        }
        Ok((table_name, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S2 green gate: parse_copy_from_stmt handles all documented variants.
    #[test]
    fn parse_copy_from_stmt_variants() {
        // Basic form — no column list
        let (t, cols) = parse_copy_from_stmt("COPY t FROM STDIN").unwrap();
        assert_eq!(t, "t");
        assert!(cols.is_empty(), "expected empty columns, got {cols:?}");

        // With explicit column list
        let (t, cols) = parse_copy_from_stmt("COPY t (a, b, c) FROM STDIN").unwrap();
        assert_eq!(t, "t");
        assert_eq!(cols, &["a", "b", "c"]);

        // With column list and WITH clause
        let (t, cols) =
            parse_copy_from_stmt("COPY my_table (id, val) FROM STDIN WITH (FORMAT TEXT)").unwrap();
        assert_eq!(t, "my_table");
        assert_eq!(cols, &["id", "val"]);

        // Trailing semicolon is accepted
        let (t, cols) = parse_copy_from_stmt("COPY events (ts, kind) FROM STDIN;").unwrap();
        assert_eq!(t, "events");
        assert_eq!(cols, &["ts", "kind"]);

        // Table name lowercased
        let (t, _) = parse_copy_from_stmt("COPY MyTable FROM STDIN").unwrap();
        assert_eq!(t, "mytable");

        // Missing FROM STDIN → error
        assert!(parse_copy_from_stmt("SELECT * FROM t").is_err());

        // Missing table name → error
        assert!(parse_copy_from_stmt("COPY FROM STDIN").is_err());
    }
}
