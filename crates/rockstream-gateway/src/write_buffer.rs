//! Per-connection DML write buffer — bounded accumulator for INSERT/UPDATE/DELETE ops.
//!
//! Named upper bound: `WRITE_BUFFER_LIMIT_BYTES = 64 MiB`.
//! Fill-level metric:  `WriteBuffer::byte_count()`.
//! Backpressure path:  `RS-2019 write.shard_backpressure` from `push()` when full.

use crate::error::GatewayError;

/// Named upper bound for per-connection write buffer (64 MiB).
pub const WRITE_BUFFER_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Named upper bound for savepoints per transaction.
pub const MAX_SAVEPOINTS_PER_BUFFER: usize = 128;

/// A single DML operation buffered before COMMIT.
#[derive(Debug, Clone)]
pub enum DmlOp {
    Insert {
        table: String,
        cols: Vec<String>,
        /// Tab-separated column values.
        values_tsv: String,
        /// Deterministic row key: `col1=val1|col2=val2|...`
        row_key: String,
    },
    Update {
        table: String,
        old_row_key: String,
        old_tsv: String,
        new_row_key: String,
        new_tsv: String,
    },
    Delete {
        table: String,
        row_key: String,
        /// Pre-delete row image, captured before the write when a
        /// `RETURNING` clause was present (v0.48 Slice A4). `None` for a
        /// plain `DELETE` with no `RETURNING` — the row is gone from
        /// `view_output` once the `WriteBatch` commits, so this is the only
        /// point the pre-image can be captured. Never touched by the
        /// `WriteBatch` construction path; used solely for post-commit
        /// projection.
        returning_tsv: Option<String>,
    },
}

impl DmlOp {
    /// Approximate byte size of this operation (used for fill-level tracking).
    pub fn byte_size(&self) -> usize {
        match self {
            DmlOp::Insert {
                table,
                cols,
                values_tsv,
                row_key,
            } => {
                table.len()
                    + cols.iter().map(|c| c.len()).sum::<usize>()
                    + values_tsv.len()
                    + row_key.len()
                    + 64
            }
            DmlOp::Update {
                table,
                old_row_key,
                old_tsv,
                new_row_key,
                new_tsv,
            } => {
                table.len()
                    + old_row_key.len()
                    + old_tsv.len()
                    + new_row_key.len()
                    + new_tsv.len()
                    + 64
            }
            DmlOp::Delete {
                table,
                row_key,
                returning_tsv,
            } => {
                table.len()
                    + row_key.len()
                    + returning_tsv.as_ref().map(|s| s.len()).unwrap_or(0)
                    + 32
            }
        }
    }
}

/// Per-connection DML accumulator, bounded by `limit_bytes`.
#[derive(Debug)]
pub struct WriteBuffer {
    ops: Vec<DmlOp>,
    byte_count: usize,
    /// Named upper bound: `WRITE_BUFFER_LIMIT_BYTES`.
    limit_bytes: usize,
    /// Named savepoints: (name, ops_len at save time). Bound: MAX_SAVEPOINTS_PER_BUFFER.
    savepoints: Vec<(String, usize)>,
}

impl WriteBuffer {
    /// Create a new empty write buffer with the default limit.
    pub fn new() -> Self {
        Self::with_limit_bytes(WRITE_BUFFER_LIMIT_BYTES)
    }

    /// Create an empty buffer with an explicit limit.
    pub fn with_limit_bytes(limit_bytes: usize) -> Self {
        WriteBuffer {
            ops: Vec::new(),
            byte_count: 0,
            limit_bytes,
            savepoints: Vec::new(),
        }
    }

    /// Push a DML operation.
    ///
    /// Returns `Err(GatewayError::ShardBackpressure)` when `limit_bytes` would
    /// be exceeded — the caller must surface `RS-2019` to the client.
    pub fn push(&mut self, op: DmlOp) -> Result<(), GatewayError> {
        let op_bytes = op.byte_size();
        if self.byte_count + op_bytes > self.limit_bytes {
            return Err(GatewayError::ShardBackpressure {
                current_bytes: self.byte_count,
                limit_bytes: self.limit_bytes,
            });
        }
        self.byte_count += op_bytes;
        self.ops.push(op);
        Ok(())
    }

    /// Drain all operations, resetting the buffer.
    pub fn drain(&mut self) -> Vec<DmlOp> {
        self.byte_count = 0;
        std::mem::take(&mut self.ops)
    }

    /// Read-only slice of buffered operations.
    pub fn ops(&self) -> &[DmlOp] {
        &self.ops
    }

    /// Clear without returning the ops.
    pub fn clear(&mut self) {
        self.ops.clear();
        self.byte_count = 0;
        self.savepoints.clear();
    }

    /// Create or overwrite a named savepoint at the current ops position.
    ///
    /// Postgres semantics: SAVEPOINT with existing name replaces the prior entry.
    /// Returns `SavepointLimitExceeded` when a new entry would exceed `MAX_SAVEPOINTS_PER_BUFFER`.
    pub fn create_savepoint(&mut self, name: &str) -> Result<(), GatewayError> {
        let pos = self.ops.len();
        // If name already exists, overwrite it (Postgres replace semantics).
        if let Some(idx) = self.savepoints.iter().rposition(|(n, _)| n == name) {
            self.savepoints[idx] = (name.to_string(), pos);
            return Ok(());
        }
        if self.savepoints.len() == MAX_SAVEPOINTS_PER_BUFFER {
            return Err(GatewayError::SavepointLimitExceeded {
                limit: MAX_SAVEPOINTS_PER_BUFFER,
            });
        }
        self.savepoints.push((name.to_string(), pos));
        Ok(())
    }

    /// Release a named savepoint and all savepoints after it.
    ///
    /// Does NOT discard ops (RELEASE commits ops up to the point where the savepoint was created).
    pub fn release_savepoint(&mut self, name: &str) -> Result<(), GatewayError> {
        let idx = self
            .savepoints
            .iter()
            .rposition(|(n, _)| n == name)
            .ok_or_else(|| GatewayError::SavepointNotFound {
                name: name.to_string(),
            })?;
        self.savepoints.truncate(idx);
        Ok(())
    }

    /// Roll back ops to the named savepoint position.
    ///
    /// Removes savepoints after (but not including) the matched entry so further
    /// ROLLBACK TO the same savepoint name remains valid (Postgres semantics).
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), GatewayError> {
        let idx = self
            .savepoints
            .iter()
            .rposition(|(n, _)| n == name)
            .ok_or_else(|| GatewayError::SavepointNotFound {
                name: name.to_string(),
            })?;
        let ops_len = self.savepoints[idx].1;
        self.ops.truncate(ops_len);
        self.byte_count = self.ops.iter().map(|op| op.byte_size()).sum();
        // Keep the matched savepoint; drop all entries after it.
        self.savepoints.truncate(idx + 1);
        Ok(())
    }

    /// Clear all savepoints — called at COMMIT and ROLLBACK.
    pub fn clear_savepoints(&mut self) {
        self.savepoints.clear();
    }

    /// Fill-level metric: number of active savepoints.
    pub fn savepoints_len(&self) -> usize {
        self.savepoints.len()
    }

    /// Current byte count (fill-level metric: `direct_write_pending_bytes`).
    pub fn byte_count(&self) -> usize {
        self.byte_count
    }

    /// Returns true if the buffer has no pending operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Number of pending operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Resolve the latest row image for `table`/`row_key` within this buffer.
    ///
    /// Returns:
    /// - `None` when the buffer has not touched the row key
    /// - `Some(Some(tsv))` when the buffer's latest state for the key is a row image
    /// - `Some(None)` when the buffer deleted the key, or updated it away from this key
    pub fn current_row_image(&self, table: &str, row_key: &str) -> Option<Option<String>> {
        for op in self.ops.iter().rev() {
            match op {
                DmlOp::Insert {
                    table: op_table,
                    values_tsv,
                    row_key: op_row_key,
                    ..
                } if op_table.eq_ignore_ascii_case(table) && op_row_key == row_key => {
                    return Some(Some(values_tsv.clone()));
                }
                DmlOp::Update {
                    table: op_table,
                    old_row_key,
                    new_row_key,
                    new_tsv,
                    ..
                } if op_table.eq_ignore_ascii_case(table) => {
                    if new_row_key == row_key {
                        return Some(Some(new_tsv.clone()));
                    }
                    if old_row_key == row_key {
                        return Some(None);
                    }
                }
                DmlOp::Delete {
                    table: op_table,
                    row_key: op_row_key,
                    ..
                } if op_table.eq_ignore_ascii_case(table) && op_row_key == row_key => {
                    return Some(None);
                }
                _ => {}
            }
        }
        None
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_insert(table: &str, val: &str) -> DmlOp {
        DmlOp::Insert {
            table: table.to_string(),
            cols: vec!["id".to_string(), "val".to_string()],
            values_tsv: val.to_string(),
            row_key: format!("id=1|val={val}"),
        }
    }

    /// v0.48 Slice A4 green gate: `DmlOp::Delete::returning_tsv` (the
    /// captured pre-image for `DELETE ... RETURNING`) is accounted for in
    /// `byte_size()`, and stays inside the existing 64 MiB
    /// `WRITE_BUFFER_LIMIT_BYTES` bound — an existing bounded buffer gains a
    /// field, not a new unbounded one.
    #[test]
    fn write_buffer_accounts_delete_returning_capture_bytes() {
        let plain_delete = DmlOp::Delete {
            table: "t".to_string(),
            row_key: "id=1".to_string(),
            returning_tsv: None,
        };
        let delete_with_capture = DmlOp::Delete {
            table: "t".to_string(),
            row_key: "id=1".to_string(),
            returning_tsv: Some("1\thello\tworld".to_string()),
        };
        assert!(
            delete_with_capture.byte_size() > plain_delete.byte_size(),
            "captured returning_tsv must increase byte_size over a plain DELETE"
        );
        assert_eq!(
            delete_with_capture.byte_size() - plain_delete.byte_size(),
            "1\thello\tworld".len(),
            "byte_size delta must equal the captured tsv's exact length"
        );

        let mut buf = WriteBuffer::new();
        buf.push(delete_with_capture).unwrap();
        assert!(
            buf.byte_count() < WRITE_BUFFER_LIMIT_BYTES,
            "a single capture-bearing DELETE must stay far under the named bound"
        );
    }

    /// S1 green gate: basic push/drain/clear cycle.
    #[test]
    fn write_buffer_push_drain_clear() {
        let mut buf = WriteBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.byte_count(), 0);

        buf.push(make_insert("t", "1\t2")).unwrap();
        buf.push(make_insert("t", "3\t4")).unwrap();
        assert_eq!(buf.len(), 2);
        assert!(!buf.is_empty());
        assert!(buf.byte_count() > 0);

        let drained = buf.drain();
        assert_eq!(drained.len(), 2);
        assert!(buf.is_empty());
        assert_eq!(buf.byte_count(), 0);

        buf.push(make_insert("t", "5\t6")).unwrap();
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.byte_count(), 0);
    }

    /// S1 green gate: limit enforcement returns RS-2019.
    #[test]
    fn write_buffer_limit_returns_rs2019() {
        // Create a buffer with a tiny limit.
        let mut buf = WriteBuffer {
            ops: Vec::new(),
            byte_count: 0,
            limit_bytes: 10, // 10 bytes — far smaller than any real op
            savepoints: Vec::new(),
        };

        let op = make_insert("t", "hello");
        let result = buf.push(op);
        assert!(
            result.is_err(),
            "expected RS-2019 error when limit exceeded"
        );
        match result.unwrap_err() {
            GatewayError::ShardBackpressure { .. } => {}
            e => panic!("expected ShardBackpressure, got {e:?}"),
        }
    }

    /// S2 green gate: ROLLBACK TO s truncates ops; savepoint s remains.
    #[test]
    fn savepoint_create_rollback_to() {
        let mut buf = WriteBuffer::new();
        buf.push(make_insert("t", "1")).unwrap();
        buf.push(make_insert("t", "2")).unwrap();
        buf.push(make_insert("t", "3")).unwrap();
        buf.create_savepoint("s").unwrap();
        buf.push(make_insert("t", "4")).unwrap();
        buf.push(make_insert("t", "5")).unwrap();
        buf.rollback_to_savepoint("s").unwrap();
        assert_eq!(buf.len(), 3);
        assert!(
            buf.savepoints.iter().any(|(n, _)| n == "s"),
            "savepoint 's' should remain after ROLLBACK TO"
        );
    }

    /// S2 green gate: RELEASE does not discard ops.
    #[test]
    fn savepoint_release_does_not_discard() {
        let mut buf = WriteBuffer::new();
        buf.push(make_insert("t", "1")).unwrap();
        buf.create_savepoint("s").unwrap();
        buf.push(make_insert("t", "2")).unwrap();
        buf.push(make_insert("t", "3")).unwrap();
        buf.release_savepoint("s").unwrap();
        assert_eq!(buf.len(), 3, "release must not discard ops");
        assert!(
            buf.savepoints.is_empty(),
            "savepoints should be empty after release"
        );
    }

    /// S2 green gate: rollback_to nonexistent name returns SavepointNotFound.
    #[test]
    fn savepoint_not_found_returns_error() {
        let mut buf = WriteBuffer::new();
        match buf.rollback_to_savepoint("nonexistent") {
            Err(GatewayError::SavepointNotFound { name }) => {
                assert_eq!(name, "nonexistent");
            }
            other => panic!("expected SavepointNotFound, got {other:?}"),
        }
    }

    /// S2 green gate: exceeding MAX_SAVEPOINTS_PER_BUFFER returns SavepointLimitExceeded.
    #[test]
    fn savepoint_limit_exceeded() {
        let mut buf = WriteBuffer::new();
        for i in 0..MAX_SAVEPOINTS_PER_BUFFER {
            buf.create_savepoint(&format!("sp_{i}")).unwrap();
        }
        match buf.create_savepoint("overflow") {
            Err(GatewayError::SavepointLimitExceeded { limit }) => {
                assert_eq!(limit, MAX_SAVEPOINTS_PER_BUFFER);
            }
            other => panic!("expected SavepointLimitExceeded, got {other:?}"),
        }
    }

    /// S2 green gate: SAVEPOINT with existing name overwrites (Postgres semantics).
    #[test]
    fn savepoint_overwrite_replaces() {
        let mut buf = WriteBuffer::new();
        buf.push(make_insert("t", "1")).unwrap();
        buf.create_savepoint("s").unwrap(); // ops_len = 1
        buf.push(make_insert("t", "2")).unwrap();
        buf.push(make_insert("t", "3")).unwrap();
        buf.create_savepoint("s").unwrap(); // ops_len = 3 (overwrite)
        buf.push(make_insert("t", "4")).unwrap();
        buf.rollback_to_savepoint("s").unwrap();
        assert_eq!(
            buf.len(),
            3,
            "should roll back to the overwritten savepoint position"
        );
    }
}
