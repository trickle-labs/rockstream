//! Per-connection DML write buffer — bounded accumulator for INSERT/UPDATE/DELETE ops.
//!
//! Named upper bound: `WRITE_BUFFER_LIMIT_BYTES = 64 MiB`.
//! Fill-level metric:  `WriteBuffer::byte_count()`.
//! Backpressure path:  `RS-2019 write.shard_backpressure` from `push()` when full.

use crate::error::GatewayError;

/// Named upper bound for per-connection write buffer (64 MiB).
pub const WRITE_BUFFER_LIMIT_BYTES: usize = 64 * 1024 * 1024;

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
            DmlOp::Delete { table, row_key } => table.len() + row_key.len() + 32,
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
}

impl WriteBuffer {
    /// Create a new empty write buffer with the default limit.
    pub fn new() -> Self {
        WriteBuffer {
            ops: Vec::new(),
            byte_count: 0,
            limit_bytes: WRITE_BUFFER_LIMIT_BYTES,
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

    /// Clear without returning the ops.
    pub fn clear(&mut self) {
        self.ops.clear();
        self.byte_count = 0;
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
}
