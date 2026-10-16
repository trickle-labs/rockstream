//! Producer-facing COPY OUT response bounds for #135 / v0.72.
//!
//! These are the frozen interactive result limits. This preflight prevents
//! sending a partial COPY stream; it does not itself bound storage materialization.

/// Frozen v0.72 interactive maximum for returned rows.
pub const MAX_COPY_OUT_ROWS: usize = 10_000;
/// Frozen v0.72 interactive maximum for encoded result bytes.
pub const MAX_COPY_OUT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyOutLimitError {
    Rows { actual: usize, maximum: usize },
    Bytes { actual: usize, maximum: usize },
}

impl std::fmt::Display for CopyOutLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rows { actual, maximum } => {
                write!(f, "COPY OUT rows {actual} exceed limit {maximum}")
            }
            Self::Bytes { actual, maximum } => {
                write!(f, "COPY OUT encoded bytes {actual} exceed limit {maximum}")
            }
        }
    }
}

/// Validate the complete response *before* sending CopyOutResponse or CopyData.
/// Each emitted TSV row includes a trailing newline byte.
pub fn check_copy_out_bounds(rows: &[Vec<u8>]) -> Result<(), CopyOutLimitError> {
    if rows.len() > MAX_COPY_OUT_ROWS {
        return Err(CopyOutLimitError::Rows {
            actual: rows.len(),
            maximum: MAX_COPY_OUT_ROWS,
        });
    }
    let mut total = 0usize;
    for row in rows {
        total = total
            .checked_add(row.len())
            .and_then(|bytes| bytes.checked_add(1))
            .unwrap_or(usize::MAX);
        if total > MAX_COPY_OUT_BYTES {
            return Err(CopyOutLimitError::Bytes {
                actual: total,
                maximum: MAX_COPY_OUT_BYTES,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_out_valid_rows_include_newline_in_encoded_budget() {
        let rows = vec![vec![b'x'; MAX_COPY_OUT_BYTES - 2], vec![]];
        assert_eq!(check_copy_out_bounds(&rows), Ok(()));
        let oversized = vec![vec![b'x'; MAX_COPY_OUT_BYTES - 1], vec![]];
        assert_eq!(
            check_copy_out_bounds(&oversized),
            Err(CopyOutLimitError::Bytes {
                actual: MAX_COPY_OUT_BYTES + 1,
                maximum: MAX_COPY_OUT_BYTES,
            })
        );
    }

    #[test]
    fn copy_out_rejects_oversized_single_row_before_any_frame() {
        let rows = vec![vec![b'x'; MAX_COPY_OUT_BYTES]];
        assert_eq!(
            check_copy_out_bounds(&rows),
            Err(CopyOutLimitError::Bytes {
                actual: MAX_COPY_OUT_BYTES + 1,
                maximum: MAX_COPY_OUT_BYTES,
            })
        );
    }

    #[test]
    fn copy_out_row_bound_preserves_exact_threshold() {
        let mut rows = vec![Vec::new(); MAX_COPY_OUT_ROWS];
        assert_eq!(check_copy_out_bounds(&rows), Ok(()));
        rows.push(Vec::new());
        assert_eq!(
            check_copy_out_bounds(&rows),
            Err(CopyOutLimitError::Rows {
                actual: MAX_COPY_OUT_ROWS + 1,
                maximum: MAX_COPY_OUT_ROWS,
            })
        );
    }

    #[test]
    fn copy_out_empty_result_is_valid_and_error_is_explicit() {
        assert_eq!(check_copy_out_bounds(&[]), Ok(()));
        assert_eq!(
            CopyOutLimitError::Rows {
                actual: 10_001,
                maximum: 10_000
            }
            .to_string(),
            "COPY OUT rows 10001 exceed limit 10000"
        );
        assert_eq!(
            CopyOutLimitError::Bytes {
                actual: 8_388_609,
                maximum: 8_388_608
            }
            .to_string(),
            "COPY OUT encoded bytes 8388609 exceed limit 8388608"
        );
    }
}
