//! Subscribe SQL parser.
//!
//! Parses `SUBSCRIBE <view> [AS OF NOW WITH SNAPSHOT | AS OF EPOCH <n>]
//!         [WHERE <pred>] [(<col1>, <col2>, ...)]`
//! into a `SubscribeRequest` value type.  No I/O is performed here.

use std::fmt;

/// Starting point for a subscribe stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeStart {
    /// Deliver current snapshot then live deltas.
    NowWithSnapshot,
    /// Replay from the given epoch then live-tail.
    Epoch(u64),
}

/// Parsed subscribe request — fully typed, no raw SQL.
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    pub view_name: String,
    pub start: SubscribeStart,
    /// Raw WHERE predicate string (if any).
    pub where_clause: Option<String>,
    /// Projected column names (if any).
    pub projection: Option<Vec<String>>,
}

/// Error from subscribe parsing.
#[derive(Debug, PartialEq, Eq)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "subscribe parse error: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// Parse a SUBSCRIBE statement.
///
/// Grammar (case-insensitive):
/// ```text
/// SUBSCRIBE <view_name>
///   [AS OF NOW WITH SNAPSHOT | AS OF EPOCH <n>]
///   [WHERE <predicate>]
///   [(<col>, ...)]
/// ```
pub fn parse_subscribe(sql: &str) -> Result<SubscribeRequest, ParseError> {
    let s = sql.trim().trim_end_matches(';');

    // Must start with SUBSCRIBE (case-insensitive)
    let lower = s.to_lowercase();
    if !lower.starts_with("subscribe ") {
        return Err(ParseError(format!(
            "expected SUBSCRIBE keyword, got: {:?}",
            &s[..s.len().min(20)]
        )));
    }

    let rest = s["subscribe ".len()..].trim();

    // Extract optional column-projection list: ends with `(col, col, ...)`
    // We look for a trailing parenthesised list that is not the AS OF clause.
    let (rest, projection) = extract_trailing_projection(rest);
    let rest = rest.trim();

    // Split on WHERE (case-insensitive)
    let (rest, where_clause) = split_on_where(rest);
    let rest = rest.trim();

    // Now parse: <view_name> [AS OF NOW WITH SNAPSHOT | AS OF EPOCH n]
    let lower_rest = rest.to_lowercase();

    // Try "AS OF EPOCH <n>"
    if let Some(pos) = lower_rest.find(" as of epoch ") {
        let view_name = rest[..pos].trim().to_lowercase();
        let epoch_str = rest[pos + " as of epoch ".len()..].trim();
        let epoch: u64 = epoch_str
            .parse()
            .map_err(|_| ParseError(format!("invalid epoch: {epoch_str:?}")))?;
        return Ok(SubscribeRequest {
            view_name,
            start: SubscribeStart::Epoch(epoch),
            where_clause,
            projection,
        });
    }

    // Try "AS OF NOW WITH SNAPSHOT"
    if let Some(pos) = lower_rest.find(" as of now with snapshot") {
        let view_name = rest[..pos].trim().to_lowercase();
        return Ok(SubscribeRequest {
            view_name,
            start: SubscribeStart::NowWithSnapshot,
            where_clause,
            projection,
        });
    }

    // No AS OF clause — default to NowWithSnapshot
    let view_name = rest.trim().to_lowercase();
    if view_name.is_empty() {
        return Err(ParseError("missing view name".to_string()));
    }
    Ok(SubscribeRequest {
        view_name,
        start: SubscribeStart::NowWithSnapshot,
        where_clause,
        projection,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Split on the last occurrence of WHERE (so WHERE inside a projection is safe).
fn split_on_where(s: &str) -> (&str, Option<String>) {
    let lower = s.to_lowercase();
    // Find " where " — must be preceded by a space or be at a token boundary.
    if let Some(pos) = lower.rfind(" where ") {
        let pred = s[pos + " where ".len()..].trim().to_string();
        (&s[..pos], Some(pred))
    } else {
        (s, None)
    }
}

/// If the string ends with a parenthesised projection `(col, col, ...)`,
/// strip it and return the remaining prefix and the column list.
fn extract_trailing_projection(s: &str) -> (&str, Option<Vec<String>>) {
    let s = s.trim();
    if !s.ends_with(')') {
        return (s, None);
    }
    // Find the matching opening paren.
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut open_pos = None;
    for (i, &b) in bytes.iter().enumerate().rev() {
        match b {
            b')' => depth += 1,
            b'(' => {
                depth -= 1;
                if depth == 0 {
                    open_pos = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(open) = open_pos else {
        return (s, None);
    };
    // The paren must be preceded by whitespace or end of view name; not part of "SNAPSHOT".
    if open == 0 {
        return (s, None);
    }
    let col_str = &s[open + 1..s.len() - 1];
    let cols: Vec<String> = col_str
        .split(',')
        .map(|c| c.trim().to_lowercase())
        .filter(|c| !c.is_empty())
        .collect();
    if cols.is_empty() {
        return (s, None);
    }
    (&s[..open], Some(cols))
}

// ── Tests (S2 green gates) ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_parse_as_of_now_with_snapshot() {
        let req = parse_subscribe("SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT").unwrap();
        assert_eq!(req.view_name, "orders_mv");
        assert_eq!(req.start, SubscribeStart::NowWithSnapshot);
        assert!(req.where_clause.is_none());
        assert!(req.projection.is_none());
    }

    #[test]
    fn subscribe_parse_as_of_epoch() {
        let req = parse_subscribe("SUBSCRIBE orders_mv AS OF EPOCH 42").unwrap();
        assert_eq!(req.view_name, "orders_mv");
        assert_eq!(req.start, SubscribeStart::Epoch(42));
        assert!(req.where_clause.is_none());
        assert!(req.projection.is_none());
    }

    #[test]
    fn subscribe_parse_where_projection() {
        let req = parse_subscribe(
            "SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT WHERE id > 5 (id, value)",
        )
        .unwrap();
        assert_eq!(req.view_name, "orders_mv");
        assert_eq!(req.start, SubscribeStart::NowWithSnapshot);
        assert_eq!(req.where_clause.as_deref(), Some("id > 5"));
        assert_eq!(req.projection, Some(vec!["id".to_string(), "value".to_string()]));
    }

    #[test]
    fn subscribe_parse_default_no_as_of() {
        let req = parse_subscribe("SUBSCRIBE my_view").unwrap();
        assert_eq!(req.view_name, "my_view");
        assert_eq!(req.start, SubscribeStart::NowWithSnapshot);
    }

    #[test]
    fn subscribe_parse_epoch_with_semicolon() {
        let req = parse_subscribe("SUBSCRIBE my_view AS OF EPOCH 100;").unwrap();
        assert_eq!(req.start, SubscribeStart::Epoch(100));
    }

    #[test]
    fn subscribe_parse_error_no_view_name() {
        assert!(parse_subscribe("SUBSCRIBE").is_err());
        assert!(parse_subscribe("INSERT INTO foo VALUES (1)").is_err());
    }
}
