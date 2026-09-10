//! Embedded PostgreSQL client for `rockstream query`, `rockstream shell`, and project operations.
//!
//! Communicates strictly over live TCP pgwire using `tokio-postgres`. Never invokes
//! gateway Rust handlers or mock clients directly in production.

use crate::CliError;
use rockstream_types::error_code::{ErrorCode, RS_0004, RS_0005, RS_2001, RS_2005};
use std::fs;
use std::path::Path;
use std::time::Instant;
use tokio_postgres::NoTls;

/// Maximum allowed SQL file size in bytes (10 MB).
pub const MAX_SQL_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;

/// Maximum allowed query result rows accumulated in memory.
pub const MAX_QUERY_RESULT_ROWS: usize = 10_000;

/// Maximum statement length allowed in shell accumulator (64 KB).
pub const MAX_SHELL_STATEMENT_BYTES: usize = 64 * 1024;

/// Parse host and port from endpoint string, e.g. "127.0.0.1:5432" or "localhost:5432".
pub fn parse_endpoint(endpoint: &str) -> (String, u16) {
    if let Some((host, port_str)) = endpoint.split_once(':') {
        let port = port_str.parse::<u16>().unwrap_or(5432);
        (host.to_string(), port)
    } else {
        (endpoint.to_string(), 5432)
    }
}

/// Connect to RockStream gateway over pgwire.
pub async fn connect_client(
    endpoint: &str,
    timeout_secs: u64,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), CliError> {
    let (host, port) = parse_endpoint(endpoint);
    let config_str = format!(
        "host={} port={} user=rockstream dbname=rockstream connect_timeout={}",
        host, port, timeout_secs
    );

    let connect_future = tokio_postgres::connect(&config_str, NoTls);
    let timeout_duration = std::time::Duration::from_secs(timeout_secs);

    let (client, connection) = match tokio::time::timeout(timeout_duration, connect_future).await {
        Ok(Ok((c, conn))) => (c, conn),
        Ok(Err(e)) => {
            let msg = format!("cannot reach RockStream gateway at {endpoint}: {e}");
            return Err(CliError::new(
                RS_0004,
                msg,
                "Verify that 'rockstream start' is running and the configured gateway endpoint is accessible.",
            ));
        }
        Err(_) => {
            return Err(CliError::new(
                RS_0004,
                format!("connection to RockStream gateway at {endpoint} timed out after {timeout_secs}s"),
                "Verify that 'rockstream start' is running and responsive.",
            ));
        }
    };

    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    Ok((client, handle))
}

/// Structured row representation for table/JSON/CSV rendering.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub elapsed_ms: f64,
}

impl QueryResult {
    pub fn format_table(&self, include_timing: bool) -> String {
        if self.columns.is_empty() && self.rows.is_empty() {
            let mut out = "(0 rows)
"
            .to_string();
            if include_timing {
                out.push_str(&format!(
                    "Time: {:.2} ms
",
                    self.elapsed_ms
                ));
            }
            return out;
        }

        let num_cols = self.columns.len();
        let mut col_widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();

        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                let len = cell.as_deref().unwrap_or("NULL").len();
                if i < col_widths.len() && len > col_widths[i] {
                    col_widths[i] = len;
                }
            }
        }

        let mut out = String::new();

        // Header
        let header_cells: Vec<String> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{:width$}", col, width = col_widths[i]))
            .collect();
        out.push_str(&header_cells.join(" | "));
        out.push('\n');

        // Separator
        let sep_cells: Vec<String> = col_widths.iter().map(|&w| "-".repeat(w)).collect();
        out.push_str(&sep_cells.join("-+-"));
        out.push('\n');

        // Rows
        for row in &self.rows {
            let row_cells: Vec<String> = (0..num_cols)
                .map(|i| {
                    let text = row.get(i).and_then(|c| c.as_deref()).unwrap_or("NULL");
                    format!("{:width$}", text, width = col_widths[i])
                })
                .collect();
            out.push_str(&row_cells.join(" | "));
            out.push('\n');
        }

        let row_count = self.rows.len();
        out.push_str(&format!(
            "({row_count} row{})
",
            if row_count == 1 { "" } else { "s" }
        ));
        if include_timing {
            out.push_str(&format!(
                "Time: {:.2} ms
",
                self.elapsed_ms
            ));
        }

        out
    }

    pub fn format_json(&self, include_timing: bool) -> String {
        let mut array = Vec::new();
        for row in &self.rows {
            let mut map = serde_json::Map::new();
            for (i, col) in self.columns.iter().enumerate() {
                let val = match row.get(i).and_then(|c| c.as_ref()) {
                    Some(s) => {
                        // Attempt numeric / boolean parse, otherwise string
                        if let Ok(b) = s.parse::<bool>() {
                            serde_json::Value::Bool(b)
                        } else if let Ok(n) = s.parse::<i64>() {
                            serde_json::Value::Number(serde_json::Number::from(n))
                        } else if let Ok(f) = s.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(f) {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::String(s.clone())
                            }
                        } else {
                            serde_json::Value::String(s.clone())
                        }
                    }
                    None => serde_json::Value::Null,
                };
                map.insert(col.clone(), val);
            }
            array.push(serde_json::Value::Object(map));
        }

        let json_text = serde_json::to_string_pretty(&serde_json::Value::Array(array))
            .unwrap_or_else(|_| "[]".to_string());

        if include_timing {
            format!(
                "{json_text}
// Time: {:.2} ms",
                self.elapsed_ms
            )
        } else {
            json_text
        }
    }

    pub fn format_csv(&self, include_timing: bool) -> String {
        let mut out = String::new();

        // Header
        let escaped_headers: Vec<String> =
            self.columns.iter().map(|c| escape_csv_cell(c)).collect();
        out.push_str(&escaped_headers.join(","));
        out.push('\n');

        // Rows
        for row in &self.rows {
            let cells: Vec<String> = row
                .iter()
                .map(|cell| match cell {
                    Some(s) => escape_csv_cell(s),
                    None => "".to_string(),
                })
                .collect();
            out.push_str(&cells.join(","));
            out.push('\n');
        }

        if include_timing {
            out.push_str(&format!(
                "# Time: {:.2} ms
",
                self.elapsed_ms
            ));
        }

        out
    }
}

fn escape_csv_cell(s: &str) -> String {
    if s.contains(',') || s.contains('\n') || s.contains('\r') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Map errors from tokio-postgres to RockStream CliError.
pub fn map_pg_error(e: tokio_postgres::Error) -> CliError {
    let err_str = e.to_string();

    // Check if error contains an RS-XXXX code
    if let Some(db_err) = e.as_db_error() {
        let message = db_err.message();
        let code = extract_or_map_code(message, db_err.code().code());
        return CliError::new(
            code,
            format!("query execution failed: {message}"),
            "Verify SQL syntax, referenced tables/views, and column types.",
        );
    }

    let code = extract_or_map_code(&err_str, "");
    CliError::new(
        code,
        format!("database operation failed: {err_str}"),
        "Verify query statement and gateway connection.",
    )
}

fn extract_or_map_code(msg: &str, pg_code: &str) -> ErrorCode {
    if let Some(pos) = msg.find("RS-") {
        let snippet = &msg[pos..];
        if snippet.len() >= 7 {
            let code_str = &snippet[..7];
            if let Ok(num) = code_str[3..].parse::<u16>() {
                return ErrorCode::new(num);
            }
        }
    }

    match pg_code {
        "42P01" => RS_2005,                     // undefined table / view
        "42601" => RS_2001,                     // syntax error
        "53000" | "53100" | "53200" => RS_0005, // resource / limit
        "08001" | "08006" => RS_0004,           // connection failure
        _ => {
            if msg.contains("syntax error") || msg.contains("cannot parse") {
                RS_2001
            } else if msg.contains("not found") || msg.contains("does not exist") {
                RS_2005
            } else if msg.contains("connection refused") || msg.contains("cannot reach") {
                RS_0004
            } else {
                RS_2001
            }
        }
    }
}

/// Execute a query string over an established client.
pub async fn execute_query(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<QueryResult, CliError> {
    let start = Instant::now();

    let messages = client.simple_query(sql).await.map_err(map_pg_error)?;

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();

    for msg in messages {
        match msg {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                if columns.is_empty() {
                    let len = r.columns().len();
                    for i in 0..len {
                        columns.push(r.columns()[i].name().to_string());
                    }
                }

                if rows.len() >= MAX_QUERY_RESULT_ROWS {
                    return Err(CliError::new(
                        RS_0005,
                        format!(
                            "query result exceeded maximum allowed limit of {} rows",
                            MAX_QUERY_RESULT_ROWS
                        ),
                        "Add a LIMIT clause to restrict query result accumulation.",
                    ));
                }

                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                rows.push(row);
            }
            tokio_postgres::SimpleQueryMessage::CommandComplete(_cnt) => {}
            _ => {}
        }
    }

    Ok(QueryResult {
        columns,
        rows,
        elapsed_ms,
    })
}

/// Entry point for `rockstream query`.
pub async fn run_embedded_query(
    query: &str,
    file: Option<&Path>,
    format_name: &str,
    timing: bool,
    endpoint: &str,
) -> Result<String, CliError> {
    let sql = if let Some(path) = file {
        let metadata = fs::metadata(path).map_err(|e| {
            CliError::new(
                RS_0004,
                format!("failed to read SQL file '{}': {e}", path.display()),
                "Verify file path and read permissions.",
            )
        })?;

        if metadata.len() > MAX_SQL_FILE_SIZE_BYTES {
            return Err(CliError::new(
                RS_0005,
                format!(
                    "SQL file '{}' exceeds maximum allowed size of 10 MB (size: {} bytes)",
                    path.display(),
                    metadata.len()
                ),
                "Reduce SQL file size or execute statements in smaller batches.",
            ));
        }

        fs::read_to_string(path).map_err(|e| {
            CliError::new(
                RS_0004,
                format!("failed to read SQL file '{}': {e}", path.display()),
                "Verify file read permissions.",
            )
        })?
    } else {
        if query.trim().is_empty() {
            return Err(CliError::new(
                RS_2001,
                "no SQL query provided; specify a query string or --file <path>",
                "Pass a query string like 'SELECT 1;' or use --file <path>.",
            ));
        }
        query.to_string()
    };

    let (client, _handle) = connect_client(endpoint, 30).await?;
    let result = execute_query(&client, &sql).await?;

    let rendered = match format_name.to_lowercase().as_str() {
        "json" => result.format_json(timing),
        "csv" => result.format_csv(timing),
        _ => result.format_table(timing),
    };

    Ok(rendered)
}
