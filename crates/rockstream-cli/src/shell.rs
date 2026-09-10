//! Interactive multiline SQL REPL shell over pgwire.
//!
//! Provides interactive command execution connecting to RockStream gateway.
//! Features multiline buffering, semicolon statement termination, meta-commands,
//! bounded input accumulation (64 KB), and error recovery.

use crate::client::{connect_client, execute_query, MAX_SHELL_STATEMENT_BYTES};
use crate::CliError;
use std::io::{self, BufRead, Write};

/// Synchronous entry point for `rockstream shell`.
pub fn run_interactive_shell(endpoint: &str) -> Result<String, CliError> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        CliError::new(
            rockstream_types::error_code::RS_0004,
            format!("failed to initialize tokio runtime for shell: {e}"),
            "Check system resources and limits.",
        )
    })?;

    rt.block_on(async move {
        let (client, _handle) = connect_client(endpoint, 30).await?;

        println!("RockStream interactive SQL shell (pgwire)");
        println!("Connected to {endpoint}");
        println!("Type \\q to exit, \\? for help. Terminate statements with a semicolon (;).\n");

        let stdin = io::stdin();
        run_shell_with_io(&client, stdin.lock(), io::stdout()).await?;
        Ok("Shell session closed.".to_string())
    })
}

/// Generic REPL loop supporting arbitrary I/O streams.
pub async fn run_shell_with_io<R: BufRead, W: Write>(
    client: &tokio_postgres::Client,
    mut reader: R,
    mut writer: W,
) -> Result<(), CliError> {
    let mut buffer = String::new();

    loop {
        if buffer.is_empty() {
            write!(writer, "rockstream=> ").map_err(io_err)?;
        } else {
            write!(writer, "       -> ").map_err(io_err)?;
        }
        let _ = writer.flush();

        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                let _ = writeln!(writer, "Error reading input: {e}");
                break;
            }
        }

        let trimmed = line.trim();

        // Meta commands on single-line empty buffer
        if buffer.is_empty() {
            if trimmed == "\\q" || trimmed == "exit" || trimmed == "quit" {
                break;
            }
            if trimmed == "\\?" || trimmed == "\\h" || trimmed == "help" {
                let _ = writeln!(writer, "General help:");
                let _ = writeln!(writer, "  \\q              Quit the shell");
                let _ = writeln!(writer, "  \\?              Show this help");
                let _ = writeln!(writer, "  \\d              List tables and views");
                let _ = writeln!(
                    writer,
                    "  <SQL statement>; Execute query terminated by semicolon (;)\n"
                );
                continue;
            }
            if trimmed == "\\d" {
                match execute_query(client, "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name;").await {
                    Ok(res) => {
                        let _ = write!(writer, "{}", res.format_table(false));
                    }
                    Err(e) => {
                        let _ = writeln!(writer, "Error: {}", e.message);
                    }
                }
                continue;
            }
        }

        if buffer.len() + line.len() > MAX_SHELL_STATEMENT_BYTES {
            let _ = writeln!(
                writer,
                "RS-0005: shell statement buffer exceeded maximum limit of {} bytes; resetting statement buffer.",
                MAX_SHELL_STATEMENT_BYTES
            );
            buffer.clear();
            continue;
        }

        buffer.push_str(&line);

        if buffer.trim().ends_with(';') {
            let stmt = buffer.trim().to_string();
            buffer.clear();

            if !stmt.is_empty() {
                match execute_query(client, &stmt).await {
                    Ok(res) => {
                        let _ = write!(writer, "{}", res.format_table(true));
                    }
                    Err(e) => {
                        let _ = writeln!(writer, "Error {}: {}", e.code, e.message);
                        if !e.next_steps.is_empty() {
                            let _ = writeln!(writer, "Next steps: {}", e.next_steps);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn io_err(e: io::Error) -> CliError {
    CliError::new(
        rockstream_types::error_code::RS_0004,
        format!("shell I/O error: {e}"),
        "Check system I/O streams.",
    )
}
