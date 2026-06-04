//! Local HTTP catalog REST server for RockStream (v0.52.5).
//!
//! Provides a simple HTTP endpoint that serves `rockstream_catalog` metadata
//! so that SQL-visible catalog data and HTTP responses stay in sync across
//! restart and direct HTTP checks (ROADMAP §v0.52.5).
//!
//! Endpoints
//! ---------
//! - `GET /catalog/v1/health`              — liveness probe
//! - `GET /catalog/v1/namespaces`          — list all namespaces
//! - `GET /catalog/v1/namespaces/{ns}/tables` — list tables in namespace
//! - `GET /catalog/v1/merge-laws`          — list registered merge laws
//!
//! The server is intentionally minimal: it uses only `tokio` TCP sockets
//! and the standard library for HTTP parsing so that no extra crate is added.
//!
//! # Auth model
//!
//! The endpoint is open by default (no TLS / no token required) in the local
//! development topology exercised by the E2E tests. A future hardening version
//! will add an optional bearer-token gate.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::rockstream_catalog::{catalog_merge_laws, CatalogMergeLaw};
use crate::InlineViewCatalog;
use rockstream_types::laws::LawRegistry;

// ── Namespace / table registry ───────────────────────────────────────────────

/// A thread-safe, in-process catalog registry shared between the SQL gateway
/// and the HTTP catalog server so both surfaces see the same state.
#[derive(Default, Clone)]
pub struct CatalogRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    /// namespace → list of table names
    namespaces: BTreeMap<String, Vec<String>>,
}

impl CatalogRegistry {
    /// Create a new empty registry seeded with the `public` namespace.
    pub fn new() -> Self {
        let mut inner = RegistryInner::default();
        // Seed with a `public` namespace that reflects the demo schema
        // pre-registered in the pgwire server.
        inner
            .namespaces
            .entry("public".to_string())
            .or_default()
            .extend(["orders_mv", "balances", "a", "b", "users"].map(String::from));
        inner
            .namespaces
            .entry("marketing".to_string())
            .or_default()
            .push("orders".to_string());
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Return all namespace names, sorted.
    pub fn namespaces(&self) -> Vec<String> {
        let g = self.inner.lock().unwrap();
        g.namespaces.keys().cloned().collect()
    }

    /// Return all table names for `namespace`, or an empty vec if unknown.
    pub fn tables_in(&self, namespace: &str) -> Vec<String> {
        let g = self.inner.lock().unwrap();
        g.namespaces.get(namespace).cloned().unwrap_or_default()
    }

    /// Register a table under a namespace. Idempotent.
    pub fn register_table(&self, namespace: &str, table: &str) {
        let mut g = self.inner.lock().unwrap();
        let entry = g.namespaces.entry(namespace.to_string()).or_default();
        if !entry.contains(&table.to_string()) {
            entry.push(table.to_string());
        }
    }

    /// Sync from an `InlineViewCatalog` so that newly created inline views
    /// appear in the HTTP catalog without a restart.
    pub fn sync_from_inline_catalog(&self, cat: &InlineViewCatalog) {
        for name in cat.view_names() {
            self.register_table("public", &name);
        }
    }
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

fn json_ok(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn not_found() -> String {
    let body = r#"{"error":"not found"}"#;
    format!(
        "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_request() -> String {
    let body = r#"{"error":"bad request"}"#;
    format!(
        "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

// ── Connection handler ────────────────────────────────────────────────────────

async fn handle_connection(mut stream: TcpStream, registry: CatalogRegistry) {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);

    // Read the request line.
    let mut request_line = String::new();
    if buf_reader.read_line(&mut request_line).await.is_err() {
        return;
    }
    let request_line = request_line.trim().to_string();

    // Drain headers (we don't need them).
    loop {
        let mut header = String::new();
        match buf_reader.read_line(&mut header).await {
            Ok(0) | Err(_) => break,
            Ok(_) if header.trim().is_empty() => break,
            _ => {}
        }
    }

    // Parse: METHOD PATH HTTP/1.x
    let parts: Vec<&str> = request_line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        let _ = writer.write_all(bad_request().as_bytes()).await;
        return;
    }
    let method = parts[0];
    let path = parts[1];

    if method != "GET" {
        let body = r#"{"error":"method not allowed"}"#;
        let resp = format!(
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(), body
        );
        let _ = writer.write_all(resp.as_bytes()).await;
        return;
    }

    // Route.
    let response = route(path, &registry);
    let _ = writer.write_all(response.as_bytes()).await;
}

fn route(path: &str, registry: &CatalogRegistry) -> String {
    // Strip query string.
    let path = path.split('?').next().unwrap_or(path);

    match path {
        "/catalog/v1/health" | "/catalog/v1/health/" => json_ok(r#"{"status":"ok"}"#),
        "/catalog/v1/namespaces" | "/catalog/v1/namespaces/" => {
            let namespaces = registry.namespaces();
            let items: Vec<String> = namespaces
                .iter()
                .map(|ns| format!(r#"{{"name":"{}"}}"#, escape_json(ns)))
                .collect();
            let body = format!(r#"{{"namespaces":[{}]}}"#, items.join(","));
            json_ok(&body)
        }
        "/catalog/v1/merge-laws" | "/catalog/v1/merge-laws/" => {
            let registry_inner = LawRegistry::with_builtins();
            let laws: Vec<CatalogMergeLaw> = catalog_merge_laws(&registry_inner);
            let items: Vec<String> = laws
                .iter()
                .map(|l| {
                    format!(
                        r#"{{"id":{},"name":"{}","version":{},"class":"{}","idempotent":{}}}"#,
                        l.id,
                        escape_json(&l.name),
                        l.version,
                        escape_json(&l.class),
                        l.idempotent,
                    )
                })
                .collect();
            let body = format!(r#"{{"laws":[{}]}}"#, items.join(","));
            json_ok(&body)
        }
        p if p.starts_with("/catalog/v1/namespaces/") => {
            // Try to match /catalog/v1/namespaces/{ns}/tables
            let rest = &p["/catalog/v1/namespaces/".len()..];
            let rest = rest.trim_end_matches('/');
            if let Some(ns) = rest.strip_suffix("/tables") {
                let tables = registry.tables_in(ns);
                let items: Vec<String> = tables
                    .iter()
                    .map(|t| {
                        format!(
                            r#"{{"name":"{}","namespace":"{}"}}"#,
                            escape_json(t),
                            escape_json(ns)
                        )
                    })
                    .collect();
                let body = format!(
                    r#"{{"namespace":"{}","tables":[{}]}}"#,
                    escape_json(ns),
                    items.join(",")
                );
                json_ok(&body)
            } else {
                // GET /catalog/v1/namespaces/{ns} — namespace detail
                let ns = rest;
                if registry.namespaces().contains(&ns.to_string()) {
                    let body = format!(r#"{{"name":"{}"}}"#, escape_json(ns));
                    json_ok(&body)
                } else {
                    not_found()
                }
            }
        }
        _ => not_found(),
    }
}

/// Minimal JSON string escaping (handles the characters we actually encounter
/// in namespace / table names, which are all ASCII identifiers).
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

// ── Server entry point ────────────────────────────────────────────────────────

/// Start the catalog HTTP server and serve until the process ends.
///
/// `bind_addr` — TCP address to listen on, e.g. `"0.0.0.0:8181"`.
pub async fn run_catalog_rest_server(
    bind_addr: &str,
    registry: CatalogRegistry,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = %bind_addr, "catalog REST server listening");
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let reg = registry.clone();
                tokio::spawn(async move {
                    handle_connection(stream, reg).await;
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "catalog REST: accept error");
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry() -> CatalogRegistry {
        CatalogRegistry::new()
    }

    #[test]
    fn route_health() {
        let reg = make_registry();
        let resp = route("/catalog/v1/health", &reg);
        assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
        assert!(resp.contains(r#""status":"ok""#));
    }

    #[test]
    fn route_namespaces() {
        let reg = make_registry();
        let resp = route("/catalog/v1/namespaces", &reg);
        assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
        assert!(resp.contains("public"));
        assert!(resp.contains("marketing"));
    }

    #[test]
    fn route_tables_in_public() {
        let reg = make_registry();
        let resp = route("/catalog/v1/namespaces/public/tables", &reg);
        assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
        assert!(resp.contains("orders_mv"));
        assert!(resp.contains("balances"));
    }

    #[test]
    fn route_tables_unknown_namespace() {
        let reg = make_registry();
        let resp = route("/catalog/v1/namespaces/nonexistent/tables", &reg);
        // Returns 200 with an empty table list (namespace doesn't exist, so tables_in returns [])
        assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
        assert!(resp.contains(r#""tables":[]"#));
    }

    #[test]
    fn route_unknown_path() {
        let reg = make_registry();
        let resp = route("/catalog/v1/unknown", &reg);
        assert!(resp.contains("404 Not Found"), "expected 404, got: {resp}");
    }

    #[test]
    fn route_merge_laws() {
        let reg = make_registry();
        let resp = route("/catalog/v1/merge-laws", &reg);
        assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
        assert!(resp.contains("WeightAdd"));
        assert!(resp.contains("SumCount"));
    }

    #[test]
    fn register_table_idempotent() {
        let reg = make_registry();
        reg.register_table("test_ns", "my_table");
        reg.register_table("test_ns", "my_table"); // duplicate
        let tables = reg.tables_in("test_ns");
        assert_eq!(
            tables.iter().filter(|t| t.as_str() == "my_table").count(),
            1
        );
    }

    #[test]
    fn namespaces_sorted() {
        let reg = CatalogRegistry::new();
        reg.register_table("zebra", "t1");
        reg.register_table("alpha", "t2");
        let ns = reg.namespaces();
        let mut sorted = ns.clone();
        sorted.sort();
        assert_eq!(ns, sorted, "namespaces must be lexicographically sorted");
    }

    #[test]
    fn escape_json_basic() {
        assert_eq!(escape_json("hello"), "hello");
        assert_eq!(escape_json(r#"say "hi""#), r#"say \"hi\""#);
        assert_eq!(escape_json("a\\b"), "a\\\\b");
    }
}
