//! `GatewayServer` — PostgreSQL wire protocol server.
//!
//! Accepts TCP connections and serves reads of maintained views using the
//! pgwire library. The same handler implements both simple and extended query
//! protocols.

use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::SinkExt;
use futures::{stream, Sink, StreamExt};
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
    FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{
    CopyData, CopyData as MsgCopyData, CopyDone, CopyDone as MsgCopyDone, CopyFail,
    CopyOutResponse as MsgCopyOutResponse,
};
use pgwire::messages::response::CommandComplete;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use crate::auth::{AuthMode, JwtVerifier, Principal};
use crate::catalog_stubs::{
    arrow_type_to_pg_oid, CatalogColumn, CatalogResponse, CatalogStubs, CatalogTable,
};
use crate::copy_state::{
    CopyState, COPY_IN_BUFFER_ROWS, COPY_IN_FLUSH_BYTES, MAX_COPY_IN_BATCH_ROWS,
};
use crate::session::{FreshnessToken, SessionState};
use crate::view_reader::{ViewReadStrategy, ViewReader};
use crate::write_buffer::{DmlOp, WriteBuffer};

// ── S9 metrics ────────────────────────────────────────────────────────────────

/// Total number of session RYW / explicit wait_for triggers.
pub static SESSION_WAIT_FOR_TRIGGERED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Cumulative milliseconds spent satisfying wait_for epochs.
pub static SESSION_WAIT_FOR_SATISFIED_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Number of wait_for calls that timed out (RS-2012).
pub static SESSION_WAIT_FOR_TIMEOUT_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// ── S8 WaitResult ─────────────────────────────────────────────────────────────

#[derive(Debug)]
enum WaitResult {
    Satisfied { elapsed_ms: u64 },
    TimedOut,
    NoStorage,
}

// ── Postgres Type from OID helper ─────────────────────────────────────────────

fn pg_type_from_oid(oid: i32) -> Type {
    match oid {
        23 => Type::INT4,
        20 => Type::INT8,
        701 => Type::FLOAT8,
        25 => Type::TEXT,
        16 => Type::BOOL,
        17 => Type::BYTEA,
        1114 => Type::TIMESTAMP,
        _ => Type::TEXT,
    }
}

// ── GatewayHandler ────────────────────────────────────────────────────────────

/// Core handler shared across all pgwire protocol phases.
///
/// `Arc<GatewayHandler>` is the `PgWireServerHandlers` factory.
pub struct GatewayHandler {
    catalog: Arc<CatalogStubs>,
    view_reader: Arc<dyn ViewReader>,
    query_parser: Arc<NoopQueryParser>,
    /// Per-connection write buffers keyed by connection ID.
    /// Bound: WRITE_BUFFER_LIMIT_BYTES per connection (64 MiB).
    write_buffers: Arc<DashMap<String, WriteBuffer>>,
    /// Per-connection COPY IN state keyed by connection ID.
    /// Bound: MAX_COPY_IN_BATCH_ROWS rows or COPY_IN_FLUSH_BYTES bytes.
    copy_states: Arc<DashMap<String, CopyState>>,
    /// Per-connection session state (idempotency key, isolation, etc.).
    pub sessions: Arc<DashMap<String, SessionState>>,
    /// Optional ShardDb for direct-write DML commits.
    shard_db: Option<Arc<rockstream_storage::ShardDb>>,
    /// Authentication mode for this gateway instance.
    auth_mode: AuthMode,
    /// Optional JWT verifier (populated when auth_mode == Oidc).
    jwt_verifier: Option<Arc<JwtVerifier>>,
    /// ACL store for RBAC enforcement.
    pub acl_store: Arc<rockstream_control::AclStore>,
    /// Namespace catalog.
    namespace_catalog: Arc<rockstream_control::NamespaceCatalog>,
    audit_log: Option<Arc<rockstream_control::audit::FileAuditLog>>,
}

impl GatewayHandler {
    pub fn new(catalog: Arc<CatalogStubs>, view_reader: Arc<dyn ViewReader>) -> Self {
        GatewayHandler {
            catalog,
            view_reader,
            query_parser: Arc::new(NoopQueryParser),
            write_buffers: Arc::new(DashMap::new()),
            copy_states: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            shard_db: None,
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
        }
    }

    pub fn with_shard_db(
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        GatewayHandler {
            catalog,
            view_reader,
            query_parser: Arc::new(NoopQueryParser),
            write_buffers: Arc::new(DashMap::new()),
            copy_states: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            shard_db: Some(shard_db),
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
        }
    }

    pub fn with_audit_log(mut self, log: Arc<rockstream_control::audit::FileAuditLog>) -> Self {
        self.audit_log = Some(log);
        self
    }

    /// Wait until the shard's frontier epoch reaches `target_epoch` or `timeout_ms` elapses.
    ///
    /// Polls the in-memory `last_epoch` AtomicU64 every 10 ms.
    /// Returns immediately with `NoStorage` if no shard is configured.
    async fn wait_for_epoch(&self, target_epoch: u64, timeout_ms: u64) -> WaitResult {
        let Some(shard_db) = &self.shard_db else {
            return WaitResult::NoStorage;
        };
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        loop {
            let current = shard_db.last_epoch().load(Ordering::SeqCst);
            if current >= target_epoch {
                return WaitResult::Satisfied {
                    elapsed_ms: start.elapsed().as_millis() as u64,
                };
            }
            if start.elapsed() >= timeout {
                return WaitResult::TimedOut;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Dispatch a synchronous (non-view-read) query and return pgwire responses.
    /// Returns `None` if the query needs async handling (DML, COMMIT, ROLLBACK, SELECT).
    fn dispatch_sync<'a>(&'a self, query: &'a str) -> Option<PgWireResult<Vec<Response<'a>>>> {
        let q = query.trim();
        let ql = q.to_lowercase();

        // SERIALIZABLE → RS-2003
        if ql.contains("serializable") && ql.contains("isolation") {
            return Some(Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25001".to_owned(),
                "[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED or REPEATABLE READ".to_owned(),
            )))]));
        }

        // Catalog stubs
        if let Some(catalog_resp) = self.catalog.handle_query(q) {
            return Some(Ok(vec![catalog_resp_to_response(catalog_resp)]));
        }

        // COPY <view> TO STDOUT — handled via streaming in do_query; skip here.
        // CREATE VIEW / CREATE MATERIALIZED VIEW
        if ql.starts_with("create view ")
            || ql.starts_with("create materialized view ")
            || ql.starts_with("create or replace view ")
        {
            return Some(self.handle_create_view(q));
        }

        // CREATE TABLE [IF NOT EXISTS] — register in catalog
        if ql.starts_with("create table ") || ql.starts_with("create table if not exists ") {
            return Some(self.handle_create_table(q));
        }

        // Transaction control
        if ql == "begin" || ql == "begin;" || ql.starts_with("begin ") {
            return Some(Ok(vec![Response::TransactionStart(
                Tag::new("BEGIN").with_rows(0),
            )]));
        }

        // COMMIT and ROLLBACK are handled in dispatch_async (need write buffer access).

        None
    }

    /// Dispatch any query asynchronously.  Catalog/session queries are handled
    /// immediately; DML/COMMIT/ROLLBACK uses write buffer; view SELECT queries await the storage read.
    async fn dispatch_async(&self, query: &str) -> PgWireResult<Vec<Response<'static>>> {
        self.dispatch_async_with_conn(query, None).await
    }

    /// Dispatch with an optional connection ID for write buffer routing.
    pub async fn dispatch_async_with_conn(
        &self,
        query: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let q = query.trim();
        let ql = q.to_lowercase();

        // SET rockstream.* must be intercepted before catalog stubs handle generic SET commands.
        if ql.starts_with("set rockstream.") || ql.starts_with("set local rockstream.") {
            return self.handle_set_rockstream(q, &ql, conn_id);
        }

        // SET search_path = <namespace> (v0.26 namespace isolation)
        if ql.starts_with("set search_path") || ql.starts_with("set local search_path") {
            if let Some(id) = conn_id {
                // Extract namespace: SET search_path = <ns> or SET search_path TO <ns>
                let after_eq = if let Some(pos) = ql.find('=') {
                    q[pos + 1..]
                        .trim()
                        .trim_end_matches(';')
                        .trim()
                        .trim_matches('\'')
                        .to_string()
                } else if let Some(pos) = ql.find(" to ") {
                    q[pos + 4..]
                        .trim()
                        .trim_end_matches(';')
                        .trim()
                        .trim_matches('\'')
                        .to_string()
                } else {
                    "public".to_string()
                };
                let ns = after_eq
                    .split(',')
                    .next()
                    .unwrap_or("public")
                    .trim()
                    .trim_matches('"')
                    .to_string();
                let mut session = self
                    .sessions
                    .entry(id.to_string())
                    .or_insert_with(SessionState::new);
                session.current_namespace = ns.clone();
                session.search_path = ns;
            }
            return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
        }

        // CREATE NAMESPACE <name> (v0.26)
        if ql.starts_with("create namespace ") {
            let after = q["create namespace ".len()..].trim().trim_end_matches(';');
            let ns_name = after.trim().to_lowercase();
            if !ns_name.is_empty() {
                self.namespace_catalog.create_namespace(&ns_name);
                if let Some(log) = &self.audit_log {
                    let actor = conn_id
                        .and_then(|id| self.sessions.get(id).map(|s| s.principal.actor()))
                        .unwrap_or_else(|| "system".to_string());
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        actor,
                        "create_namespace",
                        &ns_name,
                    ));
                }
            }
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("CREATE NAMESPACE").with_rows(0),
            ))]);
        }

        // EXPLAIN <query> — return plan annotation with pushdown info.
        if ql.starts_with("explain ") {
            let inner_sql = q["explain ".len()..].trim();
            let pushdown = crate::multi_shard_reader::can_pushdown_partial_agg(inner_sql);
            let pushdown_note = if pushdown {
                "partial_pushdown: true  -- O(distinct_groups × shards) rows returned"
            } else {
                "partial_pushdown: false"
            };
            let plan_text = format!("Plan: SeqScan → {pushdown_note}\nQuery: {inner_sql}");
            let schema = Arc::new(vec![FieldInfo::new(
                "QUERY PLAN".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let rows = vec![plan_text];
            let schema_ref = schema.clone();
            let data_stream = stream::iter(rows).map(move |line| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&Some(line.as_str()))?;
                encoder.finish()
            });
            return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                schema,
                data_stream,
            )))]);
        }

        if let Some(result) = self.dispatch_sync(query) {
            // Promote lifetime — responses from dispatch_sync hold no borrows
            // from `query`, only owned data.
            return result.map(|v| v.into_iter().map(promote_response).collect());
        }

        // COMMIT — flush write buffer to shard atomically.
        if ql == "commit" || ql == "commit;" {
            return self.handle_commit(conn_id).await;
        }

        // ROLLBACK — discard write buffer.
        if ql == "rollback" || ql == "rollback;" {
            return self.handle_rollback(conn_id).await;
        }

        // INSERT — accumulate in write buffer.
        if ql.starts_with("insert into ") {
            return self.handle_insert(q, conn_id).await;
        }

        // UPDATE — accumulate in write buffer.
        if ql.starts_with("update ") {
            return self.handle_update(q, conn_id).await;
        }

        // DELETE — accumulate in write buffer.
        if ql.starts_with("delete from ") {
            return self.handle_delete(q, conn_id).await;
        }

        // SELECT … FROM <view> [LIMIT n]
        // Apply explicit wait_for or session RYW before reading (S8/S9).
        if ql.contains("from ") {
            if let Some(id) = conn_id {
                let (wait_token, timeout_ms) = {
                    let mut session = self
                        .sessions
                        .entry(id.to_string())
                        .or_insert_with(SessionState::new);
                    // Explicit wait_for takes priority; fall back to session RYW.
                    let explicit = session.wait_for_token.take();
                    let auto = if session.session_wait_for_enabled {
                        session.last_written_epoch.clone()
                    } else {
                        None
                    };
                    let timeout = session.session_wait_for_timeout_ms;
                    (explicit.or(auto), timeout)
                    // RefMut drops here — must not hold across await
                };
                if let Some(token) = wait_token {
                    SESSION_WAIT_FOR_TRIGGERED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match self.wait_for_epoch(token.source_epoch, timeout_ms).await {
                        WaitResult::Satisfied { elapsed_ms } => {
                            SESSION_WAIT_FOR_SATISFIED_MS.fetch_add(elapsed_ms, Ordering::Relaxed);
                        }
                        WaitResult::TimedOut => {
                            SESSION_WAIT_FOR_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                "[RS-2012] wait.read_your_writes_timeout: \
                                 wait_for epoch {} timed out after {timeout_ms}ms — \
                                 proceeding at current frontier",
                                token.source_epoch
                            );
                        }
                        WaitResult::NoStorage => {}
                    }
                }
            }

            if let Some(view_name) = extract_view_name_from_select(q) {
                if !view_name.starts_with("pg_") && !view_name.starts_with("information_schema") {
                    let limit = extract_limit(q);
                    return self.read_view_response(&view_name, limit, conn_id).await;
                }
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("OK")))])
    }

    /// Read rows from a view and build a pgwire `Response::Query`.
    /// Enforces ACL (RS-2401) and namespace isolation (RS-2402) when conn_id is provided.
    async fn read_view_response(
        &self,
        view_name: &str,
        limit: Option<usize>,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        // Get principal and session namespace (v0.26)
        let (principal, session_namespace) = if let Some(id) = conn_id {
            let session = self
                .sessions
                .entry(id.to_string())
                .or_insert_with(SessionState::new);
            (session.principal.clone(), session.current_namespace.clone())
        } else {
            (Principal::System, "public".to_string())
        };

        // ACL check: Viewer role required for SELECT (RS-2401)
        use rockstream_types::acl::Role;
        if !principal.is_system() {
            if let Err(e) = self.acl_store.check(
                principal.identity(),
                &session_namespace,
                Some(view_name),
                Role::Viewer,
            ) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42501".to_owned(), e.to_string()),
                )))]);
            }
        }

        // Namespace isolation check (RS-2402)
        if let Some(cv) = self.catalog.get_view(view_name) {
            let view_ns = &cv.namespace;
            if view_ns != &session_namespace && !principal.is_system() {
                // Check if principal has Admin in own namespace (can cross-namespace)
                let is_admin = self
                    .acl_store
                    .check(principal.identity(), &session_namespace, None, Role::Admin)
                    .is_ok();
                if !is_admin {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42501".to_owned(),
                            format!(
                                "[RS-2402] auth.namespace_access_denied: principal '{}' cannot access namespace '{}' from session namespace '{}'",
                                principal.identity(), view_ns, session_namespace
                            ),
                        ),
                    )))]);
                }
            }
        }

        let schema_fields: Vec<FieldInfo> = if let Some(cv) = self.catalog.get_view(view_name) {
            cv.columns
                .iter()
                .map(|c| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    FieldInfo::new(
                        c.name.clone(),
                        None,
                        None,
                        pg_type_from_oid(oid),
                        FieldFormat::Text,
                    )
                })
                .collect()
        } else {
            vec![FieldInfo::new(
                "result".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]
        };

        // Prefer reading directly from ShardDb when available.  ShardDb reads
        // from its in-memory memtable (WAL + SSTs), which reflects the latest
        // committed writes immediately after the post-COMMIT flush.  The
        // ShardReader (DbReader) polls for a new manifest every 1 s and would
        // return stale results until the next poll fires.
        let raw_rows: Vec<Vec<u8>> = if let Some(shard_db) = &self.shard_db {
            let prefix = format!("view_output/{view_name}/");
            let kvs = shard_db
                .scan_prefix(prefix.as_bytes())
                .await
                .map_err(|e| {
                    PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                })?;
            let mut rows: Vec<Vec<u8>> = kvs.into_iter().map(|(_, v)| v.to_vec()).collect();
            if let Some(n) = limit {
                rows.truncate(n);
            }
            rows
        } else {
            self.view_reader
                .read_view(view_name, limit, ViewReadStrategy::HotOnly)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        };

        let schema = Arc::new(schema_fields);
        let schema_ref = schema.clone();
        let data_stream = stream::iter(raw_rows).map(move |raw: Vec<u8>| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let row_str = String::from_utf8_lossy(&raw).into_owned();
            let col_count = schema_ref.len();
            let fields: Vec<&str> = row_str.split('\t').collect();
            for i in 0..col_count {
                let val: Option<&str> = fields.get(i).copied();
                encoder
                    .encode_field(&val)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_stream,
        ))])
    }

    fn handle_create_view<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let ql = q.to_lowercase();
        let is_materialized = ql.contains("materialized view");
        let tag = if is_materialized {
            "CREATE MATERIALIZED VIEW"
        } else {
            "CREATE VIEW"
        };

        // Extract view name and query SQL for cycle detection.
        if let Some(view_name) = parse_create_view_name(q) {
            let select_sql = parse_create_view_query(q).unwrap_or_default();
            let deps = extract_sql_refs(&select_sql);

            // Cycle detection: returns RS-1011 if a cycle would be introduced.
            if let Some((cycle_view, cycle_path)) =
                self.catalog.detect_cycle_with_new_view(&view_name, &deps)
            {
                let msg = format!(
                    "[RS-1011] Cycle detected in view dependencies: view '{}' forms a cycle via path: {:?}",
                    cycle_view, cycle_path
                );
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P17".to_owned(),
                    msg,
                )))]);
            }

            // Register view in the catalog.
            use crate::catalog_stubs::CatalogView;

            // Pre-populate column names by static analysis of the SELECT list.
            // This allows `SELECT * FROM view` to return correct column headers
            // even before the first DML commit triggers a full materialization.
            // Types default to Utf8 and are refined to the true Arrow types once
            // the view is first materialized via `update_view_columns`.
            let initial_columns: Vec<crate::catalog_stubs::CatalogColumn> =
                infer_select_columns(&select_sql)
                    .into_iter()
                    .map(|name| crate::catalog_stubs::CatalogColumn {
                        name,
                        data_type: "Utf8".to_string(),
                    })
                    .collect();

            self.catalog.add_view_with_deps(
                CatalogView {
                    name: view_name.clone(),
                    sql: select_sql,
                    columns: initial_columns,
                    namespace: "public".to_string(),
                },
                deps,
            );
            if let Some(log) = &self.audit_log {
                let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                    "system",
                    "create_view",
                    &view_name,
                ));
            }
        }

        Ok(vec![Response::Execution(Tag::new(tag).with_rows(0))])
    }

    fn handle_create_table<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let ql = q.to_lowercase();
        let if_not_exists = ql.contains("if not exists");

        // Parse: "CREATE TABLE [IF NOT EXISTS] <name> (col type, ...)"
        let after = if if_not_exists {
            let pos = ql.find("if not exists").unwrap() + "if not exists".len();
            q[pos..].trim()
        } else {
            let pos = ql.find("create table").unwrap() + "create table".len();
            q[pos..].trim()
        };

        // Extract table name (up to first whitespace or '(')
        let name_end = after
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after.len());
        let table_name = after[..name_end].trim().to_lowercase();
        if table_name.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "syntax error: missing table name in CREATE TABLE".to_owned(),
            )))]);
        }

        // Check for duplicate (non-IF NOT EXISTS)
        if self.catalog.get_table(&table_name).is_some() {
            if if_not_exists {
                return Ok(vec![Response::Execution(
                    Tag::new("CREATE TABLE").with_rows(0),
                )]);
            }
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P07".to_owned(),
                format!("relation \"{table_name}\" already exists"),
            )))]);
        }

        // Parse column list: content between outermost parentheses.
        let cols = parse_create_table_columns(after);

        self.catalog.add_table(CatalogTable {
            name: table_name.clone(),
            columns: cols,
        });

        Ok(vec![Response::Execution(
            Tag::new("CREATE TABLE").with_rows(0),
        )])
    }

    /// Handle `SET rockstream.<var> = <value>` — update per-connection session state.
    fn handle_set_rockstream(
        &self,
        q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let Some(id) = conn_id else {
            return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
        };

        // Parse: SET [LOCAL] rockstream.<var> = <value>
        // ql is already lowercased
        let after_set = if ql.starts_with("set local rockstream.") {
            &ql["set local rockstream.".len()..]
        } else {
            &ql["set rockstream.".len()..]
        };
        // after_set: "idempotency_key = 'str'" or "source_epoch = 42"
        let eq_pos = after_set.find('=').unwrap_or(after_set.len());
        let var_name = after_set[..eq_pos].trim();
        let val_raw = after_set[eq_pos + 1..].trim().trim_end_matches(';');

        let mut session = self
            .sessions
            .entry(id.to_string())
            .or_insert_with(SessionState::new);

        match var_name {
            "idempotency_key" => {
                if val_raw == "default" || val_raw == "null" || val_raw == "''" {
                    session.idempotency_key = None;
                } else {
                    // Strip surrounding single quotes
                    let key_str = val_raw.trim_matches('\'');
                    let hash = sha256_16(key_str.as_bytes());
                    session.idempotency_key = Some(hash);
                }
            }
            "source_epoch" => {
                if let Ok(n) = val_raw.trim_matches('\'').parse::<u64>() {
                    session.source_epoch_envelope = Some(n);
                }
            }
            "wait_for" => {
                // Accepts JSON: {"table_name":"t","source_epoch":42}
                let json_str = val_raw.trim_matches('\'');
                if let Ok(token) = serde_json::from_str::<FreshnessToken>(json_str) {
                    session.wait_for_token = Some(token);
                }
            }
            "session_wait_for" => {
                session.session_wait_for_enabled = val_raw.trim_matches('\'') != "off";
            }
            "session_wait_for_timeout_ms" => {
                if let Ok(n) = val_raw.trim_matches('\'').parse::<u64>() {
                    session.session_wait_for_timeout_ms = n;
                }
            }
            _ => {}
        }
        drop(session);
        Ok(vec![promote_response(Response::Execution(Tag::new("SET")))])
    }

    /// COMMIT handler: flush write buffer to ShardDb atomically.
    async fn handle_commit(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        let Some(conn_id) = conn_id else {
            return Ok(vec![promote_response(Response::TransactionEnd(
                Tag::new("COMMIT").with_rows(0),
            ))]);
        };

        let mut entry = self.write_buffers.entry(conn_id.to_string()).or_default();
        if entry.is_empty() {
            return Ok(vec![promote_response(Response::TransactionEnd(
                Tag::new("COMMIT").with_rows(0),
            ))]);
        }

        let Some(shard_db) = &self.shard_db else {
            // No shard — discard buffer, return COMMIT (best effort without storage)
            entry.clear();
            return Ok(vec![promote_response(Response::TransactionEnd(
                Tag::new("COMMIT").with_rows(0),
            ))]);
        };

        // ── Idempotency check ─────────────────────────────────────────────────
        let (idempotency_key, source_epoch_envelope) = {
            let session = self
                .sessions
                .entry(conn_id.to_string())
                .or_insert_with(SessionState::new);
            (session.idempotency_key, session.source_epoch_envelope)
        };
        if idempotency_key.is_none() && source_epoch_envelope.is_none() {
            entry.clear();
            return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "23000".to_owned(),
                "[RS-2007] write.idempotency_key_required: Every write must carry either SET rockstream.idempotency_key or SET rockstream.source_epoch. Next steps: run SET rockstream.idempotency_key = '<unique-key>' before COMMIT.".to_owned(),
            ))))]);
        }
        if let Some(key_hash) = idempotency_key {
            // Check for prior commit with this key — idempotent replay → noop
            match shard_db.get_idempotency_epoch(0, key_hash).await {
                Ok(Some(_prev_epoch)) => {
                    // Already committed — discard buffer and return COMMIT noop
                    entry.clear();
                    return Ok(vec![promote_response(Response::TransactionEnd(
                        Tag::new("COMMIT").with_rows(0),
                    ))]);
                }
                Ok(None) => {} // proceed
                Err(e) => {
                    return Err(PgWireError::ApiError(Box::new(
                        crate::error::GatewayError::Storage(e),
                    )));
                }
            }
        }

        let ops = entry.drain();
        let affected = ops.len();
        drop(entry); // release DashMap entry guard before await

        // Allocate next epoch
        let epoch = shard_db.last_epoch().fetch_add(1, Ordering::SeqCst) + 1;

        // Build WriteBatch from DmlOps — only Put and Delete, no range-delete.
        let mut batch = rockstream_storage::WriteBatch::new();
        for op in &ops {
            match op {
                DmlOp::Insert {
                    table,
                    row_key,
                    values_tsv,
                    ..
                } => {
                    let key = format!("view_output/{table}/{row_key}");
                    batch.put(key.as_bytes(), values_tsv.as_bytes());
                }
                DmlOp::Update {
                    table,
                    old_row_key,
                    new_row_key,
                    new_tsv,
                    ..
                } => {
                    let old_key = format!("view_output/{table}/{old_row_key}");
                    let new_key = format!("view_output/{table}/{new_row_key}");
                    batch.delete(old_key.as_bytes());
                    batch.put(new_key.as_bytes(), new_tsv.as_bytes());
                }
                DmlOp::Delete { table, row_key } => {
                    let key = format!("view_output/{table}/{row_key}");
                    batch.delete(key.as_bytes());
                }
            }
        }
        // Persist idempotency key so replays are no-ops
        if let Some(key_hash) = idempotency_key {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            rockstream_storage::ShardDb::put_idempotency_key(
                &mut batch, 0, key_hash, epoch, now_ms,
            );
        }
        // Advance shard frontier
        batch.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );

        shard_db
            .write_batch(batch)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e))))?;

        // ── Last hop: materialise dependent views ─────────────────────────────
        // Collect the unique tables touched by this commit, then re-evaluate
        // every view that transitively depends on them.  This converts the
        // serving shard from a raw DML store into a live view serving layer.
        {
            let changed_tables: std::collections::HashSet<String> = ops
                .iter()
                .map(|op| match op {
                    DmlOp::Insert { table, .. } => table.clone(),
                    DmlOp::Update { table, .. } => table.clone(),
                    DmlOp::Delete { table, .. } => table.clone(),
                })
                .collect();
            crate::view_materializer::materialize_views(
                &self.catalog,
                shard_db,
                &changed_tables,
            )
            .await;
            // Flush the WAL so that materialised view output is immediately
            // visible to the ShardReader (DbReader reads from SSTs, not the
            // in-memory WAL buffer; without a flush the SELECT after COMMIT
            // would see stale state until the background 100 ms flush fires).
            if let Err(e) = shard_db.flush().await {
                tracing::warn!("post-commit shard flush failed (non-fatal): {e}");
            }
        }

        let table_name = ops
            .iter()
            .last()
            .map(|op| match op {
                DmlOp::Insert { table, .. } => table.clone(),
                DmlOp::Update { table, .. } => table.clone(),
                DmlOp::Delete { table, .. } => table.clone(),
            })
            .unwrap_or_default();
        if let Some(log) = &self.audit_log {
            let actor = if let Some(s) = self.sessions.get(conn_id) {
                s.principal.actor()
            } else {
                "system".to_string()
            };
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                actor,
                "commit",
                &table_name,
            ));
        }

        // Update session's last_written_epoch with a FreshnessToken (S7).
        if let Some(mut session) = self.sessions.get_mut(conn_id) {
            session.last_written_epoch = Some(FreshnessToken::new(table_name, epoch));
        }

        Ok(vec![promote_response(Response::TransactionEnd(
            Tag::new("COMMIT").with_rows(affected),
        ))])
    }

    /// ROLLBACK handler: discard write buffer without any shard writes.
    async fn handle_rollback(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(conn_id) = conn_id {
            if let Some(mut entry) = self.write_buffers.get_mut(conn_id) {
                entry.clear();
            }
        }
        Ok(vec![promote_response(Response::TransactionEnd(
            Tag::new("ROLLBACK").with_rows(0),
        ))])
    }

    /// INSERT handler: accumulate rows in the write buffer.
    async fn handle_insert(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        // Parse INSERT INTO <table> [(cols)] VALUES (v1, v2, ...) [RETURNING ...]
        let returning = q.to_lowercase().contains(" returning ");
        let (table, cols, values) = match parse_insert(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        // Build row_key: deterministic from col=val pairs
        let row_key = build_row_key(&cols, &values);
        let values_tsv = values.join("\t");

        let op = DmlOp::Insert {
            table: table.clone(),
            cols: cols.clone(),
            values_tsv: values_tsv.clone(),
            row_key: row_key.clone(),
        };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                )))]);
            }
        }

        if returning {
            // Auto-commit single INSERT … RETURNING outside explicit transaction
            let schema_fields = if let Some(ct) = self.catalog.get_table(&table) {
                ct.columns
                    .iter()
                    .map(|c| {
                        let oid = arrow_type_to_pg_oid(&c.data_type);
                        FieldInfo::new(
                            c.name.clone(),
                            None,
                            None,
                            pg_type_from_oid(oid),
                            FieldFormat::Text,
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                cols.iter()
                    .map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text))
                    .collect()
            };
            let schema = Arc::new(schema_fields);
            let schema_ref = schema.clone();
            let row_values: Vec<Option<String>> = values.iter().map(|v| Some(v.clone())).collect();
            let stream = Box::pin(stream::once(async move {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                for v in &row_values {
                    encoder
                        .encode_field(v)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                encoder.finish()
            }));
            return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                schema, stream,
            )))]);
        }

        Ok(vec![promote_response(Response::Execution(
            Tag::new("INSERT 0 1").with_rows(1),
        ))])
    }

    /// UPDATE handler: accumulate in write buffer.
    async fn handle_update(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let (table, set_pairs, where_pairs) = match parse_update(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        // Build old row key from WHERE clause, new values from SET clause
        let (old_cols, old_vals): (Vec<_>, Vec<_>) = where_pairs
            .iter()
            .map(|(c, v)| (c.clone(), v.clone()))
            .unzip();
        let old_row_key = build_row_key(&old_cols, &old_vals);
        let old_tsv = old_vals.join("\t");

        let (new_cols, new_vals): (Vec<_>, Vec<_>) = set_pairs
            .iter()
            .map(|(c, v)| (c.clone(), v.clone()))
            .unzip();
        let new_row_key = build_row_key(&new_cols, &new_vals);
        let new_tsv = new_vals.join("\t");

        let op = DmlOp::Update {
            table,
            old_row_key,
            old_tsv,
            new_row_key,
            new_tsv,
        };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                )))]);
            }
        }

        Ok(vec![promote_response(Response::Execution(
            Tag::new("UPDATE 1").with_rows(1),
        ))])
    }

    /// DELETE handler: accumulate in write buffer.
    async fn handle_delete(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let (table, where_pairs) = match parse_delete(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        let (cols, vals): (Vec<_>, Vec<_>) = where_pairs
            .iter()
            .map(|(c, v)| (c.clone(), v.clone()))
            .unzip();
        let row_key = build_row_key(&cols, &vals);

        let op = DmlOp::Delete { table, row_key };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                )))]);
            }
        }

        Ok(vec![promote_response(Response::Execution(
            Tag::new("DELETE 1").with_rows(1),
        ))])
    }

    // ── COPY IN helpers ──────────────────────────────────────────────────────

    /// Detect `COPY <table> FROM STDIN`, register CopyState, return CopyInResponse.
    ///
    /// Returns RS-2500 if the table is not in the catalog (S6).
    /// Enforces PipelineOwner role (RS-2400/RS-2401) when auth is enabled (S7).
    fn handle_copy_from_stdin(
        &self,
        query: &str,
        conn_id: &str,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let (table, requested_cols) = match crate::copy_state::parse_copy_from_stmt(query) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        // S6: Table must exist in catalog (RS-2500).
        let catalog_table = self.catalog.get_table(&table);
        if catalog_table.is_none() {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    format!(
                        "[RS-2500] copy.table_not_found: table '{}' does not exist. \
                         next_steps: Create the table with CREATE TABLE before issuing COPY.",
                        table
                    ),
                ),
            )))]);
        }

        // S7: Auth enforcement — PipelineOwner role required.
        let principal = if let Some(session) = self.sessions.get(conn_id) {
            session.principal.clone()
        } else {
            Principal::System
        };

        use rockstream_types::acl::Role;
        if !principal.is_system() {
            let session_namespace = self
                .sessions
                .get(conn_id)
                .map(|s| s.current_namespace.clone())
                .unwrap_or_else(|| "public".to_string());
            if let Err(e) = self.acl_store.check(
                principal.identity(),
                &session_namespace,
                Some(&table),
                Role::PipelineOwner,
            ) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42501".to_owned(), e.to_string()),
                )))]);
            }
        }

        // Resolve columns: use declared list, or infer from catalog.
        let columns = if !requested_cols.is_empty() {
            requested_cols
        } else {
            catalog_table
                .unwrap()
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect()
        };

        let col_count = columns.len();

        // Audit: log control-plane action.
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "copy_in_start",
                &table,
            ));
        }

        self.copy_states
            .insert(conn_id.to_string(), CopyState::new(table, columns));

        let col_fmt_count = col_count.max(1);
        Ok(vec![promote_response(Response::CopyIn(CopyResponse::new(
            0,
            col_fmt_count,
            vec![0i16; col_fmt_count],
        )))])
    }

    /// Public wrapper for `handle_copy_from_stdin` — for integration tests that
    /// need to exercise auth/error paths without going through the full pgwire stack.
    #[doc(hidden)]
    pub fn copy_from_stdin_response(
        &self,
        query: &str,
        conn_id: &str,
    ) -> PgWireResult<Vec<Response<'static>>> {
        self.handle_copy_from_stdin(query, conn_id)
    }

    /// Flush `rows` to the shard as a single `WriteBatch`.
    ///
    /// Returns the number of rows written.  If no shard is configured the rows
    /// are silently discarded (test / no-storage mode).
    async fn flush_copy_batch_rows(&self, table: &str, rows: &[DmlOp]) -> PgWireResult<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let Some(shard_db) = &self.shard_db else {
            return Ok(rows.len()); // no storage — pretend success
        };

        let epoch = shard_db.last_epoch().fetch_add(1, Ordering::SeqCst) + 1;
        let mut batch = rockstream_storage::WriteBatch::new();
        for op in rows {
            if let DmlOp::Insert {
                table: op_table,
                row_key,
                values_tsv,
                ..
            } = op
            {
                let key = format!("view_output/{op_table}/{row_key}");
                batch.put(key.as_bytes(), values_tsv.as_bytes());
            }
        }
        // Advance shard frontier.
        batch.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );

        shard_db
            .write_batch(batch)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e))))?;

        // Audit: log the flush.
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "copy_in_flush",
                table,
            ));
        }

        Ok(rows.len())
    }
}

#[async_trait]
impl StartupHandler for GatewayHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> pgwire::error::PgWireResult<()>
    where
        C: pgwire::api::ClientInfo
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send,
        C::Error: std::fmt::Debug,
        pgwire::error::PgWireError:
            From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        if let pgwire::messages::PgWireFrontendMessage::Startup(ref startup) = message {
            save_startup_parameters_to_metadata(client, startup);

            match &self.auth_mode {
                AuthMode::Off => {
                    client
                        .metadata_mut()
                        .insert("_rs_principal".to_string(), "system".to_string());
                }
                AuthMode::Oidc => {
                    let auth_param = startup
                        .parameters
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == "authorization")
                        .map(|(_, v)| v.clone());

                    match auth_param {
                        None => {
                            return Err(pgwire::error::PgWireError::UserError(Box::new(
                                pgwire::error::ErrorInfo::new(
                                    "FATAL".to_string(),
                                    "28000".to_string(),
                                    "[RS-2400] auth.unauthenticated: Request missing credentials; provide Authorization: Bearer <token> in startup parameters. next_steps: Provide valid credentials (Bearer token or mTLS certificate)".to_string(),
                                ),
                            )));
                        }
                        Some(auth_val) => {
                            let token = auth_val.strip_prefix("Bearer ").unwrap_or("").trim();
                            if token.is_empty() {
                                return Err(pgwire::error::PgWireError::UserError(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "FATAL".to_string(),
                                        "28000".to_string(),
                                        "[RS-2400] auth.unauthenticated: Bearer token missing or empty. next_steps: Provide valid credentials (Bearer token or mTLS certificate)".to_string(),
                                    ),
                                )));
                            }
                            if let Some(verifier) = &self.jwt_verifier {
                                match verifier.verify(token) {
                                    Ok(claims) => {
                                        client.metadata_mut().insert(
                                            "_rs_principal".to_string(),
                                            format!("jwt:{}", claims.sub),
                                        );
                                    }
                                    Err(e) => {
                                        return Err(pgwire::error::PgWireError::UserError(
                                            Box::new(pgwire::error::ErrorInfo::new(
                                                "FATAL".to_string(),
                                                "28000".to_string(),
                                                format!("{e}. next_steps: Provide valid credentials (Bearer token or mTLS certificate)"),
                                            )),
                                        ));
                                    }
                                }
                            } else {
                                client
                                    .metadata_mut()
                                    .insert("_rs_principal".to_string(), format!("jwt:{token}"));
                            }
                        }
                    }
                }
                AuthMode::Mtls => {
                    let cn = startup
                        .parameters
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == "cn")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| "unknown".to_string());
                    client
                        .metadata_mut()
                        .insert("_rs_principal".to_string(), format!("cert:{cn}"));
                }
            }

            finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for GatewayHandler {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Extract or generate a stable per-connection ID stored in client metadata.
        let conn_id = {
            let existing = client.metadata().get("_rs_conn_id").cloned();
            if let Some(id) = existing {
                id
            } else {
                use rand::Rng;
                let id = format!("{:032x}", rand::thread_rng().gen::<u128>());
                client
                    .metadata_mut()
                    .insert("_rs_conn_id".to_string(), id.clone());
                id
            }
        };

        // Sync principal from startup metadata into the session (once per connection).
        if let Some(raw_principal) = client.metadata().get("_rs_principal").cloned() {
            let mut session = self
                .sessions
                .entry(conn_id.clone())
                .or_insert_with(SessionState::new);
            if session.principal == Principal::System && raw_principal != "system" {
                session.principal = if let Some(sub) = raw_principal.strip_prefix("jwt:") {
                    Principal::Jwt {
                        sub: sub.to_string(),
                    }
                } else {
                    Principal::System
                };
            }
        }

        // COPY IN: enter COPY IN mode, store CopyState, return CopyInResponse.
        let ql = query.trim().to_lowercase();
        if ql.starts_with("copy ") && ql.contains(" from stdin") {
            return self.handle_copy_from_stdin(query, &conn_id);
        }

        // COPY OUT: stream CopyData messages directly through the client sink.
        if ql.starts_with("copy ") && ql.contains(" to stdout") {
            if let Some(view_name) = parse_copy_to_stdout_view(query) {
                let rows = self
                    .view_reader
                    .read_view(&view_name, None, ViewReadStrategy::HotOnly)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                let row_count = rows.len();

                let col_count = self
                    .catalog
                    .get_view(&view_name)
                    .map(|v| v.columns.len())
                    .unwrap_or(1);

                // 1. CopyOutResponse
                let copy_out_resp =
                    MsgCopyOutResponse::new(0, col_count as i16, vec![0i16; col_count]);
                client
                    .feed(PgWireBackendMessage::CopyOutResponse(copy_out_resp))
                    .await?;

                // 2. CopyData — one message per row (tab-separated text + newline)
                for row in &rows {
                    let mut data = row.clone();
                    data.push(b'\n');
                    let copy_data = MsgCopyData::new(bytes::Bytes::from(data));
                    client
                        .feed(PgWireBackendMessage::CopyData(copy_data))
                        .await?;
                }

                // 3. CopyDone
                client
                    .feed(PgWireBackendMessage::CopyDone(MsgCopyDone::new()))
                    .await?;
                client.flush().await?;

                // 4. Return CommandComplete — pgwire on_query will send it and
                //    then send ReadyForQuery since state is no longer CopyInProgress.
                return Ok(vec![Response::Execution(
                    Tag::new(&format!("COPY {row_count}")).with_rows(row_count),
                )]);
            }
        }

        self.dispatch_async_with_conn(query, Some(&conn_id)).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for GatewayHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        <C as ClientPortalStore>::PortalStore:
            pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = describe_fields_for_query(&self.catalog, &target.statement);
        Ok(DescribeStatementResponse::new(vec![], fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        <C as ClientPortalStore>::PortalStore:
            pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = describe_fields_for_query(&self.catalog, target.statement.statement.as_str());
        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        <C as ClientPortalStore>::PortalStore:
            pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Ensure a stable connection ID is stored for COPY IN routing.
        let conn_id = {
            let existing = client.metadata().get("_rs_conn_id").cloned();
            if let Some(id) = existing {
                id
            } else {
                use rand::Rng;
                let id = format!("{:032x}", rand::thread_rng().gen::<u128>());
                client
                    .metadata_mut()
                    .insert("_rs_conn_id".to_string(), id.clone());
                id
            }
        };

        // Sync principal from startup metadata into the session (once per connection).
        if let Some(raw_principal) = client.metadata().get("_rs_principal").cloned() {
            let mut session = self
                .sessions
                .entry(conn_id.clone())
                .or_insert_with(SessionState::new);
            if session.principal == Principal::System && raw_principal != "system" {
                session.principal = if let Some(sub) = raw_principal.strip_prefix("jwt:") {
                    Principal::Jwt {
                        sub: sub.to_string(),
                    }
                } else {
                    Principal::System
                };
            }
        }

        let query = portal.statement.statement.as_str();
        let ql = query.trim().to_lowercase();

        // COPY IN via extended query protocol (e.g. tokio_postgres.copy_in()).
        if ql.starts_with("copy ") && ql.contains(" from stdin") {
            let responses = self.handle_copy_from_stdin(query, &conn_id)?;
            return Ok(responses
                .into_iter()
                .next()
                .unwrap_or(Response::Execution(Tag::new("OK"))));
        }

        let responses = self.dispatch_async_with_conn(query, Some(&conn_id)).await?;
        Ok(responses
            .into_iter()
            .next()
            .unwrap_or(Response::Execution(Tag::new("OK"))))
    }
}

// ── CopyHandler implementation ────────────────────────────────────────────────

#[async_trait]
impl CopyHandler for GatewayHandler {
    /// Receive a `CopyData` message, parse TSV rows, and buffer them.
    ///
    /// Handles partial lines that span `CopyData` message boundaries via
    /// `CopyState::partial_line`.  Validates column count against the declared
    /// schema (RS-2501).  Auto-flush when bounds are exceeded (S5).
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_default();

        // Parse all complete lines from this chunk.
        // We take what we need from the state, then release the guard before
        // any await point.
        let (table, columns, rows_to_add, new_partial, needs_flush) = {
            let mut state = match self.copy_states.get_mut(&conn_id) {
                Some(s) => s,
                None => {
                    return Err(PgWireError::ApiError(Box::new(
                        crate::error::GatewayError::NotSupported(
                            "CopyData received without active COPY IN session".to_string(),
                        ),
                    )));
                }
            };

            let chunk = match std::str::from_utf8(&copy_data.data) {
                Ok(s) => s,
                Err(_) => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P05".to_owned(),
                        "[RS-2501] copy.invalid_encoding: CopyData contains invalid UTF-8"
                            .to_owned(),
                    ))));
                }
            };

            state.partial_line.push_str(chunk);

            // Process all complete lines.
            let mut new_ops: Vec<DmlOp> = Vec::new();
            let mut col_mismatch: Option<(usize, usize)> = None;

            loop {
                let nl = state.partial_line.find('\n');
                if nl.is_none() {
                    break;
                }
                let nl_pos = nl.unwrap();
                let raw_line = state.partial_line[..nl_pos].to_string();
                state.partial_line.drain(..=nl_pos);

                // Strip carriage return for Windows-style line endings.
                let line = raw_line.trim_end_matches('\r');

                // Skip the COPY sentinel `\.`.
                if line == "\\." {
                    continue;
                }
                if line.is_empty() {
                    continue;
                }

                let fields: Vec<&str> = line.split('\t').collect();

                // Column count validation (RS-2501).
                if !state.columns.is_empty() && fields.len() != state.columns.len() {
                    col_mismatch = Some((state.columns.len(), fields.len()));
                    break;
                }

                let cols: Vec<String> = if state.columns.is_empty() {
                    (0..fields.len()).map(|i| format!("col{i}")).collect()
                } else {
                    state.columns.clone()
                };

                let row_key = cols
                    .iter()
                    .zip(fields.iter())
                    .map(|(c, v)| format!("{c}={v}"))
                    .collect::<Vec<_>>()
                    .join("|");
                let values_tsv = fields.join("\t");
                let op_bytes = values_tsv.len() + row_key.len() + state.table.len() + 64;

                let op = DmlOp::Insert {
                    table: state.table.clone(),
                    cols,
                    values_tsv,
                    row_key,
                };
                state.buf_bytes += op_bytes;
                new_ops.push(op);
                COPY_IN_BUFFER_ROWS.fetch_add(1, Ordering::Relaxed);
            }

            if let Some((expected, got)) = col_mismatch {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P04".to_owned(),
                    format!(
                        "[RS-2501] copy.column_count_mismatch: expected {expected} fields \
                         but got {got}. next_steps: Check that the TSV row matches the \
                         column count declared in COPY or the catalog."
                    ),
                ))));
            }

            for op in &new_ops {
                state.buf_rows.push(op.clone());
            }

            let needs_flush = state.buf_rows.len() >= MAX_COPY_IN_BATCH_ROWS
                || state.buf_bytes >= COPY_IN_FLUSH_BYTES;

            let partial = state.partial_line.clone();
            let table = state.table.clone();
            let columns = state.columns.clone();
            (table, columns, new_ops, partial, needs_flush)
            // RefMut drops here — safe before await
        };

        // Auto-flush when a bound is exceeded (S5).
        if needs_flush {
            let (rows_to_flush, _prev_total) = {
                let mut state = match self.copy_states.get_mut(&conn_id) {
                    Some(s) => s,
                    None => return Ok(()),
                };
                let rows = std::mem::take(&mut state.buf_rows);
                let prev = state.total_rows_flushed;
                state.buf_bytes = 0;
                let n = rows.len() as u64;
                COPY_IN_BUFFER_ROWS.fetch_sub(
                    n.min(COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed)),
                    Ordering::Relaxed,
                );
                (rows, prev)
            };
            let flushed = self.flush_copy_batch_rows(&table, &rows_to_flush).await?;
            if let Some(mut state) = self.copy_states.get_mut(&conn_id) {
                state.total_rows_flushed += flushed;
            }
        }

        let _ = (columns, rows_to_add, new_partial); // suppress unused warnings
        Ok(())
    }

    /// `CopyDone` — flush remaining buffer and send `CommandComplete`.
    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_default();

        // Extract remaining rows + total before any await.
        let (table, rows, prev_total) = match self.copy_states.get_mut(&conn_id) {
            Some(mut state) => {
                let rows = std::mem::take(&mut state.buf_rows);
                let n = rows.len() as u64;
                COPY_IN_BUFFER_ROWS.fetch_sub(
                    n.min(COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed)),
                    Ordering::Relaxed,
                );
                state.buf_bytes = 0;
                let total = state.total_rows_flushed;
                let table = state.table.clone();
                (table, rows, total)
            }
            None => return Ok(()), // no state — nothing to do
        };

        let flushed = self.flush_copy_batch_rows(&table, &rows).await?;
        let total = prev_total + flushed;

        // Remove COPY state.
        self.copy_states.remove(&conn_id);

        // Emit audit event.
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "copy_in_done",
                &table,
            ));
        }

        // Send CommandComplete: `COPY N`
        client
            .feed(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                format!("COPY {total}"),
            )))
            .await?;
        client.flush().await?;

        Ok(())
    }

    /// `CopyFail` — clean up state and surface the failure.
    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_default();

        if let Some(mut state) = self.copy_states.get_mut(&conn_id) {
            let n = state.buf_rows.len() as u64;
            COPY_IN_BUFFER_ROWS.fetch_sub(
                n.min(COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
            state.buf_rows.clear();
            state.buf_bytes = 0;
        }
        self.copy_states.remove(&conn_id);

        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "57014".to_owned(),
            format!("COPY FROM STDIN aborted by client: {}", fail.message),
        )))
    }
}

// ── Handler factory ───────────────────────────────────────────────────────────

struct GatewayHandlerFactory {
    handler: Arc<GatewayHandler>,
}

impl PgWireServerHandlers for GatewayHandlerFactory {
    type StartupHandler = GatewayHandler;
    type SimpleQueryHandler = GatewayHandler;
    type ExtendedQueryHandler = GatewayHandler;
    type CopyHandler = GatewayHandler;
    type ErrorHandler = NoopErrorHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }
    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }
    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }
    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.handler.clone()
    }
    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

// ── GatewayServer ─────────────────────────────────────────────────────────────

/// A running PostgreSQL-wire-protocol server.
pub struct GatewayServer {
    addr: std::net::SocketAddr,
    handler: Arc<GatewayHandler>,
}

impl GatewayServer {
    /// Create a new gateway server listening on `addr`.
    pub fn new(addr: std::net::SocketAddr, view_reader: Arc<dyn ViewReader>) -> Self {
        let catalog = Arc::new(CatalogStubs::new());
        GatewayServer {
            addr,
            handler: Arc::new(GatewayHandler::new(catalog, view_reader)),
        }
    }

    /// Create a new gateway server with an explicit catalog (for testing).
    pub fn with_catalog(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
    ) -> Self {
        GatewayServer {
            addr,
            handler: Arc::new(GatewayHandler::new(catalog, view_reader)),
        }
    }

    /// Create a gateway server with a catalog and ShardDb for direct-write DML.
    pub fn with_shard_db(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        GatewayServer {
            addr,
            handler: Arc::new(GatewayHandler::with_shard_db(
                catalog,
                view_reader,
                shard_db,
            )),
        }
    }

    /// Create a gateway with OIDC auth enabled (for auth integration tests).
    pub fn with_shard_db_and_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
        jwt_secret: &[u8],
    ) -> Self {
        let mut handler = GatewayHandler::with_shard_db(catalog, view_reader, shard_db);
        handler.auth_mode = AuthMode::Oidc;
        handler.jwt_verifier = Some(Arc::new(JwtVerifier::with_hs256_key(jwt_secret.to_vec())));
        GatewayServer {
            addr,
            handler: Arc::new(handler),
        }
    }

    /// Return a reference to the handler (for seeding ACL and sessions in tests).
    pub fn handler(&self) -> &Arc<GatewayHandler> {
        &self.handler
    }

    /// Return a reference to the handler's catalog stubs (for seeding in tests).
    pub fn catalog(&self) -> &Arc<CatalogStubs> {
        &self.handler.catalog
    }

    /// Start listening.  Blocks until the future is dropped.
    pub async fn serve(self) -> std::io::Result<()> {
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler,
        });
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Gateway listening on {}", self.addr);
        loop {
            let (socket, _peer) = listener.accept().await?;
            let factory_ref = factory.clone();
            tokio::spawn(async move {
                if let Err(e) = pgwire::tokio::process_socket(socket, None, factory_ref).await {
                    tracing::debug!("gateway connection error: {e}");
                }
            });
        }
    }

    /// Bind to `addr`, return the actual local address (useful for port 0 tests),
    /// and serve connections in a background task.
    pub async fn serve_background(
        self,
    ) -> std::io::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler,
        });
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _peer)) = listener.accept().await else {
                    break;
                };
                let factory_ref = factory.clone();
                tokio::spawn(async move {
                    if let Err(e) = pgwire::tokio::process_socket(socket, None, factory_ref).await {
                        tracing::debug!("gateway connection error: {e}");
                    }
                });
            }
        });
        Ok((local_addr, handle))
    }
}

// ── Query helpers ─────────────────────────────────────────────────────────────

/// Compute the first 16 bytes of SHA-256 of `data`.
/// Used as a deterministic 128-bit key hash for idempotency tracking.
fn sha256_16(data: &[u8]) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(data);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// Convert a `CatalogResponse` to a pgwire `Response`.
fn catalog_resp_to_response(resp: CatalogResponse) -> Response<'static> {
    match resp {
        CatalogResponse::CommandComplete(tag) => Response::Execution(Tag::new(&tag)),
        CatalogResponse::Rows { columns, rows } => {
            let fields: Vec<FieldInfo> = columns
                .iter()
                .map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text))
                .collect();
            let schema = Arc::new(fields);
            let schema_ref = schema.clone();
            let stream = stream::iter(rows).map(move |row: Vec<Option<String>>| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                for field in &row {
                    encoder
                        .encode_field(field)
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                encoder.finish()
            });
            Response::Query(QueryResponse::new(schema, stream))
        }
    }
}

/// Promote a `Response<'_>` to `Response<'static>` by ensuring all contained
/// data is owned.  All data produced by our handlers is already owned, so this
/// is safe via transmute-free coercion through boxing.
fn promote_response(r: Response<'_>) -> Response<'static> {
    // SAFETY: Our responses contain only owned data (Arc, Vec, etc.).  No
    // borrows from a query string are embedded.  We transmute the lifetime
    // annotation only, not the data itself.
    //
    // In practice, `Response<'a>` is only lifetime-parameterised because
    // `QueryResponse<'a>` holds a `BoxStream<'a, ...>`.  Since our stream owns
    // all its data, it is effectively `'static`.
    //
    // This is the standard pattern in pgwire examples that need to escape the
    // `'a` lifetime from `do_query`.
    unsafe { std::mem::transmute(r) }
}

/// Extract the view name from a simple `SELECT … FROM <name> [LIMIT n]` query.
fn extract_view_name_from_select(q: &str) -> Option<String> {
    let ql = q.to_lowercase();
    let from_pos = ql.find(" from ")?;
    let rest = q[from_pos + 6..].trim();
    // Take until whitespace, semicolon, or end.
    let end = rest
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(rest.len());
    let raw = rest[..end].trim().trim_end_matches(';');
    // Strip schema prefix (e.g. `public.my_view` → `my_view`)
    let name = if let Some(dot) = raw.rfind('.') {
        &raw[dot + 1..]
    } else {
        raw
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Extract LIMIT value from a query string.
fn extract_limit(q: &str) -> Option<usize> {
    let ql = q.to_lowercase();
    let pos = ql.find(" limit ")?;
    let rest = q[pos + 7..].trim();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// Extract the view name from `COPY <view> TO STDOUT`.
fn parse_copy_to_stdout_view(q: &str) -> Option<String> {
    let ql = q.to_lowercase();
    let after_copy_lower = ql.strip_prefix("copy ")?.trim_start().to_string();
    let to_pos = after_copy_lower.find(" to ")?;
    let after_copy_orig = q[q.to_lowercase().find("copy ")? + 5..].trim_start();
    let raw = after_copy_orig[..to_pos].trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

/// Build FieldInfo list for a query (for DESCRIBE).
fn describe_fields_for_query(catalog: &CatalogStubs, q: &str) -> Vec<FieldInfo> {
    if let Some(view_name) = extract_view_name_from_select(q) {
        if let Some(cv) = catalog.get_view(&view_name) {
            return cv
                .columns
                .iter()
                .map(|c| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    FieldInfo::new(
                        c.name.clone(),
                        None,
                        None,
                        pg_type_from_oid(oid),
                        FieldFormat::Text,
                    )
                })
                .collect();
        }
    }
    vec![]
}

/// Extract the view name from `CREATE [MATERIALIZED] VIEW <name> AS …`.
/// Find the byte offset of " as" followed by whitespace or end-of-string
/// within `s` (which must already be lowercased). Returns the index of the
/// space that precedes "as".
///
/// This handles both `AS ` (space) and `AS\n` / `AS\t` / `AS\r` so that
/// multi-line SQL like `CREATE VIEW foo AS\n  SELECT …` parses correctly.
fn find_as_separator(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i + 2 < len {
        if bytes[i] == b' ' && bytes[i + 1] == b'a' && bytes[i + 2] == b's' {
            let next_ok = i + 3 >= len || (bytes[i + 3] as char).is_ascii_whitespace();
            if next_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Extract output column names from a SELECT SQL string by static analysis.
///
/// Handles simple column references (`url`) and aliased expressions
/// (`COUNT(*) AS hits`, `SUM(amount) AS spend`).  Returns an empty vec for
/// `SELECT *` or SQL that cannot be statically decomposed.
fn infer_select_columns(sql: &str) -> Vec<String> {
    let trimmed = sql.trim();
    let lower = trimmed.to_lowercase();
    // Must start with SELECT
    let after_select = if let Some(s) = lower.strip_prefix("select ").or_else(|| {
        lower.strip_prefix("select\n").or_else(|| lower.strip_prefix("select\t"))
    }) {
        &trimmed[trimmed.len() - s.len()..]
    } else {
        return vec![];
    };

    // Find the FROM keyword at nesting depth 0
    let from_pos = {
        let bytes = after_select.as_bytes();
        let lower_after = after_select.to_lowercase();
        let mut depth: usize = 0;
        let mut found = after_select.len();
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' if depth > 0 => depth -= 1,
                _ => {}
            }
            if depth == 0 && lower_after[i..].starts_with(" from ") {
                found = i;
                break;
            }
            i += 1;
        }
        found
    };

    let select_list = &after_select[..from_pos];

    // Split the column list by commas at depth 0
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;
    for ch in select_list.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' if depth > 0 => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let p = current.trim().to_string();
                if !p.is_empty() {
                    parts.push(p);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let last = current.trim().to_string();
    if !last.is_empty() {
        parts.push(last);
    }

    // For each part: extract alias (after AS) or the last identifier token
    parts
        .iter()
        .filter_map(|p| {
            if p == "*" {
                return None; // SELECT * — can't statically name it
            }
            let pl = p.to_lowercase();
            if let Some(as_pos) = find_as_separator(&pl) {
                // Has an AS alias
                let alias = p[as_pos + 3..].trim().to_lowercase();
                if alias.is_empty() {
                    None
                } else {
                    Some(alias)
                }
            } else {
                // No alias: take the last whitespace-delimited token
                let last = p.split_whitespace().last().unwrap_or("").to_lowercase();
                if last.is_empty() {
                    None
                } else {
                    Some(last)
                }
            }
        })
        .collect()
}

fn parse_create_view_name(q: &str) -> Option<String> {
    let ql = q.trim().to_lowercase();
    let after: &str = if ql.starts_with("create or replace materialized view ") {
        q[36..].trim()
    } else if ql.starts_with("create materialized view ") {
        q[25..].trim()
    } else if ql.starts_with("create or replace view ") {
        q[23..].trim()
    } else if ql.starts_with("create view ") {
        q[12..].trim()
    } else {
        return None;
    };
    // Take up to " AS" followed by any whitespace (handles AS\n, AS\t, AS )
    let as_pos = find_as_separator(&after.to_lowercase())?;
    let raw = after[..as_pos].trim().trim_matches('"');
    let name = raw.rsplit('.').next().unwrap_or(raw).trim_matches('"');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Extract the SELECT SQL from `CREATE … AS <select>`.
///
/// Correctly handles `AS` followed by any whitespace, including newlines
/// produced by multi-line SQL input (e.g. `CREATE VIEW foo AS\n  SELECT …`).
fn parse_create_view_query(q: &str) -> Option<String> {
    let ql = q.trim().to_lowercase();
    // Determine the byte offset at which the view name begins.
    let name_start: usize = if ql.starts_with("create or replace materialized view ") {
        36
    } else if ql.starts_with("create materialized view ") {
        25
    } else if ql.starts_with("create or replace view ") {
        23
    } else if ql.starts_with("create view ") {
        12
    } else {
        return None;
    };
    // Find " as" + whitespace in the portion of ql that starts at the view name.
    // This avoids matching an AS alias inside the SELECT body (e.g. COUNT(*) AS hits).
    let after_lower = &ql[name_start..];
    let as_pos_in_after = find_as_separator(after_lower)?;
    // Skip the leading space (1) + "as" (2) = 3 bytes, then trim surrounding whitespace.
    Some(q[name_start + as_pos_in_after + 3..].trim().to_string())
}

/// Extract table/view names referenced in FROM and JOIN clauses.
///
/// Used for dependency tracking in CREATE VIEW cycle detection.
fn extract_sql_refs(sql: &str) -> Vec<String> {
    let tokens_orig: Vec<&str> = sql.split_whitespace().collect();
    let tokens_lower: Vec<String> = tokens_orig.iter().map(|t| t.to_lowercase()).collect();
    let mut deps = Vec::new();
    for i in 0..tokens_lower.len() {
        if tokens_lower[i] == "from" || tokens_lower[i] == "join" {
            if let Some(next) = tokens_orig.get(i + 1) {
                // Skip subquery openers
                if next.starts_with('(') {
                    continue;
                }
                let name = next.trim_matches(|c| c == '"' || c == ',' || c == ';');
                let name = name.rsplit('.').next().unwrap_or(name);
                if !name.is_empty() {
                    deps.push(name.to_string());
                }
            }
        }
    }
    deps.dedup();
    deps
}

// ── DML parsers ───────────────────────────────────────────────────────────────

/// Parse `CREATE TABLE <name> (col type, ...)` column list.
///
/// Extracts the content between the first `(` and the matching `)` and splits
/// on commas to produce `(col_name, arrow_type)` pairs.
fn parse_create_table_columns(after_table_name: &str) -> Vec<CatalogColumn> {
    let start = match after_table_name.find('(') {
        Some(i) => i + 1,
        None => return vec![],
    };
    let end = match after_table_name.rfind(')') {
        Some(i) => i,
        None => return vec![],
    };
    let cols_str = &after_table_name[start..end];
    cols_str
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            let mut tokens = part.split_whitespace();
            let col_name = tokens.next()?.to_lowercase();
            let pg_type = tokens.next()?.to_uppercase();
            // Normalize multi-word types (e.g. "DOUBLE PRECISION")
            let full_type = if pg_type == "DOUBLE" {
                let next = tokens.next().map(|s| s.to_uppercase()).unwrap_or_default();
                if next == "PRECISION" {
                    "DOUBLE PRECISION".to_string()
                } else {
                    pg_type
                }
            } else {
                pg_type
            };
            let arrow_type = pg_type_to_arrow(&full_type);
            Some(CatalogColumn {
                name: col_name,
                data_type: arrow_type.to_string(),
            })
        })
        .collect()
}

/// Map a Postgres type keyword to an Arrow data type name.
fn pg_type_to_arrow(pg_type: &str) -> &'static str {
    match pg_type {
        "BIGINT" | "INT8" => "Int64",
        "INT" | "INT4" | "INTEGER" => "Int32",
        "SMALLINT" | "INT2" => "Int32",
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" => "Utf8",
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT" => "Float64",
        "FLOAT4" | "REAL" => "Float64",
        "BOOL" | "BOOLEAN" => "Boolean",
        "BYTEA" => "Binary",
        "TIMESTAMP" => "Timestamp",
        _ => "Utf8",
    }
}

/// Build a deterministic row key from column names and values.
///
/// Format: `col1=val1|col2=val2|...` — stable ordering is preserved from the
/// input slice. This ensures retries of the same INSERT produce the same key
/// (idempotent within the write buffer).
fn build_row_key(cols: &[String], vals: &[String]) -> String {
    if cols.is_empty() {
        // No explicit column list: use positional indices so each row gets a
        // unique key even when column names are unknown (e.g. INSERT INTO t
        // VALUES (...) without a column list).
        vals.iter()
            .enumerate()
            .map(|(i, v)| format!("col{i}={v}"))
            .collect::<Vec<_>>()
            .join("|")
    } else {
        cols.iter()
            .zip(vals.iter())
            .map(|(c, v)| format!("{c}={v}"))
            .collect::<Vec<_>>()
            .join("|")
    }
}

/// Parse `INSERT INTO <table> [(cols)] VALUES (v1, v2, ...)`.
///
/// Returns `(table_name, col_names, values)`.
fn parse_insert(q: &str) -> Result<(String, Vec<String>, Vec<String>), String> {
    let ql = q.to_lowercase();
    let start = ql.find("insert into ").ok_or("not an INSERT")?;
    let after_lower = ql[start + 12..].trim_start().to_string();
    let orig_start = start + "insert into ".len();
    let orig_after = q[orig_start..].trim_start();

    // Table name: up to first whitespace or '('
    let name_end = after_lower
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(after_lower.len());
    let table = after_lower[..name_end].trim().to_string();
    let orig_name_end = orig_after
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(orig_after.len());

    let rest_lower = after_lower[name_end..].trim_start();
    let rest_orig = orig_after[orig_name_end..].trim_start();

    // Find VALUES keyword position to split off column list
    let values_pos_lower = rest_lower.find("values").ok_or("missing VALUES keyword")?;
    let values_pos_orig = {
        let lo = rest_orig.to_lowercase();
        lo.find("values").ok_or("missing VALUES keyword")?
    };

    // Optional column list is everything before VALUES
    let before_values_lower = rest_lower[..values_pos_lower].trim();
    let (cols, _) = if before_values_lower.starts_with('(') {
        let close = before_values_lower
            .rfind(')')
            .ok_or("missing ) in column list")?;
        let col_str = &before_values_lower[1..close];
        let cols: Vec<String> = col_str.split(',').map(|c| c.trim().to_string()).collect();
        (cols, ())
    } else {
        (vec![], ())
    };

    // Values list: content between outer parentheses after VALUES
    let after_values = rest_orig[values_pos_orig + 6..].trim_start();
    if !after_values.starts_with('(') {
        return Err("missing ( after VALUES".to_string());
    }
    let paren_end = after_values.rfind(')').ok_or("missing ) in VALUES")?;
    let vals_str = &after_values[1..paren_end];
    let values: Vec<String> = parse_value_list(vals_str);

    // Strip trailing semicolon if present from last value
    let values: Vec<String> = values
        .into_iter()
        .map(|v| v.trim_end_matches(';').trim().to_string())
        .collect();

    Ok((table, cols, values))
}

/// Parse `UPDATE <table> SET col = val [, ...] WHERE col = val`.
///
/// Returns `(table, set_pairs, where_pairs)`.
fn parse_update(q: &str) -> Result<(String, Vec<(String, String)>, Vec<(String, String)>), String> {
    let ql = q.to_lowercase();
    let after = ql.strip_prefix("update ").ok_or("not UPDATE")?.trim_start();
    let _orig_after = q[q.to_lowercase().find("update ").unwrap() + 7..].trim_start();

    // Table name
    let name_end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    let table = after[..name_end].trim().to_lowercase();

    // SET clause
    let set_pos = ql.find(" set ").ok_or("missing SET")?;
    let where_pos = ql
        .find(" where ")
        .ok_or("missing WHERE (v0.24 requires WHERE)")?;

    let set_str_lower = &ql[set_pos + 5..where_pos];
    let set_str_orig = &q[set_pos + 5..where_pos];
    let _ = set_str_lower; // used for case-insensitive parsing
    let set_pairs = parse_kv_list(set_str_orig);

    let where_str = &q[where_pos + 7..].trim_end_matches(';');
    let where_pairs = parse_kv_list(where_str);

    Ok((table, set_pairs, where_pairs))
}

/// Parse `DELETE FROM <table> WHERE col = val`.
///
/// Returns `(table, where_pairs)`.
fn parse_delete(q: &str) -> Result<(String, Vec<(String, String)>), String> {
    let ql = q.to_lowercase();
    let after = ql
        .strip_prefix("delete from ")
        .ok_or("not DELETE FROM")?
        .trim_start();

    let name_end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    let table = after[..name_end].trim().to_lowercase();

    let where_pos = ql
        .find(" where ")
        .ok_or("missing WHERE (v0.24 requires WHERE)")?;
    let where_str = &q[where_pos + 7..].trim_end_matches(';');
    let where_pairs = parse_kv_list(where_str);

    Ok((table, where_pairs))
}

/// Parse a comma-separated list of `col = val` pairs.
///
/// Handles simple single-quoted string values and unquoted numerics.
fn parse_kv_list(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .filter_map(|part| {
            let eq = part.find('=')?;
            let col = part[..eq].trim().to_lowercase();
            let val = part[eq + 1..].trim().trim_matches('\'').to_string();
            if col.is_empty() {
                None
            } else {
                Some((col, val))
            }
        })
        .collect()
}

/// Parse a comma-separated VALUES list, respecting single-quoted strings.
///
/// Handles: `'text value', 42, NULL, 'it''s'`
fn parse_value_list(s: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\'' if !in_quote => {
                in_quote = true;
                i += 1;
            }
            '\'' if in_quote => {
                // Escaped single quote?
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    current.push('\'');
                    i += 2;
                } else {
                    in_quote = false;
                    i += 1;
                }
            }
            ',' if !in_quote => {
                values.push(current.trim().to_string());
                current = String::new();
                i += 1;
            }
            c => {
                current.push(c);
                i += 1;
            }
        }
    }
    let last = current.trim().to_string();
    if !last.is_empty() {
        values.push(last);
    }
    values
}

#[cfg(test)]
mod s4_tests {
    use super::*;
    use crate::auth::Principal;
    use crate::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
    use crate::view_reader::{ViewReadStrategy, ViewReader};
    use rockstream_types::acl::{AclEntry, Role};
    use std::sync::Arc;

    struct NoopViewReader;

    #[async_trait]
    impl ViewReader for NoopViewReader {
        async fn read_view(
            &self,
            _view_name: &str,
            _limit: Option<usize>,
            _strategy: ViewReadStrategy,
        ) -> Result<Vec<Vec<u8>>, crate::error::GatewayError> {
            Ok(vec![])
        }

        fn published_frontier(&self) -> Option<u64> {
            None
        }
    }

    fn make_handler() -> Arc<GatewayHandler> {
        let catalog = Arc::new(CatalogStubs::new());
        let reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
        Arc::new(GatewayHandler::new(catalog, reader))
    }

    /// S4 green gate: namespace_isolation_blocks_cross_access
    /// A non-admin principal in ns-a cannot access a view in ns-b.
    #[tokio::test]
    async fn namespace_isolation_blocks_cross_access() {
        let handler = make_handler();

        // Register view in ns-b
        handler.catalog.add_view_in_namespace(CatalogView {
            name: "ns_b_view".to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![CatalogColumn {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
            }],
            namespace: "ns-b".to_string(),
        });

        // Grant alice Viewer on ns-a only
        handler.acl_store.grant(AclEntry {
            principal: "alice".to_string(),
            namespace: "ns-a".to_string(),
            view_name: None,
            role: Role::Viewer,
        });

        // Create a session for alice in ns-a
        let conn_id = "test-conn-1";
        {
            let mut session = handler
                .sessions
                .entry(conn_id.to_string())
                .or_insert_with(SessionState::new);
            session.current_namespace = "ns-a".to_string();
            session.principal = Principal::Jwt {
                sub: "alice".to_string(),
            };
        }

        // Try to read ns_b_view from ns-a session — should get RS-2402 error
        let responses = handler
            .read_view_response("ns_b_view", None, Some(conn_id))
            .await
            .unwrap();
        let got_error = responses.iter().any(|r| {
            if let Response::Error(e) = r {
                e.message.contains("RS-2402")
            } else {
                false
            }
        });
        assert!(got_error, "expected RS-2402 namespace isolation error");
    }

    /// S4 green gate: admin_can_access_cross_namespace
    /// A principal with Admin role can access views in other namespaces.
    #[tokio::test]
    async fn admin_can_access_cross_namespace() {
        let handler = make_handler();

        // Register view in ns-b
        handler.catalog.add_view_in_namespace(CatalogView {
            name: "ns_b_admin_view".to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![CatalogColumn {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
            }],
            namespace: "ns-b".to_string(),
        });

        // Grant carol Admin on ns-a (allows cross-namespace)
        handler.acl_store.grant(AclEntry {
            principal: "carol".to_string(),
            namespace: "ns-a".to_string(),
            view_name: None,
            role: Role::Admin,
        });

        // Create a session for carol in ns-a
        let conn_id = "test-conn-2";
        {
            let mut session = handler
                .sessions
                .entry(conn_id.to_string())
                .or_insert_with(SessionState::new);
            session.current_namespace = "ns-a".to_string();
            session.principal = Principal::Jwt {
                sub: "carol".to_string(),
            };
        }

        // Try to read ns_b_admin_view from ns-a session — admin should succeed
        let responses = handler
            .read_view_response("ns_b_admin_view", None, Some(conn_id))
            .await
            .unwrap();
        let got_error = responses.iter().any(|r| matches!(r, Response::Error(_)));
        assert!(
            !got_error,
            "admin should be able to access cross-namespace view"
        );
    }

    // ── S5: audit_carries_principal_actor ────────────────────────────────────

    /// S5 green gate: COMMIT with a Jwt principal produces an audit event with actor = "jwt:alice".
    #[tokio::test]
    async fn audit_carries_principal_actor() {
        use rockstream_control::audit::FileAuditLog;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::new().unwrap();
        let log = Arc::new(FileAuditLog::open(tmp.path()).unwrap());

        let store = Arc::new(object_store::memory::InMemory::new());
        let shard_db = Arc::new(
            rockstream_storage::ShardDb::builder("audit-shard", store)
                .build()
                .await
                .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::default());
        let reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
        let handler = Arc::new(
            GatewayHandler::with_shard_db(catalog, reader, shard_db).with_audit_log(log.clone()),
        );

        let conn_id = "audit-conn";
        {
            let mut s = handler
                .sessions
                .entry(conn_id.to_string())
                .or_insert_with(crate::session::SessionState::new);
            s.principal = crate::auth::Principal::Jwt {
                sub: "alice".to_string(),
            };
            s.idempotency_key = Some([1u8; 16]);
        }

        handler
            .dispatch_async_with_conn("INSERT INTO t (id, val) VALUES (1, 'x')", Some(conn_id))
            .await
            .unwrap();
        handler
            .dispatch_async_with_conn("COMMIT", Some(conn_id))
            .await
            .unwrap();

        let events = log.read_all().unwrap();
        assert!(
            events.iter().any(|e| e.actor == "jwt:alice"),
            "expected jwt:alice actor in audit log, got: {:?}",
            events.iter().map(|e| &e.actor).collect::<Vec<_>>()
        );
    }

    // ── S5: audit_carries_system_actor_when_auth_off ─────────────────────────

    /// S5 green gate: COMMIT with --auth=off / System principal has actor = "system".
    #[tokio::test]
    async fn audit_carries_system_actor_when_auth_off() {
        use rockstream_control::audit::FileAuditLog;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::new().unwrap();
        let log = Arc::new(FileAuditLog::open(tmp.path()).unwrap());

        let store = Arc::new(object_store::memory::InMemory::new());
        let shard_db = Arc::new(
            rockstream_storage::ShardDb::builder("audit-shard-sys", store)
                .build()
                .await
                .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::default());
        let reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
        let handler = Arc::new(
            GatewayHandler::with_shard_db(catalog, reader, shard_db).with_audit_log(log.clone()),
        );

        let conn_id = "sys-conn";
        {
            let mut s = handler
                .sessions
                .entry(conn_id.to_string())
                .or_insert_with(crate::session::SessionState::new);
            s.idempotency_key = Some([2u8; 16]);
        }

        handler
            .dispatch_async_with_conn("INSERT INTO t (id, val) VALUES (2, 'y')", Some(conn_id))
            .await
            .unwrap();
        handler
            .dispatch_async_with_conn("COMMIT", Some(conn_id))
            .await
            .unwrap();

        let events = log.read_all().unwrap();
        assert!(
            events.iter().any(|e| e.actor == "system"),
            "expected system actor in audit log, got: {:?}",
            events.iter().map(|e| &e.actor).collect::<Vec<_>>()
        );
    }
}
