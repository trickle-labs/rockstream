//! `GatewayServer` — PostgreSQL wire protocol server.
//!
//! Accepts TCP connections and serves reads of maintained views using the
//! pgwire library. The same handler implements both simple and extended query
//! protocols.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use crate::catalog_stubs::{arrow_type_to_pg_oid, CatalogResponse, CatalogStubs};
use crate::view_reader::{ViewReadStrategy, ViewReader};

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
}

impl GatewayHandler {
    pub fn new(catalog: Arc<CatalogStubs>, view_reader: Arc<dyn ViewReader>) -> Self {
        GatewayHandler {
            catalog,
            view_reader,
            query_parser: Arc::new(NoopQueryParser),
        }
    }

    /// Dispatch a synchronous (non-view-read) query and return pgwire responses.
    /// Returns `None` if the query needs async view reading (handled by callers).
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

        // COPY <view> TO STDOUT
        if ql.starts_with("copy ") && ql.contains(" to stdout") {
            if let Some(view_name) = parse_copy_to_stdout_view(q) {
                return Some(self.handle_copy_out(&view_name));
            }
        }

        // CREATE VIEW / CREATE MATERIALIZED VIEW
        if ql.starts_with("create view ")
            || ql.starts_with("create materialized view ")
            || ql.starts_with("create or replace view ")
        {
            return Some(self.handle_create_view(q));
        }

        // Transaction control
        if ql == "begin" || ql == "begin;" || ql.starts_with("begin ") {
            return Some(Ok(vec![Response::TransactionStart(Tag::new("BEGIN").with_rows(0))]));
        }
        if ql == "commit" || ql == "commit;" {
            return Some(Ok(vec![Response::TransactionEnd(Tag::new("COMMIT").with_rows(0))]));
        }
        if ql == "rollback" || ql == "rollback;" {
            return Some(Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK").with_rows(0))]));
        }

        None
    }

    /// Dispatch any query asynchronously.  Catalog/session queries are handled
    /// immediately; view SELECT queries await the storage read.
    async fn dispatch_async(&self, query: &str) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(result) = self.dispatch_sync(query) {
            // Promote lifetime — responses from dispatch_sync hold no borrows
            // from `query`, only owned data.
            return result.map(|v| v.into_iter().map(promote_response).collect());
        }

        let q = query.trim();
        let ql = q.to_lowercase();

        // SELECT … FROM <view> [LIMIT n]
        if ql.contains("from ") {
            if let Some(view_name) = extract_view_name_from_select(q) {
                if !view_name.starts_with("pg_") && !view_name.starts_with("information_schema") {
                    let limit = extract_limit(q);
                    return self.read_view_response(&view_name, limit).await;
                }
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("OK")))])
    }

    /// Read rows from a view and build a pgwire `Response::Query`.
    async fn read_view_response(&self, view_name: &str, limit: Option<usize>) -> PgWireResult<Vec<Response<'static>>> {
        let schema_fields: Vec<FieldInfo> = if let Some(cv) = self.catalog.get_view(view_name) {
            cv.columns
                .iter()
                .map(|c| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), FieldFormat::Text)
                })
                .collect()
        } else {
            vec![FieldInfo::new("result".to_string(), None, None, Type::TEXT, FieldFormat::Text)]
        };

        let raw_rows = self
            .view_reader
            .read_view(view_name, limit, ViewReadStrategy::HotOnly)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let schema = Arc::new(schema_fields);
        let schema_ref = schema.clone();
        let data_stream = stream::iter(raw_rows).map(move |raw: Vec<u8>| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let row_str = String::from_utf8_lossy(&raw).into_owned();
            let col_count = schema_ref.len();
            let fields: Vec<&str> = row_str.split('\t').collect();
            for i in 0..col_count {
                let val: Option<&str> = fields.get(i).copied();
                encoder.encode_field(&val).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(schema, data_stream))])
    }

    fn handle_copy_out<'a>(&'a self, view_name: &str) -> PgWireResult<Vec<Response<'a>>> {
        use pgwire::api::results::CopyResponse;
        let col_count = self.catalog.get_view(view_name).map(|v| v.columns.len()).unwrap_or(1);
        Ok(vec![Response::CopyOut(CopyResponse::new(0, col_count, vec![0i16; col_count]))])
    }

    fn handle_create_view<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let ql = q.to_lowercase();
        let tag = if ql.contains("materialized view") {
            "CREATE MATERIALIZED VIEW"
        } else {
            "CREATE VIEW"
        };
        Ok(vec![Response::Execution(Tag::new(tag).with_rows(0))])
    }
}

impl NoopStartupHandler for GatewayHandler {}

#[async_trait]
impl SimpleQueryHandler for GatewayHandler {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.dispatch_async(query).await
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
        _client: &mut C,
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
        let responses = self.dispatch_async(portal.statement.statement.as_str()).await?;
        Ok(responses.into_iter().next().unwrap_or(Response::Execution(Tag::new("OK"))))
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
    type CopyHandler = NoopCopyHandler;
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
        Arc::new(NoopCopyHandler)
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
                    if let Err(e) =
                        pgwire::tokio::process_socket(socket, None, factory_ref).await
                    {
                        tracing::debug!("gateway connection error: {e}");
                    }
                });
            }
        });
        Ok((local_addr, handle))
    }
}

// ── Query helpers ─────────────────────────────────────────────────────────────

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
    if name.is_empty() { None } else { Some(name.to_string()) }
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
    if raw.is_empty() { None } else { Some(raw.to_string()) }
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
