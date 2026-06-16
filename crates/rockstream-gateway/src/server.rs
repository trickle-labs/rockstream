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
use futures::{stream, Sink, StreamExt};
use futures::SinkExt;
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
use pgwire::messages::copy::{
    CopyData as MsgCopyData, CopyDone as MsgCopyDone, CopyOutResponse as MsgCopyOutResponse,
};
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use crate::catalog_stubs::{arrow_type_to_pg_oid, CatalogColumn, CatalogResponse, CatalogStubs, CatalogTable};
use crate::view_reader::{ViewReadStrategy, ViewReader};
use crate::write_buffer::{DmlOp, WriteBuffer};

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
    /// Optional ShardDb for direct-write DML commits.
    shard_db: Option<Arc<rockstream_storage::ShardDb>>,
}

impl GatewayHandler {
    pub fn new(catalog: Arc<CatalogStubs>, view_reader: Arc<dyn ViewReader>) -> Self {
        GatewayHandler {
            catalog,
            view_reader,
            query_parser: Arc::new(NoopQueryParser),
            write_buffers: Arc::new(DashMap::new()),
            shard_db: None,
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
            shard_db: Some(shard_db),
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
            return Some(Ok(vec![Response::TransactionStart(Tag::new("BEGIN").with_rows(0))]));
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
    async fn dispatch_async_with_conn(
        &self,
        query: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(result) = self.dispatch_sync(query) {
            // Promote lifetime — responses from dispatch_sync hold no borrows
            // from `query`, only owned data.
            return result.map(|v| v.into_iter().map(promote_response).collect());
        }

        let q = query.trim();
        let ql = q.to_lowercase();

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
            self.catalog.add_view_with_deps(
                CatalogView {
                    name: view_name.clone(),
                    sql: select_sql,
                    columns: vec![],
                },
                deps,
            );
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
                return Ok(vec![Response::Execution(Tag::new("CREATE TABLE").with_rows(0))]);
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

        Ok(vec![Response::Execution(Tag::new("CREATE TABLE").with_rows(0))])
    }

    /// COMMIT handler: flush write buffer to ShardDb atomically.
    async fn handle_commit(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        let Some(conn_id) = conn_id else {
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new("COMMIT").with_rows(0)))]);
        };

        let mut entry = self.write_buffers.entry(conn_id.to_string()).or_default();
        if entry.is_empty() {
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new("COMMIT").with_rows(0)))]);
        }

        let Some(shard_db) = &self.shard_db else {
            // No shard — discard buffer, return COMMIT (best effort without storage)
            entry.clear();
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new("COMMIT").with_rows(0)))]);
        };

        let ops = entry.drain();
        let affected = ops.len();
        drop(entry); // release DashMap entry guard before await

        // Allocate next epoch
        let epoch = shard_db.last_epoch().fetch_add(1, Ordering::SeqCst) + 1;

        // Build WriteBatch from DmlOps — only Put and Delete, no range-delete.
        let mut batch = rockstream_storage::WriteBatch::new();
        for op in &ops {
            match op {
                DmlOp::Insert { table, row_key, values_tsv, .. } => {
                    let key = format!("view_output/{table}/{row_key}");
                    batch.put(key.as_bytes(), values_tsv.as_bytes());
                }
                DmlOp::Update { table, old_row_key, new_row_key, new_tsv, .. } => {
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
        // Advance shard frontier
        batch.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );

        shard_db
            .write_batch(batch)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e))))?;

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
        Ok(vec![promote_response(Response::TransactionEnd(Tag::new("ROLLBACK").with_rows(0)))])
    }

    /// INSERT handler: accumulate rows in the write buffer.
    async fn handle_insert(&self, q: &str, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        // Parse INSERT INTO <table> [(cols)] VALUES (v1, v2, ...) [RETURNING ...]
        let returning = q.to_lowercase().contains(" returning ");
        let (table, cols, values) = match parse_insert(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "42601".to_owned(), e,
                ))))]);
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
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "53400".to_owned(), e.to_string(),
                ))))]);
            }
        }

        if returning {
            // Auto-commit single INSERT … RETURNING outside explicit transaction
            let schema_fields = if let Some(ct) = self.catalog.get_table(&table) {
                ct.columns.iter().map(|c| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), FieldFormat::Text)
                }).collect::<Vec<_>>()
            } else {
                cols.iter().map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text)).collect()
            };
            let schema = Arc::new(schema_fields);
            let schema_ref = schema.clone();
            let row_values: Vec<Option<String>> = values.iter().map(|v| Some(v.clone())).collect();
            let stream = Box::pin(stream::once(async move {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                for v in &row_values {
                    encoder.encode_field(v).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                encoder.finish()
            }));
            return Ok(vec![promote_response(Response::Query(QueryResponse::new(schema, stream)))]);
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("INSERT 0 1").with_rows(1)))])
    }

    /// UPDATE handler: accumulate in write buffer.
    async fn handle_update(&self, q: &str, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        let (table, set_pairs, where_pairs) = match parse_update(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "42601".to_owned(), e,
                ))))]);
            }
        };

        // Build old row key from WHERE clause, new values from SET clause
        let (old_cols, old_vals): (Vec<_>, Vec<_>) = where_pairs.iter().map(|(c, v)| (c.clone(), v.clone())).unzip();
        let old_row_key = build_row_key(&old_cols, &old_vals);
        let old_tsv = old_vals.join("\t");

        let (new_cols, new_vals): (Vec<_>, Vec<_>) = set_pairs.iter().map(|(c, v)| (c.clone(), v.clone())).unzip();
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
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "53400".to_owned(), e.to_string(),
                ))))]);
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("UPDATE 1").with_rows(1)))])
    }

    /// DELETE handler: accumulate in write buffer.
    async fn handle_delete(&self, q: &str, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        let (table, where_pairs) = match parse_delete(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "42601".to_owned(), e,
                ))))]);
            }
        };

        let (cols, vals): (Vec<_>, Vec<_>) = where_pairs.iter().map(|(c, v)| (c.clone(), v.clone())).unzip();
        let row_key = build_row_key(&cols, &vals);

        let op = DmlOp::Delete { table, row_key };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(), "53400".to_owned(), e.to_string(),
                ))))]);
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("DELETE 1").with_rows(1)))])
    }
}

impl NoopStartupHandler for GatewayHandler {}

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
                client.metadata_mut().insert("_rs_conn_id".to_string(), id.clone());
                id
            }
        };

        // COPY OUT: stream CopyData messages directly through the client sink.
        let ql = query.trim().to_lowercase();
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
                let copy_out_resp = MsgCopyOutResponse::new(
                    0,
                    col_count as i16,
                    vec![0i16; col_count],
                );
                client
                    .feed(PgWireBackendMessage::CopyOutResponse(copy_out_resp))
                    .await?;

                // 2. CopyData — one message per row (tab-separated text + newline)
                for row in &rows {
                    let mut data = row.clone();
                    data.push(b'\n');
                    let copy_data = MsgCopyData::new(bytes::Bytes::from(data));
                    client.feed(PgWireBackendMessage::CopyData(copy_data)).await?;
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

    /// Create a gateway server with a catalog and ShardDb for direct-write DML.
    pub fn with_shard_db(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        GatewayServer {
            addr,
            handler: Arc::new(GatewayHandler::with_shard_db(catalog, view_reader, shard_db)),
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

/// Extract the view name from `CREATE [MATERIALIZED] VIEW <name> AS …`.
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
    // Take up to " AS " (case-insensitive)
    let as_pos = after.to_lowercase().find(" as ")?;
    let raw = after[..as_pos].trim().trim_matches('"');
    let name = raw.rsplit('.').next().unwrap_or(raw).trim_matches('"');
    if name.is_empty() { None } else { Some(name.to_string()) }
}

/// Extract the SELECT SQL from `CREATE … AS <select>`.
fn parse_create_view_query(q: &str) -> Option<String> {
    let ql = q.trim().to_lowercase();
    let as_pos = ql.find(" as ")?;
    Some(q[as_pos + 4..].trim().to_string())
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
                if next == "PRECISION" { "DOUBLE PRECISION".to_string() } else { pg_type }
            } else {
                pg_type
            };
            let arrow_type = pg_type_to_arrow(&full_type);
            Some(CatalogColumn { name: col_name, data_type: arrow_type.to_string() })
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
    cols.iter()
        .zip(vals.iter())
        .map(|(c, v)| format!("{c}={v}"))
        .collect::<Vec<_>>()
        .join("|")
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
        let close = before_values_lower.rfind(')').ok_or("missing ) in column list")?;
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
    let values: Vec<String> = values.into_iter()
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
    let where_pos = ql.find(" where ").ok_or("missing WHERE (v0.24 requires WHERE)")?;

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
    let after = ql.strip_prefix("delete from ").ok_or("not DELETE FROM")?.trim_start();

    let name_end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    let table = after[..name_end].trim().to_lowercase();

    let where_pos = ql.find(" where ").ok_or("missing WHERE (v0.24 requires WHERE)")?;
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
            if col.is_empty() { None } else { Some((col, val)) }
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
