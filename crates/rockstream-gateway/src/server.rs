//! `GatewayServer` — PostgreSQL wire protocol server.
//!
//! Accepts TCP connections and serves reads of maintained views using the
//! pgwire library. The same handler implements both simple and extended query
//! protocols.

#![deny(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::memory::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::SinkExt;
use futures::{stream, Sink, StreamExt};
use parking_lot::Mutex;
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, ServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::{
    send_copy_both_response, send_copy_in_response, send_copy_out_response, CopyHandler,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{send_execution_response, ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
    FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{
    CopyData, CopyData as MsgCopyData, CopyDone, CopyDone as MsgCopyDone, CopyFail,
    CopyOutResponse as MsgCopyOutResponse,
};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Parse, ParseComplete, PortalSuspended,
    TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{CommandComplete, EmptyQueryResponse, NoticeResponse};
use pgwire::messages::startup::{BackendKeyData, ParameterStatus};
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use base64::Engine as _;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, CdcOperation, KafkaSource, OffsetToken,
    PgOutputConfig, PgOutputEvent, PgOutputRelationMetadata, PostgresCdcSource, S3Source,
    SnapshotDeltaFence, SourceCheckpointStore, SourceConnector, SourceOwnerLease,
    SourceRuntimeCoordinator,
};
use rockstream_ops::sink::{column_values_to_tsv_bytes, materialize_view_state};
use rockstream_ops::ArrowZSet;
use rockstream_sql::SqlFrontend;
use rockstream_types::config::ScatterPruningConfig;
use rockstream_types::explain::ExplainLevel;
use rockstream_types::frontier::{build_exact_membership_filter, ColumnStats, ShardColumnStats};
use rockstream_types::ids::{ConnectorId, OperatorId, ShardId, ViewId};
use rockstream_types::workload::{FreshnessSlo, MemoryLimit, WorkloadDef, WorkloadPriority};

use crate::auth::{
    scram_server_key, scram_server_signature, scram_stored_key, verify_client_proof, AuthMode,
    JwtVerifier, Principal,
};
use crate::catalog_stubs::{
    arrow_type_to_pg_oid, CatalogColumn, CatalogResponse, CatalogSinkEntry, CatalogSourceEntry,
    CatalogStubs, CatalogTable, PgOutputSourceRuntimeDetail,
};

use crate::copy_state::{
    CopyState, COPY_IN_BUFFER_ROWS, COPY_IN_FLUSH_BYTES, MAX_COPY_IN_BATCH_ROWS,
};
use crate::notify_registry::NotifyRegistry;
use crate::pgoutput_coordinator::{
    append_blocked_state, BlockedRelationState, BufferedPgOutputEnvelope, ColumnRoute,
    EncodedChange, RelationChange, RelationRoute, ReplicaIdentity, SharedPgOutputCoordinator,
    SourceIdentityV1,
};
use crate::role_catalog::RoleCatalog;
use crate::session::{FreshnessToken, ScramAuthState, SessionNotice, SessionState};
use crate::view_reader::{ViewReadStrategy, ViewReader};
use crate::webhook_source::{
    HttpWebhookSource, WebhookFormat, WebhookResult, HTTP_WEBHOOK_MAX_REQUEST_BYTES,
};
use crate::write_buffer::{DmlOp, WriteBuffer};
use crate::GatewayError;
use pgwire::messages::response::NotificationResponse;

// ── Cancellation primitive ────────────────────────────────────────────────────

/// Per-connection cancel handle. Backed by `tokio::sync::watch`.
/// `cancel()` sets the flag; `cancelled()` is an async future that resolves
/// when the flag is set.
#[derive(Clone)]
pub struct CancelToken {
    tx: Arc<tokio::sync::watch::Sender<bool>>,
    rx: tokio::sync::watch::Receiver<bool>,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }
    /// Signal cancellation.
    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }
    /// Async future that resolves when cancelled.
    pub async fn cancelled(&mut self) {
        let _ = self.rx.wait_for(|v| *v).await;
    }
}

// Task-local storage for connection ID, portal format, and cancellation token
tokio::task_local! {
    pub static CONN_ID: String;
    pub static PORTAL_FORMAT: pgwire::api::portal::Format;
    pub static CANCEL_TOKEN: CancelToken;
    /// v0.51.5: the connecting peer's socket address, set once per accepted
    /// connection before the TLS handshake begins. Read synchronously by
    /// `crate::tls::MtlsCnExtractingVerifier::verify_client_cert`, which runs
    /// deep inside rustls's handshake state machine but on this same task.
    pub static PEER_ADDR: std::net::SocketAddr;
}

// ── S1 Custom Parameter Provider ──────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct GatewayServerParameterProvider;

impl ServerParameterProvider for GatewayServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<std::collections::HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = std::collections::HashMap::new();
        params.insert("server_version".to_owned(), "14.9 (RockStream)".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("integer_datetimes".to_owned(), "on".to_owned());
        params.insert("standard_conforming_strings".to_owned(), "on".to_owned());
        params.insert("DateStyle".to_owned(), "ISO, YMD".to_owned());
        params.insert("IntervalStyle".to_owned(), "postgres".to_owned());
        Some(params)
    }
}

// ── S2 Prepared Statement Structures ──────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub sql: String,
    pub parameter_types: Vec<Type>,
}

#[derive(Debug)]
pub struct PreparedStatementCache {
    catalog: Arc<CatalogStubs>,
}

#[async_trait]
impl QueryParser for PreparedStatementCache {
    type Statement = PreparedStatement;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let parameter_types = infer_parameter_types(&self.catalog, sql).await;
        Ok(PreparedStatement {
            name: String::new(),
            sql: sql.to_string(),
            parameter_types,
        })
    }
}

// Helper functions for parameter type inference
async fn infer_parameter_types(catalog: &CatalogStubs, sql: &str) -> Vec<Type> {
    let mut indices = std::collections::BTreeSet::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut max_idx = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > start {
                if let Ok(num_str) = std::str::from_utf8(&bytes[start..i]) {
                    if let Ok(idx) = num_str.parse::<usize>() {
                        indices.insert(idx);
                        if idx > max_idx {
                            max_idx = idx;
                        }
                    }
                }
            }
        } else {
            i += 1;
        }
    }

    if max_idx == 0 {
        return vec![];
    }

    let mut inferred_types = vec![Type::TEXT; max_idx];

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut explicit_casts = std::collections::HashMap::new();
    if let Ok(statements) = sqlparser::parser::Parser::parse_sql(&dialect, sql) {
        for stmt in &statements {
            struct AstVisitor<'a> {
                casts: &'a mut std::collections::HashMap<usize, Type>,
                in_any: bool,
            }
            impl<'a> AstVisitor<'a> {
                fn visit_expr(&mut self, expr: &sqlparser::ast::Expr) {
                    use sqlparser::ast::{Expr, Value};
                    match expr {
                        Expr::Cast {
                            expr: inner,
                            data_type,
                            ..
                        } => {
                            if let Expr::Value(v) = &**inner {
                                if let Value::Placeholder(name) = &v.value {
                                    if let Some(rest) = name.strip_prefix('$') {
                                        if let Ok(idx) = rest.parse::<usize>() {
                                            self.casts
                                                .insert(idx, map_data_type_to_pg_type(data_type));
                                        }
                                    }
                                }
                            } else {
                                self.visit_expr(inner);
                            }
                        }
                        Expr::Value(v) => {
                            if let Value::Placeholder(name) = &v.value {
                                if let Some(rest) = name.strip_prefix('$') {
                                    if let Ok(idx) = rest.parse::<usize>() {
                                        if self.in_any {
                                            self.casts.insert(idx, Type::TEXT_ARRAY);
                                        }
                                    }
                                }
                            }
                        }
                        Expr::AnyOp { left, right, .. } => {
                            self.visit_expr(left);
                            let old_any = self.in_any;
                            self.in_any = true;
                            self.visit_expr(right);
                            self.in_any = old_any;
                        }
                        Expr::AllOp { left, right, .. } => {
                            self.visit_expr(left);
                            let old_any = self.in_any;
                            self.in_any = true;
                            self.visit_expr(right);
                            self.in_any = old_any;
                        }
                        Expr::Nested(inner) => {
                            self.visit_expr(inner);
                        }
                        Expr::BinaryOp { left, right, .. } => {
                            self.visit_expr(left);
                            self.visit_expr(right);
                        }
                        Expr::Function(func) => {
                            use sqlparser::ast::{FunctionArg, FunctionArguments};
                            if let FunctionArguments::List(arg_list) = &func.args {
                                for arg in &arg_list.args {
                                    match arg {
                                        FunctionArg::Unnamed(arg_expr) => {
                                            use sqlparser::ast::FunctionArgExpr;
                                            if let FunctionArgExpr::Expr(e) = arg_expr {
                                                self.visit_expr(e);
                                            }
                                        }
                                        FunctionArg::Named { arg, .. } => {
                                            use sqlparser::ast::FunctionArgExpr;
                                            if let FunctionArgExpr::Expr(e) = arg {
                                                self.visit_expr(e);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Expr::InList {
                            expr: inner, list, ..
                        } => {
                            self.visit_expr(inner);
                            for e in list {
                                self.visit_expr(e);
                            }
                        }
                        Expr::Between {
                            expr: inner,
                            low,
                            high,
                            ..
                        } => {
                            self.visit_expr(inner);
                            self.visit_expr(low);
                            self.visit_expr(high);
                        }
                        Expr::Case {
                            operand,
                            conditions,
                            else_result,
                            ..
                        } => {
                            if let Some(op) = operand {
                                self.visit_expr(op);
                            }
                            for cond in conditions {
                                self.visit_expr(&cond.condition);
                                self.visit_expr(&cond.result);
                            }
                            if let Some(res) = else_result {
                                self.visit_expr(res);
                            }
                        }
                        _ => {}
                    }
                }
            }

            let mut visitor = AstVisitor {
                casts: &mut explicit_casts,
                in_any: false,
            };
            if let sqlparser::ast::Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = &*q.body {
                    for projection in &select.projection {
                        use sqlparser::ast::SelectItem;
                        match projection {
                            SelectItem::UnnamedExpr(expr) => visitor.visit_expr(expr),
                            SelectItem::ExprWithAlias { expr, .. } => visitor.visit_expr(expr),
                            _ => {}
                        }
                    }
                    if let Some(selection) = &select.selection {
                        visitor.visit_expr(selection);
                    }
                }
            }
        }
    }

    for (idx, ty) in &explicit_casts {
        if *idx > 0 && *idx <= max_idx {
            inferred_types[*idx - 1] = ty.clone();
        }
    }

    let ctx = SessionContext::new();
    rockstream_sql::frontend::register_session_sql_udf(&ctx);
    for view in catalog.list_views() {
        let mut fields = Vec::new();
        for col in &view.columns {
            fields.push(Field::new(
                &col.name,
                string_to_arrow_datatype(&col.data_type),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));
        match MemTable::try_new(schema, vec![vec![]]) {
            Ok(mem_table) => {
                let _ = ctx.register_table(
                    datafusion::sql::TableReference::from(view.name.as_str()),
                    Arc::new(mem_table),
                );
            }
            Err(e) => {
                eprintln!("Failed to create MemTable for view {}: {:?}", view.name, e);
            }
        }
    }
    for table in catalog.list_tables() {
        let mut fields = Vec::new();
        for col in &table.columns {
            fields.push(Field::new(
                &col.name,
                string_to_arrow_datatype(&col.data_type),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));
        match MemTable::try_new(schema, vec![vec![]]) {
            Ok(mem_table) => {
                let _ = ctx.register_table(
                    datafusion::sql::TableReference::from(table.name.as_str()),
                    Arc::new(mem_table),
                );
            }
            Err(e) => {
                eprintln!(
                    "Failed to create MemTable for table {}: {:?}",
                    table.name, e
                );
            }
        }
    }

    let mut df_inferred = std::collections::HashMap::new();
    match ctx.sql(sql).await {
        Ok(df) => {
            if let Ok(opt_plan) = df.into_optimized_plan() {
                visit_plan_expressions(&opt_plan, &mut |expr| {
                    visit_expr_placeholders(expr, &mut |id, dt| {
                        if let Some(rest) = id.strip_prefix('$') {
                            if let Ok(idx) = rest.parse::<usize>() {
                                if let Some(arrow_dt) = dt {
                                    df_inferred.insert(idx, pg_type_from_arrow_datatype(arrow_dt));
                                }
                            }
                        }
                    });
                });
            }
        }
        Err(e) => {
            eprintln!("DataFusion parse/infer failed for SQL ({}): {:?}", sql, e);
        }
    }

    for (idx, ty) in df_inferred {
        if idx > 0 && idx <= max_idx && !explicit_casts.contains_key(&idx) {
            inferred_types[idx - 1] = ty;
        }
    }

    inferred_types
}

fn string_to_arrow_datatype(dt: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    if let Some((precision, scale)) = dt
        .strip_prefix("Decimal(")
        .or_else(|| dt.strip_prefix("DECIMAL("))
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.split_once(','))
        .and_then(|(precision, scale)| {
            Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?))
        })
    {
        return DataType::Decimal128(precision, scale);
    }
    match dt {
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Utf8" | "LargeUtf8" => DataType::Utf8,
        "Boolean" => DataType::Boolean,
        "Binary" | "LargeBinary" => DataType::Binary,
        "Timestamp" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "TimestampTz" => DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("+00:00".into()),
        ),
        "Date32" | "Date64" => DataType::Date32,
        "Time32" | "Time64" => DataType::Time32(datafusion::arrow::datatypes::TimeUnit::Second),
        "Uuid" | "UUID" => DataType::FixedSizeBinary(16),
        "Decimal" | "Decimal128" | "Decimal256" => DataType::Decimal128(38, 10),
        "Json" | "JSON" | "Jsonb" | "JSONB" => DataType::Utf8,
        "Varchar" | "VARCHAR" => DataType::Utf8,
        "Char" | "CHAR" => DataType::Utf8,
        "Interval" => DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        "_int4" | "List(Int32)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Int32, true),
        )),
        "_int8" | "List(Int64)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Int64, true),
        )),
        "_text" | "List(Utf8)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Utf8, true),
        )),
        "_float8" | "List(Float64)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Float64, true),
        )),
        "_bool" | "List(Boolean)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Boolean, true),
        )),
        "_uuid" | "List(Uuid)" => DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::FixedSizeBinary(16), true),
        )),
        _ => DataType::Utf8,
    }
}

fn catalog_columns_to_schema(columns: &[CatalogColumn]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|col| Field::new(&col.name, string_to_arrow_datatype(&col.data_type), true))
            .collect::<Vec<_>>(),
    ))
}

fn pg_type_from_arrow_datatype(dt: &datafusion::arrow::datatypes::DataType) -> Type {
    use datafusion::arrow::datatypes::DataType;
    let oid = match dt {
        DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Utf8 | DataType::LargeUtf8 => 25,
        DataType::Boolean => 16,
        DataType::Binary | DataType::LargeBinary => 17,
        DataType::Timestamp(_, None) => 1114,
        DataType::Timestamp(_, Some(_)) => 1184,
        DataType::Date32 | DataType::Date64 => 1082,
        DataType::Time32(_) | DataType::Time64(_) => 1083,
        DataType::FixedSizeBinary(16) => 2950,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => 1700,
        DataType::Interval(_) | DataType::Duration(_) => 1186,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            match field.data_type() {
                DataType::Int32 => 1007,
                DataType::Int64 => 1016,
                DataType::Utf8 | DataType::LargeUtf8 => 1009,
                DataType::Float64 => 1022,
                DataType::Boolean => 1000,
                DataType::FixedSizeBinary(16) => 2951,
                _ => 1009,
            }
        }
        _ => 25,
    };
    pg_type_from_oid(oid)
}

fn map_data_type_to_pg_type(dt: &sqlparser::ast::DataType) -> Type {
    use sqlparser::ast::DataType;
    match dt {
        DataType::Integer(_) => Type::INT4,
        DataType::BigInt(_) => Type::INT8,
        DataType::Double(..) => Type::FLOAT8,
        DataType::Float8 => Type::FLOAT8,
        DataType::Float(..) => Type::FLOAT4,
        DataType::Int8(..) => Type::INT8,
        DataType::Int4(..) => Type::INT4,
        DataType::Int2(..) => Type::INT2,
        DataType::Text => Type::TEXT,
        DataType::Boolean => Type::BOOL,
        DataType::Bytea => Type::BYTEA,
        DataType::Custom(name, _) => {
            let n = name.to_string().to_lowercase();
            match n.as_str() {
                "int2" | "smallint" => Type::INT2,
                "int4" | "integer" | "int" => Type::INT4,
                "int8" | "bigint" => Type::INT8,
                "float4" | "real" => Type::FLOAT4,
                "float8" | "double" | "float" => Type::FLOAT8,
                "text" | "varchar" => Type::TEXT,
                "bool" | "boolean" => Type::BOOL,
                "bytea" => Type::BYTEA,
                "timestamp" => Type::TIMESTAMP,
                "timestamptz" => Type::TIMESTAMPTZ,
                "date" => Type::DATE,
                "time" => Type::TIME,
                "uuid" => Type::UUID,
                "numeric" | "decimal" => Type::NUMERIC,
                "json" => Type::JSON,
                "jsonb" => Type::JSONB,
                "interval" => Type::INTERVAL,
                "_int4" => Type::INT4_ARRAY,
                "_int8" => Type::INT8_ARRAY,
                "_text" => Type::TEXT_ARRAY,
                "_float8" => Type::FLOAT8_ARRAY,
                "_bool" => Type::BOOL_ARRAY,
                "_uuid" => Type::UUID_ARRAY,
                _ => Type::TEXT,
            }
        }
        _ => {
            let s = format!("{:?}", dt).to_lowercase();
            if s.contains("smallint") {
                Type::INT2
            } else if s.contains("timestamp") && s.contains("timezone") {
                Type::TIMESTAMPTZ
            } else if s.contains("timestamp") {
                Type::TIMESTAMP
            } else if s.contains("date") {
                Type::DATE
            } else if s.contains("time") {
                Type::TIME
            } else if s.contains("uuid") {
                Type::UUID
            } else if s.contains("numeric") || s.contains("decimal") {
                Type::NUMERIC
            } else if s.contains("json") {
                Type::JSON
            } else if s.contains("varchar") {
                Type::VARCHAR
            } else if s.contains("char") {
                Type::CHAR
            } else if s.contains("interval") {
                Type::INTERVAL
            } else if s.contains("array") {
                if s.contains("int4") || s.contains("integer") {
                    Type::INT4_ARRAY
                } else if s.contains("int8") || s.contains("bigint") {
                    Type::INT8_ARRAY
                } else if s.contains("text") || s.contains("varchar") {
                    Type::TEXT_ARRAY
                } else if s.contains("float8") || s.contains("double") {
                    Type::FLOAT8_ARRAY
                } else if s.contains("bool") {
                    Type::BOOL_ARRAY
                } else if s.contains("uuid") {
                    Type::UUID_ARRAY
                } else {
                    Type::TEXT_ARRAY
                }
            } else {
                Type::TEXT
            }
        }
    }
}

fn visit_plan_expressions<F>(plan: &datafusion::logical_expr::LogicalPlan, f: &mut F)
where
    F: FnMut(&datafusion::logical_expr::Expr),
{
    use datafusion::logical_expr::LogicalPlan;
    match plan {
        LogicalPlan::Projection(proj) => {
            for expr in &proj.expr {
                f(expr);
            }
        }
        LogicalPlan::Filter(filter) => {
            f(&filter.predicate);
        }
        LogicalPlan::Aggregate(agg) => {
            for expr in &agg.group_expr {
                f(expr);
            }
            for expr in &agg.aggr_expr {
                f(expr);
            }
        }
        LogicalPlan::Join(join) => {
            if let Some(expr) = &join.filter {
                f(expr);
            }
        }
        LogicalPlan::Window(window) => {
            for expr in &window.window_expr {
                f(expr);
            }
        }
        LogicalPlan::Sort(sort) => {
            for sort_expr in &sort.expr {
                f(&sort_expr.expr);
            }
        }
        _ => {}
    }

    for input in plan.inputs() {
        visit_plan_expressions(input, f);
    }
}

fn visit_expr_placeholders<F>(expr: &datafusion::logical_expr::Expr, f: &mut F)
where
    F: FnMut(&String, &Option<datafusion::arrow::datatypes::DataType>),
{
    use datafusion::logical_expr::Expr;
    match expr {
        Expr::Placeholder(placeholder) => {
            let dt = placeholder
                .field
                .as_ref()
                .map(|field| field.data_type().clone());
            f(&placeholder.id, &dt);
        }
        _ => {
            use datafusion::common::tree_node::TreeNode;
            let _ = expr.apply(|e| {
                if let Expr::Placeholder(placeholder) = e {
                    let dt = placeholder
                        .field
                        .as_ref()
                        .map(|field| field.data_type().clone());
                    f(&placeholder.id, &dt);
                }
                Ok::<_, datafusion::error::DataFusionError>(
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            });
        }
    }
}

// ── S2 metrics ────────────────────────────────────────────────────────────────

pub static PREPARED_STATEMENTS_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static PORTALS_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// v0.51.6 Slice 1: bound on prepared statements/portals per connection.
/// Exceeding this bound no longer errors (`RS-2600`/`RS-2601`) — the
/// least-recently-used entry is evicted instead.
pub const MAX_PREPARED_STATEMENTS_PER_CONN: usize = 1000;
pub const MAX_PORTALS_PER_CONN: usize = 1000;

/// Cumulative count of prepared statements evicted via LRU (v0.51.6 Slice 1).
pub static PREPARED_STATEMENTS_EVICTED_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Cumulative count of portals evicted via LRU (v0.51.6 Slice 1).
pub static PORTALS_EVICTED_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

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
/// Total rows scanned into query-time DataFusion MemTables.
pub static QUERY_TIME_DATAFUSION_ROWS_SCANNED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Total rows scanned by COMMIT-time full rescans that drive compiled view refresh.
///
/// Retired by v0.51.4 Slice 0: the compiled-view refresh path no longer
/// rescans the full source table (`recompute_compiled_view` now consumes the
/// commit's own row-level `WriteBatch` delta). This counter is kept
/// read-only-compatible (never incremented again) so any external dashboard
/// referencing it degrades gracefully to "always zero increase" rather than
/// disappearing; new code should read `COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL`
/// instead.
pub static COMMIT_COMPILED_VIEW_ROWS_SCANNED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Total rows actually fed into a compiled view's pipeline per commit —
/// v0.51.4 Slice 0. Unlike the retired
/// `COMMIT_COMPILED_VIEW_ROWS_SCANNED_TOTAL`, this counts only the rows in
/// the commit's own row-level delta (insert/update/delete), so it is
/// proportional to the size of the commit, not the size of the source
/// table. This is the counter `commit_refresh_is_proportional_to_delta_not_table_size`
/// and Slice 9's regression benchmark assert against.
pub static COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Total rows processed by `CREATE INDEX` automatic backfill scans (Slice 5, v0.51.2).
pub static INDEX_BACKFILL_ROWS_PROCESSED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Number of `CREATE INDEX` backfills currently in progress (gauge, Slice 5, v0.51.2).
pub static INDEX_BACKFILL_IN_PROGRESS_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Maximum rows buffered by the query-time scatter source before backpressure.
pub const QUERY_TIME_SCATTER_MAX_IN_FLIGHT_ROWS: usize = 16_384;
/// Maximum key/value bytes buffered by the query-time scatter source.
pub const QUERY_TIME_SCATTER_MAX_IN_FLIGHT_BYTES: usize = 8 * 1024 * 1024;
/// The bounded scheduler runs one shard page at a time; this keeps a complete
/// relation streamed without retaining a merged cross-shard vector.
pub const QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES: usize = 1;
/// Explicit ceiling for genuinely pathological scans, deliberately far above
/// the retired one-million-row / 64-MiB compatibility caps.
pub const QUERY_TIME_SCATTER_PATHOLOGICAL_ROW_LIMIT: usize = 100_000_000;
/// Explicit byte ceiling paired with `QUERY_TIME_SCATTER_PATHOLOGICAL_ROW_LIMIT`.
pub const QUERY_TIME_SCATTER_PATHOLOGICAL_BYTE_LIMIT: usize = 32 * 1024 * 1024 * 1024;
/// Bound for the separate compiled-view source refresh path; this is not used
/// by ad hoc query-time execution.
pub const MAX_COMPILED_VIEW_SOURCE_ROWS: usize = 1_000_000;
/// Byte bound paired with `MAX_COMPILED_VIEW_SOURCE_ROWS`.
pub const MAX_COMPILED_VIEW_SOURCE_SCAN_BYTES: usize = 64 * 1024 * 1024;
/// Maximum source rows consumed in one bounded backfill poll.
pub const BACKFILL_BATCH_MAX_ROWS: usize = 2_000;
/// Maximum bytes a connector may return for one catch-up M3 step.
pub const BACKFILL_LIVE_DELTA_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Reservation capacity dedicated to source backfill; it never evicts views.
pub const BACKFILL_ADMISSION_CAPACITY_BYTES: u64 = BACKFILL_LIVE_DELTA_MAX_BYTES as u64;

struct BackfillReservation {
    controller: Arc<crate::admission::BackfillAdmissionController>,
    bytes: u64,
}

impl Drop for BackfillReservation {
    fn drop(&mut self) {
        self.controller.release(self.bytes);
    }
}

/// Observable current fill-levels for the bounded query-time scatter source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTimeScatterFillLevels {
    pub rows: usize,
    pub bytes: usize,
    pub batches: usize,
}

/// Explicit resource ceiling for one query-time scatter execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTimeScatterBudget {
    pub row_limit: usize,
    pub byte_limit: usize,
}

impl Default for QueryTimeScatterBudget {
    fn default() -> Self {
        Self {
            row_limit: QUERY_TIME_SCATTER_PATHOLOGICAL_ROW_LIMIT,
            byte_limit: QUERY_TIME_SCATTER_PATHOLOGICAL_BYTE_LIMIT,
        }
    }
}

pub static QUERY_TIME_SCATTER_ROWS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
pub static QUERY_TIME_SCATTER_BYTES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
pub static QUERY_TIME_SCATTER_BATCHES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// High-water marks for the same source. These make its backpressure bounds
/// auditable after a query has completed and the current gauges return to zero.
pub static QUERY_TIME_SCATTER_PEAK_ROWS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
pub static QUERY_TIME_SCATTER_PEAK_BYTES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
pub static QUERY_TIME_SCATTER_PEAK_BATCHES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static QUERY_TIME_SCATTER_BATCH_PERMITS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES);

pub fn query_time_scatter_fill_levels() -> QueryTimeScatterFillLevels {
    QueryTimeScatterFillLevels {
        rows: QUERY_TIME_SCATTER_ROWS_IN_FLIGHT.load(Ordering::Relaxed),
        bytes: QUERY_TIME_SCATTER_BYTES_IN_FLIGHT.load(Ordering::Relaxed),
        batches: QUERY_TIME_SCATTER_BATCHES_IN_FLIGHT.load(Ordering::Relaxed),
    }
}

/// Return the high-water fill levels observed by the query-time scatter source.
pub fn query_time_scatter_peak_fill_levels() -> QueryTimeScatterFillLevels {
    QueryTimeScatterFillLevels {
        rows: QUERY_TIME_SCATTER_PEAK_ROWS_IN_FLIGHT.load(Ordering::Relaxed),
        bytes: QUERY_TIME_SCATTER_PEAK_BYTES_IN_FLIGHT.load(Ordering::Relaxed),
        batches: QUERY_TIME_SCATTER_PEAK_BATCHES_IN_FLIGHT.load(Ordering::Relaxed),
    }
}
/// Hard row cap for a single `CREATE INDEX` automatic backfill scan (Slice 5,
/// v0.51.2). A table exceeding this fails backfill with RS-2027 rather than
/// leaving the index catalog entry stuck in `Building` forever. `CREATE
/// INDEX` backfill runs synchronously on the issuing session (no
/// `CONCURRENTLY` support), so this bound also caps how long a single
/// `CREATE INDEX` statement can block that session.
pub const MAX_INDEX_BACKFILL_ROWS: usize = 2_000;
/// Batch size for `CREATE INDEX` automatic backfill scans (Slice 5, v0.51.2).
pub const INDEX_BACKFILL_BATCH_ROWS: usize = 500;

// ── S8 WaitResult ─────────────────────────────────────────────────────────────

/// Maximum number of simultaneous connections.
/// Used to bound CancellationRegistry and ActivityRegistry.
/// Fill-level metric: `cancellation_registry.len()`.
pub const MAX_CONNECTIONS: usize = 10_000;

#[derive(Debug)]
enum WaitResult {
    Satisfied { elapsed_ms: u64 },
    TimedOut,
    NoStorage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GeneratedColumnKind {
    RandomUuid,
    Identity,
}

#[derive(Debug)]
struct TableInsertMetadata {
    generated_columns: HashMap<String, GeneratedColumnKind>,
    identity_sequences: HashMap<String, Arc<AtomicU64>>,
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Postgres Type from OID helper ─────────────────────────────────────────────

fn pg_type_from_oid(oid: i32) -> Type {
    match oid {
        21 => Type::INT2,
        23 => Type::INT4,
        20 => Type::INT8,
        700 => Type::FLOAT4,
        701 => Type::FLOAT8,
        25 => Type::TEXT,
        16 => Type::BOOL,
        17 => Type::BYTEA,
        1114 => Type::TIMESTAMP,
        1184 => Type::TIMESTAMPTZ,
        1082 => Type::DATE,
        1083 => Type::TIME,
        2950 => Type::UUID,
        1700 => Type::NUMERIC,
        114 => Type::JSON,
        3802 => Type::JSONB,
        1043 => Type::VARCHAR,
        1042 => Type::CHAR,
        1186 => Type::INTERVAL,
        1007 => Type::INT4_ARRAY,
        1016 => Type::INT8_ARRAY,
        1009 => Type::TEXT_ARRAY,
        1022 => Type::FLOAT8_ARRAY,
        1000 => Type::BOOL_ARRAY,
        2951 => Type::UUID_ARRAY,
        _ => Type::TEXT,
    }
}

// ── Binary-format-aware field encoding ────────────────────────────────────────
//
// Every SELECT response path (row-store, view-reader, and query-time
// DataFusion) stores/produces column values as plain UTF-8 text and must
// encode them per the client's negotiated `FieldFormat` (text or binary).
// `DataRowEncoder::encode_field` dispatches to the correct wire
// representation based on the *Rust type* passed to it (via `ToSql`), not
// the target Postgres OID — passing a bare `&str`/`Option<&str>` always
// produces a text-format value, even when the client asked for binary. So
// for every OID with a native binary format we must first parse the text
// into the matching typed Rust value before calling `encode_field`.
//
// This function is shared by all 4 SELECT dispatch sites so that adding a
// new OID's binary support (Slices 6-11) only requires touching one place.

/// Parse a `TIMESTAMP` (no time zone) text value into `chrono::NaiveDateTime`.
/// Accepts both `"YYYY-MM-DD HH:MM:SS[.ffffff]"` and ISO-8601
/// `"YYYY-MM-DDTHH:MM:SS[.ffffff]"` forms.
fn parse_pg_timestamp(s: &str) -> Option<chrono::NaiveDateTime> {
    let s = s.trim();
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
}

/// Parse a `TIMESTAMPTZ` text value into `chrono::DateTime<Utc>`. Accepts an
/// explicit UTC offset (e.g. `"+00"`, `"+00:00"`, `"Z"`); if none is present
/// the value is treated as already being in UTC (matching this gateway's
/// existing text-format behavior of passing the raw string through
/// unchanged).
fn parse_pg_timestamptz(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    parse_pg_timestamp(s)
        .map(|naive| chrono::DateTime::from_naive_utc_and_offset(naive, chrono::Utc))
}

/// Parse a `DATE` text value (`"YYYY-MM-DD"`) into `chrono::NaiveDate`.
fn parse_pg_date(s: &str) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()
}

/// Parse a `TIME` text value (`"HH:MM:SS[.ffffff]"`) into `chrono::NaiveTime`.
fn parse_pg_time(s: &str) -> Option<chrono::NaiveTime> {
    chrono::NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f").ok()
}

/// Parse a `UUID` text value into `uuid::Uuid`.
fn parse_pg_uuid(s: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(s.trim()).ok()
}

/// Newtype wrapping `uuid::Uuid` so this crate (not `pgwire` or `uuid`) owns
/// the impl of `pgwire`'s `ToSqlText` trait -- neither upstream crate
/// implements it for `uuid::Uuid`, so passing a bare `Option<uuid::Uuid>` to
/// `encoder.encode_field` fails to compile (`ToSqlText` is a supertrait
/// bound). Binary encoding delegates to `uuid::Uuid`'s own `ToSql` impl
/// (`postgres-types`' `with-uuid-1` feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PgUuid(uuid::Uuid);

impl postgres_types::ToSql for PgUuid {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <uuid::Uuid as postgres_types::ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgUuid {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.to_string().as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// Parse a `JSON`/`JSONB` text value into `serde_json::Value`.
fn parse_pg_json(s: &str) -> Option<serde_json::Value> {
    serde_json::from_str(s).ok()
}

/// Newtype wrapping `serde_json::Value` so this crate owns the `ToSqlText`
/// impl (`pgwire` implements it for neither `serde_json::Value` nor
/// `postgres_types::Json<T>`). Binary encoding delegates to
/// `serde_json::Value`'s own `ToSql` impl, which already writes the correct
/// representation for both OIDs -- a plain UTF-8 JSON document for `JSON`,
/// and the same document prefixed with the JSONB version byte (`0x01`) when
/// `ty == Type::JSONB` (confirmed from `postgres_types::serde_json_1`
/// source).
#[derive(Debug, Clone, PartialEq, Eq)]
struct PgJson(serde_json::Value);

impl postgres_types::ToSql for PgJson {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <serde_json::Value as postgres_types::ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl pgwire::types::ToSqlText for PgJson {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let text = serde_json::to_string(&self.0)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
        out.extend_from_slice(text.as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// A Postgres `INTERVAL` value in its 3-component (months, days,
/// microseconds) binary representation. `postgres_types` has no built-in
/// interval type, so this crate implements the wire format by hand: the
/// binary layout is `[microseconds: i64][days: i32][months: i32]`, all in
/// network (big-endian) byte order (see Postgres's `interval_send`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl postgres_types::ToSql for PgInterval {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        out.put_i64(self.microseconds);
        out.put_i32(self.days);
        out.put_i32(self.months);
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> postgres_types::FromSql<'a> for PgInterval {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<PgInterval, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("[RS-0001] invalid interval binary length; next_steps: send the 16-byte PostgreSQL INTERVAL binary representation".into());
        }
        let microseconds = i64::from_be_bytes(
            raw[0..8]
                .try_into()
                .map_err(|_| "[RS-0001] invalid interval slice")?,
        );
        let days = i32::from_be_bytes(
            raw[8..12]
                .try_into()
                .map_err(|_| "[RS-0001] invalid interval slice")?,
        );
        let months = i32::from_be_bytes(
            raw[12..16]
                .try_into()
                .map_err(|_| "[RS-0001] invalid interval slice")?,
        );

        Ok(PgInterval {
            months,
            days,
            microseconds,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

impl pgwire::types::ToSqlText for PgInterval {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let years = self.months / 12;
        let months = self.months % 12;
        let mut micros = self.microseconds;
        let negative_time = micros < 0;
        if negative_time {
            micros = -micros;
        }
        let hours = micros / 3_600_000_000;
        micros %= 3_600_000_000;
        let minutes = micros / 60_000_000;
        micros %= 60_000_000;
        let seconds = micros / 1_000_000;
        let frac = micros % 1_000_000;
        let mut parts = Vec::new();
        if years != 0 {
            parts.push(format!(
                "{years} year{}",
                if years.abs() == 1 { "" } else { "s" }
            ));
        }
        if months != 0 {
            parts.push(format!(
                "{months} mon{}",
                if months.abs() == 1 { "" } else { "s" }
            ));
        }
        if self.days != 0 {
            parts.push(format!(
                "{} day{}",
                self.days,
                if self.days.abs() == 1 { "" } else { "s" }
            ));
        }
        let sign = if negative_time { "-" } else { "" };
        let time_str = if frac != 0 {
            format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{frac:06}")
        } else {
            format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
        };
        if !time_str.is_empty() && (time_str != "00:00:00" || parts.is_empty()) {
            parts.push(time_str);
        }
        out.extend_from_slice(parts.join(" ").as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// Parse a textual `INTERVAL` literal (e.g. `"1 year 2 mons 3 days
/// 04:05:06.123456"`, `"-5 days"`, `"00:00:00"`) into a `PgInterval`. Accepts
/// any subset/ordering of `<n> year(s)`, `<n> mon(s)`/`<n> month(s)`,
/// `<n> day(s)` components followed by an optional `[-]HH:MM:SS[.ffffff]`
/// clock component.
fn parse_pg_interval(s: &str) -> Option<PgInterval> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut months = 0i32;
    let mut days = 0i32;
    let mut microseconds = 0i64;
    let mut saw_any = false;

    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        if let Ok(n) = tok.parse::<i64>() {
            if i + 1 < tokens.len() {
                let unit = tokens[i + 1].to_ascii_lowercase();
                if unit.starts_with("year") {
                    months += (n as i32) * 12;
                    saw_any = true;
                    i += 2;
                    continue;
                } else if unit.starts_with("mon") {
                    months += n as i32;
                    saw_any = true;
                    i += 2;
                    continue;
                } else if unit.starts_with("day") {
                    days += n as i32;
                    saw_any = true;
                    i += 2;
                    continue;
                }
            }
            i += 1;
        } else if tok.contains(':') {
            let neg = tok.starts_with('-');
            let clock = tok.trim_start_matches('-').trim_start_matches('+');
            let parts: Vec<&str> = clock.split(':').collect();
            if parts.len() < 2 {
                return None;
            }
            let hours: i64 = parts[0].parse().ok()?;
            let minutes: i64 = parts[1].parse().ok()?;
            let seconds_f: f64 = if parts.len() > 2 {
                parts[2].parse().ok()?
            } else {
                0.0
            };
            let whole_seconds = seconds_f.trunc() as i64;
            let frac_micros = ((seconds_f.fract()) * 1_000_000.0).round() as i64;
            let mut total = hours * 3_600_000_000
                + minutes * 60_000_000
                + whole_seconds * 1_000_000
                + frac_micros;
            if neg {
                total = -total;
            }
            microseconds += total;
            saw_any = true;
            i += 1;
        } else {
            i += 1;
        }
    }
    if !saw_any {
        return None;
    }
    Some(PgInterval {
        months,
        days,
        microseconds,
    })
}

/// Parse a Postgres array text literal (`"{a,b,c}"`, with `NULL` and
/// double-quoted/escaped elements per Postgres's array-output rules) into a
/// vector of optional element text (`None` == array element `NULL`), then
/// apply `parse_elem` to every non-`NULL` element. Returns `None` (encoded as
/// SQL `NULL` by the caller) if the outer literal is malformed or any
/// element fails to parse -- mirroring `encode_typed_field`'s existing
/// parse-failure-becomes-`NULL` behavior for every other type below.
fn parse_pg_array<T>(s: &str, parse_elem: impl Fn(&str) -> Option<T>) -> Option<Vec<Option<T>>> {
    let elems = parse_pg_array_text(s)?;
    let mut out = Vec::with_capacity(elems.len());
    for e in elems {
        match e {
            None => out.push(None),
            Some(v) => out.push(Some(parse_elem(&v)?)),
        }
    }
    Some(out)
}

/// Split a `"{...}"` array text literal into its element strings (`None` for
/// an unquoted `NULL`), honoring double-quoted elements with `\`-escapes.
fn parse_pg_array_text(s: &str) -> Option<Vec<Option<String>>> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut elems = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut was_quoted = false;
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0;
    let push_elem = |elems: &mut Vec<Option<String>>, current: &str, was_quoted: bool| {
        let trimmed = current.trim();
        if !was_quoted && trimmed.eq_ignore_ascii_case("null") {
            elems.push(None);
        } else {
            elems.push(Some(trimmed.to_string()));
        }
    };
    while i < chars.len() {
        match chars[i] {
            '"' if !in_quote => {
                in_quote = true;
                was_quoted = true;
                i += 1;
            }
            '"' if in_quote => {
                in_quote = false;
                i += 1;
            }
            '\\' if in_quote && i + 1 < chars.len() => {
                current.push(chars[i + 1]);
                i += 2;
            }
            ',' if !in_quote => {
                push_elem(&mut elems, &current, was_quoted);
                current = String::new();
                was_quoted = false;
                i += 1;
            }
            c => {
                current.push(c);
                i += 1;
            }
        }
    }
    push_elem(&mut elems, &current, was_quoted);
    Some(elems)
}

/// Encode one field's already-extracted text value (`None` == SQL `NULL`)
/// into `encoder`, using the Rust type matching `datatype` so that binary-
/// format results are correctly encoded and not just passed through as text.
fn encode_typed_field(
    encoder: &mut DataRowEncoder,
    datatype: &Type,
    val: Option<&str>,
) -> PgWireResult<()> {
    let encode_res = match *datatype {
        Type::INT2 => {
            let parsed = val.and_then(|s| s.parse::<i16>().ok());
            encoder.encode_field(&parsed)
        }
        Type::INT4 => {
            let parsed = val.and_then(|s| s.parse::<i32>().ok());
            encoder.encode_field(&parsed)
        }
        Type::INT8 => {
            let parsed = val.and_then(|s| s.parse::<i64>().ok());
            encoder.encode_field(&parsed)
        }
        Type::FLOAT4 => {
            let parsed = val.and_then(|s| s.parse::<f32>().ok());
            encoder.encode_field(&parsed)
        }
        Type::FLOAT8 => {
            let parsed = val.and_then(|s| s.parse::<f64>().ok());
            encoder.encode_field(&parsed)
        }
        Type::BOOL => {
            // An empty string is this row-store's "no value" sentinel (see
            // `parse_value_list`), used identically to a parse failure for
            // numeric/date/time types above -- unlike those, a bare
            // `.map()` over `Some("")` would produce `Some(false)` instead
            // of propagating `NULL`, so it must be filtered out first.
            let parsed = val
                .filter(|s| !s.is_empty())
                .map(|s| s == "t" || s == "true" || s == "1");
            encoder.encode_field(&parsed)
        }
        Type::TIMESTAMP => {
            let parsed = val.and_then(parse_pg_timestamp);
            encoder.encode_field(&parsed)
        }
        Type::TIMESTAMPTZ => {
            let parsed = val.and_then(parse_pg_timestamptz);
            encoder.encode_field(&parsed)
        }
        Type::DATE => {
            let parsed = val.and_then(parse_pg_date);
            encoder.encode_field(&parsed)
        }
        Type::TIME => {
            let parsed = val.and_then(parse_pg_time);
            encoder.encode_field(&parsed)
        }
        Type::UUID => {
            let parsed = val.and_then(parse_pg_uuid).map(PgUuid);
            encoder.encode_field(&parsed)
        }
        Type::NUMERIC => {
            let parsed = val.and_then(|s| s.trim().parse::<rust_decimal::Decimal>().ok());
            encoder.encode_field(&parsed)
        }
        Type::JSON | Type::JSONB => {
            let parsed = val.and_then(parse_pg_json).map(PgJson);
            encoder.encode_field(&parsed)
        }
        Type::INTERVAL => {
            let parsed = val.and_then(parse_pg_interval);
            encoder.encode_field(&parsed)
        }
        Type::INT4_ARRAY => {
            let parsed = val.and_then(|s| parse_pg_array(s, |e| e.parse::<i32>().ok()));
            encoder.encode_field(&parsed)
        }
        Type::INT8_ARRAY => {
            let parsed = val.and_then(|s| parse_pg_array(s, |e| e.parse::<i64>().ok()));
            encoder.encode_field(&parsed)
        }
        Type::TEXT_ARRAY => {
            let parsed = val.and_then(|s| parse_pg_array(s, |e| Some(e.to_string())));
            encoder.encode_field(&parsed)
        }
        Type::FLOAT8_ARRAY => {
            let parsed = val.and_then(|s| parse_pg_array(s, |e| e.parse::<f64>().ok()));
            encoder.encode_field(&parsed)
        }
        Type::BOOL_ARRAY => {
            let parsed = val.and_then(|s| {
                parse_pg_array(s, |e| {
                    if e.is_empty() {
                        None
                    } else {
                        Some(e == "t" || e == "true" || e == "1")
                    }
                })
            });
            encoder.encode_field(&parsed)
        }
        Type::UUID_ARRAY => {
            let parsed = val.and_then(|s| parse_pg_array(s, |e| parse_pg_uuid(e).map(PgUuid)));
            encoder.encode_field(&parsed)
        }
        _ => encoder.encode_field(&val),
    };
    encode_res.map_err(|e| PgWireError::ApiError(Box::new(e)))
}

#[derive(Debug, Clone)]
pub struct PortalState {
    pub rows: Vec<pgwire::messages::data::DataRow>,
    pub schema: Arc<Vec<FieldInfo>>,
    pub command_tag: String,
    pub offset: usize,
}

/// Fill-level snapshot of every per-connection state map on `GatewayHandler`
/// (v0.51.6 Slice 2). See `GatewayHandler::connection_state_totals`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionStateTotals {
    pub prepared_statement_conns: usize,
    pub active_portal_conns: usize,
    pub portal_states: usize,
    pub sessions: usize,
    pub write_buffers: usize,
    pub copy_states: usize,
}

// ── GatewayHandler ────────────────────────────────────────────────────────────

/// The complete set of readers that owns a query-time relation at one pinned
/// cluster frontier.  Supplying this dependency is the distributed gateway
/// wiring: a handler may not silently reduce a configured topology to its
/// local shard.
#[derive(Clone)]
pub struct QueryTimeShardTopology {
    readers: Vec<Arc<rockstream_storage::ShardReader>>,
    pinned_frontier: u64,
    query_time_scatter_budget: QueryTimeScatterBudget,
}

/// One durable shard source owned by the query-time topology provider.
#[derive(Clone)]
pub struct QueryTimeShardReaderSpec {
    path: String,
    object_store: Arc<dyn object_store::ObjectStore>,
}

impl QueryTimeShardReaderSpec {
    pub fn new(path: impl Into<String>, object_store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            path: path.into(),
            object_store,
        }
    }
}

/// Refreshes every configured owning shard and selects a common durable
/// frontier for each query-time execution. It deliberately fails closed when
/// even one shard is missing or ahead/behind the selected frontier.
#[derive(Clone)]
pub struct QueryTimeShardTopologyProvider {
    readers: Vec<QueryTimeShardReaderSpec>,
    query_time_scatter_budget: QueryTimeScatterBudget,
}

impl QueryTimeShardTopologyProvider {
    pub fn new(readers: Vec<QueryTimeShardReaderSpec>) -> Self {
        Self {
            readers,
            query_time_scatter_budget: QueryTimeScatterBudget::default(),
        }
    }

    pub fn with_query_time_scatter_budget(
        readers: Vec<QueryTimeShardReaderSpec>,
        query_time_scatter_budget: QueryTimeScatterBudget,
    ) -> Self {
        Self {
            readers,
            query_time_scatter_budget,
        }
    }

    pub async fn refresh(&self) -> Result<QueryTimeShardTopology, GatewayError> {
        if self.readers.is_empty() {
            return Err(GatewayError::QueryTimeScatterTopologyUnavailable);
        }
        let readers = futures::future::try_join_all(self.readers.iter().map(|reader| async {
            rockstream_storage::ShardReader::open(reader.path.clone(), reader.object_store.clone())
                .await
        }))
        .await
        .map_err(|_| GatewayError::QueryTimeScatterTopologyUnavailable)?
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let frontier_key = rockstream_storage::ShardKeyEncoder::frontier_key();
        let mut pinned_frontier = None;
        for reader in &readers {
            let frontier = match reader
                .get(&frontier_key)
                .await
                .map_err(|_| GatewayError::QueryTimeScatterTopologyUnavailable)?
            {
                Some(bytes) if bytes.len() == 8 => u64::from_be_bytes(
                    bytes[..8]
                        .try_into()
                        .map_err(|_| GatewayError::QueryTimeScatterTopologyUnavailable)?,
                ),

                Some(_) => return Err(GatewayError::QueryTimeScatterTopologyUnavailable),
                None => 0,
            };
            if let Some(expected) = pinned_frontier {
                if frontier != expected {
                    return Err(GatewayError::QueryTimeScatterFrontierMismatch {
                        shard_path: reader.path().to_string(),
                        expected,
                        actual: frontier,
                    });
                }
            } else {
                pinned_frontier = Some(frontier);
            }
        }
        Ok(QueryTimeShardTopology::with_query_time_scatter_budget(
            readers,
            pinned_frontier.unwrap_or(0),
            self.query_time_scatter_budget,
        ))
    }
}

impl QueryTimeShardTopology {
    pub fn new(readers: Vec<Arc<rockstream_storage::ShardReader>>, pinned_frontier: u64) -> Self {
        Self {
            readers,
            pinned_frontier,
            query_time_scatter_budget: QueryTimeScatterBudget::default(),
        }
    }

    /// Build a topology with an explicit per-query scatter budget.
    pub fn with_query_time_scatter_budget(
        readers: Vec<Arc<rockstream_storage::ShardReader>>,
        pinned_frontier: u64,
        query_time_scatter_budget: QueryTimeScatterBudget,
    ) -> Self {
        Self {
            readers,
            pinned_frontier,
            query_time_scatter_budget,
        }
    }

    pub fn pinned_frontier(&self) -> u64 {
        self.pinned_frontier
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    pub fn query_time_scatter_budget(&self) -> QueryTimeScatterBudget {
        self.query_time_scatter_budget
    }
}

/// Core handler shared across all pgwire protocol phases.
///
/// `Arc<GatewayHandler>` is the `PgWireServerHandlers` factory.
pub struct GatewayHandler {
    catalog: Arc<CatalogStubs>,
    view_reader: Arc<dyn ViewReader>,
    query_parser: Arc<PreparedStatementCache>,
    /// Per-connection prepared-statement name cache, bounded at
    /// `MAX_PREPARED_STATEMENTS_PER_CONN` with LRU eviction (v0.51.6 Slice 1):
    /// opening more than the bound without `DISCARD ALL`/`DEALLOCATE` evicts
    /// the least-recently-used statement instead of erroring.
    prepared_statements: Arc<DashMap<String, lru::LruCache<String, ()>>>,
    /// Per-connection portal name cache, bounded at `MAX_PORTALS_PER_CONN`
    /// with LRU eviction (v0.51.6 Slice 1), mirroring `prepared_statements`.
    active_portals: Arc<DashMap<String, lru::LruCache<String, ()>>>,
    portal_states: Arc<DashMap<(String, String), PortalState>>, // (conn_id, portal_name) -> PortalState
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
    /// Optional distributed query-time topology. A configured topology is
    /// always used in full; it is never reduced to the gateway-local shard.
    query_time_shard_topology: Option<Arc<QueryTimeShardTopology>>,
    /// Dynamic production provider. Unlike a test topology, this refreshes
    /// all configured owning readers and validates their shared frontier on
    /// every query-time request.
    query_time_shard_topology_provider: Option<Arc<QueryTimeShardTopologyProvider>>,
    /// Authentication mode for this gateway instance.
    auth_mode: AuthMode,
    /// Optional JWT verifier (populated when auth_mode == Oidc).
    jwt_verifier: Option<Arc<JwtVerifier>>,
    /// Role catalog for SCRAM/MD5 auth (always present; empty for Off/Oidc/Mtls).
    pub role_catalog: Arc<RoleCatalog>,
    /// ACL store for RBAC enforcement.
    pub acl_store: Arc<rockstream_control::AclStore>,
    /// Namespace catalog.
    namespace_catalog: Arc<rockstream_control::NamespaceCatalog>,
    audit_log: Option<Arc<rockstream_control::audit::FileAuditLog>>,
    /// CancelRequest registry: (backend_pid, cancel_secret) -> CancelToken.
    /// Bound: MAX_CONNECTIONS = 10_000 entries.
    /// Fill-level metric: registry.len().
    pub cancellation_registry: Arc<DashMap<(u32, u32), CancelToken>>,
    /// LISTEN/NOTIFY channel registry. Bound: MAX_NOTIFY_CHANNELS = 1_000 channels.
    pub notify_registry: Arc<NotifyRegistry>,
    /// Transactional NOTIFYs buffered until COMMIT. Bound: MAX_OUTBOX_PER_CONNECTION.
    pending_notifies: Arc<DashMap<String, Vec<(String, String)>>>,
    /// CREATE TABLE-side metadata needed for server-assigned INSERT values.
    table_insert_metadata: Arc<DashMap<String, Arc<TableInsertMetadata>>>,
    /// Wall-clock publish timestamp of the most recently advanced shard frontier.
    frontier_published_at_ms: Arc<AtomicU64>,
    /// Compiled `PlanNode → Operator` pipelines for views registered through
    /// the "one data plane" fast path (v0.51.3 Slice 3/4, extended through
    /// v0.51.4 Slices 1-7 to every Nexmark operator family), keyed by view
    /// name. Populated by `handle_create_view` when the view's SELECT
    /// compiles via `rockstream_ops::compile_plan`; a shard-backed gateway
    /// (`--role all`) rejects `CREATE VIEW`/`CREATE MATERIALIZED VIEW`
    /// outright (`RS-1019`) when compilation fails — there is no
    /// materializer fallback left (v0.51.4 Slice 8).
    compiled_views: Arc<DashMap<String, Arc<rockstream_ops::CompiledView>>>,
    /// Runtime-only webhook credentials and bounded epoch buffers.  Entries
    /// are installed and removed with the catalog source lifecycle.
    // Audit: each guard protects a synchronous source-state transition that
    // remains valid after a holder panic; guards are dropped before awaits.
    webhook_sources: Arc<DashMap<String, Arc<Mutex<HttpWebhookSource>>>>,
    backfill_admission: Arc<crate::admission::BackfillAdmissionController>,
    /// Bound only by `GatewayServer`; source tasks upgrade it per poll and
    /// exit when the server releases its handler.
    self_ref: Arc<Mutex<Weak<GatewayHandler>>>,
    source_workers: Arc<DashMap<String, ()>>,
    pgoutput_coordinators:
        Arc<DashMap<ConnectorId, Arc<tokio::sync::Mutex<SharedPgOutputCoordinator>>>>,
    /// ponytail: source creation is rare; replace this global lock with
    /// per-identity locks only after measured contention.
    pgoutput_registry_lock: Arc<tokio::sync::Mutex<()>>,
    /// ponytail: shard-wide serialization is the correctness ceiling; split
    /// by dependency component only after measured contention.
    shard_commit_lock: Arc<tokio::sync::Mutex<()>>,
}

impl GatewayHandler {
    fn bind_server(&self, handler: &Arc<Self>) {
        *self.self_ref.lock() = Arc::downgrade(handler);
    }

    /// Fill-level snapshot of every per-connection state map (v0.51.6
    /// Slice 2). Used to prove abnormal-disconnect cleanup (raw TCP kill,
    /// not graceful `Terminate`/`DISCARD ALL`) removes all per-connection
    /// state rather than leaking it — none of these DashMaps previously had
    /// an observable fill-level metric of their own.
    pub fn connection_state_totals(&self) -> ConnectionStateTotals {
        ConnectionStateTotals {
            prepared_statement_conns: self.prepared_statements.len(),
            active_portal_conns: self.active_portals.len(),
            portal_states: self.portal_states.len(),
            sessions: self.sessions.len(),
            write_buffers: self.write_buffers.len(),
            copy_states: self.copy_states.len(),
        }
    }

    async fn accept_webhook(
        &self,
        source_name: &str,
        token: &[u8],
        delivery_id: Option<&str>,
        payload: &[u8],
    ) -> WebhookResult {
        let Some(source_entry) = self.catalog.get_source(source_name) else {
            return WebhookResult::NotFound;
        };
        if source_entry.source_type != "http_webhook" {
            return WebhookResult::NotFound;
        }
        let Some(source) = self
            .webhook_sources
            .get(source_name)
            .map(|source| source.value().clone())
        else {
            return WebhookResult::NotFound;
        };
        let (result, pending) = {
            let mut source = source.lock();
            let result = source.accept(token, delivery_id, payload);
            let pending = if result == WebhookResult::Accepted {
                source.next_pending()
            } else {
                None
            };
            (result, pending)
        };
        if let Some(pending) = pending {
            if let Some(shard_db) = &self.shard_db {
                let key = format!(
                    "source_input/{source_name}/epoch/{:020}",
                    pending.source_epoch
                );
                let payload = match serde_json::to_vec(&pending) {
                    Ok(payload) => payload,
                    Err(_) => {
                        source.lock().abort_pending(&pending.delivery_id);
                        return WebhookResult::DurabilityFailed;
                    }
                };
                let mut batch = rockstream_storage::WriteBatch::new();
                batch.put(key.as_bytes(), &payload);
                if shard_db.write_batch(batch).await.is_err() || shard_db.flush().await.is_err() {
                    source.lock().abort_pending(&pending.delivery_id);
                    return WebhookResult::DurabilityFailed;
                }
            }

            // The success response is emitted only after the M3 source-input
            // transaction commits. A gateway without an attached ShardDb is
            // the in-memory test/control-plane mode and retains its bounded
            // local acknowledgement semantics.
            let mut source = source.lock();
            let Some(committed) = source.commit_pending(&pending.delivery_id) else {
                // Delivery ID was not found in the accepted queue — return
                // DurabilityFailed rather than panicking. This can occur
                // if a concurrent abort already removed the entry (RS-4017).
                return WebhookResult::DurabilityFailed;
            };
            self.catalog.update_source_runtime_detail(
                source_name,
                Some("gateway:webhook".to_string()),
                Some(committed.source_epoch),
                committed.digest,
                source.buffered_epochs() as u64,
                Some(source.buffered_epochs()),
                None,
            );
        }
        result
    }

    pub fn new(catalog: Arc<CatalogStubs>, view_reader: Arc<dyn ViewReader>) -> Self {
        GatewayHandler {
            catalog: catalog.clone(),
            view_reader,
            query_parser: Arc::new(PreparedStatementCache { catalog }),
            prepared_statements: Arc::new(DashMap::new()),
            active_portals: Arc::new(DashMap::new()),
            portal_states: Arc::new(DashMap::new()),
            write_buffers: Arc::new(DashMap::new()),
            copy_states: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            shard_db: None,
            query_time_shard_topology: None,
            query_time_shard_topology_provider: None,
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            role_catalog: Arc::new(RoleCatalog::new()),
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
            cancellation_registry: Arc::new(DashMap::new()),
            notify_registry: Arc::new(NotifyRegistry::new()),
            pending_notifies: Arc::new(DashMap::new()),
            table_insert_metadata: Arc::new(DashMap::new()),
            frontier_published_at_ms: Arc::new(AtomicU64::new(current_time_ms())),
            compiled_views: Arc::new(DashMap::new()),
            webhook_sources: Arc::new(DashMap::new()),
            backfill_admission: Arc::new(crate::admission::BackfillAdmissionController::default()),
            self_ref: Arc::new(Mutex::new(Weak::new())),
            source_workers: Arc::new(DashMap::new()),
            pgoutput_coordinators: Arc::new(DashMap::new()),
            pgoutput_registry_lock: Arc::new(tokio::sync::Mutex::new(())),
            shard_commit_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn with_shard_db(
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        GatewayHandler {
            catalog: catalog.clone(),
            view_reader,
            query_parser: Arc::new(PreparedStatementCache { catalog }),
            prepared_statements: Arc::new(DashMap::new()),
            active_portals: Arc::new(DashMap::new()),
            portal_states: Arc::new(DashMap::new()),
            write_buffers: Arc::new(DashMap::new()),
            copy_states: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            shard_db: Some(shard_db),
            query_time_shard_topology: None,
            query_time_shard_topology_provider: None,
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            role_catalog: Arc::new(RoleCatalog::new()),
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
            cancellation_registry: Arc::new(DashMap::new()),
            notify_registry: Arc::new(NotifyRegistry::new()),
            pending_notifies: Arc::new(DashMap::new()),
            table_insert_metadata: Arc::new(DashMap::new()),
            frontier_published_at_ms: Arc::new(AtomicU64::new(current_time_ms())),
            compiled_views: Arc::new(DashMap::new()),
            webhook_sources: Arc::new(DashMap::new()),
            backfill_admission: Arc::new(crate::admission::BackfillAdmissionController::default()),
            self_ref: Arc::new(Mutex::new(Weak::new())),
            source_workers: Arc::new(DashMap::new()),
            pgoutput_coordinators: Arc::new(DashMap::new()),
            pgoutput_registry_lock: Arc::new(tokio::sync::Mutex::new(())),
            shard_commit_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Inject the complete pinned shard-reader topology used by query-time
    /// DataFusion reads. Production constructors retain this dependency and
    /// auth wrappers operate on the same handler instance.
    pub fn with_query_time_shard_topology(mut self, topology: QueryTimeShardTopology) -> Self {
        self.query_time_shard_topology = Some(Arc::new(topology));
        self
    }

    pub fn with_query_time_shard_topology_provider(
        mut self,
        provider: QueryTimeShardTopologyProvider,
    ) -> Self {
        self.query_time_shard_topology_provider = Some(Arc::new(provider));
        self
    }

    #[doc(hidden)]
    pub fn set_frontier_published_at_ms_for_test(&self, timestamp_ms: u64) {
        self.frontier_published_at_ms
            .store(timestamp_ms, Ordering::SeqCst);
    }

    /// Whether a compiled operator pipeline (v0.51.3 Slice 4) has been
    /// registered for `view_name`. Used by tests to confirm `CREATE VIEW`
    /// routed through the direct operator compiler.
    #[doc(hidden)]
    pub fn has_compiled_view(&self, view_name: &str) -> bool {
        self.compiled_views.contains_key(view_name)
    }

    fn build_explain_frontend(&self) -> Result<SqlFrontend, PgWireError> {
        let frontend = SqlFrontend::new();
        for view in self.catalog.list_views() {
            frontend
                .register_table(&view.name, catalog_columns_to_schema(&view.columns))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        for table in self.catalog.list_tables() {
            frontend
                .register_table(&table.name, catalog_columns_to_schema(&table.columns))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        Ok(frontend)
    }

    /// v0.51.3 Slice 4: try to compile `select_sql` (a view's body) directly
    /// into an executable `Source → [Filter] → [Project|Map] → ViewSink`
    /// operator chain via `rockstream_ops::compile_plan`.
    ///
    /// Registers all known tables/views as DataFusion schemas (reusing
    /// `build_explain_frontend`'s pattern) so the SQL parses, lowers to a
    /// `PlanNode` via `SqlFrontend::sql_to_plan_node`, then compiles that
    /// plan. Returns an error (not a panic) for any SQL that fails to parse
    /// or lower, or any `PlanNode` shape `compile_plan` doesn't support —
    /// callers treat this as a normal "not eligible for the fast path" case.
    async fn try_compile_view(
        &self,
        view_name: &str,
        select_sql: &str,
        output_column_count: usize,
        table_schemas: &HashMap<String, SchemaRef>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Result<rockstream_ops::CompiledView, String> {
        let frontend = self
            .build_explain_frontend()
            .map_err(|e| format!("[RS-1019] frontend setup failed: {e:?}; next_steps: simplify the view query or verify its source schemas"))?;

        let view_plan = rockstream_plan::PlanNode::ViewSink {
            view_name: view_name.to_string(),
            pk: full_row_pk(output_column_count),
            child: Box::new(
                frontend
                    .sql_to_plan_node(select_sql)
                    .await
                    .map_err(|e| format!("sql_to_plan_node: {e}"))?,
            ),
        };

        match rockstream_ops::compile_plan(&view_plan, shard_db, table_schemas) {
            Ok(compiled) => Ok(compiled),
            Err(compile_err) => {
                let mut diff_ctx = rockstream_diff::DiffCtx::new();
                let _physical_plan = diff_ctx
                    .differentiate(&view_plan)
                    .map_err(|e| format!("[RS-1019] DiffCtx physical plan lowering failed: {e:?}; next_steps: simplify the view query or verify its source schemas"))?;
                Err(format!("[RS-1019] compile_plan: {compile_err}; next_steps: simplify the view query or verify its source schemas"))
            }
        }
    }

    /// Same as `try_compile_view`, but reuses `sink_op_id` instead of
    /// minting a fresh one — see `recover_compiled_views`.
    async fn try_compile_view_with_sink_id(
        &self,
        view_name: &str,
        select_sql: &str,
        output_column_count: usize,
        table_schemas: &HashMap<String, SchemaRef>,
        shard_db: Arc<rockstream_storage::ShardDb>,
        sink_op_id: OperatorId,
    ) -> Result<rockstream_ops::CompiledView, String> {
        let frontend = self
            .build_explain_frontend()
            .map_err(|e| format!("[RS-1019] frontend setup failed: {e:?}; next_steps: simplify the view query or verify its source schemas"))?;

        let view_plan = rockstream_plan::PlanNode::ViewSink {
            view_name: view_name.to_string(),
            pk: full_row_pk(output_column_count),
            child: Box::new(
                frontend
                    .sql_to_plan_node(select_sql)
                    .await
                    .map_err(|e| format!("sql_to_plan_node: {e}"))?,
            ),
        };

        match rockstream_ops::compile_plan_with_sink_id(
            &view_plan,
            shard_db,
            table_schemas,
            sink_op_id,
        ) {
            Ok(compiled) => Ok(compiled),
            Err(compile_err) => {
                let mut diff_ctx = rockstream_diff::DiffCtx::new();
                let _physical_plan = diff_ctx
                    .differentiate(&view_plan)
                    .map_err(|e| format!("[RS-1019] DiffCtx physical plan lowering failed: {e:?}; next_steps: simplify the view query or verify its source schemas"))?;
                Err(format!("[RS-1019] compile_plan_with_sink_id: {compile_err}; next_steps: simplify the view query or verify its source schemas"))
            }
        }
    }

    /// Recompile every catalog-registered view that already carries a
    /// durable `op_id` back into this handler's local `compiled_views`
    /// cache. `compiled_views` (unlike `CatalogView.op_id`) is process-local
    /// state, populated only by `handle_create_view` — without this step, a
    /// fresh gateway process (after a restart) would still report a view as
    /// compiled (`op_id: Some(_)` survives via the catalog) but silently
    /// stop applying live incremental commits to it, since
    /// `reachable_compiled_views` only considers views present in this
    /// (empty, for a fresh process) map. Reuses the view's exact pre-restart
    /// `sink_op_id` (`try_compile_view_with_sink_id`) and reproduces its
    /// exact pre-restart internal stage ids (`compile_plan`'s
    /// `with_view_id_scope`, seeded from the view name) so the recompiled
    /// pipeline addresses the same storage keys the prior process wrote to.
    /// A no-op for a gateway with no local `shard_db` (multi-shard
    /// `--role gateway`, which never populates `compiled_views` at all).
    pub async fn recover_compiled_views(&self) {
        let Some(shard_db) = self.shard_db.clone() else {
            return;
        };
        for view in self.catalog.list_views() {
            let Some(op_id) = view.op_id else {
                continue;
            };
            if self.compiled_views.contains_key(&view.name) {
                continue;
            }
            let inlined_sql =
                inline_view_dependencies(&view.sql, &self.catalog, MAX_VIEW_INLINE_DEPTH);
            let compile_deps = extract_sql_refs(&inlined_sql);
            let deps_are_base_tables = compile_deps
                .iter()
                .all(|dep| self.catalog.get_table(dep).is_some());
            if !deps_are_base_tables {
                continue;
            }
            let table_schemas: HashMap<String, SchemaRef> = compile_deps
                .iter()
                .map(|dep| (dep.clone(), query_time_relation_schema(&self.catalog, dep)))
                .collect();
            match self
                .try_compile_view_with_sink_id(
                    &view.name,
                    &inlined_sql,
                    view.columns.len(),
                    &table_schemas,
                    shard_db.clone(),
                    OperatorId(op_id),
                )
                .await
            {
                Ok(compiled) => {
                    if let Some(join) = &compiled.join {
                        if let Err(e) = join.pipeline.restore(&shard_db).await {
                            tracing::warn!(
                                view = %view.name,
                                error = %e,
                                "recover_compiled_views: failed to restore persisted join \
                                 arrangement state; recompiled pipeline will start from empty \
                                 state"
                            );
                        }
                    } else if let Err(e) = compiled.pipeline.restore(&shard_db).await {
                        tracing::warn!(
                            view = %view.name,
                            error = %e,
                            "recover_compiled_views: failed to restore persisted pipeline \
                             arrangement state; recompiled pipeline will start from empty state"
                        );
                    }
                    self.compiled_views
                        .insert(view.name.clone(), Arc::new(compiled));
                    let sources = compile_deps
                        .iter()
                        .filter_map(|relation| {
                            self.catalog.get_source(relation).filter(|source| {
                                source.table_name.as_deref() == Some(relation.as_str())
                            })
                        })
                        .collect::<Vec<_>>();
                    if !sources.is_empty() {
                        let mut estimated_rows = 0;
                        let mut all_running = true;
                        for source in &sources {
                            let connector_id = source_checkpoint_connector_id(source, &view.name);
                            let lifecycle = SourceCheckpointStore::new(
                                Arc::clone(&shard_db),
                                connector_id.0 as u128,
                                connector_id,
                            )
                            .backfill_lifecycle(&view.name)
                            .await;
                            match lifecycle {
                                Ok(Some(lifecycle)) => {
                                    estimated_rows += lifecycle.estimated_rows;
                                    all_running &= lifecycle.phase == BackfillPhase::Running;
                                }
                                Ok(None) => all_running = false,
                                Err(error) => {
                                    tracing::warn!(
                                        view = %view.name,
                                        error = %error,
                                        "recover_compiled_views: failed to load source backfill lifecycle"
                                    );
                                    all_running = false;
                                }
                            }
                        }
                        self.catalog.begin_backfill(&view.name, estimated_rows);
                        if all_running {
                            self.refresh_backfill_progress(&view.name).await;
                            self.catalog.publish_backfill(&view.name);
                            for source in sources {
                                match source.source_type.as_str() {
                                    "s3" => self.spawn_s3_source_worker(
                                        source,
                                        view.name.clone(),
                                        Arc::clone(&shard_db),
                                    ),
                                    "kafka" => self.spawn_kafka_source_worker(
                                        source,
                                        view.name.clone(),
                                        Arc::clone(&shard_db),
                                    ),
                                    "postgres_cdc" => self.spawn_postgres_cdc_source_worker(
                                        source,
                                        view.name.clone(),
                                        Arc::clone(&shard_db),
                                    ),
                                    _ => {}
                                }
                            }
                        } else if let Err(error) = self
                            .backfill_source_view(&sources, &view.name, &shard_db)
                            .await
                        {
                            tracing::warn!(
                                view = %view.name,
                                error = %error,
                                "recover_compiled_views: source backfill remains unpublished"
                            );
                        }
                    }
                    tracing::info!(
                        view = %view.name,
                        op_id,
                        "recover_compiled_views: recompiled view pipeline after restart"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        view = %view.name,
                        error = %e,
                        "recover_compiled_views: failed to recompile a previously-compiled \
                         view; it will not receive live incremental refresh until CREATE VIEW \
                         is re-issued"
                    );
                }
            }
        }
    }

    fn compiled_view_pk(&self, view_name: &str, column_count: usize) -> Vec<usize> {
        self.compiled_views
            .get(view_name)
            .map(|view| view.pk.clone())
            .unwrap_or_else(|| {
                if column_count == 0 {
                    Vec::new()
                } else {
                    full_row_pk(column_count)
                }
            })
    }

    async fn read_compiled_view_rows(
        &self,
        view_name: &str,
        view: &crate::catalog_stubs::CatalogView,
        shard_db: &rockstream_storage::ShardDb,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        let Some(op_id) = view.op_id else {
            return Ok(Vec::new());
        };
        let stored =
            rockstream_ops::read_view_output(shard_db, OperatorId(op_id), view.columns.len())
                .await
                .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("read_view_output({view_name}): {e}"),
                })?;
        let state = materialize_view_state(
            stored,
            &self.compiled_view_pk(view_name, view.columns.len()),
        );
        Ok(state
            .into_values()
            .flat_map(|(row, count)| {
                std::iter::repeat_with({
                    let row = row.clone();
                    move || column_values_to_tsv_bytes(&row)
                })
                .take(count.max(0) as usize)
            })
            .collect())
    }

    async fn scan_relation_rows_bounded(
        &self,
        relation_name: &str,
        shard_db: &rockstream_storage::ShardDb,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        let prefix = format!("view_output/{relation_name}/");
        let (rows, truncated) = shard_db
            .scan_prefix_bounded(prefix.as_bytes(), MAX_COMPILED_VIEW_SOURCE_SCAN_BYTES)
            .await?;
        if truncated || rows.len() > MAX_COMPILED_VIEW_SOURCE_ROWS {
            return Err(GatewayError::QueryTimeResultSetTooLarge {
                relation: relation_name.to_string(),
                row_limit: MAX_COMPILED_VIEW_SOURCE_ROWS,
            });
        }
        Ok(rows
            .into_iter()
            .map(|(_, value)| value.to_vec())
            .collect::<Vec<_>>())
    }

    /// Scan `relation_name`'s full current contents and wrap them as a
    /// weight-`1` `ArrowZSet` — used only for a compiled view's one-time
    /// initial backfill (`populate_compiled_view_from_scratch`), never for
    /// per-commit refresh (Slice 0's whole point is that per-commit refresh
    /// never does this).
    async fn full_table_zset(
        &self,
        relation_name: &str,
        shard_db: &rockstream_storage::ShardDb,
    ) -> Result<ArrowZSet, GatewayError> {
        let rows = self
            .scan_relation_rows_bounded(relation_name, shard_db)
            .await?;
        let schema = query_time_relation_schema(&self.catalog, relation_name);
        let batch = tsv_to_record_batch(schema.clone(), &rows)
            .unwrap_or_else(|_| RecordBatch::new_empty(schema));
        let weights = vec![1; batch.num_rows()];
        Ok(ArrowZSet::new(batch, weights))
    }

    fn reachable_compiled_views(&self, changed_relations: &HashSet<String>) -> Vec<String> {
        let mut reachable = changed_relations.clone();
        loop {
            let mut progressed = false;
            for view in self.catalog.list_views() {
                let deps = self.catalog.get_view_deps(&view.name);
                if deps.iter().any(|dep| reachable.contains(dep)) && reachable.insert(view.name) {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        let candidates = reachable
            .iter()
            .filter(|name| self.compiled_views.contains_key(*name))
            .cloned()
            .collect::<HashSet<_>>();
        let mut indegree = BTreeMap::new();
        let mut dependents = HashMap::<String, Vec<String>>::new();
        for view in &candidates {
            let dependencies = self.catalog.get_view_deps(view);
            indegree.insert(
                view.clone(),
                dependencies
                    .iter()
                    .filter(|dependency| candidates.contains(*dependency))
                    .count(),
            );
            for dependency in dependencies {
                if candidates.contains(&dependency) {
                    dependents.entry(dependency).or_default().push(view.clone());
                }
            }
        }
        let mut ready = indegree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(view, _)| view.clone())
            .collect::<BTreeSet<_>>();
        let mut ordered = Vec::with_capacity(candidates.len());
        while let Some(view) = ready.iter().next().cloned() {
            ready.remove(&view);
            ordered.push(view.clone());
            for dependent in dependents.get(&view).into_iter().flatten() {
                if let Some(degree) = indegree.get_mut(dependent) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.insert(dependent.clone());
                    }
                }
            }
        }
        ordered
    }

    /// v0.51.4 Slice 0: refresh a compiled view from the commit's own
    /// row-level delta rather than a full-table rescan.
    ///
    /// `ops` is the commit's drained `WriteBatch` DML ops (already known to
    /// `handle_commit` — no extra read against storage). Only the ops
    /// touching this view's source table are turned into a signed-weight
    /// `ArrowZSet` (`+1` insert, `-1` delete, paired `-1`/`+1` update) and
    /// fed through the compiled pipeline; since the stateless operators
    /// (`FilterOp`/`ProjectOp`/`MapOp`) are linear (DBSP linear-operator
    /// rule), the pipeline's output on a true delta *is* the output delta —
    /// no old/new snapshot diff is needed, unlike the retired full-rescan
    /// path.
    async fn recompute_compiled_view(
        &self,
        view_name: &str,
        ops: &[DmlOp],
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let Some(compiled) = self.compiled_views.get(view_name) else {
            return Ok(());
        };

        // v0.51.4 Slice 3: a join-shaped view has two independent delta
        // sources (its left and right source tables) instead of one. Build
        // each side's delta from this commit's own ops (either or both may
        // be empty — `JoinPipeline::process` handles that correctly, same
        // as `JoinOp`/`OuterJoinOp`'s underlying bilinear-rule semantics).
        if let Some(join) = &compiled.join {
            let left_schema = query_time_relation_schema(&self.catalog, &join.left_source);
            let right_schema = query_time_relation_schema(&self.catalog, &join.right_source);
            let left_delta = build_delta_zset_for_table(&join.left_source, ops, left_schema)?;
            let right_delta = build_delta_zset_for_table(&join.right_source, ops, right_schema)?;
            if left_delta.is_empty() && right_delta.is_empty() {
                return Ok(());
            }
            COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.fetch_add(
                (left_delta.num_rows() + right_delta.num_rows()) as u64,
                Ordering::Relaxed,
            );
            let output = join
                .pipeline
                .process(left_delta, right_delta)
                .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled join pipeline process({view_name}): {e}"),
                })?;
            if !output.is_empty() {
                compiled.sink.write_next_epoch(&output).await.map_err(|e| {
                    GatewayError::QueryTimeExecutionFailed {
                        detail: format!("write compiled view_output({view_name}): {e}"),
                    }
                })?;
            }
            join.pipeline
                .persist(shard_db.as_ref())
                .await
                .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("persist compiled join pipeline state({view_name}): {e}"),
                })?;
            return Ok(());
        }

        let deps = self.catalog.get_view_deps(view_name);
        if deps.is_empty() {
            return Ok(());
        }
        let source_name = deps[0].clone();
        let source_schema = query_time_relation_schema(&self.catalog, &source_name);
        let input = build_delta_zset_for_table(&source_name, ops, source_schema)?;
        if input.is_empty() {
            return Ok(());
        }
        COMMIT_VIEW_REFRESH_DELTA_ROWS_TOTAL.fetch_add(input.num_rows() as u64, Ordering::Relaxed);
        let output = compiled.pipeline.process(input).map_err(|e| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("compiled pipeline process({view_name}): {e}"),
            }
        })?;
        if !output.is_empty() {
            compiled.sink.write_next_epoch(&output).await.map_err(|e| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("write compiled view_output({view_name}): {e}"),
                }
            })?;
        }
        // Persist any stateful stage's arrangement (Aggregate/Distinct/
        // TumbleWindow/HopWindow/Window/TopK — v0.51.4 Slices 1-5). A no-op
        // for a stateless-only pipeline (Slice 0's q0-q2 shapes).
        compiled
            .pipeline
            .persist(shard_db.as_ref())
            .await
            .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                detail: format!("persist compiled pipeline state({view_name}): {e}"),
            })?;
        Ok(())
    }

    /// One-time initial backfill for a freshly-created compiled view
    /// (materialized *or* plain `CREATE VIEW` — both need to reflect
    /// already-committed source-table data immediately, see call site): the
    /// view has no rows yet, so (unlike per-commit refresh, Slice 0) there
    /// is no smaller row-level delta to consume — the whole source table's
    /// current contents *are* the initial delta. This is the one
    /// legitimate remaining use of a full-table scan (bounded by
    /// `MAX_COMPILED_VIEW_SOURCE_ROWS`/`MAX_COMPILED_VIEW_SOURCE_SCAN_BYTES`, same as
    /// query-time reads); it runs exactly once per view creation, not once
    /// per commit.
    async fn populate_compiled_view_from_scratch(
        &self,
        view_name: &str,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let Some(compiled) = self.compiled_views.get(view_name) else {
            return Ok(());
        };

        // v0.51.4 Slice 3: a join-shaped view's initial backfill scans both
        // source tables' current contents (weight 1 per row) and feeds them
        // through the join in one shot.
        if let Some(join) = &compiled.join {
            let left = self
                .full_table_zset(&join.left_source, shard_db.as_ref())
                .await?;
            let right = self
                .full_table_zset(&join.right_source, shard_db.as_ref())
                .await?;
            if left.is_empty() && right.is_empty() {
                return Ok(());
            }
            let output = join.pipeline.process(left, right).map_err(|e| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled join pipeline process({view_name}): {e}"),
                }
            })?;
            if !output.is_empty() {
                compiled.sink.write_next_epoch(&output).await.map_err(|e| {
                    GatewayError::QueryTimeExecutionFailed {
                        detail: format!("write compiled view_output({view_name}): {e}"),
                    }
                })?;
            }
            join.pipeline
                .persist(shard_db.as_ref())
                .await
                .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("persist compiled join pipeline state({view_name}): {e}"),
                })?;
            return Ok(());
        }

        let deps = self.catalog.get_view_deps(view_name);
        if deps.is_empty() {
            return Ok(());
        }
        let source_name = deps[0].clone();
        let input = self
            .full_table_zset(&source_name, shard_db.as_ref())
            .await?;
        if input.is_empty() {
            return Ok(());
        }
        let output = compiled.pipeline.process(input).map_err(|e| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("compiled pipeline process({view_name}): {e}"),
            }
        })?;
        if !output.is_empty() {
            compiled.sink.write_next_epoch(&output).await.map_err(|e| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("write compiled view_output({view_name}): {e}"),
                }
            })?;
        }
        compiled
            .pipeline
            .persist(shard_db.as_ref())
            .await
            .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                detail: format!("persist compiled pipeline state({view_name}): {e}"),
            })?;
        Ok(())
    }

    /// Drive one source through snapshot, bounded catch-up, then publication.
    /// Every accepted batch uses `commit_backfill_epoch`, so table input, view
    /// output, operator state, checkpoint, cursor, lifecycle, and frontier
    /// share one M3 write.
    async fn backfill_bound_source<S: SourceConnector>(
        &self,
        source_name: &str,
        view_name: &str,
        mut runtime: SourceRuntimeCoordinator<S>,
        publish: bool,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let table = self.catalog.source_table(source_name).ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("source '{source_name}' is not bound to a table"),
            }
        })?;
        runtime.recover().await.map_err(source_backfill_error)?;
        let lease = runtime
            .acquire_owner(format!("gateway:{view_name}"))
            .map_err(source_backfill_error)?;
        let recovered = runtime
            .backfill_lifecycle(view_name)
            .await
            .map_err(source_backfill_error)?;
        if recovered
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.phase == BackfillPhase::Running)
        {
            if publish {
                self.catalog.publish_backfill(view_name);
            }
            return Ok(());
        }
        let fence = recovered
            .as_ref()
            .map(|lifecycle| lifecycle.cursor.fence.clone())
            .unwrap_or(
                runtime
                    .capture_snapshot_delta_fence()
                    .await
                    .map_err(source_backfill_error)?,
            );
        if recovered.is_none() {
            runtime
                .persist_backfill_intent(&BackfillLifecycle::new(
                    BackfillPhase::Snapshotting,
                    BackfillCursor::new(view_name, 0, Vec::new(), fence.clone(), 0),
                    0,
                    0,
                    0,
                    None,
                ))
                .await
                .map_err(source_backfill_error)?;
        }
        let mut snapshot_after = recovered.as_ref().and_then(|lifecycle| {
            (lifecycle.phase == BackfillPhase::Snapshotting)
                .then(|| OffsetToken::new(lifecycle.cursor.last_key.clone()))
        });
        let snapshot = if recovered
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.phase == BackfillPhase::CatchingUp)
        {
            rockstream_connectors::SnapshotStream::new(Vec::new())
        } else {
            runtime
                .start_snapshot(&fence, snapshot_after.clone(), BACKFILL_BATCH_MAX_ROWS)
                .await
                .map_err(source_backfill_error)?
        };
        let estimated_rows = recovered
            .as_ref()
            .map(|lifecycle| lifecycle.estimated_rows)
            .filter(|estimated_rows| *estimated_rows > 0)
            .unwrap_or(snapshot.remaining_rows() as u64);
        let mut snapshot_rows_remaining = snapshot.remaining_rows() as u64;
        if recovered.is_none() {
            self.catalog.begin_backfill(view_name, estimated_rows);
        }
        let mut live_offset = recovered
            .as_ref()
            .filter(|lifecycle| lifecycle.phase == BackfillPhase::CatchingUp)
            .map(|lifecycle| OffsetToken::new(lifecycle.cursor.last_key.clone()))
            .unwrap_or_else(|| fence.live.clone());

        let mut snapshot = snapshot;
        loop {
            let mut committed_chunk = false;
            for chunk in snapshot {
                snapshot_rows_remaining =
                    snapshot_rows_remaining.saturating_sub(chunk.batch.num_rows() as u64);
                snapshot_after = Some(chunk.resume_offset.clone());
                self.commit_bound_source_batch(
                    &mut runtime,
                    &lease,
                    view_name,
                    &table,
                    &fence,
                    chunk.resume_offset,
                    &chunk.batch,
                    BackfillPhase::Snapshotting,
                    None,
                    snapshot_rows_remaining,
                    estimated_rows,
                    shard_db,
                )
                .await?;
                committed_chunk = true;
            }
            if !committed_chunk
                || snapshot_after
                    .as_ref()
                    .is_some_and(|cursor| cursor == &fence.snapshot)
            {
                break;
            }
            snapshot = runtime
                .start_snapshot(&fence, snapshot_after.clone(), BACKFILL_BATCH_MAX_ROWS)
                .await
                .map_err(source_backfill_error)?;
            snapshot_rows_remaining = snapshot.remaining_rows() as u64;
        }
        self.catalog.catch_up_backfill(view_name, None);

        loop {
            let delta = runtime
                .poll_delta_after(
                    live_offset.clone(),
                    BACKFILL_LIVE_DELTA_MAX_BYTES,
                    BACKFILL_BATCH_MAX_ROWS,
                )
                .await
                .map_err(source_backfill_error)?;
            if delta.batches.is_empty() {
                break;
            }
            let delta_bytes = delta
                .batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            if delta_bytes > BACKFILL_LIVE_DELTA_MAX_BYTES {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "[RS-4020] backfill.live_delta_buffer_full: live delta buffer is {delta_bytes} bytes, above BACKFILL_LIVE_DELTA_MAX_BYTES={BACKFILL_LIVE_DELTA_MAX_BYTES}"
                    ),
                });
            }
            let mut combined = Vec::new();
            for batch in &delta.batches {
                combined.extend(
                    source_batch_to_dml_ops(&table.name, &table.columns, batch)
                        .map_err(|detail| GatewayError::QueryTimeExecutionFailed { detail })?,
                );
            }
            self.commit_bound_source_ops(
                &mut runtime,
                &lease,
                view_name,
                &table,
                &fence,
                delta.new_offset.clone(),
                combined,
                BackfillPhase::CatchingUp,
                None,
                0,
                estimated_rows,
                shard_db,
            )
            .await?;
            live_offset = delta.new_offset;
        }

        let epoch = runtime.next_epoch().map_err(source_backfill_error)?;
        self.commit_bound_source_ops(
            &mut runtime,
            &lease,
            view_name,
            &table,
            &fence,
            live_offset,
            Vec::new(),
            BackfillPhase::Running,
            Some(epoch),
            0,
            estimated_rows,
            shard_db,
        )
        .await?;
        if publish {
            self.catalog.publish_backfill(view_name);
        }
        Ok(())
    }

    fn build_s3_source(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
    ) -> Result<(CatalogTable, ConnectorId, S3Source), GatewayError> {
        let table = self.catalog.source_table(&source.name).ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("source '{}' is not bound to a table", source.name),
            }
        })?;
        let bucket =
            source
                .options
                .get("bucket")
                .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("S3 source '{}' requires bucket", source.name),
                })?;
        let connector_id = source_view_connector_id(&source.name, view_name);
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(
                source
                    .options
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or("us-east-1"),
            );
        if let Some(endpoint) = source.options.get("endpoint") {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        if let Some(access_key) = source.options.get("access_key") {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = source.options.get("secret_key") {
            builder = builder.with_secret_access_key(secret_key);
        }
        let object_store =
            Arc::new(
                builder
                    .build()
                    .map_err(|error| GatewayError::QueryTimeExecutionFailed {
                        detail: format!("build S3 source '{}': {error}", source.name),
                    })?,
            );
        let runtime = S3Source::new(connector_id, catalog_columns_to_schema(&table.columns))
            .with_object_store(object_store, source.options.get("prefix").cloned());
        Ok((table, connector_id, runtime))
    }

    fn build_kafka_source(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
    ) -> Result<(CatalogTable, ConnectorId, KafkaSource), GatewayError> {
        let table = self.catalog.source_table(&source.name).ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("source '{}' is not bound to a table", source.name),
            }
        })?;
        let bootstrap = source
            .options
            .get("bootstrap.servers")
            .or_else(|| source.options.get("bootstrap_servers"))
            .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                detail: format!("Kafka source '{}' requires bootstrap.servers", source.name),
            })?;
        let topic =
            source
                .options
                .get("topic")
                .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("Kafka source '{}' requires topic", source.name),
                })?;
        let connector_id = source_view_connector_id(&source.name, view_name);
        let group_id = source
            .options
            .get("group.id")
            .or_else(|| source.options.get("group_id"))
            .cloned()
            .unwrap_or_else(|| format!("rockstream-{connector_id}"));
        let runtime = KafkaSource::connect(
            connector_id,
            catalog_columns_to_schema(&table.columns),
            bootstrap,
            topic,
            &group_id,
        )
        .map_err(source_backfill_error)?;
        Ok((table, connector_id, runtime))
    }

    async fn build_postgres_cdc_source(
        &self,
        source: &CatalogSourceEntry,
        _view_name: &str,
    ) -> Result<(CatalogTable, ConnectorId, PostgresCdcSource), GatewayError> {
        if source.format != "pgoutput" {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!(
                    "PostgreSQL CDC source '{}' requires FORMAT pgoutput; wal2json has no native gateway runtime",
                    source.name
                ),
            });
        }
        let table = self.catalog.source_table(&source.name).ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("source '{}' is not bound to a table", source.name),
            }
        })?;
        let option = |name: &str| {
            source.options.get(name).cloned().ok_or_else(|| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("PostgreSQL CDC source '{}' requires {name}", source.name),
                }
            })
        };
        let credential_ref = option("credential_ref")?;
        let password = if let Some(variable) = credential_ref.strip_prefix("env://") {
            Some(
                std::env::var(variable).map_err(|_| GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                    "PostgreSQL CDC source '{}' cannot resolve credential_ref '{credential_ref}'",
                    source.name
                ),
                })?,
            )
        } else if credential_ref == "none://trusted" {
            None
        } else {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!(
                    "PostgreSQL CDC source '{}' requires credential_ref env://<PASSWORD_ENV> or none://trusted",
                    source.name
                ),
            });
        };
        let identity = pgoutput_source_identity(source)?;
        let connector_id = identity.connector_id();
        let runtime = PostgresCdcSource::configured_pgoutput(
            connector_id,
            catalog_columns_to_schema(&table.columns),
            PgOutputConfig {
                host: identity.host,
                port: identity.port,
                database: identity.database,
                user: identity.auth_principal,
                password,
                slot: identity.slot,
                publication: identity.publication,
                table: source
                    .options
                    .get("table")
                    .cloned()
                    .unwrap_or_else(|| table.name.clone()),
            },
        )
        .map_err(source_backfill_error)?;
        Ok((table, connector_id, runtime))
    }

    async fn backfill_s3_source(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
        publish: bool,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        if let crate::admission::BackfillAdmissionDecision::Reject { code, reason } =
            self.backfill_admission.reserve(
                BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
                BACKFILL_ADMISSION_CAPACITY_BYTES,
            )
        {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!("[{code}] {reason}"),
            });
        }
        let _reservation = BackfillReservation {
            controller: Arc::clone(&self.backfill_admission),
            bytes: BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
        };
        let (_, connector_id, source_runtime) = self.build_s3_source(source, view_name)?;
        let checkpoint_store =
            SourceCheckpointStore::new(Arc::clone(shard_db), connector_id.0 as u128, connector_id);
        self.backfill_bound_source(
            &source.name,
            view_name,
            SourceRuntimeCoordinator::new(
                source_runtime,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoint_store,
            ),
            publish,
            shard_db,
        )
        .await?;
        if publish {
            self.spawn_s3_source_worker(
                source.clone(),
                view_name.to_string(),
                Arc::clone(shard_db),
            );
        }
        Ok(())
    }

    async fn backfill_kafka_source(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
        publish: bool,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        if let crate::admission::BackfillAdmissionDecision::Reject { code, reason } =
            self.backfill_admission.reserve(
                BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
                BACKFILL_ADMISSION_CAPACITY_BYTES,
            )
        {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!("[{code}] {reason}"),
            });
        }
        let _reservation = BackfillReservation {
            controller: Arc::clone(&self.backfill_admission),
            bytes: BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
        };
        let (_, connector_id, source_runtime) = self.build_kafka_source(source, view_name)?;
        let checkpoint_store =
            SourceCheckpointStore::new(Arc::clone(shard_db), connector_id.0 as u128, connector_id);
        self.backfill_bound_source(
            &source.name,
            view_name,
            SourceRuntimeCoordinator::new(
                source_runtime,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoint_store,
            ),
            publish,
            shard_db,
        )
        .await?;
        if publish {
            self.spawn_kafka_source_worker(
                source.clone(),
                view_name.to_string(),
                Arc::clone(shard_db),
            );
        }
        Ok(())
    }

    async fn backfill_postgres_cdc_source(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
        publish: bool,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        if self.catalog.is_backfill_published(view_name) {
            self.catalog.begin_backfill(view_name, 0);
        }
        let identity = pgoutput_source_identity(source)?;
        let _registry_guard = self.pgoutput_registry_lock.lock().await;
        {
            let _guard = self.shard_commit_lock.lock().await;
            identity.register(shard_db).await?;
        }
        if let Some(coordinator) = self
            .pgoutput_coordinators
            .get(&identity.connector_id())
            .map(|entry| entry.value().clone())
        {
            if !coordinator.lock().await.shares_shard(shard_db) {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: "RS-4013: pgoutput aliases and dependent views must share one shard"
                        .to_string(),
                });
            }
            return self
                .backfill_attached_pgoutput_view(source, view_name, publish, coordinator, shard_db)
                .await;
        }
        if let crate::admission::BackfillAdmissionDecision::Reject { code, reason } =
            self.backfill_admission.reserve(
                BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
                BACKFILL_ADMISSION_CAPACITY_BYTES,
            )
        {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!("[{code}] {reason}"),
            });
        }
        let _reservation = BackfillReservation {
            controller: Arc::clone(&self.backfill_admission),
            bytes: BACKFILL_LIVE_DELTA_MAX_BYTES as u64,
        };
        self.backfill_new_pgoutput_coordinator(source, view_name, publish, shard_db)
            .await?;
        if publish {
            self.spawn_postgres_cdc_source_worker(
                source.clone(),
                view_name.to_string(),
                Arc::clone(shard_db),
            );
        }
        Ok(())
    }

    async fn backfill_new_pgoutput_coordinator(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
        publish: bool,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let identity = pgoutput_source_identity(source)?;
        let connector_id = identity.connector_id();
        let aliases = self.pgoutput_source_aliases(connector_id);
        let (_, _, source_runtime) = self.build_postgres_cdc_source(source, view_name).await?;
        let checkpoint_store =
            SourceCheckpointStore::new(Arc::clone(shard_db), connector_id.0 as u128, connector_id);
        let coordinator = Arc::new(tokio::sync::Mutex::new(SharedPgOutputCoordinator::new(
            identity,
            SourceRuntimeCoordinator::new(
                source_runtime,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoint_store,
            ),
            Arc::clone(shard_db),
        )));
        let mut needs_attachment = false;
        {
            let mut coordinator = coordinator.lock().await;
            for alias in &aliases {
                coordinator.attach_alias(alias.name.clone());
            }
            coordinator.restore_catalog(shard_db).await?;
            if let Some(blocked) = &coordinator.blocked_state {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "{}: pgoutput source remains blocked at xid {} after incompatible relation {}",
                        blocked.code, blocked.xid, blocked.relation.relation_id
                    ),
                });
            }
            coordinator
                .runtime
                .recover()
                .await
                .map_err(source_backfill_error)?;
            self.validate_pgoutput_lifecycles(&coordinator).await?;
            let lease = coordinator
                .runtime
                .acquire_owner(format!("gateway:pgoutput:{}", connector_id.0))
                .map_err(source_backfill_error)?;
            coordinator.owner_lease = Some(lease.clone());
            coordinator
                .runtime
                .open_pgoutput(&lease)
                .await
                .map_err(source_backfill_error)?;
            if coordinator.runtime.committed_epoch() != 0 {
                needs_attachment = true;
                let durable = rockstream_connectors::PgLsn::from_offset_token(
                    coordinator.runtime.committed_offset(),
                )
                .map_err(source_backfill_error)?;
                if coordinator
                    .runtime
                    .pgoutput_confirmed_lsn(&lease)
                    .await
                    .map_err(source_backfill_error)?
                    .is_some_and(|slot| slot > durable)
                {
                    return Err(GatewayError::QueryTimeExecutionFailed {
                        detail: "RS-4013: PostgreSQL slot is ahead of the durable M3 checkpoint"
                            .to_string(),
                    });
                }
                coordinator
                    .runtime
                    .acknowledge_recovered(&lease)
                    .await
                    .map_err(source_backfill_error)?;
                coordinator.cleanup_recovered_spill(shard_db).await?;
            } else {
                let relations = aliases
                    .iter()
                    .filter_map(|alias| {
                        let table = self.catalog.source_table(&alias.name)?;
                        Some((
                            alias
                                .options
                                .get("table")
                                .cloned()
                                .unwrap_or_else(|| table.name.clone()),
                            catalog_columns_to_schema(&table.columns),
                        ))
                    })
                    .collect::<Vec<_>>();
                let snapshot = coordinator
                    .runtime
                    .capture_pgoutput_source_snapshot(&lease, &relations)
                    .await
                    .map_err(source_backfill_error)?;
                let estimated_rows = snapshot
                    .relations
                    .iter()
                    .map(|relation| relation.rows.len() as u64)
                    .sum();
                self.catalog.begin_backfill(view_name, estimated_rows);
                coordinator.begin(0)?;
                coordinator.activate_view(view_name);
                for relation in snapshot.relations {
                    let relation_id = relation.relation.relation_id;
                    self.stage_pgoutput_relation(
                        &mut coordinator,
                        &aliases,
                        0,
                        relation.relation,
                        &relation.column_policies,
                        shard_db,
                    )
                    .await?;
                    for row in relation.rows {
                        coordinator.push_change(
                            0,
                            relation_id,
                            CdcOperation::Insert,
                            None,
                            Some(row),
                        )?;
                    }
                }
                let envelope = coordinator.finish_envelope(0, snapshot.lsn)?;
                if let Err(error) = coordinator.commit_envelope(envelope, self, shard_db).await {
                    self.restore_compiled_pipeline_state(shard_db).await;
                    return Err(error);
                }
                self.catalog.update_backfill_progress(
                    view_name,
                    coordinator.runtime.committed_epoch().to_string(),
                    0,
                    estimated_rows,
                );
            }
        }
        self.pgoutput_coordinators
            .insert(connector_id, Arc::clone(&coordinator));
        if needs_attachment {
            self.backfill_attached_pgoutput_view(source, view_name, publish, coordinator, shard_db)
                .await?;
        }
        if publish {
            self.catalog.publish_backfill(view_name);
        }
        Ok(())
    }

    async fn backfill_attached_pgoutput_view(
        &self,
        source: &CatalogSourceEntry,
        view_name: &str,
        publish: bool,
        coordinator: Arc<tokio::sync::Mutex<SharedPgOutputCoordinator>>,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let mut coordinator = coordinator.lock().await;
        let _guard = self.shard_commit_lock.lock().await;
        let compiled = self
            .compiled_views
            .get(view_name)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                detail: format!("compiled view '{view_name}' is unavailable"),
            })?;
        let epoch = shard_db
            .try_next_epoch()
            .ok_or(GatewayError::CommitEpochExhausted)?;
        let output = if self.catalog.backfill_has_cursor(view_name) {
            None
        } else {
            Some(
                if let Some(join) = &compiled.join {
                    let left = self.full_table_zset(&join.left_source, shard_db).await?;
                    let right = self.full_table_zset(&join.right_source, shard_db).await?;
                    join.pipeline.process(left, right)
                } else {
                    let dependency = self
                        .catalog
                        .get_view_deps(view_name)
                        .into_iter()
                        .next()
                        .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                            detail: format!("compiled view '{view_name}' has no dependency"),
                        })?;
                    compiled
                        .pipeline
                        .process(self.full_table_zset(&dependency, shard_db).await?)
                }
                .map_err(|error| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled pipeline backfill({view_name}): {error}"),
                })?,
            )
        };
        let mut m3 = rockstream_storage::WriteBatch::new();
        if let Some(output) = &output {
            compiled.sink.append_epoch(&mut m3, output, epoch);
            if let Some(join) = &compiled.join {
                join.pipeline.append_state(shard_db, &mut m3).await
            } else {
                compiled.pipeline.append_state(shard_db, &mut m3).await
            }
            .map_err(|error| GatewayError::QueryTimeExecutionFailed {
                detail: format!("append compiled pipeline backfill state({view_name}): {error}"),
            })?;
        }
        m3.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );
        let offset = coordinator.runtime.committed_offset().clone();
        let lifecycle = BackfillLifecycle::new(
            BackfillPhase::Running,
            BackfillCursor::new(
                view_name,
                0,
                offset.as_bytes().to_vec(),
                SnapshotDeltaFence::new(offset.clone(), offset),
                epoch,
            ),
            0,
            output.as_ref().map_or(0, ArrowZSet::num_rows) as u64,
            0,
            Some(epoch),
        );
        let lease = coordinator.owner_lease.clone().ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: "RS-4013: pgoutput coordinator owner is fenced".to_string(),
            }
        })?;
        coordinator
            .runtime
            .commit_attachment(&lease, &lifecycle, m3)
            .await
            .map_err(source_backfill_error)?;
        coordinator.attach_alias(source.name.clone());
        if let Some(output) = output {
            self.catalog.update_backfill_progress(
                view_name,
                epoch.to_string(),
                0,
                output.num_rows() as u64,
            );
        }
        if publish {
            self.catalog.publish_backfill(view_name);
        }
        Ok(())
    }

    async fn backfill_source_view(
        &self,
        sources: &[CatalogSourceEntry],
        view_name: &str,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let mut pgoutput_identities = HashSet::new();
        for source in sources {
            match source.source_type.as_str() {
                "s3" => {
                    self.backfill_s3_source(source, view_name, false, shard_db)
                        .await?
                }
                "kafka" => {
                    self.backfill_kafka_source(source, view_name, false, shard_db)
                        .await?
                }
                "postgres_cdc" => {
                    let connector_id = pgoutput_source_identity(source)?.connector_id();
                    if !pgoutput_identities.insert(connector_id) {
                        continue;
                    }
                    self.backfill_postgres_cdc_source(source, view_name, false, shard_db)
                        .await?
                }
                source_type => {
                    return Err(GatewayError::QueryTimeExecutionFailed {
                        detail: format!(
                            "source type '{source_type}' has no gateway backfill runtime; materialized view '{view_name}' remains unpublished"
                        ),
                    });
                }
            }
        }
        self.refresh_backfill_progress(view_name).await;
        self.catalog.publish_backfill(view_name);
        for source in sources {
            match source.source_type.as_str() {
                "s3" => self.spawn_s3_source_worker(
                    source.clone(),
                    view_name.to_string(),
                    Arc::clone(shard_db),
                ),
                "kafka" => self.spawn_kafka_source_worker(
                    source.clone(),
                    view_name.to_string(),
                    Arc::clone(shard_db),
                ),
                "postgres_cdc" => self.spawn_postgres_cdc_source_worker(
                    source.clone(),
                    view_name.to_string(),
                    Arc::clone(shard_db),
                ),
                _ => unreachable!("source type was checked above"),
            }
        }
        Ok(())
    }

    async fn refresh_backfill_progress(&self, view_name: &str) {
        let Some(shard_db) = &self.shard_db else {
            return;
        };
        let sources = self
            .catalog
            .get_view_deps(view_name)
            .into_iter()
            .filter_map(|relation| {
                self.catalog
                    .get_source(&relation)
                    .filter(|source| source.table_name.as_deref() == Some(relation.as_str()))
            })
            .collect::<Vec<_>>();
        let mut cursor_positions = Vec::with_capacity(sources.len());
        let mut rows_remaining = 0;
        let mut estimated_rows = 0;
        for source in &sources {
            let connector_id = source_checkpoint_connector_id(source, view_name);
            let Ok(Some(lifecycle)) = SourceCheckpointStore::new(
                Arc::clone(shard_db),
                connector_id.0 as u128,
                connector_id,
            )
            .backfill_lifecycle(view_name)
            .await
            else {
                return;
            };
            rows_remaining += lifecycle.rows_remaining;
            estimated_rows += lifecycle.estimated_rows;
            cursor_positions.push((source.name.clone(), lifecycle.cursor.committed_epoch));
        }
        if cursor_positions.is_empty() {
            return;
        }
        cursor_positions.sort_by(|left, right| left.0.cmp(&right.0));
        let cursor_position = if cursor_positions.len() == 1 {
            cursor_positions[0].1.to_string()
        } else {
            cursor_positions
                .into_iter()
                .map(|(source, epoch)| format!("{source}:{epoch}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        self.catalog.update_backfill_progress(
            view_name,
            cursor_position,
            rows_remaining,
            estimated_rows,
        );
    }

    fn spawn_s3_source_worker(
        &self,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let key = format!("{}:{view_name}", source.name);
        if self.source_workers.insert(key.clone(), ()).is_some() {
            return;
        }
        let weak = self.self_ref.lock().clone();
        if weak.strong_count() == 0 {
            self.source_workers.remove(&key);
            return;
        }
        tokio::spawn(async move {
            GatewayHandler::run_s3_source_worker(weak.clone(), source, view_name, shard_db).await;
            if let Some(handler) = weak.upgrade() {
                handler.source_workers.remove(&key);
            }
        });
    }

    fn spawn_kafka_source_worker(
        &self,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let key = format!("{}:{view_name}", source.name);
        if self.source_workers.insert(key.clone(), ()).is_some() {
            return;
        }
        let weak = self.self_ref.lock().clone();
        if weak.strong_count() == 0 {
            self.source_workers.remove(&key);
            return;
        }
        tokio::spawn(async move {
            GatewayHandler::run_kafka_source_worker(weak.clone(), source, view_name, shard_db)
                .await;
            if let Some(handler) = weak.upgrade() {
                handler.source_workers.remove(&key);
            }
        });
    }

    fn spawn_postgres_cdc_source_worker(
        &self,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let Ok(identity) = pgoutput_source_identity(&source) else {
            return;
        };
        let connector_id = identity.connector_id();
        let key = format!("pgoutput:{}", connector_id.0);
        if self.source_workers.insert(key.clone(), ()).is_some() {
            return;
        }
        let weak = self.self_ref.lock().clone();
        if weak.strong_count() == 0 {
            self.source_workers.remove(&key);
            return;
        }
        tokio::spawn(async move {
            GatewayHandler::run_postgres_cdc_source_worker(
                weak.clone(),
                source,
                view_name,
                shard_db,
            )
            .await;
            if let Some(handler) = weak.upgrade() {
                handler.source_workers.remove(&key);
                handler.pgoutput_coordinators.remove(&connector_id);
            }
        });
    }

    async fn run_kafka_source_worker(
        weak: Weak<GatewayHandler>,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let Some(handler) = weak.upgrade() else {
            return;
        };
        let Ok((table, connector_id, source_runtime)) =
            handler.build_kafka_source(&source, &view_name)
        else {
            return;
        };
        drop(handler);
        let checkpoint_store =
            SourceCheckpointStore::new(Arc::clone(&shard_db), connector_id.0 as u128, connector_id);
        Self::run_live_source_worker(
            weak,
            source,
            view_name,
            table,
            SourceRuntimeCoordinator::new(
                source_runtime,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoint_store,
            ),
            shard_db,
        )
        .await;
    }

    async fn run_postgres_cdc_source_worker(
        weak: Weak<GatewayHandler>,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let Some(handler) = weak.upgrade() else {
            return;
        };
        let Ok(identity) = pgoutput_source_identity(&source) else {
            return;
        };
        let connector_id = identity.connector_id();
        let registry_guard = handler.pgoutput_registry_lock.lock().await;
        let (coordinator, created) = if let Some(existing) = handler
            .pgoutput_coordinators
            .get(&connector_id)
            .map(|entry| entry.value().clone())
        {
            if !existing.lock().await.shares_shard(&shard_db) {
                handler.block_pgoutput_aliases(
                    std::slice::from_ref(&source),
                    "RS-4013: pgoutput aliases and dependent views must share one shard"
                        .to_string(),
                );
                return;
            }
            (existing, false)
        } else {
            let Ok((_, _, source_runtime)) =
                handler.build_postgres_cdc_source(&source, &view_name).await
            else {
                return;
            };
            let checkpoint_store = SourceCheckpointStore::new(
                Arc::clone(&shard_db),
                connector_id.0 as u128,
                connector_id,
            );
            let coordinator = Arc::new(tokio::sync::Mutex::new(SharedPgOutputCoordinator::new(
                identity,
                SourceRuntimeCoordinator::new(
                    source_runtime,
                    connector_id,
                    OffsetToken::new(Vec::new()),
                    checkpoint_store,
                ),
                Arc::clone(&shard_db),
            )));
            handler
                .pgoutput_coordinators
                .insert(connector_id, Arc::clone(&coordinator));
            (coordinator, true)
        };
        let initialized = async {
            let mut coordinator = coordinator.lock().await;
            if coordinator.owner_lease.is_none() {
                coordinator.attach_alias(source.name.clone());
                coordinator
                    .restore_catalog(&shard_db)
                    .await
                    .map_err(|_| ())?;
                if coordinator.blocked_state.is_some() {
                    handler.block_pgoutput_aliases(
                        std::slice::from_ref(&source),
                        "RS-1002: pgoutput source remains blocked by an incompatible relation"
                            .to_string(),
                    );
                    return Err(());
                }
                coordinator.runtime.resume().await.map_err(|_| ())?;
                handler
                    .validate_pgoutput_lifecycles(&coordinator)
                    .await
                    .map_err(|_| ())?;
                let lease = coordinator
                    .runtime
                    .acquire_owner(format!("gateway:pgoutput:{}", connector_id.0))
                    .map_err(|_| ())?;
                coordinator.owner_lease = Some(lease.clone());
                coordinator
                    .runtime
                    .open_pgoutput(&lease)
                    .await
                    .map_err(|_| ())?;
                let durable_lsn = rockstream_connectors::PgLsn::from_offset_token(
                    coordinator.runtime.committed_offset(),
                )
                .map_err(|_| ())?;
                let slot_lsn = coordinator
                    .runtime
                    .pgoutput_confirmed_lsn(&lease)
                    .await
                    .map_err(|_| ())?;
                if coordinator.runtime.committed_epoch() != 0
                    && slot_lsn.is_some_and(|slot_lsn| slot_lsn > durable_lsn)
                {
                    handler.block_pgoutput_aliases(
                        std::slice::from_ref(&source),
                        "RS-4013: PostgreSQL slot is ahead of the durable M3 checkpoint"
                            .to_string(),
                    );
                    return Err(());
                }
                coordinator
                    .runtime
                    .acknowledge_recovered(&lease)
                    .await
                    .map_err(|_| ())?;
                coordinator
                    .cleanup_recovered_spill(&shard_db)
                    .await
                    .map_err(|_| ())?;
            }
            Ok::<(), ()>(())
        }
        .await;
        if initialized.is_err() {
            if created {
                handler.pgoutput_coordinators.remove(&connector_id);
            }
            return;
        }
        drop(registry_guard);
        drop(handler);

        loop {
            let Some(handler) = weak.upgrade() else {
                break;
            };
            let aliases = handler.pgoutput_source_aliases(connector_id);
            if aliases.is_empty() {
                let mut coordinator = coordinator.lock().await;
                let dropped = handler.pgoutput_registered_aliases(connector_id).is_empty();
                let cleaned = if dropped {
                    coordinator.drop_durable_state(&shard_db).await.is_ok()
                } else if let Some(lease) = coordinator.owner_lease.clone() {
                    let closed = coordinator.runtime.close_pgoutput(&lease);
                    coordinator.owner_lease = None;
                    closed
                } else {
                    true
                };
                drop(coordinator);
                if dropped && cleaned {
                    handler.pgoutput_coordinators.remove(&connector_id);
                }
                break;
            }
            let mut coordinator = coordinator.lock().await;
            for alias in &aliases {
                coordinator.attach_alias(alias.name.clone());
            }
            let Some(lease) = coordinator.owner_lease.clone() else {
                break;
            };
            let event = match coordinator
                .runtime
                .poll_pgoutput_event(&lease, BACKFILL_BATCH_MAX_ROWS)
                .await
            {
                Ok(Some(event)) => event,
                Ok(None) => {
                    drop(coordinator);
                    drop(handler);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => {
                    handler
                        .block_pgoutput_aliases(&aliases, format!("pgoutput poll failed: {error}"));
                    break;
                }
            };
            let result = match event {
                PgOutputEvent::Begin { xid } => coordinator.begin(xid),
                PgOutputEvent::Relation { xid, relation } => {
                    match coordinator
                        .runtime
                        .pgoutput_relation_column_policies(&lease, relation.relation_id)
                        .await
                    {
                        Ok(column_policies) => {
                            handler
                                .stage_pgoutput_relation(
                                    &mut coordinator,
                                    &aliases,
                                    xid,
                                    relation,
                                    &column_policies,
                                    &shard_db,
                                )
                                .await
                        }
                        Err(error) => Err(source_backfill_error(error)),
                    }
                }
                PgOutputEvent::Insert {
                    xid,
                    relation_id,
                    new_values,
                } => coordinator.push_change(
                    xid,
                    relation_id,
                    CdcOperation::Insert,
                    None,
                    Some(new_values),
                ),
                PgOutputEvent::Update {
                    xid,
                    relation_id,
                    old_values,
                    new_values,
                } => coordinator.push_change(
                    xid,
                    relation_id,
                    CdcOperation::Update,
                    Some(old_values),
                    Some(new_values),
                ),
                PgOutputEvent::Delete {
                    xid,
                    relation_id,
                    old_values,
                } => coordinator.push_change(
                    xid,
                    relation_id,
                    CdcOperation::Delete,
                    Some(old_values),
                    None,
                ),
                PgOutputEvent::Commit { xid, commit_lsn } => {
                    match coordinator.finish_envelope(xid, commit_lsn) {
                        Ok(envelope) => {
                            coordinator
                                .commit_envelope(envelope, &handler, &shard_db)
                                .await
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            if let Err(error) = result {
                handler.block_pgoutput_aliases(&aliases, error.to_string());
                handler.restore_compiled_pipeline_state(&shard_db).await;
                let _ = coordinator.runtime.close_pgoutput(&lease);
                coordinator.owner_lease = None;
                break;
            }
            handler.project_pgoutput_status(&coordinator);
        }
    }

    fn pgoutput_source_aliases(&self, connector_id: ConnectorId) -> Vec<CatalogSourceEntry> {
        self.pgoutput_registered_aliases(connector_id)
            .into_iter()
            .filter(|source| source.status == "OK")
            .collect()
    }

    fn pgoutput_registered_aliases(&self, connector_id: ConnectorId) -> Vec<CatalogSourceEntry> {
        self.catalog
            .list_sources()
            .into_iter()
            .filter(|source| {
                source.source_type == "postgres_cdc"
                    && source.format == "pgoutput"
                    && pgoutput_source_identity(source)
                        .is_ok_and(|identity| identity.connector_id() == connector_id)
            })
            .collect()
    }

    fn block_pgoutput_aliases(&self, aliases: &[CatalogSourceEntry], reason: String) {
        for alias in aliases {
            self.catalog.update_source_status(&alias.name, "BLOCKED");
            self.catalog.update_source_runtime_detail(
                &alias.name,
                Some("gateway:pgoutput:fenced".to_string()),
                None,
                alias.live_offset.clone(),
                alias.live_lag,
                None,
                Some(reason.clone()),
            );
        }
    }

    fn project_pgoutput_status(&self, coordinator: &SharedPgOutputCoordinator) {
        let detail = PgOutputSourceRuntimeDetail {
            source_identity_hash: format!("{:016x}", coordinator.connector_id.0),
            active_xid: coordinator
                .active_envelope
                .as_ref()
                .map(|active| active.xid),
            envelope_bytes: coordinator.envelope_bytes(),
            in_memory_bytes: coordinator.in_memory_bytes(),
            spill_bytes: coordinator.spill_bytes(),
            attached_view_count: coordinator.attached_view_count,
            affected_view_count: coordinator.affected_view_count,
            relation_schema_version: coordinator
                .relation_routes
                .values()
                .map(|route| route.schema_version)
                .max()
                .unwrap_or(0),
        };
        for alias in coordinator.aliases() {
            self.catalog
                .update_pgoutput_source_runtime(alias, detail.clone());
        }
    }

    async fn restore_compiled_pipeline_state(&self, shard_db: &rockstream_storage::ShardDb) {
        let compiled = self
            .compiled_views
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for view in compiled {
            let result = if let Some(join) = &view.join {
                join.pipeline.restore(shard_db).await
            } else {
                view.pipeline.restore(shard_db).await
            };
            if let Err(error) = result {
                tracing::error!(view = %view.view_name, %error, "restore fenced pgoutput pipeline failed");
            }
        }
    }

    async fn validate_pgoutput_lifecycles(
        &self,
        coordinator: &SharedPgOutputCoordinator,
    ) -> Result<(), GatewayError> {
        if coordinator.runtime.committed_epoch() == 0 {
            return Ok(());
        }
        let relations = coordinator
            .aliases()
            .filter_map(|alias| self.catalog.source_table(alias))
            .map(|table| table.name)
            .collect::<HashSet<_>>();
        for view in self
            .reachable_compiled_views(&relations)
            .into_iter()
            .filter(|view| self.catalog.is_backfill_published(view))
        {
            let lifecycle = coordinator
                .runtime
                .backfill_lifecycle(&view)
                .await
                .map_err(source_backfill_error)?
                .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "RS-4019: active pgoutput view '{view}' has no durable lifecycle"
                    ),
                })?;
            if lifecycle.cursor.last_key != coordinator.runtime.committed_offset().as_bytes() {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "RS-4019: active pgoutput view '{view}' cursor differs from source checkpoint"
                    ),
                });
            }
        }
        Ok(())
    }

    async fn stage_pgoutput_relation(
        &self,
        coordinator: &mut SharedPgOutputCoordinator,
        aliases: &[CatalogSourceEntry],
        xid: u32,
        relation: PgOutputRelationMetadata,
        column_policies: &[(bool, bool)],
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        if let Some(existing) = coordinator.relation_routes.get(&relation.relation_id) {
            if existing.upstream_namespace != relation.namespace
                || existing.upstream_relation != relation.name
            {
                return self
                    .block_relation_change(coordinator, xid, relation, shard_db)
                    .await;
            }
        }
        let Some(alias) = aliases.iter().find(|alias| {
            let configured = alias
                .options
                .get("table")
                .map(String::as_str)
                .or(alias.table_name.as_deref())
                .unwrap_or(&alias.name);
            let (namespace, name) = configured.split_once('.').unwrap_or(("public", configured));
            namespace == relation.namespace && name == relation.name
        }) else {
            return coordinator.stage_unrouted(xid, relation.relation_id);
        };
        let table = self.catalog.source_table(&alias.name).ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("source '{}' is not bound to an imported table", alias.name),
            }
        })?;
        if column_policies.len() != relation.columns.len()
            || relation
                .columns
                .iter()
                .any(|column| !matches!(column.type_oid, 20 | 23 | 25 | 1043 | 1700))
        {
            return self
                .block_relation_change(coordinator, xid, relation, shard_db)
                .await;
        }
        let previous = coordinator
            .relation_routes
            .get(&relation.relation_id)
            .cloned();
        let next_schema_version = coordinator.next_schema_version()?;
        if previous.is_none()
            && (table.columns.len() != relation.columns.len()
                || table
                    .columns
                    .iter()
                    .zip(&relation.columns)
                    .any(|(imported, upstream)| {
                        imported.name != upstream.name
                            || !catalog_type_accepts_pg_oid(&imported.data_type, upstream.type_oid)
                    }))
        {
            return self
                .block_relation_change(coordinator, xid, relation, shard_db)
                .await;
        }
        let route = RelationRoute {
            version: 1,
            relation_id: relation.relation_id,
            upstream_namespace: relation.namespace,
            upstream_relation: relation.name,
            imported_table_id: rockstream_types::rendezvous::fnv1a_64(table.name.as_bytes()),
            imported_table_name: table.name,
            columns: relation
                .columns
                .into_iter()
                .enumerate()
                .map(|(index, upstream)| {
                    let imported_name = previous
                        .as_ref()
                        .and_then(|route| route.columns.get(index))
                        .map_or_else(
                            || {
                                table
                                    .columns
                                    .get(index)
                                    .map(|column| column.name.clone())
                                    .unwrap_or_else(|| upstream.name.clone())
                            },
                            |column| column.imported_name.clone(),
                        );
                    ColumnRoute {
                        upstream_name: upstream.name,
                        imported_name,
                        type_oid: upstream.type_oid,
                        type_modifier: upstream.type_modifier,
                        nullable: column_policies[index].0,
                        has_default: column_policies[index].1,
                        key: upstream.flags & 1 != 0,
                    }
                })
                .collect(),
            replica_identity: ReplicaIdentity::from_wire(relation.replica_identity)?,
            schema_version: next_schema_version,
        };
        if let Some(previous) = &previous {
            match previous.classify(&route) {
                RelationChange::Unchanged => return Ok(()),
                RelationChange::Compatible => {}
                RelationChange::Breaking(_) => {
                    let relation = PgOutputRelationMetadata {
                        relation_id: route.relation_id,
                        namespace: route.upstream_namespace,
                        name: route.upstream_relation,
                        replica_identity: relation.replica_identity,
                        columns: route
                            .columns
                            .into_iter()
                            .map(|column| rockstream_connectors::PgOutputColumn {
                                flags: u8::from(column.key),
                                name: column.upstream_name,
                                type_oid: column.type_oid,
                                type_modifier: column.type_modifier,
                            })
                            .collect(),
                    };
                    return self
                        .block_relation_change(coordinator, xid, relation, shard_db)
                        .await;
                }
            }
        }
        coordinator.stage_route(xid, route)
    }

    async fn block_relation_change(
        &self,
        coordinator: &mut SharedPgOutputCoordinator,
        xid: u32,
        relation: PgOutputRelationMetadata,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let _guard = self.shard_commit_lock.lock().await;
        let last_safe_lsn =
            rockstream_connectors::PgLsn::from_offset_token(coordinator.runtime.committed_offset())
                .map_err(source_backfill_error)?;
        let mut batch = rockstream_storage::WriteBatch::new();
        let blocked = BlockedRelationState {
            code: "RS-1002".to_string(),
            xid,
            relation,
            last_safe_lsn,
        };
        append_blocked_state(&mut batch, coordinator.connector_id, &blocked)?;
        shard_db.write_batch(batch).await?;
        shard_db.flush().await?;
        coordinator.blocked_state = Some(blocked);
        Err(GatewayError::QueryTimeExecutionFailed {
            detail: "RS-1002: incompatible upstream relation change blocked the pgoutput source"
                .to_string(),
        })
    }

    pub(crate) async fn commit_pgoutput_envelope(
        &self,
        coordinator: &mut SharedPgOutputCoordinator,
        envelope: BufferedPgOutputEnvelope,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let _guard = self.shard_commit_lock.lock().await;
        let epoch = shard_db
            .try_next_epoch()
            .ok_or(GatewayError::CommitEpochExhausted)?;
        let staged_routes = envelope
            .route_updates
            .iter()
            .map(|route| (route.relation_id, route))
            .collect::<HashMap<_, _>>();
        let route_schemas = coordinator
            .relation_routes
            .values()
            .chain(&envelope.route_updates)
            .map(|route| {
                (
                    route.imported_table_name.clone(),
                    relation_route_schema(route),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut ops = Vec::new();
        for change in &envelope.changes {
            let route = staged_routes
                .get(&change.relation_id)
                .copied()
                .or_else(|| coordinator.relation_routes.get(&change.relation_id))
                .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "RS-4013: pgoutput relation {} has no durable route",
                        change.relation_id
                    ),
                })?;
            if route.schema_version != change.schema_version {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "RS-1002: pgoutput relation {} change used schema version {}, expected {}",
                        change.relation_id, change.schema_version, route.schema_version
                    ),
                });
            }
            ops.push(pgoutput_change_to_dml(route, change)?);
        }
        let changed_relations = ops
            .iter()
            .map(dml_table_name)
            .map(str::to_string)
            .collect::<HashSet<_>>();
        let mut attached_relations = coordinator
            .relation_routes
            .values()
            .chain(&envelope.route_updates)
            .map(|route| route.imported_table_name.clone())
            .collect::<HashSet<_>>();
        for alias in coordinator.aliases() {
            if let Some(table) = self.catalog.source_table(alias) {
                attached_relations.insert(table.name);
            }
        }
        let mut active_views = self
            .reachable_compiled_views(&attached_relations)
            .into_iter()
            .filter(|view| self.catalog.is_backfill_published(view))
            .collect::<Vec<_>>();
        for view in coordinator.activating_views() {
            if !active_views.iter().any(|active| active == view) {
                active_views.push(view.to_string());
            }
        }
        active_views.sort();
        let affected = self
            .reachable_compiled_views(&changed_relations)
            .into_iter()
            .filter(|view| active_views.contains(view))
            .collect::<Vec<_>>();
        coordinator.attached_view_count = active_views.len();
        coordinator.affected_view_count = affected.len();

        let mut m3 = rockstream_storage::WriteBatch::new();
        append_dml_ops(&mut m3, &ops);
        let mut deltas = HashMap::<String, ArrowZSet>::new();
        for relation in &changed_relations {
            deltas.insert(
                relation.clone(),
                build_delta_zset_for_table(
                    relation,
                    &ops,
                    route_schemas
                        .get(relation)
                        .cloned()
                        .unwrap_or_else(|| query_time_relation_schema(&self.catalog, relation)),
                )?,
            );
        }
        for view_name in &affected {
            let compiled = self
                .compiled_views
                .get(view_name)
                .map(|entry| entry.value().clone())
                .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled view '{view_name}' is unavailable"),
                })?;
            let output = if let Some(join) = &compiled.join {
                let left = deltas.get(&join.left_source).cloned().unwrap_or_else(|| {
                    ArrowZSet::empty(query_time_relation_schema(&self.catalog, &join.left_source))
                });
                let right = deltas.get(&join.right_source).cloned().unwrap_or_else(|| {
                    ArrowZSet::empty(query_time_relation_schema(
                        &self.catalog,
                        &join.right_source,
                    ))
                });
                join.pipeline.process(left, right)
            } else {
                let deps = self.catalog.get_view_deps(view_name);
                let schema = deps
                    .first()
                    .map(|dep| query_time_relation_schema(&self.catalog, dep))
                    .unwrap_or_else(|| query_time_relation_schema(&self.catalog, view_name));
                let inputs = deps
                    .iter()
                    .filter_map(|dep| deltas.get(dep).cloned())
                    .collect::<Vec<_>>();
                let input =
                    rockstream_ops::join::concat_zsets(inputs, schema).map_err(|error| {
                        GatewayError::QueryTimeExecutionFailed {
                            detail: format!("combine compiled view input({view_name}): {error}"),
                        }
                    })?;
                compiled.pipeline.process(input)
            }
            .map_err(|error| GatewayError::QueryTimeExecutionFailed {
                detail: format!("compiled pipeline process({view_name}): {error}"),
            })?;
            compiled.sink.append_epoch(&mut m3, &output, epoch);
            if let Some(join) = &compiled.join {
                join.pipeline.append_state(shard_db, &mut m3).await
            } else {
                compiled.pipeline.append_state(shard_db, &mut m3).await
            }
            .map_err(|error| GatewayError::QueryTimeExecutionFailed {
                detail: format!("append compiled pipeline state({view_name}): {error}"),
            })?;
            deltas.insert(view_name.clone(), output);
        }
        coordinator.append_route_updates(&mut m3, &envelope.route_updates)?;
        m3.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );

        let offset = envelope.commit_lsn.to_offset_token();
        let activating_views = coordinator
            .activating_views()
            .map(str::to_string)
            .collect::<HashSet<_>>();
        let mut lifecycles = Vec::with_capacity(active_views.len());
        for view_name in &active_views {
            let previous = coordinator
                .runtime
                .backfill_lifecycle(view_name)
                .await
                .map_err(source_backfill_error)?;
            if previous.is_none() && !activating_views.contains(view_name) {
                return Err(GatewayError::QueryTimeExecutionFailed {
                    detail: format!(
                        "RS-4019: active pgoutput view '{view_name}' has no lifecycle cursor"
                    ),
                });
            }
            let fence = previous
                .as_ref()
                .map(|lifecycle| lifecycle.cursor.fence.clone())
                .unwrap_or_else(|| SnapshotDeltaFence::new(offset.clone(), offset.clone()));
            let estimated_rows = previous
                .as_ref()
                .map_or(envelope.changes.len() as u64, |lifecycle| {
                    lifecycle.estimated_rows
                });
            lifecycles.push(BackfillLifecycle::new(
                BackfillPhase::Running,
                BackfillCursor::new(view_name, 0, offset.as_bytes().to_vec(), fence, epoch),
                0,
                estimated_rows,
                0,
                Some(epoch),
            ));
        }
        let lease = coordinator.owner_lease.clone().ok_or_else(|| {
            GatewayError::QueryTimeExecutionFailed {
                detail: "RS-4013: pgoutput coordinator owner is fenced".to_string(),
            }
        })?;
        coordinator
            .runtime
            .commit_replayable_epoch(&lease, epoch, offset, &lifecycles, m3)
            .await
            .map_err(source_backfill_error)?;
        coordinator.cleanup_committed(shard_db).await?;
        for route in &envelope.route_updates {
            self.catalog.update_table_columns(
                &route.imported_table_name,
                route
                    .columns
                    .iter()
                    .map(|column| CatalogColumn {
                        name: column.imported_name.clone(),
                        data_type: pg_oid_catalog_type(column.type_oid).to_string(),
                    })
                    .collect(),
            );
        }
        self.frontier_published_at_ms
            .store(current_time_ms(), Ordering::SeqCst);
        for alias in coordinator.aliases() {
            self.catalog.update_source_runtime_detail(
                alias,
                Some(format!("gateway:pgoutput:{}", coordinator.connector_id.0)),
                Some(epoch),
                envelope.commit_lsn.to_string(),
                0,
                Some(0),
                None,
            );
        }
        Ok(())
    }

    async fn run_s3_source_worker(
        weak: Weak<GatewayHandler>,
        source: CatalogSourceEntry,
        view_name: String,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        let Some(handler) = weak.upgrade() else {
            return;
        };
        let Ok((table, connector_id, source_runtime)) =
            handler.build_s3_source(&source, &view_name)
        else {
            return;
        };
        drop(handler);
        let checkpoint_store =
            SourceCheckpointStore::new(Arc::clone(&shard_db), connector_id.0 as u128, connector_id);
        Self::run_live_source_worker(
            weak,
            source,
            view_name,
            table,
            SourceRuntimeCoordinator::new(
                source_runtime,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoint_store,
            ),
            shard_db,
        )
        .await;
    }

    async fn run_live_source_worker<S: SourceConnector>(
        weak: Weak<GatewayHandler>,
        source: CatalogSourceEntry,
        view_name: String,
        table: CatalogTable,
        mut runtime: SourceRuntimeCoordinator<S>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) {
        if runtime.recover().await.is_err() {
            return;
        }
        let Ok(lease) = runtime.acquire_owner(format!("gateway:{view_name}:live")) else {
            return;
        };
        loop {
            let Some(handler) = weak.upgrade() else {
                break;
            };
            if handler.catalog.get_source(&source.name).is_none() {
                break;
            }
            drop(handler);
            let Ok(Some(lifecycle)) = runtime.backfill_lifecycle(&view_name).await else {
                break;
            };
            if lifecycle.phase != BackfillPhase::Running {
                break;
            }
            let delta = match runtime
                .poll_delta_after(
                    OffsetToken::new(lifecycle.cursor.last_key.clone()),
                    BACKFILL_LIVE_DELTA_MAX_BYTES,
                    BACKFILL_BATCH_MAX_ROWS,
                )
                .await
            {
                Ok(delta) => delta,
                Err(error) => {
                    tracing::warn!(view = %view_name, error = %error, "live source poll failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if delta.batches.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let delta_bytes = delta
                .batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            if delta_bytes > BACKFILL_LIVE_DELTA_MAX_BYTES {
                tracing::warn!(
                    view = %view_name,
                    delta_bytes,
                    "live source exceeded BACKFILL_LIVE_DELTA_MAX_BYTES"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let mut ops = Vec::new();
            let mut decode_failed = false;
            for batch in &delta.batches {
                match source_batch_to_dml_ops(&table.name, &table.columns, batch) {
                    Ok(batch_ops) => ops.extend(batch_ops),
                    Err(error) => {
                        tracing::warn!(view = %view_name, %error, "live source batch decode failed");
                        decode_failed = true;
                        break;
                    }
                }
            }
            if decode_failed {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let Ok(epoch) = runtime.next_epoch() else {
                break;
            };
            let Some(handler) = weak.upgrade() else {
                break;
            };
            if let Err(error) = handler
                .commit_bound_source_ops(
                    &mut runtime,
                    &lease,
                    &view_name,
                    &table,
                    &lifecycle.cursor.fence,
                    delta.new_offset,
                    ops,
                    BackfillPhase::Running,
                    Some(epoch),
                    0,
                    lifecycle.estimated_rows,
                    &shard_db,
                )
                .await
            {
                tracing::warn!(view = %view_name, %error, "live source M3 commit failed");
            } else {
                handler.refresh_backfill_progress(&view_name).await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_bound_source_batch<S: SourceConnector>(
        &self,
        runtime: &mut SourceRuntimeCoordinator<S>,
        lease: &SourceOwnerLease,
        view_name: &str,
        table: &CatalogTable,
        fence: &SnapshotDeltaFence,
        offset: OffsetToken,
        batch: &RecordBatch,
        phase: BackfillPhase,
        published_frontier: Option<u64>,
        rows_remaining: u64,
        estimated_rows: u64,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let ops = source_batch_to_dml_ops(&table.name, &table.columns, batch)
            .map_err(|detail| GatewayError::QueryTimeExecutionFailed { detail })?;
        self.commit_bound_source_ops(
            runtime,
            lease,
            view_name,
            table,
            fence,
            offset,
            ops,
            phase,
            published_frontier,
            rows_remaining,
            estimated_rows,
            shard_db,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_bound_source_ops<S: SourceConnector>(
        &self,
        runtime: &mut SourceRuntimeCoordinator<S>,
        lease: &SourceOwnerLease,
        view_name: &str,
        table: &CatalogTable,
        fence: &SnapshotDeltaFence,
        offset: OffsetToken,
        ops: Vec<DmlOp>,
        phase: BackfillPhase,
        published_frontier: Option<u64>,
        rows_remaining: u64,
        estimated_rows: u64,
        shard_db: &Arc<rockstream_storage::ShardDb>,
    ) -> Result<(), GatewayError> {
        let _guard = self.shard_commit_lock.lock().await;
        let epoch = shard_db
            .try_next_epoch()
            .ok_or(GatewayError::CommitEpochExhausted)?;
        let published_frontier = published_frontier.map(|_| epoch);
        let compiled = self
            .compiled_views
            .get(view_name)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                detail: format!("compiled view '{view_name}' is unavailable"),
            })?;
        let input = build_delta_zset_for_table(
            &table.name,
            &ops,
            query_time_relation_schema(&self.catalog, &table.name),
        )?;
        let output = if let Some(join) = &compiled.join {
            let left = if join.left_source == table.name {
                input.clone()
            } else {
                ArrowZSet::empty(query_time_relation_schema(&self.catalog, &join.left_source))
            };
            let right = if join.right_source == table.name {
                input
            } else {
                ArrowZSet::empty(query_time_relation_schema(
                    &self.catalog,
                    &join.right_source,
                ))
            };
            join.pipeline.process(left, right).map_err(|error| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled join pipeline process({view_name}): {error}"),
                }
            })?
        } else {
            compiled.pipeline.process(input).map_err(|error| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("compiled pipeline process({view_name}): {error}"),
                }
            })?
        };
        let cursor = BackfillCursor::new(
            view_name,
            0,
            offset.as_bytes().to_vec(),
            fence.clone(),
            epoch,
        );
        let lifecycle = BackfillLifecycle::new(
            phase,
            cursor,
            rows_remaining,
            estimated_rows,
            0,
            published_frontier,
        );
        let mut m3 = rockstream_storage::WriteBatch::new();
        append_dml_ops(&mut m3, &ops);
        compiled.sink.append_epoch(&mut m3, &output, epoch);
        if let Some(join) = &compiled.join {
            join.pipeline.append_state(shard_db.as_ref(), &mut m3).await
        } else {
            compiled
                .pipeline
                .append_state(shard_db.as_ref(), &mut m3)
                .await
        }
        .map_err(|error| GatewayError::QueryTimeExecutionFailed {
            detail: format!("persist compiled pipeline state({view_name}): {error}"),
        })?;
        m3.put(
            &rockstream_storage::ShardKeyEncoder::frontier_key(),
            &epoch.to_be_bytes(),
        );
        runtime
            .commit_backfill_epoch(lease, epoch, offset, lifecycle, m3)
            .await
            .map_err(source_backfill_error)?;
        self.catalog.update_backfill_progress(
            view_name,
            epoch.to_string(),
            rows_remaining,
            estimated_rows,
        );
        if phase == BackfillPhase::CatchingUp {
            self.catalog
                .catch_up_backfill(view_name, Some(epoch.to_string()));
        }
        Ok(())
    }

    fn published_frontier_age_ms(&self) -> Option<u64> {
        let published_at = self.frontier_published_at_ms.load(Ordering::SeqCst);
        if published_at == 0 {
            return None;
        }
        Some(current_time_ms().saturating_sub(published_at))
    }

    fn capture_max_staleness_metadata(&self, conn_id: &str) {
        let age_ms = self.published_frontier_age_ms();
        let mut session = self.sessions.entry(conn_id.to_string()).or_default();
        session.pending_notice = None;
        if let Some(max_staleness) = session.max_staleness {
            let age_ms = age_ms.unwrap_or(0);
            session.frontier_age_ms = Some(age_ms);
            session
                .guc_params
                .insert("frontier_age_ms".to_string(), age_ms.to_string());
            rockstream_types::metrics::set_session_frontier_age_ms("max_staleness", age_ms);
            if age_ms > max_staleness.as_millis() as u64 {
                rockstream_types::metrics::inc_session_staleness_exceeded("max_staleness");
                session.pending_notice = Some(SessionNotice {
                    severity: "NOTICE".to_string(),
                    sqlstate: "01000".to_string(),
                    message: format!(
                        "[RS-2018] session.staleness_exceeded: published frontier age {age_ms}ms exceeded rockstream.max_staleness={}ms. next_steps: Increase rockstream.max_staleness, reduce publish lag, or switch back to session_wait_for mode.",
                        max_staleness.as_millis()
                    ),
                });
            }
        } else {
            session.frontier_age_ms = None;
            session.guc_params.remove("frontier_age_ms");
        }
    }

    async fn emit_session_annotations<C>(&self, client: &mut C, conn_id: &str) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (notice, frontier_age_ms) = if let Some(mut session) = self.sessions.get_mut(conn_id) {
            (session.pending_notice.take(), session.frontier_age_ms)
        } else {
            (None, None)
        };
        if let Some(notice) = notice {
            client
                .feed(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                    ErrorInfo::new(notice.severity, notice.sqlstate, notice.message),
                )))
                .await?;
        }
        if let Some(age_ms) = frontier_age_ms {
            client
                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    "frontier_age_ms".to_string(),
                    age_ms.to_string(),
                )))
                .await?;
        }
        Ok(())
    }

    async fn do_query_single<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        query: &'a str,
        conn_id: &str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Drain pending notifications and send them before processing the query.
        // This delivers notifications from other connections to this one.
        {
            let pending = self.notify_registry.drain_outbox(conn_id);
            for (channel, payload, pid) in pending {
                let notif = NotificationResponse::new(pid, channel, payload);
                client
                    .feed(PgWireBackendMessage::NotificationResponse(notif))
                    .await
                    .map_err(PgWireError::from)?;
            }
            client.flush().await.map_err(PgWireError::from)?;
        }

        // COPY IN: enter COPY IN mode, store CopyState, return CopyInResponse.
        let ql = query.trim().to_lowercase();
        if ql.starts_with("copy ") && ql.contains(" from stdin") {
            return self.handle_copy_from_stdin(query, conn_id);
        }

        // COPY OUT: stream CopyData messages directly through the client sink.
        if ql.starts_with("copy ") && ql.contains(" to stdout") {
            if let Some(view_name) = parse_copy_to_stdout_view(query) {
                if self.catalog.get_view(&view_name).is_some()
                    && !self.catalog.is_backfill_published(&view_name)
                {
                    return Ok(backfill_not_published_response(&view_name));
                }
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

        // DEALLOCATE [ALL] or DEALLOCATE <statement_name>
        if ql.starts_with("deallocate") {
            let q_trim = query.trim().trim_end_matches(';');
            let name_part = q_trim["deallocate".len()..].trim();
            let name_part_lower = name_part.to_lowercase();
            if name_part_lower == "all" {
                self.prepared_statements.remove(conn_id);
                self.active_portals.remove(conn_id);
                self.portal_states.retain(|k, _| k.0 != conn_id);
            } else {
                let stmt_name = name_part.trim_matches('"').trim_matches('\'');
                if let Some(mut stmts) = self.prepared_statements.get_mut(conn_id) {
                    stmts.pop(stmt_name);
                }
            }
            return Ok(vec![Response::Execution(Tag::new("DEALLOCATE"))]);
        }

        let responses = self.dispatch_async_with_conn(query, Some(conn_id)).await?;
        self.emit_session_annotations(client, conn_id).await?;
        Ok(responses)
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
    async fn dispatch_sync<'a>(
        &'a self,
        query: &'a str,
        session_info: &crate::catalog_stubs::SessionInfo,
    ) -> Option<PgWireResult<Vec<Response<'a>>>> {
        let q = query.trim();
        let ql = q.to_lowercase();

        // SERIALIZABLE → RS-2003
        if ql.contains("serializable") && ql.contains("isolation") {
            return Some(Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25001".to_owned(),
                "[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED".to_owned(),
            )))]));
        }

        // REPEATABLE READ → RS-2004
        if ql.contains("repeatable read") && ql.contains("isolation") {
            return Some(Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25001".to_owned(),
                "[RS-2004] isolation.repeatable_read_not_supported: REPEATABLE READ isolation is not supported; use READ COMMITTED".to_owned(),
            )))]));
        }

        // Catalog stubs
        if let Some(catalog_resp) = self.catalog.handle_query(q, session_info) {
            return Some(Ok(vec![catalog_resp_to_response(catalog_resp)]));
        }

        // ── Slice 6: pg_stat_activity virtual table ────────────────────────────
        // Handle: SELECT ... FROM pg_stat_activity [WHERE ...]
        if ql.contains("pg_stat_activity") && ql.contains("from") {
            let schema = Arc::new(vec![
                FieldInfo::new("pid".to_string(), None, None, Type::INT4, FieldFormat::Text),
                FieldInfo::new(
                    "usename".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "application_name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "state".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "query".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "query_start".to_string(),
                    None,
                    None,
                    Type::TIMESTAMP,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "client_addr".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
            ]);
            // Snapshot sessions
            let snapshot: Vec<(u32, String, String, String)> = self
                .sessions
                .iter()
                .map(|entry| {
                    let session = entry.value();
                    let pid = session.backend_pid;
                    let usename = match &session.principal {
                        crate::auth::Principal::Jwt { sub } => sub.clone(),
                        _ => "postgres".to_string(),
                    };
                    let app_name = session.application_name.clone();
                    let state = match session.tx_status {
                        crate::session::TxStatus::Idle => "idle".to_string(),
                        _ => "active".to_string(),
                    };
                    (pid, usename, app_name, state)
                })
                .collect();
            let schema_ref = schema.clone();
            let data_stream = stream::iter(snapshot).map(move |(pid, usename, app_name, state)| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&Some(pid.to_string().as_str()))?;
                encoder.encode_field(&Some(usename.as_str()))?;
                encoder.encode_field(&Some(app_name.as_str()))?;
                encoder.encode_field(&Some(state.as_str()))?;
                encoder.encode_field(&Some(""))?; // query
                encoder.encode_field::<Option<&str>>(&None)?; // query_start
                encoder.encode_field::<Option<&str>>(&None)?; // client_addr
                encoder.finish()
            });
            return Some(Ok(vec![promote_response(Response::Query(
                QueryResponse::new(schema, data_stream),
            ))]));
        }
        // ── End Slice 6 ────────────────────────────────────────────────────────

        // COPY <view> TO STDOUT — handled via streaming in do_query; skip here.
        // CREATE VIEW / CREATE MATERIALIZED VIEW
        if ql.starts_with("create view ")
            || ql.starts_with("create materialized view ")
            || ql.starts_with("create or replace view ")
        {
            return Some(self.handle_create_view(q).await);
        }

        // REFRESH MATERIALIZED VIEW
        if ql.starts_with("refresh materialized view ") {
            return Some(Ok(self.handle_refresh_materialized_view(q)));
        }

        // CREATE TABLE [IF NOT EXISTS] — register in catalog
        if ql.starts_with("create table ") || ql.starts_with("create table if not exists ") {
            return Some(self.handle_create_table(q));
        }

        // CREATE SINK — v0.44 pgwire DDL wiring
        if ql.starts_with("create sink ") {
            return Some(self.handle_create_sink(q));
        }

        // CREATE SOURCE / ALTER SOURCE / DROP SOURCE — v0.51.9 pgwire DDL wiring
        if ql.starts_with("create source ") {
            return Some(self.handle_create_source(q));
        }
        if ql.starts_with("alter source ") || ql.starts_with("drop source ") {
            return Some(self.handle_alter_source(q));
        }

        // CREATE INDEX / DROP INDEX / REBUILD INDEX / MARK INDEX READY — v0.32 pgwire DDL wiring

        if ql.starts_with("create index ") {
            return Some(self.handle_create_index(q).await);
        }
        if ql.starts_with("drop index ") {
            return Some(self.handle_drop_index(q));
        }
        if ql.starts_with("rebuild index ") {
            return Some(self.handle_rebuild_index(q));
        }

        // BEGIN is handled in dispatch_async_with_conn (needs session state for idempotency).

        // COMMIT and ROLLBACK are handled in dispatch_async_with_conn (need write buffer access).

        None
    }

    /// Dispatch with an optional connection ID for write buffer routing.
    pub async fn dispatch_async_with_conn(
        &self,
        query: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let q = query.trim();
        let ql = q.to_lowercase();

        // ── Aborted-transaction guard ────────────────────────────────────────────
        // Any command inside a failed block is bounced with SQLSTATE 25P02, except
        // ROLLBACK (which exits the failed block) and ROLLBACK TO <name> (which
        // re-activates the block from a savepoint).
        if let Some(id) = conn_id {
            let in_failed = self
                .sessions
                .get(id)
                .map(|s| s.is_in_failed_block())
                .unwrap_or(false);
            if in_failed {
                let is_rollback =
                    ql == "rollback" || ql == "rollback;" || ql.starts_with("rollback to");
                if !is_rollback {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "25P02".to_owned(),
                            "[RS-2560] transaction.in_failed_sql_transaction: query cannot run inside a failed transaction block. next_steps: Issue ROLLBACK to exit the failed block, then retry.".to_owned(),
                        ),
                    )))]);
                }
            }

            if ql.starts_with("mark index ") {
                return self.handle_mark_index_ready(q).await;
            }
        }

        // ── BEGIN — with idempotency (already in transaction → silent succeed) ──
        if ql == "begin" || ql == "begin;" || ql.starts_with("begin ") {
            // BEGIN ISOLATION LEVEL SERIALIZABLE / REPEATABLE READ → RS-2003 / RS-2004,
            // the same honest rejection dispatch_sync applies to SET TRANSACTION.
            if ql.contains("isolation") && ql.contains("serializable") {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        "25001".to_owned(),
                        "[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED".to_owned(),
                    ),
                )))]);
            }
            if ql.contains("isolation") && ql.contains("repeatable read") {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        "25001".to_owned(),
                        "[RS-2004] isolation.repeatable_read_not_supported: REPEATABLE READ isolation is not supported; use READ COMMITTED".to_owned(),
                    ),
                )))]);
            }
            if let Some(id) = conn_id {
                let tx_status = self
                    .sessions
                    .get(id)
                    .map(|s| s.tx_status)
                    .unwrap_or(crate::session::TxStatus::Idle);
                if tx_status == crate::session::TxStatus::InTransaction {
                    // Already in a transaction — succeed silently (Postgres issues a warning
                    // but we keep it simple in v0.41).
                    return Ok(vec![promote_response(Response::Execution(Tag::new(
                        "BEGIN",
                    )))]);
                }
                let mut session = self.sessions.entry(id.to_string()).or_default();
                session.begin_explicit();
            }
            return Ok(vec![promote_response(Response::TransactionStart(
                Tag::new("BEGIN"),
            ))]);
        }

        // ── END alias for COMMIT ─────────────────────────────────────────────────
        if ql == "end" || ql == "end;" {
            let result = self.handle_commit(conn_id).await;
            if let Some(id) = conn_id {
                if result.is_ok() {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.end_transaction();
                    }
                }
            }
            return result;
        }

        // ── SAVEPOINT commands ────────────────────────────────────────────────────
        if ql.starts_with("savepoint ") {
            let name = q["savepoint ".len()..].trim().trim_end_matches(';').trim();
            if let Some(id) = conn_id {
                let in_block = self
                    .sessions
                    .get(id)
                    .map(|s| s.in_explicit_block)
                    .unwrap_or(false);
                if !in_block {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "3B001".to_owned(),
                            "SAVEPOINT can only be used in transaction blocks".to_owned(),
                        ),
                    )))]);
                }
                self.write_buffers
                    .entry(id.to_string())
                    .or_default()
                    .create_savepoint(name)
                    .map_err(PgWireError::from)?;
            }
            return Ok(vec![promote_response(Response::Execution(Tag::new(
                "SAVEPOINT",
            )))]);
        }

        if ql.starts_with("release savepoint ") || ql.starts_with("release ") {
            let after = if ql.starts_with("release savepoint ") {
                &q["release savepoint ".len()..]
            } else {
                &q["release ".len()..]
            };
            let name = after.trim().trim_end_matches(';').trim();
            if let Some(id) = conn_id {
                self.write_buffers
                    .entry(id.to_string())
                    .or_default()
                    .release_savepoint(name)
                    .map_err(PgWireError::from)?;
            }
            return Ok(vec![promote_response(Response::Execution(Tag::new(
                "RELEASE",
            )))]);
        }

        if ql.starts_with("rollback to savepoint ") || ql.starts_with("rollback to ") {
            let after = if ql.starts_with("rollback to savepoint ") {
                &q["rollback to savepoint ".len()..]
            } else {
                &q["rollback to ".len()..]
            };
            let name = after.trim().trim_end_matches(';').trim();
            if let Some(id) = conn_id {
                self.write_buffers
                    .entry(id.to_string())
                    .or_default()
                    .rollback_to_savepoint(name)
                    .map_err(PgWireError::from)?;
                // ROLLBACK TO reactivates a failed transaction block.
                if let Some(mut session) = self.sessions.get_mut(id) {
                    if session.tx_status == crate::session::TxStatus::Failed {
                        session.tx_status = crate::session::TxStatus::InTransaction;
                    }
                }
            }
            return Ok(vec![promote_response(Response::Execution(Tag::new(
                "ROLLBACK",
            )))]);
        }

        // Two-phase commit — not supported.
        if ql.starts_with("prepare transaction")
            || ql.starts_with("commit prepared")
            || ql.starts_with("rollback prepared")
        {
            return Err(crate::error::GatewayError::TwoPhaseNotSupported.into());
        }
        // ─────────────────────────────────────────────────────────────────────────

        // ── S8: LISTEN/UNLISTEN/NOTIFY ────────────────────────────────────────────
        if ql == "listen" || ql.starts_with("listen ") {
            if let Some(id) = conn_id {
                let channel = q["listen".len()..].trim().trim_end_matches(';').trim();
                if !channel.is_empty() {
                    self.notify_registry
                        .subscribe(channel, id)
                        .map_err(PgWireError::from)?;
                }
            }
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("LISTEN").with_rows(0),
            ))]);
        }

        if ql == "unlisten" || ql == "unlisten *" || ql.starts_with("unlisten ") {
            if let Some(id) = conn_id {
                let rest = q["unlisten".len()..].trim().trim_end_matches(';').trim();
                if rest.is_empty() || rest == "*" {
                    self.notify_registry.unsubscribe_all(id);
                    self.pending_notifies.remove(id);
                } else {
                    self.notify_registry.unsubscribe(rest, id);
                }
            }
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("UNLISTEN").with_rows(0),
            ))]);
        }

        if ql == "notify" || ql.starts_with("notify ") {
            let rest = q["notify".len()..].trim().trim_end_matches(';').trim();
            let (channel, payload) = if let Some(comma_pos) = rest.find(',') {
                let ch = rest[..comma_pos].trim().to_string();
                let pl = rest[comma_pos + 1..]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string();
                (ch, pl)
            } else {
                (rest.to_string(), String::new())
            };
            if let Some(id) = conn_id {
                let in_block = self
                    .sessions
                    .get(id)
                    .map(|s| s.in_explicit_block)
                    .unwrap_or(false);
                if in_block {
                    let mut outbox = self.pending_notifies.entry(id.to_string()).or_default();
                    if outbox.len() < crate::notify_registry::MAX_OUTBOX_PER_CONNECTION {
                        outbox.push((channel, payload));
                    }
                } else {
                    let sender_pid = self
                        .sessions
                        .get(id)
                        .map(|s| s.backend_pid as i32)
                        .unwrap_or(0);
                    self.notify_registry.deliver(&channel, &payload, sender_pid);
                }
            } else {
                self.notify_registry.deliver(&channel, &payload, 0);
            }
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("NOTIFY").with_rows(0),
            ))]);
        }
        // ─────────────────────────────────────────────────────────────────────────

        // SET rockstream.* must be intercepted before catalog stubs handle generic SET commands.
        if ql.starts_with("set rockstream.") || ql.starts_with("set local rockstream.") {
            return self.handle_set_rockstream(q, &ql, conn_id);
        }

        // SET search_path = <namespace> (v0.26 namespace isolation)
        if ql.starts_with("set search_path") || ql.starts_with("set local search_path") {
            let is_local = ql.starts_with("set local search_path");
            if let Some(id) = conn_id {
                // Extract namespace: SET search_path = <ns> or SET search_path TO <ns>
                let after_eq = if let Some(pos) = ql.find('=') {
                    q[pos + 1..]
                        .trim()
                        .trim_end_matches(';')
                        .trim()
                        .trim_matches('\'')
                        .to_string()
                } else if let Some(pos) = q.to_lowercase().find(" to ") {
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
                let mut session = self.sessions.entry(id.to_string()).or_default();
                if is_local {
                    // SET LOCAL: store in local_guc_params; cleared at ROLLBACK/COMMIT.
                    session
                        .local_guc_params
                        .insert("search_path".to_string(), ns);
                } else {
                    session.current_namespace = ns.clone();
                    session.search_path = ns.clone();
                    session.guc_params.insert("search_path".to_string(), ns);
                    session.search_path_set = true;
                }
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

        if ql.starts_with("explain incremental ") {
            let explain_args = q["explain incremental ".len()..]
                .trim()
                .trim_end_matches(';');
            let (level, target_sql) = if let Some(rest) = explain_args.strip_prefix("VERBOSE ") {
                (ExplainLevel::Verbose, rest.trim())
            } else if let Some(rest) = explain_args.strip_prefix("ANALYZE ") {
                (ExplainLevel::Analyze, rest.trim())
            } else if let Some(rest) = explain_args.strip_prefix("ESTIMATE ") {
                (ExplainLevel::Default, rest.trim())
            } else {
                (ExplainLevel::Default, explain_args)
            };

            let normalized_sql = if target_sql.to_ascii_lowercase().starts_with("select ")
                || target_sql.to_ascii_lowercase().starts_with("with ")
            {
                target_sql.to_string()
            } else {
                format!("SELECT * FROM {}", target_sql.trim())
            };

            let frontend = self.build_explain_frontend()?;
            let explain_text = if explain_args.starts_with("ESTIMATE ") {
                frontend
                    .explain_incremental_estimate_text(&normalized_sql, 1_000, 10_000)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?
            } else {
                let stats = if level == ExplainLevel::Analyze {
                    rockstream_control::ControlService::new(
                        rockstream_control::TopologyCatalog::new(),
                    )
                    .collect_operator_stats(0)
                } else {
                    Vec::new()
                };
                frontend
                    .explain_incremental_for_sql(&normalized_sql, level, &stats)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?
            };

            let schema = Arc::new(vec![FieldInfo::new(
                "QUERY PLAN".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let schema_ref = schema.clone();
            let data_stream = stream::iter(vec![explain_text]).map(move |line| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&Some(line.as_str()))?;
                encoder.finish()
            });
            return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                schema,
                data_stream,
            )))]);
        }

        // EXPLAIN <query> — return plan annotation with pushdown info and index state.
        if ql.starts_with("explain ") {
            let inner_sql = q["explain ".len()..].trim();
            let pushdown = crate::multi_shard_reader::can_pushdown_partial_agg(inner_sql);
            let pushdown_note = if pushdown {
                "partial_pushdown: true  -- O(distinct_groups × shards) rows returned"
            } else {
                "partial_pushdown: false"
            };

            // Surface index state in EXPLAIN (RS-2014 / RS-2015 hints). Scan the
            // gateway's index catalog for any index covering a table that appears
            // in the query, and annotate the plan text accordingly.
            let index_note = {
                use crate::catalog_stubs::CatalogIndexState;
                let inner_upper = inner_sql.to_uppercase();
                let mut note = String::new();
                for idx_name in self.catalog.list_index_names() {
                    if let Some(entry) = self.catalog.get_index(&idx_name) {
                        let table_upper = entry.table.to_uppercase();
                        if inner_upper.contains(&table_upper) {
                            match entry.state {
                                CatalogIndexState::Building => {
                                    note = format!(
                                        "\n[RS-2014] index '{}' on table '{}' is BUILDING — \
                                         falling back to shard scan. \
                                         Wait for READY state before relying on index scan.",
                                        entry.name, entry.table
                                    );
                                }
                                CatalogIndexState::Ready => {
                                    let cols = entry.index_cols.join(", ");
                                    note = format!(
                                        "\nindex_scan: index='{}' table='{}' cols=[{}] state=READY",
                                        entry.name, entry.table, cols
                                    );
                                }
                            }
                            break;
                        }
                    }
                }
                note
            };

            // Surface sink registration on EXPLAIN (v0.44 slice 10, P4): if
            // the queried view has a `CREATE SINK` registered against it,
            // append the sink's target/format/state so operators can see at
            // a glance where a view's incremental output is landing. This
            // is intentionally just an annotation — wiring the full
            // `EXPLAIN INCREMENTAL` plan library stays v0.45 scope.
            let sink_note = {
                let inner_upper = inner_sql.to_uppercase();
                let mut note = String::new();
                for entry in self.catalog.list_sinks() {
                    let view_upper = entry.view.to_uppercase();
                    if inner_upper.contains(&view_upper) {
                        let last_snapshot_epoch = entry
                            .last_snapshot_epoch
                            .map(|epoch| epoch.to_string())
                            .unwrap_or_else(|| "none".to_string());
                        note = format!(
                            "\nsink_target: name='{}' format={} path='{}' last_snapshot_epoch={} state={}",
                            entry.name,
                            entry.format.to_uppercase(),
                            entry.path,
                            last_snapshot_epoch,
                            entry.state
                        );
                        break;
                    }
                }
                note
            };

            let shard_note = build_scatter_explain_note(&self.catalog, inner_sql);

            // Slice 4 (v0.51.2): replace the string-concatenation stub with a
            // real DataFusion plan tree. Reuses Slice 1's SessionContext +
            // MemTable registration (`query_time_datafusion_select`) so the
            // real scan/filter/join node names come from DataFusion's own
            // `Display` output, not a hand-written string. The pushdown/
            // index/sink/shard annotations are preserved as extra rows after
            // the real plan — no information is lost, only the fabricated
            // "Plan: SeqScan → …" one-liner is removed.
            let mut plan_rows: Vec<String> = match self.query_time_shard_topology().await {
                Ok(topology) => {
                    let analyzed = analyze_select_query(&self.catalog, inner_sql);
                    let referenced_tables = analyzed
                        .as_ref()
                        .map(|a| a.referenced_tables.clone())
                        .unwrap_or_default();
                    match query_time_datafusion_select(
                        &self.catalog,
                        &topology,
                        &format!("EXPLAIN {inner_sql}"),
                        &referenced_tables,
                    )
                    .await
                    {
                        Ok(batches) => explain_batches_to_plan_lines(&batches),
                        Err(e) => vec![format!("Plan: SeqScan (DataFusion explain failed: {e})")],
                    }
                }
                Err(e) => vec![format!(
                    "Plan: SeqScan (query-time scatter unavailable: {e})"
                )],
            };
            for note in [
                pushdown_note.to_string(),
                index_note.clone(),
                sink_note.clone(),
                shard_note.clone(),
            ] {
                let trimmed = note.trim();
                if !trimmed.is_empty() {
                    plan_rows.push(trimmed.to_string());
                }
            }
            plan_rows.push(format!("Query: {inner_sql}"));

            let schema = Arc::new(vec![FieldInfo::new(
                "QUERY PLAN".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);
            let rows = plan_rows;
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

        // S6: SET TRANSACTION ISOLATION LEVEL / SET TRANSACTION READ ONLY|WRITE
        if ql.starts_with("set transaction") || ql.starts_with("set local transaction") {
            if ql.contains("isolation level") {
                if ql.contains("serializable") || ql.contains("repeatable read") {
                    // Fall through to dispatch_sync which returns RS-2003/RS-2004.
                } else if let Some(id) = conn_id {
                    let mut session = self.sessions.entry(id.to_string()).or_default();
                    session.isolation_level = crate::session::IsolationLevel::ReadCommitted;
                    return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
                } else {
                    return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
                }
            } else {
                // SET TRANSACTION READ ONLY / READ WRITE — accept silently.
                return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
            }
        }

        // S8: Generic SET <key> [=|TO] <value> — store in session GUC params.
        // Must come after the specific SET handlers (SET rockstream.*, SET search_path).
        // Exclude SET TRANSACTION so dispatch_sync can enforce SERIALIZABLE → RS-2003.
        if (ql.starts_with("set ") || ql.starts_with("set local "))
            && !ql.starts_with("set rockstream.")
            && !ql.starts_with("set local rockstream.")
            && !ql.starts_with("set search_path")
            && !ql.starts_with("set local search_path")
            && !ql.starts_with("set transaction")
            && !ql.starts_with("set local transaction")
        {
            let is_local = ql.starts_with("set local ");
            if let Some(id) = conn_id {
                let remainder = if is_local {
                    &q["set local ".len()..]
                } else {
                    &q["set ".len()..]
                };
                let remainder = remainder.trim().trim_end_matches(';');
                let (key, raw_val) = if let Some(eq_pos) = remainder.find('=') {
                    (
                        remainder[..eq_pos].trim().to_lowercase(),
                        remainder[eq_pos + 1..].trim().to_string(),
                    )
                } else if let Some(to_pos) = remainder.to_lowercase().find(" to ") {
                    (
                        remainder[..to_pos].trim().to_lowercase(),
                        remainder[to_pos + 4..].trim().to_string(),
                    )
                } else {
                    (String::new(), String::new())
                };
                if !key.is_empty() {
                    let val = raw_val
                        .trim_end_matches(';')
                        .trim()
                        .trim_matches('\'')
                        .trim_matches('"')
                        .to_string();
                    let mut session = self.sessions.entry(id.to_string()).or_default();
                    if is_local {
                        session.local_guc_params.insert(key, val);
                    } else if session.guc_params.len() < crate::session::MAX_GUC_PARAMS
                        || session.guc_params.contains_key(&key)
                    {
                        session.guc_params.insert(key, val);
                    }
                }
            }
            return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
        }

        // S8: SHOW <key> — return from session GUC params or session fields.
        if ql.trim_end_matches(';') == "show pipeline stalls"
            || ql.trim_end_matches(';') == "show frontiers"
        {
            let cols = vec![
                "view_name".to_string(),
                "op_id".to_string(),
                "shard_id".to_string(),
                "frontier_epoch".to_string(),
                "is_slowest_input".to_string(),
                "is_holding_back_commit".to_string(),
                "lag_behind_max_ms".to_string(),
            ];
            let snapshots = rockstream_types::metrics::pipeline_stall_report(None);
            let rows = snapshots
                .into_iter()
                .map(|s| {
                    vec![
                        Some(s.view_name),
                        Some(s.op_id.0.to_string()),
                        Some(s.shard_id.to_string()),
                        Some(s.frontier_epoch.to_string()),
                        Some(s.is_slowest_input.to_string()),
                        Some(s.is_holding_back_commit.to_string()),
                        Some(s.lag_behind_max_ms.to_string()),
                    ]
                })
                .collect();
            return Ok(vec![promote_response(catalog_resp_to_response(
                CatalogResponse::Rows {
                    columns: cols,
                    rows,
                },
            ))]);
        }
        if ql.starts_with("show pipeline stalls for ") {
            let view_name = q["show pipeline stalls for ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('"');
            if self.catalog.get_view(view_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-1001] view.not_found: view '{view_name}' does not exist. Next steps: check SHOW VIEWS for active pipelines"
                    ),
                ))))]);
            }
            let cols = vec![
                "view_name".to_string(),
                "op_id".to_string(),
                "shard_id".to_string(),
                "frontier_epoch".to_string(),
                "is_slowest_input".to_string(),
                "is_holding_back_commit".to_string(),
                "lag_behind_max_ms".to_string(),
            ];
            let snapshots = rockstream_types::metrics::pipeline_stall_report(Some(view_name));
            let rows = snapshots
                .into_iter()
                .map(|s| {
                    vec![
                        Some(s.view_name),
                        Some(s.op_id.0.to_string()),
                        Some(s.shard_id.to_string()),
                        Some(s.frontier_epoch.to_string()),
                        Some(s.is_slowest_input.to_string()),
                        Some(s.is_holding_back_commit.to_string()),
                        Some(s.lag_behind_max_ms.to_string()),
                    ]
                })
                .collect();
            return Ok(vec![promote_response(catalog_resp_to_response(
                CatalogResponse::Rows {
                    columns: cols,
                    rows,
                },
            ))]);
        }
        if ql.starts_with("show arrangement ") {
            let rest = q["show arrangement ".len()..].trim().trim_end_matches(';');
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            if tokens.len() < 3 {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "[RS-2001] syntax.error: SHOW ARRANGEMENT requires <view_name> <op_id> <key>. Next steps: specify view_name, op_id, and key".to_owned(),
                ))))]);
            }
            let view_name = tokens[0].trim_matches('"');
            let op_id_str = tokens[1];
            let key_str = tokens[2].trim_matches('\'').trim_matches('"');

            if self.catalog.get_view(view_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-1001] view.not_found: view '{view_name}' does not exist. Next steps: check SHOW VIEWS for active pipelines"
                    ),
                ))))]);
            }

            let op_id: u64 = match op_id_str.parse() {
                Ok(id) => id,
                Err(_) => {
                    return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        format!(
                            "[RS-2001] syntax.error: invalid op_id '{op_id_str}'. Next steps: specify a numeric operator ID"
                        ),
                    ))))]);
                }
            };

            let cols = vec![
                "view_name".to_string(),
                "op_id".to_string(),
                "key".to_string(),
                "weight".to_string(),
                "epoch".to_string(),
            ];

            let mut rows = Vec::new();
            if let Some(shard_db) = &self.shard_db {
                let mut db_key = Vec::with_capacity(9 + key_str.len());
                db_key.push(0x01);
                db_key.extend_from_slice(&op_id.to_be_bytes());
                if let Ok(num_key) = key_str.parse::<i64>() {
                    db_key.extend_from_slice(&num_key.to_be_bytes());
                } else {
                    db_key.extend_from_slice(key_str.as_bytes());
                }

                if let Ok(Some(val)) = shard_db.get(&db_key).await {
                    let weight = if val.len() >= 16 {
                        i64::from_be_bytes(val[8..16].try_into().unwrap_or([0; 8]))
                    } else if val.len() >= 8 {
                        i64::from_be_bytes(val[0..8].try_into().unwrap_or([0; 8]))
                    } else {
                        1i64
                    };
                    let published_epoch = self.view_reader.published_frontier().unwrap_or(0);
                    rows.push(vec![
                        Some(view_name.to_string()),
                        Some(op_id.to_string()),
                        Some(key_str.to_string()),
                        Some(weight.to_string()),
                        Some(published_epoch.to_string()),
                    ]);
                }
            } else if let Ok(Some((weight, epoch))) = self
                .view_reader
                .peek_arrangement(view_name, op_id, key_str)
                .await
            {
                rows.push(vec![
                    Some(view_name.to_string()),
                    Some(op_id.to_string()),
                    Some(key_str.to_string()),
                    Some(weight.to_string()),
                    Some(epoch.to_string()),
                ]);
            }

            return Ok(vec![promote_response(catalog_resp_to_response(
                CatalogResponse::Rows {
                    columns: cols,
                    rows,
                },
            ))]);
        }
        if ql.trim_end_matches(';') == "show sources" {
            return Ok(vec![promote_response(catalog_resp_to_response(
                self.catalog.sources_response(),
            ))]);
        }
        if ql.trim_end_matches(';') == "show source status" {
            return Ok(vec![promote_response(catalog_resp_to_response(
                self.catalog.source_status_response(None),
            ))]);
        }
        if ql.starts_with("show source status for ") {
            let source_name = q["show source status for ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('"');
            if self.catalog.get_source(source_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-4009] source.not_found: source '{}' does not exist. Next steps: run CREATE SOURCE ... to create it.",
                        source_name
                    ),
                ))))]);
            }
            return Ok(vec![promote_response(catalog_resp_to_response(
                self.catalog.source_status_response(Some(source_name)),
            ))]);
        }
        if ql.starts_with("show backfill status for materialized view ") {
            let view_name = q["show backfill status for materialized view ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('"');
            if self.catalog.get_view(view_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-4022] backfill.not_published: materialized view '{}' does not exist. Next steps: run CREATE MATERIALIZED VIEW {} AS SELECT ... first.",
                        view_name, view_name
                    ),
                ))))]);
            }
            return Ok(vec![promote_response(catalog_resp_to_response(
                self.catalog.backfill_status_response(view_name),
            ))]);
        }
        if ql.starts_with("show backfill status") {
            return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "[RS-2001] sql.invalid_syntax: expected SHOW BACKFILL STATUS FOR MATERIALIZED VIEW <name>. Next steps: provide a materialized view name.".to_owned(),
            ))))]);
        }
        if ql.trim_end_matches(';') == "show resource usage" {
            return Ok(vec![catalog_resp_to_response(
                self.catalog.view_resource_usage(&[]),
            )]);
        }
        if ql.trim_end_matches(';') == "show workload status" {
            return Ok(vec![catalog_resp_to_response(
                self.catalog.workload_status(),
            )]);
        }
        if ql.starts_with("show workload status for ") {
            let workload_name = q["show workload status for ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('"');
            if self.catalog.get_workload(workload_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before assigning views to it.",
                        workload_name, workload_name
                    ),
                ))))]);
            }
            let response = if let CatalogResponse::Rows { columns: _, rows } =
                self.catalog.workload_status()
            {
                let matching = rows
                    .into_iter()
                    .filter(|row| {
                        row.first().and_then(|value| value.as_deref()) == Some(workload_name)
                    })
                    .collect();
                CatalogResponse::Rows {
                    columns: crate::catalog_stubs::workload_status_columns(),
                    rows: matching,
                }
            } else {
                CatalogResponse::Rows {
                    columns: crate::catalog_stubs::workload_status_columns(),
                    rows: Vec::new(),
                }
            };
            return Ok(vec![catalog_resp_to_response(response)]);
        }
        if ql.starts_with("show resource usage for workload ") {
            let workload_name = q["show resource usage for workload ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim_matches('"');
            if self.catalog.get_workload(workload_name).is_none() {
                return Ok(vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!(
                        "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before assigning views to it.",
                        workload_name, workload_name
                    ),
                ))))]);
            }
            let response = if let CatalogResponse::Rows { columns: _, rows } =
                self.catalog.workload_resource_usage(&[])
            {
                let matching = rows
                    .into_iter()
                    .filter(|row| {
                        row.first().and_then(|value| value.as_deref()) == Some(workload_name)
                    })
                    .collect();
                CatalogResponse::Rows {
                    columns: crate::catalog_stubs::workload_resource_usage_columns(),
                    rows: matching,
                }
            } else {
                CatalogResponse::Rows {
                    columns: crate::catalog_stubs::workload_resource_usage_columns(),
                    rows: Vec::new(),
                }
            };
            return Ok(vec![catalog_resp_to_response(response)]);
        }
        if ql.trim_end_matches(';') == "show cluster resource usage" {
            let columns = crate::catalog_stubs::workload_resource_usage_columns();
            let row = crate::catalog_stubs::project_workload_resource_usage(
                &columns,
                &self.catalog.cluster_resource_usage_entry(),
            );
            return Ok(vec![catalog_resp_to_response(CatalogResponse::Rows {
                columns,
                rows: vec![row],
            })]);
        }
        if let Some(rest) = ql.strip_prefix("show ") {
            let key_raw = rest.trim().trim_end_matches(';').to_string();
            let session_val: Option<String> = if let Some(id) = conn_id {
                self.sessions.get(id).map(|s| {
                    // local_guc_params first (SET LOCAL), then guc_params, then session fields
                    if let Some(v) = s.effective_guc(&key_raw) {
                        return v.to_owned();
                    }
                    match key_raw.as_str() {
                        "rockstream.session_mode" => s.session_mode().to_string(),
                        "search_path" => s.search_path.clone(),
                        "client_encoding" | "server_encoding" => "UTF8".to_string(),
                        "timezone" => "UTC".to_string(),
                        "application_name" => s.application_name.clone(),
                        _ => String::new(),
                    }
                })
            } else {
                None
            };
            if let Some(val) = session_val {
                // Only intercept if we have a non-empty value or it's search_path
                if !val.is_empty() || key_raw == "search_path" {
                    let schema = Arc::new(vec![FieldInfo::new(
                        key_raw.clone(),
                        None,
                        None,
                        Type::TEXT,
                        FieldFormat::Text,
                    )]);
                    let schema_ref = schema.clone();
                    let data_stream = stream::iter(vec![val]).map(move |v| {
                        let mut encoder = DataRowEncoder::new(schema_ref.clone());
                        encoder.encode_field(&Some(v.as_str()))?;
                        encoder.finish()
                    });
                    return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                        schema,
                        data_stream,
                    )))]);
                }
            }
            // Fall through to catalog stubs for static SHOW values (server_version, etc.)
        }

        // Build SessionInfo for catalog handlers
        let session_info = if let Some(id) = conn_id {
            if let Some(session) = self.sessions.get(id) {
                crate::catalog_stubs::SessionInfo {
                    backend_pid: session.backend_pid,
                    search_path: session.search_path.clone(),
                    principal_name: session.principal.identity().to_string(),
                }
            } else {
                crate::catalog_stubs::SessionInfo::default()
            }
        } else {
            crate::catalog_stubs::SessionInfo::default()
        };

        if ql.starts_with("create workload ") {
            return self.handle_create_workload(query).await;
        }
        if ql.starts_with("alter workload ") {
            return self.handle_alter_workload(query).await;
        }
        if ql.starts_with("drop workload ") {
            return self.handle_drop_workload(query).await;
        }

        if let Some(result) = self.dispatch_sync(query, &session_info).await {
            // Promote lifetime — responses from dispatch_sync hold no borrows
            // from `query`, only owned data.
            let result = result.map(|v| v.into_iter().map(promote_response).collect());
            let is_error = result.is_err()
                || result.as_ref().is_ok_and(|v: &Vec<Response<'_>>| {
                    v.iter().any(|r| matches!(r, Response::Error(_)))
                });
            if is_error {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.fail_transaction();
                    }
                }
            }
            return result;
        }

        // COMMIT — flush write buffer to shard atomically.
        if ql == "commit" || ql == "commit;" {
            let result = self.handle_commit(conn_id).await;
            if result.is_ok() {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.end_transaction();
                    }
                }
            }
            return result;
        }

        // ROLLBACK — discard write buffer.
        if ql == "rollback" || ql == "rollback;" {
            let result = self.handle_rollback(conn_id).await;
            if result.is_ok() {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.end_transaction();
                    }
                }
            }
            return result;
        }

        // ── Slice 5: PgBouncer compat commands ──────────────────────────────
        // DISCARD ALL: clear cursors, prepared statements, portals, session state.
        if ql == "discard all" || ql == "discard all;" {
            return self.handle_discard_all(conn_id);
        }
        // RESET ALL: reset GUC settings only (keep cursors, prepared statements).
        if ql == "reset all" || ql == "reset all;" {
            return self.handle_reset_all(conn_id);
        }
        // ─────────────────────────────────────────────────────────────────────

        // ── Slice 3: Named Cursor commands ────────────────────────────────────
        // DECLARE <name> CURSOR FOR <query>
        if ql.starts_with("declare ") && ql.contains(" cursor ") {
            return self.handle_declare_cursor(q, &ql, conn_id).await;
        }

        // FETCH [FORWARD] n FROM <name> | FETCH ALL FROM <name>
        if ql.starts_with("fetch ") {
            return self.handle_fetch_cursor(q, &ql, conn_id);
        }

        // MOVE [FORWARD] n FROM <name> | MOVE ALL FROM <name>
        if ql.starts_with("move ") {
            return self.handle_move_cursor(q, &ql, conn_id);
        }

        // CLOSE <name> | CLOSE ALL
        if ql.starts_with("close ") {
            return self.handle_close_cursor(q, &ql, conn_id);
        }
        // ─────────────────────────────────────────────────────────────────────

        // INSERT — accumulate in write buffer.
        if ql.starts_with("insert into ") {
            let result = self.handle_insert(q, conn_id).await;
            if result.is_err() {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.fail_transaction();
                    }
                }
            }
            return result;
        }

        // UPDATE — accumulate in write buffer.
        if ql.starts_with("update ") {
            let result = self.handle_update(q, conn_id).await;
            if result.is_err() {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.fail_transaction();
                    }
                }
            }
            return result;
        }

        // DELETE — accumulate in write buffer.
        if ql.starts_with("delete from ") {
            let result = self.handle_delete(q, conn_id).await;
            if result.is_err() {
                if let Some(id) = conn_id {
                    if let Some(mut session) = self.sessions.get_mut(id) {
                        session.fail_transaction();
                    }
                }
            }
            return result;
        }

        // SELECT … FROM <view> [LIMIT n]
        // Apply explicit wait_for or session RYW before reading (S8/S9).
        if ql.contains("from ") {
            if let Some(id) = conn_id {
                let query_after_fence = extract_after_fence_token(q);
                self.capture_max_staleness_metadata(id);
                let (wait_token, timeout_ms) = {
                    let mut session = self.sessions.entry(id.to_string()).or_default();
                    // Explicit wait_for takes priority; fall back to session RYW.
                    let explicit = query_after_fence.or_else(|| session.wait_for_token.take());
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

            if let Some(select_plan) = analyze_select_query(&self.catalog, q) {
                if let Some(top_level_relation) = &select_plan.top_level_relation {
                    if select_plan
                        .top_level_relation_full
                        .as_deref()
                        .is_some_and(is_system_relation_name)
                    {
                        // Fall through to catalog/system-table handling below.
                    } else if !top_level_relation.starts_with("pg_")
                        && !top_level_relation.starts_with("information_schema")
                    {
                        let target_rel = select_plan
                            .referenced_tables
                            .first()
                            .cloned()
                            .unwrap_or_else(|| top_level_relation.clone());

                        let has_view = self.catalog.get_view(&target_rel).is_some();
                        let has_table = self.catalog.get_table(&target_rel).is_some();
                        if !has_view && !has_table {
                            return Ok(vec![promote_response(Response::Error(Box::new(
                                ErrorInfo::new(
                                    "ERROR".to_string(),
                                    "42P01".to_string(),
                                    format!(
                                        "[RS-1004] relation.does_not_exist: relation \"{}\" does not exist. next_steps: Ensure table or view has been created and check search_path.",
                                        target_rel
                                    ),
                                ),
                            )))]);
                        }

                        if let Some(view_name) = select_plan.referenced_tables.iter().find(|name| {
                            self.catalog.get_view(name).is_some()
                                && !self.catalog.is_backfill_published(name)
                        }) {
                            return Ok(backfill_not_published_response(view_name));
                        }

                        let view_name = target_rel;
                        // S9: search_path-aware resolution for unqualified view names.
                        // Only applies when search_path was explicitly configured via SET.
                        let is_qualified = ql.contains(&format!(".{}", view_name.to_lowercase()));
                        if !is_qualified {
                            let (search_path, path_was_set) = if let Some(id) = conn_id {
                                self.sessions
                                    .get(id)
                                    .map(|s| (s.search_path.clone(), s.search_path_set))
                                    .unwrap_or_else(|| ("public".to_string(), false))
                            } else {
                                ("public".to_string(), false)
                            };
                            if path_was_set {
                                let path_parts: Vec<&str> =
                                    search_path.split(',').map(|s| s.trim()).collect();
                                // If the view exists but is not in the search_path, reject.
                                if self.catalog.get_view(&view_name).is_some()
                                    && self.catalog.resolve_view(&view_name, &path_parts).is_none()
                                {
                                    return Ok(vec![promote_response(Response::Error(Box::new(
                                    ErrorInfo::new(
                                        "ERROR".to_string(),
                                        "42P01".to_string(),
                                        format!(
                                            "relation \"{}\" does not exist (not in search_path: {})",
                                            view_name, search_path
                                        ),
                                    ),
                                )))]);
                                }
                            }
                        }

                        if select_plan.referenced_tables.len() == 1 {
                            if let Some(responses) =
                                self.maybe_index_point_lookup(q, &view_name).await?
                            {
                                return Ok(responses);
                            }
                            if let Some(responses) =
                                self.maybe_index_range_lookup(q, &view_name).await?
                            {
                                return Ok(responses);
                            }
                        }

                        let result = if select_plan.requires_query_time_datafusion
                            && self.shard_db.is_some()
                        {
                            let mut cancel_token = CANCEL_TOKEN
                                .try_with(|t| t.clone())
                                .unwrap_or_else(|_| CancelToken::new());
                            let query_fut = self.query_time_datafusion_response(
                                q,
                                &select_plan.referenced_tables,
                                conn_id,
                            );
                            tokio::select! {
                                res = query_fut => res,
                                _ = cancel_token.cancelled() => {
                                    Ok(vec![promote_response(Response::Error(Box::new(
                                        ErrorInfo::new(
                                            "ERROR".to_string(),
                                            "57014".to_string(),
                                            "[RS-2050] query.cancelled: query was cancelled by a client CancelRequest. next_steps: Retry the query or await client timeout settings.".to_string(),
                                        )
                                    )))])
                                }
                            }
                        } else {
                            let limit = extract_limit(q);
                            let order_by = extract_order_by(q);
                            // Wrap read in cancellation select (Slice 2)
                            let mut cancel_token = CANCEL_TOKEN
                                .try_with(|t| t.clone())
                                .unwrap_or_else(|_| CancelToken::new());
                            let read_fut =
                                self.read_view_response(&view_name, limit, order_by, conn_id);
                            tokio::select! {
                                res = read_fut => res,
                                _ = cancel_token.cancelled() => {
                                    Ok(vec![promote_response(Response::Error(Box::new(
                                        ErrorInfo::new(
                                            "ERROR".to_string(),
                                            "57014".to_string(),
                                            "[RS-2050] query.cancelled: query was cancelled by a client CancelRequest. next_steps: Retry the query or await client timeout settings.".to_string(),
                                        )
                                    )))])
                                }
                            }
                        };
                        let is_error = result.is_err()
                            || result.as_ref().is_ok_and(|v: &Vec<Response<'_>>| {
                                v.iter().any(|r| matches!(r, Response::Error(_)))
                            });
                        if is_error {
                            if let Some(id) = conn_id {
                                if let Some(mut session) = self.sessions.get_mut(id) {
                                    session.fail_transaction();
                                }
                            }
                        }
                        return result;
                    }
                }
            }
        }

        // DataFusion execution path for literal SELECT queries (no recognized FROM clause).
        // Handles queries like `SELECT 42`, `SELECT 42 AS n`, `SELECT now()`, etc.
        if ql.starts_with("select ") {
            if ql.contains("rockstream.write_fence()") && !ql.contains(" from ") {
                let fence = conn_id
                    .and_then(|id| self.sessions.get(id))
                    .and_then(|s| s.last_written_epoch.clone())
                    .map(|t| serde_json::to_string(&t).unwrap_or_default());
                let schema = Arc::new(vec![FieldInfo::new(
                    "fence".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                )]);
                let schema_ref = schema.clone();
                let data_stream = stream::iter(vec![fence]).map(move |value| {
                    let mut encoder = DataRowEncoder::new(schema_ref.clone());
                    encoder.encode_field(&value.as_deref())?;
                    encoder.finish()
                });
                return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                    schema,
                    data_stream,
                )))]);
            }
            if let Some(responses) = self.try_datafusion_select(q).await {
                return Ok(responses);
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new("OK")))])
    }

    /// Execute a SELECT query directly via DataFusion when it doesn't reference
    /// any catalog view/table (or references tables we can register as empty
    /// MemTables so DataFusion can resolve column types).
    ///
    /// Returns `Some(responses)` on success, `None` if DataFusion cannot execute
    /// the query (caller falls through to the `Tag::new("OK")` response).
    async fn try_datafusion_select(&self, q: &str) -> Option<Vec<Response<'static>>> {
        // Build a DataFusion session and register catalog objects as empty MemTables
        // so the planner can resolve any referenced names.
        let ctx = SessionContext::new();
        rockstream_sql::frontend::register_session_sql_udf(&ctx);
        for view in self.catalog.list_views() {
            let mut fields = Vec::new();
            for col in &view.columns {
                fields.push(Field::new(
                    &col.name,
                    string_to_arrow_datatype(&col.data_type),
                    true,
                ));
            }
            let schema = Arc::new(Schema::new(fields));
            if let Ok(mem_table) = MemTable::try_new(schema, vec![vec![]]) {
                let _ = ctx.register_table(
                    datafusion::sql::TableReference::from(view.name.as_str()),
                    Arc::new(mem_table),
                );
            }
        }
        for table in self.catalog.list_tables() {
            let mut fields = Vec::new();
            for col in &table.columns {
                fields.push(Field::new(
                    &col.name,
                    string_to_arrow_datatype(&col.data_type),
                    true,
                ));
            }
            let schema = Arc::new(Schema::new(fields));
            if let Ok(mem_table) = MemTable::try_new(schema, vec![vec![]]) {
                let _ = ctx.register_table(
                    datafusion::sql::TableReference::from(table.name.as_str()),
                    Arc::new(mem_table),
                );
            }
        }

        let df = match ctx.sql(q).await {
            Ok(df) => df,
            Err(_) => return None,
        };

        let output_schema = Arc::new(df.schema().as_arrow().clone());
        let mut batches = match df.collect().await {
            Ok(b) => b,
            Err(_) => return None,
        };
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(output_schema));
        }
        Some(datafusion_batches_to_query_response(&batches))
    }

    /// Try to serve a SELECT via an index arrangement point lookup.
    ///
    /// Returns `Some(responses)` when:
    ///   - the query has a single `WHERE <col> = <int>` predicate,
    ///   - a READY index covers `col` on `view_name`, and
    ///   - `shard_db` is available with the arrangement's `op_id`.
    ///
    /// Returns `None` to fall through to the normal full-scan path.
    async fn maybe_index_point_lookup(
        &self,
        q: &str,
        view_name: &str,
    ) -> PgWireResult<Option<Vec<Response<'static>>>> {
        let shard_db = match &self.shard_db {
            Some(db) => db.clone(),
            None => return Ok(None),
        };

        let (pred_col, pred_val) = match extract_where_equality(q) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Find a READY index on the view/table that covers the predicate column.
        let matching_idx = self
            .catalog
            .list_index_names()
            .into_iter()
            .find_map(|idx_name| {
                let entry = self.catalog.get_index(&idx_name)?;
                if entry.table != view_name {
                    return None;
                }
                if entry.state != crate::catalog_stubs::CatalogIndexState::Ready {
                    return None;
                }
                let op_id = entry.op_id?;
                if entry.index_cols.first()?.as_str() == pred_col {
                    Some(op_id)
                } else {
                    None
                }
            });

        let op_id = match matching_idx {
            Some(id) => id,
            None => return Ok(None),
        };

        // Build the binary prefix: [0x03][op_id 8 BE][pred_val 8 BE]
        let mut prefix = Vec::with_capacity(17);
        prefix.push(0x03u8); // ShardPrefix::ViewOutput
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix.extend_from_slice(&pred_val.to_be_bytes());

        let entries = shard_db
            .scan_prefix(&prefix)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e))))?;

        Ok(Some(self.index_entries_to_response(view_name, entries)?))
    }

    /// Slice 5 (v0.51.2): range-lookup accelerator sibling of
    /// `maybe_index_point_lookup`. Scans the bounded sub-set of a `Ready`
    /// index's `0x03‖op_id‖…` rows whose key-encoded predicate column value
    /// falls within `[lower, upper]` (inclusive), instead of scanning the
    /// full table — the "range-lookup accelerator" half of the roadmap's
    /// Scope wording (v0.32 shipped only equality/point lookups).
    async fn maybe_index_range_lookup(
        &self,
        q: &str,
        view_name: &str,
    ) -> PgWireResult<Option<Vec<Response<'static>>>> {
        let shard_db = match &self.shard_db {
            Some(db) => db.clone(),
            None => return Ok(None),
        };

        let (pred_col, lower, upper) = match extract_where_range(q) {
            Some(p) => p,
            None => return Ok(None),
        };

        let matching_idx = self
            .catalog
            .list_index_names()
            .into_iter()
            .find_map(|idx_name| {
                let entry = self.catalog.get_index(&idx_name)?;
                if entry.table != view_name {
                    return None;
                }
                if entry.state != crate::catalog_stubs::CatalogIndexState::Ready {
                    return None;
                }
                let op_id = entry.op_id?;
                if entry.index_cols.first()?.as_str() == pred_col {
                    Some(op_id)
                } else {
                    None
                }
            });

        let op_id = match matching_idx {
            Some(id) => id,
            None => return Ok(None),
        };

        // Bounded sub-scan: only this index's own `0x03‖op_id‖…` key space,
        // never the full table.
        let mut op_prefix = Vec::with_capacity(9);
        op_prefix.push(0x03u8); // ShardPrefix::ViewOutput
        op_prefix.extend_from_slice(&op_id.to_be_bytes());

        let entries = shard_db
            .scan_prefix(&op_prefix)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e))))?;

        let filtered: Vec<(bytes::Bytes, bytes::Bytes)> = entries
            .into_iter()
            .filter(|(key, _)| {
                if key.len() < 17 {
                    return false;
                }
                let val_bytes: [u8; 8] = match key[9..17].try_into() {
                    Ok(b) => b,
                    Err(_) => return false,
                };
                let val = i64::from_be_bytes(val_bytes);
                val >= lower && val <= upper
            })
            .collect();

        Ok(Some(self.index_entries_to_response(view_name, filtered)?))
    }

    /// Shared decode-and-encode logic for both index-accelerated point and
    /// range lookups: decodes each arrangement value (all columns encoded as
    /// fixed-width i64 BE) and builds the pgwire `QUERY` response.
    fn index_entries_to_response(
        &self,
        view_name: &str,
        entries: Vec<(bytes::Bytes, bytes::Bytes)>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        // Determine column names from the catalog (fall back to positional names).
        let col_names: Vec<String> = if let Some(cv) = self.catalog.get_view(view_name) {
            cv.columns.iter().map(|c| c.name.clone()).collect()
        } else if let Some(ct) = self.catalog.get_table(view_name) {
            ct.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            vec![]
        };

        // Decode each arrangement value: all columns encoded as i64 BE (8 bytes each).
        let raw_rows: Vec<Vec<u8>> = entries
            .into_iter()
            .filter_map(|(_, val)| {
                if val.len() % 8 != 0 {
                    return None;
                }
                let n_cols = val.len() / 8;
                let fields: Vec<String> = (0..n_cols)
                    .map(|i| {
                        let bytes: [u8; 8] = val[i * 8..(i + 1) * 8].try_into().ok()?;
                        Some(i64::from_be_bytes(bytes).to_string())
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(fields.join("\t").into_bytes())
            })
            .collect();

        let n_cols = if raw_rows.is_empty() {
            col_names.len().max(1)
        } else {
            raw_rows[0].iter().filter(|&&b| b == b'\t').count() + 1
        };

        let schema_fields: Vec<FieldInfo> = if let Some(cv) = self.catalog.get_view(view_name) {
            cv.columns
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let oid = crate::catalog_stubs::arrow_type_to_pg_oid(&c.data_type);
                    let format = PORTAL_FORMAT
                        .try_with(|f| f.format_for(idx))
                        .unwrap_or(FieldFormat::Text);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), format)
                })
                .collect()
        } else if let Some(ct) = self.catalog.get_table(view_name) {
            ct.columns
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let oid = crate::catalog_stubs::arrow_type_to_pg_oid(&c.data_type);
                    let format = PORTAL_FORMAT
                        .try_with(|f| f.format_for(idx))
                        .unwrap_or(FieldFormat::Text);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), format)
                })
                .collect()
        } else {
            (0..n_cols)
                .map(|i| {
                    let name = col_names
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("col_{i}"));
                    let format = PORTAL_FORMAT
                        .try_with(|f| f.format_for(i))
                        .unwrap_or(FieldFormat::Text);
                    FieldInfo::new(name, None, None, Type::INT8, format)
                })
                .collect()
        };

        let schema = Arc::new(schema_fields);
        let schema_ref = schema.clone();
        let data_stream = stream::iter(raw_rows).map(move |raw: Vec<u8>| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let row_str = String::from_utf8_lossy(&raw).into_owned();
            let col_count = schema_ref.len();
            let fields: Vec<&str> = row_str.split('\t').collect();
            for i in 0..col_count {
                let val = fields.get(i).copied().filter(|value| *value != r"\N");
                let datatype = schema_ref[i].datatype();
                encode_typed_field(&mut encoder, datatype, val)?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_stream,
        ))])
    }

    async fn query_time_datafusion_response(
        &self,
        raw_sql: &str,
        referenced_tables: &[String],
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        for relation_name in referenced_tables {
            if let Some(error_responses) = self.select_access_error_response(relation_name, conn_id)
            {
                return Ok(error_responses);
            }
        }

        let topology = match self.query_time_shard_topology().await {
            Ok(topology) => topology,
            Err(error) => return Ok(query_time_error_response(error)),
        };
        let batches = match query_time_datafusion_select(
            &self.catalog,
            &topology,
            raw_sql,
            referenced_tables,
        )
        .await
        {
            Ok(batches) => batches,
            Err(error) => return Ok(query_time_error_response(error)),
        };
        Ok(datafusion_batches_to_query_response(&batches))
    }

    async fn query_time_shard_topology(&self) -> Result<QueryTimeShardTopology, GatewayError> {
        if let Some(provider) = &self.query_time_shard_topology_provider {
            return provider.refresh().await;
        }
        if let Some(topology) = &self.query_time_shard_topology {
            if topology.readers.is_empty()
                || self.shard_db.as_ref().is_some_and(|shard_db| {
                    !topology
                        .readers
                        .iter()
                        .any(|reader| reader.path() == shard_db.path())
                })
            {
                return Err(GatewayError::QueryTimeScatterTopologyUnavailable);
            }
            return Ok((**topology).clone());
        }

        // A shard-backed single-node gateway has exactly one known owner. Turn
        // it into the same reader topology as the distributed path; a gateway
        // without either dependency fails closed instead of reading a local
        // shard as an implicit partial answer.
        let shard_db = self
            .shard_db
            .as_ref()
            .ok_or(GatewayError::QueryTimeScatterTopologyUnavailable)?;
        let reader = rockstream_storage::ShardReader::open(
            shard_db.path().to_string(),
            shard_db.object_store(),
        )
        .await?;
        Ok(QueryTimeShardTopology::new(
            vec![Arc::new(reader)],
            shard_db.last_epoch().load(Ordering::SeqCst),
        ))
    }

    fn select_access_error_response(
        &self,
        relation_name: &str,
        conn_id: Option<&str>,
    ) -> Option<Vec<Response<'static>>> {
        let (principal, session_namespace) = if let Some(id) = conn_id {
            let session = self.sessions.entry(id.to_string()).or_default();
            (session.principal.clone(), session.current_namespace.clone())
        } else {
            (Principal::System, "public".to_string())
        };

        use rockstream_types::acl::Role;
        if !principal.is_system() {
            if let Err(e) = self.acl_store.check(
                principal.identity(),
                &session_namespace,
                Some(relation_name),
                Role::Viewer,
            ) {
                return Some(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42501".to_owned(), e.to_string()),
                )))]);
            }
        }

        if let Some(cv) = self.catalog.get_view(relation_name) {
            let view_ns = &cv.namespace;
            if view_ns != &session_namespace && !principal.is_system() {
                let is_admin = self
                    .acl_store
                    .check(principal.identity(), &session_namespace, None, Role::Admin)
                    .is_ok();
                if !is_admin {
                    return Some(vec![promote_response(Response::Error(Box::new(
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

        None
    }

    /// Read rows from a view and build a pgwire `Response::Query`.
    /// Enforces ACL (RS-2401) and namespace isolation (RS-2402) when conn_id is provided.
    async fn read_view_response(
        &self,
        view_name: &str,
        limit: Option<usize>,
        order_by: Vec<(String, bool)>,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(error_responses) = self.select_access_error_response(view_name, conn_id) {
            return Ok(error_responses);
        }
        if self.catalog.get_view(view_name).is_some()
            && !self.catalog.is_backfill_published(view_name)
        {
            return Ok(backfill_not_published_response(view_name));
        }

        let schema_fields: Vec<FieldInfo> = if let Some(cv) = self.catalog.get_view(view_name) {
            cv.columns
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    let format = PORTAL_FORMAT
                        .try_with(|f| f.format_for(idx))
                        .unwrap_or(FieldFormat::Text);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), format)
                })
                .collect()
        } else if let Some(ct) = self.catalog.get_table(view_name) {
            ct.columns
                .iter()
                .enumerate()
                .map(|(idx, c)| {
                    let oid = arrow_type_to_pg_oid(&c.data_type);
                    let format = PORTAL_FORMAT
                        .try_with(|f| f.format_for(idx))
                        .unwrap_or(FieldFormat::Text);
                    FieldInfo::new(c.name.clone(), None, None, pg_type_from_oid(oid), format)
                })
                .collect()
        } else {
            let format = PORTAL_FORMAT
                .try_with(|f| f.format_for(0))
                .unwrap_or(FieldFormat::Text);
            vec![FieldInfo::new(
                "result".to_string(),
                None,
                None,
                Type::TEXT,
                format,
            )]
        };

        // Prefer reading directly from ShardDb when available.  ShardDb reads
        // from its in-memory memtable (WAL + SSTs), which reflects the latest
        // committed writes immediately after the post-COMMIT flush.  The
        // ShardReader (DbReader) polls for a new manifest every 1 s and would
        // return stale results until the next poll fires.
        let raw_rows: Vec<Vec<u8>> = if let Some(shard_db) = &self.shard_db {
            let mut rows: Vec<Vec<u8>> = if let Some(view) = self.catalog.get_view(view_name) {
                if view.op_id.is_some() {
                    self.read_compiled_view_rows(view_name, &view, shard_db)
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                } else {
                    let prefix = format!("view_output/{view_name}/");
                    let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.map_err(|e| {
                        PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                    })?;
                    kvs.into_iter().map(|(_, v)| v.to_vec()).collect()
                }
            } else {
                let prefix = format!("view_output/{view_name}/");
                let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.map_err(|e| {
                    PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                })?;
                kvs.into_iter().map(|(_, v)| v.to_vec()).collect()
            };
            // Apply ORDER BY if specified
            if !order_by.is_empty() {
                // Build column-name → index map from schema_fields
                let col_idx: std::collections::HashMap<String, usize> = schema_fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name().to_lowercase(), i))
                    .collect();
                rows.sort_by(|a, b| {
                    let a_fields: Vec<&str> =
                        std::str::from_utf8(a).unwrap_or("").split('\t').collect();
                    let b_fields: Vec<&str> =
                        std::str::from_utf8(b).unwrap_or("").split('\t').collect();
                    for (col, desc) in &order_by {
                        let idx = match col_idx.get(col.as_str()) {
                            Some(&i) => i,
                            None => continue,
                        };
                        let av = a_fields.get(idx).copied().unwrap_or("");
                        let bv = b_fields.get(idx).copied().unwrap_or("");
                        let ord = if let (Ok(an), Ok(bn)) = (av.parse::<i64>(), bv.parse::<i64>()) {
                            an.cmp(&bn)
                        } else if let (Ok(af), Ok(bf)) = (av.parse::<f64>(), bv.parse::<f64>()) {
                            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
                        } else {
                            av.cmp(bv)
                        };
                        let ord = if *desc { ord.reverse() } else { ord };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
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
                let val = fields.get(i).copied().filter(|value| *value != r"\N");
                let datatype = schema_ref[i].datatype();
                encode_typed_field(&mut encoder, datatype, val)?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_stream,
        ))])
    }

    async fn handle_create_view<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
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
            let workload_name = parse_create_view_workload(q);
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

            if let Some(workload_name) = workload_name.as_deref() {
                if self.catalog.get_workload(workload_name).is_none() {
                    return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42704".to_owned(),
                        format!(
                            "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before assigning the view.",
                            workload_name, workload_name
                        ),
                    )))]);
                }
            }

            // Pre-populate column names by static analysis of the SELECT list.
            // This allows `SELECT * FROM view` to return correct column headers
            // even before the first DML commit triggers a full materialization.
            // Types default to Utf8 and are refined to the true Arrow types once
            // the view is first materialized via `update_view_columns`.
            let mut initial_columns: Vec<crate::catalog_stubs::CatalogColumn> =
                infer_select_columns(&select_sql)
                    .into_iter()
                    .map(|name| crate::catalog_stubs::CatalogColumn {
                        name,
                        data_type: "Utf8".to_string(),
                    })
                    .collect();
            // `infer_select_columns` can't statically name a `SELECT *`
            // (v0.51.4 Slice 8 finding: this used to self-heal once
            // `view_materializer.rs`'s DataFusion pass ran and called
            // `update_view_columns` with the real output schema — that
            // fallback no longer exists, so a bare `SELECT * FROM t`
            // view would otherwise register with zero columns forever,
            // and `SELECT * FROM view` would return zero-column rows).
            // For the exact-passthrough shape (`SELECT * FROM
            // <single-base-table>`, e.g. Nexmark q0), the source table's
            // own already-known column list *is* the view's column list.
            if initial_columns.is_empty() {
                let select_list = select_sql
                    .trim()
                    .to_lowercase()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                if select_list.starts_with("select * from ") && deps.len() == 1 {
                    if let Some(table) = self.catalog.get_table(&deps[0]) {
                        initial_columns = table.columns.clone();
                    }
                }
            }

            // v0.51.4 Slice 8: attempt to compile the view's SELECT into an
            // executable operator chain *before* registering it in the
            // catalog. There is no DataFusion-materializer fallback left
            // (`view_materializer.rs` is deleted) — a shard-backed gateway
            // (`--role all`) either compiles a view for real or the
            // `CREATE VIEW`/`CREATE MATERIALIZED VIEW` itself fails with a
            // real, user-visible `RS-1019` error, exactly like any other
            // unsupported-SQL rejection. A standalone `--role gateway` (no
            // local `ShardDb`) has nothing to compile against locally and
            // registers the view unconditionally — its data is served from
            // elsewhere via `ViewReader` (multi-shard scatter-read), not
            // this process's own compiled pipeline.
            let compiled_op_id: Option<u64> = if let Some(shard_db) = self.shard_db.clone() {
                // v0.51.4 Slice 8: a view-of-view (any dep that's itself a
                // view, not a base table) is inlined as a subquery before
                // compilation — the *catalog*'s own dependency graph
                // (`deps`, used for cycle detection and the reachability
                // BFS at commit time) still tracks the original, uninlined
                // names, so a commit to the innermost base table correctly
                // reaches every view in the chain.
                let inlined_sql =
                    inline_view_dependencies(&select_sql, &self.catalog, MAX_VIEW_INLINE_DEPTH);
                let compile_deps = extract_sql_refs(&inlined_sql);
                let deps_are_base_tables = compile_deps
                    .iter()
                    .all(|dep| self.catalog.get_table(dep).is_some());
                let compile_result = if deps_are_base_tables {
                    let table_schemas: HashMap<String, SchemaRef> = compile_deps
                        .iter()
                        .map(|dep| (dep.clone(), query_time_relation_schema(&self.catalog, dep)))
                        .collect();
                    self.try_compile_view(
                        &view_name,
                        &inlined_sql,
                        initial_columns.len(),
                        &table_schemas,
                        shard_db.clone(),
                    )
                    .await
                } else {
                    Err(format!(
                        "[RS-1019] view.compile_failed: view depends on non-base-table \
                         relation(s) {compile_deps:?} that could not be inlined to base \
                         tables; compile_plan only supports views over base tables \
                         (directly, or transitively through other views)"
                    ))
                };
                match compile_result {
                    Ok(compiled) => {
                        let op_id = compiled.sink_op_id.0;
                        let pk = compiled.pk.clone();
                        self.compiled_views
                            .insert(view_name.clone(), Arc::new(compiled));
                        tracing::debug!(
                            view = %view_name,
                            op_id,
                            "handle_create_view: compiled view through direct operator pipeline"
                        );
                        // v0.51.4 Slice 8: record the view_name -> op_id
                        // directory entry so a standalone `--role gateway`
                        // process (no shared in-memory catalog) can resolve
                        // and read this view's compiled output through
                        // `ViewReader`/`ShardReader` alone (the multi-shard
                        // publish/read path — see `rockstream_ops::sink`'s
                        // view-directory doc comment).
                        if let Err(e) = rockstream_ops::sink::write_view_directory_entry(
                            &shard_db,
                            &view_name,
                            OperatorId(op_id),
                            initial_columns.len(),
                            &pk,
                        )
                        .await
                        {
                            tracing::warn!(
                                view = %view_name,
                                error = %e,
                                "handle_create_view: failed to write view directory entry \
                                 (multi-shard/published reads of this view may return no rows)"
                            );
                        }
                        Some(op_id)
                    }
                    Err(e) => {
                        return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42601".to_owned(),
                            format!(
                                "[RS-1019] view.compile_failed: {tag} '{view_name}' could not be \
                                 compiled into an executable operator pipeline: {e}. Next steps: \
                                 simplify the query to a supported shape (see \
                                 docs/language-features.md), or reference only base tables."
                            ),
                        )))]);
                    }
                }
            } else {
                None
            };

            // Register view in the catalog — only reached once compilation
            // (when attempted) has already succeeded.
            use crate::catalog_stubs::CatalogView;
            self.catalog.add_view_with_deps(
                CatalogView {
                    name: view_name.clone(),
                    sql: select_sql,
                    columns: initial_columns,
                    namespace: "public".to_string(),
                    op_id: compiled_op_id,
                },
                deps.clone(),
            );
            if is_materialized {
                self.catalog.begin_backfill(&view_name, 0);
            }
            if let Some(workload_name) = workload_name {
                self.catalog
                    .assign_view_workload(&view_name, &workload_name);
            }
            if let Some(log) = &self.audit_log {
                let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                    "system",
                    "create_view",
                    &view_name,
                ));
            }

            // Immediate materialization: a view (materialized *or* plain)
            // must reflect its source tables' already-committed data before
            // this response reaches the client, not just the next COMMIT —
            // standard SQL view semantics ("a view is the query over the
            // *current* state of its base tables"), not something specific
            // to `MATERIALIZED`. This was previously gated on
            // `is_materialized` only, which meant a plain `CREATE VIEW`
            // (e.g. a join over a table that already had committed rows
            // before the view existed) silently missed that pre-existing
            // data forever — never exercised by any prior test, since every
            // prior compiled-view test created the view before any base
            // table had data. `populate_compiled_view_from_scratch` is a
            // no-op when every source table is still empty, so this is safe
            // for the common (empty-at-creation) case too. v0.51.4 Slice 8:
            // the compiled view's own initial-backfill path is the *only*
            // population step now — no redundant double-write through a
            // separately-materialized legacy pass.
            let bound_sources = if is_materialized {
                deps.iter()
                    .filter_map(|relation| {
                        self.catalog.get_source(relation).filter(|source| {
                            source.table_name.as_deref() == Some(relation.as_str())
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let mut backfill_ready = !is_materialized;
            if let Some(shard_db) = &self.shard_db {
                if !bound_sources.is_empty() {
                    let result = self
                        .backfill_source_view(&bound_sources, &view_name, shard_db)
                        .await;
                    if let Err(error) = result {
                        return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "55000".to_owned(),
                            format!(
                                "[RS-4022] backfill.not_published: materialized view '{view_name}' failed during source backfill: {error}"
                            ),
                        )))]);
                    }
                    backfill_ready = true;
                } else if self.compiled_views.contains_key(&view_name) {
                    if let Err(error) = self
                        .populate_compiled_view_from_scratch(&view_name, shard_db)
                        .await
                    {
                        return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "55000".to_owned(),
                            format!(
                                "[RS-4022] backfill.not_published: materialized view '{view_name}' failed during initial backfill: {error}"
                            ),
                        )))]);
                    } else {
                        backfill_ready = true;
                    }
                }
                if is_materialized && bound_sources.is_empty() {
                    self.catalog.catch_up_backfill(&view_name, None);
                    if let Err(e) = shard_db.flush().await {
                        tracing::warn!(
                            "post-CREATE-MATERIALIZED-VIEW shard flush failed (non-fatal): {e}"
                        );
                    } else if backfill_ready {
                        self.catalog.publish_backfill(&view_name);
                    }
                }
            } else if is_materialized && bound_sources.is_empty() {
                self.catalog.publish_backfill(&view_name);
            } else if is_materialized {
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "55000".to_owned(),
                    format!(
                        "[RS-4022] backfill.not_published: materialized view '{view_name}' requires a shard-backed source runtime"
                    ),
                )))]);
            }
        }

        Ok(vec![Response::Execution(Tag::new(tag).with_rows(0))])
    }

    async fn handle_create_workload(&self, q: &str) -> PgWireResult<Vec<Response<'static>>> {
        let Some(parsed) = parse_create_workload(q) else {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "[RS-1006] workload.invalid_definition: CREATE WORKLOAD requires a name and optional WITH (...) settings. Next steps: use CREATE WORKLOAD fast WITH (MEMORY_LIMIT = 1048576, FRESHNESS_SLO_MS = 500).".to_owned(),
            )))]);
        };
        let inserted = self
            .catalog
            .add_workload_async(parsed.clone())
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        if !inserted {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42710".to_owned(),
                format!(
                    "[RS-1006] workload.already_exists: workload '{}' already exists. Next steps: choose a different workload name or drop the existing workload first.",
                    parsed.name
                ),
            )))]);
        }
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "create_workload",
                &parsed.name,
            ));
        }
        Ok(vec![Response::Execution(
            Tag::new("CREATE WORKLOAD").with_rows(0),
        )])
    }

    async fn handle_alter_workload(&self, q: &str) -> PgWireResult<Vec<Response<'static>>> {
        let Some((workload_name, changes)) = parse_alter_workload(q) else {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "[RS-1006] workload.invalid_definition: ALTER WORKLOAD requires SET (...) assignments. Next steps: use ALTER WORKLOAD fast SET (MEMORY_LIMIT = 1048576).".to_owned(),
            )))]);
        };
        let Some(mut workload) = self.catalog.get_workload(&workload_name) else {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!(
                    "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before altering it.",
                    workload_name, workload_name
                ),
            )))]);
        };
        apply_workload_settings(&mut workload, &changes);
        let updated = self
            .catalog
            .update_workload_async(workload.clone())
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        if !updated {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!(
                    "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before altering it.",
                    workload_name, workload_name
                ),
            )))]);
        }
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "workload.altered",
                &workload_name,
            ));
        }
        Ok(vec![Response::Execution(
            Tag::new("ALTER WORKLOAD").with_rows(0),
        )])
    }

    async fn handle_drop_workload(&self, q: &str) -> PgWireResult<Vec<Response<'static>>> {
        let Some(workload_name) = parse_drop_workload(q) else {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "[RS-1006] workload.invalid_definition: DROP WORKLOAD requires a workload name. Next steps: use DROP WORKLOAD fast.".to_owned(),
            )))]);
        };
        if self.catalog.get_workload(&workload_name).is_none() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!(
                    "[RS-1005] workload.not_found: workload '{}' does not exist. Next steps: run CREATE WORKLOAD {} WITH (...) before dropping it.",
                    workload_name, workload_name
                ),
            )))]);
        }
        let assigned_views = self.catalog.views_for_workload(&workload_name);
        if !assigned_views.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "2BP01".to_owned(),
                format!(
                    "[RS-1014] workload.has_assigned_views: workload '{}' is still assigned to views {}. Next steps: reassign or drop those views before dropping the workload.",
                    workload_name,
                    assigned_views.join(", ")
                ),
            )))]);
        }
        self.catalog
            .remove_workload_async(&workload_name)
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "workload.dropped",
                &workload_name,
            ));
        }
        Ok(vec![Response::Execution(
            Tag::new("DROP WORKLOAD").with_rows(0),
        )])
    }

    fn handle_refresh_materialized_view(&self, q: &str) -> Vec<Response<'static>> {
        let ql = q.trim().to_lowercase();
        let after = ql
            .strip_prefix("refresh materialized view ")
            .unwrap_or("")
            .trim();
        let view_name = after
            .trim_end_matches(';')
            .trim_matches('"')
            .rsplit('.')
            .next()
            .unwrap_or(after)
            .trim_matches('"')
            .to_string();

        if view_name.is_empty() || self.catalog.get_view(&view_name).is_none() {
            return vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!(
                    "[RS-2001] refresh_materialized_view.not_found: materialized view '{}' does not exist. Next steps: verify the view name with \\dv in psql.",
                    view_name
                ),
            ))))];
        }

        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "refresh_materialized_view",
                &view_name,
            ));
        }

        vec![promote_response(Response::Execution(
            Tag::new("REFRESH MATERIALIZED VIEW").with_rows(0),
        ))]
    }

    fn handle_create_table<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let ql = q.to_lowercase();
        let if_not_exists = ql.contains("if not exists");

        let after = if if_not_exists {
            let pos = match ql.find("if not exists") {
                Some(p) => p + "if not exists".len(),
                None => {
                    return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        "[RS-2000] malformed CREATE TABLE DDL".to_owned(),
                    )))]);
                }
            };
            q.get(pos..).unwrap_or("").trim()
        } else {
            let pos = match ql.find("create table") {
                Some(p) => p + "create table".len(),
                None => {
                    return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        "[RS-2000] malformed CREATE TABLE DDL".to_owned(),
                    )))]);
                }
            };
            q.get(pos..).unwrap_or("").trim()
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
        let parsed = parse_create_table_columns(after);

        self.catalog.add_table(CatalogTable {
            name: table_name.clone(),
            columns: parsed.columns,
        });
        if !parsed.generated_columns.is_empty() {
            let identity_sequences = parsed
                .generated_columns
                .iter()
                .filter_map(|(name, kind)| {
                    if *kind == GeneratedColumnKind::Identity {
                        Some((name.clone(), Arc::new(AtomicU64::new(0))))
                    } else {
                        None
                    }
                })
                .collect();
            self.table_insert_metadata.insert(
                table_name.clone(),
                Arc::new(TableInsertMetadata {
                    generated_columns: parsed.generated_columns,
                    identity_sequences,
                }),
            );
        }

        Ok(vec![Response::Execution(
            Tag::new("CREATE TABLE").with_rows(0),
        )])
    }

    /// Handle `CREATE SINK <name> FOR VIEW <view> TO ICEBERG|DELTA '<path>' WITH (...)` — v0.44.
    fn handle_create_sink<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let parsed = match parse_create_sink_ddl(q) {
            Ok(parsed) => parsed,
            Err(message) => return Ok(vec![create_sink_error_response(message)]),
        };

        if self.catalog.get_view(&parsed.view).is_none() {
            return Ok(vec![create_sink_error_response(format!(
                "[RS-4007] CREATE SINK references unknown view '{}'. Next steps: {CREATE_SINK_NEXT_STEPS}",
                parsed.view
            ))]);
        }

        if !matches!(
            parsed.catalog.as_str(),
            "filesystem" | "glue" | "rest" | "hive" | "ducklake"
        ) {
            return Ok(vec![create_sink_error_response(format!(
                "[RS-4007] CREATE SINK catalog '{}' is invalid; expected filesystem|glue|rest|hive|ducklake. Next steps: {CREATE_SINK_NEXT_STEPS}",
                parsed.catalog
            ))]);
        }

        let entry = CatalogSinkEntry {
            name: parsed.name.clone(),
            view: parsed.view.clone(),
            format: parsed.format,
            path: parsed.path,
            snapshot_interval_epochs: parsed.snapshot_interval_epochs,
            snapshot_interval_ms: parsed.snapshot_interval_ms,
            parquet_row_group_bytes: parsed.parquet_row_group_bytes,
            format_version: parsed.format_version,
            partition_by: parsed.partition_by,
            catalog: parsed.catalog,
            last_snapshot_epoch: None,
            state: "OK".to_string(),
        };

        let _ = self.catalog.add_sink(entry);

        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "create_sink",
                &parsed.name,
            ));
        }

        Ok(vec![Response::Execution(
            Tag::new("CREATE SINK").with_rows(0),
        )])
    }

    fn handle_create_source<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let parsed = match parse_create_source_ddl(q) {
            Ok(parsed) => parsed,
            Err(message) => return Ok(vec![create_source_error_response(message)]),
        };

        if self.catalog.get_source(&parsed.name).is_some() {
            return Ok(vec![create_source_error_response(format!(
                "[RS-4010] source.already_exists: source '{}' already exists. Next steps: {CREATE_SOURCE_NEXT_STEPS}",
                parsed.name
            ))]);
        }

        let entry = CatalogSourceEntry {
            name: parsed.name.clone(),
            table_name: self
                .catalog
                .get_table(&parsed.name)
                .map(|_| parsed.name.clone()),
            source_type: parsed.source_type.clone(),
            options: parsed.options.clone(),
            format: parsed.format.clone(),
            status: "OK".to_string(),
            live_offset: "0".to_string(),
            live_lag: 0,
        };

        if entry.source_type == "postgres_cdc" && entry.format == "pgoutput" {
            let identity = match pgoutput_source_identity(&entry) {
                Ok(identity) => identity,
                Err(error) => return Ok(vec![create_source_error_response(error.to_string())]),
            };
            if let Some((owner, _)) = self
                .catalog
                .list_sources()
                .into_iter()
                .filter(|source| {
                    source.source_type == "postgres_cdc" && source.format == "pgoutput"
                })
                .filter_map(|source| {
                    pgoutput_source_identity(&source)
                        .ok()
                        .map(|existing| (source.name, existing))
                })
                .find(|(_, existing)| {
                    identity.has_same_physical_slot(existing) && identity != *existing
                })
            {
                return Ok(vec![create_source_error_response(format!(
                    "[RS-4013] physical pgoutput slot is already owned by source '{owner}'"
                ))]);
            }
        }

        if !self.catalog.add_source(entry) {
            return Ok(vec![create_source_error_response(format!(
                "[RS-4010] source.already_exists: source '{}' already exists. Next steps: {CREATE_SOURCE_NEXT_STEPS}",
                parsed.name
            ))]);
        }
        if parsed.source_type == "http_webhook" {
            // A credential reference is catalog-safe metadata.  The listener
            // keeps its verifier only in runtime memory and never returns it
            // through SHOW SOURCE STATUS.
            let Some(format) = WebhookFormat::parse(&parsed.format) else {
                return Ok(vec![create_source_error_response(
                    "[RS-4008] invalid webhook format".to_string(),
                )]);
            };
            let Some(token) = parsed.options.get("credential_ref") else {
                return Ok(vec![create_source_error_response(
                    "[RS-4008] missing credential_ref".to_string(),
                )]);
            };
            self.webhook_sources.insert(
                parsed.name.clone(),
                Arc::new(Mutex::new(HttpWebhookSource::new(token, format))),
            );
        }

        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system",
                "create_source",
                &parsed.name,
            ));
        }

        Ok(vec![Response::Execution(
            Tag::new("CREATE SOURCE").with_rows(0),
        )])
    }

    fn handle_alter_source<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let parsed = match parse_alter_source_ddl(q) {
            Ok(parsed) => parsed,
            Err(message) => return Ok(vec![create_source_error_response(message)]),
        };

        if self.catalog.get_source(&parsed.name).is_none() {
            return Ok(vec![create_source_error_response(format!(
                "[RS-4009] source.not_found: source '{}' does not exist. Next steps: {ALTER_SOURCE_NEXT_STEPS}",
                parsed.name
            ))]);
        }

        match parsed.action {
            AlterSourceAction::Pause => {
                self.catalog.update_source_status(&parsed.name, "PAUSED");
                if let Some(source) = self.webhook_sources.get(&parsed.name) {
                    source.lock().set_paused(true);
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.pause",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(0),
                )])
            }
            AlterSourceAction::Resume => {
                self.catalog.update_source_status(&parsed.name, "OK");
                if let (Some(source), Some(shard_db)) = (
                    self.catalog.get_source(&parsed.name),
                    self.shard_db.as_ref(),
                ) {
                    if source.source_type == "postgres_cdc" && source.format == "pgoutput" {
                        if let Some(view) = self.catalog.list_views().into_iter().find(|view| {
                            self.catalog
                                .get_view_deps(&view.name)
                                .contains(&source.name)
                        }) {
                            self.spawn_postgres_cdc_source_worker(
                                source,
                                view.name,
                                Arc::clone(shard_db),
                            );
                        }
                    }
                }
                if let Some(source) = self.webhook_sources.get(&parsed.name) {
                    source.lock().set_paused(false);
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.resume",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(0),
                )])
            }
            AlterSourceAction::Drop => {
                let source = self.catalog.get_source(&parsed.name);
                if source.as_ref().is_some_and(|source| {
                    source.source_type == "postgres_cdc"
                        && self.catalog.list_views().into_iter().any(|view| {
                            self.catalog
                                .get_view_deps(&view.name)
                                .contains(&parsed.name)
                        })
                }) {
                    return Ok(vec![create_source_error_response(format!(
                        "[RS-4013] pgoutput source '{}' still has dependent views; drop them before DROP SOURCE",
                        parsed.name
                    ))]);
                }
                let pgoutput_id = source.as_ref().and_then(|source| {
                    (source.source_type == "postgres_cdc" && source.format == "pgoutput")
                        .then(|| pgoutput_source_identity(source).ok())
                        .flatten()
                        .map(|identity| identity.connector_id())
                });
                self.catalog.remove_source(&parsed.name);
                self.webhook_sources.remove(&parsed.name);
                if let (Some(connector_id), Some(shard_db), Ok(runtime)) = (
                    pgoutput_id.filter(|connector_id| {
                        self.pgoutput_registered_aliases(*connector_id).is_empty()
                    }),
                    self.shard_db.as_ref(),
                    tokio::runtime::Handle::try_current(),
                ) {
                    if let Some(coordinator) = self
                        .pgoutput_coordinators
                        .get(&connector_id)
                        .map(|entry| entry.value().clone())
                    {
                        let shard_db = Arc::clone(shard_db);
                        let registry = Arc::clone(&self.pgoutput_coordinators);
                        runtime.spawn(async move {
                            let mut coordinator = coordinator.lock().await;
                            if coordinator.drop_durable_state(&shard_db).await.is_ok() {
                                drop(coordinator);
                                registry.remove(&connector_id);
                            }
                        });
                    }
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.drop",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("DROP SOURCE").with_rows(0),
                )])
            }
            AlterSourceAction::AdvanceWatermark(watermark) => {
                let Some(source) = self.webhook_sources.get(&parsed.name) else {
                    return Ok(vec![create_source_error_response(format!(
                        "[RS-4016] ALTER SOURCE ADVANCE WATERMARK is supported only for http_webhook sources. Next steps: {ALTER_SOURCE_NEXT_STEPS}",
                    ))]);
                };
                let res = source.lock().advance_watermark(watermark);
                if let Err(message) = res {
                    return Ok(vec![create_source_error_response(message.to_string())]);
                }

                self.catalog
                    .update_source_runtime(&parsed.name, watermark.to_string(), 0);
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.advance_watermark",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(0),
                )])
            }
            AlterSourceAction::ReplayDlq { since, until } => {
                let mut count = 0u64;
                {
                    let mut dlq = rockstream_types::dlq::get_global_dlq().lock();
                    for entry in dlq.iter_mut() {
                        if entry.source_name.eq_ignore_ascii_case(&parsed.name) {
                            if let Some(s) = since {
                                if entry.arrived_at < s {
                                    continue;
                                }
                            }
                            if let Some(u) = until {
                                if entry.arrived_at > u {
                                    continue;
                                }
                            }
                            entry.replay_attempt += 1;
                            count += 1;
                        }
                    }
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.replay_dlq",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(count as usize),
                )])
            }
            AlterSourceAction::DismissDlq { condition } => {
                let count;
                {
                    let mut dlq = rockstream_types::dlq::get_global_dlq().lock();
                    let len_before = dlq.len();
                    dlq.retain(|entry| {
                        if !entry.source_name.eq_ignore_ascii_case(&parsed.name) {
                            return true;
                        }
                        if let Some(cond) = &condition {
                            let cond_lower = cond.to_lowercase();
                            if cond_lower.contains("error_code") {
                                if let Some(target) = cond_lower.split('=').nth(1) {
                                    let clean = target.trim().trim_matches('\'');
                                    if entry.error_code.eq_ignore_ascii_case(clean) {
                                        return false; // dismiss
                                    }
                                }
                            }
                            true // keep
                        } else {
                            false // dismiss all for source if no condition
                        }
                    });
                    count = len_before - dlq.len();
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.dismiss_dlq",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(count),
                )])
            }
            AlterSourceAction::SetOptions(_options) => {
                if self.catalog.get_source(&parsed.name).is_some_and(|source| {
                    source.source_type == "postgres_cdc" && source.status == "OK"
                }) {
                    return Ok(vec![create_source_error_response(
                        "[RS-4013] pgoutput identity options are immutable while running; pause, drain, and explicitly rebind the source"
                            .to_string(),
                    )]);
                }
                if let Some(log) = &self.audit_log {
                    let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                        "system",
                        "alter_source.set",
                        &parsed.name,
                    ));
                }
                Ok(vec![Response::Execution(
                    Tag::new("ALTER SOURCE").with_rows(0),
                )])
            }
        }
    }

    /// Handle `CREATE INDEX <name> ON <table> (<col>, ...) [WHERE <pred>]` — v0.32.
    ///
    /// Registers the index in `Building` state in the gateway catalog stubs,
    /// then (v0.51.2 Slice 5) synchronously backfills it from the table/
    /// view's already-materialized rows and transitions it to `Ready` — a
    /// standard `CREATE INDEX` blocks the issuing session until done (no
    /// `CONCURRENTLY` support in this version's Scope). Returns RS-2016 if an
    /// index with the same name exists for a different table, or RS-2027 if
    /// the table exceeds the backfill row-count bound (the index catalog
    /// entry is removed rather than left stuck in `Building`).
    async fn handle_create_index(&self, q: &str) -> PgWireResult<Vec<Response<'static>>> {
        use crate::catalog_stubs::{CatalogIndexEntry, CatalogIndexState};

        // Parse: CREATE INDEX <name> ON <table> (<cols>)
        let after_keyword = q["CREATE INDEX".len()..].trim();
        let upper_after = after_keyword.to_uppercase();

        let on_pos = match upper_after.find(" ON ") {
            Some(p) => p,
            None => {
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "CREATE INDEX requires ON clause".to_owned(),
                )))]);
            }
        };

        let index_name = after_keyword[..on_pos].trim().to_lowercase();
        let after_on = after_keyword[on_pos + 4..].trim();

        let paren_open = match after_on.find('(') {
            Some(p) => p,
            None => {
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "CREATE INDEX requires column list in parentheses".to_owned(),
                )))]);
            }
        };
        let table = after_on[..paren_open].trim().to_lowercase();
        let paren_close = after_on.rfind(')').unwrap_or(after_on.len());
        let cols_str = &after_on[paren_open + 1..paren_close];
        let index_cols: Vec<String> = cols_str
            .split(',')
            .map(|c| c.trim().to_lowercase())
            .filter(|c| !c.is_empty())
            .collect();

        if index_cols.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "CREATE INDEX requires at least one column".to_owned(),
            )))]);
        }

        let entry = CatalogIndexEntry {
            name: index_name.clone(),
            table: table.clone(),
            index_cols: index_cols.clone(),
            state: CatalogIndexState::Building,
            op_id: None,
        };

        if !self.catalog.add_index(entry) {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42710".to_owned(),
                format!(
                    "[RS-2016] Index name conflict: index '{index_name}' already exists for a \
                     different table. Choose a unique index name or drop the existing index first."
                ),
            )))]);
        }

        if let Err(e) = self.backfill_index(&index_name, &table, &index_cols).await {
            // Backpressure/error path: never leave the index stuck in
            // Building — remove the catalog entry so a retry (e.g. after
            // shrinking the table) starts clean.
            self.catalog.remove_index(&index_name);
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                crate::error::sqlstate_for(&e).to_owned(),
                e.to_string(),
            )))]);
        }

        if let Some(log) = &self.audit_log {
            let _ = log.append(&rockstream_types::audit::AuditEvent::now(
                "system".to_string(),
                "create_index",
                &index_name,
            ));
        }

        Ok(vec![Response::Execution(
            Tag::new("CREATE INDEX").with_rows(0),
        )])
    }

    /// Backfills a `CREATE INDEX` automatic index (v0.51.2 Slice 5): scans
    /// the target table/view's already-materialized `view_output/{table}/`
    /// rows in bounded batches, decodes the indexed column plus every other
    /// column as fixed-width i64 (the encoding `maybe_index_point_lookup`
    /// already reads), writes `0x03‖op_id(BE8)‖col_val(BE8)` → row bytes into
    /// `shard_db`, mints a fresh gateway-local `op_id`, and calls
    /// `catalog.mark_index_ready`. A table whose backfill scan exceeds
    /// `MAX_INDEX_BACKFILL_ROWS` fails with RS-2027 rather than buffering
    /// unboundedly or leaving the index half-built.
    ///
    /// If there is no shard-backed data plane, or the target table isn't yet
    /// registered in the catalog (so there is nothing to scan), the index is
    /// left in `Building` — matching the pre-existing (v0.32) behavior for
    /// catalog-only harnesses where `MARK INDEX ... READY` remains the only
    /// path to `Ready`.
    async fn backfill_index(
        &self,
        index_name: &str,
        table: &str,
        index_cols: &[String],
    ) -> Result<(), GatewayError> {
        let Some(shard_db) = &self.shard_db else {
            return Ok(());
        };

        let col_idx = self.catalog.get_table(table).and_then(|t| {
            t.columns.iter().position(|c| {
                index_cols
                    .first()
                    .is_some_and(|ic| ic.eq_ignore_ascii_case(&c.name))
            })
        });

        let Some(col_idx) = col_idx else {
            return Ok(());
        };

        INDEX_BACKFILL_IN_PROGRESS_COUNT.fetch_add(1, Ordering::Relaxed);
        let scan_result = self
            .backfill_index_scan(shard_db, index_name, table, col_idx)
            .await;
        INDEX_BACKFILL_IN_PROGRESS_COUNT.fetch_sub(1, Ordering::Relaxed);
        let op_id = scan_result?;

        self.catalog.mark_index_ready(index_name, op_id);
        self.publish_exact_index_stats(table, index_cols).await;
        Ok(())
    }

    /// Scans `view_output/{table}/` in bounded batches, encodes each row as
    /// `0x03‖op_id(BE8)‖col_val(BE8)` → fixed-width-i64-encoded row bytes,
    /// and writes it into `shard_db`. Returns the minted `op_id` on success.
    async fn backfill_index_scan(
        &self,
        shard_db: &rockstream_storage::ShardDb,
        index_name: &str,
        table: &str,
        col_idx: usize,
    ) -> Result<u64, GatewayError> {
        let prefix = format!("view_output/{table}/");
        let (rows, truncated) = shard_db
            .scan_prefix_bounded(prefix.as_bytes(), MAX_INDEX_BACKFILL_ROWS * 1024)
            .await?;
        if truncated || rows.len() > MAX_INDEX_BACKFILL_ROWS {
            return Err(GatewayError::IndexBackfillRowLimitExceeded {
                index_name: index_name.to_string(),
                table: table.to_string(),
                row_limit: MAX_INDEX_BACKFILL_ROWS,
            });
        }

        let op_id = self.catalog.mint_index_op_id();
        for batch in rows.chunks(INDEX_BACKFILL_BATCH_ROWS) {
            for (_, value) in batch {
                let row_str = String::from_utf8_lossy(value);
                let fields: Vec<&str> = row_str.split('\t').collect();
                let Some(col_val) = fields.get(col_idx).and_then(|s| s.parse::<i64>().ok()) else {
                    continue;
                };
                let mut encoded_row = Vec::with_capacity(fields.len() * 8);
                let mut all_int = true;
                for f in &fields {
                    match f.parse::<i64>() {
                        Ok(v) => encoded_row.extend_from_slice(&v.to_be_bytes()),
                        Err(_) => {
                            all_int = false;
                            break;
                        }
                    }
                }
                if !all_int {
                    continue;
                }
                let mut key = Vec::with_capacity(17);
                key.push(0x03u8); // ShardPrefix::ViewOutput
                key.extend_from_slice(&op_id.to_be_bytes());
                key.extend_from_slice(&col_val.to_be_bytes());
                shard_db.put(&key, &encoded_row).await?;
                INDEX_BACKFILL_ROWS_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(op_id)
    }

    async fn publish_exact_index_stats(&self, table: &str, index_cols: &[String]) {
        let Some(shard_db) = &self.shard_db else {
            return;
        };
        let Some(catalog_table) = self.catalog.get_table(table) else {
            return;
        };
        let rows = match shard_db
            .scan_prefix(format!("view_output/{table}/").as_bytes())
            .await
        {
            Ok(rows) => rows,
            Err(_) => return,
        };

        let mut col_stats = Vec::new();
        for index_col in index_cols {
            let Some(col_idx) = catalog_table
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(index_col))
            else {
                continue;
            };
            let values: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|(_, value)| {
                    String::from_utf8_lossy(value)
                        .split('\t')
                        .nth(col_idx)
                        .map(|field| field.as_bytes().to_vec())
                })
                .collect();
            let exact_values: Vec<Vec<u8>> =
                values.iter().filter_map(|value| value.clone()).collect();
            let mut stats = ColumnStats::from_values(
                col_idx as u16,
                &values,
                ScatterPruningConfig::default().shard_bloom_budget_bytes,
            );
            if !exact_values.is_empty() {
                let filter = build_exact_membership_filter(&exact_values);
                rockstream_types::metrics::set_shard_bloom_filter_bytes_used(
                    1,
                    0,
                    col_idx as u16,
                    filter.len() as u64,
                );
                stats.bloom_filter = Some(filter);
            }
            col_stats.push(stats);
        }

        if !col_stats.is_empty() {
            self.catalog.set_shard_stats(
                table,
                vec![ShardColumnStats {
                    shard_id: ShardId(0),
                    view_id: ViewId(1),
                    checkpoint_epoch: 1,
                    col_stats,
                }],
            );
        }
    }

    /// Handle `DROP INDEX <name>` — v0.32.
    fn handle_drop_index<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let rest = q["DROP INDEX".len()..].trim();
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .to_lowercase();

        if name.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "DROP INDEX requires an index name".to_owned(),
            )))]);
        }

        if self.catalog.get_index(&name).is_none() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("index \"{name}\" does not exist"),
            )))]);
        }

        self.catalog.remove_index(&name);
        Ok(vec![Response::Execution(
            Tag::new("DROP INDEX").with_rows(0),
        )])
    }

    /// Handle `REBUILD INDEX <name>` — v0.32. Transitions index back to Building.
    fn handle_rebuild_index<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
        let rest = q["REBUILD INDEX".len()..].trim();
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .to_lowercase();

        if name.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "REBUILD INDEX requires an index name".to_owned(),
            )))]);
        }

        if !self.catalog.rebuild_index(&name) {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("index \"{name}\" does not exist"),
            )))]);
        }

        Ok(vec![Response::Execution(
            Tag::new("REBUILD INDEX").with_rows(0),
        )])
    }

    /// Handle `MARK INDEX <name> READY op_id=<n>` — transition index to Ready and bind its op_id.
    ///
    /// Called by the IVM engine (or admin) after backfill completes so the gateway
    /// can route equality-predicate SELECTs through the index arrangement.
    async fn handle_mark_index_ready(&self, q: &str) -> PgWireResult<Vec<Response<'static>>> {
        // Parse: MARK INDEX <name> READY [op_id=<n>]
        let rest = q["MARK INDEX".len()..].trim();
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 2 || parts[1].to_lowercase() != "ready" {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "MARK INDEX requires: MARK INDEX <name> READY [op_id=<n>]".to_owned(),
            )))]);
        }
        let name = parts[0].trim_end_matches(';').to_lowercase();
        if name.is_empty() {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "MARK INDEX requires an index name".to_owned(),
            )))]);
        }

        // Parse optional op_id=<n>
        let op_id: Option<u64> = parts.iter().find_map(|p| {
            let pl = p.to_lowercase();
            pl.strip_prefix("op_id=")
                .and_then(|v| v.trim_end_matches(';').parse().ok())
        });

        let op_id_val = match op_id {
            Some(v) => v,
            None => {
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "MARK INDEX READY requires op_id=<n>".to_owned(),
                )))]);
            }
        };

        if !self.catalog.mark_index_ready(&name, op_id_val) {
            return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("index \"{name}\" does not exist"),
            )))]);
        }

        if let Some(entry) = self.catalog.get_index(&name) {
            self.publish_exact_index_stats(&entry.table, &entry.index_cols)
                .await;
        }

        Ok(vec![Response::Execution(
            Tag::new("MARK INDEX").with_rows(0),
        )])
    }

    /// Handle `SET rockstream.<var> = <value>` — update per-connection session state.
    fn handle_set_rockstream(
        &self,
        _q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let Some(id) = conn_id else {
            return Ok(vec![promote_response(Response::Execution(Tag::new("SET")))]);
        };

        // Parse: SET [LOCAL] rockstream.<var> = <value>
        // ql is already lowercased
        let after_set = if let Some(rest) = ql.strip_prefix("set local rockstream.") {
            rest
        } else {
            ql.strip_prefix("set rockstream.").unwrap_or(ql)
        };
        // after_set: "idempotency_key = 'str'" or "source_epoch = 42"
        let eq_pos = after_set.find('=').unwrap_or(after_set.len());
        let var_name = after_set[..eq_pos].trim();
        let val_raw = after_set[eq_pos + 1..].trim().trim_end_matches(';');

        let mut session = self.sessions.entry(id.to_string()).or_default();

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
                    session.max_staleness = None;
                }
            }
            "session_wait_for" => {
                session.session_wait_for_enabled = val_raw.trim_matches('\'') != "off";
                if session.session_wait_for_enabled {
                    session.max_staleness = None;
                }
            }
            "session_wait_for_timeout_ms" => {
                if let Ok(n) = val_raw.trim_matches('\'').parse::<u64>() {
                    session.session_wait_for_timeout_ms = n;
                }
            }
            "max_staleness" => {
                let parsed = parse_duration_literal(val_raw.trim_matches('\''));
                session.max_staleness = parsed;
                session.frontier_age_ms = None;
                session.pending_notice = None;
                if parsed.is_some() {
                    session.wait_for_token = None;
                    session.session_wait_for_enabled = false;
                }
                if parsed.is_none() {
                    session.guc_params.remove("frontier_age_ms");
                }
            }
            _ => {}
        }
        drop(session);
        Ok(vec![promote_response(Response::Execution(Tag::new("SET")))])
    }

    /// COMMIT handler: flush write buffer to ShardDb atomically.
    /// Deliver any transactional NOTIFYs buffered during this transaction.
    fn flush_pending_notifies(&self, conn_id: &str) {
        if let Some((_, pending)) = self.pending_notifies.remove(conn_id) {
            let sender_pid = self
                .sessions
                .get(conn_id)
                .map(|s| s.backend_pid as i32)
                .unwrap_or(0);
            for (channel, payload) in pending {
                self.notify_registry.deliver(&channel, &payload, sender_pid);
            }
        }
    }

    async fn handle_commit(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        let Some(conn_id) = conn_id else {
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new(
                "COMMIT",
            )))]);
        };

        let mut entry = self.write_buffers.entry(conn_id.to_string()).or_default();
        entry.clear_savepoints();
        if entry.is_empty() {
            // No DML — still deliver any transactional NOTIFYs.
            self.flush_pending_notifies(conn_id);
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new(
                "COMMIT",
            )))]);
        }

        let Some(shard_db) = &self.shard_db else {
            // No shard — discard buffer, return COMMIT (best effort without storage)
            self.flush_pending_notifies(conn_id);
            entry.clear();
            return Ok(vec![promote_response(Response::TransactionEnd(Tag::new(
                "COMMIT",
            )))]);
        };

        // ── Idempotency envelope ─────────────────────────────────────────────
        // A write must carry a dedup envelope so a client-side retry of a
        // COMMIT can never double-apply it. If neither an explicit
        // `SET rockstream.idempotency_key` nor `SET rockstream.source_epoch`
        // was set, the server mints a fresh CSPRNG-derived key here — this is
        // per-commit and can never collide with a prior key, so (unlike the
        // explicit-key path) no prior-commit replay lookup is needed for it.
        let (idempotency_key, _source_epoch_envelope, server_generated) = {
            let session = self.sessions.entry(conn_id.to_string()).or_default();
            (
                session.idempotency_key,
                session.source_epoch_envelope,
                session.idempotency_key.is_none() && session.source_epoch_envelope.is_none(),
            )
        };
        let idempotency_key = if server_generated {
            use rand::RngCore;
            let mut generated = [0u8; 16];
            rand::thread_rng().fill_bytes(&mut generated);
            Some(generated)
        } else {
            idempotency_key
        };
        if !server_generated {
            if let Some(key_hash) = idempotency_key {
                // Explicit client key — check for prior commit with this key
                // (idempotent replay → noop). Server-generated keys skip this
                // lookup: they are freshly minted per commit and can never
                // collide with a previous one.
                match shard_db.get_idempotency_epoch(0, key_hash).await {
                    Ok(Some(_prev_epoch)) => {
                        // Already committed — discard buffer and return COMMIT noop
                        entry.clear();
                        return Ok(vec![promote_response(Response::TransactionEnd(Tag::new(
                            "COMMIT",
                        )))]);
                    }
                    Ok(None) => {} // proceed
                    Err(e) => {
                        return Err(PgWireError::ApiError(Box::new(
                            crate::error::GatewayError::Storage(e),
                        )));
                    }
                }
            }
        }

        let ops = entry.drain();
        let affected = ops.len();
        drop(entry); // release DashMap entry guard before await
        let _commit_guard = self.shard_commit_lock.lock().await;

        // Allocate next epoch
        let epoch = shard_db.try_next_epoch().ok_or_else(|| {
            PgWireError::ApiError(Box::new(crate::error::GatewayError::CommitEpochExhausted))
        })?;

        let mut batch = rockstream_storage::WriteBatch::new();
        append_dml_ops(&mut batch, &ops);
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
        self.frontier_published_at_ms
            .store(current_time_ms(), Ordering::SeqCst);

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
            for view_name in self.reachable_compiled_views(&changed_tables) {
                if let Err(error) = self
                    .recompute_compiled_view(&view_name, &ops, shard_db)
                    .await
                {
                    tracing::warn!(
                        view = %view_name,
                        error = %error,
                        "compiled view refresh failed after commit"
                    );
                }
            }
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

        // Deliver transactional NOTIFYs buffered during this transaction.
        self.flush_pending_notifies(conn_id);

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
            // Discard transactional NOTIFYs — aborted.
            self.pending_notifies.remove(conn_id);
        }
        Ok(vec![promote_response(Response::TransactionEnd(Tag::new(
            "ROLLBACK",
        )))])
    }

    // ── Slice 5: PgBouncer compat ─────────────────────────────────────────────

    /// DISCARD ALL: reset all session state for connection pooler compatibility.
    /// Clears: cursors, prepared statements, portals, write buffer, idempotency key,
    /// source_epoch_envelope, wait_for_token, TxStatus → Idle.
    fn handle_discard_all(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(id) = conn_id {
            // Clear write buffer
            if let Some(mut wb) = self.write_buffers.get_mut(id) {
                wb.clear();
            }
            // Clear all copy state
            self.copy_states.remove(id);
            // Clear all portals for this connection
            let portal_keys: Vec<_> = self
                .portal_states
                .iter()
                .filter(|e| e.key().0 == id)
                .map(|e| e.key().clone())
                .collect();
            for k in portal_keys {
                self.portal_states.remove(&k);
            }
            // Clear prepared statements set for this connection
            self.prepared_statements.remove(id);
            self.active_portals.remove(id);
            // Reset session state
            if let Some(mut session) = self.sessions.get_mut(id) {
                session.cursors.clear();
                session.idempotency_key = None;
                session.source_epoch_envelope = None;
                session.wait_for_token = None;
                session.last_written_epoch = None;
                session.max_staleness = None;
                session.frontier_age_ms = None;
                session.pending_notice = None;
                session.tx_status = crate::session::TxStatus::Idle;
                session.search_path = "public".to_string();
                session.current_namespace = "public".to_string();
                session.isolation_level = crate::session::IsolationLevel::ReadCommitted;
                session.session_wait_for_timeout_ms = 5_000;
                session.session_wait_for_enabled = true;
                session.guc_params.remove("frontier_age_ms");
            }
            // Unsubscribe from all LISTEN channels and discard pending NOTIFYs.
            self.notify_registry.unsubscribe_all(id);
            self.pending_notifies.remove(id);
        }
        Ok(vec![promote_response(Response::Execution(Tag::new(
            "DISCARD ALL",
        )))])
    }

    /// RESET ALL: reset GUC settings without clearing cursors or prepared statements.
    /// Resets: search_path, isolation_level, wait_for_timeout_ms, session_wait_for_enabled.
    fn handle_reset_all(&self, conn_id: Option<&str>) -> PgWireResult<Vec<Response<'static>>> {
        if let Some(id) = conn_id {
            if let Some(mut session) = self.sessions.get_mut(id) {
                session.search_path = "public".to_string();
                session.current_namespace = "public".to_string();
                session.isolation_level = crate::session::IsolationLevel::ReadCommitted;
                session.session_wait_for_timeout_ms = 5_000;
                session.session_wait_for_enabled = true;
                session.max_staleness = None;
                session.frontier_age_ms = None;
                session.pending_notice = None;
                session.guc_params.remove("frontier_age_ms");
                // Note: cursors, prepared statements, portals are NOT cleared by RESET ALL
            }
        }
        Ok(vec![promote_response(Response::Execution(Tag::new(
            "RESET",
        )))])
    }

    // ── End Slice 5 ───────────────────────────────────────────────────────────

    // ── Slice 3: Named Cursor handlers ────────────────────────────────────────

    /// DECLARE <name> CURSOR FOR <query>
    /// Executes the query eagerly, stores rows in session.cursors.
    /// Bound: MAX_CURSORS_PER_CONNECTION = 100 per connection.
    async fn handle_declare_cursor(
        &self,
        q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        use crate::session::{CursorState, MAX_CURSORS_PER_CONNECTION};
        // Parse: DECLARE <name> [NO SCROLL] CURSOR [WITH|WITHOUT HOLD] FOR <query>
        // Simple regex-free parse: after "declare " find "cursor" keyword
        let after_declare = &ql["declare ".len()..].trim_start();
        let cursor_pos = match after_declare.find(" cursor ") {
            Some(p) => p,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "42601".to_string(),
                        "syntax error in DECLARE CURSOR".to_string(),
                    ),
                )))]);
            }
        };
        let cursor_name = after_declare[..cursor_pos].trim().to_string();
        // Everything after "cursor [WITH HOLD|WITHOUT HOLD] for " is the query
        let rest = &after_declare[cursor_pos + " cursor ".len()..];
        // Handle: rest starts with "for " directly (no scroll/hold options), or " for " embedded
        if !rest.starts_with("for ") && !rest.contains(" for ") {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "missing FOR in DECLARE CURSOR".to_string(),
                ),
            )))]);
        }
        // Original-case query
        let ql_cursor_pos = "declare ".len() + cursor_pos;
        let ql_for_search = &q[ql_cursor_pos + " cursor ".len()..];
        let lower_for_search = ql_for_search.to_lowercase();
        let inner_start = if lower_for_search.starts_with("for ") {
            "for ".len()
        } else {
            lower_for_search
                .find(" for ")
                .map(|p| p + " for ".len())
                .unwrap_or(0)
        };
        let inner_sql = ql_for_search[inner_start..].trim();

        let conn_id_str = match conn_id {
            Some(id) => id,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        "DECLARE requires a connection context".to_string(),
                    ),
                )))]);
            }
        };

        // Check cursor limit
        let cursor_count = {
            let session = self.sessions.entry(conn_id_str.to_string()).or_default();
            session.cursors.len()
        };
        if cursor_count >= MAX_CURSORS_PER_CONNECTION {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new("ERROR".to_string(), "42P03".to_string(),
                    format!("[RS-2052] cursor.already_exists: too many open cursors (max {MAX_CURSORS_PER_CONNECTION}). next_steps: CLOSE the existing cursor or use a different name.")),
            )))]);
        }

        // Check for duplicate cursor name
        {
            let session = self.sessions.entry(conn_id_str.to_string()).or_default();
            if session.cursors.contains_key(&cursor_name) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_string(), "42P03".to_string(),
                        format!("[RS-2052] cursor.already_exists: cursor '{cursor_name}' already exists. next_steps: CLOSE the existing cursor or use a different name.")),
                )))]);
            }
        }

        // Execute the inner query to collect rows
        let rows: Vec<Vec<u8>> = if let Some(view_name) = extract_view_name_from_select(inner_sql) {
            if self.catalog.get_view(&view_name).is_some()
                && !self.catalog.is_backfill_published(&view_name)
            {
                return Ok(backfill_not_published_response(&view_name));
            }
            if let Some(shard_db) = &self.shard_db {
                let prefix = format!("view_output/{view_name}/");
                shard_db
                    .scan_prefix(prefix.as_bytes())
                    .await
                    .map(|kvs| kvs.into_iter().map(|(_, v)| v.to_vec()).collect())
                    .unwrap_or_default()
            } else {
                self.view_reader
                    .read_view(&view_name, None, ViewReadStrategy::HotOnly)
                    .await
                    .unwrap_or_default()
            }
        } else {
            vec![]
        };

        // Store in session
        {
            let mut session = self.sessions.entry(conn_id_str.to_string()).or_default();
            session
                .cursors
                .insert(cursor_name.clone(), CursorState { rows, position: 0 });
        }

        Ok(vec![promote_response(Response::Execution(Tag::new(
            "DECLARE",
        )))])
    }

    fn handle_fetch_cursor(
        &self,
        _q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let conn_id_str = match conn_id {
            Some(id) => id,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        "FETCH requires a connection context".to_string(),
                    ),
                )))])
            }
        };

        // Parse: FETCH [FORWARD] <n|ALL> FROM <name>
        // or: FETCH ALL FROM <name>
        let after_fetch = ql["fetch ".len()..].trim();

        let (count_str, cursor_name) = if let Some(from_pos) = after_fetch.find(" from ") {
            let count_part = after_fetch[..from_pos].trim();
            // Strip "forward" keyword
            let count_part = if let Some(rest) = count_part.strip_prefix("forward ") {
                rest.trim()
            } else {
                count_part
            };
            let name_part = after_fetch[from_pos + " from ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_string();
            (count_part.to_string(), name_part)
        } else {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "syntax error in FETCH: missing FROM".to_string(),
                ),
            )))]);
        };

        let fetch_all = count_str == "all";
        let fetch_count: usize = if fetch_all {
            usize::MAX
        } else {
            count_str.parse().unwrap_or(0)
        };

        // Get session rows
        let session_opt = self.sessions.get(conn_id_str);
        let cursor_data = match &session_opt {
            Some(session) => session.cursors.get(&cursor_name).map(|c| {
                let start = c.position;
                let end = if fetch_all {
                    c.rows.len()
                } else {
                    (c.position + fetch_count).min(c.rows.len())
                };
                (start, end, c.rows[start..end].to_vec())
            }),
            None => None,
        };
        drop(session_opt);

        let (_start, end, fetched_rows) = match cursor_data {
            Some(d) => d,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_string(), "34000".to_string(),
                        format!("[RS-2051] cursor.not_found: cursor '{cursor_name}' does not exist. next_steps: Use DECLARE to open a cursor before FETCH/MOVE/CLOSE.")),
                )))]);
            }
        };

        // Advance position
        if let Some(mut session) = self.sessions.get_mut(conn_id_str) {
            if let Some(cursor) = session.cursors.get_mut(&cursor_name) {
                cursor.position = end;
            }
        }

        let _n_fetched = fetched_rows.len();

        // Build DataRow responses
        let schema = Arc::new(vec![FieldInfo::new(
            "result".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )]);
        let schema_ref = schema.clone();
        let data_stream = futures::stream::iter(fetched_rows).map(move |row| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let s = String::from_utf8_lossy(&row).into_owned();
            encoder
                .encode_field(&Some(s.as_str()))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.finish()
        });

        Ok(vec![promote_response(Response::Query(QueryResponse::new(
            schema,
            data_stream,
        )))])
    }

    /// MOVE [FORWARD] n FROM <name> | MOVE ALL FROM <name>
    fn handle_move_cursor(
        &self,
        _q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let conn_id_str = match conn_id {
            Some(id) => id,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        "MOVE requires a connection context".to_string(),
                    ),
                )))])
            }
        };

        let after_move = ql["move ".len()..].trim();
        let (count_str, cursor_name) = if let Some(from_pos) = after_move.find(" from ") {
            let count_part = after_move[..from_pos].trim();
            let count_part = if let Some(rest) = count_part.strip_prefix("forward ") {
                rest.trim()
            } else {
                count_part
            };
            let name_part = after_move[from_pos + " from ".len()..]
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_string();
            (count_part.to_string(), name_part)
        } else {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "syntax error in MOVE: missing FROM".to_string(),
                ),
            )))]);
        };

        let move_all = count_str == "all";
        let move_count: usize = if move_all {
            usize::MAX
        } else {
            count_str.parse().unwrap_or(0)
        };

        let moved = if let Some(mut session) = self.sessions.get_mut(conn_id_str) {
            if let Some(cursor) = session.cursors.get_mut(&cursor_name) {
                let start = cursor.position;
                let end = if move_all {
                    cursor.rows.len()
                } else {
                    (cursor.position + move_count).min(cursor.rows.len())
                };
                cursor.position = end;
                end - start
            } else {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_string(), "34000".to_string(),
                        format!("[RS-2051] cursor.not_found: cursor '{cursor_name}' does not exist. next_steps: Use DECLARE to open a cursor before FETCH/MOVE/CLOSE.")),
                )))]);
            }
        } else {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_string(),
                    "34000".to_string(),
                    format!("[RS-2051] cursor.not_found: cursor '{cursor_name}' does not exist."),
                ),
            )))]);
        };

        Ok(vec![promote_response(Response::Execution(
            Tag::new("MOVE").with_rows(moved),
        ))])
    }

    /// CLOSE <name> | CLOSE ALL
    fn handle_close_cursor(
        &self,
        _q: &str,
        ql: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let conn_id_str = match conn_id {
            Some(id) => id,
            None => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        "CLOSE requires a connection context".to_string(),
                    ),
                )))])
            }
        };

        let name = ql["close ".len()..]
            .trim()
            .trim_end_matches(';')
            .trim()
            .to_string();

        if name == "all" {
            if let Some(mut session) = self.sessions.get_mut(conn_id_str) {
                session.cursors.clear();
            }
        } else {
            if let Some(mut session) = self.sessions.get_mut(conn_id_str) {
                if session.cursors.remove(&name).is_none() {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new("ERROR".to_string(), "34000".to_string(),
                            format!("[RS-2051] cursor.not_found: cursor '{name}' does not exist. next_steps: Use DECLARE to open a cursor before FETCH/MOVE/CLOSE.")),
                    )))]);
                }
            }
        }

        Ok(vec![promote_response(Response::Execution(Tag::new(
            "CLOSE",
        )))])
    }

    // ── End Slice 3 ───────────────────────────────────────────────────────────

    /// INSERT handler: accumulate rows in the write buffer.
    ///
    /// Supports multi-row `VALUES (v1, v2), (v3, v4), ...` lists (v0.42.2):
    /// each row tuple becomes its own buffered `DmlOp::Insert`, reusing the
    /// existing single-row semantics per row. A malformed tuple (wrong value
    /// count) is a hard parse error (`RS-2056`), not silent corruption.
    async fn handle_insert(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        // Parse INSERT INTO <table> [(cols)] VALUES (v1, v2, ...)[, (v1, v2, ...)]* [RETURNING ...]
        let returning = q.to_lowercase().contains(" returning ");
        let (table, cols, rows) = match parse_insert(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        let known_table_columns: Option<Vec<String>> = self
            .catalog
            .get_table(&table)
            .map(|ct| ct.columns.into_iter().map(|c| c.name).collect());
        if cols.is_empty() && known_table_columns.is_none() {
            return Ok(vec![promote_response(Response::Error(Box::new(
                ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!(
                        "[RS-2056] write.malformed_values_list: INSERT INTO {table} has no column list and the table's schema is unknown. next_steps: Provide an explicit column list, e.g. INSERT INTO {table} (col1, col2) VALUES (...), or create the table before inserting into it."
                    ),
                ),
            )))]);
        }
        let table_columns: Vec<String> = known_table_columns.unwrap_or_else(|| cols.clone());
        // Column list used to resolve VALUES tuples into named fields: the
        // explicit column list if given, otherwise the table's declared
        // column order for positional (no-column-list) INSERTs.
        let insert_cols: Vec<String> = if cols.is_empty() {
            table_columns.clone()
        } else {
            cols.clone()
        };
        if self.catalog.get_table(&table).is_none() && !insert_cols.is_empty() {
            self.catalog.add_table(CatalogTable {
                name: table.clone(),
                columns: insert_cols
                    .iter()
                    .map(|c| CatalogColumn {
                        name: c.clone(),
                        data_type: "Utf8".to_string(),
                    })
                    .collect(),
            });
        }
        let metadata = self
            .table_insert_metadata
            .get(&table)
            .map(|entry| entry.value().clone());

        let mut returning_rows: Vec<Vec<String>> = Vec::with_capacity(rows.len());
        let mut row_keys = Vec::with_capacity(rows.len());
        for values in &rows {
            let mut value_map: HashMap<String, String> = insert_cols
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect();
            if let Some(metadata) = &metadata {
                for (column, kind) in &metadata.generated_columns {
                    if value_map.contains_key(column) {
                        continue;
                    }
                    let generated = match kind {
                        GeneratedColumnKind::RandomUuid => generate_uuid_v4_string(),
                        GeneratedColumnKind::Identity => metadata
                            .identity_sequences
                            .get(column)
                            .map(|seq| seq.fetch_add(1, Ordering::SeqCst) + 1)
                            .unwrap_or(1)
                            .to_string(),
                    };
                    value_map.insert(column.clone(), generated);
                }
            }

            let stored_cols = if table_columns.is_empty() {
                cols.clone()
            } else {
                table_columns.clone()
            };
            let stored_values: Vec<String> = stored_cols
                .iter()
                .map(|col| value_map.get(col).cloned().unwrap_or_default())
                .collect();
            let row_key = build_row_key(&stored_cols, &stored_values);
            let values_tsv = stored_values.join("\t");

            let op = DmlOp::Insert {
                table: table.clone(),
                cols: stored_cols.clone(),
                values_tsv,
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
            row_keys.push(row_key);
            returning_rows.push(stored_values);
        }

        // Standard-PostgreSQL autocommit: any statement outside an explicit
        // `BEGIN...COMMIT`/`ROLLBACK` block commits immediately on success,
        // whether or not it has a `RETURNING` clause. Inside an explicit
        // block, buffering is unchanged.
        let in_explicit_block = conn_id
            .and_then(|id| self.sessions.get(id))
            .map(|s| s.in_explicit_block)
            .unwrap_or(false);
        if !in_explicit_block {
            let needs_read_back = returning
                && metadata
                    .as_ref()
                    .is_some_and(|m| !m.generated_columns.is_empty());
            let commit_responses = self.handle_commit(conn_id).await?;
            if commit_responses
                .iter()
                .any(|response| matches!(response, Response::Error(_)))
            {
                return Ok(commit_responses);
            }
            if needs_read_back {
                if let Some(id) = conn_id {
                    let timeout_ms = self
                        .sessions
                        .get(id)
                        .map(|s| s.session_wait_for_timeout_ms)
                        .unwrap_or(5_000);
                    if let Some(token) = self
                        .sessions
                        .get(id)
                        .and_then(|s| s.last_written_epoch.clone())
                    {
                        let _ = self.wait_for_epoch(token.source_epoch, timeout_ms).await;
                    }
                }
                if let Some(shard_db) = &self.shard_db {
                    let cols_for_read = if table_columns.is_empty() {
                        cols.clone()
                    } else {
                        table_columns.clone()
                    };
                    let mut read_back_rows = Vec::with_capacity(row_keys.len());
                    for row_key in &row_keys {
                        let key = format!("view_output/{table}/{row_key}");
                        if let Some(raw) = shard_db.get(key.as_bytes()).await.map_err(|e| {
                            PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                        })? {
                            let fields: Vec<String> = String::from_utf8_lossy(&raw)
                                .split('\t')
                                .map(|s| s.to_string())
                                .collect();
                            let mut row = fields;
                            row.resize(cols_for_read.len(), String::new());
                            read_back_rows.push(row);
                        }
                    }
                    if !read_back_rows.is_empty() {
                        returning_rows = read_back_rows;
                    }
                }
            }
        }

        if returning {
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
            let data_stream = stream::iter(returning_rows).map(move |values| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                for v in &values {
                    encoder
                        .encode_field(&Some(v.clone()))
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                }
                encoder.finish()
            });
            let stream = Box::pin(data_stream);
            return Ok(vec![promote_response(Response::Query(QueryResponse::new(
                schema, stream,
            )))]);
        }

        Ok(vec![promote_response(Response::Execution(
            Tag::new("INSERT").with_oid(0u32).with_rows(rows.len()),
        ))])
    }

    /// Build a `RETURNING` response for `UPDATE`/`DELETE`, projecting `rows`
    /// (each aligned to `table_columns` order) down to `returning_cols`
    /// (`["*"]` means "all declared columns").
    fn build_returning_response(
        &self,
        table: &str,
        table_columns: &[String],
        returning_cols: &[String],
        rows: Vec<Vec<String>>,
    ) -> Response<'static> {
        let is_star = returning_cols.len() == 1 && returning_cols[0] == "*";
        let projected_cols: Vec<String> = if is_star {
            table_columns.to_vec()
        } else {
            returning_cols.to_vec()
        };
        let catalog_table = self.catalog.get_table(table);
        let schema_fields: Vec<FieldInfo> = projected_cols
            .iter()
            .map(|col| {
                if let Some(ct) = &catalog_table {
                    if let Some(c) = ct.columns.iter().find(|c| c.name.eq_ignore_ascii_case(col)) {
                        let oid = arrow_type_to_pg_oid(&c.data_type);
                        return FieldInfo::new(
                            c.name.clone(),
                            None,
                            None,
                            pg_type_from_oid(oid),
                            FieldFormat::Text,
                        );
                    }
                }
                FieldInfo::new(col.clone(), None, None, Type::TEXT, FieldFormat::Text)
            })
            .collect();
        let schema = Arc::new(schema_fields);
        let schema_ref = schema.clone();
        let projected_rows: Vec<Vec<String>> = rows
            .into_iter()
            .map(|row| {
                projected_cols
                    .iter()
                    .map(|col| {
                        table_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(col))
                            .and_then(|idx| row.get(idx).cloned())
                            .unwrap_or_default()
                    })
                    .collect::<Vec<String>>()
            })
            .collect();
        let data_stream = stream::iter(projected_rows).map(move |values| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for v in &values {
                encoder
                    .encode_field(&Some(v.clone()))
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
            encoder.finish()
        });
        let stream = Box::pin(data_stream);
        promote_response(Response::Query(QueryResponse::new(schema, stream)))
    }

    /// UPDATE handler: true read-modify-write (v0.48 Slice A2/A3).
    ///
    /// Reads the existing row via `shard_db.get()` *before* buffering the
    /// write, so the complete new row (untouched columns preserved, per
    /// DESIGN.md §12.8.2) can be built and — when `RETURNING` was requested —
    /// projected back to the client. A nonexistent row is a zero-row no-op:
    /// no `DmlOp` is buffered.
    async fn handle_update(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let (table, set_pairs, where_pairs, returning_cols) = match parse_update(q) {
            Ok(v) => v,
            Err(e) => {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), e),
                )))]);
            }
        };

        // Build old row key from WHERE clause (unchanged from prior versions).
        let (old_cols, old_vals): (Vec<_>, Vec<_>) = where_pairs
            .iter()
            .map(|(c, v)| (c.clone(), v.clone()))
            .unzip();
        let old_row_key = build_row_key(&old_cols, &old_vals);

        // Full declared column order — used to build the complete merged
        // row. Falls back to WHERE ∪ SET columns if the table isn't in the
        // catalog (defensive; CREATE TABLE always registers it in practice).
        let table_columns: Vec<String> = self
            .catalog
            .get_table(&table)
            .map(|ct| ct.columns.into_iter().map(|c| c.name).collect())
            .unwrap_or_else(|| {
                let mut cols = old_cols.clone();
                for (c, _) in &set_pairs {
                    if !cols.contains(c) {
                        cols.push(c.clone());
                    }
                }
                cols
            });

        let Some(shard_db) = &self.shard_db else {
            // No shard attached: nothing to read or write.
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("UPDATE 0").with_rows(0),
            ))]);
        };

        let buffered_existing = conn_id.and_then(|id| {
            self.write_buffers
                .get(id)
                .and_then(|buffer| buffer.current_row_image(&table, &old_row_key))
        });
        let existing_str = match buffered_existing {
            Some(Some(tsv)) => Some(tsv),
            Some(None) => None,
            None => {
                let old_key = format!("view_output/{table}/{old_row_key}");
                shard_db
                    .get(old_key.as_bytes())
                    .await
                    .map_err(|e| {
                        PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                    })?
                    .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
            }
        };

        let Some(existing_str) = existing_str else {
            // Row does not exist: zero rows affected, no write buffered.
            if let Some(returning_cols) = &returning_cols {
                return Ok(vec![self.build_returning_response(
                    &table,
                    &table_columns,
                    returning_cols,
                    Vec::new(),
                )]);
            }
            return Ok(vec![promote_response(Response::Execution(
                Tag::new("UPDATE 0").with_rows(0),
            ))]);
        };

        let existing_fields: Vec<String> =
            existing_str.split('\t').map(|s| s.to_string()).collect();
        let mut value_map: HashMap<String, String> = HashMap::new();
        for (i, col) in table_columns.iter().enumerate() {
            value_map.insert(
                col.clone(),
                existing_fields.get(i).cloned().unwrap_or_default(),
            );
        }
        for (c, v) in &set_pairs {
            value_map.insert(c.clone(), v.clone());
        }
        let new_vals: Vec<String> = table_columns
            .iter()
            .map(|c| value_map.get(c).cloned().unwrap_or_default())
            .collect();
        let new_row_key = build_row_key(&table_columns, &new_vals);
        let new_tsv = new_vals.join("\t");

        let op = DmlOp::Update {
            table: table.clone(),
            old_row_key: old_row_key.clone(),
            old_tsv: existing_str,
            new_row_key: new_row_key.clone(),
            new_tsv: new_tsv.clone(),
        };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                )))]);
            }
        }

        // Standard-PostgreSQL autocommit: any statement outside an explicit
        // `BEGIN...COMMIT`/`ROLLBACK` block commits immediately on success,
        // whether or not it has a `RETURNING` clause.
        let in_explicit_block = conn_id
            .and_then(|id| self.sessions.get(id))
            .map(|s| s.in_explicit_block)
            .unwrap_or(false);
        let mut result_row = new_vals.clone();
        if !in_explicit_block {
            let commit_responses = self.handle_commit(conn_id).await?;
            if commit_responses
                .iter()
                .any(|response| matches!(response, Response::Error(_)))
            {
                return Ok(commit_responses);
            }
            if returning_cols.is_some() {
                if let Some(id) = conn_id {
                    let timeout_ms = self
                        .sessions
                        .get(id)
                        .map(|s| s.session_wait_for_timeout_ms)
                        .unwrap_or(5_000);
                    if let Some(token) = self
                        .sessions
                        .get(id)
                        .and_then(|s| s.last_written_epoch.clone())
                    {
                        let _ = self.wait_for_epoch(token.source_epoch, timeout_ms).await;
                    }
                }
                let new_key = format!("view_output/{table}/{new_row_key}");
                if let Some(raw) = shard_db.get(new_key.as_bytes()).await.map_err(|e| {
                    PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
                })? {
                    let mut row: Vec<String> = String::from_utf8_lossy(&raw)
                        .split('\t')
                        .map(|s| s.to_string())
                        .collect();
                    row.resize(table_columns.len(), String::new());
                    result_row = row;
                } else {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            "[RS-2013] transaction.returning_key_not_found: UPDATE ... RETURNING committed, but the gateway could not read the expected post-update row at the current frontier. next_steps: Retry the write; if the row is consistently missing, check that the frontier used for the read-back has advanced past the commit epoch.".to_owned(),
                        ),
                    )))]);
                }
            }
        }
        if let Some(returning_cols) = &returning_cols {
            // Slice A3: reuse the INSERT ... RETURNING read-back pattern —
            // only performed outside an explicit transaction block; inside
            // one, RETURNING resolves at the eventual COMMIT using the
            // already-computed merged row (matches INSERT's literal-echo
            // behavior when no server-side generated value is involved).
            return Ok(vec![self.build_returning_response(
                &table,
                &table_columns,
                returning_cols,
                vec![result_row],
            )]);
        }

        Ok(vec![promote_response(Response::Execution(
            Tag::new("UPDATE 1").with_rows(1),
        ))])
    }

    /// DELETE handler: pre-image capture for RETURNING (v0.48 Slice A4/A5).
    ///
    /// When a `RETURNING` clause is present, the existing row is read via
    /// `shard_db.get()` *before* the write is enqueued — the row is gone
    /// from `view_output` once the `WriteBatch` commits, so this is the only
    /// point the pre-delete state can be captured (DESIGN.md §13.5.2). Plain
    /// `DELETE` (no `RETURNING`) skips this extra read entirely.
    async fn handle_delete(
        &self,
        q: &str,
        conn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response<'static>>> {
        let (table, where_pairs, returning_cols) = match parse_delete(q) {
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

        let table_columns: Vec<String> = self
            .catalog
            .get_table(&table)
            .map(|ct| ct.columns.into_iter().map(|c| c.name).collect())
            .unwrap_or_else(|| cols.clone());

        // Pre-image capture: v0.51.4 Slice 0 needs this for every DELETE (not
        // just `RETURNING` ones) so the compiled-view refresh path can build
        // a true row-level delta (weight -1) instead of a full-table
        // rescan — see `DmlOp::Delete::returning_tsv` doc comment. This is a
        // single-row `get()` by key, bounded and proportional to one row,
        // not a table scan.
        let mut captured_row: Option<Vec<String>> = None;
        {
            let buffered_existing = conn_id.and_then(|id| {
                self.write_buffers
                    .get(id)
                    .and_then(|buffer| buffer.current_row_image(&table, &row_key))
            });
            let existing_str = match buffered_existing {
                Some(Some(tsv)) => Some(tsv),
                Some(None) => None,
                None => {
                    if let Some(shard_db) = &self.shard_db {
                        let key = format!("view_output/{table}/{row_key}");
                        shard_db
                            .get(key.as_bytes())
                            .await
                            .map_err(|e| {
                                PgWireError::ApiError(Box::new(
                                    crate::error::GatewayError::Storage(e),
                                ))
                            })?
                            .map(|raw| String::from_utf8_lossy(&raw).to_string())
                    } else {
                        None
                    }
                }
            };
            match existing_str {
                Some(tsv) => {
                    let mut row: Vec<String> = tsv.split('\t').map(|s| s.to_string()).collect();
                    row.resize(table_columns.len(), String::new());
                    captured_row = Some(row);
                }
                None if returning_cols.is_some() => {
                    if let Some(ref rcols) = returning_cols {
                        return Ok(vec![self.build_returning_response(
                            &table,
                            &table_columns,
                            rcols,
                            Vec::new(),
                        )]);
                    }
                }

                None => {}
            }
        }

        let returning_tsv = captured_row.as_ref().map(|row| row.join("\t"));

        let op = DmlOp::Delete {
            table: table.clone(),
            row_key,
            returning_tsv,
        };

        if let Some(id) = conn_id {
            let mut entry = self.write_buffers.entry(id.to_string()).or_default();
            if let Err(e) = entry.push(op) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                )))]);
            }
        }

        // Standard-PostgreSQL autocommit: any statement outside an explicit
        // `BEGIN...COMMIT`/`ROLLBACK` block commits immediately on success,
        // whether or not it has a `RETURNING` clause.
        let in_explicit_block = conn_id
            .and_then(|id| self.sessions.get(id))
            .map(|s| s.in_explicit_block)
            .unwrap_or(false);
        if !in_explicit_block {
            let commit_responses = self.handle_commit(conn_id).await?;
            if commit_responses
                .iter()
                .any(|response| matches!(response, Response::Error(_)))
            {
                return Ok(commit_responses);
            }
        }

        if let Some(returning_cols) = &returning_cols {
            // Slice A5: project the captured pre-image directly — the row
            // is gone from view_output once the WriteBatch commits, so
            // there is no post-commit re-read to perform. ROLLBACK / ROLLBACK
            // TO SAVEPOINT discard this buffered DmlOp::Delete (and its
            // captured returning_tsv) via WriteBuffer's existing mechanism;
            // nothing is ever written to the shard before an actual COMMIT.
            let row = captured_row.unwrap_or_default();
            return Ok(vec![self.build_returning_response(
                &table,
                &table_columns,
                returning_cols,
                vec![row],
            )]);
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
        } else if let Some(ref cat_tbl) = catalog_table {
            cat_tbl.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            Vec::new()
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
        let _commit_guard = self.shard_commit_lock.lock().await;

        let epoch = shard_db.try_next_epoch().ok_or_else(|| {
            PgWireError::ApiError(Box::new(crate::error::GatewayError::CommitEpochExhausted))
        })?;
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
        self.frontier_published_at_ms
            .store(current_time_ms(), Ordering::SeqCst);

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
        use pgwire::api::PgWireConnectionState;
        use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
        use pgwire::messages::startup::{Authentication, ParameterStatus};

        // ── Initial Startup message ───────────────────────────────────────────
        if let pgwire::messages::PgWireFrontendMessage::Startup(ref startup) = message {
            save_startup_parameters_to_metadata(client, startup);

            let app_name = startup
                .parameters
                .iter()
                .find(|(k, _)| k.to_lowercase() == "application_name")
                .map(|(_, v)| v.clone())
                .unwrap_or_default();

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
                    // v0.51.5: the old spoofable `cn` startup-parameter stub
                    // has been removed. The CN is now only ever the Subject
                    // CN of a real, CA-validated TLS client certificate,
                    // recorded by `MtlsCnExtractingVerifier` during the TLS
                    // handshake and looked up here by peer socket address.
                    match crate::tls::lookup_mtls_cn(&client.socket_addr()) {
                        Some(cn) => {
                            client
                                .metadata_mut()
                                .insert("_rs_principal".to_string(), format!("cert:{cn}"));
                        }
                        None => {
                            return Err(pgwire::error::PgWireError::UserError(Box::new(
                                pgwire::error::ErrorInfo::new(
                                    "FATAL".to_string(),
                                    "28000".to_string(),
                                    "[RS-2404] auth.mtls_no_verified_cert: no verified client certificate CN for this connection. next_steps: Connect with a client certificate signed by the configured CA over sslmode=verify-full; a bare TCP or TLS connection without a client cert cannot use --auth=mtls.".to_string(),
                                ),
                            )));
                        }
                    }
                }
                AuthMode::Scram => {
                    if let Ok(conn_id) = CONN_ID.try_with(|id| id.clone()) {
                        client
                            .metadata_mut()
                            .insert("_rs_conn_id".to_string(), conn_id.clone());
                        {
                            let mut session = self.sessions.entry(conn_id.clone()).or_default();
                            session.application_name = app_name;
                            session.scram_auth_state = ScramAuthState::Idle;
                        }
                        if self.cancellation_registry.len() < MAX_CONNECTIONS {
                            let token = CANCEL_TOKEN
                                .try_with(|t| t.clone())
                                .unwrap_or_else(|_| CancelToken::new());
                            if let Some(session) = self.sessions.get(&conn_id) {
                                self.cancellation_registry
                                    .insert((session.backend_pid, session.cancel_secret), token);
                            }
                        }
                        client
                            .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                                vec!["SCRAM-SHA-256".to_owned()],
                            )))
                            .await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    } else {
                        // No CONN_ID — fallback (e.g. test without task-local scope)
                        client
                            .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                                vec!["SCRAM-SHA-256".to_owned()],
                            )))
                            .await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    }
                    return Ok(());
                }
                AuthMode::Md5 => {
                    // S6 (second half): send MD5 challenge
                    if let Ok(conn_id) = CONN_ID.try_with(|id| id.clone()) {
                        client
                            .metadata_mut()
                            .insert("_rs_conn_id".to_string(), conn_id.clone());
                        let salt: [u8; 4] = rand::random();
                        {
                            let mut session = self.sessions.entry(conn_id.clone()).or_default();
                            session.application_name = app_name;
                            session.md5_auth_salt = Some(salt);
                        }
                        if self.cancellation_registry.len() < MAX_CONNECTIONS {
                            let token = CANCEL_TOKEN
                                .try_with(|t| t.clone())
                                .unwrap_or_else(|_| CancelToken::new());
                            if let Some(session) = self.sessions.get(&conn_id) {
                                self.cancellation_registry
                                    .insert((session.backend_pid, session.cancel_secret), token);
                            }
                        }
                        client
                            .send(PgWireBackendMessage::Authentication(
                                Authentication::MD5Password(salt.to_vec()),
                            ))
                            .await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    } else {
                        let salt: [u8; 4] = rand::random();
                        client
                            .send(PgWireBackendMessage::Authentication(
                                Authentication::MD5Password(salt.to_vec()),
                            ))
                            .await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    }
                    return Ok(());
                }
            }

            // Off / Oidc / Mtls: finish authentication with per-connection pid/secret
            if let Ok(conn_id) = CONN_ID.try_with(|id| id.clone()) {
                client
                    .metadata_mut()
                    .insert("_rs_conn_id".to_string(), conn_id.clone());
                let (pid, secret) = {
                    let mut session = self.sessions.entry(conn_id.clone()).or_default();
                    session.application_name = app_name;
                    (session.backend_pid, session.cancel_secret)
                };
                if self.cancellation_registry.len() < MAX_CONNECTIONS {
                    let token = CANCEL_TOKEN
                        .try_with(|t| t.clone())
                        .unwrap_or_else(|_| CancelToken::new());
                    self.cancellation_registry.insert((pid, secret), token);
                }
                client
                    .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
                    .await?;
                if let Some(params) = GatewayServerParameterProvider.server_parameters(client) {
                    for (k, v) in params {
                        client
                            .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                                k, v,
                            )))
                            .await?;
                    }
                }
                client
                    .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
                        pid as i32,
                        secret as i32,
                    )))
                    .await?;
                client
                    .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        TransactionStatus::Idle,
                    )))
                    .await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
            } else {
                finish_authentication(client, &GatewayServerParameterProvider).await?;
            }
            return Ok(());
        }

        // ── Password / SASL exchange messages ────────────────────────────────
        if let pgwire::messages::PgWireFrontendMessage::PasswordMessageFamily(msg) = message {
            let conn_id = client
                .metadata()
                .get("_rs_conn_id")
                .cloned()
                .unwrap_or_default();

            match &self.auth_mode {
                AuthMode::Scram => {
                    // Read current auth state
                    let auth_state = self
                        .sessions
                        .get(&conn_id)
                        .map(|s| s.scram_auth_state.clone())
                        .unwrap_or(ScramAuthState::Idle);

                    match auth_state {
                        ScramAuthState::Idle => {
                            // SASLInitialResponse — client-first-message
                            let sasl_resp = msg.into_sasl_initial_response()?;
                            let data = sasl_resp.data.ok_or_else(|| {
                                pgwire::error::PgWireError::UserError(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "FATAL".to_string(),
                                        "28P01".to_string(),
                                        "empty SASLInitialResponse".to_string(),
                                    ),
                                ))
                            })?;
                            let client_first = String::from_utf8_lossy(&data).into_owned();

                            // Parse gs2-header by skipping to byte after second comma.
                            // RFC 5802: gs2-header ends at second comma; bare starts after.
                            let client_first_bare = {
                                let mut comma_count = 0u32;
                                let mut start = 0usize;
                                for (i, c) in client_first.char_indices() {
                                    if c == ',' {
                                        comma_count += 1;
                                        if comma_count == 2 {
                                            start = i + 1;
                                            break;
                                        }
                                    }
                                }
                                client_first[start..].to_string()
                            };

                            // Postgres SCRAM: username is always empty in the SASL message ("n=,");
                            // the actual username comes from the startup "user" parameter.
                            let username = client
                                .metadata()
                                .get(pgwire::api::METADATA_USER)
                                .cloned()
                                .unwrap_or_default();

                            // Extract client nonce from "r=nonce"
                            let client_nonce = client_first_bare
                                .split(',')
                                .find(|p| p.starts_with("r="))
                                .and_then(|p| p.strip_prefix("r="))
                                .unwrap_or("")
                                .to_string();

                            // Look up user in role catalog
                            let role = self.role_catalog.get(&username).ok_or_else(|| {
                                pgwire::error::PgWireError::UserError(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "FATAL".to_string(),
                                        "28P01".to_string(),
                                        format!("[RS-2401] auth.invalid_password: password authentication failed for user '{username}'. next_steps: Check password and retry"),
                                    ),
                                ))
                            })?;

                            // Build server-first-message
                            let server_nonce_suffix: String =
                                B64_STANDARD.encode(rand::random::<[u8; 18]>());
                            let server_nonce = format!("{client_nonce}{server_nonce_suffix}");
                            let salt_b64 = B64_STANDARD.encode(&role.scram_salt);
                            let server_first = format!(
                                "r={server_nonce},s={salt_b64},i={}",
                                role.scram_iterations
                            );

                            // auth_message prefix: client_first_bare + "," + server_first
                            let auth_message_prefix = format!("{client_first_bare},{server_first}");

                            // Store state in session
                            if let Some(mut session) = self.sessions.get_mut(&conn_id) {
                                session.scram_auth_state = ScramAuthState::ServerFirstSent {
                                    salted_password: role.scram_salted_password.clone(),
                                    server_nonce: server_nonce.clone(),
                                    auth_message: auth_message_prefix,
                                    username,
                                };
                            }

                            client
                                .send(PgWireBackendMessage::Authentication(
                                    Authentication::SASLContinue(bytes::Bytes::from(server_first)),
                                ))
                                .await?;
                        }

                        ScramAuthState::ServerFirstSent {
                            salted_password,
                            server_nonce,
                            auth_message: auth_msg_prefix,
                            username,
                        } => {
                            // SASLResponse — client-final-message
                            let sasl_resp = msg.into_sasl_response()?;
                            let client_final =
                                String::from_utf8_lossy(&sasl_resp.data).into_owned();

                            // Parse client-final: c=...,r=...,p=...
                            let r_val = client_final
                                .split(',')
                                .find(|p| p.starts_with("r="))
                                .and_then(|p| p.strip_prefix("r="))
                                .unwrap_or("");
                            if r_val != server_nonce {
                                return Err(pgwire::error::PgWireError::UserError(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "FATAL".to_string(),
                                        "28P01".to_string(),
                                        format!("[RS-2401] auth.invalid_password: password authentication failed for user '{username}'. next_steps: Check password and retry"),
                                    ),
                                )));
                            }

                            let proof_b64 = client_final
                                .split(',')
                                .find(|p| p.starts_with("p="))
                                .and_then(|p| p.strip_prefix("p="))
                                .unwrap_or("");

                            // client-final-without-proof = everything before ",p="
                            let client_final_without_proof = client_final
                                .find(",p=")
                                .map(|i| &client_final[..i])
                                .unwrap_or(&client_final);

                            // Complete auth-message
                            let auth_message =
                                format!("{auth_msg_prefix},{client_final_without_proof}");

                            // Verify
                            let stored_key = scram_stored_key(&salted_password);
                            if !verify_client_proof(&stored_key, proof_b64, &auth_message) {
                                // Reset state
                                if let Some(mut session) = self.sessions.get_mut(&conn_id) {
                                    session.scram_auth_state = ScramAuthState::Idle;
                                }
                                return Err(pgwire::error::PgWireError::UserError(Box::new(
                                    pgwire::error::ErrorInfo::new(
                                        "FATAL".to_string(),
                                        "28P01".to_string(),
                                        format!("[RS-2401] auth.invalid_password: password authentication failed for user '{username}'. next_steps: Check password and retry"),
                                    ),
                                )));
                            }

                            // Compute and send server signature
                            let server_key = scram_server_key(&salted_password);
                            let server_sig = scram_server_signature(&server_key, &auth_message);
                            let server_sig_b64 = B64_STANDARD.encode(server_sig);
                            client
                                .feed(PgWireBackendMessage::Authentication(
                                    Authentication::SASLFinal(bytes::Bytes::from(format!(
                                        "v={server_sig_b64}"
                                    ))),
                                ))
                                .await?;

                            // Update principal and reset auth state
                            let (pid, secret) =
                                if let Some(mut session) = self.sessions.get_mut(&conn_id) {
                                    session.principal = Principal::ScramUser {
                                        username: username.clone(),
                                    };
                                    session.scram_auth_state = ScramAuthState::Idle;
                                    (session.backend_pid, session.cancel_secret)
                                } else {
                                    (0u32, 0u32)
                                };

                            // Complete authentication
                            client
                                .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
                                .await?;
                            if let Some(params) =
                                GatewayServerParameterProvider.server_parameters(client)
                            {
                                for (k, v) in params {
                                    client
                                        .feed(PgWireBackendMessage::ParameterStatus(
                                            ParameterStatus::new(k, v),
                                        ))
                                        .await?;
                                }
                            }
                            client
                                .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
                                    pid as i32,
                                    secret as i32,
                                )))
                                .await?;
                            client
                                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                                    TransactionStatus::Idle,
                                )))
                                .await?;
                            client.set_state(PgWireConnectionState::ReadyForQuery);
                        }
                    }
                }
                AuthMode::Md5 => {
                    // S6 (second half): verify MD5 password
                    let conn_id_clone = conn_id.clone();
                    let username = client.metadata().get("user").cloned().unwrap_or_default();
                    let salt = self
                        .sessions
                        .get(&conn_id_clone)
                        .and_then(|s| s.md5_auth_salt)
                        .unwrap_or([0u8; 4]);

                    let pwd_msg = msg.into_password()?;
                    let client_response = pwd_msg.password;

                    let role = self.role_catalog.get(&username);
                    let valid = role
                        .as_ref()
                        .and_then(|r| r.md5_hash.as_deref())
                        .map(|stored| {
                            crate::auth::verify_md5(stored, &username, &salt, &client_response)
                        })
                        .unwrap_or(false);

                    if !valid {
                        return Err(pgwire::error::PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "FATAL".to_string(),
                                "28P01".to_string(),
                                format!("[RS-2401] auth.invalid_password: password authentication failed for user '{username}'. next_steps: Check password and retry"),
                            ),
                        )));
                    }

                    let (pid, secret) =
                        if let Some(mut session) = self.sessions.get_mut(&conn_id_clone) {
                            session.principal = Principal::ScramUser {
                                username: username.clone(),
                            };
                            (session.backend_pid, session.cancel_secret)
                        } else {
                            (0u32, 0u32)
                        };

                    client
                        .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
                        .await?;
                    if let Some(params) = GatewayServerParameterProvider.server_parameters(client) {
                        for (k, v) in params {
                            client
                                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                                    k, v,
                                )))
                                .await?;
                        }
                    }
                    client
                        .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
                            pid as i32,
                            secret as i32,
                        )))
                        .await?;
                    client
                        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            TransactionStatus::Idle,
                        )))
                        .await?;
                    client.set_state(PgWireConnectionState::ReadyForQuery);
                }
                _ => {}
            }
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
        let conn_id = CONN_ID.try_with(|id| id.clone()).unwrap_or_else(|_| {
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
        });

        // Sync principal from startup metadata into the session (once per connection).
        if let Some(raw_principal) = client.metadata().get("_rs_principal").cloned() {
            let mut session = self.sessions.entry(conn_id.clone()).or_default();
            if session.principal == Principal::System && raw_principal != "system" {
                session.principal = if let Some(sub) = raw_principal.strip_prefix("jwt:") {
                    Principal::Jwt {
                        sub: sub.to_string(),
                    }
                } else if let Some(cn) = raw_principal.strip_prefix("cert:") {
                    Principal::CertCn { cn: cn.to_string() }
                } else {
                    Principal::System
                };
            }
        }

        let is_empty_query = query.chars().all(|c| c.is_whitespace() || c == ';');
        if is_empty_query {
            return Ok(vec![Response::EmptyQuery]);
        }

        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Parse query using sqlparser to check for semicolon-separated statements.
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        if let Ok(statements) = sqlparser::parser::Parser::parse_sql(&dialect, trimmed) {
            let active_statements: Vec<_> = statements
                .into_iter()
                .filter(|s| {
                    let s_str = s.to_string();
                    !s_str.trim().is_empty() && s_str.trim() != ";"
                })
                .collect();

            if active_statements.is_empty() {
                return Ok(vec![Response::EmptyQuery]);
            }
            if active_statements.len() > 1 {
                let mut all_responses = Vec::new();
                for stmt in active_statements {
                    let stmt_sql = stmt.to_string();
                    let res = self.do_query_single(client, &stmt_sql, &conn_id).await?;
                    for r in res {
                        all_responses.push(promote_response(r));
                    }
                }
                let coerced: Vec<Response<'a>> = all_responses.into_iter().collect();
                return Ok(coerced);
            } else if active_statements.len() == 1 {
                return self.do_query_single(client, query, &conn_id).await;
            }
        }

        self.do_query_single(client, query, &conn_id).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for GatewayHandler {
    type Statement = PreparedStatement;
    type QueryParser = PreparedStatementCache;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let stmt_name = message.name.clone().unwrap_or_default();

        // Bound prepared statements per connection with LRU eviction
        // (v0.51.6 Slice 1) instead of hard-erroring at the cap.
        {
            let mut conn_stmts = self
                .prepared_statements
                .entry(conn_id.clone())
                .or_insert_with(|| {
                    lru::LruCache::new(
                        std::num::NonZeroUsize::new(MAX_PREPARED_STATEMENTS_PER_CONN)
                            .unwrap_or(std::num::NonZeroUsize::MIN),
                    )
                });
            let is_new = !conn_stmts.contains(&stmt_name);
            if is_new && conn_stmts.len() >= MAX_PREPARED_STATEMENTS_PER_CONN {
                if let Some((evicted_name, _)) = conn_stmts.pop_lru() {
                    client.portal_store().rm_statement(&evicted_name);
                    PREPARED_STATEMENTS_COUNT.fetch_sub(1, Ordering::Relaxed);
                    PREPARED_STATEMENTS_EVICTED_COUNT.fetch_add(1, Ordering::Relaxed);
                }
            }
            conn_stmts.put(stmt_name.clone(), ());
            if is_new {
                PREPARED_STATEMENTS_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }

        let parser = self.query_parser();
        let types = message
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid).unwrap_or(Type::UNKNOWN))
            .collect::<Vec<Type>>();
        let parsed_stmt = parser.parse_sql(&message.query, &types).await?;

        let stmt = StoredStatement::new(
            message
                .name
                .clone()
                .unwrap_or_else(|| pgwire::api::DEFAULT_NAME.to_owned()),
            parsed_stmt,
            types,
        );

        client.portal_store().put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;

        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let portal_name = message.portal_name.clone().unwrap_or_default();

        // Bound portals per connection with LRU eviction (v0.51.6 Slice 1)
        // instead of hard-erroring at the cap.
        {
            let mut conn_portals =
                self.active_portals
                    .entry(conn_id.clone())
                    .or_insert_with(|| {
                        lru::LruCache::new(
                            std::num::NonZeroUsize::new(MAX_PORTALS_PER_CONN)
                                .unwrap_or(std::num::NonZeroUsize::MIN),
                        )
                    });
            let is_new = !conn_portals.contains(&portal_name);
            if is_new && conn_portals.len() >= MAX_PORTALS_PER_CONN {
                if let Some((evicted_name, _)) = conn_portals.pop_lru() {
                    client.portal_store().rm_portal(&evicted_name);
                    self.portal_states
                        .remove(&(conn_id.clone(), evicted_name.clone()));
                    PORTALS_COUNT.fetch_sub(1, Ordering::Relaxed);
                    PORTALS_EVICTED_COUNT.fetch_add(1, Ordering::Relaxed);
                }
            }
            conn_portals.put(portal_name.clone(), ());
            if is_new {
                PORTALS_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }

        let statement_name = message
            .statement_name
            .as_deref()
            .unwrap_or(pgwire::api::DEFAULT_NAME);

        // Binding a portal to a statement counts as use of that statement —
        // promote it to most-recently-used so it isn't evicted while a live
        // portal still depends on it (v0.51.6 Slice 1).
        if let Some(mut stmts) = self.prepared_statements.get_mut(&conn_id) {
            stmts.get(statement_name);
        }

        if let Some(statement) = client.portal_store().get_statement(statement_name) {
            let portal = Portal::try_new(&message, statement)?;
            client.portal_store().put_portal(Arc::new(portal));
            self.portal_states
                .remove(&(conn_id.clone(), portal_name.clone()));
            client
                .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
                .await?;
            Ok(())
        } else {
            Err(PgWireError::StatementNotFound(statement_name.to_owned()))
        }
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let name = message.name.as_deref().unwrap_or(pgwire::api::DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                client.portal_store().rm_statement(name);
                if let Some(mut stmts) = self.prepared_statements.get_mut(&conn_id) {
                    if stmts.pop(name).is_some() {
                        PREPARED_STATEMENTS_COUNT.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store().rm_portal(name);
                if let Some(mut portals) = self.active_portals.get_mut(&conn_id) {
                    if portals.pop(name).is_some() {
                        PORTALS_COUNT.fetch_sub(1, Ordering::Relaxed);
                    }
                }
                self.portal_states
                    .remove(&(conn_id.clone(), name.to_string()));
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_execute<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Execute,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn_id = client
            .metadata()
            .get("_rs_conn_id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let portal_name = message
            .name
            .clone()
            .unwrap_or_else(|| pgwire::api::DEFAULT_NAME.to_string());

        // Get portal
        let portal = if let Some(p) = client.portal_store().get_portal(&portal_name) {
            p
        } else {
            return Err(PgWireError::PortalNotFound(portal_name));
        };

        // Make sure client connection state is set
        client.set_state(pgwire::api::PgWireConnectionState::QueryInProgress);

        let format = portal.result_column_format.clone();
        PORTAL_FORMAT
            .scope(format, async move {
                // Check if we already have cached results for this portal.
                let cached = self
                    .portal_states
                    .get(&(conn_id.clone(), portal_name.clone()))
                    .map(|r| {
                        (
                            r.rows.clone(),
                            r.schema.clone(),
                            r.command_tag.clone(),
                            r.offset,
                        )
                    });

                let (rows, _schema, command_tag, offset) = if let Some(state) = cached {
                    state
                } else {
                    match ExtendedQueryHandler::do_query(
                        self,
                        client,
                        portal.as_ref(),
                        message.max_rows as usize,
                    )
                    .await?
                    {
                        Response::EmptyQuery => {
                            client
                                .send(PgWireBackendMessage::EmptyQueryResponse(
                                    EmptyQueryResponse::new(),
                                ))
                                .await?;
                            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                            return Ok(());
                        }
                        Response::Query(results) => {
                            let command_tag = results.command_tag().to_owned();
                            let schema = results.row_schema();
                            let mut data_rows = results.data_rows();
                            let mut rows = Vec::new();
                            while let Some(row) = data_rows.next().await {
                                rows.push(row?);
                            }

                            let new_state = PortalState {
                                rows: rows.clone(),
                                schema: schema.clone(),
                                command_tag: command_tag.clone(),
                                offset: 0,
                            };
                            self.portal_states
                                .insert((conn_id.clone(), portal_name.clone()), new_state);
                            (rows, schema, command_tag, 0)
                        }
                        Response::Execution(tag) => {
                            send_execution_response(client, tag).await?;
                            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                            return Ok(());
                        }
                        Response::TransactionStart(tag) => {
                            send_execution_response(client, tag).await?;
                            let mut transaction_status = client.transaction_status();
                            transaction_status = transaction_status.to_in_transaction_state();
                            client.set_transaction_status(transaction_status);
                            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                            return Ok(());
                        }
                        Response::TransactionEnd(tag) => {
                            send_execution_response(client, tag).await?;
                            let mut transaction_status = client.transaction_status();
                            transaction_status = transaction_status.to_idle_state();
                            client.set_transaction_status(transaction_status);
                            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                            return Ok(());
                        }
                        Response::Error(err) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                                .await?;
                            let mut transaction_status = client.transaction_status();
                            transaction_status = transaction_status.to_error_state();
                            client.set_transaction_status(transaction_status);
                            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                            return Ok(());
                        }
                        Response::CopyIn(result) => {
                            client.set_state(pgwire::api::PgWireConnectionState::CopyInProgress(
                                true,
                            ));
                            send_copy_in_response(client, result).await?;
                            return Ok(());
                        }
                        Response::CopyOut(result) => {
                            client.set_state(pgwire::api::PgWireConnectionState::CopyInProgress(
                                true,
                            ));
                            send_copy_out_response(client, result).await?;
                            return Ok(());
                        }
                        Response::CopyBoth(result) => {
                            client.set_state(pgwire::api::PgWireConnectionState::CopyInProgress(
                                true,
                            ));
                            send_copy_both_response(client, result).await?;
                            return Ok(());
                        }
                    }
                };

                let max_rows = message.max_rows as usize;
                let limit = if max_rows > 0 {
                    max_rows
                } else {
                    rows.len() - offset
                };

                let end = std::cmp::min(offset + limit, rows.len());
                let mut rows_sent = 0;
                for row in &rows[offset..end] {
                    client
                        .feed(PgWireBackendMessage::DataRow(row.clone()))
                        .await?;
                    rows_sent += 1;
                }

                let new_offset = offset + rows_sent;
                let suspended = max_rows > 0 && new_offset < rows.len();

                if suspended {
                    if let Some(mut state) = self
                        .portal_states
                        .get_mut(&(conn_id.clone(), portal_name.clone()))
                    {
                        state.offset = new_offset;
                    }
                    client
                        .send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                        .await?;
                } else {
                    self.portal_states
                        .remove(&(conn_id.clone(), portal_name.clone()));
                    let tag = Tag::new(&command_tag).with_rows(rows.len());
                    client
                        .send(PgWireBackendMessage::CommandComplete(tag.into()))
                        .await?;
                }

                client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
                Ok(())
            })
            .await
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
        let fields = describe_fields_for_query(&self.catalog, &target.statement.sql);
        Ok(DescribeStatementResponse::new(
            target.statement.parameter_types.clone(),
            fields,
        ))
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
        let fields = describe_fields_for_query(&self.catalog, &target.statement.statement.sql);
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
        let conn_id = CONN_ID.try_with(|id| id.clone()).unwrap_or_else(|_| {
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
        });

        // Sync principal from startup metadata into the session (once per connection).
        if let Some(raw_principal) = client.metadata().get("_rs_principal").cloned() {
            let mut session = self.sessions.entry(conn_id.clone()).or_default();
            if session.principal == Principal::System && raw_principal != "system" {
                session.principal = if let Some(sub) = raw_principal.strip_prefix("jwt:") {
                    Principal::Jwt {
                        sub: sub.to_string(),
                    }
                } else if let Some(cn) = raw_principal.strip_prefix("cert:") {
                    Principal::CertCn { cn: cn.to_string() }
                } else {
                    Principal::System
                };
            }
        }

        let query = portal.statement.statement.sql.as_str();
        let ql = query.trim().to_lowercase();

        // COPY IN via extended query protocol (e.g. tokio_postgres.copy_in()).
        if ql.starts_with("copy ") && ql.contains(" from stdin") {
            let responses = self.handle_copy_from_stdin(query, &conn_id)?;
            return Ok(responses
                .into_iter()
                .next()
                .unwrap_or(Response::Execution(Tag::new("OK"))));
        }

        // Parameter substitution: when the query has `$1`, `$2`, … placeholders
        // AND the portal has bound values, substitute them so that DataFusion
        // (and any other execution path) can evaluate the literals directly.
        let effective_query: String;
        let dispatch_query: &str = if !portal.parameters.is_empty() && query.contains('$') {
            effective_query = substitute_params(query, &portal.parameters);
            &effective_query
        } else {
            query
        };

        let responses = self
            .dispatch_async_with_conn(dispatch_query, Some(&conn_id))
            .await?;
        self.emit_session_annotations(client, &conn_id).await?;
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

            while let Some(nl_pos) = state.partial_line.find('\n') {
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

async fn relay_negotiated_3_2_connection(
    mut client_socket: tokio::net::TcpStream,
    full_msg: Vec<u8>,
    tls_acceptor_ref: Option<Arc<tokio_rustls::TlsAcceptor>>,
    factory_ref: Arc<GatewayHandlerFactory>,
    peer_addr: std::net::SocketAddr,
) {
    use tokio::io::AsyncWriteExt;

    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("[RS-0001] Failed to bind local relay listener: {e}; next_steps: retry the connection or inspect local socket limits");
            return;
        }
    };
    let local_addr = match listener.local_addr() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("[RS-0001] Failed to get local relay addr: {e}; next_steps: retry the connection or inspect local socket limits");
            return;
        }
    };

    let connect_fut = tokio::net::TcpStream::connect(local_addr);
    let accept_fut = listener.accept();

    let (local_client, (mut local_server, _)) = match tokio::try_join!(connect_fut, accept_fut) {
        Ok(res) => (res.0, res.1),
        Err(e) => {
            tracing::error!("[RS-0001] Failed to set up local relay streams: {e}; next_steps: retry the connection or inspect local socket limits");
            return;
        }
    };

    let pgwire_task = tokio::spawn(async move {
        let res =
            pgwire::tokio::process_socket(local_client, tls_acceptor_ref, factory_ref.clone())
                .await;
        (res, factory_ref)
    });

    if local_server.write_all(&full_msg).await.is_err() {
        return;
    }
    if local_server.flush().await.is_err() {
        return;
    }

    let _ = tokio::io::copy_bidirectional(&mut client_socket, &mut local_server).await;

    if let Ok((res, factory_ref)) = pgwire_task.await {
        if let Err(e) = res {
            tracing::debug!("gateway connection error: {e}");
        }
        crate::tls::remove_mtls_cn(&peer_addr);
        let cid = CONN_ID.with(|id| id.clone());
        factory_ref.handler.notify_registry.unsubscribe_all(&cid);
        factory_ref.handler.pending_notifies.remove(&cid);
        cleanup_connection_state(&factory_ref.handler, &cid);
    }
}

// ── GatewayServer ─────────────────────────────────────────────────────────────

/// A running PostgreSQL-wire-protocol server.
pub struct GatewayServer {
    addr: std::net::SocketAddr,
    handler: Arc<GatewayHandler>,
    /// v0.51.5: gateway-facing TLS acceptor. `None` (the default) preserves
    /// the pre-v0.51.5 plaintext-refusal `SSLRequest` behavior.
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
}

impl GatewayServer {
    fn from_handler(addr: std::net::SocketAddr, handler: GatewayHandler) -> Self {
        let handler = Arc::new(handler);
        GatewayServer {
            addr,
            handler,
            tls_acceptor: None,
        }
    }

    /// Attach the complete pinned shard-reader topology to a server created by
    /// any constructor, including the authentication-bearing constructors.
    /// Query-time execution keeps that topology intact rather than reverting to
    /// the local shard after authentication wrapping.
    pub fn with_query_time_shard_topology(mut self, topology: QueryTimeShardTopology) -> Self {
        if let Some(h) = Arc::get_mut(&mut self.handler) {
            h.query_time_shard_topology = Some(Arc::new(topology));
        }
        self
    }

    /// Attach a dynamic production topology provider. It refreshes all owning
    /// readers and validates one common frontier per query-time request.
    pub fn with_query_time_shard_topology_provider(
        mut self,
        provider: QueryTimeShardTopologyProvider,
    ) -> Self {
        if let Some(h) = Arc::get_mut(&mut self.handler) {
            h.query_time_shard_topology_provider = Some(Arc::new(provider));
        }
        self
    }

    /// Create a new gateway server listening on `addr`.
    pub fn new(addr: std::net::SocketAddr, view_reader: Arc<dyn ViewReader>) -> Self {
        let catalog = Arc::new(CatalogStubs::new());
        GatewayServer::from_handler(addr, GatewayHandler::new(catalog, view_reader))
    }

    /// Create a new gateway server with an explicit catalog (for testing).
    pub fn with_catalog(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
    ) -> Self {
        GatewayServer::from_handler(addr, GatewayHandler::new(catalog, view_reader))
    }

    /// Create a gateway server with a catalog and ShardDb for direct-write DML.
    pub fn with_shard_db(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        GatewayServer::from_handler(
            addr,
            GatewayHandler::with_shard_db(catalog, view_reader, shard_db),
        )
    }

    /// Create a shard-backed gateway whose query-time reads scatter across the
    /// supplied complete pinned topology.
    pub fn with_shard_db_and_query_time_shard_topology(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
        topology: QueryTimeShardTopology,
    ) -> Self {
        GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db)
            .with_query_time_shard_topology(topology)
    }

    /// Create a gateway with SCRAM-SHA-256 auth and a pre-populated RoleCatalog.
    pub fn with_scram_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        role_catalog: Arc<RoleCatalog>,
    ) -> Self {
        let mut handler = GatewayHandler::new(catalog, view_reader);
        handler.auth_mode = AuthMode::Scram;
        handler.role_catalog = role_catalog;
        GatewayServer::from_handler(addr, handler)
    }

    /// Create a gateway with MD5 auth and a pre-populated RoleCatalog.
    pub fn with_md5_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        role_catalog: Arc<RoleCatalog>,
    ) -> Self {
        let mut handler = GatewayHandler::new(catalog, view_reader);
        handler.auth_mode = AuthMode::Md5;
        handler.role_catalog = role_catalog;
        GatewayServer::from_handler(addr, handler)
    }

    /// Create a gateway with SCRAM-SHA-256 auth, ShardDb, and RoleCatalog.
    pub fn with_shard_db_and_scram_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
        role_catalog: Arc<RoleCatalog>,
    ) -> Self {
        let mut handler = GatewayHandler::with_shard_db(catalog, view_reader, shard_db);
        handler.auth_mode = AuthMode::Scram;
        handler.role_catalog = role_catalog;
        GatewayServer::from_handler(addr, handler)
    }

    /// Create a gateway with MD5 auth, ShardDb, and RoleCatalog.
    pub fn with_shard_db_and_md5_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
        role_catalog: Arc<RoleCatalog>,
    ) -> Self {
        let mut handler = GatewayHandler::with_shard_db(catalog, view_reader, shard_db);
        handler.auth_mode = AuthMode::Md5;
        handler.role_catalog = role_catalog;
        GatewayServer::from_handler(addr, handler)
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
        GatewayServer::from_handler(addr, handler)
    }

    /// v0.51.5: enable gateway-facing TLS termination (and, when
    /// `ca_cert_path` is `Some`, mTLS client-certificate authentication).
    /// Fails fast (returns `Err`, refuses to start) if `--auth=mtls` is
    /// configured on this server's handler but `ca_cert_path` is `None` —
    /// mTLS without TLS is a startup misconfiguration, not a runtime
    /// fallback.
    pub fn with_tls(
        mut self,
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
        ca_cert_path: Option<&std::path::Path>,
    ) -> Result<Self, crate::tls::GatewayTlsError> {
        crate::tls::require_ca_cert_for_mtls(
            self.handler.auth_mode == AuthMode::Mtls,
            ca_cert_path,
        )?;
        self.tls_acceptor = Some(crate::tls::build_tls_acceptor(
            cert_path,
            key_path,
            ca_cert_path,
        )?);
        Ok(self)
    }

    /// Create a gateway with mTLS auth enabled and TLS already configured
    /// (for auth integration tests exercising the mTLS startup branch
    /// without going through the CLI).
    pub fn with_shard_db_and_mtls_auth(
        addr: std::net::SocketAddr,
        catalog: Arc<CatalogStubs>,
        view_reader: Arc<dyn ViewReader>,
        shard_db: Arc<rockstream_storage::ShardDb>,
    ) -> Self {
        let mut handler = GatewayHandler::with_shard_db(catalog, view_reader, shard_db);
        handler.auth_mode = AuthMode::Mtls;
        GatewayServer::from_handler(addr, handler)
    }

    /// Return a reference to the handler (for seeding ACL and sessions in tests).
    pub fn handler(&self) -> &Arc<GatewayHandler> {
        &self.handler
    }
    pub fn catalog(&self) -> &Arc<CatalogStubs> {
        &self.handler.catalog
    }

    /// Bind the independent HTTP webhook listener.  It intentionally does not
    /// share the pgwire socket: only `POST /webhook/<source>` is accepted.
    pub async fn serve_webhook_background(
        &self,
        addr: std::net::SocketAddr,
    ) -> std::io::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let handler = self.handler.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let _ = serve_webhook_connection(socket, handler).await;
                });
            }
        });
        Ok((local_addr, task))
    }

    /// Start pgwire and webhook listeners together for the single Rockstream
    /// binary.  Both listeners share the same gateway handler and catalog.
    pub async fn serve_background_with_webhook(
        self,
        webhook_addr: std::net::SocketAddr,
    ) -> std::io::Result<(
        std::net::SocketAddr,
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    )> {
        let (webhook_addr, webhook_handle) = self.serve_webhook_background(webhook_addr).await?;
        let (pgwire_addr, pgwire_handle) = self.serve_background().await?;
        Ok((pgwire_addr, webhook_addr, pgwire_handle, webhook_handle))
    }

    /// Test and embedding hook for routing one already-authenticated webhook
    /// request through the same source lifecycle as the TCP HTTP listener.
    pub async fn accept_webhook(
        &self,
        source_name: &str,
        token: &[u8],
        delivery_id: Option<&str>,
        payload: &[u8],
    ) -> WebhookResult {
        self.handler
            .accept_webhook(source_name, token, delivery_id, payload)
            .await
    }

    /// Start listening.  Blocks until the future is dropped.
    pub async fn serve(self) -> std::io::Result<()> {
        self.handler.bind_server(&self.handler);
        self.handler.recover_compiled_views().await;
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler.clone(),
        });
        let registry = self.handler.cancellation_registry.clone();
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Gateway listening on {}", self.addr);
        loop {
            let (socket, peer) = listener.accept().await?;
            let factory_ref = factory.clone();
            let registry_ref = registry.clone();
            let tls_acceptor_ref = self.tls_acceptor.clone();
            use rand::Rng;
            let conn_id = format!("{:032x}", rand::thread_rng().gen::<u128>());
            let cancel_token = CancelToken::new();
            let token_for_task = cancel_token.clone();
            tokio::spawn(CANCEL_TOKEN.scope(
                token_for_task,
                CONN_ID.scope(
                    conn_id,
                    PEER_ADDR.scope(peer, async move {
                        let mut socket = socket;
                        let peer_addr = peer;
                        let mut buf = [0u8; 16];
                        if let Ok(n) = socket.peek(&mut buf).await {
                            // CancelRequest: [0,0,0,16, 4,210,22,46, pid(4), secret(4)]
                            if n >= 16 && buf[..8] == [0, 0, 0, 16, 4, 210, 22, 46] {
                                use tokio::io::AsyncReadExt;
                                let mut cancel_buf = [0u8; 16];
                                let _ = socket.read_exact(&mut cancel_buf).await;
                                let pid = u32::from_be_bytes([
                                    cancel_buf[8],
                                    cancel_buf[9],
                                    cancel_buf[10],
                                    cancel_buf[11],
                                ]);
                                let secret = u32::from_be_bytes([
                                    cancel_buf[12],
                                    cancel_buf[13],
                                    cancel_buf[14],
                                    cancel_buf[15],
                                ]);
                                if let Some(token) = registry_ref.get(&(pid, secret)) {
                                    token.cancel();
                                }
                                return; // CancelRequest connections don't do further work
                            }
                        }

                        let mut peek_buf = [0u8; 8];
                        if let Ok(8) = socket.peek(&mut peek_buf).await {
                            if tls_acceptor_ref.is_none()
                                && peek_buf != [0, 0, 0, 8, 4, 210, 22, 47]
                            {
                                let msg_len = u32::from_be_bytes([
                                    peek_buf[0],
                                    peek_buf[1],
                                    peek_buf[2],
                                    peek_buf[3],
                                ]) as usize;
                                let version = u32::from_be_bytes([
                                    peek_buf[4],
                                    peek_buf[5],
                                    peek_buf[6],
                                    peek_buf[7],
                                ]);
                                let major = (version >> 16) as u16;
                                let minor = (version & 0xffff) as u16;
                                if major == 3 && minor > 0 && (8..=100_000).contains(&msg_len) {
                                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                    let mut full_msg = vec![0u8; msg_len];
                                    if socket.read_exact(&mut full_msg).await.is_ok() {
                                        // Parse _pq_.* parameters for NegotiateProtocolVersion ('v')
                                        let mut pq_options = Vec::new();
                                        let mut idx = 8;
                                        while idx < full_msg.len() {
                                            let key_start = idx;
                                            while idx < full_msg.len() && full_msg[idx] != 0 {
                                                idx += 1;
                                            }
                                            if idx >= full_msg.len() {
                                                break;
                                            }
                                            let key =
                                                String::from_utf8_lossy(&full_msg[key_start..idx])
                                                    .to_string();
                                            idx += 1;
                                            if key.is_empty() {
                                                break;
                                            }
                                            while idx < full_msg.len() && full_msg[idx] != 0 {
                                                idx += 1;
                                            }
                                            if idx < full_msg.len() {
                                                idx += 1;
                                            }
                                            if key.starts_with("_pq_.") {
                                                pq_options.push(key);
                                            }
                                        }

                                        // Send NegotiateProtocolVersion ('v') specifying minor version 0
                                        let mut payload = Vec::new();
                                        payload.extend_from_slice(&0u32.to_be_bytes()); // minor ver 0
                                        payload.extend_from_slice(
                                            &(pq_options.len() as u32).to_be_bytes(),
                                        );
                                        for opt in &pq_options {
                                            payload.extend_from_slice(opt.as_bytes());
                                            payload.push(0);
                                        }
                                        let neg_len = (4 + payload.len()) as u32;
                                        let mut neg_msg = Vec::new();
                                        neg_msg.push(b'v');
                                        neg_msg.extend_from_slice(&neg_len.to_be_bytes());
                                        neg_msg.extend_from_slice(&payload);

                                        let _ = socket.write_all(&neg_msg).await;
                                        let _ = socket.flush().await;

                                        // Downgrade requested version in full_msg to 196608 (3.0)
                                        full_msg[4..8].copy_from_slice(&196608u32.to_be_bytes());

                                        relay_negotiated_3_2_connection(
                                            socket,
                                            full_msg,
                                            tls_acceptor_ref,
                                            factory_ref,
                                            peer_addr,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                            }
                        }

                        if let Err(e) = pgwire::tokio::process_socket(
                            socket,
                            tls_acceptor_ref.clone(),
                            factory_ref.clone(),
                        )
                        .await
                        {
                            tracing::debug!("gateway connection error: {e}");
                        }
                        crate::tls::remove_mtls_cn(&peer_addr);
                        // Cleanup on disconnect — runs unconditionally on
                        // BOTH graceful close and I/O error/EOF (abnormal,
                        // dropped-TCP disconnect), so this is the single
                        // correct hook for all per-connection state
                        // (v0.51.6 Slice 2). This must remove every
                        // per-connection map, not just LISTEN subscriptions,
                        // or a client that never sends DISCARD ALL/Terminate
                        // (e.g. a TCP-level kill) leaks prepared statements,
                        // portals, session state, write buffers, and COPY
                        // state forever.
                        let cid = CONN_ID.with(|id| id.clone());
                        factory_ref.handler.notify_registry.unsubscribe_all(&cid);
                        factory_ref.handler.pending_notifies.remove(&cid);
                        cleanup_connection_state(&factory_ref.handler, &cid);
                    }),
                ),
            ));
        }
    }

    /// Process a TCP stream through pgwire.
    pub async fn process_raw_socket(&self, socket: tokio::net::TcpStream) {
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler.clone(),
        });
        let _ = pgwire::tokio::process_socket(socket, self.tls_acceptor.clone(), factory).await;
    }

    /// Bind to `addr`, return the actual local address (useful for port 0 tests),
    /// and serve connections in a background task.
    pub async fn serve_background(
        self,
    ) -> std::io::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
        self.handler.bind_server(&self.handler);
        self.handler.recover_compiled_views().await;
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler.clone(),
        });
        let registry = self.handler.cancellation_registry.clone();
        let tls_acceptor = self.tls_acceptor.clone();
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, peer)) = listener.accept().await else {
                    break;
                };
                let factory_ref = factory.clone();
                let registry_ref = registry.clone();
                let tls_acceptor_ref = tls_acceptor.clone();
                use rand::Rng;
                let conn_id = format!("{:032x}", rand::thread_rng().gen::<u128>());
                let cancel_token = CancelToken::new();
                let token_for_task = cancel_token.clone();
                tokio::spawn(CANCEL_TOKEN.scope(
                    token_for_task,
                    CONN_ID.scope(
                        conn_id,
                        PEER_ADDR.scope(peer, async move {
                            let mut socket = socket;
                            let peer_addr = peer;
                            let mut buf = [0u8; 16];
                            if let Ok(n) = socket.peek(&mut buf).await {
                                // CancelRequest: [0,0,0,16, 4,210,22,46, pid(4), secret(4)]
                                if n >= 16 && buf[..8] == [0, 0, 0, 16, 4, 210, 22, 46] {
                                    use tokio::io::AsyncReadExt;
                                    let mut cancel_buf = [0u8; 16];
                                    let _ = socket.read_exact(&mut cancel_buf).await;
                                    let pid = u32::from_be_bytes([
                                        cancel_buf[8],
                                        cancel_buf[9],
                                        cancel_buf[10],
                                        cancel_buf[11],
                                    ]);
                                    let secret = u32::from_be_bytes([
                                        cancel_buf[12],
                                        cancel_buf[13],
                                        cancel_buf[14],
                                        cancel_buf[15],
                                    ]);
                                    if let Some(token) = registry_ref.get(&(pid, secret)) {
                                        token.cancel();
                                    }
                                    return; // CancelRequest connections don't do further work
                                }
                            }

                            let mut peek_buf = [0u8; 8];
                            if let Ok(8) = socket.peek(&mut peek_buf).await {
                                if tls_acceptor_ref.is_none()
                                    && peek_buf != [0, 0, 0, 8, 4, 210, 22, 47]
                                {
                                    let msg_len = u32::from_be_bytes([
                                        peek_buf[0],
                                        peek_buf[1],
                                        peek_buf[2],
                                        peek_buf[3],
                                    ]) as usize;
                                    let version = u32::from_be_bytes([
                                        peek_buf[4],
                                        peek_buf[5],
                                        peek_buf[6],
                                        peek_buf[7],
                                    ]);
                                    let major = (version >> 16) as u16;
                                    let minor = (version & 0xffff) as u16;
                                    if major == 3 && minor > 0 && (8..=100_000).contains(&msg_len) {
                                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                        let mut full_msg = vec![0u8; msg_len];
                                        if socket.read_exact(&mut full_msg).await.is_ok() {
                                            // Parse _pq_.* parameters for NegotiateProtocolVersion ('v')
                                            let mut pq_options = Vec::new();
                                            let mut idx = 8;
                                            while idx < full_msg.len() {
                                                let key_start = idx;
                                                while idx < full_msg.len() && full_msg[idx] != 0 {
                                                    idx += 1;
                                                }
                                                if idx >= full_msg.len() {
                                                    break;
                                                }
                                                let key = String::from_utf8_lossy(
                                                    &full_msg[key_start..idx],
                                                )
                                                .to_string();
                                                idx += 1;
                                                if key.is_empty() {
                                                    break;
                                                }
                                                while idx < full_msg.len() && full_msg[idx] != 0 {
                                                    idx += 1;
                                                }
                                                if idx < full_msg.len() {
                                                    idx += 1;
                                                }
                                                if key.starts_with("_pq_.") {
                                                    pq_options.push(key);
                                                }
                                            }

                                            // Send NegotiateProtocolVersion ('v') specifying minor version 0
                                            let mut payload = Vec::new();
                                            payload.extend_from_slice(&0u32.to_be_bytes()); // minor ver 0
                                            payload.extend_from_slice(
                                                &(pq_options.len() as u32).to_be_bytes(),
                                            );
                                            for opt in &pq_options {
                                                payload.extend_from_slice(opt.as_bytes());
                                                payload.push(0);
                                            }
                                            let neg_len = (4 + payload.len()) as u32;
                                            let mut neg_msg = Vec::new();
                                            neg_msg.push(b'v');
                                            neg_msg.extend_from_slice(&neg_len.to_be_bytes());
                                            neg_msg.extend_from_slice(&payload);

                                            let _ = socket.write_all(&neg_msg).await;
                                            let _ = socket.flush().await;

                                            // Downgrade requested version in full_msg to 196608 (3.0)
                                            full_msg[4..8]
                                                .copy_from_slice(&196608u32.to_be_bytes());

                                            relay_negotiated_3_2_connection(
                                                socket,
                                                full_msg,
                                                tls_acceptor_ref,
                                                factory_ref,
                                                peer_addr,
                                            )
                                            .await;
                                            return;
                                        }
                                    }
                                }
                            }

                            if let Err(e) = pgwire::tokio::process_socket(
                                socket,
                                tls_acceptor_ref.clone(),
                                factory_ref.clone(),
                            )
                            .await
                            {
                                tracing::debug!("gateway connection error: {e}");
                            }
                            crate::tls::remove_mtls_cn(&peer_addr);
                            // Cleanup on disconnect — runs unconditionally on
                            // BOTH graceful close and I/O error/EOF (abnormal,
                            // dropped-TCP disconnect); see `serve()` for the
                            // full rationale (v0.51.6 Slice 2).
                            let cid = CONN_ID.with(|id| id.clone());
                            factory_ref.handler.notify_registry.unsubscribe_all(&cid);
                            factory_ref.handler.pending_notifies.remove(&cid);
                            cleanup_connection_state(&factory_ref.handler, &cid);
                        }),
                    ),
                ));
            }
        });
        Ok((local_addr, handle))
    }
}

/// Remove every per-connection state map entry for `conn_id` (v0.51.6
/// Slice 2). Called unconditionally after `pgwire::tokio::process_socket`
/// returns in both `serve()` and `serve_background()` — on graceful close
/// **and** on abnormal disconnect (dropped TCP, I/O error/EOF) — so a
/// well-behaved-but-never-`DISCARD ALL`'d client that simply vanishes (e.g.
/// killed at the raw-socket level) does not leak state forever. This
/// complements `handle_discard_all`, which covers only the graceful
/// `DISCARD ALL`/`RESET ALL` path (since v0.37/v0.39).
fn cleanup_connection_state(handler: &GatewayHandler, conn_id: &str) {
    if let Some((_, stmts)) = handler.prepared_statements.remove(conn_id) {
        PREPARED_STATEMENTS_COUNT.fetch_sub(stmts.len() as u64, Ordering::Relaxed);
    }
    if let Some((_, portals)) = handler.active_portals.remove(conn_id) {
        PORTALS_COUNT.fetch_sub(portals.len() as u64, Ordering::Relaxed);
    }
    handler.portal_states.retain(|k, _| k.0 != conn_id);
    handler.sessions.remove(conn_id);
    handler.write_buffers.remove(conn_id);
    handler.copy_states.remove(conn_id);
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
                        .encode_field(&field.as_deref())
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

fn parse_duration_literal(raw: &str) -> Option<Duration> {
    let value = raw.trim().to_lowercase();
    if value.is_empty() || value == "0" || value == "none" {
        return None;
    }
    if let Some(ms) = value.strip_suffix("ms") {
        return ms.parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(sec) = value.strip_suffix('s') {
        return sec.parse::<u64>().ok().map(Duration::from_secs);
    }
    None
}

fn generate_uuid_v4_string() -> String {
    use rand::RngCore;

    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

#[derive(Debug, Clone, Default)]
struct AnalyzedSelectQuery {
    top_level_relation: Option<String>,
    top_level_relation_full: Option<String>,
    referenced_tables: Vec<String>,
    requires_query_time_datafusion: bool,
}

fn is_system_relation_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("pg_") || lower.starts_with("information_schema.")
}

fn resolve_catalog_relation_name(catalog: &CatalogStubs, raw_name: &str) -> Option<String> {
    let normalized = raw_name
        .trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(raw_name)
        .trim_matches('"')
        .to_string();
    if normalized.is_empty() {
        return None;
    }
    if catalog.get_table(&normalized).is_some() || catalog.get_view(&normalized).is_some() {
        return Some(normalized);
    }
    let lower = normalized.to_ascii_lowercase();
    if catalog.get_table(&lower).is_some() || catalog.get_view(&lower).is_some() {
        return Some(lower);
    }
    None
}

fn object_name_to_relation_name(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let full = name.to_string();
    let normalized = full
        .trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or(full.as_str())
        .trim_matches('"')
        .to_string();
    (full, normalized)
}

fn group_by_is_empty(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            exprs.is_empty() && modifiers.is_empty()
        }
    }
}

fn limit_clause_is_plain(limit_clause: &sqlparser::ast::LimitClause) -> bool {
    use sqlparser::ast::{Expr, LimitClause, Value};

    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            offset.is_none()
                && limit_by.is_empty()
                && limit.as_ref().is_none_or(|expr| {
                    matches!(
                        expr,
                        Expr::Value(v)
                            if matches!(v.value, Value::Number(_, false))
                    )
                })
        }
        LimitClause::OffsetCommaLimit { .. } => false,
    }
}

fn order_by_is_plain(order_by: &sqlparser::ast::OrderBy) -> bool {
    use sqlparser::ast::{Expr, OrderByKind};

    if order_by.interpolate.is_some() {
        return false;
    }
    match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs.iter().all(|expr| {
            expr.with_fill.is_none()
                && matches!(expr.expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        }),
        OrderByKind::All(_) => false,
    }
}

fn projection_item_is_plain(item: &sqlparser::ast::SelectItem) -> bool {
    use sqlparser::ast::{Expr, SelectItem, SelectItemQualifiedWildcardKind};

    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        | SelectItem::Wildcard(_) => true,
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(_), options) => {
            options.opt_exclude.is_none()
                && options.opt_except.is_none()
                && options.opt_rename.is_none()
                && options.opt_replace.is_none()
        }
        _ => false,
    }
}

fn expr_is_after_fence_predicate(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::{Expr, FunctionArguments};

    match expr {
        Expr::Nested(inner) => expr_is_after_fence_predicate(inner),
        Expr::Function(function) => {
            function
                .name
                .to_string()
                .eq_ignore_ascii_case("rockstream.after_fence")
                && matches!(&function.args, FunctionArguments::List(arg_list) if arg_list.args.len() == 1)
        }
        _ => false,
    }
}

fn query_has_non_projection_features(query: &sqlparser::ast::Query) -> bool {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || query
            .order_by
            .as_ref()
            .is_some_and(|order_by| !order_by_is_plain(order_by))
        || query
            .limit_clause
            .as_ref()
            .is_some_and(|limit_clause| !limit_clause_is_plain(limit_clause))
    {
        return true;
    }

    match &*query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            select.distinct.is_some()
                || select
                    .select_modifiers
                    .as_ref()
                    .is_some_and(|mods| mods.is_any_set())
                || select.top.is_some()
                || select.exclude.is_some()
                || select.into.is_some()
                || select.from.len() != 1
                || select.from.iter().any(|table| !table.joins.is_empty())
                || !select.lateral_views.is_empty()
                || select.prewhere.is_some()
                || select
                    .selection
                    .as_ref()
                    .is_some_and(|expr| !expr_is_after_fence_predicate(expr))
                || !select.connect_by.is_empty()
                || !group_by_is_empty(&select.group_by)
                || !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty()
                || select.having.is_some()
                || !select.named_window.is_empty()
                || select.qualify.is_some()
                || select.value_table_mode.is_some()
                || !matches!(select.flavor, sqlparser::ast::SelectFlavor::Standard)
                || select
                    .projection
                    .iter()
                    .any(|item| !projection_item_is_plain(item))
        }
        _ => true,
    }
}

fn collect_query_relations(
    query: &sqlparser::ast::Query,
    relations: &mut Vec<String>,
    top_level_relation: &mut Option<String>,
    top_level_relation_full: &mut Option<String>,
    is_top_level: bool,
) {
    use sqlparser::ast::{
        FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, TableFactor,
    };

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_relations(
                &cte.query,
                relations,
                top_level_relation,
                top_level_relation_full,
                false,
            );
        }
    }

    fn visit_expr(
        expr: &sqlparser::ast::Expr,
        relations: &mut Vec<String>,
        top_level_relation: &mut Option<String>,
        top_level_relation_full: &mut Option<String>,
    ) {
        use sqlparser::ast::Expr;

        match expr {
            Expr::BinaryOp { left, right, .. } => {
                visit_expr(left, relations, top_level_relation, top_level_relation_full);
                visit_expr(
                    right,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                );
            }
            Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
                visit_expr(expr, relations, top_level_relation, top_level_relation_full);
            }
            Expr::Function(function) => {
                if let FunctionArguments::List(arg_list) = &function.args {
                    for arg in &arg_list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                            | FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(expr),
                                ..
                            } => {
                                visit_expr(
                                    expr,
                                    relations,
                                    top_level_relation,
                                    top_level_relation_full,
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                visit_expr(expr, relations, top_level_relation, top_level_relation_full);
                for item in list {
                    visit_expr(item, relations, top_level_relation, top_level_relation_full);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                visit_expr(expr, relations, top_level_relation, top_level_relation_full);
                visit_expr(low, relations, top_level_relation, top_level_relation_full);
                visit_expr(high, relations, top_level_relation, top_level_relation_full);
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    visit_expr(
                        operand,
                        relations,
                        top_level_relation,
                        top_level_relation_full,
                    );
                }
                for condition in conditions {
                    visit_expr(
                        &condition.condition,
                        relations,
                        top_level_relation,
                        top_level_relation_full,
                    );
                    visit_expr(
                        &condition.result,
                        relations,
                        top_level_relation,
                        top_level_relation_full,
                    );
                }
                if let Some(else_result) = else_result {
                    visit_expr(
                        else_result,
                        relations,
                        top_level_relation,
                        top_level_relation_full,
                    );
                }
            }
            Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
                collect_query_relations(
                    subquery,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                    false,
                );
            }
            Expr::InSubquery { expr, subquery, .. } => {
                visit_expr(expr, relations, top_level_relation, top_level_relation_full);
                collect_query_relations(
                    subquery,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                    false,
                );
            }
            _ => {}
        }
    }

    fn visit_table_factor(
        relation: &TableFactor,
        relations: &mut Vec<String>,
        top_level_relation: &mut Option<String>,
        top_level_relation_full: &mut Option<String>,
        capture_top_level: bool,
    ) {
        match relation {
            TableFactor::Table { name, .. } => {
                let (full, normalized) = object_name_to_relation_name(name);
                if capture_top_level && top_level_relation.is_none() {
                    *top_level_relation = Some(normalized.clone());
                    *top_level_relation_full = Some(full);
                }
                relations.push(normalized);
            }
            TableFactor::Derived { subquery, .. } => {
                collect_query_relations(
                    subquery,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                    false,
                );
            }
            _ => {}
        }
    }

    match &*query.body {
        SetExpr::Select(select) => {
            for projection in &select.projection {
                match projection {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        visit_expr(expr, relations, top_level_relation, top_level_relation_full);
                    }
                    SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
                }
            }
            for (idx, table) in select.from.iter().enumerate() {
                visit_table_factor(
                    &table.relation,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                    is_top_level && idx == 0,
                );
                for join in &table.joins {
                    visit_table_factor(
                        &join.relation,
                        relations,
                        top_level_relation,
                        top_level_relation_full,
                        false,
                    );
                }
            }
            if let Some(selection) = &select.selection {
                visit_expr(
                    selection,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                );
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for expr in exprs {
                    visit_expr(expr, relations, top_level_relation, top_level_relation_full);
                }
            }
            if let Some(having) = &select.having {
                visit_expr(
                    having,
                    relations,
                    top_level_relation,
                    top_level_relation_full,
                );
            }
        }
        SetExpr::Query(subquery) => collect_query_relations(
            subquery,
            relations,
            top_level_relation,
            top_level_relation_full,
            false,
        ),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_relations(
                left,
                relations,
                top_level_relation,
                top_level_relation_full,
            );
            collect_set_expr_relations(
                right,
                relations,
                top_level_relation,
                top_level_relation_full,
            );
        }
        _ => {}
    }
}

fn collect_set_expr_relations(
    set_expr: &sqlparser::ast::SetExpr,
    relations: &mut Vec<String>,
    top_level_relation: &mut Option<String>,
    top_level_relation_full: &mut Option<String>,
) {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => {
            let query = sqlparser::ast::Query {
                with: None,
                body: Box::new(sqlparser::ast::SetExpr::Select(select.clone())),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            };
            collect_query_relations(
                &query,
                relations,
                top_level_relation,
                top_level_relation_full,
                false,
            );
        }
        sqlparser::ast::SetExpr::Query(query) => collect_query_relations(
            query,
            relations,
            top_level_relation,
            top_level_relation_full,
            false,
        ),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_relations(
                left,
                relations,
                top_level_relation,
                top_level_relation_full,
            );
            collect_set_expr_relations(
                right,
                relations,
                top_level_relation,
                top_level_relation_full,
            );
        }
        _ => {}
    }
}

fn analyze_select_query(catalog: &CatalogStubs, q: &str) -> Option<AnalyzedSelectQuery> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmt = sqlparser::parser::Parser::parse_sql(&dialect, q)
        .ok()?
        .into_iter()
        .next()?;
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return None;
    };

    let mut top_level_relation = None;
    let mut top_level_relation_full = None;
    let mut raw_relations = Vec::new();
    collect_query_relations(
        &query,
        &mut raw_relations,
        &mut top_level_relation,
        &mut top_level_relation_full,
        true,
    );

    let mut seen = HashSet::new();
    let referenced_tables = raw_relations
        .into_iter()
        .filter_map(|raw_name| resolve_catalog_relation_name(catalog, &raw_name))
        .filter(|name| seen.insert(name.clone()))
        .collect();

    Some(AnalyzedSelectQuery {
        top_level_relation,
        top_level_relation_full,
        referenced_tables,
        requires_query_time_datafusion: query_has_non_projection_features(&query),
    })
}

fn backfill_not_published_response(view_name: &str) -> Vec<Response<'static>> {
    vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "55000".to_owned(),
        format!(
            "[RS-4022] backfill.not_published: materialized view '{}' is not published yet. Next steps: run SHOW BACKFILL STATUS FOR MATERIALIZED VIEW {} and retry when phase is RUNNING.",
            view_name, view_name
        ),
    ))))]
}

fn datafusion_batches_to_query_response(batches: &[RecordBatch]) -> Vec<Response<'static>> {
    use datafusion::arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        StringArray,
    };
    use datafusion::arrow::datatypes::DataType as ArrowDataType;

    if batches.is_empty() {
        let schema = Arc::new(Vec::<FieldInfo>::new());
        let schema_ref = schema.clone();
        let data_stream = stream::iter(Vec::<Vec<Option<String>>>::new()).map(move |row_vals| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for (col_idx, val) in row_vals.iter().enumerate() {
                let datatype = schema_ref[col_idx].datatype();
                encode_typed_field(&mut encoder, datatype, val.as_deref())?;
            }
            encoder.finish()
        });
        return vec![Response::Query(QueryResponse::new(schema, data_stream))];
    }

    let arrow_schema = batches[0].schema();
    let schema_fields: Vec<FieldInfo> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let pg_type = match f.data_type() {
                ArrowDataType::Int16 => Type::INT2,
                ArrowDataType::Int32 => Type::INT4,
                ArrowDataType::Int64 => Type::INT8,
                ArrowDataType::Float32 => Type::FLOAT4,
                ArrowDataType::Float64 => Type::FLOAT8,
                ArrowDataType::Boolean => Type::BOOL,
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::TEXT,
                _ => Type::TEXT,
            };
            let format = PORTAL_FORMAT
                .try_with(|fmt| fmt.format_for(idx))
                .unwrap_or(FieldFormat::Text);
            FieldInfo::new(f.name().clone(), None, None, pg_type, format)
        })
        .collect();

    let schema = Arc::new(schema_fields);
    let mut encoded_rows: Vec<Vec<Option<String>>> = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_vals: Vec<Option<String>> = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row_vals.push(None);
                    continue;
                }
                let val = match col.data_type() {
                    ArrowDataType::Int16 => col
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .map(|a| a.value(row_idx).to_string()),
                    ArrowDataType::Int32 => col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .map(|a| a.value(row_idx).to_string()),
                    ArrowDataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.value(row_idx).to_string()),
                    ArrowDataType::Float32 => col
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .map(|a| a.value(row_idx).to_string()),
                    ArrowDataType::Float64 => col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|a| a.value(row_idx).to_string()),
                    ArrowDataType::Boolean => {
                        col.as_any().downcast_ref::<BooleanArray>().map(|a| {
                            if a.value(row_idx) {
                                "t".to_string()
                            } else {
                                "f".to_string()
                            }
                        })
                    }
                    ArrowDataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value(row_idx).to_string()),
                    _ => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value(row_idx).to_string()),
                };
                row_vals.push(val);
            }
            encoded_rows.push(row_vals);
        }
    }

    let schema_ref = schema.clone();
    let data_stream = stream::iter(encoded_rows).map(move |row_vals| {
        let mut encoder = DataRowEncoder::new(schema_ref.clone());
        for (col_idx, val) in row_vals.iter().enumerate() {
            let datatype = schema_ref[col_idx].datatype();
            encode_typed_field(&mut encoder, datatype, val.as_deref())?;
        }
        encoder.finish()
    });

    vec![Response::Query(QueryResponse::new(schema, data_stream))]
}

fn query_time_relation_schema(catalog: &CatalogStubs, relation_name: &str) -> SchemaRef {
    if let Some(table) = catalog.get_table(relation_name) {
        let fields: Vec<Field> = table
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name,
                    string_to_arrow_datatype(&column.data_type),
                    true,
                )
            })
            .collect();
        return Arc::new(Schema::new(fields));
    }
    if let Some(view) = catalog.get_view(relation_name) {
        if !view.columns.is_empty() {
            let fields: Vec<Field> = view
                .columns
                .iter()
                .map(|column| {
                    Field::new(
                        &column.name,
                        string_to_arrow_datatype(&column.data_type),
                        true,
                    )
                })
                .collect();
            return Arc::new(Schema::new(fields));
        }
    }
    Arc::new(Schema::new(vec![Field::new(
        "_value",
        datafusion::arrow::datatypes::DataType::Utf8,
        true,
    )]))
}

fn full_row_pk(column_count: usize) -> Vec<usize> {
    (0..column_count).collect()
}

/// v0.51.4 Slice 0: build a signed-weight `ArrowZSet` from the commit's own
/// row-level `DmlOp`s for a single source table — the true delta fed into a
/// compiled view's pipeline, replacing the retired full-table rescan.
///
/// Weights: `+1` per inserted row, `-1` per deleted row (using the delete's
/// captured pre-image — see `DmlOp::Delete::returning_tsv`, now always
/// captured, v0.51.4 Slice 0), and a paired `-1`/`+1` for each update (old
/// row image retracted, new row image asserted). Ops for other tables are
/// ignored. Table name matching is case-insensitive, matching
/// `WriteBuffer::current_row_image`'s convention.
/// Parse a list of tab-separated rows into an Arrow `RecordBatch`.
///
/// Values are cast from string to the declared column type. Rows that
/// cannot be parsed for a column fall back to `null`. (Moved here from the
/// now-deleted `view_materializer.rs` in v0.51.4 Slice 8 — this is a
/// generic TSV-storage-format helper used by the query-time read paths
/// below, independent of that module's retired DataFusion-materializer
/// logic.)
fn tsv_to_record_batch(schema: SchemaRef, rows: &[Vec<u8>]) -> Result<RecordBatch, String> {
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let n = rows.len();
    let num_cols = schema.fields().len();

    let mut col_strs: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(n); num_cols];
    for row in rows {
        let s = String::from_utf8_lossy(row);
        let fields: Vec<&str> = s.split('\t').collect();
        for (i, col) in col_strs.iter_mut().enumerate() {
            col.push(
                fields
                    .get(i)
                    .filter(|value| **value != r"\N")
                    .map(|value| (*value).to_string()),
            );
        }
    }

    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| match field.data_type() {
            DataType::Int32 => {
                let vals: Vec<Option<i32>> = col_strs[i]
                    .iter()
                    .map(|s| s.as_deref().and_then(|v| v.parse().ok()))
                    .collect();
                Arc::new(Int32Array::from(vals)) as ArrayRef
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = col_strs[i]
                    .iter()
                    .map(|s| s.as_deref().and_then(|v| v.parse().ok()))
                    .collect();
                Arc::new(Int64Array::from(vals)) as ArrayRef
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = col_strs[i]
                    .iter()
                    .map(|s| s.as_deref().and_then(|v| v.parse().ok()))
                    .collect();
                Arc::new(Float64Array::from(vals)) as ArrayRef
            }
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = col_strs[i]
                    .iter()
                    .map(|s| {
                        s.as_deref()
                            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "t" | "1"))
                    })
                    .collect();
                Arc::new(BooleanArray::from(vals)) as ArrayRef
            }
            _ => {
                let vals: Vec<Option<String>> = col_strs[i]
                    .iter()
                    .map(|s| s.as_ref().map(|v| v.to_string()))
                    .collect();
                Arc::new(StringArray::from(vals)) as ArrayRef
            }
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

fn build_delta_zset_for_table(
    table: &str,
    ops: &[DmlOp],
    schema: SchemaRef,
) -> Result<ArrowZSet, GatewayError> {
    let mut tsv_rows: Vec<Vec<u8>> = Vec::new();
    let mut weights: Vec<i64> = Vec::new();
    for op in ops {
        match op {
            DmlOp::Insert {
                table: op_table,
                values_tsv,
                ..
            } if op_table.eq_ignore_ascii_case(table) => {
                tsv_rows.push(values_tsv.clone().into_bytes());
                weights.push(1);
            }
            DmlOp::Update {
                table: op_table,
                old_tsv,
                new_tsv,
                ..
            } if op_table.eq_ignore_ascii_case(table) => {
                tsv_rows.push(old_tsv.clone().into_bytes());
                weights.push(-1);
                tsv_rows.push(new_tsv.clone().into_bytes());
                weights.push(1);
            }
            DmlOp::Delete {
                table: op_table,
                returning_tsv: Some(tsv),
                ..
            } if op_table.eq_ignore_ascii_case(table) => {
                tsv_rows.push(tsv.clone().into_bytes());
                weights.push(-1);
            }
            // A delete with no captured pre-image (e.g. the row never
            // existed) contributes no delta row — nothing to retract.
            DmlOp::Delete { .. } => {}
            _ => {}
        }
    }
    if tsv_rows.is_empty() {
        return Ok(ArrowZSet::empty(schema));
    }
    let batch = tsv_to_record_batch(schema.clone(), &tsv_rows).map_err(|e| {
        GatewayError::QueryTimeExecutionFailed {
            detail: format!("build_delta_zset_for_table({table}): {e}"),
        }
    })?;
    Ok(ArrowZSet::new(batch, weights))
}

/// Append logical table mutations to an existing M3 write. This is shared by
/// client COMMIT and source-backed ingestion so they use identical row keys.
fn append_dml_ops(batch: &mut rockstream_storage::WriteBatch, ops: &[DmlOp]) {
    for op in ops {
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
            DmlOp::Delete { table, row_key, .. } => {
                let key = format!("view_output/{table}/{row_key}");
                batch.delete(key.as_bytes());
            }
        }
    }
}

fn source_view_connector_id(source_name: &str, view_name: &str) -> ConnectorId {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in source_name.bytes().chain([0]).chain(view_name.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    ConnectorId(hash)
}

fn source_checkpoint_connector_id(source: &CatalogSourceEntry, view_name: &str) -> ConnectorId {
    if source.source_type == "postgres_cdc" && source.format == "pgoutput" {
        if let Ok(identity) = pgoutput_source_identity(source) {
            return identity.connector_id();
        }
    }
    source_view_connector_id(&source.name, view_name)
}

fn pgoutput_source_identity(source: &CatalogSourceEntry) -> Result<SourceIdentityV1, GatewayError> {
    let port = source.options.get("port").map_or(Ok(None), |port| {
        port.parse::<u16>()
            .map(Some)
            .map_err(|_| GatewayError::QueryTimeExecutionFailed {
                detail: format!(
                    "PostgreSQL CDC source '{}' has invalid port '{port}'",
                    source.name
                ),
            })
    })?;
    let required =
        |name: &str| {
            source.options.get(name).cloned().ok_or_else(|| {
                GatewayError::QueryTimeExecutionFailed {
                    detail: format!("PostgreSQL CDC source '{}' requires {name}", source.name),
                }
            })
        };
    SourceIdentityV1::new(
        source
            .options
            .get("host")
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        port,
        source
            .options
            .get("database")
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        required("slot")?,
        required("publication")?,
        source
            .options
            .get("user")
            .cloned()
            .unwrap_or_else(|| "postgres".to_string()),
        required("credential_ref")?,
    )
}

fn catalog_type_accepts_pg_oid(data_type: &str, oid: u32) -> bool {
    match data_type.to_ascii_lowercase().as_str() {
        "int32" | "int" | "integer" => oid == 23,
        "int64" | "bigint" => oid == 20 || oid == 23,
        "utf8" | "text" | "varchar" => matches!(oid, 25 | 1043),
        data_type if data_type.starts_with("decimal") || data_type.starts_with("numeric") => {
            oid == 1700
        }
        _ => false,
    }
}

fn pg_oid_catalog_type(oid: u32) -> &'static str {
    match oid {
        20 => "Int64",
        23 => "Int32",
        25 | 1043 => "Utf8",
        1700 => "Decimal128(38, 10)",
        _ => "Utf8",
    }
}

fn relation_route_schema(route: &RelationRoute) -> SchemaRef {
    Arc::new(Schema::new(
        route
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.imported_name,
                    string_to_arrow_datatype(pg_oid_catalog_type(column.type_oid)),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn pgoutput_change_to_dml(
    route: &RelationRoute,
    change: &EncodedChange,
) -> Result<DmlOp, GatewayError> {
    let names = route
        .columns
        .iter()
        .map(|column| column.imported_name.clone())
        .collect::<Vec<_>>();
    let row = |values: &Option<Vec<Option<String>>>, kind: &str| {
        let values = values
            .as_ref()
            .ok_or_else(|| GatewayError::QueryTimeExecutionFailed {
                detail: format!("pgoutput {kind} is missing its row image"),
            })?;
        if values.len() != names.len() {
            return Err(GatewayError::QueryTimeExecutionFailed {
                detail: format!(
                    "RS-1002: pgoutput {kind} tuple has {} columns, route has {}",
                    values.len(),
                    names.len()
                ),
            });
        }
        let values = values
            .iter()
            .map(|value| value.clone().unwrap_or_else(|| r"\N".to_string()))
            .collect::<Vec<_>>();
        Ok((values.join("\t"), build_row_key(&names, &values)))
    };
    match change.operation {
        CdcOperation::Insert => {
            let (values_tsv, row_key) = row(&change.new_values, "INSERT")?;
            Ok(DmlOp::Insert {
                table: route.imported_table_name.clone(),
                cols: names,
                values_tsv,
                row_key,
            })
        }
        CdcOperation::Update => {
            let (old_tsv, old_row_key) = row(&change.old_values, "UPDATE old")?;
            let (new_tsv, new_row_key) = row(&change.new_values, "UPDATE new")?;
            Ok(DmlOp::Update {
                table: route.imported_table_name.clone(),
                old_row_key,
                old_tsv,
                new_row_key,
                new_tsv,
            })
        }
        CdcOperation::Delete => {
            let (returning_tsv, row_key) = row(&change.old_values, "DELETE")?;
            Ok(DmlOp::Delete {
                table: route.imported_table_name.clone(),
                row_key,
                returning_tsv: Some(returning_tsv),
            })
        }
    }
}

fn dml_table_name(op: &DmlOp) -> &str {
    match op {
        DmlOp::Insert { table, .. } | DmlOp::Update { table, .. } | DmlOp::Delete { table, .. } => {
            table
        }
    }
}

fn source_backfill_error(error: rockstream_connectors::SourceError) -> GatewayError {
    GatewayError::QueryTimeExecutionFailed {
        detail: format!("source-backed backfill: {error}"),
    }
}

/// Convert a weighted connector batch to the same DML shape as pgwire writes.
/// Connector deletes retain their pre-image so downstream views can retract
/// them in the M3 transaction that advances the source cursor.
fn source_batch_to_dml_ops(
    table: &str,
    columns: &[CatalogColumn],
    batch: &RecordBatch,
) -> Result<Vec<DmlOp>, String> {
    use datafusion::arrow::util::display::array_value_to_string;

    let (data, weights) = rockstream_types::arrow_batch::split_weight_column(batch)
        .unwrap_or_else(|| (batch.clone(), vec![1; batch.num_rows()]));
    if data.num_columns() != columns.len() {
        return Err(format!(
            "source batch has {} column(s), but table '{table}' has {}",
            data.num_columns(),
            columns.len()
        ));
    }
    let names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut ops = Vec::with_capacity(data.num_rows());
    for (row, &weight) in weights.iter().enumerate().take(data.num_rows()) {
        let values = data
            .columns()
            .iter()
            .map(|column| {
                if column.is_null(row) {
                    Ok(r"\N".to_string())
                } else {
                    array_value_to_string(column.as_ref(), row).map_err(|error| error.to_string())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values_tsv = values.join("\t");
        let row_key = build_row_key(&names, &values);
        match weight {
            weight if weight > 0 => ops.push(DmlOp::Insert {
                table: table.to_string(),
                cols: names.clone(),
                values_tsv,
                row_key,
            }),
            weight if weight < 0 => ops.push(DmlOp::Delete {
                table: table.to_string(),
                row_key,
                returning_tsv: Some(values_tsv),
            }),
            _ => {}
        }
    }
    Ok(ops)
}

async fn query_time_datafusion_select(
    catalog: &CatalogStubs,
    topology: &QueryTimeShardTopology,
    raw_sql: &str,
    referenced_tables: &[String],
) -> Result<Vec<RecordBatch>, GatewayError> {
    let ctx = SessionContext::new();
    rockstream_sql::frontend::register_session_sql_udf(&ctx);

    for relation_name in referenced_tables {
        let schema = query_time_relation_schema(catalog, relation_name);
        let partition = QueryTimeScatterPartition {
            schema: schema.clone(),
            relation: relation_name.clone(),
            readers: topology.readers.clone(),
            metrics: Arc::new(QueryTimeScatterMetrics::new(
                topology.query_time_scatter_budget,
            )),
        };
        let table = StreamingTable::try_new(schema, vec![Arc::new(partition)]).map_err(|e| {
            GatewayError::QueryTimeExecutionFailed {
                detail: format!("StreamingTable({relation_name}): {e}"),
            }
        })?;
        ctx.register_table(relation_name.as_str(), Arc::new(table))
            .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                detail: format!("register({relation_name}): {e}"),
            })?;
    }

    let df = ctx
        .sql(raw_sql)
        .await
        .map_err(|e| GatewayError::QueryTimeExecutionFailed {
            detail: format!("sql: {e}"),
        })?;
    let output_schema = Arc::new(df.schema().as_arrow().clone());
    let mut batches = df.collect().await.map_err(|e| {
        query_time_datafusion_error(e, referenced_tables, topology.query_time_scatter_budget)
    })?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(output_schema));
    }
    Ok(batches)
}

#[derive(Debug, Default)]
struct QueryTimeScatterMetrics {
    rows_in_flight: AtomicUsize,
    bytes_in_flight: AtomicUsize,
    batches_in_flight: AtomicUsize,
    total_rows: AtomicUsize,
    total_bytes: AtomicUsize,
    budget: QueryTimeScatterBudget,
}

impl QueryTimeScatterMetrics {
    fn new(budget: QueryTimeScatterBudget) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    fn reserve(&self, rows: usize, bytes: usize) -> bool {
        let previous_rows = self.total_rows.fetch_add(rows, Ordering::Relaxed);
        let previous_bytes = self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        if previous_rows.saturating_add(rows) > self.budget.row_limit
            || previous_bytes.saturating_add(bytes) > self.budget.byte_limit
        {
            self.total_rows.fetch_sub(rows, Ordering::Relaxed);
            self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        self.rows_in_flight.fetch_add(rows, Ordering::Relaxed);
        self.bytes_in_flight.fetch_add(bytes, Ordering::Relaxed);
        self.batches_in_flight.fetch_add(1, Ordering::Relaxed);
        QUERY_TIME_SCATTER_ROWS_IN_FLIGHT.fetch_add(rows, Ordering::Relaxed);
        QUERY_TIME_SCATTER_BYTES_IN_FLIGHT.fetch_add(bytes, Ordering::Relaxed);
        QUERY_TIME_SCATTER_BATCHES_IN_FLIGHT.fetch_add(1, Ordering::Relaxed);
        update_scatter_peak(
            &QUERY_TIME_SCATTER_PEAK_ROWS_IN_FLIGHT,
            QUERY_TIME_SCATTER_ROWS_IN_FLIGHT.load(Ordering::Relaxed),
        );
        update_scatter_peak(
            &QUERY_TIME_SCATTER_PEAK_BYTES_IN_FLIGHT,
            QUERY_TIME_SCATTER_BYTES_IN_FLIGHT.load(Ordering::Relaxed),
        );
        update_scatter_peak(
            &QUERY_TIME_SCATTER_PEAK_BATCHES_IN_FLIGHT,
            QUERY_TIME_SCATTER_BATCHES_IN_FLIGHT.load(Ordering::Relaxed),
        );
        true
    }

    fn release(&self, rows: usize, bytes: usize) {
        if rows != 0 || bytes != 0 {
            self.rows_in_flight.fetch_sub(rows, Ordering::Relaxed);
            self.bytes_in_flight.fetch_sub(bytes, Ordering::Relaxed);
            self.batches_in_flight.fetch_sub(1, Ordering::Relaxed);
            QUERY_TIME_SCATTER_ROWS_IN_FLIGHT.fetch_sub(rows, Ordering::Relaxed);
            QUERY_TIME_SCATTER_BYTES_IN_FLIGHT.fetch_sub(bytes, Ordering::Relaxed);
            QUERY_TIME_SCATTER_BATCHES_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn update_scatter_peak(peak: &AtomicUsize, value: usize) {
    let mut observed = peak.load(Ordering::Relaxed);
    while value > observed {
        match peak.compare_exchange_weak(observed, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => observed = actual,
        }
    }
}

struct QueryTimeScatterPartition {
    schema: SchemaRef,
    relation: String,
    readers: Vec<Arc<rockstream_storage::ShardReader>>,
    metrics: Arc<QueryTimeScatterMetrics>,
}

impl Debug for QueryTimeScatterPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryTimeScatterPartition")
            .field("relation", &self.relation)
            .field("reader_count", &self.readers.len())
            .finish_non_exhaustive()
    }
}

// Complex state holding readers, schema, and futures stream.
#[allow(clippy::type_complexity)]
struct QueryTimeScatterStreamState {
    schema: SchemaRef,
    relation: String,
    readers: Vec<Arc<rockstream_storage::ShardReader>>,
    prefix: Vec<u8>,
    next_reader: usize,
    receiver: Option<
        tokio::sync::mpsc::Receiver<Result<Vec<(Bytes, Bytes)>, rockstream_storage::StorageError>>,
    >,
    metrics: Arc<QueryTimeScatterMetrics>,
    current_rows: usize,
    current_bytes: usize,
    batch_permit: Option<tokio::sync::SemaphorePermit<'static>>,
}

impl PartitionStream for QueryTimeScatterPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let state = QueryTimeScatterStreamState {
            schema: self.schema.clone(),
            relation: self.relation.clone(),
            readers: self.readers.clone(),
            prefix: format!("view_output/{}/", self.relation).into_bytes(),
            next_reader: 0,
            receiver: None,
            metrics: self.metrics.clone(),
            current_rows: 0,
            current_bytes: 0,
            batch_permit: None,
        };
        let stream = futures::stream::unfold(state, |mut state| async move {
            state
                .metrics
                .release(state.current_rows, state.current_bytes);
            state.batch_permit.take();
            state.current_rows = 0;
            state.current_bytes = 0;
            loop {
                if state.receiver.is_none() {
                    let reader = state.readers.get(state.next_reader).cloned()?;
                    state.next_reader += 1;
                    let (sender, receiver) =
                        tokio::sync::mpsc::channel(QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES);
                    let prefix = state.prefix.clone();
                    tokio::spawn(async move {
                        reader
                            .scan_prefix_pages(
                                &prefix,
                                QUERY_TIME_SCATTER_MAX_IN_FLIGHT_ROWS,
                                QUERY_TIME_SCATTER_MAX_IN_FLIGHT_BYTES,
                                sender,
                            )
                            .await;
                    });
                    state.receiver = Some(receiver);
                }
                let Some(receiver) = state.receiver.as_mut() else {
                    break;
                };
                if state.batch_permit.is_none() {
                    let Ok(permit) = QUERY_TIME_SCATTER_BATCH_PERMITS.acquire().await else {
                        break;
                    };
                    state.batch_permit = Some(permit);
                }

                match receiver.recv().await {
                    Some(Ok(page)) => {
                        let rows = page.len();
                        let bytes = page
                            .iter()
                            .map(|(key, value)| key.len() + value.len())
                            .sum();
                        if !state.metrics.reserve(rows, bytes) {
                            return Some((
                                Err(DataFusionError::Execution(format!(
                                    "[RS-2029] query-time scatter budget exceeded while scanning '{}'",
                                    state.relation
                                ))),
                                state,
                            ));
                        }
                        let tsv_rows: Vec<Vec<u8>> =
                            page.into_iter().map(|(_, value)| value.to_vec()).collect();
                        let batch = match tsv_to_record_batch(state.schema.clone(), &tsv_rows) {
                            Ok(batch) => batch,
                            Err(error) => {
                                state.metrics.release(rows, bytes);
                                return Some((
                                    Err(DataFusionError::Execution(format!(
                                        "query-time scatter could not decode relation '{}': {error}",
                                        state.relation
                                    ))),
                                    state,
                                ));
                            }
                        };
                        QUERY_TIME_DATAFUSION_ROWS_SCANNED_TOTAL
                            .fetch_add(rows as u64, Ordering::Relaxed);
                        state.current_rows = rows;
                        state.current_bytes = bytes;
                        return Some((Ok(batch), state));
                    }
                    Some(Err(error)) => {
                        return Some((Err(DataFusionError::Execution(error.to_string())), state));
                    }
                    None => state.receiver = None,
                }
            }
            None
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

fn query_time_datafusion_error(
    error: DataFusionError,
    referenced_tables: &[String],
    budget: QueryTimeScatterBudget,
) -> GatewayError {
    if error.to_string().contains("[RS-2029]") {
        return GatewayError::QueryTimeScatterBudgetExceeded {
            relation: referenced_tables.first().cloned().unwrap_or_default(),
            row_limit: budget.row_limit,
            byte_limit: budget.byte_limit,
        };
    }
    GatewayError::QueryTimeExecutionFailed {
        detail: format!("collect: {error}"),
    }
}

fn query_time_error_response(error: GatewayError) -> Vec<Response<'static>> {
    vec![promote_response(Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        crate::error::sqlstate_for(&error).to_owned(),
        error.to_string(),
    ))))]
}

/// Flattens a DataFusion `EXPLAIN` result set (columns `plan_type`, `plan`,
/// one row per plan stage) into plain text lines for the `QUERY PLAN` text
/// column pgwire response. Reused by Slice 4's standard `EXPLAIN <query>`.
fn explain_batches_to_plan_lines(batches: &[RecordBatch]) -> Vec<String> {
    use datafusion::arrow::array::StringArray;

    let mut lines = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut parts: Vec<String> = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    continue;
                }
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    parts.push(arr.value(row_idx).to_string());
                }
            }
            if !parts.is_empty() {
                lines.push(parts.join(": "));
            }
        }
    }
    if lines.is_empty() {
        lines.push("Plan: (no plan produced)".to_string());
    }
    lines
}

fn extract_after_fence_token(q: &str) -> Option<FreshnessToken> {
    let ql = q.to_lowercase();
    let start = ql.find("rockstream.after_fence(")?;
    let after = &q[start + "rockstream.after_fence(".len()..];
    let quoted = after.strip_prefix('\'')?;
    let end = quoted.find('\'')?;
    serde_json::from_str::<FreshnessToken>(&quoted[..end]).ok()
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

fn extract_simple_scatter_predicates(
    catalog: &CatalogStubs,
    view_name: &str,
    q: &str,
) -> Vec<crate::multi_shard_reader::ScatterPredicate> {
    let where_pos = match q.to_lowercase().find(" where ") {
        Some(pos) => pos,
        None => return Vec::new(),
    };
    let where_clause = q[where_pos + 7..]
        .split(" ORDER BY ")
        .next()
        .unwrap_or("")
        .split(" LIMIT ")
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches(';');
    let columns = catalog
        .get_table(view_name)
        .map(|table| table.columns)
        .or_else(|| catalog.get_view(view_name).map(|view| view.columns))
        .unwrap_or_default();
    where_clause
        .split(" AND ")
        .filter_map(|part| {
            let (name, value) = part.split_once('=')?;
            let col_idx = columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name.trim()))?;
            let raw = value.trim().trim_matches('\'').trim_matches('"');
            Some(crate::multi_shard_reader::ScatterPredicate::Eq {
                col_idx: col_idx as u16,
                value: raw.as_bytes().to_vec(),
            })
        })
        .collect()
}

fn build_scatter_explain_note(catalog: &CatalogStubs, inner_sql: &str) -> String {
    let Some(view_name) = extract_view_name_from_select(inner_sql) else {
        return String::new();
    };
    let shard_stats = catalog.shard_stats(&view_name);
    if shard_stats.is_empty() {
        return String::new();
    }
    let predicates = extract_simple_scatter_predicates(catalog, &view_name, inner_sql);
    let latest_epoch = shard_stats
        .iter()
        .map(|stats| stats.checkpoint_epoch)
        .max()
        .unwrap_or(0);
    let plan =
        crate::multi_shard_reader::plan_scatter_shards(&shard_stats, &predicates, 5, latest_epoch);
    let cols = if plan.pruned_columns.is_empty() {
        String::from("none")
    } else {
        plan.pruned_columns
            .iter()
            .map(|col_idx| col_idx.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "\nshard_scan: {}/{} shards (pruned by column statistics on {})",
        plan.shard_ids.len(),
        plan.total_shards,
        cols
    )
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

/// Extract ORDER BY columns from a query string.
/// Returns a list of `(column_name, descending)` pairs.
fn extract_order_by(q: &str) -> Vec<(String, bool)> {
    let ql = q.to_lowercase();
    let ob_pos = match ql.find(" order by ") {
        Some(p) => p,
        None => return vec![],
    };
    // Everything after ORDER BY, up to LIMIT or end
    let after = q[ob_pos + 10..].trim();
    let after_lower = after.to_lowercase();
    let end = after_lower.find(" limit ").unwrap_or(after.len());
    let order_part = after[..end].trim().trim_end_matches(';');

    order_part
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let tokens: Vec<&str> = part.split_whitespace().collect();
            let col = tokens.first()?.to_lowercase();
            let desc = tokens
                .last()
                .map(|t| t.to_lowercase() == "desc")
                .unwrap_or(false);
            Some((col, desc))
        })
        .collect()
}

/// Extract a simple equality predicate `WHERE <col> = <int_literal>` from a query.
///
/// Returns `(column_name, value)` when the query contains exactly one equality
/// on an integer literal. Only used for index-scan routing — complex predicates
/// fall through to the normal full-scan path.
fn extract_where_equality(q: &str) -> Option<(String, i64)> {
    let ql = q.to_lowercase();
    let where_pos = ql.find(" where ")?;
    // Take everything after WHERE, stop at ORDER / LIMIT / semicolon.
    let after = q[where_pos + 7..].trim();
    let after_lower = after.to_lowercase();
    let end = ["order by ", "limit ", ";"]
        .iter()
        .filter_map(|kw| after_lower.find(kw))
        .min()
        .unwrap_or(after.len());
    let pred = after[..end].trim();

    // Only handle the simple `col = literal` form (no AND / OR / NOT).
    if pred.contains(" and ") || pred.contains(" or ") || pred.to_lowercase().starts_with("not ") {
        return None;
    }
    let eq_pos = pred.find('=')?;
    let col = pred[..eq_pos].trim().to_lowercase();
    let val_str = pred[eq_pos + 1..].trim().trim_end_matches(';');
    let val: i64 = val_str.parse().ok()?;
    if col.is_empty() {
        return None;
    }
    Some((col, val))
}

/// Extract a bounded single-column range predicate from a query's `WHERE`
/// clause — the "range-lookup accelerator" sibling of `extract_where_equality`
/// (v0.51.2 Slice 5). Recognizes `col > lo AND col < hi` (and `>=`/`<=`
/// variants) where both bounds reference the *same* column. Returns
/// `(column_name, lower_inclusive, upper_inclusive)`. Anything else (OR, NOT,
/// multi-column AND, no bound on one side) falls through to the normal
/// full-scan/DataFusion path.
fn extract_where_range(q: &str) -> Option<(String, i64, i64)> {
    let ql = q.to_lowercase();
    let where_pos = ql.find(" where ")?;
    let after = q[where_pos + 7..].trim();
    let after_lower = after.to_lowercase();
    let end = ["order by ", "limit ", ";"]
        .iter()
        .filter_map(|kw| after_lower.find(kw))
        .min()
        .unwrap_or(after.len());
    let pred = after[..end].trim();

    if pred.to_lowercase().contains(" or ") || pred.to_lowercase().starts_with("not ") {
        return None;
    }
    let and_pos = pred.to_lowercase().find(" and ")?;
    let left = pred[..and_pos].trim();
    let right = pred[and_pos + 5..].trim().trim_end_matches(';');
    // Reject a third AND clause (multi-predicate ranges are out of scope).
    if right.to_lowercase().contains(" and ") {
        return None;
    }

    fn parse_bound(clause: &str) -> Option<(String, char, bool, i64)> {
        // Returns (column, operator_char, is_lower_bound, value).
        for (op, is_lower) in [(">=", true), ("<=", false), (">", true), ("<", false)] {
            if let Some(pos) = clause.find(op) {
                let col = clause[..pos].trim().to_lowercase();
                let val_str = clause[pos + op.len()..].trim();
                let val: i64 = val_str.parse().ok()?;
                if col.is_empty() {
                    return None;
                }
                return Some((col, op.chars().next()?, is_lower, val));
            }
        }
        None
    }

    let (left_col, left_op, left_is_lower, left_val) = parse_bound(left)?;
    let (right_col, right_op, right_is_lower, right_val) = parse_bound(right)?;
    if left_col != right_col || left_is_lower == right_is_lower {
        return None;
    }
    let (lower_val, lower_op, upper_val, upper_op) = if left_is_lower {
        (left_val, left_op, right_val, right_op)
    } else {
        (right_val, right_op, left_val, left_op)
    };
    // Normalize exclusive bounds (`>`/`<`) to inclusive i64 bounds.
    let lower_inclusive = if lower_op == '>' {
        lower_val + 1
    } else {
        lower_val
    };
    let upper_inclusive = if upper_op == '<' {
        upper_val - 1
    } else {
        upper_val
    };
    if lower_inclusive > upper_inclusive {
        return None;
    }
    Some((left_col, lower_inclusive, upper_inclusive))
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
    if let Some(CatalogResponse::Rows { columns, .. }) =
        catalog.handle_query(q, &crate::catalog_stubs::SessionInfo::default())
    {
        return columns
            .iter()
            .map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text))
            .collect();
    }
    if let Some(view_name) = extract_view_name_from_select(q) {
        if let Some(cv) = catalog.get_view(&view_name) {
            let select_cols = infer_select_columns(q);
            let res: Vec<FieldInfo> = cv
                .columns
                .iter()
                .filter(|c| select_cols.is_empty() || select_cols.contains(&c.name.to_lowercase()))
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
            return res;
        }
        // Raw (non-view) catalog tables use the same extended-query Describe
        // path as views: RowDescription's field list/types must match what
        // `do_query`'s row-store SELECT dispatch (`arrow_type_to_pg_oid` +
        // `pg_type_from_oid`) actually encodes, or tokio-postgres's client
        // rejects the response with "DataRow field count does not match".
        if let Some(ct) = catalog.get_table(&view_name) {
            let select_cols = infer_select_columns(q);
            let res: Vec<FieldInfo> = ct
                .columns
                .iter()
                .filter(|c| select_cols.is_empty() || select_cols.contains(&c.name.to_lowercase()))
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
            return res;
        }
    }
    // For literal SELECT queries (no FROM / no recognized view), infer column names
    // from the SELECT list. This ensures RowDescription and DataRow field counts match
    // when try_datafusion_select executes the query.
    let ql = q.trim().to_lowercase();
    if ql.starts_with("select ") {
        let cols = infer_select_columns(q);
        if !cols.is_empty() {
            return cols
                .iter()
                .map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text))
                .collect();
        }
        // Fallback: return a single anonymous column so field counts match
        return vec![FieldInfo::new(
            "?column?".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )];
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
        lower
            .strip_prefix("select\n")
            .or_else(|| lower.strip_prefix("select\t"))
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
                // No alias: take the last whitespace-delimited token, then
                // strip any table qualifier (e.g. `c.name` → `name`)
                let last = p.split_whitespace().last().unwrap_or("").to_lowercase();
                if last.is_empty() {
                    None
                } else {
                    // Strip table.column → column
                    let col = last.rsplit('.').next().unwrap_or(&last).to_string();
                    Some(col)
                }
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCreateSink {
    name: String,
    view: String,
    format: String,
    path: String,
    snapshot_interval_epochs: Option<u64>,
    snapshot_interval_ms: Option<u64>,
    parquet_row_group_bytes: Option<u64>,
    format_version: Option<u64>,
    partition_by: Vec<String>,
    catalog: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CreateSinkOptionValue {
    Number(u64),
    String(String),
    Array(Vec<String>),
}

/// Actionable remediation text shared by every CREATE SINK DDL/option parse failure.
const CREATE_SINK_NEXT_STEPS: &str =
    "check CREATE SINK syntax, referenced view name, and WITH option types; use catalog=filesystem|glue|rest|hive|ducklake.";

fn create_sink_error_response(message: String) -> Response<'static> {
    Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        message,
    )))
}

fn parse_create_sink_ddl(q: &str) -> Result<ParsedCreateSink, String> {
    let trimmed = q.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("create sink ") {
        return Err(format!(
            "[RS-4007] CREATE SINK statement must start with CREATE SINK. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }

    let after_create = &trimmed["CREATE SINK".len()..].trim();
    let after_create_lower = after_create.to_lowercase();
    let for_view_pos = after_create_lower.find(" for view ").ok_or_else(|| {
        format!(
            "[RS-4007] CREATE SINK requires FOR VIEW clause. Next steps: {CREATE_SINK_NEXT_STEPS}"
        )
    })?;
    let name = after_create[..for_view_pos]
        .trim()
        .trim_matches('"')
        .to_lowercase();
    if name.is_empty() {
        return Err(format!(
            "[RS-4007] CREATE SINK requires a sink name. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }

    let after_for_view = after_create[for_view_pos + " for view ".len()..].trim();
    let after_for_view_lower = after_for_view.to_lowercase();
    let to_pos = after_for_view_lower.find(" to ").ok_or_else(|| {
        format!("[RS-4007] CREATE SINK requires TO clause. Next steps: {CREATE_SINK_NEXT_STEPS}")
    })?;
    let view = after_for_view[..to_pos]
        .trim()
        .trim_matches('"')
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase();
    if view.is_empty() {
        return Err(format!(
            "[RS-4007] CREATE SINK requires a referenced view name. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }

    let after_to = after_for_view[to_pos + " to ".len()..].trim();
    let format_end = after_to.find(char::is_whitespace).ok_or_else(|| {
        format!(
            "[RS-4007] CREATE SINK requires a quoted sink path. Next steps: {CREATE_SINK_NEXT_STEPS}"
        )
    })?;
    let format = after_to[..format_end].trim().to_uppercase();
    if !matches!(format.as_str(), "ICEBERG" | "DELTA") {
        return Err(format!(
            "[RS-4007] CREATE SINK format must be ICEBERG or DELTA. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }

    let after_format = after_to[format_end..].trim();
    let (path, consumed) = parse_sql_single_quoted_string(after_format)?;
    let after_path = after_format[consumed..].trim();
    if !after_path.to_lowercase().starts_with("with") {
        return Err(format!(
            "[RS-4007] CREATE SINK requires WITH (...) options. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }
    let with_body = after_path["with".len()..].trim();
    let option_map = parse_create_sink_options(with_body)?;

    let snapshot_interval_epochs =
        parse_optional_u64_option(&option_map, "snapshot_interval_epochs")?;
    let snapshot_interval_ms = parse_optional_u64_option(&option_map, "snapshot_interval_ms")?;
    let parquet_row_group_bytes =
        parse_optional_u64_option(&option_map, "parquet_row_group_bytes")?;
    let format_version = parse_optional_u64_option(&option_map, "format_version")?;
    let partition_by = match option_map.get("partition_by") {
        Some(CreateSinkOptionValue::Array(values)) => values.clone(),
        Some(_) => {
            return Err(format!(
                "[RS-4007] CREATE SINK option partition_by must be ARRAY[...]. Next steps: {CREATE_SINK_NEXT_STEPS}"
            ))
        }
        None => Vec::new(),
    };
    let catalog = match option_map.get("catalog") {
        Some(CreateSinkOptionValue::String(value)) => value.to_lowercase(),
        Some(_) => {
            return Err(format!(
                "[RS-4007] CREATE SINK option catalog must be an identifier or string. Next steps: {CREATE_SINK_NEXT_STEPS}"
            ))
        }
        None => "filesystem".to_string(),
    };

    Ok(ParsedCreateSink {
        name,
        view,
        format,
        path,
        snapshot_interval_epochs,
        snapshot_interval_ms,
        parquet_row_group_bytes,
        format_version,
        partition_by,
        catalog,
    })
}

fn parse_create_sink_options(
    raw_with: &str,
) -> Result<std::collections::HashMap<String, CreateSinkOptionValue>, String> {
    let raw = raw_with.trim();
    if !raw.starts_with('(') || !raw.ends_with(')') {
        return Err(format!(
            "[RS-4007] CREATE SINK WITH clause must be parenthesized. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }
    let inner = &raw[1..raw.len() - 1];
    let mut options = std::collections::HashMap::new();
    for part in split_top_level_comma_list(inner)? {
        if part.trim().is_empty() {
            continue;
        }
        let eq_idx = find_top_level_equals(&part).ok_or_else(|| {
            format!(
                "[RS-4007] CREATE SINK option '{part}' must use key=value syntax. Next steps: {CREATE_SINK_NEXT_STEPS}"
            )
        })?;
        let key = part[..eq_idx].trim().to_lowercase();
        let raw_value = part[eq_idx + 1..].trim();
        if key.is_empty() || raw_value.is_empty() {
            return Err(format!(
                "[RS-4007] CREATE SINK option '{part}' is malformed. Next steps: {CREATE_SINK_NEXT_STEPS}"
            ));
        }
        let value = parse_create_sink_option_value(raw_value)?;
        options.insert(key, value);
    }
    Ok(options)
}

fn parse_create_sink_option_value(raw: &str) -> Result<CreateSinkOptionValue, String> {
    let trimmed = raw.trim();
    if trimmed.to_uppercase().starts_with("ARRAY[") {
        if !trimmed.ends_with(']') {
            return Err(format!(
                "[RS-4007] malformed ARRAY value '{trimmed}'. Next steps: {CREATE_SINK_NEXT_STEPS}"
            ));
        }
        let inner = &trimmed[6..trimmed.len() - 1];
        let mut values = Vec::new();
        for item in split_top_level_comma_list(inner)? {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            if item.starts_with('\'') {
                let (value, consumed) = parse_sql_single_quoted_string(item)?;
                if item[consumed..].trim().is_empty() {
                    values.push(value);
                    continue;
                }
                return Err(format!(
                    "[RS-4007] malformed ARRAY item '{item}'. Next steps: {CREATE_SINK_NEXT_STEPS}"
                ));
            }
            values.push(item.trim_matches('"').to_string());
        }
        return Ok(CreateSinkOptionValue::Array(values));
    }

    if trimmed.starts_with('\'') {
        let (value, consumed) = parse_sql_single_quoted_string(trimmed)?;
        if !trimmed[consumed..].trim().is_empty() {
            return Err(format!(
                "[RS-4007] malformed string literal '{trimmed}'. Next steps: {CREATE_SINK_NEXT_STEPS}"
            ));
        }
        return Ok(CreateSinkOptionValue::String(value));
    }

    if let Ok(value) = trimmed.parse::<u64>() {
        return Ok(CreateSinkOptionValue::Number(value));
    }

    Ok(CreateSinkOptionValue::String(
        trimmed.trim_matches('"').to_string(),
    ))
}

fn parse_optional_u64_option(
    options: &std::collections::HashMap<String, CreateSinkOptionValue>,
    key: &str,
) -> Result<Option<u64>, String> {
    match options.get(key) {
        Some(CreateSinkOptionValue::Number(value)) => Ok(Some(*value)),
        Some(_) => Err(format!(
            "[RS-4007] CREATE SINK option {key} must be a number. Next steps: {CREATE_SINK_NEXT_STEPS}"
        )),
        None => Ok(None),
    }
}

fn parse_sql_single_quoted_string(input: &str) -> Result<(String, usize), String> {
    let bytes = input.as_bytes();
    if bytes.first().copied() != Some(b'\'') {
        return Err(format!(
            "[RS-4007] expected single-quoted string literal. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }
    let mut idx = 1usize;
    let mut value = String::new();
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    value.push('\'');
                    idx += 2;
                    continue;
                }
                return Ok((value, idx + 1));
            }
            byte => {
                value.push(byte as char);
                idx += 1;
            }
        }
    }
    Err(format!(
        "[RS-4007] unterminated string literal. Next steps: {CREATE_SINK_NEXT_STEPS}"
    ))
}

const CREATE_SOURCE_NEXT_STEPS: &str =
    "syntax: CREATE SOURCE <name> TYPE kafka|s3|postgres_cdc|http_webhook (...options...) FORMAT json|avro|csv|pgoutput|wal2json; postgres_cdc requires credential_ref, publication, and slot; http_webhook requires credential_ref";
const ALTER_SOURCE_NEXT_STEPS: &str =
    "syntax: ALTER SOURCE <name> {PAUSE|RESUME|DROP|ADVANCE WATERMARK <u64>}";

fn create_source_error_response(message: String) -> Response<'static> {
    Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        message,
    )))
}

#[derive(Debug, Clone)]
struct ParsedCreateSource {
    name: String,
    source_type: String,
    options: std::collections::HashMap<String, String>,
    format: String,
}

fn parse_create_source_ddl(q: &str) -> Result<ParsedCreateSource, String> {
    let trimmed = q.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("create source ") {
        return Err(format!(
            "[RS-4008] CREATE SOURCE statement must start with CREATE SOURCE. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
        ));
    }

    let after_create = trimmed["CREATE SOURCE".len()..].trim();
    let after_create_lower = after_create.to_lowercase();
    let type_pos = after_create_lower.find(" type ").ok_or_else(|| {
        format!(
            "[RS-4008] CREATE SOURCE requires TYPE clause. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
        )
    })?;

    let name = after_create[..type_pos]
        .trim()
        .trim_matches('"')
        .to_lowercase();
    if name.is_empty() {
        return Err(format!(
            "[RS-4008] CREATE SOURCE requires a source name. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
        ));
    }

    let after_type = after_create[type_pos + " type ".len()..].trim();
    let after_type_lower = after_type.to_lowercase();

    let format_pos = after_type_lower.find(" format ").ok_or_else(|| {
        format!(
            "[RS-4008] CREATE SOURCE requires FORMAT clause. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
        )
    })?;

    let type_and_opts = after_type[..format_pos].trim();
    let format_str = after_type[format_pos + " format ".len()..]
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_lowercase();

    let (source_type, options_str) = if let Some(open_paren) = type_and_opts.find('(') {
        let st = type_and_opts[..open_paren].trim().to_lowercase();
        let close_paren = type_and_opts.rfind(')').unwrap_or(type_and_opts.len());
        let opts = &type_and_opts[open_paren + 1..close_paren];
        (st, opts)
    } else {
        (type_and_opts.trim().to_lowercase(), "")
    };

    let mut options = std::collections::HashMap::new();
    if !options_str.trim().is_empty() {
        for pair in options_str.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some(eq) = pair.find('=') {
                let k = pair[..eq]
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_lowercase();
                let v = pair[eq + 1..]
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                options.insert(k, v);
            }
        }
    }

    let allowed_format = match source_type.as_str() {
        "kafka" | "s3" => matches!(format_str.as_str(), "json" | "avro" | "csv"),
        "postgres_cdc" => matches!(format_str.as_str(), "pgoutput" | "wal2json"),
        "http_webhook" => matches!(format_str.as_str(), "json" | "csv"),
        _ => {
            return Err(format!(
                "[RS-4008] CREATE SOURCE type '{}' is invalid; expected kafka|s3|postgres_cdc|http_webhook. Next steps: {CREATE_SOURCE_NEXT_STEPS}",
                source_type
            ));
        }
    };
    if !allowed_format {
        return Err(format!(
            "[RS-4008] CREATE SOURCE format '{}' is invalid for type '{}'. Next steps: {CREATE_SOURCE_NEXT_STEPS}",
            format_str, source_type
        ));
    }
    validate_typed_source_options(&source_type, &options)?;

    Ok(ParsedCreateSource {
        name,
        source_type,
        options,
        format: format_str,
    })
}

/// Enforce that catalogued source configuration contains only credential
/// references.  Resolving a reference belongs to the runtime worker, never to
/// SQL parsing or a SHOW response.
fn validate_typed_source_options(
    source_type: &str,
    options: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if source_type != "postgres_cdc" && source_type != "http_webhook" {
        return Ok(());
    }
    for key in ["password", "token", "secret", "api_key", "authorization"] {
        if options.contains_key(key) {
            return Err(format!(
                "[RS-4008] CREATE SOURCE option '{key}' contains an inline credential; use credential_ref instead. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
            ));
        }
    }
    if options.get("credential_ref").is_none_or(String::is_empty) {
        return Err(format!(
            "[RS-4008] CREATE SOURCE type '{source_type}' requires a non-empty credential_ref. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
        ));
    }
    if source_type == "postgres_cdc" {
        for key in ["publication", "slot"] {
            if options.get(key).is_none_or(String::is_empty) {
                return Err(format!(
                    "[RS-4008] CREATE SOURCE type 'postgres_cdc' requires a non-empty {key}. Next steps: {CREATE_SOURCE_NEXT_STEPS}"
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AlterSourceAction {
    Pause,
    Resume,
    Drop,
    AdvanceWatermark(u64),
    ReplayDlq {
        since: Option<u64>,
        until: Option<u64>,
    },
    DismissDlq {
        condition: Option<String>,
    },
    SetOptions(Vec<(String, String)>),
}

#[derive(Debug, Clone)]
struct ParsedAlterSource {
    name: String,
    action: AlterSourceAction,
}

async fn serve_webhook_connection(
    mut socket: tokio::net::TcpStream,
    handler: Arc<GatewayHandler>,
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;

    const MAX_HEADER_BYTES: usize = 16 * 1024;
    let mut request = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&chunk[..read]);
        if request.len() > MAX_HEADER_BYTES + HTTP_WEBHOOK_MAX_REQUEST_BYTES {
            return write_webhook_response(&mut socket, WebhookResult::PayloadTooLarge).await;
        }
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break end + 4;
        }
        if request.len() > MAX_HEADER_BYTES {
            return write_webhook_response(&mut socket, WebhookResult::InvalidPayload).await;
        }
    };

    let headers = match std::str::from_utf8(&request[..header_end]) {
        Ok(headers) => headers,
        Err(_) => return write_webhook_response(&mut socket, WebhookResult::InvalidPayload).await,
    };
    let mut lines = headers.split("\r\n");
    let Some(request_line) = lines.next() else {
        return write_webhook_response(&mut socket, WebhookResult::InvalidPayload).await;
    };
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next();
    let path = request_parts.next();
    if method != Some("POST") || request_parts.next().is_none() {
        return write_webhook_response(&mut socket, WebhookResult::NotFound).await;
    }
    let Some(source_name) = path.and_then(|path| path.strip_prefix("/webhook/")) else {
        return write_webhook_response(&mut socket, WebhookResult::NotFound).await;
    };
    if source_name.is_empty() || source_name.contains('/') {
        return write_webhook_response(&mut socket, WebhookResult::NotFound).await;
    }
    let source_name = source_name.to_ascii_lowercase();

    let mut token = None;
    let mut delivery_id = None;
    let mut content_length = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("authorization") {
            token = value
                .strip_prefix("Bearer ")
                .map(|value| value.as_bytes().to_vec());
        } else if name.eq_ignore_ascii_case("idempotency-key")
            || name.eq_ignore_ascii_case("x-delivery-id")
        {
            delivery_id = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse::<usize>().ok();
        }
    }
    let Some(content_length) = content_length else {
        return write_webhook_response(&mut socket, WebhookResult::InvalidPayload).await;
    };
    if content_length > HTTP_WEBHOOK_MAX_REQUEST_BYTES {
        return write_webhook_response(&mut socket, WebhookResult::PayloadTooLarge).await;
    }
    let body_start = header_end;
    let already_read = request.len().saturating_sub(body_start);
    if already_read > content_length {
        return write_webhook_response(&mut socket, WebhookResult::InvalidPayload).await;
    }
    request.resize(body_start + content_length, 0);
    if already_read < content_length {
        socket
            .read_exact(&mut request[body_start + already_read..])
            .await?;
    }
    let result = handler
        .accept_webhook(
            &source_name,
            token.as_deref().unwrap_or_default(),
            delivery_id.as_deref(),
            &request[body_start..],
        )
        .await;
    write_webhook_response(&mut socket, result).await
}

async fn write_webhook_response(
    socket: &mut tokio::net::TcpStream,
    result: WebhookResult,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = match result.error_code() {
        Some(code) => format!("{code}: webhook request rejected. Next steps: verify source, bearer token, payload, and source capacity\n"),
        None => "accepted\n".to_string(),
    };
    let reason = match result.status_code() {
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        _ => "Internal Server Error",
    };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        result.status_code(), reason, body.len(), body
    );
    socket.write_all(response.as_bytes()).await
}

fn parse_alter_source_ddl(q: &str) -> Result<ParsedAlterSource, String> {
    let trimmed = q.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("drop source ") {
        let name = trimmed["DROP SOURCE".len()..]
            .trim()
            .trim_matches('"')
            .to_lowercase();
        if name.is_empty() {
            return Err(format!(
                "[RS-4008] DROP SOURCE requires a source name. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
            ));
        }
        return Ok(ParsedAlterSource {
            name,
            action: AlterSourceAction::Drop,
        });
    }

    if !lower.starts_with("alter source ") {
        return Err(format!(
            "[RS-4008] ALTER SOURCE statement must start with ALTER SOURCE or DROP SOURCE. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
        ));
    }

    if lower.contains("replay dead_letter_queue") || lower.contains("replay dead letter queue") {
        let pos = lower.find("replay dead").ok_or_else(|| {
            format!(
                "[RS-4008] Invalid ALTER SOURCE statement. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
            )
        })?;
        let name = trimmed["ALTER SOURCE".len()..pos]
            .trim()
            .trim_matches('"')
            .to_lowercase();
        let after_replay = &trimmed[pos..];
        let mut since = None;
        let mut until = None;
        let lower_after = after_replay.to_lowercase();
        if let Some(spos) = lower_after.find("since ") {
            let rest = after_replay[spos + "since ".len()..].trim();
            let tok = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('\'');
            since = tok.parse::<u64>().ok();
        }
        if let Some(upos) = lower_after.find("until ") {
            let rest = after_replay[upos + "until ".len()..].trim();
            let tok = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('\'');
            until = tok.parse::<u64>().ok();
        }
        return Ok(ParsedAlterSource {
            name,
            action: AlterSourceAction::ReplayDlq { since, until },
        });
    }

    if lower.contains("dismiss dead_letter_queue") || lower.contains("dismiss dead letter queue") {
        let pos = lower.find("dismiss dead").ok_or_else(|| {
            format!(
                "[RS-4008] Invalid ALTER SOURCE statement. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
            )
        })?;
        let name = trimmed["ALTER SOURCE".len()..pos]
            .trim()
            .trim_matches('"')
            .to_lowercase();
        let after_dismiss = &trimmed[pos..];
        let mut condition = None;
        let lower_after = after_dismiss.to_lowercase();
        if let Some(wpos) = lower_after.find("where ") {
            condition = Some(after_dismiss[wpos + "where ".len()..].trim().to_string());
        }
        return Ok(ParsedAlterSource {
            name,
            action: AlterSourceAction::DismissDlq { condition },
        });
    }

    if lower.contains(" set (") || lower.contains(" set(") {
        let pos = lower.find(" set").ok_or_else(|| {
            format!(
                "[RS-4008] Invalid ALTER SOURCE statement. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
            )
        })?;
        let name = trimmed["ALTER SOURCE".len()..pos]
            .trim()
            .trim_matches('"')
            .to_lowercase();
        let after_set = &trimmed[pos..];
        let mut options = Vec::new();
        if let (Some(open), Some(close)) = (after_set.find('('), after_set.rfind(')')) {
            let opts_str = &after_set[open + 1..close];
            for pair in opts_str.split(',') {
                if let Some((k, v)) = pair.split_once('=') {
                    options.push((k.trim().to_string(), v.trim().to_string()));
                }
            }
        }
        return Ok(ParsedAlterSource {
            name,
            action: AlterSourceAction::SetOptions(options),
        });
    }

    let after_alter = trimmed["ALTER SOURCE".len()..].trim();
    let tokens: Vec<&str> = after_alter.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err(format!(
            "[RS-4008] ALTER SOURCE requires source name and action (PAUSE|RESUME|DROP). Next steps: {ALTER_SOURCE_NEXT_STEPS}"
        ));
    }

    if tokens.len() == 4
        && tokens[tokens.len() - 2].eq_ignore_ascii_case("advance")
        && tokens[tokens.len() - 1].eq_ignore_ascii_case("watermark")
    {
        return Err(format!(
            "[RS-4008] ALTER SOURCE ADVANCE WATERMARK requires an unsigned value. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
        ));
    }
    if tokens.len() == 4
        && tokens[tokens.len() - 3].eq_ignore_ascii_case("advance")
        && tokens[tokens.len() - 2].eq_ignore_ascii_case("watermark")
    {
        let watermark = tokens[tokens.len() - 1].parse::<u64>().map_err(|_| format!(
            "[RS-4008] ALTER SOURCE ADVANCE WATERMARK requires an unsigned value. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
        ))?;
        return Ok(ParsedAlterSource {
            name: tokens[..tokens.len() - 3]
                .join(" ")
                .trim_matches('"')
                .to_lowercase(),
            action: AlterSourceAction::AdvanceWatermark(watermark),
        });
    }
    let Some(last_tok) = tokens.last() else {
        return Err(format!(
            "[RS-4008] invalid ALTER SOURCE statement. Next steps: {ALTER_SOURCE_NEXT_STEPS}"
        ));
    };
    let action_str = last_tok.to_lowercase();
    let name = tokens[..tokens.len() - 1]
        .join(" ")
        .trim_matches('"')
        .to_lowercase();

    let action = match action_str.as_str() {
        "pause" => AlterSourceAction::Pause,
        "resume" => AlterSourceAction::Resume,
        "drop" => AlterSourceAction::Drop,
        _ => {
            return Err(format!(
                "[RS-4008] ALTER SOURCE action '{}' is invalid; expected PAUSE|RESUME|DROP. Next steps: {ALTER_SOURCE_NEXT_STEPS}",
                action_str
            ));
        }
    };

    Ok(ParsedAlterSource { name, action })
}

fn split_top_level_comma_list(input: &str) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push(ch);
                if in_string {
                    if chars.peek() == Some(&'\'') {
                        if let Some(next_quote) = chars.next() {
                            current.push(next_quote);
                        }
                    } else {
                        in_string = false;
                    }
                } else {
                    in_string = true;
                }
            }

            '[' if !in_string => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_string => {
                if bracket_depth == 0 {
                    return Err(format!(
                        "[RS-4007] unbalanced ] in WITH clause. Next steps: {CREATE_SINK_NEXT_STEPS}"
                    ));
                }
                bracket_depth -= 1;
                current.push(ch);
            }
            ',' if !in_string && bracket_depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_string {
        return Err(format!(
            "[RS-4007] unterminated string literal in WITH clause. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }
    if bracket_depth != 0 {
        return Err(format!(
            "[RS-4007] unbalanced ARRAY[...] in WITH clause. Next steps: {CREATE_SINK_NEXT_STEPS}"
        ));
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    Ok(parts)
}

fn find_top_level_equals(input: &str) -> Option<usize> {
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' => {
                if in_string && chars.peek().map(|(_, c)| *c) == Some('\'') {
                    chars.next();
                } else {
                    in_string = !in_string;
                }
            }
            '[' if !in_string => bracket_depth += 1,
            ']' if !in_string && bracket_depth > 0 => bracket_depth -= 1,
            '=' if !in_string && bracket_depth == 0 => return Some(idx),
            _ => {}
        }
    }
    None
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
    let raw = after[..as_pos]
        .split_once(" WITH WORKLOAD ")
        .map(|(before, _)| before)
        .or_else(|| {
            after[..as_pos]
                .split_once(" with workload ")
                .map(|(before, _)| before)
        })
        .unwrap_or(&after[..as_pos])
        .trim()
        .trim_matches('"');
    let name = raw.rsplit('.').next().unwrap_or(raw).trim_matches('"');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn parse_create_view_workload(q: &str) -> Option<String> {
    let ql = q.trim().to_lowercase();
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
    let after_lower = &ql[name_start..];
    let as_pos_in_after = find_as_separator(after_lower)?;
    let before_as = q[name_start..name_start + as_pos_in_after].trim();
    let lower_before_as = before_as.to_lowercase();
    let marker = " with workload = ";
    let marker_pos = lower_before_as.find(marker)?;
    let workload = before_as[marker_pos + marker.len()..]
        .trim()
        .trim_matches('"')
        .trim_matches('\'');
    if workload.is_empty() {
        None
    } else {
        Some(workload.to_string())
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

#[derive(Default)]
struct WorkloadSettings {
    memory_limit: Option<MemoryLimit>,
    freshness_slo: Option<FreshnessSlo>,
    priority: Option<WorkloadPriority>,
    max_parallelism: Option<u32>,
}

fn parse_workload_settings(body: &str) -> Option<WorkloadSettings> {
    let mut settings = WorkloadSettings::default();
    for assignment in body.split(',') {
        let (key, value) = assignment.split_once('=')?;
        let key = key.trim().to_lowercase();
        let value = value.trim().trim_matches('\'').trim_matches('"');
        match key.as_str() {
            "memory_limit" => {
                settings.memory_limit = Some(MemoryLimit::new(value.parse().ok()?));
            }
            "max_parallelism" => {
                settings.max_parallelism = Some(value.parse().ok()?);
            }
            "freshness_slo_ms" => {
                settings.freshness_slo = Some(FreshnessSlo::new(value.parse().ok()?));
            }
            "priority" => {
                settings.priority = Some(match value.to_ascii_uppercase().as_str() {
                    "HIGH" => WorkloadPriority::HIGH,
                    "DEFAULT" => WorkloadPriority::DEFAULT,
                    "LOW" => WorkloadPriority::LOW,
                    _ => WorkloadPriority(value.parse().ok()?),
                });
            }
            _ => {}
        }
    }
    Some(settings)
}

fn apply_workload_settings(workload: &mut WorkloadDef, settings: &WorkloadSettings) {
    if let Some(memory_limit) = settings.memory_limit {
        workload.memory_limit = Some(memory_limit);
    }
    if let Some(freshness_slo) = settings.freshness_slo {
        workload.freshness_slo = Some(freshness_slo);
    }
    if let Some(priority) = settings.priority {
        workload.priority = priority;
    }
    if let Some(max_parallelism) = settings.max_parallelism {
        workload.max_parallelism = Some(max_parallelism);
    }
}

fn parse_create_workload(q: &str) -> Option<WorkloadDef> {
    let after = q
        .trim()
        .strip_prefix("CREATE WORKLOAD ")
        .or_else(|| q.trim().strip_prefix("create workload "))?
        .trim()
        .trim_end_matches(';');
    let (name_part, options_part) = if let Some((name, rest)) = after.split_once(" WITH ") {
        (name.trim(), Some(rest.trim()))
    } else if let Some((name, rest)) = after.split_once(" with ") {
        (name.trim(), Some(rest.trim()))
    } else {
        (after.trim(), None)
    };
    let name = name_part.trim_matches('"');
    if name.is_empty() {
        return None;
    }
    let mut workload = WorkloadDef::new(name);
    if let Some(options_part) = options_part {
        let body = options_part.strip_prefix('(')?.strip_suffix(')')?.trim();
        let settings = parse_workload_settings(body)?;
        apply_workload_settings(&mut workload, &settings);
    }
    Some(workload)
}

fn parse_alter_workload(q: &str) -> Option<(String, WorkloadSettings)> {
    let after = q
        .trim()
        .strip_prefix("ALTER WORKLOAD ")
        .or_else(|| q.trim().strip_prefix("alter workload "))?
        .trim()
        .trim_end_matches(';');
    let (name_part, settings_part) = after
        .split_once(" SET ")
        .or_else(|| after.split_once(" set "))?;
    let workload_name = name_part.trim().trim_matches('"');
    let body = settings_part
        .trim()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    Some((workload_name.to_string(), parse_workload_settings(body)?))
}

fn parse_drop_workload(q: &str) -> Option<String> {
    let after = q
        .trim()
        .strip_prefix("DROP WORKLOAD ")
        .or_else(|| q.trim().strip_prefix("drop workload "))?
        .trim()
        .trim_end_matches(';');
    let workload_name = after.trim_matches('"');
    if workload_name.is_empty() {
        None
    } else {
        Some(workload_name.to_string())
    }
}

/// Extract table/view names referenced in FROM and JOIN clauses.
///
/// Used for dependency tracking in CREATE VIEW cycle detection.
/// Maximum view-of-view chain depth `inline_view_dependencies` will unwind
/// (cycle detection already runs before this is ever called, so this is
/// purely a defensive bound against a pathologically deep — but acyclic —
/// dependency chain, not a correctness requirement).
const MAX_VIEW_INLINE_DEPTH: usize = 16;

/// v0.51.4 Slice 8: recursively inline every `FROM`/`JOIN` reference to a
/// VIEW (as opposed to a base table) in `sql` as a `(view.sql)` subquery,
/// so a view-of-view (e.g. `CREATE VIEW report AS SELECT ... FROM base_tbl
/// JOIN some_view ON ...`) compiles through `compile_plan` like any other
/// view — there is no DataFusion-materializer fallback left to serve it
/// otherwise (Slice 8's whole point). Purely a textual SQL-level rewrite
/// (same token-scan convention `extract_sql_refs` uses); the referencing
/// query's own alias for the joined relation (if any) is preserved as the
/// derived table's alias, since a `(subquery) AS name alias` isn't valid
/// SQL — only when no alias follows does this substitute the view's own
/// name as the subquery's alias (so any back-reference to it, e.g. in a
/// later `WHERE`/`GROUP BY`, keeps resolving).
fn inline_view_dependencies(sql: &str, catalog: &CatalogStubs, depth_budget: usize) -> String {
    if depth_budget == 0 {
        return sql.to_string();
    }
    const NO_ALIAS_FOLLOWS: &[&str] = &[
        "on",
        "where",
        "group",
        "order",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "limit",
        "having",
        "union",
        "except",
        "intersect",
    ];
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let mut out: Vec<String> = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        let tok_lower = tok.to_lowercase();
        if (tok_lower == "from" || tok_lower == "join") && i + 1 < tokens.len() {
            let raw_next = tokens[i + 1];
            let is_subquery = raw_next.starts_with('(');
            let name = raw_next.trim_matches(|c| c == '"' || c == ',' || c == ';' || c == ')');
            let name = name.rsplit('.').next().unwrap_or(name);
            if !is_subquery {
                if let Some(view) = catalog.get_view(name) {
                    out.push(tok.to_string());
                    let inlined = inline_view_dependencies(&view.sql, catalog, depth_budget - 1);
                    let alias_follows = tokens
                        .get(i + 2)
                        .map(|t| !NO_ALIAS_FOLLOWS.contains(&t.to_lowercase().as_str()))
                        .unwrap_or(false);
                    if alias_follows {
                        out.push(format!("({inlined})"));
                    } else {
                        out.push(format!("({inlined}) AS {name}"));
                    }
                    i += 2;
                    continue;
                }
            }
        }
        out.push(tok.to_string());
        i += 1;
    }
    out.join(" ")
}

fn extract_sql_refs(sql: &str) -> Vec<String> {
    let tokens_orig: Vec<&str> = sql.split_whitespace().collect();
    let tokens_lower: Vec<String> = tokens_orig.iter().map(|t| t.to_lowercase()).collect();
    let mut deps = Vec::new();
    for (i, tok_lower) in tokens_lower.iter().enumerate() {
        if tok_lower == "from" || tok_lower == "join" {
            if let Some(next) = tokens_orig.get(i + 1) {
                // Skip subquery openers
                if next.starts_with('(') || next.contains('(') {
                    continue;
                }
                // v0.51.4 Slice 4: strip a trailing `)` too — a `FROM
                // items)` inside a parenthesized subquery (e.g. `FROM
                // (SELECT ... FROM items) WHERE ...`) previously left the
                // dependency name as `"items)"`, which never matched a real
                // catalog table and silently defeated the direct-operator
                // compile fast path for every nested-subquery view shape.
                let name = next.trim_matches(|c| c == '"' || c == ',' || c == ';' || c == ')');
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

/// Substitute `$1`, `$2`, … placeholders in `sql` with their bound values.
///
/// - `None` parameter → `NULL`
/// - `Some(bytes)` that parses as a number → inserted as-is (no quotes)
/// - `Some(bytes)` that is valid UTF-8 text → wrapped in single-quoted string
///   with internal single-quotes escaped as `''`
/// - `Some(bytes)` that is not valid UTF-8 → `NULL`
///
/// The function also strips PostgreSQL-style type casts (e.g. `$1::int`) from
/// the placeholders before substitution so DataFusion can evaluate the literal.
fn substitute_params(sql: &str, params: &[Option<bytes::Bytes>]) -> String {
    let mut result = sql.to_string();
    // Work from the highest index downward so that replacing `$10` before `$1`
    // doesn't corrupt earlier replacements.
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match param {
            None => "NULL".to_string(),
            Some(bytes) => match std::str::from_utf8(bytes) {
                Err(_) => "NULL".to_string(),
                Ok(s) => {
                    // If the value is purely numeric (integer or float), use it bare.
                    if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                        s.to_string()
                    } else {
                        // Escape single-quotes and wrap in single quotes.
                        format!("'{}'", s.replace('\'', "''"))
                    }
                }
            },
        };
        // Replace `$N::cast_type` as well as bare `$N`.
        // We do a simple regex-free replacement: find `$N` and strip any
        // trailing `::identifier` before inserting the replacement.
        let placeholder_with_cast = format!("{}::", placeholder);
        // Replace cast variants first (greedy: strip until non-alphanumeric/underscore).
        let mut new_result = String::with_capacity(result.len());
        let mut remaining = result.as_str();
        while let Some(pos) = remaining.find(&placeholder_with_cast) {
            new_result.push_str(&remaining[..pos]);
            new_result.push_str(&replacement);
            let after = &remaining[pos + placeholder_with_cast.len()..];
            // Skip the cast type name (letters, digits, underscores).
            let skip = after
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(after.len());
            remaining = &after[skip..];
        }
        new_result.push_str(remaining);
        result = new_result;
        // Now replace bare `$N` placeholders.
        result = result.replace(&placeholder, &replacement);
    }
    result
}

// ── DML parsers ───────────────────────────────────────────────────────────────

struct ParsedCreateTableColumns {
    columns: Vec<CatalogColumn>,
    generated_columns: HashMap<String, GeneratedColumnKind>,
}

/// Parse `CREATE TABLE <name> (col type, ...)` column list.
fn parse_create_table_columns(after_table_name: &str) -> ParsedCreateTableColumns {
    let start = match after_table_name.find('(') {
        Some(i) => i + 1,
        None => {
            return ParsedCreateTableColumns {
                columns: vec![],
                generated_columns: HashMap::new(),
            }
        }
    };
    let end = match after_table_name.rfind(')') {
        Some(i) => i,
        None => {
            return ParsedCreateTableColumns {
                columns: vec![],
                generated_columns: HashMap::new(),
            }
        }
    };
    let cols_str = &after_table_name[start..end];
    let mut columns = Vec::new();
    let mut generated_columns = HashMap::new();
    for part in cols_str.split(',') {
        let part = part.trim();
        if let Some((column, generated_kind)) = (|| {
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
            let generated_kind = if part.to_lowercase().contains("default gen_random_uuid()") {
                Some(GeneratedColumnKind::RandomUuid)
            } else if part.to_lowercase().contains("generated always as identity") {
                Some(GeneratedColumnKind::Identity)
            } else {
                None
            };
            Some((
                CatalogColumn {
                    name: col_name,
                    data_type: arrow_type.to_string(),
                },
                generated_kind,
            ))
        })() {
            if let Some(kind) = generated_kind {
                generated_columns.insert(column.name.clone(), kind);
            }
            columns.push(column);
        }
    }
    ParsedCreateTableColumns {
        columns,
        generated_columns,
    }
}

/// Map a Postgres type keyword to an Arrow data type name.
fn pg_type_to_arrow(pg_type: &str) -> &'static str {
    match pg_type {
        "BIGINT" | "INT8" => "Int64",
        "INT" | "INT4" | "INTEGER" => "Int32",
        "SMALLINT" | "INT2" => "Int16",
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" => "Utf8",
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT" => "Float64",
        "FLOAT4" | "REAL" => "Float32",
        "BOOL" | "BOOLEAN" => "Boolean",
        "BYTEA" => "Binary",
        "TIMESTAMP" => "Timestamp",
        "TIMESTAMPTZ" => "TimestampTz",
        "DATE" => "Date32",
        "TIME" => "Time32",
        "UUID" => "UUID",
        "NUMERIC" | "DECIMAL" => "Decimal",
        "JSON" => "Json",
        "JSONB" => "Jsonb",
        "INTERVAL" => "Interval",
        "INT4[]" | "INTEGER[]" | "INT[]" => "_int4",
        "INT8[]" | "BIGINT[]" => "_int8",
        "TEXT[]" | "VARCHAR[]" | "CHARACTER VARYING[]" => "_text",
        "FLOAT8[]" | "DOUBLE PRECISION[]" | "FLOAT[]" => "_float8",
        "BOOL[]" | "BOOLEAN[]" => "_bool",
        "UUID[]" => "_uuid",
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
/// Parse `INSERT INTO <table> [(cols)] VALUES (v1, v2, ...)[, (v1, v2, ...)]*`.
///
/// Returns `(table, cols, rows)` where `rows` has one entry per parenthesized
/// VALUES tuple (v0.42.2: multi-row `VALUES` lists are supported — each row
/// becomes its own entry instead of being silently mis-split). A malformed
/// row (wrong value count relative to the declared column list, or relative
/// to the first row when no column list is given) is a hard parse error.
/// Result of parsing an `INSERT` statement: `(table_name, col_names, rows)`.
type ParsedInsert = (String, Vec<String>, Vec<Vec<String>>);

fn parse_insert(q: &str) -> Result<ParsedInsert, String> {
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
    let cols: Vec<String> = if before_values_lower.starts_with('(') {
        let close = before_values_lower
            .rfind(')')
            .ok_or("missing ) in column list")?;
        let col_str = &before_values_lower[1..close];
        col_str.split(',').map(|c| c.trim().to_string()).collect()
    } else {
        vec![]
    };

    // Values list: one or more parenthesized row tuples after VALUES, e.g.
    // `VALUES (1, 'a'), (2, 'b'), (3, 'c')`. Each tuple's boundaries are
    // found by tracking paren depth and quote state so commas/parens inside
    // string literals never get mistaken for row/column separators. A
    // trailing ` RETURNING ...` clause is not part of the VALUES list, so it
    // is truncated off before splitting tuples.
    let after_values_full = rest_orig[values_pos_orig + 6..].trim_start();
    let after_values_full_lower = after_values_full.to_lowercase();
    let values_clause_end = after_values_full_lower
        .find(" returning ")
        .unwrap_or(after_values_full.len());
    let after_values = after_values_full[..values_clause_end].trim_end();
    let row_tuples = split_value_tuples(after_values)?;
    let rows: Vec<Vec<String>> = row_tuples
        .iter()
        .map(|tuple_str| parse_value_list(tuple_str))
        .collect();

    if !cols.is_empty() {
        for (i, row) in rows.iter().enumerate() {
            if row.len() != cols.len() {
                return Err(format!(
                    "[RS-2056] write.malformed_values_list: VALUES row {} has {} value(s) but {} column(s) were declared. next_steps: Check that every VALUES tuple has the same number of items as the column list.",
                    i + 1,
                    row.len(),
                    cols.len()
                ));
            }
        }
    } else if let Some(first_len) = rows.first().map(Vec::len) {
        for (i, row) in rows.iter().enumerate().skip(1) {
            if row.len() != first_len {
                return Err(format!(
                    "[RS-2056] write.malformed_values_list: VALUES row 1 has {} value(s) but row {} has {} value(s). next_steps: Every VALUES tuple in a multi-row INSERT must have the same number of items.",
                    first_len,
                    i + 1,
                    row.len()
                ));
            }
        }
    }

    Ok((table, cols, rows))
}

/// Split a `VALUES (...), (...), ...` clause (the text strictly after the
/// `VALUES` keyword) into the raw inner contents of each parenthesized row
/// tuple. Tracks paren depth and single-quote state so commas/parens inside
/// string literals never get mistaken for row separators, and returns a hard
/// parse error (rather than silently mis-splitting) on any malformed tuple —
/// fixing the v0.42.2 multi-row VALUES corruption bug.
fn split_value_tuples(s: &str) -> Result<Vec<String>, String> {
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut tuples = Vec::new();

    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }

    loop {
        if i >= chars.len() || chars[i] != '(' {
            return Err("[RS-2056] write.malformed_values_list: expected '(' to start a VALUES row tuple. next_steps: Ensure every VALUES row is wrapped in parentheses, e.g. VALUES (1, 'a'), (2, 'b').".to_string());
        }
        i += 1; // consume '('
        let tuple_start = i;
        let mut in_quote = false;
        let mut depth: u32 = 1;
        while i < chars.len() && depth > 0 {
            match chars[i] {
                '\'' if !in_quote => {
                    in_quote = true;
                    i += 1;
                }
                '\'' if in_quote => {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        i += 2;
                    } else {
                        in_quote = false;
                        i += 1;
                    }
                }
                '(' if !in_quote => {
                    depth += 1;
                    i += 1;
                }
                ')' if !in_quote => {
                    depth -= 1;
                    i += 1;
                }
                _ => i += 1,
            }
        }
        if depth != 0 {
            return Err("[RS-2056] write.malformed_values_list: unterminated VALUES row tuple (missing closing ')'). next_steps: Check that every VALUES tuple's parentheses are balanced.".to_string());
        }
        let inner: String = chars[tuple_start..i - 1].iter().collect();
        tuples.push(inner);

        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        if chars[i] == ';' {
            i += 1;
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            if i < chars.len() {
                let trailing: String = chars[i..].iter().collect();
                return Err(format!(
                    "[RS-2056] write.malformed_values_list: unexpected content after VALUES list: '{trailing}'. next_steps: Remove any text after the final ';' in the statement."
                ));
            }
            break;
        }
        if chars[i] == ',' {
            i += 1;
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            continue;
        }
        let trailing: String = chars[i..].iter().collect();
        return Err(format!(
            "[RS-2056] write.malformed_values_list: unexpected character after VALUES row tuple: '{trailing}'. next_steps: Separate multiple VALUES rows with a comma, e.g. VALUES (1, 'a'), (2, 'b')."
        ));
    }

    Ok(tuples)
}

/// Parse `UPDATE <table> SET col = val [, ...] WHERE col = val [RETURNING ...]`.
///
/// Returns `(table, set_pairs, where_pairs, returning_cols)`. `returning_cols`
/// is `None` when no `RETURNING` clause is present, `Some(vec!["*".into()])`
/// for `RETURNING *`, or `Some(cols)` for an explicit column list.
type ParsedUpdate = (
    String,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Option<Vec<String>>,
);

fn parse_update(q: &str) -> Result<ParsedUpdate, String> {
    let ql = q.to_lowercase();
    let after = ql.strip_prefix("update ").ok_or("not UPDATE")?.trim_start();
    let update_pos = ql.find("update ").ok_or("not UPDATE")?;
    let _orig_after = q.get(update_pos + 7..).unwrap_or("").trim_start();

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

    let rest_after_where = &q[where_pos + 7..];
    let (where_str, returning_cols) = split_returning_clause(rest_after_where)?;
    let where_pairs = parse_kv_list(where_str.trim_end_matches(';'));

    Ok((table, set_pairs, where_pairs, returning_cols))
}

/// Parse `DELETE FROM <table> WHERE col = val [RETURNING ...]`.
///
/// Returns `(table, where_pairs, returning_cols)` — see `parse_update`'s doc
/// comment for the `returning_cols` shape.
type ParsedDelete = (String, Vec<(String, String)>, Option<Vec<String>>);

fn parse_delete(q: &str) -> Result<ParsedDelete, String> {
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
    let rest_after_where = &q[where_pos + 7..];
    let (where_str, returning_cols) = split_returning_clause(rest_after_where)?;
    let where_pairs = parse_kv_list(where_str.trim_end_matches(';'));

    Ok((table, where_pairs, returning_cols))
}

/// Split a trailing ` RETURNING <col[, col...]>` or ` RETURNING *` clause off
/// the end of `s` (the text following `WHERE ...`), mirroring `parse_insert`'s
/// existing `RETURNING` handling for `INSERT`.
///
/// Returns `(remainder_without_returning, returning_cols)`. A `RETURNING`
/// keyword with no usable column list after it (empty, or containing an
/// empty/malformed column token) is a hard parse error: `RS-2022`
/// (`write.malformed_returning_clause`).
fn split_returning_clause(s: &str) -> Result<(&str, Option<Vec<String>>), String> {
    let sl = s.to_lowercase();
    let trimmed_end = sl.trim_end_matches(';').trim_end();

    let pos = if let Some(p) = sl.find(" returning ") {
        Some(p)
    } else if trimmed_end.ends_with(" returning") {
        // `RETURNING` is the very last keyword with nothing (or only
        // whitespace/`;`) after it — malformed (empty column list), not
        // "no RETURNING clause at all".
        Some(trimmed_end.len() - " returning".len())
    } else {
        None
    };
    let Some(pos) = pos else {
        return Ok((s, None));
    };

    let remainder = &s[..pos];
    let clause = s[pos..].trim_start();
    let clause_len = "returning".len();
    if clause.len() < clause_len || !clause[..clause_len].eq_ignore_ascii_case("returning") {
        return Err(malformed_returning_clause_err());
    }
    let clause = clause[clause_len..].trim().trim_end_matches(';').trim();

    if clause.is_empty() {
        return Err(malformed_returning_clause_err());
    }
    if clause == "*" {
        return Ok((remainder, Some(vec!["*".to_string()])));
    }
    let cols: Vec<String> = clause.split(',').map(|c| c.trim().to_string()).collect();
    if cols
        .iter()
        .any(|c| c.is_empty() || !c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
    {
        return Err(malformed_returning_clause_err());
    }
    Ok((remainder, Some(cols)))
}

fn malformed_returning_clause_err() -> String {
    "[RS-2022] write.malformed_returning_clause: RETURNING clause is malformed. \
     next_steps: Use RETURNING * or RETURNING <col>[, <col>...] with no trailing content."
        .to_string()
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
///
/// A bare (unquoted) `NULL` keyword is normalized to an empty string, the
/// same "no value" sentinel already used everywhere else in this row-store
/// (e.g. `value_map.get(col).cloned().unwrap_or_default()`) — otherwise the
/// literal 4-character text `"NULL"` would be stored and returned as if it
/// were a real value (visible as a bug in `encode_typed_field`'s `BOOL` arm,
/// where a stored `"NULL"` string doesn't parse-fail the way it does for
/// numeric/date types, and would decode as `false` instead of SQL `NULL`).
/// A *quoted* `'NULL'` string literal is left untouched, since that is a
/// genuine text value, not the NULL keyword.
fn parse_value_list(s: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut was_quoted = false;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let push_value = |values: &mut Vec<String>, current: &str, was_quoted: bool| {
        let trimmed = current.trim();
        if !was_quoted && trimmed.eq_ignore_ascii_case("null") {
            values.push(String::new());
        } else {
            values.push(trimmed.to_string());
        }
    };
    while i < chars.len() {
        match chars[i] {
            '\'' if !in_quote => {
                in_quote = true;
                was_quoted = true;
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
                push_value(&mut values, &current, was_quoted);
                current = String::new();
                was_quoted = false;
                i += 1;
            }
            c => {
                current.push(c);
                i += 1;
            }
        }
    }
    let last = current.trim().to_string();
    if !last.is_empty() || was_quoted {
        push_value(&mut values, &current, was_quoted);
    }
    values
}

#[cfg(test)]
// These legacy parser tests intentionally use unwrap/expect for concise fixture assertions.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod s4_tests {

    use super::*;
    use crate::auth::Principal;
    use crate::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
    use crate::view_reader::{ViewReadStrategy, ViewReader};
    use rockstream_ops::ColumnValue;
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

    #[tokio::test]
    async fn webhook_panic_does_not_block_peer_delivery_or_source_lifecycle() {
        let handler = make_handler();
        handler
            .handle_create_source(
                "CREATE SOURCE orders TYPE http_webhook (credential_ref='secret') FORMAT json",
            )
            .unwrap();
        let source = handler
            .webhook_sources
            .get("orders")
            .expect("CREATE SOURCE installs the webhook state")
            .value()
            .clone();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut source = source.lock();
            source.set_paused(false);
            panic!("injected webhook registry holder panic");
        }));
        assert!(panic.is_err());

        assert_eq!(
            handler
                .accept_webhook("orders", b"secret", Some("delivery-1"), br#"{}"#)
                .await,
            WebhookResult::Accepted
        );
        assert!(handler
            .handle_alter_source("ALTER SOURCE orders PAUSE")
            .is_ok());
        assert_eq!(
            handler
                .accept_webhook("orders", b"secret", Some("delivery-2"), br#"{}"#)
                .await,
            WebhookResult::Paused
        );
        assert!(handler
            .handle_alter_source("ALTER SOURCE orders RESUME")
            .is_ok());
        assert!(handler
            .handle_alter_source("ALTER SOURCE orders ADVANCE WATERMARK 7")
            .is_ok());
        assert!(handler.handle_alter_source("DROP SOURCE orders").is_ok());
        assert_eq!(
            handler
                .accept_webhook("orders", b"secret", Some("delivery-3"), br#"{}"#)
                .await,
            WebhookResult::NotFound
        );
    }

    #[test]
    fn compiled_view_state_preserves_full_row_multiplicity() {
        let row = vec![ColumnValue::Int64(1000), ColumnValue::Int64(2000)];
        let state = materialize_view_state(
            vec![
                (0, 0, row.clone(), 1),
                (0, 1, row.clone(), 1),
                (1, 0, row.clone(), -1),
            ],
            &full_row_pk(row.len()),
        );
        assert_eq!(state.len(), 1);
        assert_eq!(state.values().next(), Some(&(row, 1)));
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
            op_id: None,
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
            let mut session = handler.sessions.entry(conn_id.to_string()).or_default();
            session.current_namespace = "ns-a".to_string();
            session.principal = Principal::Jwt {
                sub: "alice".to_string(),
            };
        }

        // Try to read ns_b_view from ns-a session — should get RS-2402 error
        let responses = handler
            .read_view_response("ns_b_view", None, vec![], Some(conn_id))
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
            op_id: None,
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
            let mut session = handler.sessions.entry(conn_id.to_string()).or_default();
            session.current_namespace = "ns-a".to_string();
            session.principal = Principal::Jwt {
                sub: "carol".to_string(),
            };
        }

        // Try to read ns_b_admin_view from ns-a session — admin should succeed
        let responses = handler
            .read_view_response("ns_b_admin_view", None, vec![], Some(conn_id))
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
            let mut s = handler.sessions.entry(conn_id.to_string()).or_default();
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
            let mut s = handler.sessions.entry(conn_id.to_string()).or_default();
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

    fn create_sink_error_message(responses: Vec<Response<'_>>) -> String {
        responses
            .into_iter()
            .find_map(|response| match response {
                Response::Error(error) => Some(error.message),
                _ => None,
            })
            .unwrap_or_default()
    }

    #[test]
    fn create_sink_registers_catalog_entry() {
        let handler = make_handler();
        handler.catalog.add_view_in_namespace(CatalogView {
            name: "orders_view".to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            }],
            namespace: "public".to_string(),
            op_id: None,
        });

        let responses = handler
            .handle_create_sink("CREATE SINK daily_sink FOR VIEW orders_view TO ICEBERG 'file:///warehouse/orders' WITH (snapshot_interval_epochs=3, snapshot_interval_ms=500, parquet_row_group_bytes=1024, format_version=2, partition_by=ARRAY['region','day'], catalog=filesystem)")
            .unwrap();

        assert!(matches!(responses.as_slice(), [Response::Execution(_)]));

        let sink = handler
            .catalog
            .get_sink("daily_sink")
            .expect("sink registered");
        assert_eq!(sink.view, "orders_view");
        assert_eq!(sink.format, "ICEBERG");
        assert_eq!(sink.path, "file:///warehouse/orders");
        assert_eq!(sink.snapshot_interval_epochs, Some(3));
        assert_eq!(sink.snapshot_interval_ms, Some(500));
        assert_eq!(sink.parquet_row_group_bytes, Some(1024));
        assert_eq!(sink.format_version, Some(2));
        assert_eq!(
            sink.partition_by,
            vec!["region".to_string(), "day".to_string()]
        );
        assert_eq!(sink.catalog, "filesystem");
    }

    #[test]
    fn create_sink_unknown_view_returns_rs4007() {
        let handler = make_handler();
        let message = create_sink_error_message(
            handler
                .handle_create_sink("CREATE SINK missing_view_sink FOR VIEW missing_view TO ICEBERG 'file:///warehouse/orders' WITH (snapshot_interval_epochs=3, catalog=filesystem)")
                .unwrap(),
        );
        assert!(
            message.contains("RS-4007"),
            "expected RS-4007, got: {message}"
        );
        assert!(
            message.contains("unknown view"),
            "expected unknown view message, got: {message}"
        );
    }

    #[test]
    fn create_sink_bad_catalog_returns_rs4007() {
        let handler = make_handler();
        handler.catalog.add_view_in_namespace(CatalogView {
            name: "orders_view".to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![],
            namespace: "public".to_string(),
            op_id: None,
        });

        let message = create_sink_error_message(
            handler
                .handle_create_sink("CREATE SINK bad_catalog_sink FOR VIEW orders_view TO DELTA 'file:///warehouse/orders' WITH (snapshot_interval_epochs=3, catalog=bogus)")
                .unwrap(),
        );
        assert!(
            message.contains("RS-4007"),
            "expected RS-4007, got: {message}"
        );
        assert!(
            message.contains("catalog"),
            "expected catalog message, got: {message}"
        );
    }

    #[test]
    fn create_sink_malformed_with_clause_returns_rs4007() {
        let handler = make_handler();
        handler.catalog.add_view_in_namespace(CatalogView {
            name: "orders_view".to_string(),
            sql: "SELECT 1".to_string(),
            columns: vec![],
            namespace: "public".to_string(),
            op_id: None,
        });

        let message = create_sink_error_message(
            handler
                .handle_create_sink("CREATE SINK malformed_sink FOR VIEW orders_view TO ICEBERG 'file:///warehouse/orders' WITH (snapshot_interval_epochs='three', catalog=filesystem)")
                .unwrap(),
        );
        assert!(
            message.contains("RS-4007"),
            "expected RS-4007, got: {message}"
        );
        assert!(
            message.contains("snapshot_interval_epochs"),
            "expected option type message, got: {message}"
        );
    }
}

// ── v0.42.2: multi-row VALUES parsing ───────────────────────────────────────

#[cfg(test)]
// These parser tests intentionally use unwrap/expect for concise fixture assertions.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod parse_insert_tests {

    use super::*;

    /// v0.42.2 green gate: a multi-row `VALUES (...), (...), (...)` list is
    /// split into exactly one row per tuple, with no cross-row corruption.
    #[test]
    fn parse_insert_multi_row_values() {
        let (table, cols, rows) =
            parse_insert("INSERT INTO t (id, name) VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
        assert_eq!(table, "t");
        assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
                vec!["3".to_string(), "c".to_string()],
            ]
        );
    }

    /// Multi-row VALUES with a trailing semicolon and internal whitespace.
    #[test]
    fn parse_insert_multi_row_values_with_spacing_and_semicolon() {
        let (_, _, rows) =
            parse_insert("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b') ;").unwrap();
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
            ]
        );
    }

    /// A comma inside a quoted string literal must not be mistaken for a row
    /// separator, and a literal `)` inside a string must not end the tuple
    /// early.
    #[test]
    fn parse_insert_multi_row_values_with_commas_and_parens_in_strings() {
        let (_, _, rows) =
            parse_insert("INSERT INTO t (id, name) VALUES (1, 'a, b (c)'), (2, 'd''e')").unwrap();
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "a, b (c)".to_string()],
                vec!["2".to_string(), "d'e".to_string()],
            ]
        );
    }

    /// A single-row VALUES list (the pre-v0.42.2 supported case) still works.
    #[test]
    fn parse_insert_single_row_values_unchanged() {
        let (table, cols, rows) =
            parse_insert("INSERT INTO t (id, val) VALUES (1, 'hello')").unwrap();
        assert_eq!(table, "t");
        assert_eq!(cols, vec!["id".to_string(), "val".to_string()]);
        assert_eq!(rows, vec![vec!["1".to_string(), "hello".to_string()]]);
    }

    /// v0.42.2 green gate: a malformed multi-row VALUES list (wrong value
    /// count in one row against the declared column list) is a hard
    /// `RS-2056` parse error, not silent corruption.
    #[test]
    fn parse_insert_malformed_row_returns_rs2056() {
        let err =
            parse_insert("INSERT INTO t (id, name) VALUES (1,'a'),(2,'b','extra')").unwrap_err();
        assert!(
            err.contains("RS-2056"),
            "expected RS-2056 error, got: {err}"
        );
    }

    /// Same malformed-row check when no column list is given: every row must
    /// match the first row's arity.
    #[test]
    fn parse_insert_malformed_row_without_column_list_returns_rs2056() {
        let err = parse_insert("INSERT INTO t VALUES (1,'a'),(2)").unwrap_err();
        assert!(
            err.contains("RS-2056"),
            "expected RS-2056 error, got: {err}"
        );
    }

    /// A row missing its opening `(` is a hard parse error, not a panic or
    /// silent misparse.
    #[test]
    fn parse_insert_missing_open_paren_is_error() {
        let err = parse_insert("INSERT INTO t (id) VALUES 1, (2)").unwrap_err();
        assert!(
            err.contains("RS-2056"),
            "expected RS-2056 error, got: {err}"
        );
    }

    /// Unterminated VALUES tuple (missing closing paren) is a hard parse
    /// error.
    #[test]
    fn parse_insert_unterminated_tuple_is_error() {
        let err = parse_insert("INSERT INTO t (id) VALUES (1").unwrap_err();
        assert!(
            err.contains("RS-2056"),
            "expected RS-2056 error, got: {err}"
        );
    }
}

// ── v0.48: RETURNING clause parsing for UPDATE/DELETE ───────────────────────

#[cfg(test)]
// These parser tests intentionally use unwrap/expect for concise fixture assertions.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod parse_update_returning_tests {

    use super::*;

    /// v0.48 Slice A1 green gate: `UPDATE ... RETURNING *` parses, keeping
    /// SET/WHERE pairs intact.
    #[test]
    fn update_returning_star_parses() {
        let (table, set_pairs, where_pairs, returning) =
            parse_update("UPDATE t SET val = 'new' WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(table, "t");
        assert_eq!(set_pairs, vec![("val".to_string(), "new".to_string())]);
        assert_eq!(where_pairs, vec![("id".to_string(), "1".to_string())]);
        assert_eq!(returning, Some(vec!["*".to_string()]));
    }

    /// `UPDATE` with no `RETURNING` clause parses as before (`returning ==
    /// None`), and a trailing semicolon is still handled.
    #[test]
    fn update_without_returning_parses_none() {
        let (_, _, where_pairs, returning) =
            parse_update("UPDATE t SET val = 'new' WHERE id = 1;").unwrap();
        assert_eq!(where_pairs, vec![("id".to_string(), "1".to_string())]);
        assert_eq!(returning, None);
    }

    /// A malformed `RETURNING` clause (no columns after the keyword) is a
    /// hard `RS-2022` parse error.
    #[test]
    fn update_malformed_returning_clause_returns_rs2022() {
        let err = parse_update("UPDATE t SET val = 'new' WHERE id = 1 RETURNING").unwrap_err();
        assert!(
            err.contains("RS-2022"),
            "expected RS-2022 error, got: {err}"
        );
    }

    /// `RETURNING` followed by a dangling comma is also malformed.
    #[test]
    fn update_returning_trailing_comma_returns_rs2022() {
        let err = parse_update("UPDATE t SET val = 'new' WHERE id = 1 RETURNING id,").unwrap_err();
        assert!(
            err.contains("RS-2022"),
            "expected RS-2022 error, got: {err}"
        );
    }
}

#[cfg(test)]
// These parser tests intentionally use unwrap/expect for concise fixture assertions.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod parse_delete_returning_tests {

    use super::*;

    /// v0.48 Slice A1 green gate: `DELETE ... RETURNING <col list>` parses.
    #[test]
    fn delete_returning_column_list_parses() {
        let (table, where_pairs, returning) =
            parse_delete("DELETE FROM t WHERE id = 1 RETURNING id, val").unwrap();
        assert_eq!(table, "t");
        assert_eq!(where_pairs, vec![("id".to_string(), "1".to_string())]);
        assert_eq!(returning, Some(vec!["id".to_string(), "val".to_string()]));
    }

    /// `DELETE` with no `RETURNING` clause parses as before.
    #[test]
    fn delete_without_returning_parses_none() {
        let (_, where_pairs, returning) = parse_delete("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(where_pairs, vec![("id".to_string(), "1".to_string())]);
        assert_eq!(returning, None);
    }

    /// A malformed `RETURNING` clause on `DELETE` is also a hard `RS-2022`
    /// parse error.
    #[test]
    fn delete_malformed_returning_clause_returns_rs2022() {
        let err = parse_delete("DELETE FROM t WHERE id = 1 RETURNING").unwrap_err();
        assert!(
            err.contains("RS-2022"),
            "expected RS-2022 error, got: {err}"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod source_batch_tests {
    use super::*;

    struct NoopViewReader;

    #[async_trait]
    impl ViewReader for NoopViewReader {
        async fn read_view(
            &self,
            _view_name: &str,
            _limit: Option<usize>,
            _strategy: ViewReadStrategy,
        ) -> Result<Vec<Vec<u8>>, GatewayError> {
            Ok(Vec::new())
        }

        fn published_frontier(&self) -> Option<u64> {
            None
        }
    }

    #[test]
    fn source_batch_preserves_exact_insert_and_delete_preimages() {
        use datafusion::arrow::array::{Int64Array, StringArray};

        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![7, 8])) as datafusion::arrow::array::ArrayRef,
            ),
            (
                "customer",
                Arc::new(StringArray::from(vec!["ada", "bea"])) as _,
            ),
        ])
        .unwrap();
        let batch = rockstream_types::arrow_batch::append_weight_column(batch, &[1, -1]).unwrap();
        let ops = source_batch_to_dml_ops(
            "orders",
            &[
                CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                CatalogColumn {
                    name: "customer".to_string(),
                    data_type: "Utf8".to_string(),
                },
            ],
            &batch,
        )
        .unwrap();

        assert_eq!(ops.len(), 2);
        match &ops[0] {
            DmlOp::Insert {
                table,
                cols,
                values_tsv,
                row_key,
            } => assert_eq!(
                (table, cols, values_tsv, row_key),
                (
                    &"orders".to_string(),
                    &vec!["id".to_string(), "customer".to_string()],
                    &"7\tada".to_string(),
                    &"id=7|customer=ada".to_string(),
                )
            ),
            _ => panic!("positive source weight must insert"),
        }
        match &ops[1] {
            DmlOp::Delete {
                table,
                row_key,
                returning_tsv,
            } => assert_eq!(
                (table, row_key, returning_tsv),
                (
                    &"orders".to_string(),
                    &"id=8|customer=bea".to_string(),
                    &Some("8\tbea".to_string()),
                )
            ),
            _ => panic!("negative source weight must retain a delete preimage"),
        }
    }

    #[tokio::test]
    async fn source_snapshot_publishes_output_cursor_and_frontier_in_m3() {
        let shard_db = Arc::new(
            rockstream_storage::ShardDb::builder(
                "source-snapshot-m3",
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::new());
        assert!(catalog.add_table(CatalogTable {
            name: "orders".to_string(),
            columns: vec![
                CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                CatalogColumn {
                    name: "amount".to_string(),
                    data_type: "Int64".to_string(),
                },
            ],
        }));
        let handler = GatewayHandler::with_shard_db(
            Arc::clone(&catalog),
            Arc::new(NoopViewReader),
            Arc::clone(&shard_db),
        );
        handler
            .handle_create_view(
                "CREATE MATERIALIZED VIEW order_rows AS SELECT id, amount FROM orders",
            )
            .await
            .unwrap();
        assert!(catalog.add_source(CatalogSourceEntry {
            name: "orders".to_string(),
            table_name: Some("orders".to_string()),
            source_type: "s3".to_string(),
            options: HashMap::new(),
            format: "json".to_string(),
            status: "OK".to_string(),
            live_offset: "0".to_string(),
            live_lag: 0,
        }));
        catalog.begin_backfill("order_rows", 2);

        let connector_id = ConnectorId(99);
        let mut source = S3Source::new(
            connector_id,
            catalog_columns_to_schema(&catalog.get_table("orders").unwrap().columns),
        );
        source.add_file("snapshot.json".to_string(), vec![vec![1, 10], vec![2, 20]]);
        handler
            .backfill_bound_source(
                "orders",
                "order_rows",
                SourceRuntimeCoordinator::new(
                    source,
                    connector_id,
                    OffsetToken::new(Vec::new()),
                    SourceCheckpointStore::new(Arc::clone(&shard_db), 99, connector_id),
                ),
                true,
                &shard_db,
            )
            .await
            .unwrap();

        let view = catalog.get_view("order_rows").unwrap();
        assert_eq!(
            handler
                .read_compiled_view_rows("order_rows", &view, &shard_db)
                .await
                .unwrap(),
            vec![b"1\t10".to_vec(), b"2\t20".to_vec()]
        );
        let lifecycle = SourceCheckpointStore::new(Arc::clone(&shard_db), 99, connector_id)
            .backfill_lifecycle("order_rows")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            lifecycle,
            BackfillLifecycle::new(
                BackfillPhase::Running,
                BackfillCursor::new(
                    "order_rows",
                    0,
                    serde_json::to_vec(&vec![("snapshot.json".to_string(), 2_usize)]).unwrap(),
                    SnapshotDeltaFence::new(
                        OffsetToken::new(
                            serde_json::to_vec(&vec![("snapshot.json".to_string(), 2_usize)])
                                .unwrap(),
                        ),
                        OffsetToken::new(
                            serde_json::to_vec(&vec![("snapshot.json".to_string(), 2_usize)])
                                .unwrap(),
                        ),
                    ),
                    2,
                ),
                0,
                2,
                0,
                Some(2),
            )
        );
        assert_eq!(
            shard_db
                .get(&rockstream_storage::ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .as_deref(),
            Some(&2_u64.to_be_bytes()[..])
        );
    }

    #[tokio::test]
    async fn source_backfill_uses_the_configured_snapshot_batch_bound() {
        let shard_db = Arc::new(
            rockstream_storage::ShardDb::builder(
                "source-snapshot-bounded-m3",
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::new());
        assert!(catalog.add_table(CatalogTable {
            name: "orders".to_string(),
            columns: vec![
                CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                CatalogColumn {
                    name: "amount".to_string(),
                    data_type: "Int64".to_string(),
                },
            ],
        }));
        let handler = GatewayHandler::with_shard_db(
            Arc::clone(&catalog),
            Arc::new(NoopViewReader),
            Arc::clone(&shard_db),
        );
        handler
            .handle_create_view(
                "CREATE MATERIALIZED VIEW order_rows AS SELECT id, amount FROM orders",
            )
            .await
            .unwrap();
        assert!(catalog.add_source(CatalogSourceEntry {
            name: "orders".to_string(),
            table_name: Some("orders".to_string()),
            source_type: "s3".to_string(),
            options: HashMap::new(),
            format: "json".to_string(),
            status: "OK".to_string(),
            live_offset: "0".to_string(),
            live_lag: 0,
        }));
        catalog.begin_backfill("order_rows", BACKFILL_BATCH_MAX_ROWS as u64 + 1);

        let connector_id = ConnectorId(101);
        let rows = (0..=BACKFILL_BATCH_MAX_ROWS as i64)
            .map(|id| vec![id, id * 10])
            .collect::<Vec<_>>();
        let mut source = S3Source::new(
            connector_id,
            catalog_columns_to_schema(&catalog.get_table("orders").unwrap().columns),
        );
        source.add_file("snapshot.json".to_string(), rows);
        handler
            .backfill_bound_source(
                "orders",
                "order_rows",
                SourceRuntimeCoordinator::new(
                    source,
                    connector_id,
                    OffsetToken::new(Vec::new()),
                    SourceCheckpointStore::new(Arc::clone(&shard_db), 101, connector_id),
                ),
                true,
                &shard_db,
            )
            .await
            .unwrap();

        let view = catalog.get_view("order_rows").unwrap();
        let mut actual = handler
            .read_compiled_view_rows("order_rows", &view, &shard_db)
            .await
            .unwrap();
        let mut expected = (0..=BACKFILL_BATCH_MAX_ROWS as i64)
            .map(|id| format!("{id}\t{}", id * 10).into_bytes())
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        let lifecycle = SourceCheckpointStore::new(Arc::clone(&shard_db), 101, connector_id)
            .backfill_lifecycle("order_rows")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                lifecycle.phase,
                lifecycle.cursor.last_key,
                lifecycle.cursor.committed_epoch,
                lifecycle.published_frontier,
            ),
            (
                BackfillPhase::Running,
                serde_json::to_vec(&vec![(
                    "snapshot.json".to_string(),
                    BACKFILL_BATCH_MAX_ROWS + 1
                )])
                .unwrap(),
                3,
                Some(3),
            )
        );
    }

    #[tokio::test]
    async fn source_snapshot_restart_resumes_at_committed_cursor_without_replay() {
        let shard_db = Arc::new(
            rockstream_storage::ShardDb::builder(
                "source-snapshot-restart",
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::new());
        assert!(catalog.add_table(CatalogTable {
            name: "orders".to_string(),
            columns: vec![
                CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                CatalogColumn {
                    name: "amount".to_string(),
                    data_type: "Int64".to_string(),
                },
            ],
        }));
        let handler = GatewayHandler::with_shard_db(
            Arc::clone(&catalog),
            Arc::new(NoopViewReader),
            Arc::clone(&shard_db),
        );
        handler
            .handle_create_view(
                "CREATE MATERIALIZED VIEW order_rows AS SELECT id, amount FROM orders",
            )
            .await
            .unwrap();
        assert!(catalog.add_source(CatalogSourceEntry {
            name: "orders".to_string(),
            table_name: Some("orders".to_string()),
            source_type: "s3".to_string(),
            options: HashMap::new(),
            format: "json".to_string(),
            status: "OK".to_string(),
            live_offset: "0".to_string(),
            live_lag: 0,
        }));
        catalog.begin_backfill("order_rows", BACKFILL_BATCH_MAX_ROWS as u64 + 1);

        let connector_id = ConnectorId(100);
        let schema = catalog_columns_to_schema(&catalog.get_table("orders").unwrap().columns);
        let mut source = S3Source::new(connector_id, schema.clone());
        source.add_file(
            "snapshot.json".to_string(),
            (0..=BACKFILL_BATCH_MAX_ROWS as i64)
                .map(|id| vec![id, id * 10])
                .collect(),
        );
        let checkpoint_store = SourceCheckpointStore::new(Arc::clone(&shard_db), 100, connector_id);
        let mut runtime = SourceRuntimeCoordinator::new(
            source,
            connector_id,
            OffsetToken::new(Vec::new()),
            checkpoint_store,
        );
        runtime.recover().await.unwrap();
        let lease = runtime.acquire_owner("gateway:order_rows").unwrap();
        let fence = runtime.capture_snapshot_delta_fence().await.unwrap();
        let chunk = runtime
            .start_snapshot(&fence, None, BACKFILL_BATCH_MAX_ROWS)
            .await
            .unwrap()
            .next()
            .unwrap();
        handler
            .commit_bound_source_batch(
                &mut runtime,
                &lease,
                "order_rows",
                &catalog.get_table("orders").unwrap(),
                &fence,
                chunk.resume_offset,
                &chunk.batch,
                BackfillPhase::Snapshotting,
                None,
                1,
                BACKFILL_BATCH_MAX_ROWS as u64 + 1,
                &shard_db,
            )
            .await
            .unwrap();
        let CatalogResponse::Rows { columns, rows } =
            catalog.backfill_status_response("order_rows")
        else {
            panic!("backfill status must be tabular");
        };
        assert_eq!(
            (columns, rows),
            (
                vec![
                    "view_name".to_string(),
                    "phase".to_string(),
                    "cursor_position".to_string(),
                    "rows_remaining".to_string(),
                    "estimated_rows".to_string(),
                    "budget_state".to_string(),
                    "blocked_reason".to_string(),
                ],
                vec![vec![
                    Some("order_rows".to_string()),
                    Some("SNAPSHOTTING".to_string()),
                    Some("1".to_string()),
                    Some("1".to_string()),
                    Some((BACKFILL_BATCH_MAX_ROWS + 1).to_string()),
                    Some("ADMITTED".to_string()),
                    None,
                ]],
            )
        );

        let restarted = GatewayHandler::with_shard_db(
            Arc::clone(&catalog),
            Arc::new(NoopViewReader),
            Arc::clone(&shard_db),
        );
        restarted.recover_compiled_views().await;
        let mut resumed_source = S3Source::new(connector_id, schema);
        resumed_source.add_file(
            "snapshot.json".to_string(),
            (0..=BACKFILL_BATCH_MAX_ROWS as i64)
                .map(|id| vec![id, id * 10])
                .collect(),
        );
        restarted
            .backfill_bound_source(
                "orders",
                "order_rows",
                SourceRuntimeCoordinator::new(
                    resumed_source,
                    connector_id,
                    OffsetToken::new(Vec::new()),
                    SourceCheckpointStore::new(Arc::clone(&shard_db), 100, connector_id),
                ),
                true,
                &shard_db,
            )
            .await
            .unwrap();

        let view = catalog.get_view("order_rows").unwrap();
        let mut actual = restarted
            .read_compiled_view_rows("order_rows", &view, &shard_db)
            .await
            .unwrap();
        let mut expected = (0..=BACKFILL_BATCH_MAX_ROWS as i64)
            .map(|id| format!("{id}\t{}", id * 10).into_bytes())
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        let lifecycle = SourceCheckpointStore::new(Arc::clone(&shard_db), 100, connector_id)
            .backfill_lifecycle("order_rows")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                lifecycle.phase,
                lifecycle.cursor.last_key,
                lifecycle.cursor.committed_epoch,
                lifecycle.rows_remaining,
                lifecycle.estimated_rows,
                lifecycle.published_frontier,
            ),
            (
                BackfillPhase::Running,
                serde_json::to_vec(&vec![(
                    "snapshot.json".to_string(),
                    BACKFILL_BATCH_MAX_ROWS + 1
                )])
                .unwrap(),
                3,
                0,
                BACKFILL_BATCH_MAX_ROWS as u64 + 1,
                Some(3),
            )
        );
    }
}
