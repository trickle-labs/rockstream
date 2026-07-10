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
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use futures::SinkExt;
use futures::{stream, Sink, StreamExt};
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
use pgwire::messages::response::{CommandComplete, EmptyQueryResponse};
use pgwire::messages::startup::BackendKeyData;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use base64::Engine as _;

use crate::auth::{
    scram_server_key, scram_server_signature, scram_stored_key, verify_client_proof, AuthMode,
    JwtVerifier, Principal,
};
use crate::catalog_stubs::{
    arrow_type_to_pg_oid, CatalogColumn, CatalogResponse, CatalogStubs, CatalogTable,
};
use crate::copy_state::{
    CopyState, COPY_IN_BUFFER_ROWS, COPY_IN_FLUSH_BYTES, MAX_COPY_IN_BATCH_ROWS,
};
use crate::notify_registry::NotifyRegistry;
use crate::role_catalog::RoleCatalog;
use crate::session::{FreshnessToken, ScramAuthState, SessionState};
use crate::view_reader::{ViewReadStrategy, ViewReader};
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
                                    if name.starts_with('$') {
                                        if let Ok(idx) = name[1..].parse::<usize>() {
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
                                if name.starts_with('$') {
                                    if let Ok(idx) = name[1..].parse::<usize>() {
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
                            match &func.args {
                                FunctionArguments::List(arg_list) => {
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
                                _ => {}
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
            match stmt {
                sqlparser::ast::Statement::Query(q) => {
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
                _ => {}
            }
        }
    }

    for (idx, ty) in &explicit_casts {
        if *idx > 0 && *idx <= max_idx {
            inferred_types[*idx - 1] = ty.clone();
        }
    }

    let ctx = SessionContext::new();
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
                        if id.starts_with('$') {
                            if let Ok(idx) = id[1..].parse::<usize>() {
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
        if idx > 0 && idx <= max_idx {
            if !explicit_casts.contains_key(&idx) {
                inferred_types[idx - 1] = ty;
            }
        }
    }

    inferred_types
}

fn string_to_arrow_datatype(dt: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
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

#[derive(Debug, Clone)]
pub struct PortalState {
    pub rows: Vec<pgwire::messages::data::DataRow>,
    pub schema: Arc<Vec<FieldInfo>>,
    pub command_tag: String,
    pub offset: usize,
}

// ── GatewayHandler ────────────────────────────────────────────────────────────

/// Core handler shared across all pgwire protocol phases.
///
/// `Arc<GatewayHandler>` is the `PgWireServerHandlers` factory.
pub struct GatewayHandler {
    catalog: Arc<CatalogStubs>,
    view_reader: Arc<dyn ViewReader>,
    query_parser: Arc<PreparedStatementCache>,
    prepared_statements: Arc<DashMap<String, std::collections::HashSet<String>>>,
    active_portals: Arc<DashMap<String, std::collections::HashSet<String>>>,
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
}

impl GatewayHandler {
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
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            role_catalog: Arc::new(RoleCatalog::new()),
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
            cancellation_registry: Arc::new(DashMap::new()),
            notify_registry: Arc::new(NotifyRegistry::new()),
            pending_notifies: Arc::new(DashMap::new()),
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
            auth_mode: AuthMode::Off,
            jwt_verifier: None,
            role_catalog: Arc::new(RoleCatalog::new()),
            acl_store: Arc::new(rockstream_control::AclStore::new()),
            namespace_catalog: Arc::new(rockstream_control::NamespaceCatalog::new()),
            audit_log: None,
            cancellation_registry: Arc::new(DashMap::new()),
            notify_registry: Arc::new(NotifyRegistry::new()),
            pending_notifies: Arc::new(DashMap::new()),
        }
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
                    stmts.remove(stmt_name);
                }
            }
            return Ok(vec![Response::Execution(Tag::new("DEALLOCATE"))]);
        }

        self.dispatch_async_with_conn(query, Some(conn_id)).await
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
    fn dispatch_sync<'a>(
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
                "[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED or REPEATABLE READ".to_owned(),
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
            return Some(self.handle_create_view(q));
        }

        // REFRESH MATERIALIZED VIEW
        if ql.starts_with("refresh materialized view ") {
            return Some(Ok(self.handle_refresh_materialized_view(q)));
        }

        // CREATE TABLE [IF NOT EXISTS] — register in catalog
        if ql.starts_with("create table ") || ql.starts_with("create table if not exists ") {
            return Some(self.handle_create_table(q));
        }

        // CREATE INDEX / DROP INDEX / REBUILD INDEX / MARK INDEX READY — v0.32 pgwire DDL wiring
        if ql.starts_with("create index ") {
            return Some(self.handle_create_index(q));
        }
        if ql.starts_with("drop index ") {
            return Some(self.handle_drop_index(q));
        }
        if ql.starts_with("rebuild index ") {
            return Some(self.handle_rebuild_index(q));
        }
        if ql.starts_with("mark index ") {
            return Some(self.handle_mark_index_ready(q));
        }

        // BEGIN is handled in dispatch_async_with_conn (needs session state for idempotency).

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
        }

        // ── BEGIN — with idempotency (already in transaction → silent succeed) ──
        if ql == "begin" || ql == "begin;" || ql.starts_with("begin ") {
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
                let mut session = self
                    .sessions
                    .entry(id.to_string())
                    .or_insert_with(SessionState::new);
                session.begin_explicit();
                let current_frontier = self
                    .shard_db
                    .as_ref()
                    .map(|db| db.last_epoch().load(std::sync::atomic::Ordering::Acquire));
                session.begin(current_frontier);
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
                let mut session = self
                    .sessions
                    .entry(id.to_string())
                    .or_insert_with(SessionState::new);
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

            let plan_text =
                format!("Plan: SeqScan → {pushdown_note}{index_note}\nQuery: {inner_sql}");
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

        // S6: SET TRANSACTION ISOLATION LEVEL / SET TRANSACTION READ ONLY|WRITE
        if ql.starts_with("set transaction") || ql.starts_with("set local transaction") {
            if ql.contains("isolation level") {
                if ql.contains("serializable") {
                    // Fall through to dispatch_sync which returns RS-2003.
                } else if let Some(id) = conn_id {
                    let level = if ql.contains("repeatable read") {
                        crate::session::IsolationLevel::RepeatableRead
                    } else {
                        crate::session::IsolationLevel::ReadCommitted
                    };
                    let mut session = self
                        .sessions
                        .entry(id.to_string())
                        .or_insert_with(SessionState::new);
                    session.isolation_level = level;
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
                    let mut session = self
                        .sessions
                        .entry(id.to_string())
                        .or_insert_with(SessionState::new);
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
        if ql.starts_with("show ") {
            let key_raw = ql["show ".len()..].trim().trim_end_matches(';').to_string();
            let session_val: Option<String> = if let Some(id) = conn_id {
                self.sessions.get(id).map(|s| {
                    // local_guc_params first (SET LOCAL), then guc_params, then session fields
                    if let Some(v) = s.effective_guc(&key_raw) {
                        return v.to_owned();
                    }
                    match key_raw.as_str() {
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

        if let Some(result) = self.dispatch_sync(query, &session_info) {
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

                    // Try index-accelerated point lookup before falling back to full scan.
                    if let Some(responses) = self.maybe_index_point_lookup(q, &view_name).await? {
                        return Ok(responses);
                    }
                    let limit = extract_limit(q);
                    let order_by = extract_order_by(q);
                    // Wrap read in cancellation select (Slice 2)
                    let mut cancel_token = CANCEL_TOKEN
                        .try_with(|t| t.clone())
                        .unwrap_or_else(|_| CancelToken::new());
                    let read_fut = self.read_view_response(&view_name, limit, order_by, conn_id);
                    let result = tokio::select! {
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

        // DataFusion execution path for literal SELECT queries (no recognized FROM clause).
        // Handles queries like `SELECT 42`, `SELECT 42 AS n`, `SELECT now()`, etc.
        if ql.starts_with("select ") {
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
        use datafusion::arrow::array::{
            Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            StringArray,
        };
        use datafusion::arrow::datatypes::DataType as ArrowDataType;

        // Build a DataFusion session and register catalog objects as empty MemTables
        // so the planner can resolve any referenced names.
        let ctx = SessionContext::new();
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

        let batches = match df.collect().await {
            Ok(b) => b,
            Err(_) => return None,
        };

        if batches.is_empty() {
            // Return an empty result set — build schema from the first batch schema if any.
            return None;
        }

        // Build FieldInfo list from the Arrow schema of the first batch.
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

        // Collect all rows from all batches into a flat list of string-encoded rows.
        let mut encoded_rows: Vec<Vec<Option<String>>> = Vec::new();
        for batch in &batches {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            for row_idx in 0..num_rows {
                let mut row_vals: Vec<Option<String>> = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
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
                        _ => {
                            // Fallback: cast to StringArray or use debug representation.
                            col.as_any()
                                .downcast_ref::<StringArray>()
                                .map(|a| a.value(row_idx).to_string())
                        }
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
                let encode_res = match *datatype {
                    Type::INT2 => {
                        let parsed: Option<i16> = val.as_deref().and_then(|s| s.parse().ok());
                        encoder.encode_field(&parsed)
                    }
                    Type::INT4 => {
                        let parsed: Option<i32> = val.as_deref().and_then(|s| s.parse().ok());
                        encoder.encode_field(&parsed)
                    }
                    Type::INT8 => {
                        let parsed: Option<i64> = val.as_deref().and_then(|s| s.parse().ok());
                        encoder.encode_field(&parsed)
                    }
                    Type::FLOAT4 => {
                        let parsed: Option<f32> = val.as_deref().and_then(|s| s.parse().ok());
                        encoder.encode_field(&parsed)
                    }
                    Type::FLOAT8 => {
                        let parsed: Option<f64> = val.as_deref().and_then(|s| s.parse().ok());
                        encoder.encode_field(&parsed)
                    }
                    Type::BOOL => {
                        let parsed: Option<bool> =
                            val.as_deref().map(|s| s == "t" || s == "true" || s == "1");
                        encoder.encode_field(&parsed)
                    }
                    _ => {
                        let s: Option<&str> = val.as_deref();
                        encoder.encode_field(&s)
                    }
                };
                if let Err(e) = encode_res {
                    return Err(PgWireError::ApiError(Box::new(e)));
                }
            }
            encoder.finish()
        });

        Some(vec![Response::Query(QueryResponse::new(
            schema,
            data_stream,
        ))])
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
                let val: Option<&str> = fields.get(i).copied();
                let datatype = schema_ref[i].datatype();
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
                        let parsed = val.map(|s| s == "t" || s == "true" || s == "1");
                        encoder.encode_field(&parsed)
                    }
                    _ => encoder.encode_field(&val),
                };
                encode_res.map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            }
            encoder.finish()
        });

        Ok(Some(vec![Response::Query(QueryResponse::new(
            schema,
            data_stream,
        ))]))
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
            let prefix = format!("view_output/{view_name}/");
            let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.map_err(|e| {
                PgWireError::ApiError(Box::new(crate::error::GatewayError::Storage(e)))
            })?;
            let mut rows: Vec<Vec<u8>> = kvs.into_iter().map(|(_, v)| v.to_vec()).collect();
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
                let val: Option<&str> = fields.get(i).copied();
                let datatype = schema_ref[i].datatype();
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
                        let parsed = val.map(|s| s == "t" || s == "true" || s == "1");
                        encoder.encode_field(&parsed)
                    }
                    _ => encoder.encode_field(&val),
                };
                encode_res.map_err(|e| PgWireError::ApiError(Box::new(e)))?;
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

    /// Handle `CREATE INDEX <name> ON <table> (<col>, ...) [WHERE <pred>]` — v0.32.
    ///
    /// Registers the index in `Building` state in the gateway catalog stubs.
    /// Returns RS-2016 if an index with the same name exists for a different table.
    fn handle_create_index<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
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
            index_cols,
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

        Ok(vec![Response::Execution(
            Tag::new("CREATE INDEX").with_rows(0),
        )])
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
    fn handle_mark_index_ready<'a>(&'a self, q: &str) -> PgWireResult<Vec<Response<'a>>> {
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

        Ok(vec![Response::Execution(
            Tag::new("MARK INDEX").with_rows(0),
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
            crate::view_materializer::materialize_views(&self.catalog, shard_db, &changed_tables)
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
    /// source_epoch_envelope, wait_for_token, pinned_frontier, TxStatus → Idle.
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
                session.pinned_frontier = None;
                session.tx_status = crate::session::TxStatus::Idle;
                session.search_path = "public".to_string();
                session.current_namespace = "public".to_string();
                session.isolation_level = crate::session::IsolationLevel::ReadCommitted;
                session.session_wait_for_timeout_ms = 5_000;
                session.session_wait_for_enabled = true;
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
            let session = self
                .sessions
                .entry(conn_id_str.to_string())
                .or_insert_with(SessionState::new);
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
            let session = self
                .sessions
                .entry(conn_id_str.to_string())
                .or_insert_with(SessionState::new);
            if session.cursors.contains_key(&cursor_name) {
                return Ok(vec![promote_response(Response::Error(Box::new(
                    ErrorInfo::new("ERROR".to_string(), "42P03".to_string(),
                        format!("[RS-2052] cursor.already_exists: cursor '{cursor_name}' already exists. next_steps: CLOSE the existing cursor or use a different name.")),
                )))]);
            }
        }

        // Execute the inner query to collect rows
        let rows: Vec<Vec<u8>> = if let Some(view_name) = extract_view_name_from_select(inner_sql) {
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
            let mut session = self
                .sessions
                .entry(conn_id_str.to_string())
                .or_insert_with(SessionState::new);
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
        q: &str,
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
            let count_part = if count_part.starts_with("forward ") {
                count_part["forward ".len()..].trim()
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

        let (start, end, fetched_rows) = match cursor_data {
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

        let n_fetched = fetched_rows.len();

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
        q: &str,
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
            let count_part = if count_part.starts_with("forward ") {
                count_part["forward ".len()..].trim()
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
        q: &str,
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

        let mut returning_rows: Vec<Vec<String>> = Vec::with_capacity(rows.len());
        for values in &rows {
            // Build row_key: deterministic from col=val pairs
            let row_key = build_row_key(&cols, values);
            let values_tsv = values.join("\t");

            let op = DmlOp::Insert {
                table: table.clone(),
                cols: cols.clone(),
                values_tsv,
                row_key,
            };

            if let Some(id) = conn_id {
                let mut entry = self.write_buffers.entry(id.to_string()).or_default();
                if let Err(e) = entry.push(op) {
                    return Ok(vec![promote_response(Response::Error(Box::new(
                        ErrorInfo::new("ERROR".to_owned(), "53400".to_owned(), e.to_string()),
                    )))]);
                }
            }
            returning_rows.push(values.clone());
        }

        if returning {
            // Auto-commit INSERT … RETURNING outside explicit transaction;
            // every inserted row (one per VALUES tuple) is returned.
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
                AuthMode::Scram => {
                    if let Ok(conn_id) = CONN_ID.try_with(|id| id.clone()) {
                        client
                            .metadata_mut()
                            .insert("_rs_conn_id".to_string(), conn_id.clone());
                        {
                            let mut session = self
                                .sessions
                                .entry(conn_id.clone())
                                .or_insert_with(SessionState::new);
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
                            let mut session = self
                                .sessions
                                .entry(conn_id.clone())
                                .or_insert_with(SessionState::new);
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
            if let Some(conn_id) = CONN_ID.try_with(|id| id.clone()).ok() {
                client
                    .metadata_mut()
                    .insert("_rs_conn_id".to_string(), conn_id.clone());
                let (pid, secret) = {
                    let mut session = self
                        .sessions
                        .entry(conn_id.clone())
                        .or_insert_with(SessionState::new);
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
                if let Some(params) =
                    GatewayServerParameterProvider::default().server_parameters(client)
                {
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
                finish_authentication(client, &GatewayServerParameterProvider::default()).await?;
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
                                GatewayServerParameterProvider::default().server_parameters(client)
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
                    if let Some(params) =
                        GatewayServerParameterProvider::default().server_parameters(client)
                    {
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
                let coerced: Vec<Response<'a>> = all_responses.into_iter().map(|r| r).collect();
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

        // Check prepared statements limit
        {
            let mut conn_stmts = self.prepared_statements.entry(conn_id.clone()).or_default();
            if conn_stmts.len() >= 1000 && !conn_stmts.contains(&stmt_name) {
                let err: PgWireError =
                    GatewayError::PreparedStatementsLimitExceeded { limit: 1000 }.into();
                return Err(err);
            }
            conn_stmts.insert(stmt_name.clone());
            PREPARED_STATEMENTS_COUNT.fetch_add(1, Ordering::Relaxed);
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

        // Check portals limit
        {
            let mut conn_portals = self.active_portals.entry(conn_id.clone()).or_default();
            if conn_portals.len() >= 1000 && !conn_portals.contains(&portal_name) {
                let err: PgWireError = GatewayError::PortalsLimitExceeded { limit: 1000 }.into();
                return Err(err);
            }
            conn_portals.insert(portal_name.clone());
            PORTALS_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        let statement_name = message
            .statement_name
            .as_deref()
            .unwrap_or(pgwire::api::DEFAULT_NAME);

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
                    if stmts.remove(name) {
                        PREPARED_STATEMENTS_COUNT.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store().rm_portal(name);
                if let Some(mut portals) = self.active_portals.get_mut(&conn_id) {
                    if portals.remove(name) {
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

                let (rows, schema, command_tag, offset) = if let Some(state) = cached {
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
                for i in offset..end {
                    client
                        .feed(PgWireBackendMessage::DataRow(rows[i].clone()))
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
        GatewayServer {
            addr,
            handler: Arc::new(handler),
        }
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
        GatewayServer {
            addr,
            handler: Arc::new(handler),
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
            handler: self.handler.clone(),
        });
        let registry = self.handler.cancellation_registry.clone();
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Gateway listening on {}", self.addr);
        loop {
            let (socket, _peer) = listener.accept().await?;
            let factory_ref = factory.clone();
            let registry_ref = registry.clone();
            use rand::Rng;
            let conn_id = format!("{:032x}", rand::thread_rng().gen::<u128>());
            let cancel_token = CancelToken::new();
            let token_for_task = cancel_token.clone();
            tokio::spawn(CANCEL_TOKEN.scope(
                token_for_task,
                CONN_ID.scope(conn_id, async move {
                    let mut socket = socket;
                    let mut buf = [0u8; 16];
                    if let Ok(n) = socket.peek(&mut buf).await {
                        // SSLRequest: [0,0,0,8, 4,210,22,47]
                        if n >= 8 && buf[..8] == [0, 0, 0, 8, 4, 210, 22, 47] {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut ssl_buf = [0u8; 8];
                            let _ = socket.read_exact(&mut ssl_buf).await;
                            let _ = socket.write_all(b"N").await;
                            let _ = socket.flush().await;
                        }
                        // CancelRequest: [0,0,0,16, 4,210,22,46, pid(4), secret(4)]
                        else if n >= 16 && buf[..8] == [0, 0, 0, 16, 4, 210, 22, 46] {
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
                    if let Err(e) =
                        pgwire::tokio::process_socket(socket, None, factory_ref.clone()).await
                    {
                        tracing::debug!("gateway connection error: {e}");
                    }
                    // Cleanup LISTEN subscriptions on disconnect.
                    let cid = CONN_ID.with(|id| id.clone());
                    factory_ref.handler.notify_registry.unsubscribe_all(&cid);
                    factory_ref.handler.pending_notifies.remove(&cid);
                }),
            ));
        }
    }

    /// Bind to `addr`, return the actual local address (useful for port 0 tests),
    /// and serve connections in a background task.
    pub async fn serve_background(
        self,
    ) -> std::io::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
        let factory = Arc::new(GatewayHandlerFactory {
            handler: self.handler.clone(),
        });
        let registry = self.handler.cancellation_registry.clone();
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _peer)) = listener.accept().await else {
                    break;
                };
                let factory_ref = factory.clone();
                let registry_ref = registry.clone();
                use rand::Rng;
                let conn_id = format!("{:032x}", rand::thread_rng().gen::<u128>());
                let cancel_token = CancelToken::new();
                let token_for_task = cancel_token.clone();
                tokio::spawn(CANCEL_TOKEN.scope(
                    token_for_task,
                    CONN_ID.scope(conn_id, async move {
                        let mut socket = socket;
                        let mut buf = [0u8; 16];
                        if let Ok(n) = socket.peek(&mut buf).await {
                            // SSLRequest: [0,0,0,8, 4,210,22,47]
                            if n >= 8 && buf[..8] == [0, 0, 0, 8, 4, 210, 22, 47] {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                let mut ssl_buf = [0u8; 8];
                                let _ = socket.read_exact(&mut ssl_buf).await;
                                let _ = socket.write_all(b"N").await;
                                let _ = socket.flush().await;
                            }
                            // CancelRequest: [0,0,0,16, 4,210,22,46, pid(4), secret(4)]
                            else if n >= 16 && buf[..8] == [0, 0, 0, 16, 4, 210, 22, 46] {
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
                        if let Err(e) =
                            pgwire::tokio::process_socket(socket, None, factory_ref.clone()).await
                        {
                            tracing::debug!("gateway connection error: {e}");
                        }
                        // Cleanup LISTEN subscriptions on disconnect.
                        let cid = CONN_ID.with(|id| id.clone());
                        factory_ref.handler.notify_registry.unsubscribe_all(&cid);
                        factory_ref.handler.pending_notifies.remove(&cid);
                    }),
                ));
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
    if let Some(catalog_resp) =
        catalog.handle_query(q, &crate::catalog_stubs::SessionInfo::default())
    {
        match catalog_resp {
            CatalogResponse::Rows { columns, .. } => {
                return columns
                    .iter()
                    .map(|c| FieldInfo::new(c.clone(), None, None, Type::TEXT, FieldFormat::Text))
                    .collect();
            }
            _ => {}
        }
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
/// Parse `INSERT INTO <table> [(cols)] VALUES (v1, v2, ...)[, (v1, v2, ...)]*`.
///
/// Returns `(table, cols, rows)` where `rows` has one entry per parenthesized
/// VALUES tuple (v0.42.2: multi-row `VALUES` lists are supported — each row
/// becomes its own entry instead of being silently mis-split). A malformed
/// row (wrong value count relative to the declared column list, or relative
/// to the first row when no column list is given) is a hard parse error.
fn parse_insert(q: &str) -> Result<(String, Vec<String>, Vec<Vec<String>>), String> {
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

// ── v0.42.2: multi-row VALUES parsing ───────────────────────────────────────

#[cfg(test)]
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
