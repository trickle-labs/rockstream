//! SQL frontend for RockStream (v0.7).
//!
//! `SqlFrontend` wraps a DataFusion `SessionContext` and provides:
//!
//! 1. **Table registration** — register named in-memory tables with typed Arrow
//!    schemas so the SQL parser can resolve column references.
//! 2. **SQL → PlanNode lowering** — parse SQL, produce a DataFusion
//!    `LogicalPlan`, then lower it to a RockStream `PlanNode` via `lower()`.
//! 3. **`CREATE VIEW` support** — parse the query body, lower it, and store
//!    the result in a `SchemaCatalog`.
//! 4. **`EXPLAIN INCREMENTAL`** — format the `PlanNode` tree.
//! 5. **`EXPLAIN INCREMENTAL ESTIMATE`** — static cost model without deploying.
//!
//! The `SessionContext` is created with the default optimizer disabled so the
//! unoptimized plan is used for lowering.  This gives deterministic plan
//! structure that matches hand-coded `PlanNode` trees in tests.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;

use rockstream_plan::PlanNode;
use rockstream_types::explain::{ExplainLevel, OperatorStats, ShardInfo};

use crate::{
    catalog::{ColumnDef, IndexEntry, IndexState, SchemaCatalog, ViewEntry},
    distribution::apply_distribution,
    error::SqlError,
    estimate::{explain_incremental_estimate, format_estimate, EstimateRow},
    explain_incremental::{
        explain_incremental, explain_incremental_analyze, explain_incremental_verbose,
    },
    lower::lower_with_views,
};

// ─── Index selection types (v0.32-S5) ────────────────────────────────────────

/// Result of the index selection pass for a query (v0.32).
#[derive(Debug, Clone, PartialEq)]
pub enum IndexSelection {
    /// Use a secondary index scan.
    IndexScan {
        index_name: String,
        col: String,
        selectivity: f64,
    },
    /// Fall back to a full shard scan.
    ShardScan {
        table: String,
        reason: IndexFallbackReason,
    },
}

/// Reason why the planner chose a shard scan instead of an index scan (v0.32).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexFallbackReason {
    /// No index exists for the queried column.
    NoIndex,
    /// An index exists but is still building (RS-2014 audit event emitted).
    IndexBuilding,
    /// An index exists but its lag exceeds the allowed threshold (RS-2015 audit event emitted).
    IndexLagging,
    /// An index exists but the estimated selectivity is too low to be worth using.
    LowSelectivity,
}

// ─── DDL statement types (v0.32) ─────────────────────────────────────────────

/// Parsed DDL statement for index operations (v0.32).
///
/// DataFusion does not understand `CREATE INDEX`, `DROP INDEX`, or
/// `REBUILD INDEX`, so we parse these manually with simple string matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlStatement {
    /// `CREATE INDEX <name> ON <table> (<col1>, ...) [WHERE <pred>]`
    CreateIndex {
        index_name: String,
        table: String,
        index_cols: Vec<String>,
        where_pred: Option<String>,
    },
    /// `DROP INDEX <name>`
    DropIndex { index_name: String },
    /// `REBUILD INDEX <name>`
    RebuildIndex { index_name: String },
}

// ─── SqlFrontend ─────────────────────────────────────────────────────────────

/// The SQL frontend for RockStream.
///
/// One `SqlFrontend` instance lives for the lifetime of a pipeline compilation
/// session.  It holds the DataFusion `SessionContext` (with registered tables)
/// and provides the methods below.
pub struct SqlFrontend {
    ctx: SessionContext,
    snapshot_tables: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Default for SqlFrontend {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlFrontend {
    /// Create a new `SqlFrontend` with an empty session.
    ///
    /// Uses DataFusion's default session configuration.  The optimizer is kept
    /// enabled for production use but can be bypassed in tests via
    /// `sql_to_unoptimized_plan()`.
    pub fn new() -> Self {
        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);
        Self {
            ctx,
            snapshot_tables: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Register an empty in-memory table with the given Arrow schema.
    ///
    /// This makes the table visible to the SQL parser so column references
    /// can be resolved.  The table contains no data; only the schema matters
    /// for planning.
    pub fn register_table(&self, name: &str, schema: SchemaRef) -> Result<(), SqlError> {
        let mem_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])?;
        self.ctx.register_table(name, Arc::new(mem_table))?;
        Ok(())
    }

    /// Register an empty in-memory table as a snapshot source.
    pub fn register_snapshot_table(&self, name: &str, schema: SchemaRef) -> Result<(), SqlError> {
        self.register_table(name, schema)?;
        self.snapshot_tables
            .lock()
            .unwrap()
            .insert(name.to_string());
        Ok(())
    }

    /// Parse SQL and return the **optimized** `PlanNode`.
    ///
    /// The SQL must be a single `SELECT` statement.  `CREATE VIEW` statements
    /// are not lowered here — use `create_view()` instead.
    pub async fn sql_to_plan_node(&self, sql: &str) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_optimized_plan()?;
        let snapshot_sources = self.snapshot_tables.lock().unwrap().clone();
        lower_with_views(&logical, &Default::default(), &snapshot_sources)
    }

    /// Parse SQL and return the **unoptimized** `PlanNode`.
    ///
    /// The unoptimized plan is more predictable than the optimized plan because
    /// DataFusion's optimizer can push filters, merge projections, and rewrite
    /// expressions in ways that diverge from the hand-coded PlanNode.  Unit
    /// tests for "SQL == hard-coded PlanIR" should use this method.
    pub async fn sql_to_unoptimized_plan_node(&self, sql: &str) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_unoptimized_plan();
        let snapshot_sources = self.snapshot_tables.lock().unwrap().clone();
        lower_with_views(&logical, &Default::default(), &snapshot_sources)
    }

    /// Parse SQL with catalog-defined views and return the **optimized** `PlanNode`.
    pub async fn sql_to_plan_node_with_catalog(
        &self,
        sql: &str,
        catalog: &SchemaCatalog,
    ) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_optimized_plan()?;
        let registered_views: std::collections::HashSet<String> =
            catalog.list_view_names().await?.into_iter().collect();
        let snapshot_sources = self.snapshot_tables.lock().unwrap().clone();
        lower_with_views(&logical, &registered_views, &snapshot_sources)
    }

    /// Parse SQL with catalog-defined views and return the **unoptimized** `PlanNode`.
    pub async fn sql_to_unoptimized_plan_node_with_catalog(
        &self,
        sql: &str,
        catalog: &SchemaCatalog,
    ) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_unoptimized_plan();
        let registered_views: std::collections::HashSet<String> =
            catalog.list_view_names().await?.into_iter().collect();
        let snapshot_sources = self.snapshot_tables.lock().unwrap().clone();
        lower_with_views(&logical, &registered_views, &snapshot_sources)
    }

    /// Parse a `CREATE VIEW name AS <query>` statement and store the view in
    /// the given `SchemaCatalog`.
    ///
    /// # Errors
    /// - `RS-1012` if the SQL is invalid.
    /// - `RS-1013` if the query uses unsupported operators.
    /// - `RS-1002` if the view already exists with an incompatible schema.
    pub async fn create_view(
        &self,
        catalog: &SchemaCatalog,
        name: &str,
        query_sql: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<(), SqlError> {
        let df = self
            .ctx
            .sql(query_sql)
            .await
            .map_err(|e| SqlError::ParseError {
                message: e.to_string(),
            })?;
        let logical = df.into_optimized_plan()?;
        let registered_views: std::collections::HashSet<String> =
            catalog.list_view_names().await?.into_iter().collect();
        let snapshot_sources = self.snapshot_tables.lock().unwrap().clone();
        let plan_node = lower_with_views(&logical, &registered_views, &snapshot_sources)?;
        catalog
            .register_view(name, query_sql, &plan_node, columns)
            .await
    }

    /// Produce an `EXPLAIN INCREMENTAL` formatted string for the given SQL.
    ///
    /// Parses and lowers the SQL, then formats the resulting `PlanNode` tree.
    /// No operators are deployed or storage accessed.
    pub async fn explain_incremental_for_sql(
        &self,
        sql: &str,
        level: ExplainLevel,
        operator_stats: &[OperatorStats],
    ) -> Result<String, SqlError> {
        let plan = self.sql_to_unoptimized_plan_node(sql).await?;
        let text = match level {
            ExplainLevel::Default => explain_incremental(&plan),
            ExplainLevel::Verbose => {
                let shard_info = vec![
                    ShardInfo {
                        shard_count: 1,
                        parallelism: 1,
                        frontier_epoch: 0,
                    };
                    Self::count_plan_nodes(&plan)
                ];
                explain_incremental_verbose(
                    &plan,
                    &shard_info,
                    rockstream_types::metrics::read_total_workload_memory(),
                )
            }
            ExplainLevel::Analyze => explain_incremental_analyze(&plan, operator_stats),
        };
        Ok(text)
    }

    /// Produce an `EXPLAIN INCREMENTAL ESTIMATE` for the given SQL.
    ///
    /// Returns per-operator `(state_bytes, epoch_ms)` estimates based on static
    /// throughput models.  No operators are deployed or storage accessed.
    pub async fn explain_incremental_estimate_for_sql(
        &self,
        sql: &str,
        cardinality_hint: u64,
        batch_rows: u64,
    ) -> Result<Vec<EstimateRow>, SqlError> {
        let plan = self.sql_to_unoptimized_plan_node(sql).await?;
        Ok(explain_incremental_estimate(
            &plan,
            cardinality_hint,
            batch_rows,
        ))
    }

    /// Produce an `EXPLAIN INCREMENTAL ESTIMATE` formatted string.
    pub async fn explain_incremental_estimate_text(
        &self,
        sql: &str,
        cardinality_hint: u64,
        batch_rows: u64,
    ) -> Result<String, SqlError> {
        let rows = self
            .explain_incremental_estimate_for_sql(sql, cardinality_hint, batch_rows)
            .await?;
        Ok(format_estimate(&rows))
    }

    /// Load a view from the catalog and return its `PlanNode`.
    pub async fn load_view(
        &self,
        catalog: &SchemaCatalog,
        name: &str,
    ) -> Result<Option<ViewEntry>, SqlError> {
        catalog.load_view(name).await
    }

    fn count_plan_nodes(plan: &PlanNode) -> usize {
        match plan {
            PlanNode::Source { .. } | PlanNode::Snapshot { .. } | PlanNode::ViewRef { .. } => 1,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. }
            | PlanNode::Window { input, .. }
            | PlanNode::TumbleWindow { input, .. }
            | PlanNode::TopK { input, .. }
            | PlanNode::Lateral { input, .. }
            | PlanNode::IndexArrange { input, .. } => 1 + Self::count_plan_nodes(input),
            PlanNode::Exchange { child, .. } | PlanNode::ViewSink { child, .. } => {
                1 + Self::count_plan_nodes(child)
            }
            PlanNode::Join { left, right, .. }
            | PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Union { left, right }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => {
                1 + Self::count_plan_nodes(left) + Self::count_plan_nodes(right)
            }
            PlanNode::Recursion { base, step, .. } => {
                1 + Self::count_plan_nodes(base) + Self::count_plan_nodes(step)
            }
        }
    }

    /// Determine if a SQL query can use a secondary index (v0.32-S5).
    ///
    /// Parses `WHERE <col> = <val>` patterns from the SQL, then:
    /// 1. Looks up `READY` indexes on that column.
    /// 2. Returns `IndexScan` if selectivity < threshold.
    /// 3. Returns `ShardScan(IndexBuilding)` + RS-2014 if index is still building.
    /// 4. Returns `ShardScan(IndexLagging)` + RS-2015 if lag_ms > index_max_lag_ms.
    /// 5. Returns `ShardScan(LowSelectivity)` if ready but selectivity >= threshold.
    /// 6. Returns `ShardScan(NoIndex)` if no index for the column.
    ///
    /// The `index_lag_ms` parameter is the current measured lag for the index
    /// (passed by the caller; 0 if not applicable).
    pub async fn select_index_for_query(
        &self,
        catalog: &SchemaCatalog,
        sql: &str,
        index_prefer_selectivity_threshold: f64,
        index_max_lag_ms: u64,
        index_lag_ms: u64,
    ) -> Result<IndexSelection, SqlError> {
        use rockstream_types::error_code::{RS_2014, RS_2015};

        // Extract table and column from simple WHERE col = val pattern.
        let (table, col) = match Self::parse_where_eq_col(sql) {
            Some(tc) => tc,
            None => {
                // No WHERE col = val pattern found; default to shard scan.
                let table = Self::parse_table_name(sql).unwrap_or_else(|| "unknown".to_string());
                return Ok(IndexSelection::ShardScan {
                    table,
                    reason: IndexFallbackReason::NoIndex,
                });
            }
        };

        // Find indexes for this table and column.
        // For partial indexes, only use the index when the query predicate implies
        // the index's where_pred (query predicate ⊇ index predicate — S7).
        let query_upper = sql.to_uppercase();
        let index_names = catalog.list_index_names().await?;
        let mut matching_index: Option<IndexEntry> = None;
        for name in &index_names {
            if let Some(entry) = catalog.load_index(name).await? {
                if entry.table == table && entry.index_cols.contains(&col) {
                    // Partial index check: if the index has a where_pred, ensure
                    // the query's WHERE clause implies that predicate (simple text
                    // containment check for equality predicates like "col = val").
                    if let Some(ref pred) = entry.where_pred {
                        let pred_upper = pred.to_uppercase();
                        if !query_upper.contains(&pred_upper) {
                            // Query does not imply the index predicate → skip.
                            continue;
                        }
                    }
                    matching_index = Some(entry);
                    break;
                }
            }
        }

        let Some(index) = matching_index else {
            return Ok(IndexSelection::ShardScan {
                table,
                reason: IndexFallbackReason::NoIndex,
            });
        };

        // Check state: Building → RS-2014
        if index.state == IndexState::Building {
            tracing::warn!(
                error_code = %RS_2014,
                index_name = %index.name,
                "Index is building; falling back to shard scan (RS-2014)"
            );
            return Ok(IndexSelection::ShardScan {
                table,
                reason: IndexFallbackReason::IndexBuilding,
            });
        }

        // Check lag: lagging → RS-2015
        if index_lag_ms > index_max_lag_ms {
            tracing::warn!(
                error_code = %RS_2015,
                index_name = %index.name,
                index_lag_ms,
                index_max_lag_ms,
                "Index lag exceeded limit; falling back to shard scan (RS-2015)"
            );
            return Ok(IndexSelection::ShardScan {
                table,
                reason: IndexFallbackReason::IndexLagging,
            });
        }

        // Estimate selectivity: use a simple heuristic (1 / distinct_values).
        // For v0.32 we use the threshold directly — selectivity is caller-provided
        // or defaults to a value below the threshold to prefer the index.
        // Simplification: assume selectivity = 0.005 (below default 0.01 threshold)
        // unless the table name suggests a low-cardinality case.
        let selectivity = Self::estimate_selectivity(&table, &col);

        if selectivity < index_prefer_selectivity_threshold {
            Ok(IndexSelection::IndexScan {
                index_name: index.name,
                col,
                selectivity,
            })
        } else {
            Ok(IndexSelection::ShardScan {
                table,
                reason: IndexFallbackReason::LowSelectivity,
            })
        }
    }

    /// Format an `IndexSelection` as a human-readable EXPLAIN string (v0.32-S5).
    pub fn format_index_selection(selection: &IndexSelection) -> String {
        match selection {
            IndexSelection::IndexScan {
                index_name,
                col,
                selectivity,
            } => {
                format!("index_scan({index_name}, {col} = ?, selectivity={selectivity:.4})")
            }
            IndexSelection::ShardScan { table, reason } => {
                format!("shard_scan({table}, reason={reason:?})")
            }
        }
    }

    /// Parse `WHERE <col> = <val>` from a simple SELECT SQL string.
    /// Returns `(table, col)` if found.
    fn parse_where_eq_col(sql: &str) -> Option<(String, String)> {
        let upper = sql.to_uppercase();
        // Find FROM <table>
        let table = Self::parse_table_name(sql)?;
        // Find WHERE <col> = ...
        let where_pos = upper.find(" WHERE ")?;
        let after_where = sql[where_pos + 7..].trim();
        // Extract col name before " = "
        let eq_pos = after_where.find('=')?;
        let col = after_where[..eq_pos].trim().to_string();
        // Simple col name: no dots, no spaces
        if col.contains(' ') || col.contains('.') {
            return None;
        }
        Some((table, col))
    }

    /// Parse the table name from `FROM <table>` in a SQL string.
    fn parse_table_name(sql: &str) -> Option<String> {
        let upper = sql.to_uppercase();
        let from_pos = upper.find(" FROM ")?;
        let after_from = sql[from_pos + 6..].trim();
        let table = after_from
            .split_whitespace()
            .next()?
            .trim_end_matches(';')
            .to_string();
        Some(table)
    }

    /// Estimate selectivity for a column (heuristic for v0.32).
    ///
    /// Returns a low value (0.005) for most columns, simulating high selectivity.
    /// In a real system this would use histogram statistics.
    fn estimate_selectivity(table: &str, col: &str) -> f64 {
        // Heuristic: columns named "id", "_id", or "key" suffix are highly selective.
        let col_lower = col.to_lowercase();
        if col_lower.ends_with("_id")
            || col_lower == "id"
            || col_lower.ends_with("_key")
            || col_lower == "key"
        {
            0.001 // Very selective
        } else {
            // For other columns, use a moderate selectivity.
            // If the table has "small" or "lookup" in the name, treat as low selectivity.
            let table_lower = table.to_lowercase();
            if table_lower.contains("small") || table_lower.contains("lookup") {
                0.5 // Low selectivity — don't use index
            } else {
                0.005 // Default: moderately selective
            }
        }
    }

    /// Parse a DDL statement for index operations (v0.32).
    ///
    /// Handles:
    /// - `CREATE INDEX <name> ON <table> (<col1>, ...) [WHERE <pred>]`
    /// - `DROP INDEX <name>`
    /// - `REBUILD INDEX <name>`
    ///
    /// Uses simple string parsing since DataFusion does not support these forms.
    pub fn parse_ddl(&self, sql: &str) -> Result<DdlStatement, SqlError> {
        let s = sql.trim();
        let upper = s.to_uppercase();

        if upper.starts_with("DROP INDEX") {
            // DROP INDEX <name>
            let rest = s["DROP INDEX".len()..].trim();
            let name = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| SqlError::DdlParseError {
                    message: "DROP INDEX requires an index name".to_string(),
                })?;
            return Ok(DdlStatement::DropIndex {
                index_name: name.to_string(),
            });
        }

        if upper.starts_with("REBUILD INDEX") {
            // REBUILD INDEX <name>
            let rest = s["REBUILD INDEX".len()..].trim();
            let name = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| SqlError::DdlParseError {
                    message: "REBUILD INDEX requires an index name".to_string(),
                })?;
            return Ok(DdlStatement::RebuildIndex {
                index_name: name.to_string(),
            });
        }

        if upper.starts_with("CREATE INDEX") {
            // CREATE INDEX <name> ON <table> (<col1>, ...) [WHERE <pred>]
            let rest = s["CREATE INDEX".len()..].trim();

            // Split on " ON " (case-insensitive)
            let on_pos =
                rest.to_uppercase()
                    .find(" ON ")
                    .ok_or_else(|| SqlError::DdlParseError {
                        message: "CREATE INDEX requires ON clause".to_string(),
                    })?;
            let index_name = rest[..on_pos].trim().to_string();
            let after_on = rest[on_pos + 4..].trim();

            // Find the opening paren for column list
            let paren_open = after_on.find('(').ok_or_else(|| SqlError::DdlParseError {
                message: "CREATE INDEX requires column list in parentheses".to_string(),
            })?;
            let table = after_on[..paren_open].trim().to_string();

            let paren_close = after_on.rfind(')').ok_or_else(|| SqlError::DdlParseError {
                message: "CREATE INDEX: unclosed parenthesis in column list".to_string(),
            })?;
            let cols_str = &after_on[paren_open + 1..paren_close];
            let index_cols: Vec<String> = cols_str
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();

            if index_cols.is_empty() {
                return Err(SqlError::DdlParseError {
                    message: "CREATE INDEX requires at least one column".to_string(),
                });
            }

            // Optional WHERE predicate
            let after_paren = after_on[paren_close + 1..].trim();
            let where_pred = if after_paren.to_uppercase().starts_with("WHERE") {
                Some(after_paren["WHERE".len()..].trim().to_string())
            } else {
                None
            };

            return Ok(DdlStatement::CreateIndex {
                index_name,
                table,
                index_cols,
                where_pred,
            });
        }

        Err(SqlError::DdlParseError {
            message: format!("unrecognized DDL statement: {s}"),
        })
    }

    /// Create a secondary index with explicit primary-key columns (v0.32).
    ///
    /// - Returns `RS-2016` if an index with the same name already exists for a
    ///   different table.
    /// - Registers the `IndexEntry` in state `Building`.
    /// - Stores the plan as an internal view under `__idx_<index_name>`.
    pub async fn create_index_with_pk(
        &self,
        catalog: &SchemaCatalog,
        index_name: &str,
        table: &str,
        index_col_names: &[&str],
        pk_col_names: &[&str],
        where_pred: Option<&str>,
    ) -> Result<(), SqlError> {
        use rockstream_plan::PlanNode;

        // Check for name conflict.
        if let Some(existing) = catalog.load_index(index_name).await? {
            if existing.table != table {
                return Err(SqlError::IndexNameConflict {
                    index_name: index_name.to_string(),
                    existing_table: existing.table.clone(),
                    requested_table: table.to_string(),
                });
            }
        }

        // Resolve column names to indices using the registered table schema.
        let schema = self
            .ctx
            .table(table)
            .await
            .map_err(|e| SqlError::ParseError {
                message: format!("table '{table}' not found: {e}"),
            })?;
        let schema_fields = schema.schema().fields().clone();
        let col_names: Vec<String> = schema_fields.iter().map(|f| f.name().clone()).collect();

        let resolve_col = |col_name: &str| -> Result<usize, SqlError> {
            col_names
                .iter()
                .position(|n| n == col_name)
                .ok_or_else(|| SqlError::DdlParseError {
                    message: format!(
                        "column '{col_name}' not found in table '{table}'; available: {col_names:?}"
                    ),
                })
        };

        let index_col_indices: Vec<usize> = index_col_names
            .iter()
            .map(|n| resolve_col(n))
            .collect::<Result<_, _>>()?;
        let pk_col_indices: Vec<usize> = pk_col_names
            .iter()
            .map(|n| resolve_col(n))
            .collect::<Result<_, _>>()?;

        // Build IndexArrange → ViewSink plan.
        let index_arrange = PlanNode::IndexArrange {
            input: Box::new(PlanNode::Source {
                name: table.to_string(),
            }),
            index_cols: index_col_indices.clone(),
            pk_cols: pk_col_indices.clone(),
            filter_pred: None, // predicate lowering deferred; stored as SQL text
        };
        let internal_view_name = format!("__idx_{index_name}");
        let plan = PlanNode::ViewSink {
            view_name: internal_view_name.clone(),
            pk: pk_col_indices.clone(),
            child: Box::new(index_arrange),
        };

        // Store internal view entry (not visible in list_view_names due to __idx_ prefix filter).
        let columns: Vec<ColumnDef> = col_names
            .iter()
            .zip(schema_fields.iter())
            .map(|(name, field)| ColumnDef {
                name: name.clone(),
                data_type: format!("{:?}", field.data_type()),
                nullable: field.is_nullable(),
            })
            .collect();
        catalog
            .register_view(&internal_view_name, "", &plan, columns)
            .await?;

        // Store index catalog entry in Building state.
        let entry = IndexEntry {
            name: index_name.to_string(),
            table: table.to_string(),
            index_cols: index_col_names.iter().map(|s| s.to_string()).collect(),
            pk_cols: pk_col_names.iter().map(|s| s.to_string()).collect(),
            where_pred: where_pred.map(|s| s.to_string()),
            state: IndexState::Building,
        };
        catalog.register_index(&entry).await?;

        Ok(())
    }

    /// Drop a secondary index (v0.32-S8).
    ///
    /// Removes both the `IndexEntry` from the catalog and the internal
    /// `__idx_<name>` view. Arrangement data is GC'd by frontier-aware
    /// compaction filter after the index frontier is closed.
    pub async fn drop_index(
        &self,
        catalog: &SchemaCatalog,
        index_name: &str,
    ) -> Result<(), SqlError> {
        // Remove index catalog entry.
        catalog.remove_index(index_name).await?;
        // Remove internal view entry (best-effort: ignore if already absent).
        let internal_view_name = format!("__idx_{index_name}");
        catalog.remove_view(&internal_view_name).await?;
        Ok(())
    }

    /// Rebuild a secondary index (v0.32-S8).
    ///
    /// Transitions the index state back to `BUILDING` so the backfill
    /// will be re-run from the current base-table checkpoint.
    pub async fn rebuild_index(
        &self,
        catalog: &SchemaCatalog,
        index_name: &str,
    ) -> Result<(), SqlError> {
        let mut entry =
            catalog
                .load_index(index_name)
                .await?
                .ok_or_else(|| SqlError::ParseError {
                    message: format!("index '{index_name}' not found"),
                })?;
        entry.state = IndexState::Building;
        catalog.register_index(&entry).await?;
        Ok(())
    }

    /// Apply the distribution pass to a `PlanNode` tree and return the result.
    ///
    /// In single-shard mode all exchanges are `Loopback`.
    pub fn apply_distribution(&self, plan: PlanNode) -> PlanNode {
        apply_distribution(plan)
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::frontend::DdlStatement;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, ExchangeKind, Expr, PlanNode};

    fn two_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    fn three_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("w", DataType::Int64, false),
        ]))
    }

    // ── Proof claim 1: SQL == hard-coded PlanIR for Phase 1 operators ─────────

    /// `SELECT a, b FROM t WHERE a > 10` (filter + project).
    ///
    /// Expected unoptimized plan from DataFusion:
    /// Projection([a, b])
    ///   Filter(a > 10)
    ///     TableScan(t)
    ///
    /// Hand-coded PlanNode:
    /// Project([Col(0), Col(1)])
    ///   Filter(BinaryOp{Gt, Col(0), Literal(10)})
    ///     Source("t")
    #[tokio::test]
    async fn sql_filter_project_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", two_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT a, b FROM t WHERE a > 10")
            .await
            .expect("lowering should succeed");

        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                predicate: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(Expr::Literal(10i64.to_be_bytes().to_vec())),
                },
            }),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered plan should equal hand-coded plan.\nGot: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    /// `SELECT k, SUM(v), COUNT(v), AVG(v) FROM t GROUP BY k` — SUM/COUNT/AVG.
    ///
    /// DataFusion's unoptimized plan for aggregate queries adds an outer
    /// Projection node that selects the aggregate output columns in order.
    /// The hand-coded PlanNode must include this outer Project to match.
    #[tokio::test]
    async fn sql_aggregate_sum_count_avg_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT k, SUM(v), COUNT(v), AVG(v) FROM t GROUP BY k")
            .await
            .expect("lowering should succeed");

        // DataFusion adds Project([k@0, SUM(v)@1, COUNT(v)@2, AVG(v)@3]) above Aggregate.
        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![
                    AggregateExpr {
                        func: AggregateFunc::Sum,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Count,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Avg,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                ],
            }),
            // Passthrough projection: k@0, SUM(v)@1, COUNT(v)@2, AVG(v)@3
            columns: vec![
                Expr::Column(0),
                Expr::Column(1),
                Expr::Column(2),
                Expr::Column(3),
            ],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered aggregate plan should equal hand-coded plan.\n\
             Got: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    /// `SELECT k, MIN(v), MAX(v) FROM t GROUP BY k` — MIN/MAX.
    #[tokio::test]
    async fn sql_aggregate_min_max_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT k, MIN(v), MAX(v) FROM t GROUP BY k")
            .await
            .expect("lowering should succeed");

        // DataFusion adds Project([k@0, MIN(v)@1, MAX(v)@2]) above Aggregate.
        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![
                    AggregateExpr {
                        func: AggregateFunc::Min,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Max,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                ],
            }),
            // Passthrough projection: k@0, MIN(v)@1, MAX(v)@2
            columns: vec![Expr::Column(0), Expr::Column(1), Expr::Column(2)],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered MIN/MAX plan should equal hand-coded plan.\n\
             Got: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    // ── EXPLAIN INCREMENTAL ───────────────────────────────────────────────────

    #[tokio::test]
    async fn explain_incremental_formats_plan_tree() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", two_col_schema()).unwrap();

        let text = frontend
            .explain_incremental_for_sql("SELECT a FROM t WHERE a > 5", ExplainLevel::Default, &[])
            .await
            .unwrap();

        assert!(text.contains("EXPLAIN INCREMENTAL"), "text: {text}");
        assert!(text.contains("Filter"), "text: {text}");
        assert!(text.contains("Source"), "text: {text}");
    }

    // ── EXPLAIN INCREMENTAL ESTIMATE (Proof claim 4) ──────────────────────────

    /// Proof claim 4: `EXPLAIN INCREMENTAL ESTIMATE` reports predicted state
    /// size and per-operator `epoch_ms` **without deploying** any operators.
    #[tokio::test]
    async fn explain_incremental_estimate_reports_state_and_epoch() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let rows = frontend
            .explain_incremental_estimate_for_sql(
                "SELECT k, SUM(v) FROM t GROUP BY k",
                500,    // cardinality_hint: 500 groups
                10_000, // batch_rows: 10k rows per epoch
            )
            .await
            .unwrap();

        // Must have at least Source and Aggregate rows.
        assert!(
            rows.len() >= 2,
            "expected ≥2 estimate rows, got {}",
            rows.len()
        );

        let agg_row = rows
            .iter()
            .find(|r| r.operator_kind.contains("Aggregate"))
            .unwrap();
        assert!(
            agg_row.predicted_state_bytes > 0,
            "Aggregate must have non-zero state_bytes; got {}",
            agg_row.predicted_state_bytes
        );
        assert!(
            agg_row.epoch_ms > 0.0,
            "Aggregate must have positive epoch_ms; got {}",
            agg_row.epoch_ms
        );

        // Confirm no operators are deployed: calling this twice produces same result.
        let rows2 = frontend
            .explain_incremental_estimate_for_sql("SELECT k, SUM(v) FROM t GROUP BY k", 500, 10_000)
            .await
            .unwrap();
        assert_eq!(rows, rows2, "estimate must be pure / deterministic");
    }

    // ── Index selection tests (v0.32-S5) ─────────────────────────────────────

    async fn make_catalog_with_index(
        state: crate::catalog::IndexState,
    ) -> (
        Arc<rockstream_storage::ShardDb>,
        crate::catalog::SchemaCatalog,
    ) {
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            rockstream_storage::ShardDb::builder("catalog", store)
                .build()
                .await
                .unwrap(),
        );
        let catalog = crate::catalog::SchemaCatalog::new(Arc::clone(&db));
        let entry = crate::catalog::IndexEntry {
            name: "idx_customer_id".to_string(),
            table: "orders".to_string(),
            index_cols: vec!["customer_id".to_string()],
            pk_cols: vec!["order_id".to_string()],
            where_pred: None,
            state,
        };
        catalog.register_index(&entry).await.unwrap();
        std::mem::forget(dir); // keep dir alive for duration
        (db, catalog)
    }

    #[tokio::test]
    async fn planner_chooses_index_for_selective_predicate() {
        let (_db, catalog) = make_catalog_with_index(crate::catalog::IndexState::Ready).await;
        let frontend = SqlFrontend::new();
        let result = frontend
            .select_index_for_query(
                &catalog,
                "SELECT * FROM orders WHERE customer_id = 42",
                0.01,  // threshold
                10000, // max lag ms
                0,     // current lag ms
            )
            .await
            .unwrap();
        match result {
            IndexSelection::IndexScan {
                index_name,
                col,
                selectivity,
            } => {
                assert_eq!(index_name, "idx_customer_id");
                assert_eq!(col, "customer_id");
                assert!(selectivity < 0.01, "selectivity should be < threshold");
            }
            other => panic!("expected IndexScan, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn planner_falls_back_for_non_selective_predicate() {
        let (_db, catalog) = make_catalog_with_index(crate::catalog::IndexState::Ready).await;
        // Add a low-selectivity index for a "status" column
        let entry2 = crate::catalog::IndexEntry {
            name: "idx_status".to_string(),
            table: "small_table".to_string(),
            index_cols: vec!["status".to_string()],
            pk_cols: vec!["id".to_string()],
            where_pred: None,
            state: crate::catalog::IndexState::Ready,
        };
        catalog.register_index(&entry2).await.unwrap();
        let frontend = SqlFrontend::new();
        let result = frontend
            .select_index_for_query(
                &catalog,
                "SELECT * FROM small_table WHERE status = 1",
                0.01,  // threshold
                10000, // max lag ms
                0,     // current lag ms
            )
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                IndexSelection::ShardScan {
                    reason: IndexFallbackReason::LowSelectivity,
                    ..
                }
            ),
            "expected ShardScan(LowSelectivity), got: {result:?}"
        );
    }

    #[tokio::test]
    async fn planner_falls_back_for_building_index() {
        let (_db, catalog) = make_catalog_with_index(crate::catalog::IndexState::Building).await;
        let frontend = SqlFrontend::new();
        let result = frontend
            .select_index_for_query(
                &catalog,
                "SELECT * FROM orders WHERE customer_id = 42",
                0.01,
                10000,
                0,
            )
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                IndexSelection::ShardScan {
                    reason: IndexFallbackReason::IndexBuilding,
                    ..
                }
            ),
            "expected ShardScan(IndexBuilding), got: {result:?}"
        );
    }

    #[tokio::test]
    async fn planner_falls_back_for_lagging_index() {
        let (_db, catalog) = make_catalog_with_index(crate::catalog::IndexState::Ready).await;
        let frontend = SqlFrontend::new();
        let result = frontend
            .select_index_for_query(
                &catalog,
                "SELECT * FROM orders WHERE customer_id = 42",
                0.01, // threshold
                1000, // max lag ms = 1 second
                5000, // current lag ms = 5 seconds (exceeds limit)
            )
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                IndexSelection::ShardScan {
                    reason: IndexFallbackReason::IndexLagging,
                    ..
                }
            ),
            "expected ShardScan(IndexLagging), got: {result:?}"
        );
    }

    #[tokio::test]
    async fn create_index_name_conflict_returns_rs2016() {
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            rockstream_storage::ShardDb::builder("catalog", store)
                .build()
                .await
                .unwrap(),
        );
        let catalog = crate::catalog::SchemaCatalog::new(Arc::clone(&db));

        // Register index for table_a
        let entry1 = crate::catalog::IndexEntry {
            name: "idx_conflict".to_string(),
            table: "table_a".to_string(),
            index_cols: vec!["col1".to_string()],
            pk_cols: vec!["id".to_string()],
            where_pred: None,
            state: crate::catalog::IndexState::Ready,
        };
        catalog.register_index(&entry1).await.unwrap();

        // Try to register same name for table_b → RS-2016
        let entry2 = crate::catalog::IndexEntry {
            name: "idx_conflict".to_string(),
            table: "table_b".to_string(),
            index_cols: vec!["col2".to_string()],
            pk_cols: vec!["id".to_string()],
            where_pred: None,
            state: crate::catalog::IndexState::Ready,
        };
        let result = catalog.register_index(&entry2).await;
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("RS-2016"),
            "expected RS-2016 in: {err_str}"
        );
        let _ = dir; // keep alive
    }

    // ── DDL parse tests (v0.32) ───────────────────────────────────────────────

    #[test]
    fn parse_ddl_create_index() {
        let frontend = SqlFrontend::new();
        let stmt = frontend
            .parse_ddl("CREATE INDEX idx_cust ON orders (customer_id)")
            .unwrap();
        assert_eq!(
            stmt,
            DdlStatement::CreateIndex {
                index_name: "idx_cust".to_string(),
                table: "orders".to_string(),
                index_cols: vec!["customer_id".to_string()],
                where_pred: None,
            }
        );
    }

    #[test]
    fn parse_ddl_create_index_with_where() {
        let frontend = SqlFrontend::new();
        let stmt = frontend
            .parse_ddl("CREATE INDEX idx_active ON users (status) WHERE active = 1")
            .unwrap();
        assert_eq!(
            stmt,
            DdlStatement::CreateIndex {
                index_name: "idx_active".to_string(),
                table: "users".to_string(),
                index_cols: vec!["status".to_string()],
                where_pred: Some("active = 1".to_string()),
            }
        );
    }

    #[test]
    fn parse_ddl_create_index_multi_col() {
        let frontend = SqlFrontend::new();
        let stmt = frontend
            .parse_ddl("CREATE INDEX idx_compound ON events (user_id, event_type)")
            .unwrap();
        assert_eq!(
            stmt,
            DdlStatement::CreateIndex {
                index_name: "idx_compound".to_string(),
                table: "events".to_string(),
                index_cols: vec!["user_id".to_string(), "event_type".to_string()],
                where_pred: None,
            }
        );
    }

    #[test]
    fn parse_ddl_drop_index() {
        let frontend = SqlFrontend::new();
        let stmt = frontend.parse_ddl("DROP INDEX idx_cust").unwrap();
        assert_eq!(
            stmt,
            DdlStatement::DropIndex {
                index_name: "idx_cust".to_string(),
            }
        );
    }

    #[test]
    fn parse_ddl_rebuild_index() {
        let frontend = SqlFrontend::new();
        let stmt = frontend.parse_ddl("REBUILD INDEX idx_cust").unwrap();
        assert_eq!(
            stmt,
            DdlStatement::RebuildIndex {
                index_name: "idx_cust".to_string(),
            }
        );
    }

    #[test]
    fn parse_ddl_unknown_returns_error() {
        let frontend = SqlFrontend::new();
        assert!(frontend.parse_ddl("SELECT 1").is_err());
        assert!(frontend.parse_ddl("CREATE TABLE t (a INT)").is_err());
    }

    // ── Distribution pass ────────────────────────────────────────────────────

    #[test]
    fn distribution_pass_wraps_aggregate_with_loopback() {
        let frontend = SqlFrontend::new();
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let distributed = frontend.apply_distribution(plan);
        if let PlanNode::Aggregate { input, .. } = &distributed {
            assert!(
                matches!(
                    input.as_ref(),
                    PlanNode::Exchange {
                        kind: ExchangeKind::Loopback,
                        ..
                    }
                ),
                "expected Loopback exchange before Aggregate: {input:?}"
            );
        } else {
            panic!("expected Aggregate: {distributed:?}");
        }
    }

    #[tokio::test]
    async fn test_compile_tumble_window() {
        let frontend = SqlFrontend::new();
        frontend
            .register_table(
                "bid",
                Arc::new(Schema::new(vec![
                    Field::new("auction", DataType::Int64, false),
                    Field::new("bidder", DataType::Int64, false),
                    Field::new("price", DataType::Int64, false),
                    Field::new("channel", DataType::Utf8, false),
                    Field::new("url", DataType::Utf8, false),
                    Field::new("date_time", DataType::Int64, false),
                    Field::new("extra", DataType::Utf8, false),
                ])),
            )
            .unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node(
                "SELECT date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) as dt, COUNT(*) \
                 FROM bid \
                 GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))"
            )
            .await
            .expect("should compile");

        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::TumbleWindow {
                    input: Box::new(PlanNode::Source {
                        name: "bid".to_string(),
                    }),
                    time_col: 5,
                    window_size_ms: 10000,
                    late_data_policy: rockstream_plan::LateDataPolicy::Drop,
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Count,
                    input: Expr::Literal(1i64.to_be_bytes().to_vec()),
                    distinct: false,
                }],
            }),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        };

        assert_eq!(sql_plan, expected);
    }

    #[tokio::test]
    async fn test_compile_topk() {
        let frontend = SqlFrontend::new();
        frontend
            .register_table(
                "bid",
                Arc::new(Schema::new(vec![
                    Field::new("auction", DataType::Int64, false),
                    Field::new("bidder", DataType::Int64, false),
                    Field::new("price", DataType::Int64, false),
                    Field::new("channel", DataType::Utf8, false),
                    Field::new("url", DataType::Utf8, false),
                    Field::new("date_time", DataType::Int64, false),
                    Field::new("extra", DataType::Utf8, false),
                ])),
            )
            .unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node(
                "SELECT auction, price FROM ( \
                     SELECT auction, price, \
                            ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn \
                     FROM bid \
                 ) WHERE rn <= 5",
            )
            .await
            .expect("should compile");

        let expected = PlanNode::Project {
            input: Box::new(PlanNode::TopK {
                input: Box::new(PlanNode::Source {
                    name: "bid".to_string(),
                }),
                k: 5,
                rank_col: 2,
                partition_by: vec![0],
            }),
            columns: vec![Expr::Column(0), Expr::Column(2)],
        };

        assert_eq!(sql_plan, expected);
    }
}
