//! View materializer: the "last hop" that streams worker-maintained view output
//! back into the gateway's serving shard after each DML commit.
//!
//! After a `COMMIT` writes rows to one or more source tables, this module:
//!
//! 1. Identifies all registered views (and views-of-views) that transitively
//!    depend on the changed tables.
//! 2. Sorts them topologically (sources before dependents).
//! 3. For each view in order, reads the current source-table rows from the
//!    shard, executes the view's SQL using DataFusion's in-memory engine, and
//!    writes the output back to `view_output/{view_name}/` so that a subsequent
//!    `SELECT` returns live rows.
//!
//! This is a **batch re-evaluation** on every commit — correct and simple, not
//! incremental.  The incremental path (Z-set deltas through operator DAGs) is
//! proven in the `rockstream-oracle` and `rockstream-sql` test harnesses; this
//! module connects that proven machinery to the serving layer so `psql` users
//! see data flow end to end.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use tracing::debug;

use rockstream_storage::{ShardDb, WriteBatch};

use crate::catalog_stubs::{CatalogColumn, CatalogStubs};

// ── Public entry point ────────────────────────────────────────────────────────

/// After a DML commit that touched `changed_tables`, re-evaluate every view
/// that (transitively) depends on those tables and write its output into the
/// serving shard.
///
/// Errors are logged but do not propagate — a materialisation failure must not
/// abort the originating `COMMIT`.
pub async fn materialize_views(
    catalog: &CatalogStubs,
    shard_db: &ShardDb,
    changed_tables: &HashSet<String>,
) {
    if let Err(e) = try_materialize_views(catalog, shard_db, changed_tables).await {
        tracing::warn!("view materialisation error (non-fatal): {e}");
    }
}

// ── Internal implementation ───────────────────────────────────────────────────

async fn try_materialize_views(
    catalog: &CatalogStubs,
    shard_db: &ShardDb,
    changed_tables: &HashSet<String>,
) -> Result<(), String> {
    let all_views = catalog.list_views();
    if all_views.is_empty() {
        return Ok(());
    }

    // Build dep map: view_name → deps (names of tables/views it reads from)
    let dep_map: HashMap<String, Vec<String>> = all_views
        .iter()
        .map(|v| (v.name.clone(), extract_sql_refs(&v.sql)))
        .collect();

    // Topological order of views to refresh: BFS from the seed tables
    let ordered = topological_order(&dep_map, changed_tables);
    if ordered.is_empty() {
        return Ok(());
    }

    debug!(
        views = ?ordered,
        "view_materializer: refreshing views after commit"
    );

    // Per-view Arrow schema cache: grows as we materialise each view so that
    // downstream views-of-views know what columns the upstream view produced.
    let mut inferred_schemas: HashMap<String, SchemaRef> = HashMap::new();

    for view_name in &ordered {
        let view = match catalog.get_view(view_name) {
            Some(v) => v,
            None => continue,
        };

        // --- Build DataFusion context ----------------------------------------
        let ctx = SessionContext::new();

        for src_name in extract_sql_refs(&view.sql) {
            // Determine Arrow schema for the source
            let schema = source_schema(catalog, &src_name, &inferred_schemas);

            // Read current rows from shard
            let prefix = format!("view_output/{src_name}/");
            let kvs = shard_db
                .scan_prefix(prefix.as_bytes())
                .await
                .map_err(|e| format!("scan({src_name}): {e}"))?;
            let tsv_rows: Vec<Vec<u8>> = kvs.into_iter().map(|(_, v)| v.to_vec()).collect();

            // Build in-memory RecordBatch
            let batch = tsv_to_record_batch(schema.clone(), &tsv_rows)
                .unwrap_or_else(|_| RecordBatch::new_empty(schema.clone()));
            let mem_table = MemTable::try_new(schema, vec![vec![batch]])
                .map_err(|e| format!("MemTable({src_name}): {e}"))?;
            ctx.register_table(src_name.as_str(), Arc::new(mem_table))
                .map_err(|e| format!("register({src_name}): {e}"))?;
        }

        // --- Execute view SQL ------------------------------------------------
        let df = ctx
            .sql(&view.sql)
            .await
            .map_err(|e| format!("sql({view_name}): {e}"))?;

        // Capture output schema before collect() consumes df
        let output_schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

        let batches = df
            .collect()
            .await
            .map_err(|e| format!("collect({view_name}): {e}"))?;

        // --- Remove old view output then write new ---------------------------
        let old_prefix = format!("view_output/{view_name}/");
        let old_kvs = shard_db
            .scan_prefix(old_prefix.as_bytes())
            .await
            .map_err(|e| format!("scan_old({view_name}): {e}"))?;

        let mut wb = WriteBatch::new();
        for (key, _) in old_kvs {
            wb.delete(&key);
        }

        let mut row_idx = 0usize;
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let tsv = batch_row_to_tsv(batch, row);
                let key = format!("view_output/{view_name}/row_{row_idx:010}");
                wb.put(key.as_bytes(), tsv.as_bytes());
                row_idx += 1;
            }
        }

        shard_db
            .write_batch(wb)
            .await
            .map_err(|e| format!("write({view_name}): {e}"))?;

        debug!(
            view = view_name,
            rows = row_idx,
            "view_materializer: wrote view output"
        );

        // --- Update catalog with inferred output schema ---------------------
        let col_defs: Vec<CatalogColumn> = output_schema
            .fields()
            .iter()
            .map(|f| CatalogColumn {
                name: f.name().clone(),
                data_type: arrow_dt_to_catalog_name(f.data_type()).to_string(),
            })
            .collect();
        catalog.update_view_columns(view_name, col_defs);

        // Cache schema so downstream views-of-views can read it
        inferred_schemas.insert(view_name.clone(), output_schema);
    }

    Ok(())
}

// ── Topological sort (BFS from seed tables) ──────────────────────────────────

/// Return view names in dependency order (sources before dependents).
///
/// Starting from `seeds` (the tables that were written), BFS through the
/// dep_map to find all transitively dependent views.  The returned Vec is
/// sorted so that every view appears after all of its dependencies.
fn topological_order(
    dep_map: &HashMap<String, Vec<String>>,
    seeds: &HashSet<String>,
) -> Vec<String> {
    // Kahn's algorithm (BFS)
    // Build: source_name → list of view_names that directly depend on it
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for (view, deps) in dep_map {
        for dep in deps {
            dependents
                .entry(dep.clone())
                .or_default()
                .push(view.clone());
        }
    }

    // Track in-degree (number of unsatisfied dependencies)
    let mut in_degree: HashMap<String, usize> = dep_map
        .keys()
        .map(|v| {
            let satisfied = dep_map[v]
                .iter()
                .filter(|d| dep_map.contains_key(d.as_str()))
                .count();
            (v.clone(), satisfied)
        })
        .collect();

    // Seed queue with views whose direct deps are all base tables (in-degree 0
    // among views) that also transitively touch a changed table
    let reachable = reachable_views(dep_map, seeds);

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(v, &d)| d == 0 && reachable.contains(*v))
        .map(|(v, _)| v.clone())
        .collect();
    // Sort for determinism
    let mut queue_vec: Vec<String> = queue.drain(..).collect();
    queue_vec.sort();
    let mut queue: VecDeque<String> = queue_vec.into();

    let mut ordered = Vec::new();
    while let Some(v) = queue.pop_front() {
        ordered.push(v.clone());
        if let Some(deps_of_v) = dependents.get(&v) {
            let mut next_batch: Vec<String> = deps_of_v
                .iter()
                .filter(|d| reachable.contains(d.as_str()))
                .filter_map(|d| {
                    let deg = in_degree.get_mut(d)?;
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        Some(d.clone())
                    } else {
                        None
                    }
                })
                .collect();
            next_batch.sort();
            for n in next_batch {
                queue.push_back(n);
            }
        }
    }
    ordered
}

/// Return the set of view names that transitively read from any seed name.
fn reachable_views(
    dep_map: &HashMap<String, Vec<String>>,
    seeds: &HashSet<String>,
) -> HashSet<String> {
    let mut reachable = HashSet::new();
    let mut frontier: VecDeque<String> = seeds.iter().cloned().collect();
    while let Some(name) = frontier.pop_front() {
        for (view, deps) in dep_map {
            if !reachable.contains(view) && deps.contains(&name) {
                reachable.insert(view.clone());
                frontier.push_back(view.clone());
            }
        }
    }
    reachable
}

// ── Schema helpers ────────────────────────────────────────────────────────────

/// Resolve an Arrow schema for a source name (table or previously-materialised view).
fn source_schema(
    catalog: &CatalogStubs,
    name: &str,
    inferred: &HashMap<String, SchemaRef>,
) -> SchemaRef {
    // 1. Already-materialised view schema (most authoritative)
    if let Some(s) = inferred.get(name) {
        return s.clone();
    }
    // 2. Base table schema from catalog
    if let Some(ct) = catalog.get_table(name) {
        let fields: Vec<Field> = ct
            .columns
            .iter()
            .map(|c| Field::new(&c.name, catalog_name_to_arrow_dt(&c.data_type), true))
            .collect();
        return Arc::new(Schema::new(fields));
    }
    // 3. View schema from catalog (may be empty if not yet materialised)
    if let Some(cv) = catalog.get_view(name) {
        if !cv.columns.is_empty() {
            let fields: Vec<Field> = cv
                .columns
                .iter()
                .map(|c| Field::new(&c.name, catalog_name_to_arrow_dt(&c.data_type), true))
                .collect();
            return Arc::new(Schema::new(fields));
        }
    }
    // Fallback: single Utf8 column — ensures DataFusion won't panic on empty schema
    Arc::new(Schema::new(vec![Field::new(
        "_value",
        DataType::Utf8,
        true,
    )]))
}

/// Map a catalog data-type name (Arrow name) to an Arrow `DataType`.
pub fn catalog_name_to_arrow_dt(name: &str) -> DataType {
    match name {
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float64" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

/// Map an Arrow `DataType` back to the catalog name used in `CatalogColumn`.
fn arrow_dt_to_catalog_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => "Int32",
        DataType::Int64 | DataType::UInt64 => "Int64",
        DataType::Float32 | DataType::Float64 => "Float64",
        DataType::Boolean => "Boolean",
        _ => "Utf8",
    }
}

// ── RecordBatch ↔ TSV conversion ──────────────────────────────────────────────

/// Parse a list of tab-separated rows into an Arrow `RecordBatch`.
///
/// Values are cast from string to the declared column type.  Rows that cannot
/// be parsed for a column fall back to `null`.
fn tsv_to_record_batch(schema: SchemaRef, rows: &[Vec<u8>]) -> Result<RecordBatch, String> {
    let n = rows.len();
    let num_cols = schema.fields().len();

    // Collect all field values as strings first
    let mut col_strs: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(n); num_cols];
    for row in rows {
        let s = String::from_utf8_lossy(row);
        let fields: Vec<&str> = s.split('\t').collect();
        for (i, col) in col_strs.iter_mut().enumerate() {
            col.push(fields.get(i).map(|v| v.to_string()));
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
                        s.as_deref().map(|v| match v.to_lowercase().as_str() {
                            "true" | "t" | "1" => true,
                            _ => false,
                        })
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

/// Serialise one row of a `RecordBatch` to a tab-separated string.
fn batch_row_to_tsv(batch: &RecordBatch, row: usize) -> String {
    use arrow::array::Array;
    batch
        .columns()
        .iter()
        .map(|col| {
            if col.is_null(row) {
                String::new()
            } else {
                array_value_to_string(col.as_ref(), row)
            }
        })
        .collect::<Vec<_>>()
        .join("\t")
}

/// Format a single cell from an Arrow array as a display string.
fn array_value_to_string(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        _ => format!("{:?}", array.data_type()),
    }
}

// ── SQL ref extraction (mirrors server.rs::extract_sql_refs) ─────────────────

/// Extract table/view names from FROM and JOIN clauses in a SQL string.
fn extract_sql_refs(sql: &str) -> Vec<String> {
    let tokens_orig: Vec<&str> = sql.split_whitespace().collect();
    let tokens_lower: Vec<String> = tokens_orig.iter().map(|t| t.to_lowercase()).collect();
    let mut deps = Vec::new();
    for i in 0..tokens_lower.len() {
        if tokens_lower[i] == "from" || tokens_lower[i] == "join" {
            if let Some(next) = tokens_orig.get(i + 1) {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_stubs::{CatalogTable, CatalogView};
    use object_store::memory::InMemory;
    use rockstream_storage::ShardDb;
    use std::sync::Arc;

    async fn make_shard() -> Arc<ShardDb> {
        let store = Arc::new(InMemory::new());
        Arc::new(ShardDb::builder("test", store).build().await.unwrap())
    }

    #[test]
    fn topological_order_simple() {
        // view_a depends on table_t; view_b depends on view_a
        let mut dep_map = HashMap::new();
        dep_map.insert("view_a".to_string(), vec!["table_t".to_string()]);
        dep_map.insert("view_b".to_string(), vec!["view_a".to_string()]);

        let seeds: HashSet<String> = ["table_t".to_string()].into();
        let order = topological_order(&dep_map, &seeds);
        assert_eq!(order, vec!["view_a", "view_b"]);
    }

    #[test]
    fn topological_order_unrelated_view_excluded() {
        let mut dep_map = HashMap::new();
        dep_map.insert("view_a".to_string(), vec!["table_t".to_string()]);
        dep_map.insert("view_x".to_string(), vec!["table_other".to_string()]);

        let seeds: HashSet<String> = ["table_t".to_string()].into();
        let order = topological_order(&dep_map, &seeds);
        assert_eq!(order, vec!["view_a"]);
    }

    #[tokio::test]
    async fn materialise_simple_filter_view() {
        let shard = make_shard().await;
        let catalog = Arc::new(CatalogStubs::new());

        // Register table: orders(id INT, amount INT)
        catalog.add_table(CatalogTable {
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
        });

        // Register view: big_orders = SELECT id, amount FROM orders WHERE amount > 50
        catalog.add_view_with_deps(
            CatalogView {
                name: "big_orders".to_string(),
                sql: "SELECT id, amount FROM orders WHERE amount > 50".to_string(),
                columns: vec![],
                namespace: "public".to_string(),
            },
            vec!["orders".to_string()],
        );

        // Write two rows to orders shard
        let mut wb = WriteBatch::new();
        wb.put(b"view_output/orders/id=1|amount=100", b"1\t100");
        wb.put(b"view_output/orders/id=2|amount=20", b"2\t20");
        shard.write_batch(wb).await.unwrap();

        // Materialise
        let changed: HashSet<String> = ["orders".to_string()].into();
        materialize_views(&catalog, &shard, &changed).await;

        // big_orders should contain only the row with amount=100
        let prefix = b"view_output/big_orders/";
        let kvs = shard.scan_prefix(prefix).await.unwrap();
        assert_eq!(kvs.len(), 1, "expected 1 row in big_orders");
        let row_str = String::from_utf8_lossy(&kvs[0].1);
        assert!(
            row_str.contains("100"),
            "expected amount=100 in output, got: {row_str}"
        );
    }

    #[tokio::test]
    async fn materialise_view_of_view() {
        let shard = make_shard().await;
        let catalog = Arc::new(CatalogStubs::new());

        // clicks(user_id INT, url TEXT, ts INT)
        catalog.add_table(CatalogTable {
            name: "clicks".to_string(),
            columns: vec![
                CatalogColumn {
                    name: "user_id".to_string(),
                    data_type: "Int64".to_string(),
                },
                CatalogColumn {
                    name: "url".to_string(),
                    data_type: "Utf8".to_string(),
                },
                CatalogColumn {
                    name: "ts".to_string(),
                    data_type: "Int64".to_string(),
                },
            ],
        });

        // view: page_hits = SELECT url, COUNT(*) AS hits FROM clicks GROUP BY url
        catalog.add_view_with_deps(
            CatalogView {
                name: "page_hits".to_string(),
                sql: "SELECT url, COUNT(*) AS hits FROM clicks GROUP BY url".to_string(),
                columns: vec![],
                namespace: "public".to_string(),
            },
            vec!["clicks".to_string()],
        );

        // Write 3 clicks, 2 for /home and 1 for /pricing
        let mut wb = WriteBatch::new();
        wb.put(b"view_output/clicks/r1", b"1\t/home\t100");
        wb.put(b"view_output/clicks/r2", b"2\t/home\t101");
        wb.put(b"view_output/clicks/r3", b"3\t/pricing\t102");
        shard.write_batch(wb).await.unwrap();

        let changed: HashSet<String> = ["clicks".to_string()].into();
        materialize_views(&catalog, &shard, &changed).await;

        // page_hits should have 2 rows (one per URL)
        let kvs = shard.scan_prefix(b"view_output/page_hits/").await.unwrap();
        assert_eq!(kvs.len(), 2, "expected 2 rows in page_hits (one per URL)");

        // Check that hits are correct: /home → 2, /pricing → 1
        let mut hit_map: HashMap<String, i64> = HashMap::new();
        for (_, v) in &kvs {
            let s = String::from_utf8_lossy(v);
            let parts: Vec<&str> = s.split('\t').collect();
            if parts.len() >= 2 {
                let url = parts[0].to_string();
                let hits: i64 = parts[1].parse().unwrap_or(0);
                hit_map.insert(url, hits);
            }
        }
        assert_eq!(hit_map.get("/home").copied(), Some(2));
        assert_eq!(hit_map.get("/pricing").copied(), Some(1));
    }
}
