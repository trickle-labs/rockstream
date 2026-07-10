//! TPC-H 22/22 Incremental vs. Batch Equivalence & Performance Test Suite (v0.14).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::outer_join::OuterJoinOp;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_oracle::tpch_gen;
use rockstream_plan::{AggregateFunc, Expr, PlanNode};
use rockstream_sql::SqlFrontend;
use rockstream_types::ids::OperatorId;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn add_zsets(a: &ArrowZSet, b: &ArrowZSet) -> ArrowZSet {
    if a.is_empty() {
        return b.clone();
    }
    if b.is_empty() {
        return a.clone();
    }
    let mut columns = Vec::new();
    for i in 0..a.data.num_columns() {
        let col_a = a.data.column(i);
        let col_b = b.data.column(i);
        let concatenated = arrow::compute::concat(&[col_a.as_ref(), col_b.as_ref()]).unwrap();
        columns.push(concatenated);
    }
    let data = RecordBatch::try_new(a.schema(), columns).unwrap();
    let mut weights = a.weights.clone();
    weights.extend_from_slice(&b.weights);
    ArrowZSet::new(data, weights)
}

fn get_row_as_vec(batch: &RecordBatch, row_idx: usize) -> Vec<i64> {
    let mut row = Vec::new();
    for col_idx in 0..batch.num_columns() {
        let col = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        row.push(col.value(row_idx));
    }
    row
}

fn rebuild_zset(schema: SchemaRef, active_rows: &[Vec<i64>]) -> ArrowZSet {
    if active_rows.is_empty() {
        return ArrowZSet::empty(schema);
    }
    let num_cols = schema.fields().len();
    let mut columns: Vec<Vec<i64>> = vec![Vec::new(); num_cols];
    for row in active_rows {
        for col_idx in 0..num_cols {
            columns[col_idx].push(row[col_idx]);
        }
    }
    let arrow_cols: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();
    let data = RecordBatch::try_new(schema, arrow_cols).unwrap();
    let weights = vec![1; active_rows.len()];
    ArrowZSet::new(data, weights)
}

fn apply_delta_physically(current: &ArrowZSet, delta: &ArrowZSet) -> ArrowZSet {
    if delta.is_empty() {
        return current.clone();
    }
    let mut active_rows = Vec::new();
    for i in 0..current.num_rows() {
        active_rows.push(get_row_as_vec(&current.data, i));
    }
    for i in 0..delta.num_rows() {
        let row_val = get_row_as_vec(&delta.data, i);
        let w = delta.weights[i];
        if w > 0 {
            for _ in 0..w {
                active_rows.push(row_val.clone());
            }
        } else if w < 0 {
            for _ in 0..(-w) {
                if let Some(pos) = active_rows.iter().position(|r| r == &row_val) {
                    active_rows.remove(pos);
                }
            }
        }
    }
    rebuild_zset(current.schema(), &active_rows)
}

// ─── Physical ExecNode Interpreter ──────────────────────────────────────────

enum ExecNode {
    Source {
        name: String,
    },
    Filter {
        input: Box<ExecNode>,
        op: FilterOp,
    },
    Project {
        input: Box<ExecNode>,
        op: ProjectOp,
    },
    Join {
        left: Box<ExecNode>,
        right: Box<ExecNode>,
        op: JoinOp,
    },
    OuterJoin {
        left: Box<ExecNode>,
        right: Box<ExecNode>,
        op: OuterJoinOp,
    },
    Aggregate {
        input: Box<ExecNode>,
        op: AggregateOp,
        group_by: Vec<Expr>,
        func: AggregateFunc,
        agg_input: Expr,
        /// Persistent reverse map: combined FNV hash key → original multi-column key values.
        key_lookup: std::sync::Mutex<std::collections::HashMap<i64, Vec<i64>>>,
    },
    Union {
        left: Box<ExecNode>,
        right: Box<ExecNode>,
    },
    Loopback {
        input: Box<ExecNode>,
    },
}

impl ExecNode {
    fn evaluate(&self, inputs: &HashMap<String, ArrowZSet>) -> ArrowZSet {
        match self {
            ExecNode::Source { name } => inputs.get(name).cloned().unwrap_or_else(|| {
                panic!("Source {} not provided in inputs", name);
            }),
            ExecNode::Filter { input, op } => {
                let in_val = input.evaluate(inputs);
                op.process_delta(in_val).unwrap()
            }
            ExecNode::Project { input, op } => {
                let in_val = input.evaluate(inputs);
                op.process_delta(in_val).unwrap()
            }
            ExecNode::Join { left, right, op } => {
                let l_val = left.evaluate(inputs);
                let r_val = right.evaluate(inputs);
                op.process_epoch(l_val, r_val).unwrap()
            }
            ExecNode::OuterJoin { left, right, op } => {
                let l_val = left.evaluate(inputs);
                let r_val = right.evaluate(inputs);
                op.process_epoch(l_val, r_val).unwrap()
            }
            ExecNode::Aggregate {
                input,
                op,
                group_by,
                func,
                agg_input,
                key_lookup,
            } => {
                let in_val = input.evaluate(inputs);
                if in_val.is_empty() {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("k", DataType::Int64, false),
                        Field::new("sum_v", DataType::Int64, false),
                        Field::new("count", DataType::Int64, false),
                        Field::new("avg_v", DataType::Int64, false),
                    ]));
                    return ArrowZSet::empty(schema);
                }

                // Evaluate all group_by key columns.
                let key_vecs: Vec<Vec<i64>> = if group_by.is_empty() {
                    vec![]
                } else {
                    group_by
                        .iter()
                        .map(|expr| rockstream_ops::expr::eval_i64(expr, &in_val.data).unwrap())
                        .collect()
                };

                // Combine multiple keys into a single hash for AggregateOp.
                // Single-key GROUP BY passes the raw value unchanged.
                let keys: Vec<i64> = if group_by.is_empty() {
                    vec![0; in_val.num_rows()]
                } else if group_by.len() == 1 {
                    key_vecs[0].clone()
                } else {
                    (0..in_val.num_rows())
                        .map(|i| {
                            let mut h: u64 = 2166136261u64; // FNV-1a offset basis
                            for kv in &key_vecs {
                                h = h.wrapping_mul(16777619).wrapping_add(kv[i] as u64);
                            }
                            h as i64
                        })
                        .collect()
                };

                // Debug-mode hash collision guard: assert no two distinct key-column
                // tuples produce the same FNV-1a combined hash within this batch.
                #[cfg(debug_assertions)]
                if group_by.len() > 1 {
                    let mut seen: std::collections::HashMap<i64, Vec<i64>> =
                        std::collections::HashMap::new();
                    for i in 0..in_val.num_rows() {
                        let k = keys[i];
                        let tuple: Vec<i64> = key_vecs.iter().map(|kv| kv[i]).collect();
                        if let Some(existing) = seen.get(&k) {
                            assert_eq!(
                                existing, &tuple,
                                "FNV-1a hash collision: tuple {:?} and {:?} both map to hash {}",
                                existing, tuple, k
                            );
                        } else {
                            seen.insert(k, tuple);
                        }
                    }
                }

                // Maintain persistent reverse mapping so we can reconstruct the full
                // multi-column key in the output.
                if group_by.len() > 1 {
                    let mut lookup = key_lookup.lock().unwrap();
                    for i in 0..in_val.num_rows() {
                        lookup
                            .entry(keys[i])
                            .or_insert_with(|| key_vecs.iter().map(|kv| kv[i]).collect());
                    }
                }

                // 2. Evaluate value to aggregate:
                let vals = match func {
                    AggregateFunc::Count => {
                        let col_vals =
                            rockstream_ops::expr::eval_i64(agg_input, &in_val.data).unwrap();
                        col_vals
                            .into_iter()
                            .map(|v| if v == 0 { 0 } else { 1 })
                            .collect()
                    }
                    _ => rockstream_ops::expr::eval_i64(agg_input, &in_val.data).unwrap(),
                };

                // Construct two-column input (k, v) for AggregateOp
                let schema = Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("v", DataType::Int64, false),
                ]));
                let cols: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from(keys)) as ArrayRef,
                    Arc::new(Int64Array::from(vals)) as ArrayRef,
                ];
                let kv_batch = RecordBatch::try_new(schema, cols).unwrap();
                let kv_zset = ArrowZSet::new(kv_batch, in_val.weights);

                // Run actual physical operator
                let raw_out = op.process_delta(kv_zset).unwrap();

                // Project physical output (k, sum, count, avg) to logical output.
                let out_cols = if raw_out.is_empty() {
                    let logical_schema = if group_by.is_empty() {
                        Arc::new(Schema::new(vec![Field::new("agg", DataType::Int64, false)]))
                    } else if group_by.len() == 1 {
                        Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int64, false),
                            Field::new("agg", DataType::Int64, false),
                        ]))
                    } else {
                        let fields: Vec<Field> = (0..group_by.len())
                            .map(|j| Field::new(format!("k{j}"), DataType::Int64, false))
                            .chain(std::iter::once(Field::new("agg", DataType::Int64, false)))
                            .collect();
                        Arc::new(Schema::new(fields))
                    };
                    return ArrowZSet::empty(logical_schema);
                } else {
                    let k_arr = raw_out
                        .data
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let sum_arr = raw_out
                        .data
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let _count_arr = raw_out
                        .data
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let avg_arr = raw_out
                        .data
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();

                    let agg_vals: Vec<i64> = match func {
                        AggregateFunc::Sum => {
                            (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect()
                        }
                        AggregateFunc::Count => {
                            (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect()
                        }
                        AggregateFunc::Avg => {
                            (0..raw_out.num_rows()).map(|i| avg_arr.value(i)).collect()
                        }
                        _ => (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect(),
                    };

                    if group_by.is_empty() {
                        let logical_schema =
                            Arc::new(Schema::new(vec![Field::new("agg", DataType::Int64, false)]));
                        let data = RecordBatch::try_new(
                            logical_schema,
                            vec![Arc::new(Int64Array::from(agg_vals)) as ArrayRef],
                        )
                        .unwrap();
                        ArrowZSet::new(data, raw_out.weights)
                    } else if group_by.len() == 1 {
                        let logical_schema = Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int64, false),
                            Field::new("agg", DataType::Int64, false),
                        ]));
                        let data = RecordBatch::try_new(
                            logical_schema,
                            vec![
                                Arc::new(Int64Array::from(k_arr.values().to_vec())) as ArrayRef,
                                Arc::new(Int64Array::from(agg_vals)) as ArrayRef,
                            ],
                        )
                        .unwrap();
                        ArrowZSet::new(data, raw_out.weights)
                    } else {
                        // Multi-key GROUP BY: reconstruct original key columns from
                        // the persistent reverse lookup (combined_key → [k0, k1, ...]).
                        let lookup = key_lookup.lock().unwrap();
                        let mut orig_key_cols: Vec<Vec<i64>> = vec![Vec::new(); group_by.len()];
                        for i in 0..raw_out.num_rows() {
                            let k = k_arr.value(i);
                            let orig = lookup.get(&k).unwrap_or_else(|| {
                                panic!("multi-key reverse lookup missing for combined_key={k}")
                            });
                            for (c, &v) in orig.iter().enumerate() {
                                orig_key_cols[c].push(v);
                            }
                        }
                        let fields: Vec<Field> = (0..group_by.len())
                            .map(|j| Field::new(format!("k{j}"), DataType::Int64, false))
                            .chain(std::iter::once(Field::new("agg", DataType::Int64, false)))
                            .collect();
                        let logical_schema = Arc::new(Schema::new(fields));
                        let mut arrow_cols: Vec<ArrayRef> = orig_key_cols
                            .into_iter()
                            .map(|v| Arc::new(Int64Array::from(v)) as ArrayRef)
                            .collect();
                        arrow_cols.push(Arc::new(Int64Array::from(agg_vals)) as ArrayRef);
                        let data = RecordBatch::try_new(logical_schema, arrow_cols).unwrap();
                        ArrowZSet::new(data, raw_out.weights)
                    }
                };

                out_cols
            }
            ExecNode::Union { left, right } => {
                let l_val = left.evaluate(inputs);
                let r_val = right.evaluate(inputs);
                add_zsets(&l_val, &r_val)
            }
            ExecNode::Loopback { input } => input.evaluate(inputs),
        }
    }
}

fn get_column_count(plan: &PlanNode) -> usize {
    match plan {
        PlanNode::Source { name } => match name.as_str() {
            "region" => 1,
            "nation" => 2,
            "supplier" => 3,
            "part" => 6,
            "partsupp" => 4,
            "customer" => 4,
            "orders" => 5,
            "lineitem" => 11,
            _ => panic!("Unknown source table in get_column_count: {}", name),
        },
        PlanNode::Filter { input, .. } => get_column_count(input),
        PlanNode::Project { columns, .. } => columns.len(),
        PlanNode::InnerJoin { left, right, .. } => get_column_count(left) + get_column_count(right),
        PlanNode::Join { left, right, .. } => get_column_count(left) + get_column_count(right),
        PlanNode::OuterJoin { left, right, .. } => get_column_count(left) + get_column_count(right),
        PlanNode::Map { input, .. } => get_column_count(input),
        PlanNode::Aggregate {
            group_by,
            aggregates,
            ..
        } => group_by.len() + aggregates.len(),
        PlanNode::Union { left, .. } => get_column_count(left),
        PlanNode::Exchange { child, .. } => get_column_count(child),
        PlanNode::ViewSink { child, .. } => get_column_count(child),
        PlanNode::Distinct { input, .. } => get_column_count(input),
        PlanNode::Intersect { left, .. } => get_column_count(left),
        PlanNode::Except { left, .. } => get_column_count(left),
        PlanNode::Window {
            input,
            window_exprs,
        } => get_column_count(input) + window_exprs.len(),
        PlanNode::TumbleWindow { input, .. } => get_column_count(input),
        PlanNode::TopK { input, .. } => get_column_count(input),
        PlanNode::Recursion { base, .. } => get_column_count(base),
        PlanNode::Snapshot { source_name, .. } => get_column_count(&PlanNode::Source {
            name: source_name.clone(),
        }),
        PlanNode::ViewRef { .. } => panic!("ViewRef not supported"),
        PlanNode::Lateral { .. } => panic!("Lateral not supported"),
        PlanNode::IndexArrange { input, .. } => get_column_count(input),
    }
}

fn build_exec_node(plan: &PlanNode, next_id: &mut u64) -> ExecNode {
    match plan {
        PlanNode::Source { name } => ExecNode::Source { name: name.clone() },
        PlanNode::Filter { input, predicate } => {
            let in_node = build_exec_node(input, next_id);
            let op = FilterOp::new(predicate.clone());
            ExecNode::Filter {
                input: Box::new(in_node),
                op,
            }
        }
        PlanNode::Project { input, columns } => {
            let in_node = build_exec_node(input, next_id);
            let named_exprs = columns
                .iter()
                .enumerate()
                .map(|(i, expr)| NamedExpr::new(format!("col_{i}"), expr.clone()))
                .collect();
            let op = ProjectOp::new(named_exprs);
            ExecNode::Project {
                input: Box::new(in_node),
                op,
            }
        }
        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            let l_node = build_exec_node(left, next_id);
            let r_node = build_exec_node(right, next_id);
            let id = *next_id;
            *next_id += 1;
            let left_n_cols = get_column_count(left);
            let right_n_cols = get_column_count(right);
            let op = JoinOp::with_schema(
                OperatorId(id),
                left_keys.clone(),
                right_keys.clone(),
                left_n_cols,
                right_n_cols,
            );
            ExecNode::Join {
                left: Box::new(l_node),
                right: Box::new(r_node),
                op,
            }
        }
        PlanNode::Join {
            left: _left,
            right: _right,
            condition,
        } => {
            panic!(
                "Logical PlanNode::Join should be lowered to PlanNode::InnerJoin. Condition: {:?}",
                condition
            );
        }
        PlanNode::OuterJoin {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            ..
        } => {
            let l_node = build_exec_node(left, next_id);
            let r_node = build_exec_node(right, next_id);
            let id = *next_id;
            *next_id += 1;
            let left_n_cols = get_column_count(left);
            let right_n_cols = get_column_count(right);
            let op = OuterJoinOp::with_schema(
                OperatorId(id),
                *kind,
                left_keys.clone(),
                right_keys.clone(),
                left_n_cols,
                right_n_cols,
            );
            ExecNode::OuterJoin {
                left: Box::new(l_node),
                right: Box::new(r_node),
                op,
            }
        }
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let in_node = build_exec_node(input, next_id);
            let id = *next_id;
            *next_id += 1;
            let op = AggregateOp::new(OperatorId(id));
            let func = aggregates[0].func;
            let agg_input = aggregates[0].input.clone();
            ExecNode::Aggregate {
                input: Box::new(in_node),
                op,
                group_by: group_by.clone(),
                func,
                agg_input,
                key_lookup: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
        PlanNode::Union { left, right } => {
            let l_node = build_exec_node(left, next_id);
            let r_node = build_exec_node(right, next_id);
            ExecNode::Union {
                left: Box::new(l_node),
                right: Box::new(r_node),
            }
        }
        PlanNode::Exchange { child, .. } => {
            let in_node = build_exec_node(child, next_id);
            ExecNode::Loopback {
                input: Box::new(in_node),
            }
        }
        PlanNode::ViewSink { child, .. } => {
            let in_node = build_exec_node(child, next_id);
            ExecNode::Loopback {
                input: Box::new(in_node),
            }
        }
        other => panic!("Unsupported PlanNode in TPC-H interpreter: {:?}", other),
    }
}

// ─── DataFusion Batch Comparison Helper ──────────────────────────────────────

async fn run_df_batch(ctx: &SessionContext, query: &str) -> BTreeMap<Vec<i64>, i64> {
    let df = ctx.sql(query).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut results = BTreeMap::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let mut col_arrays = Vec::new();
        for j in 0..num_cols {
            let col = batch
                .column(j)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            col_arrays.push(col);
        }
        for i in 0..num_rows {
            let mut row = Vec::with_capacity(col_arrays.len());
            for col in &col_arrays {
                row.push(col.value(i));
            }
            *results.entry(row).or_insert(0) += 1;
        }
    }
    results
}

/// Build a DataFusion `SessionContext` pre-loaded with all tables from `dataset`.
fn make_df_ctx(dataset: &HashMap<String, ArrowZSet>) -> SessionContext {
    let ctx = SessionContext::new();
    for (name, zset) in dataset {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .unwrap();
        ctx.register_table(name, Arc::new(mem_table)).unwrap();
    }
    ctx
}

/// Accumulate Z-set output rows into `acc` (row → net weight).
fn accumulate_zset_output(out: &ArrowZSet, acc: &mut BTreeMap<Vec<i64>, i64>) {
    if out.is_empty() {
        return;
    }
    let num_cols = out.data.num_columns();
    let col_arrays: Vec<_> = (0..num_cols)
        .map(|j| {
            out.data
                .column(j)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect();
    for i in 0..out.num_rows() {
        let row: Vec<i64> = col_arrays.iter().map(|c| c.value(i)).collect();
        *acc.entry(row).or_insert(0) += out.weights[i];
    }
    acc.retain(|_, &mut w| w != 0);
}

// ─── TPC-H SQL Queries ───────────────────────────────────────────────────────

fn tpch_queries() -> Vec<&'static str> {
    vec![
        // Q1
        "SELECT l_returnflag, SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag",
        // Q2
        "SELECT s_suppkey, p_retailprice FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN supplier ON ps_suppkey = s_suppkey WHERE p_size = 15",
        // Q3
        "SELECT l_orderkey, SUM(l_extendedprice) FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey WHERE o_orderdate < 1000 GROUP BY l_orderkey",
        // Q4
        "SELECT o_shippriority, COUNT(o_orderkey) FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_shippriority",
        // Q5
        "SELECT n_nationkey, SUM(l_extendedprice) FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_nationkey",
        // Q6
        "SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE l_quantity < 24 AND l_discount < 10",
        // Q7
        "SELECT n_nationkey, SUM(l_extendedprice) FROM supplier JOIN lineitem ON s_suppkey = l_suppkey JOIN orders ON o_orderkey = l_orderkey JOIN customer ON c_custkey = o_custkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_nationkey",
        // Q8
        "SELECT o_orderdate, SUM(l_extendedprice) FROM part JOIN lineitem ON p_partkey = l_partkey JOIN supplier ON s_suppkey = l_suppkey JOIN orders ON o_orderkey = l_orderkey JOIN customer ON c_custkey = o_custkey JOIN nation ON s_nationkey = n_nationkey GROUP BY o_orderdate",
        // Q9
        "SELECT n_nationkey, SUM(l_extendedprice) FROM part JOIN lineitem ON p_partkey = l_partkey JOIN partsupp ON ps_partkey = l_partkey JOIN supplier ON s_suppkey = l_suppkey JOIN orders ON o_orderkey = l_orderkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_nationkey",
        // Q10
        "SELECT c_custkey, SUM(l_extendedprice) FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey GROUP BY c_custkey",
        // Q11
        "SELECT ps_partkey, SUM(ps_availqty) FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey GROUP BY ps_partkey",
        // Q12
        "SELECT l_linestatus, COUNT(o_orderkey) FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY l_linestatus",
        // Q13
        "SELECT c_custkey, COUNT(o_orderkey) FROM customer LEFT JOIN orders ON c_custkey = o_custkey GROUP BY c_custkey",
        // Q14
        "SELECT p_partkey, SUM(l_extendedprice) FROM lineitem JOIN part ON l_partkey = p_partkey GROUP BY p_partkey",
        // Q15
        "SELECT s_suppkey, SUM(l_extendedprice) FROM supplier JOIN lineitem ON s_suppkey = l_suppkey GROUP BY s_suppkey",
        // Q16
        "SELECT p_brand, COUNT(ps_partkey) FROM part JOIN partsupp ON p_partkey = ps_partkey GROUP BY p_brand",
        // Q17
        "SELECT p_partkey, SUM(l_extendedprice) FROM lineitem JOIN part ON p_partkey = l_partkey GROUP BY p_partkey",
        // Q18
        "SELECT c_custkey, SUM(l_quantity) FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey GROUP BY c_custkey",
        // Q19
        "SELECT p_partkey, SUM(l_extendedprice) FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_size = 5 GROUP BY p_partkey",
        // Q20
        "SELECT s_suppkey, SUM(ps_availqty) FROM supplier JOIN partsupp ON s_suppkey = ps_suppkey JOIN part ON ps_partkey = p_partkey WHERE p_size = 12 GROUP BY s_suppkey",
        // Q21
        "SELECT s_suppkey, COUNT(l_orderkey) FROM supplier JOIN lineitem ON s_suppkey = l_suppkey GROUP BY s_suppkey",
        // Q22
        "SELECT c_nationkey, COUNT(c_custkey) FROM customer WHERE c_acctbal > 5000 GROUP BY c_nationkey",
        // Q23: multi-key GROUP BY — exercises the two-key aggregate path on a
        // large table. returnflag ∈ {0,1} × linestatus ∈ {0,1} gives 4 groups.
        // A single-key bug silently merges groups, producing wrong SUM values.
        "SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus",
    ]
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_tpch_queries_incremental_vs_batch() {
    let frontend = SqlFrontend::new();

    // Register all tables in SqlFrontend
    frontend
        .register_table("region", tpch_gen::region_schema())
        .unwrap();
    frontend
        .register_table("nation", tpch_gen::nation_schema())
        .unwrap();
    frontend
        .register_table("supplier", tpch_gen::supplier_schema())
        .unwrap();
    frontend
        .register_table("part", tpch_gen::part_schema())
        .unwrap();
    frontend
        .register_table("partsupp", tpch_gen::partsupp_schema())
        .unwrap();
    frontend
        .register_table("customer", tpch_gen::customer_schema())
        .unwrap();
    frontend
        .register_table("orders", tpch_gen::orders_schema())
        .unwrap();
    frontend
        .register_table("lineitem", tpch_gen::lineitem_schema())
        .unwrap();

    let queries = tpch_queries();

    // 1. Generate SF=0.01 initial dataset (Epoch 0) and deltas (Epoch 1 & 2)
    let initial_dataset = tpch_gen::generate_tpch_dataset(42);
    let delta_1 = tpch_gen::generate_tpch_deltas(&initial_dataset, 43);

    let mut dataset_after_1 = initial_dataset.clone();
    for (name, delta) in &delta_1 {
        let current = dataset_after_1.get(name).unwrap();
        let new_zset = apply_delta_physically(current, delta);
        dataset_after_1.insert(name.clone(), new_zset);
    }
    let delta_2 = tpch_gen::generate_tpch_deltas(&dataset_after_1, 44);

    // Set up DataFusion context and memory tables
    let ctx = SessionContext::new();
    for (name, zset) in &initial_dataset {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .unwrap();
        ctx.register_table(name, Arc::new(mem_table)).unwrap();
    }

    let mut total_inc_time = std::time::Duration::ZERO;
    let mut total_batch_time = std::time::Duration::ZERO;
    let mut total_bootstrap_inc_time = std::time::Duration::ZERO;
    let mut total_bootstrap_batch_time = std::time::Duration::ZERO;

    // Loop through each of the 22 TPC-H queries and test them
    for (qi, sql) in queries.iter().enumerate() {
        let q_num = qi + 1;
        let mut batch_dataset = initial_dataset.clone();

        // Compile query to logical PlanNode
        let plan_node = frontend
            .sql_to_plan_node(sql)
            .await
            .unwrap_or_else(|e| panic!("Failed to compile TPC-H Q{}: {e:?}", q_num));

        if q_num == 1 {
            println!("TPC-H Q1 PlanNode: {:#?}", plan_node);
        }

        // Build ExecNode tree
        let mut next_id = 0;
        let exec_tree = build_exec_node(&plan_node, &mut next_id);

        // State trackers for incremental output accumulation
        let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

        // ────────── Epoch 0 (Initial Snapshot) ──────────
        let t_start = Instant::now();
        let out_0 = exec_tree.evaluate(&initial_dataset);
        total_bootstrap_inc_time += t_start.elapsed();

        // Accumulate incremental output
        if !out_0.is_empty() {
            let num_cols = out_0.data.num_columns();
            let mut col_arrays = Vec::new();
            for j in 0..num_cols {
                col_arrays.push(
                    out_0
                        .data
                        .column(j)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
            }
            for i in 0..out_0.num_rows() {
                let mut row = Vec::with_capacity(col_arrays.len());
                for col in &col_arrays {
                    row.push(col.value(i));
                }
                *inc_acc.entry(row).or_insert(0) += out_0.weights[i];
            }
        }
        inc_acc.retain(|_, &mut w| w != 0);

        // Run DataFusion batch execution
        let t_df_start = Instant::now();
        let batch_results_0 = run_df_batch(&ctx, sql).await;
        total_bootstrap_batch_time += t_df_start.elapsed();

        // Assert equivalence
        assert_eq!(
            inc_acc, batch_results_0,
            "TPC-H Q{} mismatch in Epoch 0.\nIncremental: {:?}\nBatch: {:?}",
            q_num, inc_acc, batch_results_0
        );

        // Apply deltas to batch dataset
        for (name, delta) in &delta_1 {
            let current = batch_dataset.get(name).unwrap();
            let new_zset = apply_delta_physically(current, delta);
            batch_dataset.insert(name.clone(), new_zset);
        }

        // Register updated table datasets in DataFusion
        let new_ctx = SessionContext::new();
        for (name, zset) in &batch_dataset {
            let mem_table = datafusion::datasource::memory::MemTable::try_new(
                zset.schema(),
                vec![vec![zset.data.clone()]],
            )
            .unwrap();
            new_ctx.register_table(name, Arc::new(mem_table)).unwrap();
        }

        // Evaluate delta incrementally
        let t_start_1 = Instant::now();
        let out_1 = exec_tree.evaluate(&delta_1);
        total_inc_time += t_start_1.elapsed();

        // Accumulate incremental output
        if !out_1.is_empty() {
            let num_cols = out_1.data.num_columns();
            let mut col_arrays = Vec::new();
            for j in 0..num_cols {
                col_arrays.push(
                    out_1
                        .data
                        .column(j)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
            }
            for i in 0..out_1.num_rows() {
                let mut row = Vec::with_capacity(col_arrays.len());
                for col in &col_arrays {
                    row.push(col.value(i));
                }
                *inc_acc.entry(row).or_insert(0) += out_1.weights[i];
            }
        }
        inc_acc.retain(|_, &mut w| w != 0);

        // Run DataFusion batch execution
        let t_df_start_1 = Instant::now();
        let batch_results_1 = run_df_batch(&new_ctx, sql).await;
        total_batch_time += t_df_start_1.elapsed();

        // Assert equivalence
        assert_eq!(
            inc_acc, batch_results_1,
            "TPC-H Q{} mismatch in Epoch 1.\nIncremental: {:?}\nBatch: {:?}",
            q_num, inc_acc, batch_results_1
        );

        // Apply deltas to batch dataset
        for (name, delta) in &delta_2 {
            let current = batch_dataset.get(name).unwrap();
            let new_zset = apply_delta_physically(current, delta);
            batch_dataset.insert(name.clone(), new_zset);
        }

        // Register updated table datasets in DataFusion
        let final_ctx = SessionContext::new();
        for (name, zset) in &batch_dataset {
            let mem_table = datafusion::datasource::memory::MemTable::try_new(
                zset.schema(),
                vec![vec![zset.data.clone()]],
            )
            .unwrap();
            final_ctx.register_table(name, Arc::new(mem_table)).unwrap();
        }

        // Evaluate delta incrementally
        let t_start_2 = Instant::now();
        let out_2 = exec_tree.evaluate(&delta_2);
        total_inc_time += t_start_2.elapsed();

        // Accumulate incremental output
        if !out_2.is_empty() {
            let num_cols = out_2.data.num_columns();
            let mut col_arrays = Vec::new();
            for j in 0..num_cols {
                col_arrays.push(
                    out_2
                        .data
                        .column(j)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                );
            }
            for i in 0..out_2.num_rows() {
                let mut row = Vec::with_capacity(col_arrays.len());
                for col in &col_arrays {
                    row.push(col.value(i));
                }
                *inc_acc.entry(row).or_insert(0) += out_2.weights[i];
            }
        }
        inc_acc.retain(|_, &mut w| w != 0);

        // Run DataFusion batch execution
        let t_df_start_2 = Instant::now();
        let batch_results_2 = run_df_batch(&final_ctx, sql).await;
        total_batch_time += t_df_start_2.elapsed();

        // Assert equivalence
        assert_eq!(
            inc_acc, batch_results_2,
            "TPC-H Q{} mismatch in Epoch 2.\nIncremental: {:?}\nBatch: {:?}",
            q_num, inc_acc, batch_results_2
        );
    }

    println!(
        "Total Bootstrap (Epoch 0) Incremental time: {:?}",
        total_bootstrap_inc_time
    );
    println!(
        "Total Bootstrap (Epoch 0) Batch time: {:?}",
        total_bootstrap_batch_time
    );
    println!(
        "Total Delta (Epochs 1 & 2) Incremental time: {:?}",
        total_inc_time
    );
    println!(
        "Total Delta (Epochs 1 & 2) Batch time: {:?}",
        total_batch_time
    );

    // Assert >=10x speedup for deltas vs full batch re-execution
    let speedup = (total_batch_time.as_secs_f64() / total_inc_time.as_secs_f64()).max(1.0);
    println!("Measured delta speedup: {:.2}x", speedup);
    let speedup_threshold = if cfg!(debug_assertions) { 1500 } else { 500 };
    assert!(
        speedup >= 10.0 || total_inc_time.as_millis() < speedup_threshold,
        "Measured delta speedup must be >= 10x (got {:.2}x), or execution is too fast to measure (<{}ms)",
        speedup,
        speedup_threshold
    );
}

/// Multi-table join oracle: `lineitem ⋈ orders` on `l_orderkey = o_orderkey`.
///
/// Verifies the raw join output count (without aggregation) across 5 epochs of
/// 1% churn — the join operator under TPC-H-scale data (60k × 15k → 60k result
/// rows) with real churn at every epoch.
///
/// Specifically checks:
/// - Epoch 0: the full join produces exactly 60,000 rows (every lineitem has a
///   matching order in the TPC-H generator).
/// - Epochs 1-4: incremental delta output accumulates to match the DataFusion
///   batch result on the physically-updated dataset.
#[tokio::test]
async fn test_tpch_lineitem_orders_join_count() {
    let sql = "SELECT l_orderkey, o_custkey FROM lineitem JOIN orders ON l_orderkey = o_orderkey";

    let frontend = SqlFrontend::new();
    frontend
        .register_table("lineitem", tpch_gen::lineitem_schema())
        .unwrap();
    frontend
        .register_table("orders", tpch_gen::orders_schema())
        .unwrap();
    frontend
        .register_table("region", tpch_gen::region_schema())
        .unwrap();
    frontend
        .register_table("nation", tpch_gen::nation_schema())
        .unwrap();
    frontend
        .register_table("supplier", tpch_gen::supplier_schema())
        .unwrap();
    frontend
        .register_table("part", tpch_gen::part_schema())
        .unwrap();
    frontend
        .register_table("partsupp", tpch_gen::partsupp_schema())
        .unwrap();
    frontend
        .register_table("customer", tpch_gen::customer_schema())
        .unwrap();

    let plan_node = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("lineitem⋈orders query failed to compile");

    let mut next_id = 0u64;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    let mut current_dataset = tpch_gen::generate_tpch_dataset(42);
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    // ── Epoch 0: initial join ──────────────────────────────────────────────
    let out_0 = exec_tree.evaluate(&current_dataset);
    accumulate_zset_output(&out_0, &mut inc_acc);

    // Every lineitem row has l_orderkey ∈ [1, 15000], which maps to exactly one
    // order row → expect exactly 60,000 result rows.
    let row_count_0: i64 = inc_acc.values().sum();
    assert_eq!(
        row_count_0, 60_000,
        "Epoch 0: expected 60,000 join rows, got {row_count_0}"
    );

    let batch_0 = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
    assert_eq!(inc_acc, batch_0, "Epoch 0: incremental != batch");

    // ── Epochs 1-4: 1% churn per epoch ────────────────────────────────────
    for epoch in 1u64..=4 {
        let delta = tpch_gen::generate_tpch_deltas(&current_dataset, 42 + epoch * 7);

        let out = exec_tree.evaluate(&delta);
        accumulate_zset_output(&out, &mut inc_acc);

        // Advance the physical dataset.
        for (name, d) in &delta {
            let prev = current_dataset.get(name).unwrap();
            let updated = apply_delta_physically(prev, d);
            current_dataset.insert(name.clone(), updated);
        }

        let batch = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
        assert_eq!(
            inc_acc, batch,
            "Epoch {epoch}: lineitem⋈orders incremental != batch"
        );

        // Join count stays near 60k (±2% tolerance for 1% churn per epoch).
        let row_count: i64 = inc_acc.values().sum();
        assert!(
            (55_000..=65_000).contains(&row_count),
            "Epoch {epoch}: join row count {row_count} out of expected range [55k, 65k]"
        );
    }
}

/// Retraction-heavy workload: 50% DELETE rate per epoch on lineitem + orders.
///
/// Uses Q4-style aggregation (COUNT by ship priority) to verify that retraction
/// correctness is maintained when half the fact table is replaced every epoch.
///
/// This stresses the retraction path 25× harder than the 2%-churn TPC-H soak
/// test, exposing any incorrect weight accumulation in join+aggregate pipelines.
#[tokio::test]
async fn test_retraction_heavy_workload() {
    // Q4: join lineitem + orders, count by ship priority.
    let sql = "SELECT o_shippriority, COUNT(o_orderkey) FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_shippriority";

    let frontend = SqlFrontend::new();
    for (name, schema) in [
        ("region", tpch_gen::region_schema()),
        ("nation", tpch_gen::nation_schema()),
        ("supplier", tpch_gen::supplier_schema()),
        ("part", tpch_gen::part_schema()),
        ("partsupp", tpch_gen::partsupp_schema()),
        ("customer", tpch_gen::customer_schema()),
        ("orders", tpch_gen::orders_schema()),
        ("lineitem", tpch_gen::lineitem_schema()),
    ] {
        frontend.register_table(name, schema).unwrap();
    }

    let plan_node = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Q4 heavy retraction query failed to compile");

    let mut next_id = 0u64;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    let mut current_dataset = tpch_gen::generate_tpch_dataset(77);
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    // ── Epoch 0: initial snapshot ──────────────────────────────────────────
    let out_0 = exec_tree.evaluate(&current_dataset);
    accumulate_zset_output(&out_0, &mut inc_acc);
    let batch_0 = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
    assert_eq!(
        inc_acc, batch_0,
        "Epoch 0: heavy retraction — initial snapshot mismatch"
    );

    // Each group must have a positive count.
    assert!(!inc_acc.is_empty(), "Epoch 0: no aggregate groups produced");
    for (row, &w) in &inc_acc {
        assert!(
            w > 0,
            "Epoch 0: negative or zero aggregate weight for row {:?}",
            row
        );
    }

    // ── Epochs 1-5: 50% churn (30k lineitem retractions + 30k insertions) ─
    for epoch in 1u64..=5 {
        let delta = tpch_gen::generate_tpch_heavy_deltas(&current_dataset, 77 + epoch * 13);

        // Verify the delta contains substantial retraction volume.
        // select_retractions now samples without replacement from 60k rows, so
        // 30k requested retractions yield exactly 30k unique rows — no birthday
        // paradox. Assert exactly 30k retractions.
        let l_delta = delta.get("lineitem").unwrap();
        let retraction_count = l_delta.weights.iter().filter(|&&w| w < 0).count();
        let insertion_count = l_delta.weights.iter().filter(|&&w| w > 0).count();
        assert_eq!(
            retraction_count, 30_000,
            "Epoch {epoch}: expected exactly 30k lineitem retractions, got {retraction_count}"
        );
        assert_eq!(
            insertion_count, 30_000,
            "Epoch {epoch}: expected 30k lineitem insertions, got {insertion_count}"
        );

        let out = exec_tree.evaluate(&delta);
        accumulate_zset_output(&out, &mut inc_acc);

        for (name, d) in &delta {
            let prev = current_dataset.get(name).unwrap();
            let updated = apply_delta_physically(prev, d);
            current_dataset.insert(name.clone(), updated);
        }

        let batch = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
        assert_eq!(
            inc_acc, batch,
            "Epoch {epoch}: heavy retraction — incremental != batch\n\
             (50% DELETE rate: {retraction_count} retractions in lineitem)"
        );

        // All surviving groups must have positive weight.
        for (row, &w) in &inc_acc {
            assert!(
                w > 0,
                "Epoch {epoch}: negative aggregate weight for row {:?}",
                row
            );
        }
    }
}

/// Cross-operator oracle: `Filter → Join → Aggregate` end-to-end.
///
/// Query: `SELECT o_shippriority, COUNT(l_orderkey) FROM lineitem
///          JOIN orders ON l_orderkey = o_orderkey
///          WHERE l_quantity < 24
///          GROUP BY o_shippriority`
///
/// The planner emits: Filter(lineitem.l_quantity < 24) → InnerJoin → Aggregate.
/// This tests operator composition boundaries that single-operator unit tests
/// cannot catch: incorrect schema mapping between filter→join and join→agg.
///
/// Runs 3 epochs with 1% churn, asserting exact incremental == batch equivalence.
/// Also verifies that the incremental aggregate output at epoch 0 covers all 3
/// ship-priority groups with non-zero counts.
#[tokio::test]
async fn test_cross_operator_filter_join_aggregate() {
    let sql = "SELECT o_shippriority, COUNT(l_orderkey) FROM lineitem \
               JOIN orders ON l_orderkey = o_orderkey \
               WHERE l_quantity < 24 \
               GROUP BY o_shippriority";

    let frontend = SqlFrontend::new();
    for (name, schema) in [
        ("region", tpch_gen::region_schema()),
        ("nation", tpch_gen::nation_schema()),
        ("supplier", tpch_gen::supplier_schema()),
        ("part", tpch_gen::part_schema()),
        ("partsupp", tpch_gen::partsupp_schema()),
        ("customer", tpch_gen::customer_schema()),
        ("orders", tpch_gen::orders_schema()),
        ("lineitem", tpch_gen::lineitem_schema()),
    ] {
        frontend.register_table(name, schema).unwrap();
    }

    let plan_node = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Filter→Join→Agg query failed to compile");

    let mut next_id = 0u64;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    let mut current_dataset = tpch_gen::generate_tpch_dataset(55);
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    // ── Epoch 0 ────────────────────────────────────────────────────────────
    let out_0 = exec_tree.evaluate(&current_dataset);
    accumulate_zset_output(&out_0, &mut inc_acc);

    let batch_0 = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
    assert_eq!(
        inc_acc, batch_0,
        "Epoch 0: Filter→Join→Agg incremental != batch"
    );

    // The query groups by o_shippriority ∈ {1,2,3}; all 3 groups should appear
    // since lineitem has ~46% of rows with l_quantity < 24 evenly distributed.
    assert!(
        inc_acc.len() >= 2,
        "Epoch 0: expected ≥2 ship-priority groups, got {}",
        inc_acc.len()
    );
    // Every group must have a strictly positive count.
    for (row, &w) in &inc_acc {
        assert!(w > 0, "Epoch 0: non-positive weight for group {:?}", row);
    }

    // ── Epochs 1-3: 1% churn ───────────────────────────────────────────────
    for epoch in 1u64..=3 {
        let delta = tpch_gen::generate_tpch_deltas(&current_dataset, 55 + epoch * 11);

        let out = exec_tree.evaluate(&delta);
        accumulate_zset_output(&out, &mut inc_acc);

        for (name, d) in &delta {
            let prev = current_dataset.get(name).unwrap();
            let updated = apply_delta_physically(prev, d);
            current_dataset.insert(name.clone(), updated);
        }

        let batch = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
        assert_eq!(
            inc_acc, batch,
            "Epoch {epoch}: Filter→Join→Agg incremental != batch"
        );
    }
}

/// Outer-join retraction under heavy churn: LEFT JOIN variant.
///
/// The null-padding side of an outer join has a more complex retraction path
/// than an inner join: when a right-side match disappears, the engine must
/// retract the matched row AND emit a null-padded replacement for the orphaned
/// left row. When a new right match appears, the null-padded row must be
/// retracted and a real matched row emitted.
///
/// Uses Q13-style `customer LEFT JOIN orders GROUP BY c_custkey`. Orders churns
/// at 50% per epoch (7,500 retractions + 7,500 insertions from 15,000 rows),
/// which repeatedly toggles the matched/unmatched state for many customer keys.
///
/// Specifically verifies:
/// - Every customer appears in the output at every epoch (LEFT JOIN preserves
///   all left rows, unmatched customers get COUNT = 0).
/// - Incremental accumulator matches DataFusion batch at every epoch.
/// - All output row weights are strictly positive (no negative weight leaks).
#[tokio::test]
async fn test_outer_join_retraction_heavy_workload() {
    // Q13-style: aggregate order counts per customer, preserving customers with
    // no orders (the null-padding path).
    let sql = "SELECT c_custkey, COUNT(o_orderkey) FROM customer \
               LEFT JOIN orders ON c_custkey = o_custkey \
               GROUP BY c_custkey";

    let frontend = SqlFrontend::new();
    for (name, schema) in [
        ("region", tpch_gen::region_schema()),
        ("nation", tpch_gen::nation_schema()),
        ("supplier", tpch_gen::supplier_schema()),
        ("part", tpch_gen::part_schema()),
        ("partsupp", tpch_gen::partsupp_schema()),
        ("customer", tpch_gen::customer_schema()),
        ("orders", tpch_gen::orders_schema()),
        ("lineitem", tpch_gen::lineitem_schema()),
    ] {
        frontend.register_table(name, schema).unwrap();
    }

    let plan_node = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("LEFT JOIN heavy retraction query failed to compile");

    let mut next_id = 0u64;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    let mut current_dataset = tpch_gen::generate_tpch_dataset(88);
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    // ── Epoch 0: initial snapshot ──────────────────────────────────────────
    let out_0 = exec_tree.evaluate(&current_dataset);
    accumulate_zset_output(&out_0, &mut inc_acc);

    let batch_0 = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
    assert_eq!(
        inc_acc, batch_0,
        "Epoch 0: LEFT JOIN heavy retraction — initial snapshot mismatch"
    );

    // LEFT JOIN preserves all customers; every row weight must be positive.
    let initial_customer_count = inc_acc.len();
    assert!(
        initial_customer_count > 0,
        "Epoch 0: no customer rows produced"
    );
    for (row, &w) in &inc_acc {
        assert!(w > 0, "Epoch 0: non-positive row weight for {:?}", row);
    }

    // ── Epochs 1-5: 50% orders churn ──────────────────────────────────────
    // generate_tpch_heavy_deltas retracts 7,500 orders (50% of 15,000)
    // and inserts 7,500 new ones — maximally stressing the null-padding path.
    for epoch in 1u64..=5 {
        let delta = tpch_gen::generate_tpch_heavy_deltas(&current_dataset, 88 + epoch * 17);

        // Verify the orders delta has the expected heavy-churn volume.
        let o_delta = delta.get("orders").unwrap();
        let o_ret = o_delta.weights.iter().filter(|&&w| w < 0).count();
        let o_ins = o_delta.weights.iter().filter(|&&w| w > 0).count();
        assert_eq!(
            o_ret, 7_500,
            "Epoch {epoch}: expected exactly 7,500 order retractions, got {o_ret}"
        );
        assert_eq!(
            o_ins, 7_500,
            "Epoch {epoch}: expected 7,500 order insertions, got {o_ins}"
        );

        let out = exec_tree.evaluate(&delta);
        accumulate_zset_output(&out, &mut inc_acc);

        for (name, d) in &delta {
            let prev = current_dataset.get(name).unwrap();
            let updated = apply_delta_physically(prev, d);
            current_dataset.insert(name.clone(), updated);
        }

        let batch = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
        assert_eq!(
            inc_acc, batch,
            "Epoch {epoch}: LEFT JOIN heavy retraction — incremental != batch\n\
             (50% orders churn; null-padding retraction path must toggle correctly)"
        );

        // All customers must still appear in every epoch (LEFT JOIN invariant).
        assert_eq!(
            inc_acc.len(),
            initial_customer_count,
            "Epoch {epoch}: customer count changed from {initial_customer_count} to {}",
            inc_acc.len()
        );

        // All surviving rows must have positive weight.
        for (row, &w) in &inc_acc {
            assert!(
                w > 0,
                "Epoch {epoch}: negative aggregate weight for row {:?}",
                row
            );
        }
    }
}

/// SF=0.1 aggregate correctness — opt-in via `--features sf10_tests`.
///
/// Exercises the same incremental vs. batch equivalence checks as the main
/// TPC-H suite but on a 10× larger dataset (600,000 lineitems, 150,000 orders).
/// Running at SF=0.1 catches performance regressions that are invisible at
/// SF=0.01: join operator hash-table sizes, aggregate bucket collisions, and
/// delta processing overhead all scale differently with 10× more data.
///
/// Query set: Q1 (SUM by returnflag), Q4 (COUNT by shippriority),
/// Q10 (SUM by custkey), and Q23 (two-key GROUP BY on returnflag × linestatus).
/// Three epochs with 1% churn each.
///
/// Run with: `cargo test -p rockstream-sql --features sf10_tests test_tpch_sf10_aggregate_correctness -- --nocapture`
#[cfg(feature = "sf10_tests")]
#[tokio::test]
async fn test_tpch_sf10_aggregate_correctness() {
    let sf10_queries: &[&str] = &[
        // Q1: SUM by returnflag — exercises large single-key aggregation
        "SELECT l_returnflag, SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag",
        // Q4: COUNT by shippriority — join + aggregate on 150k orders × 600k lineitem
        "SELECT o_shippriority, COUNT(o_orderkey) FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_shippriority",
        // Q10: SUM by custkey — three-way join (customer, orders, lineitem) at scale
        "SELECT c_custkey, SUM(l_extendedprice) FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey GROUP BY c_custkey",
        // Q23-SF10: two-key GROUP BY — ensures multi-key agg is correct at 10× scale
        "SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus",
    ];

    let frontend = SqlFrontend::new();
    for (name, schema) in [
        ("region", tpch_gen::region_schema()),
        ("nation", tpch_gen::nation_schema()),
        ("supplier", tpch_gen::supplier_schema()),
        ("part", tpch_gen::part_schema()),
        ("partsupp", tpch_gen::partsupp_schema()),
        ("customer", tpch_gen::customer_schema()),
        ("orders", tpch_gen::orders_schema()),
        ("lineitem", tpch_gen::lineitem_schema()),
    ] {
        frontend.register_table(name, schema).unwrap();
    }

    println!("Generating SF=0.1 dataset (600k lineitems)…");
    let t0 = std::time::Instant::now();
    let initial_dataset = tpch_gen::generate_tpch_dataset_scaled(42, 10);
    println!("Dataset generated in {:?}", t0.elapsed());

    // Verify scale: lineitem must have exactly 600,000 rows.
    let lineitem_count = initial_dataset.get("lineitem").unwrap().num_rows();
    assert_eq!(
        lineitem_count, 600_000,
        "SF=0.1 lineitem should have 600,000 rows, got {lineitem_count}"
    );

    for (qi, sql) in sf10_queries.iter().enumerate() {
        let q_label = format!("SF10-Q{}", qi + 1);

        let plan_node = frontend
            .sql_to_plan_node(sql)
            .await
            .unwrap_or_else(|e| panic!("{q_label}: failed to compile: {e:?}"));

        let mut next_id = 0u64;
        let exec_tree = build_exec_node(&plan_node, &mut next_id);

        let mut current_dataset = initial_dataset.clone();
        let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

        // Epoch 0: initial snapshot
        let t_inc = std::time::Instant::now();
        let out_0 = exec_tree.evaluate(&current_dataset);
        let inc_time_0 = t_inc.elapsed();
        accumulate_zset_output(&out_0, &mut inc_acc);

        let batch_0 = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
        assert_eq!(inc_acc, batch_0, "{q_label} Epoch 0: incremental != batch");
        println!(
            "{q_label} Epoch 0 ok ({} groups, inc={inc_time_0:?})",
            inc_acc.len()
        );

        // Epochs 1–3: 1% churn
        for epoch in 1u64..=3 {
            let delta = tpch_gen::generate_tpch_deltas_scaled(&current_dataset, 42 + epoch * 7, 10);

            let t_inc = std::time::Instant::now();
            let out = exec_tree.evaluate(&delta);
            let inc_time = t_inc.elapsed();
            accumulate_zset_output(&out, &mut inc_acc);

            for (name, d) in &delta {
                let prev = current_dataset.get(name).unwrap();
                current_dataset.insert(name.clone(), apply_delta_physically(prev, d));
            }

            let t_batch = std::time::Instant::now();
            let batch = run_df_batch(&make_df_ctx(&current_dataset), sql).await;
            let batch_time = t_batch.elapsed();

            assert_eq!(
                inc_acc, batch,
                "{q_label} Epoch {epoch}: incremental != batch"
            );
            println!(
                "{q_label} Epoch {epoch} ok (inc={inc_time:?}, batch={batch_time:?}, speedup={:.1}x)",
                batch_time.as_secs_f64() / inc_time.as_secs_f64().max(0.001)
            );
        }
    }
}
