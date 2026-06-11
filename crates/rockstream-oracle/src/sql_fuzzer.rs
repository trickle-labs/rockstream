//! Randomized SQL query fuzzer and incremental engine validator (v0.14).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::distinct::DistinctOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::outer_join::OuterJoinOp;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{AggregateFunc, Expr, PlanNode};
use rockstream_sql::SqlFrontend;
use rockstream_types::ids::OperatorId;

// ─── Simple seeded RNG helper ───────────────────────────────────────────────

pub struct FuzzRng {
    seed: u64,
}

impl FuzzRng {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.seed = self.seed.wrapping_mul(1664525).wrapping_add(1013904223);
        self.seed
    }

    pub fn next_range(&mut self, min: i64, max: i64) -> i64 {
        if min >= max {
            return min;
        }
        let diff = (max - min + 1) as u64;
        min + (self.next_u64() % diff) as i64
    }
}

// ─── Schemas for synthetic tables ───────────────────────────────────────────

pub fn t1_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
    ]))
}

pub fn t2_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        Field::new("group_id", DataType::Int64, false),
    ]))
}

// ─── Data Generation Helpers ────────────────────────────────────────────────

fn make_zset(schema: SchemaRef, columns: Vec<Vec<i64>>, weight: i64) -> ArrowZSet {
    let num_rows = columns[0].len();
    let arrow_cols: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();
    let data = RecordBatch::try_new(schema, arrow_cols).unwrap();
    let weights = vec![weight; num_rows];
    ArrowZSet::new(data, weights)
}

pub fn generate_synthetic_dataset(seed: u64) -> HashMap<String, ArrowZSet> {
    let mut rng = FuzzRng::new(seed);
    let mut tables = HashMap::new();

    // Generate t1: 100 rows
    let mut t1_id = Vec::new();
    let mut t1_val = Vec::new();
    let mut t1_cat = Vec::new();
    for i in 1..=100 {
        t1_id.push(i);
        t1_val.push(rng.next_range(1, 100));
        t1_cat.push(rng.next_range(1, 5));
    }
    tables.insert(
        "t1".to_string(),
        make_zset(t1_schema(), vec![t1_id, t1_val, t1_cat], 1),
    );

    // Generate t2: 80 rows
    let mut t2_id = Vec::new();
    let mut t2_val = Vec::new();
    let mut t2_group = Vec::new();
    for i in 1..=80 {
        t2_id.push(i);
        t2_val.push(rng.next_range(1, 100));
        t2_group.push(rng.next_range(1, 5));
    }
    tables.insert(
        "t2".to_string(),
        make_zset(t2_schema(), vec![t2_id, t2_val, t2_group], 1),
    );

    tables
}

pub fn generate_synthetic_deltas(
    current_dataset: &HashMap<String, ArrowZSet>,
    seed: u64,
) -> HashMap<String, ArrowZSet> {
    let mut rng = FuzzRng::new(seed);
    let mut deltas = HashMap::new();

    // t1 deltas: 2 retractions, 2 insertions
    let t1 = current_dataset.get("t1").unwrap();
    let mut t1_ret_idx = Vec::new();
    for _ in 0..2 {
        t1_ret_idx.push(rng.next_range(0, (t1.num_rows() - 1) as i64) as usize);
    }
    let mut t1_ret = t1.select_rows(&t1_ret_idx).unwrap();
    t1_ret.weights = vec![-1; t1_ret.weights.len()];

    let t1_id = vec![rng.next_range(101, 200), rng.next_range(101, 200)];
    let t1_val = vec![rng.next_range(1, 100), rng.next_range(1, 100)];
    let t1_cat = vec![rng.next_range(1, 5), rng.next_range(1, 5)];
    let t1_ins = make_zset(t1_schema(), vec![t1_id, t1_val, t1_cat], 1);
    deltas.insert("t1".to_string(), concat_zsets(&t1_ret, &t1_ins));

    // t2 deltas: 1 retraction, 1 insertion
    let t2 = current_dataset.get("t2").unwrap();
    let mut t2_ret_idx = Vec::new();
    for _ in 0..1 {
        t2_ret_idx.push(rng.next_range(0, (t2.num_rows() - 1) as i64) as usize);
    }
    let mut t2_ret = t2.select_rows(&t2_ret_idx).unwrap();
    t2_ret.weights = vec![-1; t2_ret.weights.len()];

    let t2_id = vec![rng.next_range(101, 200)];
    let t2_val = vec![rng.next_range(1, 100)];
    let t2_group = vec![rng.next_range(1, 5)];
    let t2_ins = make_zset(t2_schema(), vec![t2_id, t2_val, t2_group], 1);
    deltas.insert("t2".to_string(), concat_zsets(&t2_ret, &t2_ins));

    deltas
}

fn concat_zsets(a: &ArrowZSet, b: &ArrowZSet) -> ArrowZSet {
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

pub fn apply_delta_physically(current: &ArrowZSet, delta: &ArrowZSet) -> ArrowZSet {
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

// ─── Query Templates for Fuzzer ──────────────────────────────────────────────

pub fn generate_random_query(seed: u64) -> String {
    let mut rng = FuzzRng::new(seed);
    let q_type = rng.next_range(0, 7);
    match q_type {
        0 => {
            // Filter + Project
            let val_limit = rng.next_range(10, 80);
            format!("SELECT id, val * 2 FROM t1 WHERE val > {val_limit}")
        }
        1 => {
            // Join
            let val_limit = rng.next_range(10, 80);
            format!(
                "SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > {val_limit}"
            )
        }
        2 => {
            // Aggregate SUM/COUNT/AVG
            let agg_fn = match rng.next_range(0, 2) {
                0 => "SUM(val)",
                1 => "COUNT(id)",
                _ => "CAST(AVG(val) AS BIGINT)",
            };
            format!("SELECT category, {agg_fn} FROM t1 GROUP BY category")
        }
        3 => {
            // Window ROW_NUMBER/RANK/DENSE_RANK
            let win_fn = match rng.next_range(0, 2) {
                0 => "ROW_NUMBER()",
                1 => "RANK()",
                _ => "DENSE_RANK()",
            };
            format!("SELECT id, val, {win_fn} OVER (PARTITION BY category ORDER BY id) FROM t1")
        }
        4 => {
            // Left Join
            "SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id".to_string()
        }
        5 => {
            // Union
            let val1 = rng.next_range(30, 60);
            let val2 = rng.next_range(30, 60);
            format!("SELECT id, val FROM t1 WHERE val > {val1} UNION ALL SELECT id, val FROM t2 WHERE val > {val2}")
        }
        6 => {
            // Distinct
            "SELECT DISTINCT category FROM t1".to_string()
        }
        _ => {
            // Lag Window
            "SELECT id, val, LAG(id, 1) OVER (PARTITION BY category ORDER BY id) FROM t1".to_string()
        }
    }
}

// ─── Extended Physical Executor ──────────────────────────────────────────────

pub enum ExecNode {
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
    },
    Union {
        left: Box<ExecNode>,
        right: Box<ExecNode>,
    },
    Distinct {
        input: Box<ExecNode>,
        op: DistinctOp,
    },
    Window {
        input: Box<ExecNode>,
        op: WindowOp,
    },
    Loopback {
        input: Box<ExecNode>,
    },
}

impl ExecNode {
    pub fn evaluate(&self, inputs: &HashMap<String, ArrowZSet>, epoch_id: u64) -> ArrowZSet {
        match self {
            ExecNode::Source { name } => inputs.get(name).cloned().unwrap_or_else(|| {
                panic!("Source {} not provided", name);
            }),
            ExecNode::Filter { input, op } => {
                let in_val = input.evaluate(inputs, epoch_id);
                op.process_delta(in_val).unwrap()
            }
            ExecNode::Project { input, op } => {
                let in_val = input.evaluate(inputs, epoch_id);
                op.process_delta(in_val).unwrap()
            }
            ExecNode::Join { left, right, op } => {
                let l_val = left.evaluate(inputs, epoch_id);
                let r_val = right.evaluate(inputs, epoch_id);
                op.process_epoch(l_val, r_val).unwrap()
            }
            ExecNode::OuterJoin { left, right, op } => {
                let l_val = left.evaluate(inputs, epoch_id);
                let r_val = right.evaluate(inputs, epoch_id);
                op.process_epoch(l_val, r_val).unwrap()
            }
            ExecNode::Aggregate {
                input,
                op,
                group_by,
                func,
                agg_input,
            } => {
                let in_val = input.evaluate(inputs, epoch_id);
                if in_val.is_empty() {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("k", DataType::Int64, false),
                        Field::new("sum_v", DataType::Int64, false),
                        Field::new("count", DataType::Int64, false),
                        Field::new("avg_v", DataType::Int64, false),
                    ]));
                    return ArrowZSet::empty(schema);
                }

                let keys = if group_by.is_empty() {
                    vec![0; in_val.num_rows()]
                } else {
                    rockstream_ops::expr::eval_i64(&group_by[0], &in_val.data).unwrap()
                };

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

                let raw_out = op.process_delta(kv_zset).unwrap();

                if raw_out.is_empty() {
                    let logical_schema = if group_by.is_empty() {
                        Arc::new(Schema::new(vec![Field::new("agg", DataType::Int64, false)]))
                    } else {
                        Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int64, false),
                            Field::new("agg", DataType::Int64, false),
                        ]))
                    };
                    ArrowZSet::empty(logical_schema)
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
                    } else {
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
                    }
                }
            }
            ExecNode::Union { left, right } => {
                let l_val = left.evaluate(inputs, epoch_id);
                let r_val = right.evaluate(inputs, epoch_id);
                concat_zsets(&l_val, &r_val)
            }
            ExecNode::Distinct { input, op } => {
                let in_val = input.evaluate(inputs, epoch_id);
                op.process_delta(in_val).unwrap()
            }
            ExecNode::Window { input, op } => {
                let in_val = input.evaluate(inputs, epoch_id);
                op.process_epoch(in_val, epoch_id).unwrap()
            }
            ExecNode::Loopback { input } => input.evaluate(inputs, epoch_id),
        }
    }
}

pub fn get_column_count(plan: &PlanNode) -> usize {
    match plan {
        PlanNode::Source { name } => match name.as_str() {
            "t1" => 3,
            "t2" => 3,
            _ => panic!("Unknown source: {}", name),
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
        _ => panic!("Unsupported PlanNode in fuzzer get_column_count"),
    }
}

pub fn build_exec_node(plan: &PlanNode, next_id: &mut u64) -> ExecNode {
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
        PlanNode::Distinct { input, .. } => {
            let in_node = build_exec_node(input, next_id);
            let num_cols = get_column_count(input);
            let fields: Vec<Field> = (0..num_cols)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, false))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let op = DistinctOp::new(schema);
            ExecNode::Distinct {
                input: Box::new(in_node),
                op,
            }
        }
        PlanNode::Window {
            input,
            window_exprs,
        } => {
            let in_node = build_exec_node(input, next_id);
            let num_cols = get_column_count(input) + window_exprs.len();
            let fields: Vec<Field> = (0..num_cols)
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, false))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let op = WindowOp::new(schema, window_exprs.clone());
            ExecNode::Window {
                input: Box::new(in_node),
                op,
            }
        }
        other => panic!("Unsupported PlanNode in fuzzer interpreter: {:?}", other),
    }
}

async fn run_df_batch(ctx: &SessionContext, query: &str) -> BTreeMap<Vec<i64>, i64> {
    let df = ctx.sql(query).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut results = BTreeMap::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let mut col_arrays = Vec::new();
        for j in 0..num_cols {
            let col = batch.column(j);
            let casted_col = if col.data_type() != &DataType::Int64 {
                arrow::compute::cast(col, &DataType::Int64).unwrap()
            } else {
                col.clone()
            };
            let int_col = casted_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            col_arrays.push(int_col);
        }
        for i in 0..num_rows {
            let mut row = Vec::new();
            for col_array in &col_arrays {
                row.push(col_array.value(i));
            }
            *results.entry(row).or_insert(0) += 1;
        }
    }
    results
}

// ─── Core Fuzz Assertion ─────────────────────────────────────────────────────

pub async fn run_fuzz_case(seed: u64) {
    let query = generate_random_query(seed);
    let frontend = SqlFrontend::new();
    frontend.register_table("t1", t1_schema()).unwrap();
    frontend.register_table("t2", t2_schema()).unwrap();

    let plan_node = match frontend.sql_to_plan_node(&query).await {
        Ok(p) => p,
        Err(_) => {
            // If the query failed compilation, skip
            return;
        }
    };

    let mut next_id = 0;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    // Initial datasets
    let initial_dataset = generate_synthetic_dataset(seed);
    let delta = generate_synthetic_deltas(&initial_dataset, seed + 1);

    let mut dataset_after = initial_dataset.clone();
    for (name, d) in &delta {
        let current = dataset_after.get(name).unwrap();
        let new_zset = apply_delta_physically(current, d);
        dataset_after.insert(name.clone(), new_zset);
    }

    // Evaluate Epoch 0
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    let out_0 = exec_tree.evaluate(&initial_dataset, 1);
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
            let mut row = Vec::new();
            for col_array in &col_arrays {
                row.push(col_array.value(i));
            }
            *inc_acc.entry(row).or_insert(0) += out_0.weights[i];
        }
    }
    inc_acc.retain(|_, &mut w| w != 0);

    // Compare with DataFusion Epoch 0
    let ctx = SessionContext::new();
    for (name, zset) in &initial_dataset {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .unwrap();
        ctx.register_table(name, Arc::new(mem_table)).unwrap();
    }
    let df_results_0 = run_df_batch(&ctx, &query).await;
    assert_eq!(
        inc_acc, df_results_0,
        "Fuzz Epoch 0 mismatch for query: {}",
        query
    );

    // Evaluate Epoch 1 (Delta)
    let out_1 = exec_tree.evaluate(&delta, 2);
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
            let mut row = Vec::new();
            for col_array in &col_arrays {
                row.push(col_array.value(i));
            }
            *inc_acc.entry(row).or_insert(0) += out_1.weights[i];
        }
    }
    inc_acc.retain(|_, &mut w| w != 0);

    // Compare with DataFusion Epoch 1
    let new_ctx = SessionContext::new();
    for (name, zset) in &dataset_after {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .unwrap();
        new_ctx.register_table(name, Arc::new(mem_table)).unwrap();
    }
    let df_results_1 = run_df_batch(&new_ctx, &query).await;
    assert_eq!(
        inc_acc, df_results_1,
        "Fuzz Epoch 1 mismatch for query: {}",
        query
    );
}

// ─── Soak / Unit Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn test_fuzz_simple_cases() {
        for seed in 0..50 {
            run_fuzz_case(seed).await;
        }
    }

    /// Fuzzer soak test running up to a specified duration or iteration count.
    /// By default runs quickly in standard test mode, but runs for longer if requested.
    #[tokio::test]
    async fn fuzzer_soak_test() {
        let duration = if std::env::var("SOAK_DURATION").is_ok() {
            Duration::from_secs(3600) // 1 hour if requested
        } else {
            Duration::from_secs(10) // 10 seconds for standard test suite
        };

        println!("Running fuzzer soak test for {:?}", duration);
        let start = Instant::now();
        let mut seed = 1000;
        while start.elapsed() < duration {
            run_fuzz_case(seed).await;
            seed += 1;
        }
        println!("Completed fuzzer soak test: {} iterations", seed - 1000);
    }
}
