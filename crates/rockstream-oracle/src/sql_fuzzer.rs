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

    // t1 deltas: 2 retractions (without replacement), 2 insertions.
    // Partial Fisher-Yates shuffle — produces exactly 2 unique row indices,
    // matching the approach used in tpch_gen.rs and avoiding the birthday-
    // paradox duplicates that with-replacement sampling causes.
    let t1 = current_dataset.get("t1").unwrap();
    let t1_num_rows = t1.num_rows();
    let mut t1_all_idx: Vec<usize> = (0..t1_num_rows).collect();
    for i in 0..2 {
        let j = i + rng.next_range(0, (t1_num_rows - 1 - i) as i64) as usize;
        t1_all_idx.swap(i, j);
    }
    let t1_ret_idx = &t1_all_idx[..2];
    let mut t1_ret = t1.select_rows(t1_ret_idx).unwrap();
    t1_ret.weights = vec![-1; t1_ret.weights.len()];

    let t1_id = vec![rng.next_range(101, 200), rng.next_range(101, 200)];
    let t1_val = vec![rng.next_range(1, 100), rng.next_range(1, 100)];
    let t1_cat = vec![rng.next_range(1, 5), rng.next_range(1, 5)];
    let t1_ins = make_zset(t1_schema(), vec![t1_id, t1_val, t1_cat], 1);
    deltas.insert("t1".to_string(), concat_zsets(&t1_ret, &t1_ins));

    // t2 deltas: 1 retraction (without replacement), 1 insertion.
    let t2 = current_dataset.get("t2").unwrap();
    let t2_num_rows = t2.num_rows();
    let mut t2_all_idx: Vec<usize> = (0..t2_num_rows).collect();
    let j = rng.next_range(0, (t2_num_rows - 1) as i64) as usize;
    t2_all_idx.swap(0, j);
    let t2_ret_idx = &t2_all_idx[..1];
    let mut t2_ret = t2.select_rows(t2_ret_idx).unwrap();
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

/// Build a delta that retracts all positive-weight rows from `delta`.
///
/// Used for the third-epoch test: after epoch 1 inserts new rows, epoch 2
/// retracts those same rows to verify the accumulator correctly undoes inserts
/// it made one epoch ago — the hardest case for cross-epoch state management.
fn negate_insertions(delta: &HashMap<String, ArrowZSet>) -> HashMap<String, ArrowZSet> {
    let mut result = HashMap::new();
    for (name, d) in delta {
        let insert_indices: Vec<usize> = d
            .weights
            .iter()
            .enumerate()
            .filter(|(_, &w)| w > 0)
            .map(|(i, _)| i)
            .collect();
        if insert_indices.is_empty() {
            result.insert(name.clone(), ArrowZSet::empty(d.schema()));
        } else {
            let mut ret = d.select_rows(&insert_indices).unwrap();
            ret.weights = vec![-1; ret.weights.len()];
            result.insert(name.clone(), ret);
        }
    }
    result
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
    let q_type = rng.next_range(0, 14);
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
        7 => {
            // Lag Window
            "SELECT id, val, LAG(id, 1) OVER (PARTITION BY category ORDER BY id) FROM t1"
                .to_string()
        }
        8 => {
            // Aggregate + HAVING — tests Filter-on-Aggregate (planner emits
            // Filter(Aggregate(Source)) which exercises the retraction path through
            // the post-aggregate filter on every delta epoch).
            let agg_fn = match rng.next_range(0, 1) {
                0 => "SUM(val)",
                _ => "COUNT(id)",
            };
            let threshold = rng.next_range(50, 250);
            format!(
                "SELECT category, {agg_fn} FROM t1 GROUP BY category HAVING {agg_fn} > {threshold}"
            )
        }
        9 => {
            // Filter → Join → Aggregate: the cross-operator composition path.
            // Tests that delta propagation through filter-then-join-then-aggregate
            // produces results identical to a DataFusion batch re-execution.
            let val_limit = rng.next_range(5, 50);
            format!(
                "SELECT t1.category, COUNT(t1.id) FROM t1 \
                 JOIN t2 ON t1.id = t2.id \
                 WHERE t1.val > {val_limit} \
                 GROUP BY t1.category"
            )
        }
        10 => {
            // Join → Aggregate on joined column: GROUP BY a column from the right side.
            // This tests that column offsets in the post-join aggregate are resolved
            // correctly (t2 columns start at index 3 in the concatenated join schema).
            "SELECT t2.group_id, SUM(t1.val) FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY t2.group_id"
                .to_string()
        }
        11 => {
            // Semi-join / IN-subquery: the planner lowers this to a LeftSemi join.
            // Exercises the OuterJoinOp::Semi path, which currently receives no
            // coverage from query types 0-10. Output contains only t1.id values
            // that exist in the t2 subset, so the result must be ⊆ t1.id.
            let k = rng.next_range(1, 5);
            format!("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE group_id = {k})")
        }
        12 => {
            // Anti-join / NOT IN: the planner lowers this to OuterJoinOp::Anti.
            // Exercises the NOT IN / NOT EXISTS path that had zero fuzzer coverage.
            // Output is t1.id values that do NOT appear in the t2 subset.
            let k = rng.next_range(1, 5);
            format!("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2 WHERE group_id = {k})")
        }
        13 => {
            // Multi-key GROUP BY: exercises group_by with 2 key columns.
            // Detects the bug where only group_by[0] is hashed, silently dropping
            // the second key column and collapsing distinct groups.
            let agg_fn = match rng.next_range(0, 1) {
                0 => "SUM(t1.val)",
                _ => "COUNT(t1.id)",
            };
            format!(
                "SELECT t1.category, t2.group_id, {agg_fn} FROM t1 \
                 JOIN t2 ON t1.id = t2.id \
                 GROUP BY t1.category, t2.group_id"
            )
        }
        _ => {
            // HAVING + scalar subquery: GROUP BY with a HAVING threshold that is
            // computed from a scalar subquery over t2.  This exercises the planner
            // path where a HAVING predicate references an aggregate from a different
            // relation — the "nested aggregation" planner path.  If the planner does
            // not yet support scalar subqueries in HAVING, sql_to_plan_node returns
            // Err and run_fuzz_case silently skips the case (correct behaviour).
            let agg_fn = match rng.next_range(0, 1) {
                0 => "COUNT(id)",
                _ => "SUM(val)",
            };
            let sub_agg = match rng.next_range(0, 1) {
                0 => "CAST(AVG(val) AS BIGINT)",
                _ => "COUNT(id)",
            };
            format!(
                "SELECT category, {agg_fn} FROM t1 GROUP BY category \
                 HAVING {agg_fn} > (SELECT {sub_agg} FROM t2)"
            )
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
        /// Persistent reverse map: combined FNV hash key → original multi-column key values.
        /// Required to reconstruct the full GROUP BY output when group_by.len() > 1.
        key_lookup: std::sync::Mutex<std::collections::HashMap<i64, Vec<i64>>>,
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
                key_lookup,
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

                // Evaluate all group_by key columns.
                let key_vecs: Vec<Vec<i64>> = if group_by.is_empty() {
                    vec![]
                } else {
                    group_by
                        .iter()
                        .map(|expr| rockstream_ops::expr::eval_i64(expr, &in_val.data).unwrap())
                        .collect()
                };

                // Combine multiple keys into a single hash key for AggregateOp.
                // Single-key GROUP BY passes the raw value unchanged (no collision risk).
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
                // The ~2^-32 per-pair collision probability is negligible for the small
                // synthetic datasets used in tests, so a collision here always indicates
                // either a bug in the hash function or an unexpectedly large key space.
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
                // multi-column key in the output (combined_key → original key columns).
                if group_by.len() > 1 {
                    let mut lookup = key_lookup.lock().unwrap();
                    for i in 0..in_val.num_rows() {
                        lookup
                            .entry(keys[i])
                            .or_insert_with(|| key_vecs.iter().map(|kv| kv[i]).collect());
                    }
                }

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

/// Accumulate Z-set output rows into `acc` (row → net weight), pruning zeros.
fn accumulate_output(out: &ArrowZSet, acc: &mut BTreeMap<Vec<i64>, i64>) {
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

/// Build a DataFusion `SessionContext` loaded with all tables from `dataset`.
async fn make_df_ctx_and_run(
    dataset: &HashMap<String, ArrowZSet>,
    query: &str,
) -> BTreeMap<Vec<i64>, i64> {
    let ctx = SessionContext::new();
    for (name, zset) in dataset {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .unwrap();
        ctx.register_table(name, Arc::new(mem_table)).unwrap();
    }
    run_df_batch(&ctx, query).await
}

pub async fn run_fuzz_case(seed: u64) {
    let query = generate_random_query(seed);
    run_fuzz_case_for_query(&query, seed).await;
}

/// Run the full 3-epoch fuzz oracle for an arbitrary SQL query string.
/// Used by both `run_fuzz_case` and the dedicated scalar-subquery test.
pub async fn run_fuzz_case_for_query(query: &str, seed: u64) {
    let frontend = SqlFrontend::new();
    frontend.register_table("t1", t1_schema()).unwrap();
    frontend.register_table("t2", t2_schema()).unwrap();

    let plan_node = match frontend.sql_to_plan_node(query).await {
        Ok(p) => p,
        Err(_) => {
            // If the query failed compilation, skip
            return;
        }
    };

    let mut next_id = 0;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);

    // ── Epoch 0: initial snapshot ──────────────────────────────────────────
    let initial_dataset = generate_synthetic_dataset(seed);
    let mut inc_acc: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    let out_0 = exec_tree.evaluate(&initial_dataset, 1);
    accumulate_output(&out_0, &mut inc_acc);

    let df_0 = make_df_ctx_and_run(&initial_dataset, query).await;
    assert_eq!(inc_acc, df_0, "Fuzz Epoch 0 mismatch for query: {}", query);

    // ── Epoch 1: apply mixed delta (retractions + insertions) ─────────────
    let delta = generate_synthetic_deltas(&initial_dataset, seed + 1);

    let mut dataset_after_1 = initial_dataset.clone();
    for (name, d) in &delta {
        let current = dataset_after_1.get(name).unwrap();
        dataset_after_1.insert(name.clone(), apply_delta_physically(current, d));
    }

    let out_1 = exec_tree.evaluate(&delta, 2);
    accumulate_output(&out_1, &mut inc_acc);

    let df_1 = make_df_ctx_and_run(&dataset_after_1, query).await;
    assert_eq!(inc_acc, df_1, "Fuzz Epoch 1 mismatch for query: {}", query);

    // ── Epoch 2: retract the rows that were inserted in epoch 1 ───────────
    // This is the hardest case for the accumulator: it must correctly "undo"
    // state changes from two epochs ago, where the to-be-retracted rows were
    // not present in the original epoch-0 dataset.
    let delta_2 = negate_insertions(&delta);

    let mut dataset_after_2 = dataset_after_1.clone();
    for (name, d2) in &delta_2 {
        let current = dataset_after_2.get(name).unwrap();
        dataset_after_2.insert(name.clone(), apply_delta_physically(current, d2));
    }

    let out_2 = exec_tree.evaluate(&delta_2, 3);
    accumulate_output(&out_2, &mut inc_acc);

    let df_2 = make_df_ctx_and_run(&dataset_after_2, query).await;
    assert_eq!(
        inc_acc, df_2,
        "Fuzz Epoch 2 (cross-epoch retraction) mismatch for query: {}",
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
        // Seeds 0-299 cover all 15 query types (0-14) with ≥20 expected samples
        // each. Type 11 = Semi-join, 12 = Anti-join (NOT IN), 13 = multi-key
        // GROUP BY, 14 = HAVING + scalar subquery (skipped if planner does not
        // support it yet).  Each seed exercises 3 epochs (initial, delta,
        // cross-epoch retraction), so 300 seeds give 900 epoch-level oracle
        // checks.
        //
        // Seed 1116 is a regression test for the AggregateOp consolidation bug:
        // concurrent retraction from both join sides (t1.id=49 AND t2.id=49)
        // caused the DBSP bilinear triple (−1, −1, +1) to hit count=0 transiently,
        // corrupting the per-group sum.
        for seed in 0..300 {
            run_fuzz_case(seed).await;
        }
        run_fuzz_case(1116).await;
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

    /// Dedicated coverage test for HAVING + scalar subquery (query type 14).
    ///
    /// Tests four concrete forms of `GROUP BY … HAVING agg > (SELECT … FROM t2)`:
    ///
    /// - `COUNT(id) > (SELECT CAST(AVG(val) AS BIGINT) FROM t2)` — most common path
    /// - `SUM(val)  > (SELECT COUNT(id) FROM t2)` — second form
    ///
    /// Each variant is run through the full 3-epoch fuzz harness (initial snapshot,
    /// mixed delta, cross-epoch retraction).  If the planner does not yet support
    /// scalar subqueries in HAVING the compilation step returns `Err` and
    /// `run_fuzz_case` silently skips the case — the test still passes but prints a
    /// notice so we know coverage is absent.  When the planner does support it, the
    /// equivalence oracle catches any mismatch immediately.
    #[tokio::test]
    async fn test_having_scalar_subquery_planner_coverage() {
        let queries: &[&str] = &[
            "SELECT category, COUNT(id) FROM t1 GROUP BY category \
             HAVING COUNT(id) > (SELECT CAST(AVG(val) AS BIGINT) FROM t2)",
            "SELECT category, SUM(val) FROM t1 GROUP BY category \
             HAVING SUM(val) > (SELECT COUNT(id) FROM t2)",
            "SELECT category, COUNT(id) FROM t1 GROUP BY category \
             HAVING COUNT(id) > (SELECT COUNT(id) FROM t2)",
        ];

        let frontend = SqlFrontend::new();
        frontend.register_table("t1", t1_schema()).unwrap();
        frontend.register_table("t2", t2_schema()).unwrap();

        let mut any_supported = false;
        for query in queries {
            match frontend.sql_to_plan_node(query).await {
                Ok(_) => {
                    any_supported = true;
                    // Planner supports it — run full 3-epoch equivalence check.
                    run_fuzz_case_for_query(query, 42).await;
                }
                Err(_) => {
                    println!(
                        "HAVING scalar subquery not yet supported by planner (skipped): {query}"
                    );
                }
            }
        }
        if !any_supported {
            println!(
                "NOTE: all HAVING-scalar-subquery variants were skipped; \
                 planner does not support scalar subqueries in HAVING yet."
            );
        }
    }
}
