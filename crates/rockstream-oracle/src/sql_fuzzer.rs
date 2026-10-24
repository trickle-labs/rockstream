//! Randomized SQL query fuzzer and incremental engine validator (v0.14).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use rockstream_test_support::typed_output::{
    add_batch_rows, compare_results, empty_result, OracleMismatch, TypedResult,
};
#[cfg(test)]
use rockstream_test_support::typed_output::{OracleField, RowWeightMismatch};

use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::distinct::DistinctOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::outer_join::OuterJoinOp;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{AggregateFunc, Expr, PlanNode, WindowFunc};
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
    let mut active_rows: Vec<Vec<ScalarValue>> = (0..current.num_rows())
        .map(|i| get_row_as_scalars(&current.data, i))
        .collect();
    for i in 0..delta.num_rows() {
        let row_val = get_row_as_scalars(&delta.data, i);
        let w = delta.weights[i];
        if w > 0 {
            for _ in 0..w {
                active_rows.push(row_val.clone());
            }
        } else if w < 0 {
            for _ in 0..(-w) {
                let pos = active_rows
                    .iter()
                    .position(|r| r == &row_val)
                    .unwrap_or_else(|| panic!("invalid retraction of absent row {row_val:?}"));
                active_rows.remove(pos);
            }
        }
    }
    rebuild_zset(current.schema(), &active_rows)
}

fn get_row_as_scalars(batch: &RecordBatch, row_idx: usize) -> Vec<ScalarValue> {
    batch
        .columns()
        .iter()
        .map(|column| ScalarValue::try_from_array(column, row_idx).unwrap())
        .collect()
}

fn rebuild_zset(schema: SchemaRef, active_rows: &[Vec<ScalarValue>]) -> ArrowZSet {
    let num_cols = schema.fields().len();
    let arrow_cols: Vec<ArrayRef> = (0..num_cols)
        .map(|col_idx| {
            if active_rows.is_empty() {
                new_empty_array(schema.field(col_idx).data_type())
            } else {
                ScalarValue::iter_to_array(active_rows.iter().map(|row| row[col_idx].clone()))
                    .unwrap()
            }
        })
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
            // relation — the "nested aggregation" planner path. These exact
            // forms are allowlisted only with their contractual RS-1013 error.
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

#[allow(clippy::large_enum_variant)]
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
                let aggregate_nullable = matches!(
                    func,
                    AggregateFunc::Sum
                        | AggregateFunc::Avg
                        | AggregateFunc::Min
                        | AggregateFunc::Max
                );
                if in_val.is_empty() {
                    let fields: Vec<Field> = (0..group_by.len())
                        .map(|i| Field::new(format!("k{i}"), DataType::Int64, false))
                        .chain(std::iter::once(Field::new(
                            "agg",
                            DataType::Int64,
                            aggregate_nullable,
                        )))
                        .collect();
                    let schema = Arc::new(Schema::new(fields));
                    if epoch_id == 1 && group_by.is_empty() && matches!(func, AggregateFunc::Count)
                    {
                        return make_zset(schema, vec![vec![0]], 1);
                    }
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
                        let col =
                            rockstream_ops::expr::eval_to_array(agg_input, &in_val.data).unwrap();
                        (0..in_val.num_rows())
                            .map(|row| i64::from(!col.is_null(row)))
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

                let global_count = group_by.is_empty() && matches!(func, AggregateFunc::Count);
                let was_empty = global_count && op.live_groups() == 0;
                let raw_out = op.process_delta(kv_zset).unwrap();

                let output = if raw_out.is_empty() {
                    let logical_schema = if group_by.is_empty() {
                        Arc::new(Schema::new(vec![Field::new(
                            "agg",
                            DataType::Int64,
                            aggregate_nullable,
                        )]))
                    } else if group_by.len() == 1 {
                        Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int64, false),
                            Field::new("agg", DataType::Int64, aggregate_nullable),
                        ]))
                    } else {
                        let fields: Vec<Field> = (0..group_by.len())
                            .map(|j| Field::new(format!("k{j}"), DataType::Int64, false))
                            .chain(std::iter::once(Field::new(
                                "agg",
                                DataType::Int64,
                                aggregate_nullable,
                            )))
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
                        .downcast_ref::<Float64Array>()
                        .unwrap();

                    let agg_vals: Vec<i64> = match func {
                        AggregateFunc::Sum => {
                            (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect()
                        }
                        AggregateFunc::Count => {
                            (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect()
                        }
                        AggregateFunc::Avg => {
                            // This corpus uses `CAST(AVG(...) AS BIGINT)`, so
                            // match that SQL result type at the incremental boundary.
                            (0..raw_out.num_rows())
                                .map(|i| avg_arr.value(i) as i64)
                                .collect()
                        }
                        _ => (0..raw_out.num_rows()).map(|i| sum_arr.value(i)).collect(),
                    };

                    if group_by.is_empty() {
                        let logical_schema = Arc::new(Schema::new(vec![Field::new(
                            "agg",
                            DataType::Int64,
                            aggregate_nullable,
                        )]));
                        let data = RecordBatch::try_new(
                            logical_schema,
                            vec![Arc::new(Int64Array::from(agg_vals)) as ArrayRef],
                        )
                        .unwrap();
                        ArrowZSet::new(data, raw_out.weights)
                    } else if group_by.len() == 1 {
                        let logical_schema = Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int64, false),
                            Field::new("agg", DataType::Int64, aggregate_nullable),
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
                            .chain(std::iter::once(Field::new(
                                "agg",
                                DataType::Int64,
                                aggregate_nullable,
                            )))
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
                if global_count {
                    let schema = output.schema();
                    let zero = |weight| make_zset(schema.clone(), vec![vec![0]], weight);
                    let output = if was_empty && epoch_id > 1 {
                        concat_zsets(&output, &zero(-1))
                    } else {
                        output
                    };
                    if op.live_groups() == 0 {
                        concat_zsets(&output, &zero(1))
                    } else {
                        output
                    }
                } else {
                    output
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
        PlanNode::TumbleWindow { input, .. }
        | PlanNode::HopWindow { input, .. }
        | PlanNode::SessionWindow { input, .. } => get_column_count(input),
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
            let fields: Vec<Field> = (0..get_column_count(input))
                .map(|i| Field::new(format!("col_{i}"), DataType::Int64, false))
                .chain(window_exprs.iter().enumerate().map(|(i, expr)| {
                    let (data_type, nullable) = match &expr.func {
                        WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
                            (DataType::UInt64, false)
                        }
                        WindowFunc::Lag { .. } | WindowFunc::Lead { .. } => (DataType::Int64, true),
                        _ => (DataType::Int64, false),
                    };
                    Field::new(format!("window_{i}"), data_type, nullable)
                }))
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

fn accumulate_output(out: &ArrowZSet, acc: &mut TypedResult) {
    add_batch_rows(acc, &out.data, &out.weights);
}

async fn run_df_batch(ctx: &SessionContext, query: &str) -> Result<TypedResult, String> {
    let df = ctx
        .sql(query)
        .await
        .map_err(|error| format!("{error}; full_error={error:?}"))?;
    let mut result = empty_result(df.schema().as_arrow());
    let batches = df
        .collect()
        .await
        .map_err(|error| format!("{error}; full_error={error:?}"))?;
    for batch in batches {
        let batch_result = empty_result(batch.schema().as_ref());
        if batch_result.fields != result.fields {
            return Err(format!(
                "[RS-0001] DataFusion batch schema changed: planned={:?} batch={:?}",
                result.fields, batch_result.fields
            ));
        }
        add_batch_rows(&mut result, &batch, &vec![1; batch.num_rows()]);
    }
    Ok(result)
}

/// Build a DataFusion `SessionContext` loaded with all tables from `dataset`.
async fn make_df_ctx_and_run(
    dataset: &HashMap<String, ArrowZSet>,
    query: &str,
) -> Result<TypedResult, String> {
    let ctx = SessionContext::new();
    for (name, zset) in dataset {
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            zset.schema(),
            vec![vec![zset.data.clone()]],
        )
        .map_err(|error| format!("register table {name}: {error}; full_error={error:?}"))?;
        ctx.register_table(name, Arc::new(mem_table))
            .map_err(|error| format!("register table {name}: {error}; full_error={error:?}"))?;
    }
    run_df_batch(&ctx, query).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleCaseOutcome {
    Compared { epochs: Vec<&'static str> },
    ExpectedUnsupported { error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleCaseReport {
    pub seed: u64,
    pub sql: String,
    pub outcome: OracleCaseOutcome,
    pub operation_history: Vec<String>,
}

const EXPECTED_UNSUPPORTED_SCALAR_HAVING_ERROR: &str =
    "[RS-1013] Unsupported plan node in incremental lowering: join_filter_non_eq";
const SCALAR_HAVING_QUERIES: [&str; 4] = [
    "SELECT category, COUNT(id) FROM t1 GROUP BY category HAVING COUNT(id) > (SELECT CAST(AVG(val) AS BIGINT) FROM t2)",
    "SELECT category, COUNT(id) FROM t1 GROUP BY category HAVING COUNT(id) > (SELECT COUNT(id) FROM t2)",
    "SELECT category, SUM(val) FROM t1 GROUP BY category HAVING SUM(val) > (SELECT CAST(AVG(val) AS BIGINT) FROM t2)",
    "SELECT category, SUM(val) FROM t1 GROUP BY category HAVING SUM(val) > (SELECT COUNT(id) FROM t2)",
];

fn expected_unsupported_error(query: &str) -> Option<&'static str> {
    SCALAR_HAVING_QUERIES
        .contains(&query)
        .then_some(EXPECTED_UNSUPPORTED_SCALAR_HAVING_ERROR)
}

fn compile_failure_message(query: &str, seed: u64, error: &rockstream_sql::SqlError) -> String {
    format!(
        "SQL oracle failure: sql={query:?} seed={seed} phase=compile contractual_error={error} full_error={error:?}"
    )
}

pub async fn run_fuzz_case(seed: u64) -> OracleCaseReport {
    let query = generate_random_query(seed);
    run_fuzz_case_for_query(&query, seed).await
}

/// Run the full 3-epoch fuzz oracle for an arbitrary SQL query string.
/// Used by both `run_fuzz_case` and the dedicated scalar-subquery test.
pub async fn run_fuzz_case_for_query(query: &str, seed: u64) -> OracleCaseReport {
    let initial_dataset = generate_synthetic_dataset(seed);
    let delta = generate_synthetic_deltas(&initial_dataset, seed + 1);
    let delta_2 = negate_insertions(&delta);
    run_case_with_deltas(
        query,
        seed,
        initial_dataset,
        vec![("mixed_delta", delta), ("cross_epoch_retraction", delta_2)],
        vec![
            format!("snapshot(seed={seed})"),
            format!("mixed_delta(seed={})", seed + 1),
            format!("retract_insertions(seed={})", seed + 1),
        ],
    )
    .await
}

async fn run_case_with_deltas(
    query: &str,
    seed: u64,
    initial_dataset: HashMap<String, ArrowZSet>,
    deltas: Vec<(&'static str, HashMap<String, ArrowZSet>)>,
    history: Vec<String>,
) -> OracleCaseReport {
    let frontend = SqlFrontend::new();
    for (name, zset) in &initial_dataset {
        frontend
            .register_table(name, zset.schema())
            .unwrap_or_else(|error| {
                panic!(
                    "SQL oracle failure: sql={query:?} seed={seed} phase=register_table({name}) contractual_error={error} full_error={error:?}"
                )
            });
    }

    let plan_node = match frontend.sql_to_plan_node(query).await {
        Ok(p) => p,
        Err(error) => {
            if let Some(expected) = expected_unsupported_error(query) {
                if error.to_string() == expected {
                    return OracleCaseReport {
                        seed,
                        sql: query.to_string(),
                        outcome: OracleCaseOutcome::ExpectedUnsupported {
                            error: expected.to_string(),
                        },
                        operation_history: Vec::new(),
                    };
                }
            }
            panic!("{}", compile_failure_message(query, seed, &error));
        }
    };

    let mut next_id = 0;
    let exec_tree = build_exec_node(&plan_node, &mut next_id);
    let mut accumulated: Option<TypedResult> = None;
    let mut current_dataset = initial_dataset.clone();
    let mut compared_epochs = Vec::new();
    let epochs = std::iter::once(("snapshot", initial_dataset)).chain(deltas.into_iter());

    for (epoch_id, (phase, inputs)) in epochs.enumerate() {
        if epoch_id > 0 {
            for (name, delta) in &inputs {
                if let Some(current) = current_dataset.get(name) {
                    current_dataset.insert(name.clone(), apply_delta_physically(current, delta));
                }
            }
        }

        let out = exec_tree.evaluate(&inputs, epoch_id as u64 + 1);
        let phase_result = empty_result(out.data.schema().as_ref());
        let acc = accumulated.get_or_insert(phase_result.clone());
        if acc.fields != phase_result.fields {
            panic!(
                "SQL oracle mismatch: sql={query:?} seed={seed} phase={phase} mismatch={:?}; history={history:?}",
                OracleMismatch::Schema {
                    expected: acc.fields.clone(),
                    actual: phase_result.fields,
                }
            );
        }
        accumulate_output(&out, acc);

        let batch = make_df_ctx_and_run(&current_dataset, query)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "SQL oracle failure: sql={query:?} seed={seed} phase={phase} reference_error={error}; history={history:?}"
                )
            });
        if let Err(mismatch) = compare_results(&batch, acc) {
            panic!(
                "SQL oracle mismatch: sql={query:?} seed={seed} phase={phase} mismatch={mismatch:?}; history={history:?}"
            );
        }
        compared_epochs.push(phase);
    }

    OracleCaseReport {
        seed,
        sql: query.to_string(),
        outcome: OracleCaseOutcome::Compared {
            epochs: compared_epochs,
        },
        operation_history: history,
    }
}

// ─── Soak / Unit Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn test_fuzz_simple_cases() {
        // Seeds 0-299 cover the fixed query corpus; the report records every
        // supported SQL/epoch comparison and each exact allowlisted rejection.
        //
        // Seed 1116 is a regression test for the AggregateOp consolidation bug:
        // concurrent retraction from both join sides (t1.id=49 AND t2.id=49)
        // caused the DBSP bilinear triple (−1, −1, +1) to hit count=0 transiently,
        // corrupting the per-group sum.
        let mut reports = Vec::new();
        for seed in 0..300 {
            reports.push(run_fuzz_case(seed).await);
        }
        reports.push(run_fuzz_case(1116).await);
        assert_eq!(
            reports.iter().map(|report| report.seed).collect::<Vec<_>>(),
            (0..300).chain([1116]).collect::<Vec<_>>()
        );
        for report in &reports {
            assert_eq!(report.sql, generate_random_query(report.seed));
            match &report.outcome {
                OracleCaseOutcome::Compared { epochs } => assert_eq!(
                    epochs,
                    &["snapshot", "mixed_delta", "cross_epoch_retraction"]
                ),
                OracleCaseOutcome::ExpectedUnsupported { error } => assert_eq!(
                    Some(error.as_str()),
                    expected_unsupported_error(&report.sql)
                ),
            }
            if matches!(&report.outcome, OracleCaseOutcome::Compared { .. }) {
                assert_eq!(
                    report.operation_history,
                    [
                        format!("snapshot(seed={})", report.seed),
                        format!("mixed_delta(seed={})", report.seed + 1),
                        format!("retract_insertions(seed={})", report.seed + 1),
                    ]
                );
            } else {
                assert!(report.operation_history.is_empty());
            }
            println!("SQL_ORACLE_CASE {report:?}");
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

    /// Dedicated coverage test for HAVING + scalar subquery (query type 14).
    ///
    /// Tests four concrete forms of `GROUP BY … HAVING agg > (SELECT … FROM t2)`:
    ///
    /// - `COUNT(id) > (SELECT CAST(AVG(val) AS BIGINT) FROM t2)` — most common path
    /// - `SUM(val)  > (SELECT COUNT(id) FROM t2)` — second form
    ///
    /// Each variant is either compared at all three epochs or must match the
    /// bounded allowlist's exact contractual compiler error.
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

        for query in queries {
            let report = run_fuzz_case_for_query(query, 42).await;
            match report.outcome {
                OracleCaseOutcome::Compared { epochs } => assert_eq!(
                    epochs,
                    ["snapshot", "mixed_delta", "cross_epoch_retraction"]
                ),
                OracleCaseOutcome::ExpectedUnsupported { error } => {
                    assert_eq!(Some(error.as_str()), expected_unsupported_error(query));
                }
            }
        }
    }

    fn typed_result(fields: Vec<OracleField>, rows: Vec<(Vec<ScalarValue>, i64)>) -> TypedResult {
        TypedResult {
            fields,
            rows: rows.into_iter().collect(),
        }
    }

    #[test]
    fn typed_oracle_mutations_report_exact_mismatches() {
        let int = OracleField {
            data_type: DataType::Int64,
            nullable: false,
        };
        let nullable_int = OracleField {
            data_type: DataType::Int64,
            nullable: true,
        };
        let decimal = OracleField {
            data_type: DataType::Decimal128(12, 2),
            nullable: false,
        };

        let null_expected = typed_result(
            vec![int.clone(), nullable_int.clone()],
            vec![(
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
                1,
            )],
        );
        let null_as_zero = typed_result(
            vec![int.clone(), nullable_int],
            vec![(
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(0))],
                1,
            )],
        );
        assert_eq!(
            compare_results(&null_expected, &null_as_zero),
            Err(OracleMismatch::Rows(vec![
                RowWeightMismatch {
                    row: vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(0))],
                    expected_weight: 0,
                    actual_weight: 1,
                },
                RowWeightMismatch {
                    row: vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
                    expected_weight: 1,
                    actual_weight: 0,
                },
            ]))
        );

        let fraction_expected = typed_result(
            vec![decimal.clone()],
            vec![(vec![ScalarValue::Decimal128(Some(125), 12, 2)], 1)],
        );
        let fraction_changed = typed_result(
            vec![decimal.clone()],
            vec![(vec![ScalarValue::Decimal128(Some(126), 12, 2)], 1)],
        );
        assert_eq!(
            compare_results(&fraction_expected, &fraction_changed),
            Err(OracleMismatch::Rows(vec![
                RowWeightMismatch {
                    row: vec![ScalarValue::Decimal128(Some(125), 12, 2)],
                    expected_weight: 1,
                    actual_weight: 0,
                },
                RowWeightMismatch {
                    row: vec![ScalarValue::Decimal128(Some(126), 12, 2)],
                    expected_weight: 0,
                    actual_weight: 1,
                },
            ]))
        );

        let wrong_type = typed_result(
            vec![OracleField {
                data_type: DataType::Float64,
                nullable: false,
            }],
            vec![(vec![ScalarValue::Float64(Some(1.25))], 1)],
        );
        assert_eq!(
            compare_results(&fraction_expected, &wrong_type),
            Err(OracleMismatch::Schema {
                expected: vec![decimal],
                actual: vec![OracleField {
                    data_type: DataType::Float64,
                    nullable: false,
                }],
            })
        );

        let duplicate_expected = typed_result(
            vec![int.clone()],
            vec![(vec![ScalarValue::Int64(Some(7))], 2)],
        );
        let duplicate_dropped = typed_result(
            vec![int.clone()],
            vec![(vec![ScalarValue::Int64(Some(7))], 1)],
        );
        assert_eq!(
            compare_results(&duplicate_expected, &duplicate_dropped),
            Err(OracleMismatch::Rows(vec![RowWeightMismatch {
                row: vec![ScalarValue::Int64(Some(7))],
                expected_weight: 2,
                actual_weight: 1,
            }]))
        );

        let same_count_sum = typed_result(
            vec![int.clone()],
            vec![
                (vec![ScalarValue::Int64(Some(10))], 1),
                (vec![ScalarValue::Int64(Some(30))], 1),
            ],
        );
        let values_changed = typed_result(vec![int], vec![(vec![ScalarValue::Int64(Some(20))], 2)]);
        assert_eq!(
            compare_results(&same_count_sum, &values_changed),
            Err(OracleMismatch::Rows(vec![
                RowWeightMismatch {
                    row: vec![ScalarValue::Int64(Some(10))],
                    expected_weight: 1,
                    actual_weight: 0,
                },
                RowWeightMismatch {
                    row: vec![ScalarValue::Int64(Some(20))],
                    expected_weight: 0,
                    actual_weight: 2,
                },
                RowWeightMismatch {
                    row: vec![ScalarValue::Int64(Some(30))],
                    expected_weight: 1,
                    actual_weight: 0,
                },
            ]))
        );
    }

    #[test]
    fn compile_failure_has_sql_seed_phase_and_full_error() {
        let error = rockstream_sql::SqlError::UnsupportedPlanNode {
            node_type: "TestNode".to_string(),
        };
        assert_eq!(
            compile_failure_message("SELECT 1", 7, &error),
            "SQL oracle failure: sql=\"SELECT 1\" seed=7 phase=compile contractual_error=[RS-1013] Unsupported plan node in incremental lowering: TestNode full_error=UnsupportedPlanNode { node_type: \"TestNode\" }"
        );
    }

    #[tokio::test]
    async fn typed_outer_join_and_decimal_cases_run_through_sql_planner() {
        let initial = HashMap::from([
            (
                "t1".to_string(),
                make_zset(t1_schema(), vec![vec![1, 2], vec![10, 20], vec![1, 1]], 1),
            ),
            (
                "t2".to_string(),
                make_zset(t2_schema(), vec![vec![1], vec![0], vec![1]], 1),
            ),
        ]);
        let empty_left = ArrowZSet::empty(t1_schema());
        let empty_right = ArrowZSet::empty(t2_schema());
        let insert_match = HashMap::from([
            ("t1".to_string(), empty_left.clone()),
            (
                "t2".to_string(),
                make_zset(t2_schema(), vec![vec![2], vec![7], vec![1]], 1),
            ),
        ]);
        let retract_zero_match = HashMap::from([
            ("t1".to_string(), empty_left),
            (
                "t2".to_string(),
                make_zset(t2_schema(), vec![vec![1], vec![0], vec![1]], -1),
            ),
        ]);
        let report = run_case_with_deltas(
            "SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            501,
            initial,
            vec![
                ("insert_match", insert_match),
                ("retract_zero_match", retract_zero_match),
            ],
            vec![
                "snapshot: t1 ids 1,2; t2 id 1 value 0".to_string(),
                "insert t2 id 2 value 7".to_string(),
                "retract t2 id 1 value 0".to_string(),
            ],
        )
        .await;
        assert_eq!(
            report.outcome,
            OracleCaseOutcome::Compared {
                epochs: vec!["snapshot", "insert_match", "retract_zero_match"],
            }
        );

        let decimal_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Decimal128(12, 2), false),
            Field::new("category", DataType::Int64, false),
        ]));
        let decimal_rows = |rows: &[(i64, i128, i64, i64)]| {
            let values = Decimal128Array::from(
                rows.iter()
                    .map(|(_, value, _, _)| *value)
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(12, 2)
            .unwrap();
            let batch = RecordBatch::try_new(
                decimal_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(values) as ArrayRef,
                    Arc::new(Int64Array::from(
                        rows.iter()
                            .map(|(_, _, category, _)| *category)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ],
            )
            .unwrap();
            ArrowZSet::new(
                batch,
                rows.iter().map(|(_, _, _, weight)| *weight).collect(),
            )
        };
        let decimal_initial = HashMap::from([
            (
                "t1".to_string(),
                decimal_rows(&[(1, 125, 1, 1), (2, 875, 2, 1)]),
            ),
            ("t2".to_string(), empty_right.clone()),
        ]);
        let decimal_insert = HashMap::from([
            ("t1".to_string(), decimal_rows(&[(3, 12345, 3, 1)])),
            ("t2".to_string(), empty_right.clone()),
        ]);
        let decimal_retract = HashMap::from([
            ("t1".to_string(), decimal_rows(&[(3, 12345, 3, -1)])),
            ("t2".to_string(), empty_right),
        ]);
        let report = run_case_with_deltas(
            "SELECT val FROM t1",
            502,
            decimal_initial,
            vec![
                ("decimal_insert", decimal_insert),
                ("decimal_retract", decimal_retract),
            ],
            vec![
                "snapshot: t1 values 1.25, 8.75".to_string(),
                "insert t1 value 123.45".to_string(),
                "retract t1 value 123.45".to_string(),
            ],
        )
        .await;
        assert_eq!(
            report.outcome,
            OracleCaseOutcome::Compared {
                epochs: vec!["snapshot", "decimal_insert", "decimal_retract"],
            }
        );
    }

    #[tokio::test]
    async fn nullable_duplicate_and_empty_histories_match_each_epoch() {
        fn rows(schema: SchemaRef, values: &[[Option<i64>; 3]], weight: i64) -> ArrowZSet {
            let scalars: Vec<_> = values
                .iter()
                .map(|row| row.iter().copied().map(ScalarValue::Int64).collect())
                .collect();
            let mut zset = rebuild_zset(schema, &scalars);
            zset.weights.fill(weight);
            zset
        }

        let left = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
            Field::new("category", DataType::Int64, false),
        ]));
        let right = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Int64, true),
            Field::new("group_id", DataType::Int64, false),
        ]));
        let pair = |t1: &[[Option<i64>; 3]], t2: &[[Option<i64>; 3]], weight| {
            HashMap::from([
                ("t1".to_string(), rows(left.clone(), t1, weight)),
                ("t2".to_string(), rows(right.clone(), t2, weight)),
            ])
        };
        let seed = 239;
        let mut rng = FuzzRng::new(seed);
        let duplicate = [Some(1), None, Some(1)];
        let negative = [Some(-1), Some(-2), Some(2)];
        let left_new = [Some(2), Some(rng.next_range(1, 9)), Some(3)];
        let right_match = [Some(1), Some(rng.next_range(1, 9)), Some(1)];
        let right_new = [Some(2), Some(rng.next_range(1, 9)), Some(3)];
        let initial = pair(
            &[duplicate, duplicate, [Some(0), Some(0), Some(1)], negative],
            &[right_match, [None, Some(0), Some(1)]],
            1,
        );
        let first = HashMap::from([
            (
                "t1".to_string(),
                concat_zsets(
                    &rows(left.clone(), &[duplicate], -1),
                    &rows(left.clone(), &[left_new], 1),
                ),
            ),
            (
                "t2".to_string(),
                concat_zsets(
                    &rows(right.clone(), &[right_match], -1),
                    &rows(right.clone(), &[right_new], 1),
                ),
            ),
        ]);
        let second = pair(&[duplicate, negative], &[right_new], -1);
        let third = pair(&[duplicate, negative], &[right_match, right_new], 1);
        let history = vec![
            format!("snapshot(seed={seed}): duplicate 1, zero, negative, NULL, join match"),
            "retract one duplicate and last join match; insert matching 2".to_string(),
            "retract last duplicate and group 2 member; retract prior-epoch match 2".to_string(),
            "reinsert duplicate, group member, and join matches".to_string(),
        ];
        for sql in [
            "SELECT id, val FROM t1",
            "SELECT COUNT(val) FROM t1",
            "SELECT COUNT(*) FROM t1",
            "SELECT category, COUNT(val) FROM t1 GROUP BY category",
            "SELECT category, COUNT(*) FROM t1 GROUP BY category",
            "SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2)",
            "SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        ] {
            let report = run_case_with_deltas(
                sql,
                seed,
                initial.clone(),
                vec![
                    ("mixed", first.clone()),
                    ("extinction", second.clone()),
                    ("reinsert", third.clone()),
                ],
                history.clone(),
            )
            .await;
            assert_eq!(
                report.outcome,
                OracleCaseOutcome::Compared {
                    epochs: vec!["snapshot", "mixed", "extinction", "reinsert"],
                },
                "sql={sql}"
            );
            assert_eq!(report.operation_history, history);
        }

        let null_right = [None, Some(0), Some(4)];
        let not_in_initial = pair(&[negative], &[right_match], 1);
        let insert_null = pair(&[], &[null_right], 1);
        let remove_null = pair(&[], &[null_right], -1);
        for sql in [
            "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2)",
            "SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        ] {
            let report = run_case_with_deltas(
                sql,
                seed + 1,
                not_in_initial.clone(),
                vec![
                    ("insert_right_null", insert_null.clone()),
                    ("remove_right_null", remove_null.clone()),
                ],
                vec![
                    "snapshot: left -1, right 1".into(),
                    "insert right NULL".into(),
                    "remove right NULL".into(),
                ],
            )
            .await;
            assert_eq!(
                report.outcome,
                OracleCaseOutcome::Compared {
                    epochs: vec!["snapshot", "insert_right_null", "remove_right_null"],
                },
                "sql={sql}"
            );
        }

        let empty = pair(&[], &[], 1);
        let inserted = pair(&[duplicate], &[right_match], 1);
        let retracted = pair(&[duplicate], &[right_match], -1);
        for sql in [
            "SELECT COUNT(val) FROM t1",
            "SELECT COUNT(*) FROM t1",
            "SELECT category, COUNT(val) FROM t1 GROUP BY category",
            "SELECT category, COUNT(*) FROM t1 GROUP BY category",
            "SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        ] {
            let report = run_case_with_deltas(
                sql,
                240,
                empty.clone(),
                vec![
                    ("insert", inserted.clone()),
                    ("empty_again", retracted.clone()),
                    ("reinsert", inserted.clone()),
                ],
                vec![
                    "empty snapshot".into(),
                    "insert match".into(),
                    "remove last member".into(),
                    "reinsert match".into(),
                ],
            )
            .await;
            assert_eq!(
                report.outcome,
                OracleCaseOutcome::Compared {
                    epochs: vec!["snapshot", "insert", "empty_again", "reinsert"],
                },
                "sql={sql}"
            );
        }
    }

    #[test]
    fn equivalent_batch_splits_and_reorders_preserve_the_complete_bag() {
        // One frontier: the initial duplicate makes both intermediate retraction orders valid.
        let schema = t1_schema();
        let initial = make_zset(schema.clone(), vec![vec![1, 1], vec![3, 3], vec![1, 1]], 1);
        let retract = make_zset(schema.clone(), vec![vec![1], vec![3], vec![1]], -1);
        let insert = make_zset(schema, vec![vec![2], vec![0], vec![1]], 1);
        let coalesced = apply_delta_physically(&initial, &concat_zsets(&retract, &insert));
        let split = apply_delta_physically(&apply_delta_physically(&initial, &retract), &insert);
        let reordered =
            apply_delta_physically(&apply_delta_physically(&initial, &insert), &retract);
        let result = |zset: &ArrowZSet| {
            let mut bag = empty_result(zset.data.schema().as_ref());
            add_batch_rows(&mut bag, &zset.data, &zset.weights);
            bag
        };
        let expected = typed_result(
            vec![
                OracleField {
                    data_type: DataType::Int64,
                    nullable: false
                };
                3
            ],
            vec![
                (
                    vec![
                        ScalarValue::Int64(Some(1)),
                        ScalarValue::Int64(Some(3)),
                        ScalarValue::Int64(Some(1)),
                    ],
                    1,
                ),
                (
                    vec![
                        ScalarValue::Int64(Some(2)),
                        ScalarValue::Int64(Some(0)),
                        ScalarValue::Int64(Some(1)),
                    ],
                    1,
                ),
            ],
        );
        for actual in [&coalesced, &split, &reordered] {
            assert_eq!(result(actual), expected);
        }
    }

    #[test]
    fn absent_retraction_is_reported() {
        let current = ArrowZSet::empty(t1_schema());
        let delta = make_zset(t1_schema(), vec![vec![1], vec![2], vec![3]], -1);
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply_delta_physically(&current, &delta)
        }))
        .unwrap_err();
        assert_eq!(
            failure.downcast_ref::<String>().unwrap(),
            "invalid retraction of absent row [Int64(1), Int64(2), Int64(3)]"
        );
    }
}
