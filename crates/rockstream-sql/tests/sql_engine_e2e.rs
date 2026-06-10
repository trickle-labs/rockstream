//! End-to-end SQL Engine milestone test (v0.10 — IVM-6).
//!
//! ## Test: `sql_engine_create_view_join_group_by`
//!
//! Proves:
//! 1. **Compiles**: `SqlFrontend` parses and lowers
//!    `CREATE VIEW revenue AS SELECT o.region, SUM(l.amount) FROM orders o
//!     JOIN lineitem l ON o.id = l.order_id GROUP BY o.region`
//!    without error.
//! 2. **Deploys**: the plan is stored in the schema catalog.
//! 3. **Maintains incrementally**: the operator chain processes delta batches
//!    and the accumulated incremental output matches the batch oracle at every
//!    epoch boundary.
//!
//! ## Oracle property
//!
//! At every epoch N, the accumulated incremental output equals the result of
//! running the full SQL query over the accumulated input state.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_sql::catalog::{ColumnDef, SchemaCatalog};
use rockstream_sql::SqlFrontend;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

// ─── Schemas ─────────────────────────────────────────────────────────────────

/// orders(region: Int64, id: Int64)
fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

/// lineitem(order_id: Int64, amount: Int64)
fn lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn orders_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = orders_schema();
    let region: Vec<i64> = rows.iter().map(|(r, _, _)| *r).collect();
    let id: Vec<i64> = rows.iter().map(|(_, id, _)| *id).collect();
    let w: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(region)),
            Arc::new(Int64Array::from(id)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn lineitem_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = lineitem_schema();
    let order_id: Vec<i64> = rows.iter().map(|(oid, _, _)| *oid).collect();
    let amount: Vec<i64> = rows.iter().map(|(_, a, _)| *a).collect();
    let w: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_id)),
            Arc::new(Int64Array::from(amount)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn empty_orders() -> ArrowZSet {
    ArrowZSet::empty(orders_schema())
}

fn empty_lineitem() -> ArrowZSet {
    ArrowZSet::empty(lineitem_schema())
}

async fn open_shard(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("catalog", store).build().await.unwrap())
}

fn col(name: &str, dt: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: dt.to_string(),
        nullable: false,
    }
}

// ─── Batch oracle ─────────────────────────────────────────────────────────────

/// Compute the batch oracle: `SELECT region, SUM(amount)` over all joined rows.
///
/// Algorithm:
/// 1. Join orders and lineitem on order.id = lineitem.order_id.
/// 2. Multiply amounts by their Z-set weights.
/// 3. GROUP BY region, SUM(amount * weight).
///
/// Returns a BTreeMap<region, sum_amount> for all regions with positive weight.
fn batch_oracle(
    orders_acc: &BTreeMap<(i64, i64), i64>, // (region, id) → weight
    lineitem_acc: &BTreeMap<(i64, i64), i64>, // (order_id, amount) → weight
) -> BTreeMap<i64, i64> {
    let mut region_sum: BTreeMap<i64, i64> = BTreeMap::new();

    for ((region, id), o_weight) in orders_acc {
        if *o_weight <= 0 {
            continue;
        }
        // Find all lineitems for this order_id.
        for ((order_id, amount), l_weight) in lineitem_acc {
            if order_id != id || *l_weight <= 0 {
                continue;
            }
            let contribution = amount * o_weight * l_weight;
            *region_sum.entry(*region).or_insert(0) += contribution;
        }
    }

    // Only keep regions with positive sum.
    region_sum.retain(|_, v| *v > 0);
    region_sum
}

/// Accumulate an ArrowZSet (schema: k, v) into a BTreeMap<(k,v), weight>.
fn accumulate_kv(acc: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
    if batch.is_empty() {
        return;
    }
    let k_col = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v_col = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        let key = (k_col.value(i), v_col.value(i));
        let entry = acc.entry(key).or_insert(0);
        *entry += batch.weights[i];
        if *entry == 0 {
            acc.remove(&key);
        }
    }
}

/// Collect incremental aggregate output: region → current sum from ArrowZSet deltas.
///
/// The AggregateOp output schema is (k, sum_v, count, avg_v).
/// AggregateOp emits (retract_old, weight=-1) + (insert_new, weight=+1) for updates,
/// and only (retract_old, weight=-1) for group deletions.
///
/// Two-pass strategy: first apply retractions (remove stale state), then
/// apply insertions (set new state).  This correctly handles both updates
/// and deletions within a single output batch.
fn collect_agg_output(acc: &mut BTreeMap<i64, i64>, batch: &ArrowZSet) {
    if batch.is_empty() {
        return;
    }
    let k_col = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum_col = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Pass 1: retractions — remove old state for each retracted key.
    for i in 0..batch.num_rows() {
        if batch.weights[i] < 0 {
            acc.remove(&k_col.value(i));
        }
    }
    // Pass 2: insertions — set new state for each inserted key.
    for i in 0..batch.num_rows() {
        if batch.weights[i] > 0 {
            acc.insert(k_col.value(i), sum_col.value(i));
        }
    }
}

// ─── The end-to-end test ─────────────────────────────────────────────────────

/// End-to-end proof: SQL compiles, deploys, and maintains incrementally.
///
/// The query `SELECT o.region, SUM(l.amount) FROM orders o JOIN lineitem l
/// ON o.id = l.order_id GROUP BY o.region` is compiled via `SqlFrontend`,
/// deployed to a catalog, and the operator chain (JoinOp + AggregateOp)
/// processes delta batches producing output that matches the batch oracle
/// at every epoch boundary.
#[tokio::test]
async fn sql_engine_create_view_join_group_by() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;

    // ── Phase 1: Compile — SQL parses and lowers without error ─────────────

    let frontend = SqlFrontend::new();
    frontend.register_table("orders", orders_schema()).unwrap();
    frontend
        .register_table("lineitem", lineitem_schema())
        .unwrap();

    let query = "SELECT o.region, SUM(l.amount) \
                 FROM orders o JOIN lineitem l ON o.id = l.order_id \
                 GROUP BY o.region";

    // Compile: SQL → PlanNode.
    let plan = frontend
        .sql_to_plan_node(query)
        .await
        .expect("query must compile to PlanNode (proof: SQL Engine milestone)");

    // Verify the plan contains an Aggregate and a Join node.
    fn has_aggregate(p: &rockstream_plan::PlanNode) -> bool {
        match p {
            rockstream_plan::PlanNode::Aggregate { .. } => true,
            rockstream_plan::PlanNode::Filter { input, .. }
            | rockstream_plan::PlanNode::Project { input, .. }
            | rockstream_plan::PlanNode::Distinct { input, .. }
            | rockstream_plan::PlanNode::Map { input, .. }
            | rockstream_plan::PlanNode::ViewSink { child: input, .. }
            | rockstream_plan::PlanNode::Exchange { child: input, .. } => has_aggregate(input),
            rockstream_plan::PlanNode::InnerJoin { left, right, .. }
            | rockstream_plan::PlanNode::OuterJoin { left, right, .. }
            | rockstream_plan::PlanNode::Union { left, right } => {
                has_aggregate(left) || has_aggregate(right)
            }
            _ => false,
        }
    }

    fn has_join(p: &rockstream_plan::PlanNode) -> bool {
        match p {
            rockstream_plan::PlanNode::InnerJoin { .. }
            | rockstream_plan::PlanNode::OuterJoin { .. } => true,
            rockstream_plan::PlanNode::Filter { input, .. }
            | rockstream_plan::PlanNode::Project { input, .. }
            | rockstream_plan::PlanNode::Distinct { input, .. }
            | rockstream_plan::PlanNode::Map { input, .. }
            | rockstream_plan::PlanNode::Aggregate { input, .. }
            | rockstream_plan::PlanNode::ViewSink { child: input, .. }
            | rockstream_plan::PlanNode::Exchange { child: input, .. } => has_join(input),
            rockstream_plan::PlanNode::Union { left, right } => has_join(left) || has_join(right),
            _ => false,
        }
    }

    assert!(
        has_aggregate(&plan),
        "compiled plan must contain Aggregate node: {plan:?}"
    );
    assert!(
        has_join(&plan),
        "compiled plan must contain Join node: {plan:?}"
    );

    // ── Phase 2: Deploy — store the view in the catalog ────────────────────

    let catalog = SchemaCatalog::new(db.clone());
    frontend
        .create_view(
            &catalog,
            "revenue",
            query,
            vec![col("region", "Int64"), col("sum_amount", "Int64")],
        )
        .await
        .expect("CREATE VIEW must succeed (proof: deploy milestone)");

    // Verify the view was stored.
    let view = catalog
        .load_view("revenue")
        .await
        .expect("catalog read must succeed")
        .expect("revenue view must be in catalog");
    assert_eq!(view.name, "revenue");

    // ── Phase 3: Incremental maintenance oracle ────────────────────────────
    //
    // Instantiate JoinOp (orders JOIN lineitem on id=order_id) +
    // AggregateOp (SUM(amount) GROUP BY region) and process delta epochs.
    // At each epoch, assert incremental output == batch oracle.

    // orders schema index: region=0, id=1
    // lineitem schema index: order_id=0, amount=1
    // Join on orders.id (col 1) = lineitem.order_id (col 0).
    // Join: orders(region=0,id=1) JOIN lineitem(order_id=0,amount=1) ON orders.id=lineitem.order_id.
    // Join output schema: (region=0, id=1, order_id=2, amount=3).
    let join_op = JoinOp::new(
        OperatorId(10),
        vec![1], // left_keys: orders.id
        vec![0], // right_keys: lineitem.order_id
    );

    // Project: extract (region=col0, amount=col3) → (k=col0, v=col1) for AggregateOp.
    let project_op = ProjectOp::new(vec![
        NamedExpr::new("region", rockstream_plan::Expr::Column(0)),
        NamedExpr::new("amount", rockstream_plan::Expr::Column(3)),
    ]);

    let agg_op = AggregateOp::new(OperatorId(20));

    // Accumulators for oracle comparison.
    let mut orders_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new(); // (region, id) → weight
    let mut lineitem_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new(); // (order_id, amount) → weight
    let mut agg_output_state: BTreeMap<i64, i64> = BTreeMap::new(); // region → current_sum

    // Epoch 0: insert orders(region=1,id=100),(region=2,id=200)
    //          and lineitem(order_id=100,amount=500),(order_id=200,amount=300)
    {
        let o_delta = orders_batch(&[(1, 100, 1), (2, 200, 1)]);
        let l_delta = lineitem_batch(&[(100, 500, 1), (200, 300, 1)]);

        accumulate_kv(&mut orders_acc, &o_delta);
        accumulate_kv(&mut lineitem_acc, &l_delta);

        // Process through JoinOp: left=orders, right=lineitem.
        let join_out = join_op.process_epoch(o_delta, l_delta).unwrap();
        let proj_out = project_op.process_delta(join_out).unwrap();
        let agg_out = agg_op.process_delta(proj_out).unwrap();
        collect_agg_output(&mut agg_output_state, &agg_out);

        // Batch oracle: region 1 → 500, region 2 → 300.
        let expected = batch_oracle(&orders_acc, &lineitem_acc);
        assert_eq!(
            agg_output_state, expected,
            "epoch 0 incremental != batch: incremental={agg_output_state:?} batch={expected:?}"
        );
    }

    // Epoch 1: insert another lineitem for order 100 (amount=200).
    {
        let o_delta = empty_orders();
        let l_delta = lineitem_batch(&[(100, 200, 1)]);

        accumulate_kv(&mut orders_acc, &o_delta);
        accumulate_kv(&mut lineitem_acc, &l_delta);

        let join_out = join_op.process_epoch(o_delta, l_delta).unwrap();
        let proj_out = project_op.process_delta(join_out).unwrap();
        let agg_out = agg_op.process_delta(proj_out).unwrap();
        collect_agg_output(&mut agg_output_state, &agg_out);

        // region 1 now has orders(id=100) × lineitem[(100,500),(100,200)] = 700
        let expected = batch_oracle(&orders_acc, &lineitem_acc);
        assert_eq!(
            agg_output_state, expected,
            "epoch 1 incremental != batch: incremental={agg_output_state:?} batch={expected:?}"
        );
        assert_eq!(
            agg_output_state.get(&1).copied(),
            Some(700),
            "region 1 sum should be 700 after epoch 1"
        );
    }

    // Epoch 2: add a new order for region 1 (id=101) + lineitem for it.
    {
        let o_delta = orders_batch(&[(1, 101, 1)]);
        let l_delta = lineitem_batch(&[(101, 400, 1)]);

        accumulate_kv(&mut orders_acc, &o_delta);
        accumulate_kv(&mut lineitem_acc, &l_delta);

        let join_out = join_op.process_epoch(o_delta, l_delta).unwrap();
        let proj_out = project_op.process_delta(join_out).unwrap();
        let agg_out = agg_op.process_delta(proj_out).unwrap();
        collect_agg_output(&mut agg_output_state, &agg_out);

        let expected = batch_oracle(&orders_acc, &lineitem_acc);
        assert_eq!(
            agg_output_state, expected,
            "epoch 2 incremental != batch: incremental={agg_output_state:?} batch={expected:?}"
        );
    }

    // Epoch 3: retract order(region=2,id=200) → lineitem(200,300) no longer matches.
    {
        let o_delta = orders_batch(&[(2, 200, -1)]);
        let l_delta = empty_lineitem();

        accumulate_kv(&mut orders_acc, &o_delta);
        accumulate_kv(&mut lineitem_acc, &l_delta);

        let join_out = join_op.process_epoch(o_delta, l_delta).unwrap();
        let proj_out = project_op.process_delta(join_out).unwrap();
        let agg_out = agg_op.process_delta(proj_out).unwrap();
        collect_agg_output(&mut agg_output_state, &agg_out);

        let expected = batch_oracle(&orders_acc, &lineitem_acc);
        assert_eq!(
            agg_output_state, expected,
            "epoch 3 incremental != batch: incremental={agg_output_state:?} batch={expected:?}"
        );
        // Region 2 should disappear.
        assert!(
            !agg_output_state.contains_key(&2),
            "region 2 should be gone after retraction"
        );
    }

    // ── All epochs passed ─────────────────────────────────────────────────

    // Drop catalog before closing db (catalog holds an Arc<ShardDb> reference).
    drop(catalog);
    Arc::try_unwrap(db)
        .ok()
        .expect("single owner after catalog drop")
        .close()
        .await
        .unwrap();
}

// ─── v0.11 Window e2e test ────────────────────────────────────────────────────

/// SQL Engine e2e milestone test for window functions (v0.11 — IVM-7).
///
/// Proves:
/// 1. SQL `SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) as rn FROM t`
///    lowers to a `PlanNode::Window` containing `WindowFunc::RowNumber`.
/// 2. Processing incremental deltas through `WindowOp` accumulates to the correct
///    net state at every epoch boundary (verified against an in-process oracle).
#[tokio::test]
async fn sql_engine_window_row_number_over_view() {
    use arrow::array::ArrayRef;
    use rockstream_plan::PlanNode;
    use std::collections::HashMap;

    // Set up SqlFrontend with table t(k INT, v INT).
    let t_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let frontend = SqlFrontend::new();
    frontend.register_table("t", t_schema.clone()).unwrap();

    // Lower the window SQL.
    let sql = "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("window SQL should lower without error");

    // Verify the plan contains a Window node.
    fn has_window(plan: &PlanNode) -> bool {
        match plan {
            PlanNode::Window { .. } => true,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. }
            | PlanNode::Window { input, .. } => has_window(input),
            PlanNode::ViewSink { child, .. } | PlanNode::Exchange { child, .. } => {
                has_window(child)
            }
            PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Union { left, right }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => has_window(left) || has_window(right),
            _ => false,
        }
    }
    assert!(has_window(&plan), "plan must contain Window node: {plan:?}");

    // Set up WindowOp directly: partition by k (col 0), order by v (col 1).
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("rn", DataType::Int64, false),
    ]));
    let rn_expr = rockstream_plan::WindowExpr {
        func: rockstream_plan::WindowFunc::RowNumber,
        partition_by: vec![0],
        order_by: vec![1],
    };
    let op = WindowOp::new(out_schema, vec![rn_expr]);

    // Helper to make input ZSet from (k, v, weight) tuples.
    fn make_t(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let k: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    // Accumulate output ZSet deltas.
    let mut output_state: HashMap<(i64, i64, i64), i64> = HashMap::new();

    fn acc_out(state: &mut HashMap<(i64, i64, i64), i64>, zset: &ArrowZSet) {
        if zset.is_empty() {
            return;
        }
        let k_col = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = zset
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let r_col = zset
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..zset.num_rows() {
            let key = (k_col.value(i), v_col.value(i), r_col.value(i));
            *state.entry(key).or_insert(0) += zset.weights[i];
        }
    }

    fn live_rows(state: &HashMap<(i64, i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut rows: Vec<(i64, i64, i64)> = state
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&r, _)| r)
            .collect();
        rows.sort();
        rows
    }

    // Batch oracle: compute ROW_NUMBER from accumulated state.
    let mut input_acc: std::collections::BTreeMap<(i64, i64), i64> = Default::default();

    fn batch_rn(acc: &std::collections::BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        use std::collections::HashMap;
        let mut parts: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                parts.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in parts {
            vs.sort();
            for (i, v) in vs.iter().enumerate() {
                out.push((k, *v, (i + 1) as i64));
            }
        }
        out.sort();
        out
    }

    fn acc_input(acc: &mut std::collections::BTreeMap<(i64, i64), i64>, rows: &[(i64, i64, i64)]) {
        for &(k, v, w) in rows {
            let entry = acc.entry((k, v)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                acc.remove(&(k, v));
            }
        }
    }

    // Epoch 1: insert (1,10), (1,20), (2,30).
    let e1_rows = [(1i64, 10i64, 1i64), (1, 20, 1), (2, 30, 1)];
    acc_input(&mut input_acc, &e1_rows);
    let out1 = op.process_epoch(make_t(&e1_rows), 1).unwrap();
    acc_out(&mut output_state, &out1);
    assert_eq!(
        live_rows(&output_state),
        batch_rn(&input_acc),
        "epoch 1 mismatch"
    );

    // Epoch 2: insert (1,15), (2,25); delete (1,20).
    let e2_rows = [(1, 15, 1), (2, 25, 1), (1, 20, -1)];
    acc_input(&mut input_acc, &e2_rows);
    let out2 = op.process_epoch(make_t(&e2_rows), 2).unwrap();
    acc_out(&mut output_state, &out2);
    assert_eq!(
        live_rows(&output_state),
        batch_rn(&input_acc),
        "epoch 2 mismatch"
    );

    // Epoch 3: insert (1,5).
    let e3_rows = [(1, 5, 1)];
    acc_input(&mut input_acc, &e3_rows);
    let out3 = op.process_epoch(make_t(&e3_rows), 3).unwrap();
    acc_out(&mut output_state, &out3);
    assert_eq!(
        live_rows(&output_state),
        batch_rn(&input_acc),
        "epoch 3 mismatch"
    );

    // Verify specific values at end of epoch 3:
    // k=1: v=5,10,15 with rn=1,2,3; k=2: v=25,30 with rn=1,2.
    let final_state = live_rows(&output_state);
    assert!(final_state.contains(&(1, 5, 1)));
    assert!(final_state.contains(&(1, 10, 2)));
    assert!(final_state.contains(&(1, 15, 3)));
    assert!(final_state.contains(&(2, 25, 1)));
    assert!(final_state.contains(&(2, 30, 2)));
    assert_eq!(final_state.len(), 5);
}
