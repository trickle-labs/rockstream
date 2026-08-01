//! Compile a `PlanNode` tree directly into an executable operator chain.
//!
//! Originally (v0.51.3 Slice 3) `compile_plan` only covered the stateless
//! subset (`Source`/`Filter`/`Project`/`Map`). v0.51.4 Slices 1-5 extend it
//! to compile `Aggregate`, `TumbleWindow`/`HopWindow`, `Window`/`TopK`,
//! `Distinct`, and `InnerJoin`/`OuterJoin` PlanNodes into their existing,
//! oracle-proven `rockstream-ops` operators (`AggregateOp`,
//! `TumbleWindowOp`/`HopWindowOp`, `WindowOp`/`TopKOp`, `DistinctOp`,
//! `JoinOp`/`OuterJoinOp`), hosted by a `StatefulPipeline`/`JoinPipeline`
//! (`live_exec.rs`) instead of the stateless-only `LinearPipeline`.
//!
//! `SessionWindow`/`Recursion`/`Lateral` and other richer shapes are still
//! not supported here — those return `OpError::UnsupportedPlanNode`.
//! `compile_plan` returns `OpError::UnsupportedPlanNode` for any node it
//! does not recognize, mirroring `DiffCtx::next_op_id`'s `OperatorId`
//! assignment convention for the ids it does hand out.
//!
//! ## Operator-family scope (this version)
//!
//! `AggregateOp` (pre-existing, oracle-proven since v0.5) only supports a
//! single `Int64` group-by key and a single `Int64` aggregate value (fixed
//! `(k, v) -> (k, sum_v, count, avg_v)` shape) — this is a pre-existing
//! operator constraint, not something this wiring extends. `compile_plan`
//! therefore only compiles `PlanNode::Aggregate` nodes with exactly one
//! `group_by` expression and exactly one `Sum`/`Count`/`Avg` aggregate
//! expression; anything richer (multiple group-by columns, multiple
//! aggregates, `Min`/`Max`/`ApproxCountDistinct`/...) returns
//! `OpError::UnsupportedPlanNode` rather than silently mis-executing.
//!
//! `JoinOp`/`OuterJoinOp` (pre-existing, oracle-proven since v0.8/v0.9) only
//! support `Int64` columns on both sides, same as the other stateful
//! operators above — a join side is only compiled when its subtree is
//! `Source`/`Filter`/`Project`/`Map` (optionally nesting further stateful
//! ops) resolving to exactly one base-table `Source`; anything else (e.g. a
//! join whose side can't be traced back to a single named relation) returns
//! `OpError::UnsupportedPlanNode`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rockstream_plan::{AggregateFunc, Expr, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;

use crate::aggregate::AggregateOp;
use crate::distinct::DistinctOp;
use crate::error::OpError;
use crate::expr::lit;
use crate::filter::FilterOp;
use crate::join::JoinOp;
use crate::live_exec::{
    int64_schema, next_stateful_op_id, with_view_id_scope, GroupKeyPacker, JoinKind, JoinPipeline,
    Stage, StatefulPipeline, Utf8ColumnPacker, Utf8KeyPacker,
};
use crate::map::MapOp;
use crate::outer_join::OuterJoinOp;
use crate::project::{NamedExpr, ProjectOp};
use crate::sink::ViewSinkOp;
use crate::time_window::{HopWindowOp, SessionWindowOp, TumbleWindowOp};
use crate::topk::TopKOp;
use crate::window::WindowOp;

static NEXT_VIEW_SINK_OP_ID: AtomicU64 = AtomicU64::new(1);

/// A compiled `InnerJoin`/`OuterJoin`-shaped view (v0.51.4 Slice 3): the
/// two-input `JoinPipeline` plus the names of the two base tables whose
/// commits must be routed to it (`left_source` receives `process`'s
/// `left_delta`, `right_source` its `right_delta`).
pub struct JoinCompiled {
    pub pipeline: JoinPipeline,
    pub left_source: String,
    pub right_source: String,
}

/// The result of compiling a `PlanNode::ViewSink` tree: an executable
/// operator chain plus the sink that writes its output to storage.
pub struct CompiledView {
    /// The operator chain (everything under `ViewSink`) — may mix stateless
    /// and stateful stages (v0.51.4 Slices 1-5). Empty/unused when `join` is
    /// `Some` (a join-shaped view uses `join.pipeline` instead).
    pub pipeline: StatefulPipeline,
    /// `Some` for an `InnerJoin`/`OuterJoin`-shaped view (v0.51.4 Slice 3) —
    /// mutually exclusive with the single-input `pipeline` above.
    pub join: Option<JoinCompiled>,
    /// The sink that persists the pipeline's output to `view_output` storage.
    pub sink: ViewSinkOp,
    /// The `OperatorId` assigned to `sink` (used as the storage key prefix).
    pub sink_op_id: OperatorId,
    /// The view name from the source plan.
    pub view_name: String,
    /// Primary-key column indices for the view.
    pub pk: Vec<usize>,
}

/// Compile `plan` (which must be rooted at `PlanNode::ViewSink`) into a
/// `CompiledView` backed by `db`.
///
/// `table_schemas` maps every base-table name the plan may reference to its
/// real column schema — needed to (a) construct `TumbleWindow`/`HopWindow`/
/// `Window`/`TopK`/`InnerJoin`/`OuterJoin` operators (schema-preserving
/// through `Filter`), and (b) statically reject compiling
/// `Aggregate`/`Distinct`/`TumbleWindow`/`HopWindow`/`Window`/`TopK`/
/// `InnerJoin`/`OuterJoin` when the columns those operators would actually
/// process are not `Int64` — every one of those operators only supports
/// `Int64` columns (pre-existing constraint, not introduced by this
/// wiring); silently coercing a `Utf8`/`Boolean`/`Float64` column to `0`
/// (which is what these operators' internal `downcast_ref::<Int64Array>()
/// .unwrap_or(0)` pattern would otherwise do) would silently corrupt
/// results instead of falling back to `view_materializer.rs`. A missing
/// table name defaults to an empty schema (harmless unless a stateful op
/// actually needs it, in which case compilation correctly fails closed).
pub fn compile_plan(
    plan: &PlanNode,
    db: Arc<ShardDb>,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<CompiledView, OpError> {
    // Stateless operators (Filter/Project/Map) carry no persisted identity;
    // the sink is the only node whose `OperatorId` addresses persisted
    // `view_output` storage, so it must be unique across compiled views.
    let sink_op_id = OperatorId(NEXT_VIEW_SINK_OP_ID.fetch_add(1, Ordering::Relaxed));
    compile_plan_with_sink_id(plan, db, table_schemas, sink_op_id)
}

/// Same as `compile_plan`, but reuses `sink_op_id` instead of minting a
/// fresh one — used by `GatewayHandler::recover_compiled_views` when
/// recompiling a view that was already `CREATE VIEW`'d in a prior process:
/// the view's `sink_op_id` (`CatalogView.op_id`) is already durably known
/// and must be reused verbatim, or the recompiled sink would write to a
/// fresh, disconnected `view_output` key instead of the one already on
/// disk. Every *internal* stateful stage's id is made reproducible the same
/// way, via `with_view_id_scope` below (seeded from `view_name`, not
/// `sink_op_id`, since the internal ids predate `sink_op_id` existing as a
/// stable, catalog-durable handle).
pub fn compile_plan_with_sink_id(
    plan: &PlanNode,
    db: Arc<ShardDb>,
    table_schemas: &HashMap<String, SchemaRef>,
    sink_op_id: OperatorId,
) -> Result<CompiledView, OpError> {
    let PlanNode::ViewSink {
        view_name,
        pk,
        child,
    } = plan
    else {
        return Err(OpError::unsupported_plan_node(format!(
            "expected PlanNode::ViewSink at the root, found {}",
            plan_node_kind(plan)
        )));
    };

    with_view_id_scope(view_name, || {
        compile_plan_body(view_name, pk, child, db, table_schemas, sink_op_id)
    })
}

fn compile_plan_body(
    view_name: &str,
    pk: &[usize],
    child: &PlanNode,
    db: Arc<ShardDb>,
    table_schemas: &HashMap<String, SchemaRef>,
    sink_op_id: OperatorId,
) -> Result<CompiledView, OpError> {
    // v0.51.4 Slice 3: an `InnerJoin`/`OuterJoin`-shaped view compiles
    // through the two-input `JoinPipeline` path instead of the single-input
    // `StatefulPipeline` below.
    if let Some(shape) = try_compile_join_shape(child, table_schemas)? {
        let sink = ViewSinkOp::new(db, sink_op_id);
        return Ok(CompiledView {
            pipeline: StatefulPipeline::new(),
            join: Some(JoinCompiled {
                pipeline: JoinPipeline::new(
                    shape.left_pre,
                    shape.right_pre,
                    shape.join,
                    shape.post,
                ),
                left_source: shape.left_source,
                right_source: shape.right_source,
            }),
            sink,
            sink_op_id,
            view_name: view_name.to_string(),
            pk: pk.to_vec(),
        });
    }

    let source_name = find_source_name(child).unwrap_or_default();
    let source_schema = table_schemas
        .get(&source_name)
        .cloned()
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    let (stages, _out_schema) = compile_node(child, &source_schema)?;
    let mut pipeline = StatefulPipeline::new();
    for stage in stages {
        pipeline = pipeline.push(stage);
    }

    let sink = ViewSinkOp::new(db, sink_op_id);

    Ok(CompiledView {
        pipeline,
        join: None,
        sink,
        sink_op_id,
        view_name: view_name.to_string(),
        pk: pk.to_vec(),
    })
}

/// Best-effort static type of `expr` evaluated against `schema` — `Some(dt)`
/// only when confidently known, `None` when it can't be determined
/// statically (e.g. a `ScalarUdf` call). Used only to *gate* whether a
/// stateful, `Int64`-only operator may be compiled here; `Filter`/`Project`/
/// `Map` never consult this and remain fully dynamic.
fn static_expr_type(expr: &Expr, schema: &Schema) -> Option<DataType> {
    match expr {
        Expr::Column(i) => schema.fields().get(*i).map(|f| f.data_type().clone()),
        // `encode_scalar` (rockstream-sql) encodes every integer literal as
        // an 8-byte big-endian i64 (booleans as 1 byte, floats also 8 bytes
        // but distinguishable only by context) — an 8-byte literal is
        // treated as `Int64` here (matches every literal `compile_plan`
        // actually receives for group-by/aggregate columns: integer GROUP BY
        // keys and `COUNT(*)`'s injected `Literal(1i64)`).
        Expr::Literal(bytes) if bytes.len() == 8 => Some(DataType::Int64),
        Expr::BinaryOp { left, right, .. } => {
            match (
                static_expr_type(left, schema),
                static_expr_type(right, schema),
            ) {
                (Some(DataType::Int64), Some(DataType::Int64)) => Some(DataType::Int64),
                _ => None,
            }
        }
        // v0.51.4 Slice 8: a `CASE` all of whose branches (every `then` and
        // the `else`) are statically `Int64` is itself `Int64` — needed to
        // let Nexmark q15's `SUM(CASE WHEN ... THEN price ELSE 0 END)` and
        // `COUNT(DISTINCT CASE WHEN ... THEN bidder END)` (branches are
        // plain `Int64` columns/literals) compile as aggregate inputs.
        Expr::Case {
            when_then,
            else_expr,
        } => {
            let branches_int64 = when_then
                .iter()
                .all(|(_, then)| static_expr_type(then, schema) == Some(DataType::Int64))
                && static_expr_type(else_expr, schema) == Some(DataType::Int64);
            branches_int64.then_some(DataType::Int64)
        }
        _ => None,
    }
}

fn expr_is_int64(expr: &Expr, schema: &Schema) -> bool {
    static_expr_type(expr, schema) == Some(DataType::Int64)
}

fn expr_is_utf8(expr: &Expr, schema: &Schema) -> bool {
    static_expr_type(expr, schema) == Some(DataType::Utf8)
}

fn schema_all_int64(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| f.data_type() == &DataType::Int64)
}

/// Find the single base-table `Source` name reachable from `node` through a
/// chain of `Filter`/`Project`/`Map` (schema-preserving-enough) wrappers, or
/// through a single-input stateful node (`Aggregate`/`TumbleWindow`/
/// `HopWindow`/`SessionWindow`/`Window`/`TopK`/`Distinct`) — `None` if `node`
/// doesn't resolve to exactly one such relation (e.g. it's itself a join or
/// something else `compile_plan` doesn't trace through).
///
/// This must recurse through every stateful node kind `compile_node` knows
/// how to compile: `compile_plan`'s caller (`compile_plan` itself) uses this
/// function's result to resolve the *real* base-table schema from
/// `table_schemas` before calling `compile_node`. Missing a stateful variant
/// here silently starves every node beneath it of the real schema — this
/// previously caused a `GROUP BY` on a non-`Int64` column (e.g. `TEXT`) to
/// wrongly compile, because a `Project` between `Aggregate` and `Source`
/// fell back to `DataType::Int64` for any column whose type couldn't be
/// statically determined against the (wrongly empty) schema.
fn find_source_name(node: &PlanNode) -> Option<String> {
    match node {
        PlanNode::Source { name } => Some(name.clone()),
        PlanNode::Filter { input, .. } => find_source_name(input),
        PlanNode::Project { input, .. } => find_source_name(input),
        PlanNode::Map { input, .. } => find_source_name(input),
        PlanNode::Aggregate { input, .. } => find_source_name(input),
        PlanNode::TumbleWindow { input, .. } => find_source_name(input),
        PlanNode::HopWindow { input, .. } => find_source_name(input),
        PlanNode::SessionWindow { input, .. } => find_source_name(input),
        PlanNode::Window { input, .. } => find_source_name(input),
        PlanNode::TopK { input, .. } => find_source_name(input),
        PlanNode::Distinct { input, .. } => find_source_name(input),
        _ => None,
    }
}

/// The result of successfully recognizing an `InnerJoin`/`OuterJoin`-shaped
/// subtree — see `try_compile_join_shape`.
struct JoinShape {
    /// Stateless stages applied to the left source's delta before the join.
    left_pre: Vec<Stage>,
    /// Stateless stages applied to the right source's delta before the join.
    right_pre: Vec<Stage>,
    join: JoinKind,
    /// Stateless stages applied to the join's output, in application order
    /// (closest-to-the-join stage first) — built bottom-up as the recursion
    /// unwinds back up to `ViewSink`.
    post: Vec<Stage>,
    left_source: String,
    right_source: String,
    /// Running output column count after `post` (so far) is applied — the
    /// join's own `left_n_cols + right_n_cols` initially, updated by
    /// `Project`/`Aggregate`/`Window` wrapping arms as they change
    /// cardinality. Needed by arms (e.g. `Window`) whose operator
    /// constructor requires the *input* column count, since
    /// `try_compile_join_shape` doesn't otherwise track schema at all.
    post_n_cols: usize,
}

/// Compile one side of a join (`left` or `right` of `PlanNode::InnerJoin`/
/// `OuterJoin`): resolve its single base-table `Source`'s real schema from
/// `table_schemas`, then compile the `Filter`/`Project`/`Map`(/nested
/// stateful op) chain atop it via the existing single-input `compile_node`.
///
/// v0.51.4 Slice 8: any `Utf8` passthrough column (not the join key, not an
/// arithmetic operand — e.g. a joined-in `TEXT` column selected straight
/// through, such as `campaigns.name`/`campaigns.channel` in a view-of-view
/// join) is packed into an `Int64` surrogate via `Utf8ColumnPacker` so the
/// side's schema becomes all-`Int64` (required by `JoinOp`/`OuterJoinOp`,
/// unchanged since v0.8/v0.9). Returns the packers alongside their
/// resulting column index so the caller can unpack them from the join's
/// output afterward.
/// Build the `post`-stage unpack chain for a join's `Utf8` passthrough
/// columns: left-side packers unpack at their original column index
/// (unchanged — left columns are first in the join's output), right-side
/// packers unpack at `left_n_cols + original_index` (shifted, since the
/// join's output concatenates left columns then right columns).
fn unpack_join_utf8_post_stages(
    left_n_cols: usize,
    left_utf8: Vec<(usize, Arc<Utf8ColumnPacker>)>,
    right_utf8: Vec<(usize, Arc<Utf8ColumnPacker>)>,
) -> Vec<Stage> {
    let mut post = Vec::new();
    for (idx, packer) in left_utf8 {
        post.push(Stage::Utf8ColumnUnpack(packer, idx, next_stateful_op_id()));
    }
    for (idx, packer) in right_utf8 {
        post.push(Stage::Utf8ColumnUnpack(
            packer,
            left_n_cols + idx,
            next_stateful_op_id(),
        ));
    }
    post
}

/// `(pre-join stages, side's output schema, resolved base-table name,
/// Utf8 columns needing post-join unpack)` for one side of a join.
type JoinSideCompilation = (
    Vec<Stage>,
    SchemaRef,
    String,
    Vec<(usize, Arc<Utf8ColumnPacker>)>,
);

/// Pack every `Utf8` column of `schema` into an `Int64` surrogate via its
/// own `Utf8ColumnPacker`, appending the pack stages to `stages` — used to
/// let an `Int64`-only stateful operator (`TumbleWindowOp`/`HopWindowOp`)
/// carry a `Utf8` passthrough column (e.g. Nexmark q16/q17's `channel`)
/// through unmodified, mirroring `compile_join_side`'s identical need.
/// Returns the resulting all-`Int64` schema plus `(original_index, packer)`
/// for each packed column, for the caller to unpack afterward.
fn pack_utf8_columns(
    stages: &mut Vec<Stage>,
    schema: &SchemaRef,
) -> (SchemaRef, Vec<(usize, Arc<Utf8ColumnPacker>)>) {
    let mut utf8_packers = Vec::new();
    let mut int64_fields = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if field.data_type() == &DataType::Utf8 {
            let packer = Arc::new(Utf8ColumnPacker::new());
            stages.push(Stage::Utf8ColumnPack(
                packer.clone(),
                i,
                next_stateful_op_id(),
            ));
            utf8_packers.push((i, packer));
            int64_fields.push(Field::new(field.name(), DataType::Int64, false));
        } else {
            int64_fields.push(field.as_ref().clone());
        }
    }
    (Arc::new(Schema::new(int64_fields)), utf8_packers)
}

/// Unpack columns packed by [`pack_utf8_columns`] after the stateful
/// operator has prepended `col_offset` new columns in front of the packed
/// schema (e.g. `TumbleWindowOp`'s `window_id` at position 0) — each
/// packed column's post-operator position is `col_offset + original_index`.
fn unpack_utf8_columns(
    stages: &mut Vec<Stage>,
    col_offset: usize,
    packers: Vec<(usize, Arc<Utf8ColumnPacker>)>,
) {
    for (idx, packer) in packers {
        stages.push(Stage::Utf8ColumnUnpack(
            packer,
            col_offset + idx,
            next_stateful_op_id(),
        ));
    }
}

fn compile_join_side(
    node: &PlanNode,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<JoinSideCompilation, OpError> {
    let table_name = find_source_name(node).ok_or_else(|| {
        OpError::unsupported_plan_node(
            "join side does not resolve to a single named base-table relation",
        )
    })?;
    let schema = table_schemas
        .get(&table_name)
        .cloned()
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    let (mut stages, out_schema) = compile_node(node, &schema)?;
    let (packed_schema, utf8_packers) = pack_utf8_columns(&mut stages, &out_schema);
    Ok((stages, packed_schema, table_name, utf8_packers))
}

/// Recognize an `InnerJoin`/`OuterJoin` subtree, optionally wrapped by
/// `Filter`/`Project`/`Map` (the common "equi-join + residual filter, then a
/// final projection" Nexmark q3/q4/q8/q13/q20 shape) directly beneath
/// `ViewSink`. Returns `Ok(None)` (not an error) when `node` doesn't reach a
/// join at all, so the caller falls through to the ordinary single-input
/// `compile_node` path (which will itself report `UnsupportedPlanNode` for
/// anything neither path recognizes, with the same error text as before
/// this slice).
fn try_compile_join_shape(
    node: &PlanNode,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<Option<JoinShape>, OpError> {
    match node {
        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            ..
        } => {
            let (left_pre, left_schema, left_source, left_utf8) =
                compile_join_side(left, table_schemas)?;
            let (right_pre, right_schema, right_source, right_utf8) =
                compile_join_side(right, table_schemas)?;
            if !schema_all_int64(&left_schema) || !schema_all_int64(&right_schema) {
                return Err(OpError::unsupported_plan_node(
                    "InnerJoin over a non-Int64-only side schema (JoinOp only supports Int64 columns)",
                ));
            }
            let join_op = Arc::new(JoinOp::with_schema(
                *left_arr_id,
                left_keys.clone(),
                right_keys.clone(),
                left_schema.fields().len(),
                right_schema.fields().len(),
            ));
            let post =
                unpack_join_utf8_post_stages(left_schema.fields().len(), left_utf8, right_utf8);
            let post_n_cols = left_schema.fields().len() + right_schema.fields().len();
            Ok(Some(JoinShape {
                left_pre,
                right_pre,
                join: JoinKind::Inner(join_op),
                post,
                left_source,
                right_source,
                post_n_cols,
            }))
        }
        PlanNode::OuterJoin {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            ..
        } => {
            let (left_pre, left_schema, left_source, left_utf8) =
                compile_join_side(left, table_schemas)?;
            let (right_pre, right_schema, right_source, right_utf8) =
                compile_join_side(right, table_schemas)?;
            if !schema_all_int64(&left_schema) || !schema_all_int64(&right_schema) {
                return Err(OpError::unsupported_plan_node(
                    "OuterJoin over a non-Int64-only side schema (OuterJoinOp only supports Int64 columns)",
                ));
            }
            let join_op = Arc::new(OuterJoinOp::with_schema(
                *left_arr_id,
                *kind,
                left_keys.clone(),
                right_keys.clone(),
                left_schema.fields().len(),
                right_schema.fields().len(),
            ));
            let post =
                unpack_join_utf8_post_stages(left_schema.fields().len(), left_utf8, right_utf8);
            let post_n_cols = left_schema.fields().len() + right_schema.fields().len();
            Ok(Some(JoinShape {
                left_pre,
                right_pre,
                join: JoinKind::Outer(join_op),
                post,
                left_source,
                right_source,
                post_n_cols,
            }))
        }
        PlanNode::Filter { input, predicate } => {
            match try_compile_join_shape(input, table_schemas)? {
                Some(mut shape) => {
                    shape
                        .post
                        .push(Stage::Stateless(Arc::new(FilterOp::new(predicate.clone()))));
                    Ok(Some(shape))
                }
                None => Ok(None),
            }
        }
        PlanNode::Project { input, columns } => match try_compile_join_shape(input, table_schemas)?
        {
            Some(mut shape) => {
                let named: Vec<NamedExpr> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| NamedExpr::new(format!("col{i}"), expr.clone()))
                    .collect();
                shape.post_n_cols = named.len();
                shape
                    .post
                    .push(Stage::Stateless(Arc::new(ProjectOp::new(named))));
                Ok(Some(shape))
            }
            None => Ok(None),
        },
        PlanNode::Map { input, func } => match try_compile_join_shape(input, table_schemas)? {
            Some(mut shape) => {
                shape.post_n_cols += 1;
                shape.post.push(Stage::Stateless(Arc::new(MapOp::new(
                    func.clone(),
                    "value",
                ))));
                Ok(Some(shape))
            }
            None => Ok(None),
        },
        // v0.51.4 gap-fix: `Aggregate` directly over a join (e.g. Nexmark
        // q4's `SELECT a.category, AVG(b.price) FROM auction a JOIN bid b
        // ON ... WHERE ... GROUP BY a.category`) — only the single group-by
        // column / single aggregate expression shape is supported here
        // (mirrors `compile_node`'s simplest `Aggregate` case); composite
        // keys or multiple aggregates over a join are not yet wired.
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => match try_compile_join_shape(input, table_schemas)? {
            Some(mut shape) => {
                if group_by.len() != 1 || aggregates.len() != 1 {
                    return Err(OpError::unsupported_plan_node(
                        "Aggregate over a join only supports a single group-by column \
                         and a single aggregate expression",
                    ));
                }
                let agg = &aggregates[0];
                shape
                    .post
                    .push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                        NamedExpr::new("k", group_by[0].clone()),
                        NamedExpr::new("v", agg.input.clone()),
                    ]))));
                let result_col = compile_aggregate_body(&mut shape.post, agg)?;
                shape
                    .post
                    .push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                        NamedExpr::new("k", Expr::Column(0)),
                        NamedExpr::new("agg", Expr::Column(result_col)),
                    ]))));
                shape.post_n_cols = 2;
                Ok(Some(shape))
            }
            None => Ok(None),
        },
        // v0.51.4 gap-fix: `Window` (sliding aggregates/`ROW_NUMBER`, etc.)
        // directly over a join (e.g. Nexmark q6's sliding `AVG(price) OVER
        // (PARTITION BY seller ORDER BY date_time ROWS ...)` over a join's
        // output) — mirrors `compile_node`'s `PlanNode::Window` arm.
        PlanNode::Window {
            input,
            window_exprs,
        } => match try_compile_join_shape(input, table_schemas)? {
            Some(mut shape) => {
                let op = Arc::new(WindowOp::new(
                    int64_schema(shape.post_n_cols + window_exprs.len()),
                    window_exprs.clone(),
                ));
                shape.post.push(Stage::Window(op, next_stateful_op_id()));
                shape.post_n_cols += window_exprs.len();
                Ok(Some(shape))
            }
            None => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Append the `[Distinct]`/`Aggregate` (or `MinMax`) portion of a
/// single-aggregate lane to `stages` (which must already end with a `(k,
/// v)`-shaped delta — see the `Aggregate` arm's `pre_named` `Project` and
/// `compile_multi_aggregate_lanes`'s per-lane `pre_named` `Project`).
/// Returns the output column that answers `agg` (`AggregateOp`'s
/// `sum_v`/`count`/`avg_v`, or `MinMaxOp`'s `extremum_v`).
fn compile_aggregate_body(
    stages: &mut Vec<Stage>,
    agg: &rockstream_plan::AggregateExpr,
) -> Result<usize, OpError> {
    // `MIN`/`MAX` use the dedicated, oracle-proven, retraction-safe
    // `MinMaxOp` (v0.6) instead of `AggregateOp` — an incremental extremum
    // needs an indexed multiset per group (removing the current min/max
    // under a retraction requires recomputing from the remaining values),
    // which `AggregateOp`'s fixed sum/count/avg accumulator doesn't do.
    if matches!(agg.func, AggregateFunc::Min | AggregateFunc::Max) {
        if agg.distinct {
            // MIN/MAX are already idempotent over duplicates (the extremum
            // of a multiset equals the extremum of its distinct values), so
            // `DISTINCT` changes nothing observable — no Distinct stage needed.
        }
        let kind = if agg.func == AggregateFunc::Min {
            crate::minmax::MinMaxKind::Min
        } else {
            crate::minmax::MinMaxKind::Max
        };
        let op_id = next_stateful_op_id();
        stages.push(Stage::MinMax(
            Arc::new(crate::minmax::MinMaxOp::new(op_id, kind)),
            op_id,
        ));
        return Ok(1); // MinMaxOp output: (k, extremum_v).
    }

    let result_col = match agg.func {
        AggregateFunc::Sum => 1,
        AggregateFunc::Count => 2,
        AggregateFunc::Avg => 3,
        other => {
            return Err(OpError::unsupported_plan_node(format!(
                "Aggregate function {other:?} (AggregateOp only computes sum/count/avg)"
            )));
        }
    };
    if agg.distinct {
        // COUNT(DISTINCT v) / SUM(DISTINCT v) GROUP BY k: dedupe (k, v)
        // pairs before aggregating, per Slice 5.
        let schema = int64_schema(2);
        stages.push(Stage::Distinct(
            Arc::new(DistinctOp::new(schema)),
            next_stateful_op_id(),
        ));
    }
    let agg_op_id = next_stateful_op_id();
    stages.push(Stage::Aggregate(Arc::new(AggregateOp::new(agg_op_id))));
    Ok(result_col)
}

/// v0.51.4 Slice 8: compile `aggregates.len() > 1` sharing one group-by key
/// (`group_by`, one or more Int64 expressions) into independent
/// single-aggregate lanes (each: a `Project` down to `(k, v_i)` or
/// `(k0..k(n-1), v_i)`, `[Distinct]`, `Aggregate` — the exact same shape
/// `compile_node`'s single-aggregate `Aggregate` arm builds), then
/// cascade-joins the lanes' `(k, agg_i)` outputs via `JoinOp` into one
/// `(k, agg_0, .., agg_{N-1})` row per group. `shared_stages` is the
/// (already-compiled) prefix common to every lane — e.g. a `TumbleWindow`
/// stage for Nexmark q15 — executed once upstream of the fan-out. When
/// `group_by.len() > 1`, every lane packs its composite key into one
/// surrogate Int64 via a single `GroupKeyPacker` *shared* across all lanes
/// (so the same real key tuple always interns to the same surrogate id
/// regardless of which lane observes it first) — the cascade join is on
/// that shared surrogate, unpacked back to `(k0..k(n-1), ...)` once after
/// the last join.
fn compile_multi_aggregate_lanes(
    shared_stages: Vec<Stage>,
    group_by: &[Expr],
    aggregates: &[rockstream_plan::AggregateExpr],
    in_schema: &SchemaRef,
) -> Result<(Vec<Stage>, SchemaRef), OpError> {
    let _ = in_schema; // already type-checked by the caller
    let n_keys = group_by.len();
    let packer = (n_keys > 1).then(|| Arc::new(GroupKeyPacker::new(n_keys)));
    let mut lanes: Vec<StatefulPipeline> = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        let mut pre_named: Vec<NamedExpr> = group_by
            .iter()
            .enumerate()
            .map(|(i, g)| NamedExpr::new(format!("k{i}"), g.clone()))
            .collect();
        pre_named.push(NamedExpr::new("v", agg.input.clone()));
        let mut lane_stages: Vec<Stage> =
            vec![Stage::Stateless(Arc::new(ProjectOp::new(pre_named)))];
        if let Some(packer) = &packer {
            lane_stages.push(Stage::KeyPack(packer.clone(), next_stateful_op_id()));
        }
        if agg.distinct {
            // `COUNT(DISTINCT CASE WHEN cond THEN col END)` with no `ELSE`
            // (Nexmark q15) lowers `col`'s "no branch matched" case to
            // `CASE_MISSING_ELSE_SENTINEL` rather than a real value (see
            // that constant's doc comment) — exclude those rows before
            // `Distinct`/`Aggregate` so they don't count as a (spurious)
            // distinct value, matching SQL's "CASE with no ELSE is NULL,
            // COUNT(DISTINCT ...) ignores NULL" semantics.
            lane_stages.push(Stage::Stateless(Arc::new(FilterOp::new(Expr::BinaryOp {
                op: rockstream_plan::BinaryOp::Ne,
                left: Box::new(Expr::Column(1)),
                right: Box::new(Expr::Literal(
                    rockstream_plan::CASE_MISSING_ELSE_SENTINEL
                        .to_be_bytes()
                        .to_vec(),
                )),
            }))));
        }
        let result_col = compile_aggregate_body(&mut lane_stages, agg)?;
        // AggregateOp always emits (k, sum_v, count, avg_v); project down to
        // this lane's own (k, agg) shape so every lane has an identical,
        // joinable 2-column output.
        lane_stages.push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
            NamedExpr::new("k", Expr::Column(0)),
            NamedExpr::new("agg", Expr::Column(result_col)),
        ]))));
        let mut pipeline = StatefulPipeline::new();
        for stage in lane_stages {
            pipeline = pipeline.push(stage);
        }
        lanes.push(pipeline);
    }

    // Cascade-join the N lanes' (k, agg_i) outputs: after joining i lanes,
    // the accumulator has (i + 1) columns (k, agg_0, .., agg_{i-1}); each
    // join adds one more lane's 2-column (k, agg_i) output on the right,
    // widening the accumulator by exactly 1 (the join's own duplicate right-
    // side key column is dropped by `MultiAggregatePipeline::process`).
    // A *left* outer join, not an inner join: a group with e.g. a nonzero
    // `SUM` but zero rows for a later `COUNT(DISTINCT CASE WHEN ...)` lane
    // must still appear with that aggregate reported as `0`, matching SQL's
    // `GROUP BY` semantics — see `MultiAggregatePipeline`'s doc comment.
    let mut joins = Vec::with_capacity(aggregates.len().saturating_sub(1));
    // `acc_n_cols` tracks the running accumulator's column count (not just
    // loop position), needed to construct each `OuterJoinOp` with the
    // correct left-side width — not a plain `enumerate()` counter.
    let mut acc_n_cols = 2usize;
    #[allow(clippy::explicit_counter_loop)]
    for _ in 1..aggregates.len() {
        let join_op_id = next_stateful_op_id();
        joins.push(Arc::new(OuterJoinOp::with_schema(
            join_op_id,
            rockstream_plan::OuterJoinKind::Left,
            vec![0],
            vec![0],
            acc_n_cols,
            2,
        )));
        acc_n_cols += 1;
    }

    let mut stages = shared_stages;
    stages.push(Stage::MultiAggregate(Arc::new(
        crate::live_exec::MultiAggregatePipeline::new(lanes, joins),
    )));
    if let Some(packer) = packer {
        // Unpack the shared surrogate key back to (k0..k(n-1), agg_0..agg_{N-1}).
        stages.push(Stage::KeyUnpack(packer, next_stateful_op_id()));
        Ok((stages, int64_schema(n_keys + aggregates.len())))
    } else {
        Ok((stages, int64_schema(aggregates.len() + 1)))
    }
}

/// Recursively compile `node` (everything under `ViewSink`) into an ordered
/// list of `Stage`s, returning `(stages, output_schema)`.
fn compile_node(
    node: &PlanNode,
    source_schema: &SchemaRef,
) -> Result<(Vec<Stage>, SchemaRef), OpError> {
    match node {
        PlanNode::Source { .. } => {
            // Source rows arrive as deltas from the table's commit path; no
            // operator is needed to represent them in the pipeline.
            Ok((Vec::new(), source_schema.clone()))
        }
        PlanNode::Filter { input, predicate } => {
            let (mut stages, schema) = compile_node(input, source_schema)?;
            stages.push(Stage::Stateless(Arc::new(FilterOp::new(predicate.clone()))));
            Ok((stages, schema))
        }
        PlanNode::Project { input, columns } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            let named: Vec<NamedExpr> = columns
                .iter()
                .enumerate()
                .map(|(i, expr)| NamedExpr::new(format!("col{i}"), expr.clone()))
                .collect();
            // Best-effort output schema: a plain column reference keeps its
            // source field's real type; anything else defaults to `Int64`
            // (matches this codebase's existing arithmetic-expression
            // convention — only consulted by a stateful operator gate
            // further up the tree, never by `ProjectOp` itself).
            let out_fields: Vec<Field> = columns
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    let dt = static_expr_type(expr, &in_schema).unwrap_or(DataType::Int64);
                    Field::new(format!("col{i}"), dt, true)
                })
                .collect();
            let out_schema: SchemaRef = Arc::new(Schema::new(out_fields));
            stages.push(Stage::Stateless(Arc::new(ProjectOp::new(named))));
            Ok((stages, out_schema))
        }
        PlanNode::Map { input, func } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            stages.push(Stage::Stateless(Arc::new(MapOp::new(
                func.clone(),
                "value",
            ))));
            let mut fields: Vec<Field> = in_schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(Field::new("value", DataType::Int64, true));
            Ok((stages, Arc::new(Schema::new(fields))))
        }

        // ── v0.51.4 Slice 2: TumbleWindow / HopWindow ──────────────────────
        PlanNode::TumbleWindow {
            input,
            time_col,
            window_size_ms,
            late_data_policy,
        } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            // A `Utf8` passthrough column (e.g. Nexmark q16/q17's `channel`,
            // not the time column, not part of any arithmetic) is packed
            // into an `Int64` surrogate — `TumbleWindowOp` itself only ever
            // sees `Int64` columns, same as `JoinOp`'s side inputs.
            let (packed_schema, utf8_packers) = pack_utf8_columns(&mut stages, &in_schema);
            if !schema_all_int64(&packed_schema) {
                return Err(OpError::unsupported_plan_node(
                    "TumbleWindow over a non-Int64/Utf8 input schema (TumbleWindowOp only supports Int64 columns, plus Utf8 passthrough)",
                ));
            }
            let op = Arc::new(TumbleWindowOp::new(
                packed_schema.clone(),
                *time_col,
                *window_size_ms,
                late_data_policy.clone(),
            ));
            stages.push(Stage::TumbleWindow(op, next_stateful_op_id()));
            // `window_id` is prepended at column 0, shifting every packed
            // column's index by 1.
            unpack_utf8_columns(&mut stages, 1, utf8_packers);
            let out_schema = TumbleWindowOp::output_schema(&packed_schema);
            let out_fields: Vec<Field> = out_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i == 0 {
                        f.as_ref().clone()
                    } else {
                        in_schema.field(i - 1).clone()
                    }
                })
                .collect();
            Ok((stages, Arc::new(Schema::new(out_fields))))
        }
        PlanNode::HopWindow {
            input,
            time_col,
            window_size_ms,
            slide_ms,
            late_data_policy,
        } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            let (packed_schema, utf8_packers) = pack_utf8_columns(&mut stages, &in_schema);
            if !schema_all_int64(&packed_schema) {
                return Err(OpError::unsupported_plan_node(
                    "HopWindow over a non-Int64/Utf8 input schema (HopWindowOp only supports Int64 columns, plus Utf8 passthrough)",
                ));
            }
            let op = Arc::new(HopWindowOp::new(
                packed_schema.clone(),
                *time_col,
                *window_size_ms,
                *slide_ms,
                late_data_policy.clone(),
            ));
            stages.push(Stage::HopWindow(op, next_stateful_op_id()));
            unpack_utf8_columns(&mut stages, 1, utf8_packers);
            let out_schema = HopWindowOp::output_schema(&packed_schema);
            let out_fields: Vec<Field> = out_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i == 0 {
                        f.as_ref().clone()
                    } else {
                        in_schema.field(i - 1).clone()
                    }
                })
                .collect();
            Ok((stages, Arc::new(Schema::new(out_fields))))
        }

        // ── v0.51.4 Slice 6: SessionWindow (data-dependent, gap-delimited
        //    event-time sessions — e.g. Nexmark q11's
        //    `GROUP BY bidder, SESSION(date_time, INTERVAL '10 seconds')`) ─
        PlanNode::SessionWindow {
            input,
            time_col,
            gap_ms,
            late_data_policy,
        } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            if !schema_all_int64(&in_schema) {
                return Err(OpError::unsupported_plan_node(
                    "SessionWindow over a non-Int64-only input schema (SessionWindowOp only supports Int64 columns)",
                ));
            }
            let op = Arc::new(SessionWindowOp::new(
                in_schema.clone(),
                *time_col,
                *gap_ms,
                late_data_policy.clone(),
            ));
            stages.push(Stage::SessionWindow(op, next_stateful_op_id()));
            Ok((stages, SessionWindowOp::output_schema(&in_schema)))
        }

        // ── v0.51.4 Slice 4: Window (ROW_NUMBER/sliding aggregates) / TopK ─
        PlanNode::Window {
            input,
            window_exprs,
        } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            if !schema_all_int64(&in_schema) {
                return Err(OpError::unsupported_plan_node(
                    "Window over a non-Int64-only input schema (WindowOp only supports Int64 columns)",
                ));
            }
            let out_n = in_schema.fields().len() + window_exprs.len();
            let schema = int64_schema(out_n);
            let op = Arc::new(WindowOp::new(schema.clone(), window_exprs.clone()));
            stages.push(Stage::Window(op, next_stateful_op_id()));
            Ok((stages, schema))
        }
        PlanNode::TopK {
            input,
            k,
            rank_col,
            partition_by,
        } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            if !schema_all_int64(&in_schema) {
                return Err(OpError::unsupported_plan_node(
                    "TopK over a non-Int64-only input schema (TopKOp only supports Int64 columns)",
                ));
            }
            let op = Arc::new(TopKOp::new(
                in_schema.clone(),
                *k,
                *rank_col,
                partition_by.clone(),
            ));
            stages.push(Stage::TopK(op, next_stateful_op_id()));
            Ok((stages, in_schema))
        }

        // ── v0.51.4 Slice 5: Distinct (bare `SELECT DISTINCT`) ─────────────
        PlanNode::Distinct { input, arr_id } => {
            let (mut stages, in_schema) = compile_node(input, source_schema)?;
            if !schema_all_int64(&in_schema) {
                return Err(OpError::unsupported_plan_node(
                    "Distinct over a non-Int64-only input schema (DistinctOp only supports Int64 columns)",
                ));
            }
            stages.push(Stage::Distinct(
                Arc::new(DistinctOp::new(in_schema.clone())),
                *arr_id,
            ));
            Ok((stages, in_schema))
        }

        // ── v0.51.4 Slice 1 (+ Slice 5/6/8 composition): Aggregate ─────────
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            if aggregates.is_empty() {
                return Err(OpError::unsupported_plan_node(format!(
                    "Aggregate with {} group-by column(s) and {} aggregate expression(s) \
                     (compile_plan requires at least one aggregate expression)",
                    group_by.len(),
                    aggregates.len()
                )));
            }
            // GroupKeyPacker's composite-key packing (`KeyPacking::Composite`
            // below) is a generic `(k0, k1, ..) -> surrogate Int64` scheme —
            // it doesn't care what produces the group-by columns, so it is
            // safe for `SessionWindow`'s `GROUP BY partition_cols...,
            // session_start, session_end` shape (Nexmark q11) *and* for a
            // plain multi-column `GROUP BY` with no window operator at all
            // (e.g. Nexmark q12/q16/q17's `GROUP BY bidder, date_bin(...)`,
            // where `date_bin(...)` is just an ordinary per-row Int64
            // expression, not a `TumbleWindowOp`/`HopWindowOp` state
            // machine). `TumbleWindow`'s `date_bin`/timestamp-precision gap
            // (see `interval_ms_to_raw_units` in `rockstream-sql/src/lower.rs`)
            // is now fixed at the lowering layer, so a `TumbleWindow` input
            // is safe here too — `HopWindow`'s date_bin lowering path
            // (`try_lower_hop_window_aggregate`) was not touched by that fix
            // and remains rejected.
            if group_by.len() > 1 && matches!(input.as_ref(), PlanNode::HopWindow { .. }) {
                return Err(OpError::unsupported_plan_node(format!(
                    "Aggregate with {} group-by columns over a HopWindow input \
                     (composite-key packing over a HopWindow input has a known \
                     date_bin/timestamp-precision correctness gap, unlike TumbleWindow)",
                    group_by.len()
                )));
            }
            // v0.51.4 Slice 8: multiple aggregates sharing one group-by key
            // (e.g. Nexmark q15's `SUM(...)`, `COUNT(DISTINCT ...)` x2, all
            // `GROUP BY date_bin(...)`) — each aggregate is compiled as an
            // independent single-aggregate "lane" (mirroring the single-
            // aggregate path below), then the lanes' `(k, agg_i)` outputs are
            // cascade-joined back into one `(k, agg_0, .., agg_{N-1})` row via
            // `JoinOp` (reusing the same pre-existing, oracle-proven equi-join
            // machinery Slice 3 wires for `InnerJoin`/`OuterJoin` PlanNodes,
            // rather than inventing a new "zip by key" primitive). A
            // composite (multi-column) group-by key (e.g. Nexmark q16/q17's
            // `GROUP BY channel, date_bin(...)`) is packed into one surrogate
            // Int64 via a `GroupKeyPacker` *shared* across every lane (so the
            // same real key tuple always interns to the same surrogate id
            // regardless of which lane observes it first), joined on the
            // surrogate, then unpacked once after the cascade join —
            // `compile_multi_aggregate_lanes` handles both cases. No restart
            // persistence is wired for this shared packer (same as the
            // pre-existing single-key multi-aggregate lanes above, which
            // also have no Durability Slice coverage this version).
            if aggregates.len() > 1 {
                // The `TumbleWindowOp`/`date_bin` timestamp-precision gap
                // that used to make this collapse into one window is fixed
                // at the lowering layer (see comment above); `HopWindow`'s
                // separate date_bin lowering path was not touched by that
                // fix and remains rejected here.
                if matches!(input.as_ref(), PlanNode::HopWindow { .. }) {
                    return Err(OpError::unsupported_plan_node(
                        "multi-aggregate composition (Slice 8) over a HopWindow input \
                         is not yet supported (pre-existing date_bin/timestamp-precision \
                         gap in that lowering path, unrelated to multi-aggregate composition)",
                    ));
                }
                let (mut stages, in_schema) = compile_node(input, source_schema)?;
                // A `Utf8` group-by column (e.g. Nexmark q16's `GROUP BY
                // channel, date_bin(...)`) is packed into an `Int64`
                // surrogate via its own `Utf8ColumnPacker` — only supported
                // when it's a bare column reference (the packer operates on
                // a column index, not an arbitrary expression's runtime
                // value). Tracked so the corresponding *output* key column
                // (group-by columns come first, in order, in
                // `compile_multi_aggregate_lanes`'s output) can be unpacked
                // back to `Utf8` afterward.
                let mut utf8_key_unpacks: Vec<(usize, Arc<Utf8ColumnPacker>)> = Vec::new();
                for (i, g) in group_by.iter().enumerate() {
                    if expr_is_int64(g, &in_schema) {
                        continue;
                    }
                    let Expr::Column(col_idx) = g else {
                        return Err(OpError::unsupported_plan_node(
                            "multi-aggregate composition (Slice 8): a non-Int64 group-by \
                             expression must be a bare Utf8 column reference",
                        ));
                    };
                    if in_schema.field(*col_idx).data_type() != &DataType::Utf8 {
                        return Err(OpError::unsupported_plan_node(
                            "Aggregate over a non-Int64/Utf8 group-by expression \
                             (AggregateOp only supports Int64 keys/values, plus Utf8 group-by)",
                        ));
                    }
                    let packer = Arc::new(Utf8ColumnPacker::new());
                    stages.push(Stage::Utf8ColumnPack(
                        packer.clone(),
                        *col_idx,
                        next_stateful_op_id(),
                    ));
                    utf8_key_unpacks.push((i, packer));
                }
                if !aggregates
                    .iter()
                    .all(|a| expr_is_int64(&a.input, &in_schema))
                {
                    return Err(OpError::unsupported_plan_node(
                        "Aggregate over a non-Int64 aggregate-value expression \
                         (AggregateOp only supports Int64 values)",
                    ));
                }
                let (mut result_stages, result_schema) =
                    compile_multi_aggregate_lanes(stages, group_by, aggregates, &in_schema)?;
                if utf8_key_unpacks.is_empty() {
                    return Ok((result_stages, result_schema));
                }
                let mut out_fields: Vec<Field> = result_schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect();
                for (pos, packer) in utf8_key_unpacks {
                    result_stages.push(Stage::Utf8ColumnUnpack(packer, pos, next_stateful_op_id()));
                    out_fields[pos] = Field::new(out_fields[pos].name(), DataType::Utf8, false);
                }
                return Ok((result_stages, Arc::new(Schema::new(out_fields))));
            }

            let agg = &aggregates[0];
            let (mut stages, in_schema) = compile_node(input, source_schema)?;

            // v0.51.4 Slice 8: a single, non-Int64 GROUP BY column is
            // supported when it's `Utf8` (e.g. `GROUP BY url` where `url`
            // is `TEXT`), via `Utf8KeyPacker` (below) — everything else
            // (composite keys mixing Utf8/Int64, `Boolean`/`Float64` keys)
            // remains rejected, same as before this addition.
            let single_utf8_key = group_by.len() == 1 && expr_is_utf8(&group_by[0], &in_schema);

            // AggregateOp only supports Int64 group keys and Int64 values —
            // reject rather than silently truncate a non-Int64 column to 0.
            if !single_utf8_key
                && (!group_by.iter().all(|g| expr_is_int64(g, &in_schema))
                    || !expr_is_int64(&agg.input, &in_schema))
            {
                return Err(OpError::unsupported_plan_node(
                    "Aggregate over a non-Int64 group-by or aggregate-value expression \
                     (AggregateOp only supports Int64 keys/values)",
                ));
            }
            if single_utf8_key && !expr_is_int64(&agg.input, &in_schema) {
                return Err(OpError::unsupported_plan_node(
                    "Aggregate over a non-Int64 aggregate-value expression \
                     (AggregateOp only supports Int64 values)",
                ));
            }

            // v0.51.4 Slice 8: a global aggregate with no GROUP BY at all
            // (e.g. `SELECT SUM(balance) FROM accounts`) is compiled as a
            // single-group aggregate keyed by a synthetic constant `0`
            // column — `AggregateOp` always needs a `(k, v)` input pair.
            // Unlike the real-group-by case, the synthetic key is dropped
            // from the output entirely (not just internally renamed): the
            // frontend's lowering (`rockstream-sql/src/lower.rs`) numbers a
            // no-GROUP-BY aggregate's output columns starting at 0 for the
            // *aggregate* value (no leading key column, since DataFusion's
            // own `group_expr`-then-`aggr_expr` schema convention has zero
            // `group_expr` columns here) — so the wrapping `Project`/
            // `ViewSink` expects column 0 to be the aggregate result, not a
            // key.
            let has_real_group_by = !group_by.is_empty();
            let effective_group_by: Vec<Expr> = if has_real_group_by {
                group_by.clone()
            } else {
                vec![lit(0)]
            };
            let n_keys = effective_group_by.len();

            // Project (arbitrary) input rows down to the fixed (k0..k(n-1), v)
            // shape AggregateOp (via GroupKeyPacker, when n_keys > 1) requires.
            let mut pre_named: Vec<NamedExpr> = effective_group_by
                .iter()
                .enumerate()
                .map(|(i, g)| NamedExpr::new(format!("k{i}"), g.clone()))
                .collect();
            pre_named.push(NamedExpr::new("v", agg.input.clone()));
            stages.push(Stage::Stateless(Arc::new(ProjectOp::new(pre_named))));

            // v0.51.4 Slice 6: a composite (multi-column) group-by key is
            // packed into AggregateOp's single-Int64-key shape via
            // `GroupKeyPacker` — e.g. Nexmark q11's `GROUP BY bidder,
            // SESSION(...)`, lowered to `GROUP BY bidder, session_start,
            // session_end` — then unpacked back afterward. v0.51.4 Slice 8:
            // a single `Utf8` key goes through `Utf8KeyPacker` instead.
            enum KeyPacking {
                None,
                Composite(Arc<GroupKeyPacker>),
                Utf8(Arc<Utf8KeyPacker>),
            }
            let packer = if single_utf8_key {
                let packer = Arc::new(Utf8KeyPacker::new());
                stages.push(Stage::Utf8KeyPack(packer.clone(), next_stateful_op_id()));
                KeyPacking::Utf8(packer)
            } else if n_keys > 1 {
                let packer = Arc::new(GroupKeyPacker::new(n_keys));
                stages.push(Stage::KeyPack(packer.clone(), next_stateful_op_id()));
                KeyPacking::Composite(packer)
            } else {
                KeyPacking::None
            };

            let result_col = compile_aggregate_body(&mut stages, agg)?;

            match packer {
                KeyPacking::Composite(packer) => {
                    // Unpack the surrogate key back to
                    // (k0..k(n-1), sum, count, avg), then project down to
                    // the group-by columns plus the one aggregate result the
                    // SQL surface asked for.
                    stages.push(Stage::KeyUnpack(packer, next_stateful_op_id()));
                    let mut post_named: Vec<NamedExpr> = (0..n_keys)
                        .map(|i| NamedExpr::new(format!("k{i}"), Expr::Column(i)))
                        .collect();
                    post_named.push(NamedExpr::new("agg", Expr::Column(n_keys + result_col - 1)));
                    stages.push(Stage::Stateless(Arc::new(ProjectOp::new(post_named))));
                    Ok((stages, int64_schema(n_keys + 1)))
                }
                KeyPacking::Utf8(packer) => {
                    // Unpack the surrogate key back to the original Utf8
                    // value, then project down to (k: Utf8, agg: Int64).
                    stages.push(Stage::Utf8KeyUnpack(packer, next_stateful_op_id()));
                    stages.push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                        NamedExpr::new("k", Expr::Column(0)),
                        NamedExpr::new("agg", Expr::Column(result_col)),
                    ]))));
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("k", DataType::Utf8, false),
                        Field::new("agg", DataType::Int64, false),
                    ]));
                    Ok((stages, schema))
                }
                KeyPacking::None if has_real_group_by => {
                    // AggregateOp always emits (k, sum_v, count, avg_v);
                    // project down to the two columns the SQL surface
                    // actually asked for.
                    stages.push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                        NamedExpr::new("k", Expr::Column(0)),
                        NamedExpr::new("agg", Expr::Column(result_col)),
                    ]))));
                    Ok((stages, int64_schema(2)))
                }
                KeyPacking::None => {
                    // No real GROUP BY: drop the synthetic key entirely —
                    // the frontend expects column 0 to be the aggregate
                    // result itself, not a key (see comment above).
                    stages.push(Stage::Stateless(Arc::new(ProjectOp::new(vec![
                        NamedExpr::new("agg", Expr::Column(result_col)),
                    ]))));
                    Ok((stages, int64_schema(1)))
                }
            }
        }

        other => Err(OpError::unsupported_plan_node(plan_node_kind(other))),
    }
}

/// Human-readable name of a `PlanNode` variant, used in error messages.
fn plan_node_kind(node: &PlanNode) -> String {
    match node {
        PlanNode::Source { .. } => "Source",
        PlanNode::Filter { .. } => "Filter",
        PlanNode::Project { .. } => "Project",
        PlanNode::Map { .. } => "Map",
        PlanNode::Aggregate { .. } => "Aggregate",
        PlanNode::Join { .. } => "Join",
        PlanNode::InnerJoin { .. } => "InnerJoin",
        PlanNode::OuterJoin { .. } => "OuterJoin",
        PlanNode::TumbleWindow { .. } => "TumbleWindow",
        PlanNode::HopWindow { .. } => "HopWindow",
        PlanNode::SessionWindow { .. } => "SessionWindow",
        PlanNode::Window { .. } => "Window",
        PlanNode::TopK { .. } => "TopK",
        PlanNode::Distinct { .. } => "Distinct",
        PlanNode::ViewSink { .. } => "ViewSink",
        PlanNode::Exchange { .. } => "Exchange",
        _ => "Unsupported",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::zset::ArrowZSet;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use object_store::local::LocalFileSystem;
    use rockstream_plan::{BinaryOp, Expr};
    use tempfile::TempDir;

    async fn make_db() -> (TempDir, Arc<ShardDb>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
        (dir, db)
    }

    fn make_row_batch(ids: &[i64], names: &[&str]) -> ArrowZSet {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap();
        ArrowZSet::new(data, vec![1; ids.len()])
    }

    #[tokio::test]
    async fn rejects_non_view_sink_root() {
        let plan = PlanNode::Source {
            name: "t".to_string(),
        };
        let (_dir, db) = make_db().await;
        let result = compile_plan(&plan, db, &HashMap::new());
        let err = match result {
            Ok(_) => panic!("expected UnsupportedPlanNode error"),
            Err(e) => e,
        };
        assert!(matches!(err, OpError::UnsupportedPlanNode { .. }));
        assert!(err.to_string().contains("RS-1013"));
    }

    #[tokio::test]
    async fn compiles_source_filter_project_view_sink() {
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::Project {
                input: Box::new(PlanNode::Filter {
                    input: Box::new(PlanNode::Source {
                        name: "t".to_string(),
                    }),
                    predicate: Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        left: Box::new(Expr::Column(0)),
                        right: Box::new(lit(1)),
                    },
                }),
                columns: vec![Expr::Column(0), Expr::Column(1)],
            }),
        };

        let compiled = compile_plan(&plan, db.clone(), &HashMap::new()).unwrap();
        assert_eq!(compiled.view_name, "v");
        assert_eq!(compiled.pk, vec![0]);

        let batch = make_row_batch(&[1, 2, 3], &["a", "b", "c"]);
        let out = compiled.pipeline.process(batch).unwrap();
        // id=1 filtered out (1 > 1 is false); rows 2,3 survive.
        assert_eq!(out.num_rows(), 2);

        let epoch = compiled.sink.write_next_epoch(&out).await.unwrap();
        let rows = crate::sink::read_view_output(&db, compiled.sink_op_id, 2)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(e, _, _, _)| *e == epoch));
    }

    #[tokio::test]
    async fn unsupported_child_node_reports_kind() {
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0), Expr::Column(1)],
                aggregates: vec![],
            }),
        };
        let result = compile_plan(&plan, db, &HashMap::new());
        let err = match result {
            Ok(_) => panic!("expected UnsupportedPlanNode error"),
            Err(e) => e,
        };
        match err {
            OpError::UnsupportedPlanNode { kind, .. } => assert!(
                kind.contains("Aggregate"),
                "expected kind to mention Aggregate, got {kind:?}"
            ),
            other => panic!("expected UnsupportedPlanNode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compiles_aggregate_sum_group_by() {
        use rockstream_plan::{AggregateExpr, AggregateFunc};
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Sum,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            }),
        };
        let mut table_schemas = HashMap::new();
        table_schemas.insert("t".to_string(), int64_schema(2));
        let compiled = compile_plan(&plan, db, &table_schemas).unwrap();
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (1, 20), (2, 5)], 1);
        let out = compiled.pipeline.process(batch).unwrap();
        // k=1 has two distinct (k,v) pairs in this single batch, each
        // processed sequentially by AggregateOp's per-(k,v) consolidation:
        // the first (1,10) emits 1 insert row; the second (1,20) emits a
        // retract-old + insert-new pair (2 rows) since group k=1 already
        // existed after the first. k=2 (new group) emits 1 insert row.
        // Total: 1 + 2 + 1 = 4.
        assert_eq!(out.num_rows(), 4);
    }

    #[tokio::test]
    async fn compiles_inner_join() {
        use rockstream_plan::JoinSemantics;
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::InnerJoin {
                left: Box::new(PlanNode::Source {
                    name: "auction".to_string(),
                }),
                right: Box::new(PlanNode::Source {
                    name: "bid".to_string(),
                }),
                left_keys: vec![0],
                right_keys: vec![0],
                left_arr_id: OperatorId(1001),
                right_arr_id: OperatorId(1002),
                semantics: JoinSemantics::default(),
            }),
        };
        let mut table_schemas = HashMap::new();
        table_schemas.insert("auction".to_string(), int64_schema(2));
        table_schemas.insert("bid".to_string(), int64_schema(2));
        let compiled = compile_plan(&plan, db, &table_schemas).unwrap();
        let join = compiled
            .join
            .expect("InnerJoin should compile to JoinCompiled");

        // left: (id=1, v=100); right: (auction=1, price=5) joins on col0=1.
        let left = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
        let right = ArrowZSet::from_ab_rows(&[(1, 5)], 1);
        let out = join
            .pipeline
            .process(left, ArrowZSet::empty(int64_schema(2)))
            .unwrap();
        assert_eq!(out.num_rows(), 0, "no match yet: right side is still empty");

        let out2 = join
            .pipeline
            .process(ArrowZSet::empty(int64_schema(2)), right)
            .unwrap();
        assert_eq!(
            out2.num_rows(),
            1,
            "right delta should join with the already-staged left row"
        );
    }
}
