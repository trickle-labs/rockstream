//! Fused stateless execution kernel (`Filter → Project → Map`) (Issue #402 R3).
//!
//! Collapses adjacent stateless operators into one executable kernel that:
//! - Evaluates consecutive filters with a single selection pass and one `take`.
//! - Evaluates `Filter → Project → Map` without allocating intermediate `ArrowZSet`s.
//! - Composes pure column-projection chains directly when semantically safe.
//! - Preserves, slices, and column-remaps attached `BatchKeyCapsule`s with zero re-encoding,
//!   and optionally pre-encodes a canonical `BatchKeyCapsule` once for downstream consumers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_plan::{BinaryOp, Expr};
use rockstream_types::BatchKeyCapsule;

use crate::error::OpError;
use crate::expr::{eval_bool, eval_to_array};
use crate::op::Operator;
use crate::project::{remap_capsules_through_project, NamedExpr};
use crate::zset::ArrowZSet;

/// One logical stateless step compiled into a [`FusedStatelessKernel`].
#[derive(Debug, Clone)]
pub enum StatelessStep {
    Filter(Expr),
    Project(Vec<NamedExpr>),
    Map { expr: Expr, output_name: String },
}

/// Internal physical fused instruction produced by lowering adjacent [`StatelessStep`]s.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum FusedOp {
    /// One or more consecutive filters collapsed into one selection pass.
    FilterChain(Vec<Expr>),
    /// `Filter* -> Project -> Map*` executed in one pass with zero intermediate `ArrowZSet`.
    FilterProject {
        predicates: Vec<Expr>,
        exprs: Vec<NamedExpr>,
        maps: Vec<(Expr, String)>,
    },
    /// `Filter* -> Map+` executed in one pass with zero intermediate `ArrowZSet`.
    FilterMap {
        predicates: Vec<Expr>,
        maps: Vec<(Expr, String)>,
    },
}

/// A compiled stateless kernel fusing adjacent `Filter`, `Project`, and `Map` stages.
#[derive(Debug)]
pub struct FusedStatelessKernel {
    steps: Vec<StatelessStep>,
    ops: Vec<FusedOp>,
    prepack_key_cols: Option<Vec<usize>>,
    batches_processed: AtomicU64,
    intermediate_batches_elided: AtomicU64,
}

impl FusedStatelessKernel {
    /// Compile a sequence of adjacent stateless steps into a fused kernel.
    pub fn compile(steps: Vec<StatelessStep>) -> Self {
        let ops = lower_steps(&steps);
        Self {
            steps,
            ops,
            prepack_key_cols: None,
            batches_processed: AtomicU64::new(0),
            intermediate_batches_elided: AtomicU64::new(0),
        }
    }

    /// Configure an output `KeySpec` to encode once per batch at the end of the fused kernel
    /// if no compatible `BatchKeyCapsule` is already attached.
    pub fn with_prepack_key_cols(mut self, key_cols: Vec<usize>) -> Self {
        self.prepack_key_cols = Some(key_cols);
        self
    }

    /// Logical stateless steps fused inside this kernel.
    pub fn steps(&self) -> &[StatelessStep] {
        &self.steps
    }

    /// Optional pre-packed key columns attached at the end of this kernel.
    pub fn prepack_key_cols(&self) -> Option<&[usize]> {
        self.prepack_key_cols.as_deref()
    }

    /// Number of original logical stateless stages fused into this kernel.
    pub fn fused_stage_count(&self) -> usize {
        self.steps.len()
    }

    /// Number of lowered physical fused ops (<= `fused_stage_count`).
    pub fn physical_op_count(&self) -> usize {
        self.ops.len()
    }

    /// Total batches executed through this kernel.
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed.load(Ordering::Relaxed)
    }

    /// Total intermediate `ArrowZSet` allocations eliminated by fusion across processed batches.
    pub fn intermediate_batches_elided(&self) -> u64 {
        self.intermediate_batches_elided.load(Ordering::Relaxed)
    }

    /// Execute the fused kernel on one delta batch.
    pub fn apply(&self, input: ArrowZSet) -> Result<ArrowZSet, OpError> {
        input.validate()?;
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        let elided = self.steps.len().saturating_sub(1) as u64;
        self.intermediate_batches_elided
            .fetch_add(elided, Ordering::Relaxed);

        let mut data = input.data;
        let mut weights = input.weights;
        let frontier = input.frontier;
        let mut capsules = input.key_capsules;

        for op in &self.ops {
            match op {
                FusedOp::FilterChain(predicates) => {
                    if data.num_rows() > 0 {
                        let (next_data, next_weights, next_capsules) =
                            apply_filter_chain(data, weights, capsules, predicates)?;
                        data = next_data;
                        weights = next_weights;
                        capsules = next_capsules;
                    }
                }
                FusedOp::FilterProject {
                    predicates,
                    exprs,
                    maps,
                } => {
                    if data.num_rows() > 0 && !predicates.is_empty() {
                        let (next_data, next_weights, next_capsules) =
                            apply_filter_chain(data, weights, capsules, predicates)?;
                        data = next_data;
                        weights = next_weights;
                        capsules = next_capsules;
                    }
                    let (mut next_data, mut next_capsules) = eval_project(&data, &capsules, exprs)?;
                    for (expr, output_name) in maps {
                        let (mapped_data, mapped_capsules) =
                            eval_map(&next_data, &next_capsules, expr, output_name)?;
                        next_data = mapped_data;
                        next_capsules = mapped_capsules;
                    }
                    data = next_data;
                    capsules = next_capsules;
                }
                FusedOp::FilterMap { predicates, maps } => {
                    if data.num_rows() > 0 && !predicates.is_empty() {
                        let (next_data, next_weights, next_capsules) =
                            apply_filter_chain(data, weights, capsules, predicates)?;
                        data = next_data;
                        weights = next_weights;
                        capsules = next_capsules;
                    }
                    for (expr, output_name) in maps {
                        let (next_data, next_capsules) =
                            eval_map(&data, &capsules, expr, output_name)?;
                        data = next_data;
                        capsules = next_capsules;
                    }
                }
            }
        }

        let mut out = ArrowZSet::try_new(data, weights)?;
        out.frontier = frontier;
        out.key_capsules = capsules;
        if !out.is_empty() {
            if let Some(ref key_cols) = self.prepack_key_cols {
                let _ = out.attach_key_capsule(key_cols)?;
            }
        }
        Ok(out)
    }
}

impl Operator for FusedStatelessKernel {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.apply(delta)
    }

    fn name(&self) -> &str {
        "FusedStatelessKernel"
    }
}

/// Lower logical stateless steps into fused physical ops, composing safe pure
/// projections directly into subsequent `Project` or `Map` steps.
fn lower_steps(steps: &[StatelessStep]) -> Vec<FusedOp> {
    let mut ops: Vec<FusedOp> = Vec::new();
    let mut pending_filters: Vec<Expr> = Vec::new();

    for step in steps {
        match step {
            StatelessStep::Filter(pred) => {
                pending_filters.push(pred.clone());
            }
            StatelessStep::Project(exprs) => {
                let preds = std::mem::take(&mut pending_filters);
                // If previous op was a `FilterProject` with no trailing maps and no intervening
                // filters, compose `Project_2(Project_1)` directly when safe.
                if preds.is_empty() {
                    if let Some(FusedOp::FilterProject {
                        exprs: prev_exprs,
                        maps,
                        ..
                    }) = ops.last_mut()
                    {
                        if maps.is_empty() {
                            if let Some(composed) = try_compose_projects(prev_exprs, exprs) {
                                *prev_exprs = composed;
                                continue;
                            }
                        }
                    }
                }
                ops.push(FusedOp::FilterProject {
                    predicates: preds,
                    exprs: exprs.clone(),
                    maps: Vec::new(),
                });
            }
            StatelessStep::Map { expr, output_name } => {
                let preds = std::mem::take(&mut pending_filters);
                if preds.is_empty() {
                    match ops.last_mut() {
                        Some(FusedOp::FilterProject { maps, .. }) => {
                            maps.push((expr.clone(), output_name.clone()));
                            continue;
                        }
                        Some(FusedOp::FilterMap { maps, .. }) => {
                            maps.push((expr.clone(), output_name.clone()));
                            continue;
                        }
                        _ => {}
                    }
                }
                ops.push(FusedOp::FilterMap {
                    predicates: preds,
                    maps: vec![(expr.clone(), output_name.clone())],
                });
            }
        }
    }

    if !pending_filters.is_empty() {
        ops.push(FusedOp::FilterChain(pending_filters));
    }
    ops
}

fn is_infallible_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_))
}

fn substitute_columns(expr: &Expr, prev_exprs: &[NamedExpr]) -> Option<Expr> {
    Some(match expr {
        Expr::Column(i) => prev_exprs.get(*i)?.expr.clone(),
        Expr::Literal(bytes) => Expr::Literal(bytes.clone()),
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: *op,
            left: Box::new(substitute_columns(left, prev_exprs)?),
            right: Box::new(substitute_columns(right, prev_exprs)?),
        },
        Expr::ScalarUdf { name, args } => Expr::ScalarUdf {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_columns(a, prev_exprs))
                .collect::<Option<Vec<_>>>()?,
        },
        Expr::Case {
            when_then,
            else_expr,
        } => Expr::Case {
            when_then: when_then
                .iter()
                .map(|(w, t)| {
                    Some((
                        substitute_columns(w, prev_exprs)?,
                        substitute_columns(t, prev_exprs)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
            else_expr: Box::new(substitute_columns(else_expr, prev_exprs)?),
        },
    })
}

fn count_column_refs(expr: &Expr, counts: &mut [usize]) {
    match expr {
        Expr::Column(i) => {
            if let Some(slot) = counts.get_mut(*i) {
                *slot += 1;
            }
        }
        Expr::Literal(_) => {}
        Expr::BinaryOp { left, right, .. } => {
            count_column_refs(left, counts);
            count_column_refs(right, counts);
        }
        Expr::ScalarUdf { args, .. } => {
            for a in args {
                count_column_refs(a, counts);
            }
        }
        Expr::Case {
            when_then,
            else_expr,
        } => {
            for (w, t) in when_then {
                count_column_refs(w, counts);
                count_column_refs(t, counts);
            }
            count_column_refs(else_expr, counts);
        }
    }
}

fn expr_may_overflow_or_div(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) => false,
        Expr::BinaryOp { op, left, right } => {
            matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) || expr_may_overflow_or_div(left)
                || expr_may_overflow_or_div(right)
        }
        Expr::ScalarUdf { .. } => true,
        Expr::Case {
            when_then,
            else_expr,
        } => {
            when_then
                .iter()
                .any(|(w, t)| expr_may_overflow_or_div(w) || expr_may_overflow_or_div(t))
                || expr_may_overflow_or_div(else_expr)
        }
    }
}

fn try_compose_projects(prev: &[NamedExpr], next: &[NamedExpr]) -> Option<Vec<NamedExpr>> {
    let mut ref_counts = vec![0usize; prev.len()];
    for ne in next {
        count_column_refs(&ne.expr, &mut ref_counts);
    }
    for (i, ne) in prev.iter().enumerate() {
        // Avoid duplicating non-trivial expressions or dropping potentially failing expressions.
        if ref_counts[i] == 0 && expr_may_overflow_or_div(&ne.expr) {
            return None;
        }
        if ref_counts[i] > 1 && !is_infallible_expr(&ne.expr) {
            return None;
        }
    }
    next.iter()
        .map(|ne| {
            Some(NamedExpr::new(
                ne.name.clone(),
                substitute_columns(&ne.expr, prev)?,
            ))
        })
        .collect()
}

type FilteredBatch = (RecordBatch, Vec<i64>, Vec<Arc<BatchKeyCapsule>>);

fn apply_filter_chain(
    data: RecordBatch,
    weights: Vec<i64>,
    capsules: Vec<Arc<BatchKeyCapsule>>,
    predicates: &[Expr],
) -> Result<FilteredBatch, OpError> {
    let n = data.num_rows();
    if n == 0 || predicates.is_empty() {
        return Ok((data, weights, capsules));
    }
    let mut mask = vec![true; n];
    let mut surviving_count = n;
    for pred in predicates {
        let step_mask = eval_bool(pred, &data)?;
        surviving_count = 0;
        for (keep, pass) in mask.iter_mut().zip(step_mask.iter()) {
            *keep = *keep && *pass;
            if *keep {
                surviving_count += 1;
            }
        }
        if surviving_count == 0 {
            break;
        }
    }
    if surviving_count == n {
        return Ok((data, weights, capsules));
    }
    if surviving_count == 0 {
        let empty = ArrowZSet::empty(data.schema());
        return Ok((empty.data, Vec::new(), Vec::new()));
    }
    let indices: Vec<usize> = mask
        .iter()
        .enumerate()
        .filter_map(|(i, &keep)| keep.then_some(i))
        .collect();
    let take_indices =
        arrow::array::UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    let filtered_cols: Vec<ArrayRef> = data
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &take_indices, None).map_err(OpError::arrow))
        .collect::<Result<_, _>>()?;
    let new_data = RecordBatch::try_new(data.schema(), filtered_cols).map_err(OpError::arrow)?;
    let new_weights: Vec<i64> = indices.iter().map(|&i| weights[i]).collect();
    let new_capsules: Vec<Arc<BatchKeyCapsule>> = capsules
        .iter()
        .filter(|c| c.len() == n)
        .map(|c| Arc::new(c.select_rows(&indices)))
        .collect();
    Ok((new_data, new_weights, new_capsules))
}

fn eval_project(
    data: &RecordBatch,
    capsules: &[Arc<BatchKeyCapsule>],
    exprs: &[NamedExpr],
) -> Result<(RecordBatch, Vec<Arc<BatchKeyCapsule>>), OpError> {
    let cols: Vec<Arc<dyn Array>> = exprs
        .iter()
        .map(|ne| eval_to_array(&ne.expr, data))
        .collect::<Result<Vec<_>, _>>()?;
    let fields: Vec<Field> = exprs
        .iter()
        .zip(cols.iter())
        .map(|(ne, col)| {
            let nullable = match &ne.expr {
                Expr::Column(index) => data.schema().field(*index).is_nullable(),
                _ => col.null_count() > 0,
            };
            Field::new(&ne.name, col.data_type().clone(), nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let new_data = RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(data.num_rows())),
    )
    .map_err(OpError::arrow)?;
    let remapped = remap_capsules_through_project(capsules, exprs);
    Ok((new_data, remapped))
}

fn eval_map(
    data: &RecordBatch,
    capsules: &[Arc<BatchKeyCapsule>],
    expr: &Expr,
    output_name: &str,
) -> Result<(RecordBatch, Vec<Arc<BatchKeyCapsule>>), OpError> {
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        output_name,
        DataType::Int64,
        false,
    )]));
    if data.num_rows() == 0 {
        return Ok((ArrowZSet::empty(output_schema).data, Vec::new()));
    }
    let col = eval_to_array(expr, data)?;
    let new_data = RecordBatch::try_new(output_schema, vec![col]).map_err(OpError::arrow)?;
    let mut remapped = Vec::new();
    if let Expr::Column(src_col) = expr {
        for capsule in capsules {
            if capsule.spec.columns.as_slice() == [*src_col] {
                remapped.push(Arc::new(capsule.with_remapped_columns(vec![0])));
            }
        }
    }
    Ok((new_data, remapped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::filter::FilterOp;
    use crate::map::MapOp;
    use crate::project::ProjectOp;
    use arrow::array::Int64Array;
    use rockstream_types::measure_batch_key_capsule_encodings;

    #[test]
    fn fused_filter_project_map_matches_unfused_bit_for_bit() {
        let p1 = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(10)),
        };
        let p2 = Expr::BinaryOp {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(100)),
        };
        let proj = vec![
            NamedExpr::new("k", Expr::Column(0)),
            NamedExpr::new(
                "v2",
                Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(3)),
                },
            ),
        ];
        let map_expr = Expr::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(7)),
        };

        let input = ArrowZSet::from_ab_weighted(&[(1, 5, 1), (2, 20, 2), (3, 50, -1), (4, 120, 1)]);

        // Unfused execution
        let u1 = FilterOp::new(p1.clone()).apply(input.clone()).unwrap();
        let u2 = FilterOp::new(p2.clone()).apply(u1).unwrap();
        let u3 = ProjectOp::new(proj.clone()).apply(u2).unwrap();
        let unfused = MapOp::new(map_expr.clone(), "res").apply(u3).unwrap();

        // Fused execution
        let kernel = FusedStatelessKernel::compile(vec![
            StatelessStep::Filter(p1),
            StatelessStep::Filter(p2),
            StatelessStep::Project(proj),
            StatelessStep::Map {
                expr: map_expr,
                output_name: "res".to_string(),
            },
        ]);
        assert_eq!(kernel.fused_stage_count(), 4);
        assert_eq!(kernel.physical_op_count(), 1);
        let fused = kernel.apply(input).unwrap();

        assert_eq!(fused.schema(), unfused.schema());
        assert_eq!(fused.weights, unfused.weights);
        assert_eq!(
            fused
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            unfused
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
        assert_eq!(kernel.intermediate_batches_elided(), 3);
    }

    #[test]
    fn fused_filter_project_preserves_and_remaps_key_capsule_with_zero_reencoding() {
        let mut input = ArrowZSet::from_ab_weighted(&[(10, 1, 1), (20, 50, 1), (30, 80, -1)]);
        let (_, enc1) =
            measure_batch_key_capsule_encodings(|| input.attach_key_capsule(&[0]).unwrap());
        assert_eq!(enc1, 1);

        let kernel = FusedStatelessKernel::compile(vec![
            StatelessStep::Filter(Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(10)),
            }),
            StatelessStep::Project(vec![
                NamedExpr::new("val", Expr::Column(1)),
                NamedExpr::new("key", Expr::Column(0)),
            ]),
        ]);

        let (out, enc2) = measure_batch_key_capsule_encodings(|| kernel.apply(input).unwrap());
        assert_eq!(enc2, 0);
        let capsule = out
            .key_capsule_for(&[1])
            .expect("remapped capsule at col 1");
        assert_eq!(capsule.len(), 2);
        let fresh = BatchKeyCapsule::from_record_batch(&out.data, &[1]).unwrap();
        assert_eq!(capsule.row_bytes(0), fresh.row_bytes(0));
        assert_eq!(capsule.row_bytes(1), fresh.row_bytes(1));
    }
}
