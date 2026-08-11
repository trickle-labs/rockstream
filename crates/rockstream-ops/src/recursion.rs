//! Recursive fixed-point operator (v0.50 Track A).
//!
//! Maintains a bounded fixed-point arrangement for recursive queries. The
//! implementation uses set semantics and recomputes the fixed-point from the
//! current base relation each epoch; monotone plans reject negative deltas.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use rockstream_plan::{AggregateFunc, OpKind, OuterJoinKind, PlanNode};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::audit::AuditEvent;
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::expr::{eval_bool, eval_i64};
use crate::op::Operator;
use crate::zset::ArrowZSet;

pub const RECURSION_STATE_LIMIT: usize = 100_000;
const COST_SPIKE_MULTIPLE: u64 = 4;

type Relation = BTreeMap<Vec<i64>, i64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursionStrategy {
    SemiNaive,
    Recompute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedShardStatus {
    pub shard_id: u64,
    pub frontier_iteration: u64,
    pub delta_is_empty: bool,
    pub iteration_cost: u64,
}

#[derive(Default)]
struct RecursionState {
    input_relation: Relation,
    output_relation: Relation,
    iteration_rows: BTreeMap<(u128, u32), (Vec<i64>, i64)>,
    per_shard_strategy: HashMap<u64, RecursionStrategy>,
    audit_events: Vec<AuditEvent>,
}

impl RecursionState {
    fn state_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        for vals in self.input_relation.keys() {
            bytes += (vals.len() * 8 + 8) as u64;
        }
        for vals in self.output_relation.keys() {
            bytes += (vals.len() * 8 + 8) as u64;
        }
        for (vals, _) in self.iteration_rows.values() {
            bytes += (28 + vals.len() * 8) as u64;
        }
        bytes
    }
}

pub struct RecursionOp {
    schema: SchemaRef,
    base: PlanNode,
    step: PlanNode,
    recursive_source_name: String,
    input_source_name: String,
    max_iterations: usize,
    monotone: bool,
    state_limit: usize,
    state: Mutex<RecursionState>,
    fill_level: Arc<AtomicUsize>,
}

impl RecursionOp {
    pub fn new(
        schema: SchemaRef,
        base: PlanNode,
        step: PlanNode,
        max_iterations: usize,
        monotone: bool,
    ) -> Self {
        Self::new_with_state_limit(
            schema,
            base,
            step,
            max_iterations,
            monotone,
            RECURSION_STATE_LIMIT,
        )
    }

    pub fn new_with_state_limit(
        schema: SchemaRef,
        base: PlanNode,
        step: PlanNode,
        max_iterations: usize,
        monotone: bool,
        state_limit: usize,
    ) -> Self {
        let base_sources = collect_source_names(&base);
        let step_sources = collect_source_names(&step);
        let recursive_source_name = step_sources
            .iter()
            .find(|name| !base_sources.contains(*name))
            .cloned()
            .or_else(|| step_sources.first().cloned())
            .unwrap_or_else(|| "recursive".to_string());
        let input_source_name = base_sources
            .into_iter()
            .find(|name| name != &recursive_source_name)
            .unwrap_or_else(|| recursive_source_name.clone());
        Self {
            schema,
            base,
            step,
            recursive_source_name,
            input_source_name,
            max_iterations,
            monotone,
            state_limit,
            state: Mutex::new(RecursionState::default()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    pub fn strategy_for_shard(&self, shard_id: u64) -> Option<RecursionStrategy> {
        self.state
            .lock()
            .unwrap()
            .per_shard_strategy
            .get(&shard_id)
            .copied()
    }

    pub fn audit_events(&self) -> Vec<AuditEvent> {
        self.state.lock().unwrap().audit_events.clone()
    }

    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if self.monotone && delta.weights.iter().any(|w| *w < 0) {
            return Err(OpError::recursion_non_monotone_delta());
        }

        let mut state = self.state.lock().unwrap();
        apply_delta_to_relation(&mut state.input_relation, &delta);
        let fixed = self.compute_fixed_point(&state.input_relation)?;
        if fixed.iteration_rows.len() > self.state_limit {
            return Err(OpError::recursion_state_overflow(
                fixed.iteration_rows.len(),
                self.state_limit,
            ));
        }
        let diff = diff_relations(&state.output_relation, &fixed.output_relation);
        state.output_relation = fixed.output_relation;
        state.iteration_rows = fixed.iteration_rows;
        self.fill_level
            .store(state.iteration_rows.len(), Ordering::Relaxed);
        build_zset(&self.schema, &diff)
    }

    pub fn process_distributed_epoch(
        &self,
        shard_batches: &[(u64, ArrowZSet)],
        shard_statuses: &[DistributedShardStatus],
        epoch: u64,
    ) -> Result<ArrowZSet, OpError> {
        if !shard_statuses.is_empty() {
            let min_frontier = shard_statuses
                .iter()
                .map(|status| status.frontier_iteration)
                .min()
                .unwrap_or(0);
            let max_frontier = shard_statuses
                .iter()
                .map(|status| status.frontier_iteration)
                .max()
                .unwrap_or(0);
            if min_frontier != max_frontier
                && shard_statuses.iter().any(|status| !status.delta_is_empty)
            {
                return Err(OpError::recursion_inner_frontier_stalled());
            }

            let mut costs: Vec<u64> = shard_statuses
                .iter()
                .map(|status| status.iteration_cost)
                .collect();
            costs.sort_unstable();
            let median = costs[(costs.len().saturating_sub(1)) / 2];
            let mut state = self.state.lock().unwrap();
            for status in shard_statuses {
                state
                    .per_shard_strategy
                    .entry(status.shard_id)
                    .or_insert(RecursionStrategy::SemiNaive);
                if median > 0 && status.iteration_cost > median.saturating_mul(COST_SPIKE_MULTIPLE)
                {
                    state
                        .per_shard_strategy
                        .insert(status.shard_id, RecursionStrategy::Recompute);
                    state.audit_events.push(
                        AuditEvent::now(
                            "recursion",
                            "recursion.strategy_fallback",
                            status.shard_id.to_string(),
                        )
                        .with_detail(format!(
                            "cost={} median={} strategy=Recompute",
                            status.iteration_cost, median
                        )),
                    );
                }
            }
        }
        let merged = merge_batches(&self.schema, shard_batches)?;
        self.process_epoch(merged, epoch)
    }

    fn compute_fixed_point(&self, input_relation: &Relation) -> Result<FixedPointResult, OpError> {
        let mut bindings = HashMap::new();
        bindings.insert(
            self.input_source_name.clone(),
            positive_relation(input_relation),
        );
        bindings.insert(self.recursive_source_name.clone(), Relation::new());

        let mut output_relation = distinct_relation(&eval_plan(&self.base, &bindings)?);
        let mut iteration_rows = BTreeMap::new();
        for row in output_relation.keys() {
            iteration_rows.insert((row_hash(row), 0), (row.clone(), 1));
        }

        for iteration in 1..=self.max_iterations {
            bindings.insert(self.recursive_source_name.clone(), output_relation.clone());
            let step_relation = distinct_relation(&eval_plan(&self.step, &bindings)?);
            let delta = relation_difference(&step_relation, &output_relation);
            if delta.is_empty() {
                return Ok(FixedPointResult {
                    output_relation,
                    iteration_rows,
                });
            }
            for row in delta.keys() {
                iteration_rows.insert((row_hash(row), iteration as u32), (row.clone(), 1));
            }
            merge_relation(&mut output_relation, &delta);
            if iteration_rows.len() > self.state_limit {
                return Err(OpError::recursion_state_overflow(
                    iteration_rows.len(),
                    self.state_limit,
                ));
            }
        }

        Err(OpError::recursion_max_iterations(self.max_iterations))
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }
}

impl Operator for RecursionOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn name(&self) -> &str {
        "RecursionOp"
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }
}

struct FixedPointResult {
    output_relation: Relation,
    iteration_rows: BTreeMap<(u128, u32), (Vec<i64>, i64)>,
}

fn collect_source_names(plan: &PlanNode) -> Vec<String> {
    let mut out = Vec::new();
    collect_source_names_inner(plan, &mut out);
    out
}

fn collect_source_names_inner(plan: &PlanNode, out: &mut Vec<String>) {
    match plan {
        PlanNode::Source { name } => out.push(name.clone()),
        PlanNode::Snapshot { source_name, .. } => out.push(source_name.clone()),
        PlanNode::ViewRef { view_name } => out.push(view_name.clone()),
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Map { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Distinct { input, .. }
        | PlanNode::Window { input, .. }
        | PlanNode::TumbleWindow { input, .. }
        | PlanNode::HopWindow { input, .. }
        | PlanNode::SessionWindow { input, .. }
        | PlanNode::TopK { input, .. }
        | PlanNode::Lateral { input, .. }
        | PlanNode::IndexArrange { input, .. } => collect_source_names_inner(input, out),
        PlanNode::Exchange { child, .. } | PlanNode::ViewSink { child, .. } => {
            collect_source_names_inner(child, out)
        }
        PlanNode::Join { left, right, .. }
        | PlanNode::InnerJoin { left, right, .. }
        | PlanNode::OuterJoin { left, right, .. }
        | PlanNode::Union { left, right }
        | PlanNode::Intersect { left, right, .. }
        | PlanNode::Except { left, right, .. } => {
            collect_source_names_inner(left, out);
            collect_source_names_inner(right, out);
        }
        PlanNode::Recursion { base, step, .. } => {
            collect_source_names_inner(base, out);
            collect_source_names_inner(step, out);
        }
    }
}

fn apply_delta_to_relation(relation: &mut Relation, delta: &ArrowZSet) {
    if delta.is_empty() {
        return;
    }
    for row_idx in 0..delta.num_rows() {
        let row = extract_row(&delta.data, row_idx);
        let entry = relation.entry(row.clone()).or_insert(0);
        *entry += delta.weights[row_idx];
        if *entry == 0 {
            relation.remove(&row);
        }
    }
}

fn positive_relation(relation: &Relation) -> Relation {
    relation
        .iter()
        .filter(|(_, weight)| **weight > 0)
        .map(|(row, weight)| (row.clone(), *weight))
        .collect()
}

fn distinct_relation(relation: &Relation) -> Relation {
    relation
        .iter()
        .filter(|(_, weight)| **weight > 0)
        .map(|(row, _)| (row.clone(), 1))
        .collect()
}

fn relation_difference(left: &Relation, right: &Relation) -> Relation {
    left.iter()
        .filter(|(row, weight)| {
            **weight > 0 && !matches!(right.get(*row), Some(existing) if *existing > 0)
        })
        .map(|(row, _)| (row.clone(), 1))
        .collect()
}

fn merge_relation(dst: &mut Relation, src: &Relation) {
    for (row, weight) in src {
        let entry = dst.entry(row.clone()).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            dst.remove(row);
        }
    }
}

fn diff_relations(old: &Relation, new: &Relation) -> Relation {
    let mut diff = Relation::new();
    let keys: HashSet<Vec<i64>> = old.keys().cloned().chain(new.keys().cloned()).collect();
    for key in keys {
        let new_weight = new.get(&key).copied().unwrap_or(0);
        let old_weight = old.get(&key).copied().unwrap_or(0);
        let delta = new_weight - old_weight;
        if delta != 0 {
            diff.insert(key, delta);
        }
    }
    diff
}

fn eval_plan(plan: &PlanNode, bindings: &HashMap<String, Relation>) -> Result<Relation, OpError> {
    match plan {
        PlanNode::Source { name } => Ok(bindings.get(name).cloned().unwrap_or_default()),
        PlanNode::Snapshot { source_name, .. } => {
            Ok(bindings.get(source_name).cloned().unwrap_or_default())
        }
        PlanNode::ViewRef { view_name } => Ok(bindings.get(view_name).cloned().unwrap_or_default()),
        PlanNode::Filter { input, predicate } => {
            let input_relation = eval_plan(input, bindings)?;
            if input_relation.is_empty() {
                return Ok(Relation::new());
            }
            let batch = relation_to_batch(input_relation)?;
            let mask = eval_bool(predicate, &batch.data)?;
            let mut out = Relation::new();
            for (row_idx, keep) in mask.into_iter().enumerate() {
                if keep {
                    let row = extract_row(&batch.data, row_idx);
                    *out.entry(row).or_insert(0) += batch.weights[row_idx];
                }
            }
            Ok(out)
        }
        PlanNode::Project { input, columns } => {
            let input_relation = eval_plan(input, bindings)?;
            if input_relation.is_empty() {
                return Ok(Relation::new());
            }
            let batch = relation_to_batch(input_relation)?;
            let mut projected_rows = Relation::new();
            let values: Vec<Vec<i64>> = columns
                .iter()
                .map(|expr| eval_i64(expr, &batch.data))
                .collect::<Result<_, _>>()?;
            for row_idx in 0..batch.num_rows() {
                let row: Vec<i64> = values.iter().map(|col| col[row_idx]).collect();
                *projected_rows.entry(row).or_insert(0) += batch.weights[row_idx];
            }
            Ok(projected_rows)
        }
        PlanNode::Union { left, right } => {
            let mut out = eval_plan(left, bindings)?;
            let right_relation = eval_plan(right, bindings)?;
            merge_relation(&mut out, &right_relation);
            Ok(out)
        }
        PlanNode::Distinct { input, .. } => Ok(distinct_relation(&eval_plan(input, bindings)?)),
        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            let left_relation = positive_relation(&eval_plan(left, bindings)?);
            let right_relation = positive_relation(&eval_plan(right, bindings)?);
            let mut out = Relation::new();
            for (left_row, left_weight) in &left_relation {
                for (right_row, right_weight) in &right_relation {
                    if keys_match(left_row, right_row, left_keys, right_keys) {
                        let mut row = left_row.clone();
                        row.extend_from_slice(right_row);
                        *out.entry(row).or_insert(0) += left_weight * right_weight;
                    }
                }
            }
            Ok(out)
        }
        PlanNode::OuterJoin {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            ..
        } => {
            let left_relation = positive_relation(&eval_plan(left, bindings)?);
            let right_relation = positive_relation(&eval_plan(right, bindings)?);
            let mut out = Relation::new();
            for (left_row, left_weight) in &left_relation {
                let matched = right_relation
                    .keys()
                    .any(|right_row| keys_match(left_row, right_row, left_keys, right_keys));
                match kind {
                    OuterJoinKind::Semi if matched => {
                        *out.entry(left_row.clone()).or_insert(0) += *left_weight;
                    }
                    OuterJoinKind::Anti if !matched => {
                        *out.entry(left_row.clone()).or_insert(0) += *left_weight;
                    }
                    _ => {}
                }
            }
            Ok(out)
        }
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let input_relation = positive_relation(&eval_plan(input, bindings)?);
            if input_relation.is_empty() {
                return Ok(Relation::new());
            }
            let mut groups: BTreeMap<Vec<i64>, (i64, i64)> = BTreeMap::new();
            for (row, weight) in input_relation {
                for _ in 0..weight {
                    let batch = one_row_batch(&row)?;
                    let group_key: Vec<i64> = group_by
                        .iter()
                        .map(|expr| eval_i64(expr, &batch.data).map(|col| col[0]))
                        .collect::<Result<_, _>>()?;
                    let entry = groups.entry(group_key).or_insert((0, 0));
                    entry.1 += 1;
                    let agg_value = eval_i64(&aggregates[0].input, &batch.data)?[0];
                    entry.0 += agg_value;
                }
            }
            let mut out = Relation::new();
            for (group_key, (sum, count)) in groups {
                let agg_value = match aggregates[0].func {
                    AggregateFunc::Count => count,
                    AggregateFunc::Sum => sum,
                    AggregateFunc::Avg => {
                        if count == 0 {
                            0
                        } else {
                            sum / count
                        }
                    }
                    _ => 0,
                };
                let mut row = group_key;
                row.push(agg_value);
                out.insert(row, 1);
            }
            Ok(out)
        }
        PlanNode::Exchange { child, .. } | PlanNode::ViewSink { child, .. } => {
            eval_plan(child, bindings)
        }
        PlanNode::Recursion { .. }
        | PlanNode::Join { .. }
        | PlanNode::Intersect { .. }
        | PlanNode::Except { .. }
        | PlanNode::Map { .. }
        | PlanNode::Window { .. }
        | PlanNode::TumbleWindow { .. }
        | PlanNode::HopWindow { .. }
        | PlanNode::SessionWindow { .. }
        | PlanNode::TopK { .. }
        | PlanNode::Lateral { .. }
        | PlanNode::IndexArrange { .. } => Err(OpError::unimplemented(format!(
            "recursion evaluator for {:?}",
            plan_kind(plan)
        ))),
    }
}

fn plan_kind(plan: &PlanNode) -> OpKind {
    match plan {
        PlanNode::Source { name } => OpKind::Source { name: name.clone() },
        PlanNode::Filter { .. } => OpKind::Filter,
        PlanNode::Project { .. } => OpKind::Project,
        PlanNode::Map { .. } => OpKind::Map,
        PlanNode::Aggregate { .. } => OpKind::Aggregate,
        PlanNode::Distinct { .. } => OpKind::Distinct,
        PlanNode::Window { .. } => OpKind::Window {
            strategy: rockstream_plan::WindowStrategy::PartitionRecompute,
        },
        PlanNode::TumbleWindow {
            window_size_ms,
            late_data_policy,
            ..
        } => OpKind::TumbleWindow {
            window_size_ms: *window_size_ms,
            late_data_policy: late_data_policy.clone(),
        },
        PlanNode::HopWindow {
            window_size_ms,
            slide_ms,
            late_data_policy,
            ..
        } => OpKind::HopWindow {
            window_size_ms: *window_size_ms,
            slide_ms: *slide_ms,
            late_data_policy: late_data_policy.clone(),
        },
        PlanNode::SessionWindow {
            gap_ms,
            late_data_policy,
            ..
        } => OpKind::SessionWindow {
            gap_ms: *gap_ms,
            late_data_policy: late_data_policy.clone(),
        },
        PlanNode::TopK {
            k,
            rank_col,
            partition_by,
            ..
        } => OpKind::TopK {
            k: *k,
            rank_col: *rank_col,
            partition_by: partition_by.clone(),
        },
        PlanNode::Recursion {
            max_iterations,
            monotone,
            ..
        } => OpKind::Recursion {
            max_iterations: *max_iterations,
            monotone: *monotone,
        },
        PlanNode::Snapshot {
            source_name,
            batch_size,
        } => OpKind::Snapshot {
            source_name: source_name.clone(),
            batch_size: *batch_size,
        },
        PlanNode::ViewRef { view_name } => OpKind::ViewRef {
            view_name: view_name.clone(),
        },
        PlanNode::Lateral { func, .. } => OpKind::Lateral { func: func.clone() },
        PlanNode::Exchange { kind, .. } => OpKind::Exchange { kind: *kind },
        PlanNode::ViewSink { view_name, pk, .. } => OpKind::ViewSink {
            view_name: view_name.clone(),
            pk: pk.clone(),
        },
        PlanNode::Join { .. } => OpKind::Join,
        PlanNode::InnerJoin { .. } => OpKind::Join,
        PlanNode::OuterJoin {
            kind,
            left_keys,
            right_keys,
            ..
        } => OpKind::OuterJoin {
            kind: *kind,
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
        },
        PlanNode::Union { .. } => OpKind::Union,
        PlanNode::Intersect { all, .. } => OpKind::Intersect { all: *all },
        PlanNode::Except { all, .. } => OpKind::Except { all: *all },
        PlanNode::IndexArrange { .. } => OpKind::Map,
    }
}

fn keys_match(
    left_row: &[i64],
    right_row: &[i64],
    left_keys: &[usize],
    right_keys: &[usize],
) -> bool {
    left_keys
        .iter()
        .zip(right_keys)
        .all(|(left_key, right_key)| left_row[*left_key] == right_row[*right_key])
}

fn relation_to_batch(relation: Relation) -> Result<ArrowZSet, OpError> {
    if relation.is_empty() {
        return Ok(ArrowZSet::empty(Arc::new(Schema::new(Vec::<Field>::new()))));
    }
    let width = relation.keys().next().map(|row| row.len()).unwrap_or(0);
    let fields: Vec<Field> = (0..width)
        .map(|idx| Field::new(format!("col_{idx}"), DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let mut cols: Vec<Vec<i64>> = vec![Vec::new(); width];
    let mut weights = Vec::new();
    for (row, weight) in relation {
        if weight <= 0 {
            continue;
        }
        for (idx, value) in row.iter().enumerate() {
            cols[idx].push(*value);
        }
        weights.push(weight);
    }
    if weights.is_empty() {
        return Ok(ArrowZSet::empty(schema));
    }
    let arrays: Vec<ArrayRef> = cols
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();
    let data = RecordBatch::try_new(schema.clone(), arrays).map_err(OpError::arrow)?;
    Ok(ArrowZSet::new(data, weights))
}

fn one_row_batch(row: &[i64]) -> Result<ArrowZSet, OpError> {
    let relation = BTreeMap::from([(row.to_vec(), 1)]);
    relation_to_batch(relation)
}

fn extract_row(batch: &RecordBatch, row_idx: usize) -> Vec<i64> {
    batch
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|array| array.value(row_idx))
                .unwrap_or(0)
        })
        .collect()
}

fn row_hash(row: &[i64]) -> u128 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;
    const OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
    const OFFSET_B: u64 = 0x6c62_272e_07bb_0142;
    let mut h0 = OFFSET_A;
    let mut h1 = OFFSET_B;
    for value in row {
        for byte in value.to_be_bytes() {
            h0 ^= byte as u64;
            h0 = h0.wrapping_mul(FNV_PRIME);
            h1 ^= (byte ^ 0x5A) as u64;
            h1 = h1.wrapping_mul(FNV_PRIME);
        }
    }
    ((h0 as u128) << 64) | (h1 as u128)
}

fn build_zset(schema: &SchemaRef, relation: &Relation) -> Result<ArrowZSet, OpError> {
    if relation.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }
    let width = schema.fields().len();
    let mut cols: Vec<Vec<i64>> = vec![Vec::new(); width];
    let mut weights = Vec::new();
    for (row, weight) in relation {
        if *weight == 0 {
            continue;
        }
        for (idx, value) in row.iter().enumerate().take(width) {
            cols[idx].push(*value);
        }
        weights.push(*weight);
    }
    if weights.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }
    let arrays: Vec<ArrayRef> = cols
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();
    let data = RecordBatch::try_new(schema.clone(), arrays).map_err(OpError::arrow)?;
    Ok(ArrowZSet::new(data, weights))
}

fn merge_batches(
    schema: &SchemaRef,
    shard_batches: &[(u64, ArrowZSet)],
) -> Result<ArrowZSet, OpError> {
    let mut relation = Relation::new();
    for (_, batch) in shard_batches {
        for row_idx in 0..batch.num_rows() {
            let row = extract_row(&batch.data, row_idx);
            *relation.entry(row).or_insert(0) += batch.weights[row_idx];
        }
    }
    build_zset(schema, &relation)
}

fn encode_value(weight: i64, row: &[i64]) -> Vec<u8> {
    let mut value = Vec::with_capacity(8 + row.len() * 8);
    value.extend_from_slice(&weight.to_be_bytes());
    for cell in row {
        value.extend_from_slice(&cell.to_be_bytes());
    }
    value
}

fn decode_value(bytes: &[u8]) -> Option<(i64, Vec<i64>)> {
    if bytes.len() < 8 || !bytes.len().is_multiple_of(8) {
        return None;
    }
    let weight = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let mut row = Vec::new();
    let mut offset = 8;
    while offset < bytes.len() {
        row.push(i64::from_be_bytes(
            bytes[offset..offset + 8].try_into().ok()?,
        ));
        offset += 8;
    }
    Some((weight, row))
}

pub async fn persist_recursion_state(
    db: &ShardDb,
    op: &RecursionOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        for ((row_hash, iteration), (row, weight)) in &state.iteration_rows {
            let key = ShardKeyEncoder::recursion_key(op_id.0, *row_hash, *iteration);
            let value = encode_value(*weight, row);
            batch.put(&key, &value);
        }
        batch
    };
    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

pub async fn load_recursion_state(
    db: &ShardDb,
    schema: SchemaRef,
    base: PlanNode,
    step: PlanNode,
    max_iterations: usize,
    monotone: bool,
    op_id: OperatorId,
) -> Result<RecursionOp, OpError> {
    let op = RecursionOp::new(schema, base, step, max_iterations, monotone);
    let prefix = ShardKeyEncoder::recursion_op_prefix(op_id.0);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    let mut state = op.state.lock().unwrap();
    for (key, value) in entries {
        if key.len() < prefix.len() + 16 + 4 {
            continue;
        }
        let row_hash = u128::from_be_bytes(
            key[prefix.len()..prefix.len() + 16]
                .try_into()
                .unwrap_or([0; 16]),
        );
        let iteration = u32::from_be_bytes(
            key[prefix.len() + 16..prefix.len() + 20]
                .try_into()
                .unwrap_or([0; 4]),
        );
        if let Some((weight, row)) = decode_value(&value) {
            state
                .iteration_rows
                .insert((row_hash, iteration), (row.clone(), weight));
            if iteration == 0 {
                state.input_relation.insert(row.clone(), weight);
            }
            if weight > 0 {
                state.output_relation.insert(row, weight);
            }
        }
    }
    op.fill_level
        .store(state.iteration_rows.len(), Ordering::Relaxed);
    drop(state);
    Ok(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use rockstream_plan::{Expr, JoinSemantics, OuterJoinKind};

    fn schema_edges() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]))
    }

    fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let src: Vec<i64> = rows.iter().map(|row| row.0).collect();
        let dst: Vec<i64> = rows.iter().map(|row| row.1).collect();
        let weights: Vec<i64> = rows.iter().map(|row| row.2).collect();
        let data = RecordBatch::try_new(
            schema_edges(),
            vec![
                Arc::new(Int64Array::from(src)) as ArrayRef,
                Arc::new(Int64Array::from(dst)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, weights)
    }

    fn base_plan() -> PlanNode {
        PlanNode::Source {
            name: "edges".to_string(),
        }
    }

    fn recursive_join_project() -> PlanNode {
        PlanNode::Project {
            input: Box::new(PlanNode::InnerJoin {
                left: Box::new(PlanNode::Source {
                    name: "reach".to_string(),
                }),
                right: Box::new(PlanNode::Source {
                    name: "edges".to_string(),
                }),
                left_keys: vec![1],
                right_keys: vec![0],
                left_arr_id: OperatorId(1),
                right_arr_id: OperatorId(2),
                semantics: JoinSemantics::default(),
            }),
            columns: vec![Expr::Column(0), Expr::Column(3)],
        }
    }

    fn non_monotone_step() -> PlanNode {
        PlanNode::OuterJoin {
            kind: OuterJoinKind::Anti,
            left: Box::new(PlanNode::Distinct {
                input: Box::new(recursive_join_project()),
                arr_id: OperatorId(3),
            }),
            right: Box::new(PlanNode::Source {
                name: "edges".to_string(),
            }),
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            left_arr_id: OperatorId(4),
            right_arr_id: OperatorId(5),
            unmatched_arr_id: OperatorId(6),
        }
    }

    fn closure_rows(batch: &ArrowZSet) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        if batch.is_empty() {
            return out;
        }
        let src = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dst = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row_idx in 0..batch.num_rows() {
            *out.entry((src.value(row_idx), dst.value(row_idx)))
                .or_insert(0) += batch.weights[row_idx];
        }
        out.retain(|_, weight| *weight > 0);
        out
    }

    fn batch_transitive_closure(edges: &[(i64, i64)]) -> BTreeMap<(i64, i64), i64> {
        let mut closure: HashSet<(i64, i64)> = edges.iter().copied().collect();
        loop {
            let mut changed = false;
            let snapshot: Vec<(i64, i64)> = closure.iter().copied().collect();
            for (src, mid) in &snapshot {
                for (mid2, dst) in &snapshot {
                    if mid == mid2 && closure.insert((*src, *dst)) {
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        closure.into_iter().map(|row| (row, 1)).collect()
    }

    #[test]
    fn recursion_semi_naive_matches_batch_transitive_closure() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        assert_eq!(
            closure_rows(&out),
            batch_transitive_closure(&[(1, 2), (2, 3), (3, 4)])
        );
    }

    #[test]
    fn recursion_converges_and_stops_on_empty_delta() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let _ = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1)]), 1)
            .unwrap();
        let out = op
            .process_epoch(ArrowZSet::empty(schema_edges()), 2)
            .unwrap();
        assert!(out.is_empty());
        assert!(op.fill_level() >= 2);
    }

    #[test]
    fn recursion_max_iterations_cap_fails_epoch_conservatively() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            1,
            true,
        );
        let err = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1)]), 1)
            .expect_err("max-iteration cap must fail");
        assert!(err.to_string().contains("RS-1513"));
    }

    #[test]
    fn recursion_non_monotone_delta_rejected_with_rs_1009() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let err = op
            .process_epoch(make_input(&[(1, 2, -1)]), 1)
            .expect_err("monotone recursion must reject retractions");
        assert!(err.to_string().contains("RS-1009"));
    }

    #[test]
    fn recursion_state_bound_triggers_backpressure() {
        let op = RecursionOp::new_with_state_limit(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
            2,
        );
        let err = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .expect_err("state bound must fail");
        assert!(err.to_string().contains("RS-2019"));
    }

    #[test]
    fn distributed_recursion_converges_across_shards() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let out = op
            .process_distributed_epoch(
                &[
                    (1, make_input(&[(1, 2, 1), (2, 3, 1)])),
                    (2, make_input(&[(3, 4, 1)])),
                ],
                &[
                    DistributedShardStatus {
                        shard_id: 1,
                        frontier_iteration: 4,
                        delta_is_empty: true,
                        iteration_cost: 10,
                    },
                    DistributedShardStatus {
                        shard_id: 2,
                        frontier_iteration: 4,
                        delta_is_empty: true,
                        iteration_cost: 12,
                    },
                ],
                1,
            )
            .unwrap();
        assert_eq!(
            closure_rows(&out),
            batch_transitive_closure(&[(1, 2), (2, 3), (3, 4)])
        );
    }

    #[test]
    fn distributed_recursion_inner_frontier_stall_fails_epoch_with_rs_1512() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let err = op
            .process_distributed_epoch(
                &[
                    (1, make_input(&[(1, 2, 1)])),
                    (2, ArrowZSet::empty(schema_edges())),
                ],
                &[
                    DistributedShardStatus {
                        shard_id: 1,
                        frontier_iteration: 3,
                        delta_is_empty: false,
                        iteration_cost: 10,
                    },
                    DistributedShardStatus {
                        shard_id: 2,
                        frontier_iteration: 1,
                        delta_is_empty: true,
                        iteration_cost: 10,
                    },
                ],
                1,
            )
            .expect_err("stalled shard must fail");
        assert!(err.to_string().contains("RS-1512"));
    }

    #[test]
    fn distributed_recursion_max_iterations_fails_with_rs_1513() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            1,
            true,
        );
        let err = op
            .process_distributed_epoch(
                &[(1, make_input(&[(1, 2, 1), (2, 3, 1)]))],
                &[DistributedShardStatus {
                    shard_id: 1,
                    frontier_iteration: 2,
                    delta_is_empty: true,
                    iteration_cost: 10,
                }],
                1,
            )
            .expect_err("max-iteration cap must fail");
        assert!(err.to_string().contains("RS-1513"));
    }

    #[test]
    fn distributed_recursion_per_shard_cost_spike_falls_back_to_recompute() {
        let op = RecursionOp::new(
            schema_edges(),
            base_plan(),
            recursive_join_project(),
            16,
            true,
        );
        let _ = op
            .process_distributed_epoch(
                &[(1, make_input(&[(1, 2, 1)])), (2, make_input(&[(2, 3, 1)]))],
                &[
                    DistributedShardStatus {
                        shard_id: 1,
                        frontier_iteration: 2,
                        delta_is_empty: true,
                        iteration_cost: 10,
                    },
                    DistributedShardStatus {
                        shard_id: 2,
                        frontier_iteration: 2,
                        delta_is_empty: true,
                        iteration_cost: 100,
                    },
                ],
                1,
            )
            .unwrap();
        assert_eq!(op.strategy_for_shard(2), Some(RecursionStrategy::Recompute));
        assert!(op
            .audit_events()
            .iter()
            .any(|event| event.action == "recursion.strategy_fallback"));
    }

    #[test]
    fn recursion_non_monotone_recompute_matches_expected() {
        let op = RecursionOp::new(schema_edges(), base_plan(), non_monotone_step(), 16, false);
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        assert!(closure_rows(&out).contains_key(&(1, 3)));
    }
}
