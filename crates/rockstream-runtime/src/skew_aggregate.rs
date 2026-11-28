//! Epoch-local distributed partial aggregation with executable ADR 0004 admission.
//!
//! Partial workers own no query-visible aggregate state: they reduce the
//! admitted operands of one epoch. The original group owner applies all partial
//! deltas in one staged transition. Promotion/demotion therefore never moves
//! historical state, and signed retractions need not return to an old salt.

use std::collections::{BTreeMap, BTreeSet};

use rockstream_plan::{AggregateFunc, Expr, PlanNode};
use rockstream_types::data_plane::{
    DeploymentDescriptor, DeploymentSchema, RuntimeRow, SourceDeltaRequest,
    WorkloadTopologySnapshot,
};
use rockstream_types::ids::ShardId;
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const HOT_KEY_FACTOR: u64 = 20;
pub const MIN_HEAVY_ROWS: u64 = 64;
pub const PROMOTION_EPOCHS: u32 = 3;
pub const MAX_PARTIAL_BUCKETS: usize = 16;
const MAX_TRACKED_KEYS: usize = 1024;
const MAX_TASK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateSpec {
    pub source: String,
    pub key_column: usize,
    pub value_column: Option<usize>,
    pub literal_value: Option<i64>,
    pub column_count: usize,
    pub function: AggregateFunc,
}

fn aggregate_node(plan: &PlanNode) -> Option<&PlanNode> {
    match plan {
        PlanNode::ViewSink { child, .. } | PlanNode::Exchange { child, .. } => {
            aggregate_node(child)
        }
        PlanNode::Project { input, .. } => aggregate_node(input),
        node @ PlanNode::Aggregate { .. } => Some(node),
        _ => None,
    }
}

fn aggregate_source_columns(plan: &PlanNode, source: &str, width: usize) -> Option<Vec<usize>> {
    match plan {
        PlanNode::Source { name } if name == source => Some((0..width).collect()),
        PlanNode::Project { input, columns } => {
            let input_columns = aggregate_source_columns(input, source, width)?;
            columns
                .iter()
                .map(|expression| {
                    let Expr::Column(column) = expression else {
                        return None;
                    };
                    input_columns.get(*column).copied()
                })
                .collect()
        }
        PlanNode::Exchange { child, .. } => aggregate_source_columns(child, source, width),
        _ => None,
    }
}

pub fn aggregate_spec(
    plan_json: &str,
    schemas: &[DeploymentSchema],
    source: &str,
) -> Option<AggregateSpec> {
    let plan: PlanNode = serde_json::from_str(plan_json).ok()?;
    let PlanNode::Aggregate {
        input,
        group_by,
        aggregates,
    } = aggregate_node(&plan)?
    else {
        return None;
    };
    if group_by.len() != 1 || aggregates.len() != 1 {
        return None;
    }
    let Expr::Column(key_column) = group_by[0] else {
        return None;
    };
    let aggregate = &aggregates[0];
    if aggregate.distinct
        || !matches!(
            aggregate.func,
            AggregateFunc::Sum | AggregateFunc::Count | AggregateFunc::Avg
        )
    {
        return None;
    }
    let schema = schemas.iter().find(|schema| schema.relation == source)?;
    // SQL optimization can prune/reorder source columns before aggregation.
    // Partial tasks read raw source rows and final operands traverse this same
    // projection, so both columns must refer to their original source slots.
    let source_columns = aggregate_source_columns(input, source, schema.columns.len())?;
    let key_column = *source_columns.get(key_column)?;
    // Other columns are filled with zero in synthetic final operands. Requiring
    // an integer source makes that transformation type-preserving.
    if schema.columns.iter().any(|column| {
        !matches!(
            column.data_type.to_ascii_lowercase().as_str(),
            "i64" | "int64" | "bigint"
        )
    }) || key_column >= schema.columns.len()
    {
        return None;
    }
    let (value_column, literal_value) = match &aggregate.input {
        Expr::Column(column) => {
            let column = *source_columns.get(*column)?;
            if column == key_column {
                return None;
            }
            (Some(column), None)
        }
        Expr::Literal(bytes) if bytes.len() == 8 && aggregate.func == AggregateFunc::Count => (
            None,
            Some(i64::from_be_bytes(bytes.as_slice().try_into().ok()?)),
        ),
        _ => return None,
    };
    Some(AggregateSpec {
        source: source.to_owned(),
        key_column,
        value_column,
        literal_value,
        column_count: schema.columns.len(),
        function: aggregate.func,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
struct AggregateRoutingState {
    kind: String,
    spec: Option<AggregateSpec>,
    candidates: BTreeMap<i64, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateEpochPlan {
    pub kind: String,
    pub spec: AggregateSpec,
    pub heavy_keys: BTreeSet<i64>,
    pub bucket_shards: Vec<ShardId>,
    pub bucket_input_rows: BTreeMap<ShardId, u64>,
    pub hot_key_factor: u64,
    pub promotion_epochs: u32,
    pub single_owner_fallback_keys: Vec<i64>,
}

pub struct AggregateEpoch {
    pub plan: Value,
    pub next_state: Value,
}

#[derive(Default)]
struct KeyBudget {
    rows: u64,
    sum_abs: u128,
    count_abs: u128,
    admitted: bool,
}

fn values(spec: &AggregateSpec, row: &RuntimeRow) -> Result<(i64, i64), String> {
    let fields: Vec<_> = row.values_tsv.split('\t').collect();
    if fields.len() != spec.column_count {
        return Err("aggregate source row width mismatch".to_owned());
    }
    let key = fields[spec.key_column]
        .parse::<i64>()
        .map_err(|_| "aggregate key requires non-null i64".to_owned())?;
    if fields[spec.key_column] != key.to_string() {
        return Err("aggregate routing key requires canonical integer encoding".to_owned());
    }
    let value = match spec.value_column {
        Some(column) => fields[column]
            .parse::<i64>()
            .map_err(|_| "aggregate value requires non-null i64".to_owned())?,
        None => spec
            .literal_value
            .ok_or("aggregate literal is unavailable")?,
    };
    Ok((key, value))
}

pub fn prepare_aggregate_epoch(
    previous: Option<&Value>,
    request: &SourceDeltaRequest,
    topology: &WorkloadTopologySnapshot,
) -> Result<Option<AggregateEpoch>, String> {
    let Some(spec) = aggregate_spec(&topology.plan_json, &topology.schemas, &request.source) else {
        return Ok(None);
    };
    // Preserve historical ownership. A manually deployed plan can carry a
    // source routing column that differs from its grouping column; selecting a
    // new canonical owner would require moving its existing aggregate state.
    if topology.routing_columns.get(&request.source).copied() != Some(spec.key_column) {
        return Ok(None);
    }
    let mut budgets = BTreeMap::<i64, KeyBudget>::new();
    for row in &request.rows {
        // NULLs and unsupported runtime values retain the canonical compiler
        // semantics rather than guessing a synthetic sufficient statistic.
        let Ok((key, value)) = values(&spec, row) else {
            return Ok(None);
        };
        if !budgets.contains_key(&key) && budgets.len() >= MAX_TRACKED_KEYS {
            return Ok(None);
        }
        let budget = budgets.entry(key).or_insert_with(|| KeyBudget {
            admitted: true,
            ..Default::default()
        });
        budget.rows = budget.rows.saturating_add(1);
        budget.count_abs = budget
            .count_abs
            .saturating_add(u128::from(row.weight.unsigned_abs()));
        match value.checked_mul(row.weight) {
            Some(value) => {
                budget.sum_abs = budget
                    .sum_abs
                    .saturating_add(u128::from(value.unsigned_abs()))
            }
            None if matches!(spec.function, AggregateFunc::Sum | AggregateFunc::Avg) => {
                // This is invalid even at the canonical owner. Reject before
                // preparing a durable distributed epoch or advancing any
                // empty participant, preserving the last readable snapshot.
                return Err("RS-0001: aggregate per-row multiplication overflow".to_owned());
            }
            None => budget.admitted = false,
        }
        budget.admitted &=
            budget.sum_abs <= i64::MAX as u128 && budget.count_abs <= i64::MAX as u128;
    }
    let mut previous: AggregateRoutingState = previous
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    if previous.spec.as_ref() != Some(&spec) {
        previous = AggregateRoutingState::default();
    }
    let mut counts: Vec<_> = budgets.values().map(|budget| budget.rows).collect();
    counts.sort_unstable();
    let median = counts
        .get(counts.len().saturating_sub(1) / 2)
        .copied()
        .unwrap_or(0);
    let mut candidates = BTreeMap::new();
    let mut heavy_keys = BTreeSet::new();
    let mut fallback = Vec::new();
    for (&key, budget) in &budgets {
        let heavy = budget.rows >= MIN_HEAVY_ROWS
            && median > 0
            && budget.rows > median.saturating_mul(HOT_KEY_FACTOR);
        if !heavy {
            continue;
        }
        let consecutive = previous
            .candidates
            .get(&key)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        candidates.insert(key, consecutive.min(PROMOTION_EPOCHS));
        if consecutive >= PROMOTION_EPOCHS {
            if budget.admitted {
                heavy_keys.insert(key);
            } else {
                fallback.push(key);
            }
        }
    }
    let mut placements = topology.shards.iter().collect::<Vec<_>>();
    placements.sort_by_key(|shard| (shard.worker_id, shard.shard_id));
    placements.dedup_by_key(|shard| shard.worker_id);
    placements.truncate(MAX_PARTIAL_BUCKETS);
    let mut bucket_shards = placements
        .into_iter()
        .map(|shard| shard.shard_id)
        .collect::<Vec<_>>();
    bucket_shards.sort();
    if bucket_shards.len() < 2 {
        heavy_keys.clear();
    }
    let mut plan = AggregateEpochPlan {
        kind: "aggregate".to_owned(),
        spec: spec.clone(),
        heavy_keys,
        bucket_shards,
        bucket_input_rows: BTreeMap::new(),
        hot_key_factor: HOT_KEY_FACTOR,
        promotion_epochs: PROMOTION_EPOCHS,
        single_owner_fallback_keys: fallback,
    };
    for (index, row) in request.rows.iter().enumerate() {
        if plan.heavy_keys.contains(&values(&spec, row)?.0) {
            let shard = plan.bucket_shards[index % plan.bucket_shards.len()];
            *plan.bucket_input_rows.entry(shard).or_default() += 1;
        }
    }
    let next = AggregateRoutingState {
        kind: "aggregate".to_owned(),
        spec: Some(spec),
        candidates,
    };
    Ok(Some(AggregateEpoch {
        plan: serde_json::to_value(plan).map_err(|error| error.to_string())?,
        next_state: serde_json::to_value(next).map_err(|error| error.to_string())?,
    }))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartialAggregateTask {
    pub spec: AggregateSpec,
    pub rows: Vec<RuntimeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartialAggregateResult {
    pub input_rows: u64,
    pub deltas: BTreeMap<i64, (i64, i64)>,
}

/// Checked pure partial kernel. The receiver independently verifies plan shape
/// and arithmetic; a forged or stale command cannot mutate operator state.
pub fn compute_partial(
    descriptor: &DeploymentDescriptor,
    task: PartialAggregateTask,
) -> Result<PartialAggregateResult, String> {
    if aggregate_spec(
        &descriptor.plan_json,
        &descriptor.schemas,
        &task.spec.source,
    )
    .as_ref()
        != Some(&task.spec)
    {
        return Err("partial aggregate specification does not match deployed plan".to_owned());
    }
    let mut deltas = BTreeMap::<i64, (i64, i64)>::new();
    let mut budgets = BTreeMap::<i64, (u128, u128)>::new();
    for row in &task.rows {
        let (key, value) = values(&task.spec, row)?;
        let sum = value
            .checked_mul(row.weight)
            .ok_or("partial aggregate multiplication overflow")?;
        let budget = budgets.entry(key).or_default();
        budget.0 = budget
            .0
            .checked_add(u128::from(sum.unsigned_abs()))
            .ok_or("partial sum budget overflow")?;
        budget.1 = budget
            .1
            .checked_add(u128::from(row.weight.unsigned_abs()))
            .ok_or("partial count budget overflow")?;
        if budget.0 > i64::MAX as u128 || budget.1 > i64::MAX as u128 {
            return Err("partial aggregate lacks ADR 0004 operand admission".to_owned());
        }
        let entry = deltas.entry(key).or_default();
        entry.0 = entry.0.checked_add(sum).ok_or("partial sum overflow")?;
        entry.1 = entry
            .1
            .checked_add(row.weight)
            .ok_or("partial count overflow")?;
    }
    deltas.retain(|_, delta| *delta != (0, 0));
    Ok(PartialAggregateResult {
        input_rows: task.rows.len() as u64,
        deltas,
    })
}

pub fn canonical_shard(key: &str, shards: &[ShardId]) -> Result<ShardId, String> {
    rendezvous_hash(fnv1a_64(key.as_bytes()) % 4096, shards, 1)
        .ok_or_else(|| "no canonical aggregate shard".to_owned())
}

pub struct AggregateDispatch {
    pub raw: BTreeMap<ShardId, Vec<RuntimeRow>>,
    pub tasks: Vec<(ShardId, PartialAggregateTask)>,
}

pub fn dispatch_aggregate(
    plan: &Value,
    request: &SourceDeltaRequest,
    shards: &[ShardId],
) -> Result<AggregateDispatch, String> {
    let plan: AggregateEpochPlan =
        serde_json::from_value(plan.clone()).map_err(|error| error.to_string())?;
    if plan.kind != "aggregate"
        || plan.spec.source != request.source
        || plan
            .bucket_shards
            .iter()
            .any(|shard| !shards.contains(shard))
        || (!plan.heavy_keys.is_empty() && plan.bucket_shards.len() < 2)
    {
        return Err("invalid aggregate routing plan".to_owned());
    }
    let mut raw = BTreeMap::<ShardId, Vec<RuntimeRow>>::new();
    let mut tasks = Vec::new();
    let mut pending = BTreeMap::<ShardId, (usize, Vec<RuntimeRow>)>::new();
    for (index, row) in request.rows.iter().enumerate() {
        let (key, _) = values(&plan.spec, row)?;
        if plan.heavy_keys.contains(&key) {
            let shard = plan.bucket_shards[index % plan.bucket_shards.len()];
            let entry = pending.entry(shard).or_default();
            let bytes = row.values_tsv.len().saturating_mul(2) + 128;
            if bytes > MAX_TASK_BYTES {
                return Err("partial aggregate row exceeds task budget".to_owned());
            }
            if entry.0 + bytes > MAX_TASK_BYTES && !entry.1.is_empty() {
                tasks.push((
                    shard,
                    PartialAggregateTask {
                        spec: plan.spec.clone(),
                        rows: std::mem::take(&mut entry.1),
                    },
                ));
                entry.0 = 0;
            }
            entry.0 += bytes;
            entry.1.push(row.clone());
        } else {
            let key = row
                .values_tsv
                .split('\t')
                .nth(plan.spec.key_column)
                .ok_or("missing aggregate key")?;
            raw.entry(canonical_shard(key, shards)?)
                .or_default()
                .push(row.clone());
        }
    }
    for (shard, (_, rows)) in pending {
        if !rows.is_empty() {
            tasks.push((
                shard,
                PartialAggregateTask {
                    spec: plan.spec.clone(),
                    rows,
                },
            ));
        }
    }
    Ok(AggregateDispatch { raw, tasks })
}

/// Re-encode a sufficient statistic as at most two canonical source operands.
/// The original compiled pipeline remains the single authority for state and
/// output. Sum, count and average observe exactly the same staged transition.
pub fn append_final_operands(
    plan: &Value,
    result: PartialAggregateResult,
    shards: &[ShardId],
    raw: &mut BTreeMap<ShardId, Vec<RuntimeRow>>,
) -> Result<(), String> {
    let plan: AggregateEpochPlan =
        serde_json::from_value(plan.clone()).map_err(|error| error.to_string())?;
    for (key, (sum, count)) in result.deltas {
        if !plan.heavy_keys.contains(&key) {
            return Err("partial returned a non-heavy key".to_owned());
        }
        let mut fields = vec!["0".to_owned(); plan.spec.column_count];
        fields[plan.spec.key_column] = key.to_string();
        let output = raw
            .entry(canonical_shard(&key.to_string(), shards)?)
            .or_default();
        match plan.spec.value_column {
            Some(column) => {
                // Preserve the count operand's direction. Always using +1
                // would expand the absolute budget of an all-negative epoch.
                let first_weight = if count < 0 { -1 } else { 1 };
                fields[column] = sum
                    .checked_mul(first_weight)
                    .ok_or("partial sum inverse overflow")?
                    .to_string();
                output.push(RuntimeRow {
                    values_tsv: fields.join("\t"),
                    weight: first_weight,
                });
                fields[column] = "0".to_owned();
                let remaining = count
                    .checked_sub(first_weight)
                    .ok_or("partial count inverse overflow")?;
                if remaining != 0 {
                    output.push(RuntimeRow {
                        values_tsv: fields.join("\t"),
                        weight: remaining,
                    });
                }
            }
            None => {
                if count != 0 {
                    output.push(RuntimeRow {
                        values_tsv: fields.join("\t"),
                        weight: count,
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::AggregateExpr;
    use rockstream_types::data_plane::{DeploymentColumn, DeploymentRequest};
    use rockstream_types::ids::{OperatorId, WorkerId, WorkloadId};
    use rockstream_types::lease::ShardLease;

    fn descriptor() -> DeploymentDescriptor {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source { name: "t".into() }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        DeploymentDescriptor::new(
            DeploymentRequest {
                version: 2,
                workload_id: WorkloadId(1),
                namespace: "n".into(),
                workload_name: "w".into(),
                plan_json: serde_json::to_string(&plan).unwrap(),
                join_strategy: Default::default(),
                schemas: vec![DeploymentSchema {
                    relation: "t".into(),
                    columns: vec![
                        DeploymentColumn {
                            name: "k".into(),
                            data_type: "i64".into(),
                        },
                        DeploymentColumn {
                            name: "v".into(),
                            data_type: "i64".into(),
                        },
                    ],
                }],
                frontier: 0,
                storage_root: "root".into(),
                sink_operator_id: OperatorId(1),
                output_columns: vec!["k".into(), "s".into()],
                primary_key: vec![0],
                merge_key_columns: vec![0],
                routing_columns: BTreeMap::from([("t".into(), 0)]),
            },
            ShardLease::new(
                ShardId(1),
                WorkerId(1),
                rockstream_types::ids::LeaseToken(1),
            ),
            "s".into(),
        )
    }

    fn projected_descriptor(function: AggregateFunc, count_star: bool) -> DeploymentDescriptor {
        let mut descriptor = descriptor();
        descriptor.schemas[0].columns.insert(
            0,
            DeploymentColumn {
                name: "unused".into(),
                data_type: "i64".into(),
            },
        );
        descriptor.routing_columns.insert("t".into(), 1);
        let input = PlanNode::Project {
            input: Box::new(PlanNode::Exchange {
                kind: rockstream_plan::ExchangeKind::Hash,
                partition_key: vec![2],
                child: Box::new(PlanNode::Project {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    columns: vec![Expr::Column(2), Expr::Column(0), Expr::Column(1)],
                }),
            }),
            columns: vec![Expr::Column(2), Expr::Column(0)],
        };
        descriptor.plan_json = serde_json::to_string(&PlanNode::ViewSink {
            view_name: "projected".into(),
            pk: vec![0],
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(input),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: function,
                    input: if count_star {
                        Expr::Literal(1i64.to_be_bytes().to_vec())
                    } else {
                        Expr::Column(1)
                    },
                    distinct: false,
                }],
            }),
        })
        .unwrap();
        descriptor
    }

    #[tokio::test]
    async fn projected_sum_count_and_average_preserve_raw_schema_and_exact_retractions() {
        let db = std::sync::Arc::new(
            rockstream_storage::ShardDb::builder(
                "projected-aggregate",
                std::sync::Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        for (function, count_star) in [
            (AggregateFunc::Sum, false),
            (AggregateFunc::Count, false),
            (AggregateFunc::Avg, false),
            (AggregateFunc::Count, true),
        ] {
            let descriptor = projected_descriptor(function, count_star);
            let spec = aggregate_spec(&descriptor.plan_json, &descriptor.schemas, "t").unwrap();
            assert_eq!(
                spec,
                AggregateSpec {
                    source: "t".into(),
                    key_column: 1,
                    value_column: (!count_star).then_some(2),
                    literal_value: count_star.then_some(1),
                    column_count: 3,
                    function,
                }
            );
            let schemas =
                crate::client::deployment_schemas_from_slice(&descriptor.schemas).unwrap();
            let plan: PlanNode = serde_json::from_str(&descriptor.plan_json).unwrap();
            let oracle = rockstream_ops::compile_plan_with_sink_id(
                &plan,
                db.clone(),
                &schemas,
                OperatorId(11),
            )
            .unwrap();
            let final_owner = rockstream_ops::compile_plan_with_sink_id(
                &plan,
                db.clone(),
                &schemas,
                OperatorId(12),
            )
            .unwrap();
            let routing = serde_json::to_value(AggregateEpochPlan {
                kind: "aggregate".into(),
                spec: spec.clone(),
                heavy_keys: BTreeSet::from([7, 9]),
                bucket_shards: vec![ShardId(1)],
                bucket_input_rows: BTreeMap::new(),
                hot_key_factor: HOT_KEY_FACTOR,
                promotion_epochs: PROMOTION_EPOCHS,
                single_owner_fallback_keys: Vec::new(),
            })
            .unwrap();
            let batches = [
                vec![("99\t7\t11", 2), ("88\t7\t5", 1), ("77\t9\t4", 1)],
                vec![("99\t7\t11", -1), ("77\t9\t4", -1)],
            ];
            for (epoch, batch) in batches.into_iter().enumerate() {
                let rows = batch
                    .into_iter()
                    .map(|(value, weight)| RuntimeRow {
                        values_tsv: value.into(),
                        weight,
                    })
                    .collect::<Vec<_>>();
                let expected = oracle
                    .pipeline
                    .process(crate::client::rows_to_zset(&rows, schemas["t"].clone()).unwrap())
                    .unwrap();
                let partial = compute_partial(
                    &descriptor,
                    PartialAggregateTask {
                        spec: spec.clone(),
                        rows,
                    },
                )
                .unwrap();
                let mut routed = BTreeMap::new();
                append_final_operands(&routing, partial, &[ShardId(1)], &mut routed).unwrap();
                let synthetic = routed.remove(&ShardId(1)).unwrap();
                let expected_operands: Vec<(&str, i64)> = match (count_star, epoch) {
                    (true, 0) => vec![("0\t7\t0", 3), ("0\t9\t0", 1)],
                    (true, _) => vec![("0\t7\t0", -1), ("0\t9\t0", -1)],
                    (false, 0) => vec![("0\t7\t27", 1), ("0\t7\t0", 2), ("0\t9\t4", 1)],
                    (false, _) => vec![("0\t7\t11", -1), ("0\t9\t4", -1)],
                };
                assert_eq!(
                    synthetic,
                    expected_operands
                        .into_iter()
                        .map(|(value, weight)| RuntimeRow {
                            values_tsv: value.into(),
                            weight
                        })
                        .collect::<Vec<_>>()
                );
                let actual = final_owner
                    .pipeline
                    .process(crate::client::rows_to_zset(&synthetic, schemas["t"].clone()).unwrap())
                    .unwrap();
                assert_eq!(
                    sorted_rows(&actual),
                    sorted_rows(&expected),
                    "{function:?}, epoch {epoch}"
                );
                let expected_rows: Vec<(&str, i64)> = match (function, epoch) {
                    (AggregateFunc::Sum, 0) => vec![("7\t27", 1), ("9\t4", 1)],
                    (AggregateFunc::Sum, _) => vec![("7\t16", 1), ("7\t27", -1), ("9\t4", -1)],
                    (AggregateFunc::Count, 0) => vec![("7\t3", 1), ("9\t1", 1)],
                    (AggregateFunc::Count, _) => vec![("7\t2", 1), ("7\t3", -1), ("9\t1", -1)],
                    (AggregateFunc::Avg, 0) => vec![("7\t9", 1), ("9\t4", 1)],
                    (AggregateFunc::Avg, _) => vec![("7\t8", 1), ("7\t9", -1), ("9\t4", -1)],
                    _ => unreachable!(),
                };
                assert_eq!(
                    sorted_rows(&actual),
                    expected_rows
                        .into_iter()
                        .map(|(row, weight)| (row.into(), weight))
                        .collect::<Vec<_>>()
                );
            }
        }
        db.as_ref().clone().close().await.unwrap();
    }

    #[test]
    fn projected_aggregate_eligibility_requires_only_valid_column_transforms_and_raw_ownership() {
        let descriptor = projected_descriptor(AggregateFunc::Sum, false);
        let source = PlanNode::Source { name: "t".into() };
        let inputs = [
            PlanNode::Project {
                input: Box::new(source.clone()),
                columns: vec![Expr::Column(1), Expr::Literal(0i64.to_be_bytes().to_vec())],
            },
            PlanNode::Project {
                input: Box::new(source.clone()),
                columns: vec![Expr::Column(1), Expr::Column(3)],
            },
            PlanNode::Project {
                input: Box::new(source),
                columns: vec![Expr::Column(1), Expr::Column(1)],
            },
        ];
        for input in inputs {
            let plan = PlanNode::Aggregate {
                input: Box::new(input),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Sum,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            };
            assert_eq!(
                aggregate_spec(
                    &serde_json::to_string(&plan).unwrap(),
                    &descriptor.schemas,
                    "t"
                ),
                None
            );
        }
        let mut topology = topology(&descriptor);
        let request = SourceDeltaRequest {
            version: 2,
            request_id: "projected".into(),
            workload_id: descriptor.workload_id,
            epoch: 1,
            source: "t".into(),
            rows: vec![RuntimeRow {
                values_tsv: "999\t7\t11".into(),
                weight: 1,
            }],
        };
        let planned = prepare_aggregate_epoch(None, &request, &topology)
            .unwrap()
            .unwrap();
        let dispatch = dispatch_aggregate(
            &planned.plan,
            &request,
            &crate::skew_routing::stable_shards(&topology),
        )
        .unwrap();
        assert_eq!(
            dispatch.raw,
            BTreeMap::from([(
                canonical_shard("7", &crate::skew_routing::stable_shards(&topology)).unwrap(),
                request.rows.clone()
            )])
        );
        topology.routing_columns.insert("t".into(), 0);
        assert!(prepare_aggregate_epoch(None, &request, &topology)
            .unwrap()
            .is_none());
    }

    #[test]
    fn signed_partials_preserve_exact_epoch_operands_and_reject_unadmitted_budget() {
        let descriptor = descriptor();
        let spec = aggregate_spec(&descriptor.plan_json, &descriptor.schemas, "t").unwrap();
        let result = compute_partial(
            &descriptor,
            PartialAggregateTask {
                spec: spec.clone(),
                rows: vec![
                    RuntimeRow {
                        values_tsv: "7\t11".into(),
                        weight: 2,
                    },
                    RuntimeRow {
                        values_tsv: "7\t4".into(),
                        weight: -1,
                    },
                    RuntimeRow {
                        values_tsv: "9\t3".into(),
                        weight: 1,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(
            result,
            PartialAggregateResult {
                input_rows: 3,
                deltas: BTreeMap::from([(7, (18, 1)), (9, (3, 1))])
            }
        );
        let invalid = compute_partial(
            &descriptor,
            PartialAggregateTask {
                spec,
                rows: vec![
                    RuntimeRow {
                        values_tsv: format!("7\t{}", i64::MAX),
                        weight: 1,
                    },
                    RuntimeRow {
                        values_tsv: format!("7\t{}", -i64::MAX),
                        weight: 1,
                    },
                ],
            },
        )
        .unwrap_err();
        assert_eq!(
            invalid,
            "partial aggregate lacks ADR 0004 operand admission"
        );
    }

    fn topology(descriptor: &DeploymentDescriptor) -> WorkloadTopologySnapshot {
        WorkloadTopologySnapshot {
            topology_epoch: 1,
            workload_id: descriptor.workload_id,
            plan_json: descriptor.plan_json.clone(),
            routing_columns: descriptor.routing_columns.clone(),
            schemas: descriptor.schemas.clone(),
            shards: (1..=4)
                .map(|id| rockstream_types::data_plane::WorkloadShardPlacement {
                    shard_id: ShardId(id),
                    shard_index: (id - 1) as u32,
                    worker_id: WorkerId(id),
                    worker_address: format!("127.0.0.1:{}", 9000 + id),
                    lease_token: rockstream_types::ids::LeaseToken(id),
                    operator_id: descriptor.sink_operator_id,
                    storage_identity: format!("shard-{id}"),
                    frontier: 0,
                })
                .collect(),
            workers: Vec::new(),
        }
    }

    fn sorted_rows(delta: &rockstream_ops::zset::ArrowZSet) -> Vec<(String, i64)> {
        let mut rows = crate::client::zset_to_rows(delta)
            .unwrap()
            .into_iter()
            .map(|row| (row.values_tsv, row.weight))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn promotion_uses_four_workers_and_canonical_epoch_output_matches_unsplit_retractions() {
        let descriptor = descriptor();
        let topology = topology(&descriptor);
        let shards = crate::skew_routing::stable_shards(&topology);
        let schema =
            crate::client::deployment_schemas_from_slice(&descriptor.schemas).unwrap()["t"].clone();
        let mut oracle = rockstream_ops::aggregate::AggregateStateCore::new(OperatorId(19));
        let mut owners = shards
            .iter()
            .map(|shard| {
                (
                    *shard,
                    rockstream_ops::aggregate::AggregateStateCore::new(OperatorId(19)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut previous = Value::Null;
        for epoch in 1..=4 {
            let mut rows = (0..1600)
                .map(|_| RuntimeRow {
                    values_tsv: "7\t5".into(),
                    weight: if epoch <= 2 { 1 } else { -1 },
                })
                .collect::<Vec<_>>();
            for key in 20..28 {
                rows.extend((0..32).map(|_| RuntimeRow {
                    values_tsv: format!("{key}\t3"),
                    weight: 1,
                }));
            }
            let request = SourceDeltaRequest {
                version: 2,
                workload_id: descriptor.workload_id,
                request_id: format!("r{epoch}"),
                epoch,
                source: "t".into(),
                rows,
            };
            let planned = prepare_aggregate_epoch(Some(&previous), &request, &topology)
                .unwrap()
                .unwrap();
            let plan: AggregateEpochPlan = serde_json::from_value(planned.plan.clone()).unwrap();
            if epoch >= 3 {
                assert_eq!(plan.heavy_keys, BTreeSet::from([7]));
                assert_eq!(
                    plan.bucket_input_rows,
                    BTreeMap::from([
                        (ShardId(1), 400),
                        (ShardId(2), 400),
                        (ShardId(3), 400),
                        (ShardId(4), 400)
                    ])
                );
            } else {
                assert_eq!(plan.heavy_keys, BTreeSet::new());
                assert_eq!(plan.bucket_input_rows, BTreeMap::new());
            }
            let expected = oracle
                .process_delta_mut(
                    crate::client::rows_to_zset(&request.rows, schema.clone()).unwrap(),
                )
                .unwrap();
            let mut dispatch = dispatch_aggregate(&planned.plan, &request, &shards).unwrap();
            for (_, task) in dispatch.tasks {
                let result = compute_partial(&descriptor, task).unwrap();
                append_final_operands(&planned.plan, result, &shards, &mut dispatch.raw).unwrap();
            }
            let mut actual = Vec::new();
            for (shard, rows) in dispatch.raw {
                let output = owners
                    .get_mut(&shard)
                    .unwrap()
                    .process_delta_mut(crate::client::rows_to_zset(&rows, schema.clone()).unwrap())
                    .unwrap();
                actual.extend(sorted_rows(&output));
            }
            actual.sort();
            assert_eq!(actual, sorted_rows(&expected), "epoch {epoch}");
            previous = planned.next_state;
        }
    }

    #[test]
    fn inadmissible_hot_key_stays_on_canonical_owner_and_is_explained() {
        let descriptor = descriptor();
        let topology = topology(&descriptor);
        let spec = aggregate_spec(&descriptor.plan_json, &descriptor.schemas, "t").unwrap();
        let previous = serde_json::to_value(AggregateRoutingState {
            kind: "aggregate".into(),
            spec: Some(spec),
            candidates: BTreeMap::from([(7, 2)]),
        })
        .unwrap();
        let mut rows = vec![
            RuntimeRow {
                values_tsv: format!("7\t{}", i64::MAX),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: format!("7\t{}", -i64::MAX),
                weight: 1,
            },
        ];
        rows.extend((0..98).map(|_| RuntimeRow {
            values_tsv: "7\t0".into(),
            weight: 1,
        }));
        for key in 20..28 {
            rows.push(RuntimeRow {
                values_tsv: format!("{key}\t1"),
                weight: 1,
            });
        }
        let request = SourceDeltaRequest {
            version: 2,
            workload_id: descriptor.workload_id,
            request_id: "r3".into(),
            epoch: 3,
            source: "t".into(),
            rows,
        };
        let planned = prepare_aggregate_epoch(Some(&previous), &request, &topology)
            .unwrap()
            .unwrap();
        let plan: AggregateEpochPlan = serde_json::from_value(planned.plan.clone()).unwrap();
        assert_eq!(plan.single_owner_fallback_keys, vec![7]);
        assert_eq!(plan.heavy_keys, BTreeSet::new());
        let dispatch = dispatch_aggregate(
            &planned.plan,
            &request,
            &crate::skew_routing::stable_shards(&topology),
        )
        .unwrap();
        assert_eq!(dispatch.tasks, Vec::new());
        let mut actual = dispatch
            .raw
            .into_values()
            .flatten()
            .map(|row| (row.values_tsv, row.weight))
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = request
            .rows
            .into_iter()
            .map(|row| (row.values_tsv, row.weight))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn negative_synthetic_operands_do_not_expand_the_admitted_count_budget() {
        let descriptor = descriptor();
        let spec = aggregate_spec(&descriptor.plan_json, &descriptor.schemas, "t").unwrap();
        let plan = serde_json::to_value(AggregateEpochPlan {
            kind: "aggregate".into(),
            spec,
            heavy_keys: BTreeSet::from([7]),
            bucket_shards: vec![ShardId(1), ShardId(2)],
            bucket_input_rows: BTreeMap::new(),
            hot_key_factor: 20,
            promotion_epochs: 3,
            single_owner_fallback_keys: Vec::new(),
        })
        .unwrap();
        let mut raw = BTreeMap::new();
        append_final_operands(
            &plan,
            PartialAggregateResult {
                input_rows: 1,
                deltas: BTreeMap::from([(7, (-17, -i64::MAX))]),
            },
            &[ShardId(1)],
            &mut raw,
        )
        .unwrap();
        assert_eq!(
            raw,
            BTreeMap::from([(
                ShardId(1),
                vec![
                    RuntimeRow {
                        values_tsv: "7\t17".into(),
                        weight: -1
                    },
                    RuntimeRow {
                        values_tsv: "7\t0".into(),
                        weight: -i64::MAX + 1
                    },
                ]
            )])
        );
    }

    #[test]
    fn aggregate_routing_retains_existing_source_ownership_when_grouping_differs() {
        let descriptor = descriptor();
        let mut topology = topology(&descriptor);
        topology.routing_columns.insert("t".to_owned(), 1);
        let request = SourceDeltaRequest {
            version: 2,
            workload_id: descriptor.workload_id,
            request_id: "different-partition".to_owned(),
            epoch: 1,
            source: "t".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "7\t5".to_owned(),
                weight: 1,
            }],
        };
        let planned = prepare_aggregate_epoch(None, &request, &topology).unwrap();
        assert_eq!(planned.map(|plan| (plan.plan, plan.next_state)), None);
    }
}
