//! Epoch-staged heavy/light routing for the production owned-shuffle path.
//!
//! The anchor journal persists each affected key and each salted high-side row
//! as independent point records. The journal's prepare/finish transaction owns
//! the records; this module only proposes immutable, replayable routing plans.
//! A promotion retains historical high rows on their original shard and seeds
//! the bounded low-side snapshot on every new target in the same worker commit
//! as the first salted delta. Deletions consume salted copies before historical
//! copies, so an insertion predating promotion never needs a full-state move.

use std::collections::{BTreeMap, BTreeSet};

use rockstream_plan::{AggregateFunc, Expr, PlanNode};
use rockstream_types::data_plane::{
    JoinBootstrap, RuntimeRow, SourceDeltaRequest, WorkloadTopologySnapshot,
};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A receiver admits at most this many low-side snapshot bytes in one epoch.
pub const MAX_JOIN_BOOTSTRAP_BYTES: usize = 4 * 1024 * 1024;
// Reserve most of the bounded journal for row routes and point mutations.
// Counting every replica also bounds simultaneous promotions across keys.
const MAX_JOIN_EPOCH_SEED_BYTES: usize = MAX_JOIN_BOOTSTRAP_BYTES / 4;
/// Bound the replicated side of one key; the high-side ledger has no total
/// dataset limit because it uses independent durable point records.
pub const MAX_JOIN_LOW_SIDE_BYTES: usize = 64 * 1024;
pub const MIN_JOIN_HIGH_ROWS: i64 = 32;
pub const JOIN_HIGH_LOW_RATIO: i64 = 8;
pub const JOIN_PROMOTION_EPOCHS: u32 = 3;
pub const MAX_JOIN_BUCKETS: usize = 16;
const VIRTUAL_BUCKETS: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinInputSide {
    Left,
    Right,
}

impl JoinInputSide {
    fn opposite(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[derive(Debug, Clone)]
struct JoinSpec {
    left_source: String,
    right_source: String,
    left_key: usize,
    right_key: usize,
    value: Option<(JoinInputSide, usize)>,
    aggregate: Option<AggregateFunc>,
    merge_key_columns: Vec<usize>,
    plan_digest: String,
}

impl JoinSpec {
    fn side(&self, source: &str) -> Option<JoinInputSide> {
        if source == self.left_source {
            Some(JoinInputSide::Left)
        } else if source == self.right_source {
            Some(JoinInputSide::Right)
        } else {
            None
        }
    }

    fn key_column(&self, side: JoinInputSide) -> usize {
        match side {
            JoinInputSide::Left => self.left_key,
            JoinInputSide::Right => self.right_key,
        }
    }

    fn source(&self, side: JoinInputSide) -> &str {
        match side {
            JoinInputSide::Left => &self.left_source,
            JoinInputSide::Right => &self.right_source,
        }
    }
}

// Skew eligibility is deliberately narrower than ordinary join eligibility.
// Stateful input transforms, residual filters above the join, DISTINCT,
// non-additive aggregates, and grouping unrelated to the join key need their
// own distribution proof and retain the ordinary single-owner path.
fn source_projection(
    node: &PlanNode,
    topology: &WorkloadTopologySnapshot,
) -> Option<(String, Vec<usize>)> {
    match node {
        PlanNode::Source { name } => {
            let width = topology
                .schemas
                .iter()
                .find(|schema| schema.relation == *name)?
                .columns
                .len();
            Some((name.clone(), (0..width).collect()))
        }
        PlanNode::Project { input, columns } => {
            let (source, input) = source_projection(input, topology)?;
            let columns = columns
                .iter()
                .map(|expr| {
                    if let Expr::Column(column) = expr {
                        input.get(*column).copied()
                    } else {
                        None
                    }
                })
                .collect::<Option<Vec<_>>>()?;
            Some((source, columns))
        }
        PlanNode::Exchange { child, .. } => source_projection(child, topology),
        _ => None,
    }
}

/// Resolve a column-only projection stack to its join or aggregate. Optimized
/// SQL can prune/reorder columns between the join and its consuming aggregate,
/// as well as at either source and at the final output.
fn projected_operator<'a>(
    node: &'a PlanNode,
    topology: &WorkloadTopologySnapshot,
) -> Option<(&'a PlanNode, Vec<usize>)> {
    match node {
        PlanNode::Project { input, columns } => {
            let (operator, input_columns) = projected_operator(input, topology)?;
            let columns = columns
                .iter()
                .map(|expr| match expr {
                    Expr::Column(column) => input_columns.get(*column).copied(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            Some((operator, columns))
        }
        PlanNode::Exchange { child, .. } => projected_operator(child, topology),
        PlanNode::Aggregate {
            group_by,
            aggregates,
            ..
        } => Some((node, (0..group_by.len() + aggregates.len()).collect())),
        PlanNode::InnerJoin { left, right, .. } => {
            let (_, left) = source_projection(left, topology)?;
            let (_, right) = source_projection(right, topology)?;
            Some((node, (0..left.len() + right.len()).collect()))
        }
        _ => None,
    }
}

fn join_spec(plan_json: &str, topology: Option<&WorkloadTopologySnapshot>) -> Option<JoinSpec> {
    let topology = topology?;
    let plan: PlanNode = serde_json::from_str(plan_json).ok()?;
    let node = match &plan {
        PlanNode::ViewSink { child, .. } => child.as_ref(),
        node => node,
    };
    let (node, output_columns) = projected_operator(node, topology)?;
    let (join, join_columns, aggregate) = match node {
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } if group_by.len() == 1 && aggregates.len() == 1 => {
            let aggregate = &aggregates[0];
            if aggregate.distinct
                || !matches!(aggregate.func, AggregateFunc::Sum | AggregateFunc::Count)
            {
                return None;
            }
            let (join, columns) = projected_operator(input, topology)?;
            (join, columns, Some((&group_by[0], aggregate)))
        }
        PlanNode::InnerJoin { .. } => (node, Vec::new(), None),
        _ => return None,
    };
    let PlanNode::InnerJoin {
        left,
        right,
        left_keys,
        right_keys,
        ..
    } = join
    else {
        return None;
    };
    let (left_source, left_columns) = source_projection(left, topology)?;
    let (right_source, right_columns) = source_projection(right, topology)?;
    if left_source == right_source || left_keys.len() != 1 || right_keys.len() != 1 {
        return None;
    }
    let mut value = None;
    let left_key = *left_columns.get(left_keys[0])?;
    let right_key = *right_columns.get(right_keys[0])?;
    let mut merge_key_columns = Vec::new();
    if let Some((group, agg)) = aggregate {
        let Expr::Column(group) = group else {
            return None;
        };
        let group = *join_columns.get(*group)?;
        let left_width = left_columns.len();
        if group != left_keys[0] && group != left_width + right_keys[0] {
            return None;
        }
        if agg.func == AggregateFunc::Sum {
            let Expr::Column(column) = agg.input else {
                return None;
            };
            let column = *join_columns.get(column)?;
            value = Some(if column < left_width {
                (JoinInputSide::Left, *left_columns.get(column)?)
            } else {
                (
                    JoinInputSide::Right,
                    *right_columns.get(column - left_width)?,
                )
            });
        }
        merge_key_columns.push(0);
    }
    if aggregate.is_some() {
        // Reordering the two aggregate columns is safe; duplicating or
        // computing expressions would require another final-stage proof.
        if output_columns.len() != 2
            || output_columns.iter().copied().collect::<BTreeSet<_>>() != BTreeSet::from([0, 1])
        {
            return None;
        }
        merge_key_columns = vec![output_columns.iter().position(|column| *column == 0)?];
    }
    for (source, key) in [(&left_source, left_key), (&right_source, right_key)] {
        let schema = topology
            .schemas
            .iter()
            .find(|schema| schema.relation == *source)?;
        // Integer keys have one canonical equality/serialization contract
        // in both the ordinary and factorized operator implementations.
        let data_type = &schema.columns.get(key)?.data_type;
        if !matches!(
            data_type.to_ascii_lowercase().as_str(),
            "i64" | "int64" | "bigint"
        ) {
            return None;
        }
    }
    Some(JoinSpec {
        left_source: left_source.clone(),
        right_source: right_source.clone(),
        left_key,
        right_key,
        value,
        aggregate: aggregate.map(|(_, aggregate)| aggregate.func),
        merge_key_columns,
        plan_digest: blake3::hash(plan_json.as_bytes()).to_hex().to_string(),
    })
}

/// Final SUM/COUNT columns must be combined by the query-visible group.
/// Uses deployed schemas to resolve columns on either join input.
pub fn join_merge_key_columns(
    plan_json: &str,
    topology: &WorkloadTopologySnapshot,
) -> Option<Vec<usize>> {
    join_spec(plan_json, Some(topology)).map(|spec| spec.merge_key_columns)
}

pub fn validate_join_final_rows(
    descriptor: &rockstream_types::data_plane::DeploymentDescriptor,
    rows: &[RuntimeRow],
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let topology = WorkloadTopologySnapshot {
        topology_epoch: 0,
        workload_id: descriptor.workload_id,
        plan_json: descriptor.plan_json.clone(),
        schemas: descriptor.schemas.clone(),
        routing_columns: descriptor.routing_columns.clone(),
        shards: Vec::new(),
        workers: Vec::new(),
    };
    let spec = join_spec(&descriptor.plan_json, Some(&topology))
        .ok_or("join finalization shape is not supported")?;
    if spec.aggregate.is_none() {
        return Err("RS-1013: join finalization requires SUM or COUNT".into());
    }
    for row in rows {
        if !matches!(row.weight, -1 | 1) || row.values_tsv.split('\t').count() != 2 {
            return Err("RS-1002: invalid canonical join group transition".into());
        }
        for column in row.values_tsv.split('\t') {
            let value: i64 = column
                .parse()
                .map_err(|_| "invalid canonical join scalar")?;
            if value.to_string() != column {
                return Err("RS-1002: noncanonical join final scalar".into());
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SideState {
    live_weight: i64,
    abs_value_sum: u64,
    budget_inexact: bool,
    snapshot_incomplete: bool,
    rows: BTreeMap<String, i64>,
    #[serde(default = "zero_sum")]
    signed_value_sum: String,
}

fn zero_sum() -> String {
    "0".into()
}

fn signed_sum(side: &SideState) -> Result<i128, String> {
    if side.signed_value_sum.is_empty() {
        return Ok(0);
    }
    side.signed_value_sum
        .parse()
        .map_err(|_| "invalid durable join sum".into())
}

impl SideState {
    fn apply(
        &mut self,
        rows: &[IndexedRow<'_>],
        value_column: Option<usize>,
    ) -> Result<(), String> {
        let mut weight_delta = 0i128;
        let mut value_delta = 0i128;
        let mut signed_delta = 0i128;
        for row in rows {
            weight_delta = weight_delta
                .checked_add(i128::from(row.weight))
                .ok_or("join key weight overflow")?;
            if let Some(column) = value_column {
                let value = row_value(row.row, column)?;
                signed_delta = signed_delta
                    .checked_add(
                        i128::from(value)
                            .checked_mul(i128::from(row.weight))
                            .ok_or("join signed contribution overflow")?,
                    )
                    .ok_or("join signed contribution overflow")?;
                value_delta = value_delta
                    .checked_add(i128::from(value.unsigned_abs()) * i128::from(row.weight))
                    .ok_or("join absolute operand budget overflow")?;
            }
            if !self.snapshot_incomplete {
                let before = self.rows.get(&row.row.values_tsv).copied().unwrap_or(0);
                let after = before
                    .checked_add(row.weight)
                    .ok_or("join row weight overflow")?;
                if after < 0 {
                    return Err("RS-1002: join deletion exceeds the live row multiplicity".into());
                }
                if after == 0 {
                    self.rows.remove(&row.row.values_tsv);
                } else {
                    self.rows.insert(row.row.values_tsv.clone(), after);
                }
                if snapshot_bytes(&self.rows)? > MAX_JOIN_LOW_SIDE_BYTES {
                    self.rows.clear();
                    self.snapshot_incomplete = true;
                }
            }
        }
        self.signed_value_sum = signed_sum(self)?
            .checked_add(signed_delta)
            .ok_or("join signed summary overflow")?
            .to_string();
        let next_weight = i128::from(self.live_weight)
            .checked_add(weight_delta)
            .ok_or("join key weight overflow")?;
        self.live_weight = i64::try_from(next_weight).map_err(|_| "join key weight overflow")?;
        if self.live_weight < 0 {
            return Err("RS-1002: join key has negative live multiplicity".into());
        }
        if !self.budget_inexact {
            let budget = i128::from(self.abs_value_sum)
                .checked_add(value_delta)
                .ok_or("join absolute operand budget overflow")?;
            if budget < 0 {
                return Err("RS-0001: join absolute operand budget is negative".into());
            }
            match u64::try_from(budget) {
                Ok(budget) => self.abs_value_sum = budget,
                Err(_) => {
                    self.budget_inexact = true;
                    self.abs_value_sum = 0;
                }
            }
        }
        if self.live_weight == 0 {
            // All row multiplicities are nonnegative; an empty key has no
            // retained arrangement or missing snapshot history.
            *self = Self::default();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PromotedKey {
    high_side: JoinInputSide,
    original_shard: ShardId,
    targets: Vec<ShardId>,
    generation: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct KeyState {
    key: String,
    left: SideState,
    right: SideState,
    candidate: Option<JoinInputSide>,
    sustained_epochs: u32,
    last_observed_epoch: Option<u64>,
    promoted: Option<PromotedKey>,
    #[serde(default)]
    partial_high: BTreeMap<ShardId, SideState>,
    #[serde(default)]
    canonical_owner: Option<ShardId>,
}

impl KeyState {
    fn side(&self, side: JoinInputSide) -> &SideState {
        match side {
            JoinInputSide::Left => &self.left,
            JoinInputSide::Right => &self.right,
        }
    }
    fn side_mut(&mut self, side: JoinInputSide) -> &mut SideState {
        match side {
            JoinInputSide::Left => &mut self.left,
            JoinInputSide::Right => &mut self.right,
        }
    }
    fn candidate(&self) -> Option<JoinInputSide> {
        [JoinInputSide::Left, JoinInputSide::Right]
            .into_iter()
            .find(|side| {
                let high = self.side(*side);
                let low = self.side(side.opposite());
                high.live_weight >= MIN_JOIN_HIGH_ROWS
                    && low.live_weight > 0
                    && high.live_weight / low.live_weight >= JOIN_HIGH_LOW_RATIO
                    && !low.snapshot_incomplete
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SaltedRow {
    row: String,
    weight: i64,
    total_weight: i64,
}

/// Only exceptional arithmetic fallback needs a paged scan. Normal salted
/// epochs use affected-row point lookups. The scan proves each joined row's
/// checked multiplication, rather than allowing wide cancellation to hide a
/// scalar overflow that the canonical operator would reject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinLedgerScan {
    pub prefix: String,
    pub value_column: Option<usize>,
}

impl JoinLedgerScan {
    pub fn result_key(&self) -> String {
        format!("{}stats", self.prefix)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JoinLedgerStats {
    max_row_weight: i64,
    min_weighted_value: String,
    max_weighted_value: String,
    total_weight: String,
}

impl JoinLedgerStats {
    pub fn observe(&mut self, value: &Value, column: Option<usize>) -> Result<(), String> {
        let row: SaltedRow =
            serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
        if row.total_weight < 0 {
            return Err("RS-0001: negative durable join row multiplicity".into());
        }
        self.max_row_weight = self.max_row_weight.max(row.total_weight);
        let total: i128 = self.total_weight.parse().unwrap_or(0);
        self.total_weight = total
            .checked_add(i128::from(row.total_weight))
            .ok_or("join ledger total overflow")?
            .to_string();
        if let Some(column) = column {
            let value = row_value(
                &RuntimeRow {
                    values_tsv: row.row,
                    weight: row.total_weight,
                },
                column,
            )?;
            let weighted = i128::from(value)
                .checked_mul(i128::from(row.total_weight))
                .ok_or("join weighted value overflow")?;
            let min: i128 = self.min_weighted_value.parse().unwrap_or(0);
            let max: i128 = self.max_weighted_value.parse().unwrap_or(0);
            self.min_weighted_value = min.min(weighted).to_string();
            self.max_weighted_value = max.max(weighted).to_string();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexedRow<'a> {
    index: usize,
    row: &'a RuntimeRow,
    weight: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowRoute {
    index: usize,
    weight: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ShardPlan {
    rows: Vec<RowRoute>,
    seed_rows: Vec<RuntimeRow>,
    bootstrap: Option<JoinBootstrap>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRoutingDiagnostic {
    pub key_digest: String,
    pub decision: String,
    pub high_rows: i64,
    pub low_rows: i64,
    pub buckets: usize,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EpochPlan {
    kind: String,
    request_digest: String,
    shards: BTreeMap<ShardId, ShardPlan>,
    diagnostics: Vec<JoinRoutingDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinShardBatch {
    pub rows: Vec<RuntimeRow>,
    pub bootstrap: Option<JoinBootstrap>,
}

#[derive(Debug)]
pub struct JoinEpoch {
    pub batches: BTreeMap<ShardId, JoinShardBatch>,
    pub plan: Value,
    pub next_state: Value,
    pub point_mutations: BTreeMap<String, Option<Value>>,
}

fn snapshot_bytes(rows: &BTreeMap<String, i64>) -> Result<usize, String> {
    rows.keys().try_fold(0usize, |bytes, row| {
        bytes
            .checked_add(row.len())
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(|| "RS-3006: join snapshot size overflow".to_string())
    })
}

fn row_value(row: &RuntimeRow, column: usize) -> Result<i64, String> {
    row.values_tsv
        .split('\t')
        .nth(column)
        .ok_or_else(|| format!("join row missing column {column}"))?
        .parse::<i64>()
        .map_err(|_| "join aggregate operand is not an integer".to_string())
}

fn key_for(row: &RuntimeRow, column: usize) -> Result<&str, String> {
    row.values_tsv
        .split('\t')
        .nth(column)
        .ok_or_else(|| format!("RS-1002: join row missing key column {column}"))
}

fn validate_row_identity(
    request: &SourceDeltaRequest,
    topology: &WorkloadTopologySnapshot,
) -> Result<(), String> {
    let schema = topology
        .schemas
        .iter()
        .find(|schema| schema.relation == request.source)
        .ok_or("join source schema is missing")?;
    for row in &request.rows {
        let fields: Vec<_> = row.values_tsv.split('\t').collect();
        if fields.len() != schema.columns.len() {
            return Err("RS-1002: join row does not match its source schema".into());
        }
        for (field, column) in fields.into_iter().zip(&schema.columns) {
            if field == "\\N" {
                continue;
            }
            if matches!(
                column.data_type.to_ascii_lowercase().as_str(),
                "i64" | "int64" | "bigint"
            ) {
                let value: i64 = field
                    .parse()
                    .map_err(|_| "join row has an invalid integer")?;
                if value.to_string() != field {
                    return Err(
                        "RS-1002: join routing requires canonical integer row identities".into(),
                    );
                }
            }
        }
    }
    Ok(())
}

fn state_key(spec: &JoinSpec, key: &str) -> String {
    format!(
        "join/v1/{}/{}/key",
        spec.plan_digest,
        blake3::hash(key.as_bytes()).to_hex()
    )
}

fn row_key(spec: &JoinSpec, key: &str, side: JoinInputSide, row: &str) -> String {
    format!(
        "{}{}",
        row_prefix(spec, key, side),
        blake3::hash(row.as_bytes()).to_hex()
    )
}

fn row_prefix(spec: &JoinSpec, key: &str, side: JoinInputSide) -> String {
    format!(
        "join/v1/{}/{}/{side:?}/",
        spec.plan_digest,
        blake3::hash(key.as_bytes()).to_hex()
    )
}

fn request_digest(request: &SourceDeltaRequest) -> Result<String, String> {
    serde_json::to_vec(request)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|error| error.to_string())
}

/// Fetch only point records touched by this epoch. No prefix scan or in-memory
/// map of the entire high-side history is required.
pub fn join_state_keys(
    request: &SourceDeltaRequest,
    topology: &WorkloadTopologySnapshot,
    plan_json: &str,
) -> Result<Vec<String>, String> {
    let Some(spec) = join_spec(plan_json, Some(topology)) else {
        return Ok(Vec::new());
    };
    let side = spec
        .side(&request.source)
        .ok_or("source is not a join input")?;
    validate_row_identity(request, topology)?;
    let mut keys = BTreeSet::new();
    for row in &request.rows {
        if row.weight == 0 {
            continue;
        }
        let key = key_for(row, spec.key_column(side))?;
        if key == "\\N" {
            continue;
        }
        keys.insert(state_key(&spec, key));
        keys.insert(row_key(&spec, key, side, &row.values_tsv));
    }
    Ok(keys.into_iter().collect())
}

fn scalar_budget(
    before: &KeyState,
    side: JoinInputSide,
    rows: &[IndexedRow<'_>],
    spec: &JoinSpec,
) -> Result<bool, String> {
    if spec.aggregate.is_none() {
        return Ok(true);
    }
    if before.left.budget_inexact || before.right.budget_inexact {
        return Ok(false);
    }
    let delta_count = rows.iter().try_fold(0u128, |total, row| {
        total
            .checked_add(u128::from(row.weight.unsigned_abs()))
            .ok_or("RS-1002: join operand budget overflow")
    })?;
    let count = |input: JoinInputSide| {
        u128::try_from(before.side(input).live_weight)
            .unwrap_or(u128::MAX)
            .saturating_add(if input == side { delta_count } else { 0 })
    };
    let Some(count_budget) = count(JoinInputSide::Left).checked_mul(count(JoinInputSide::Right))
    else {
        return Ok(false);
    };
    if count_budget > i64::MAX as u128 {
        return Ok(false);
    }
    if let Some((value_side, column)) = spec.value {
        let delta_value = if side == value_side {
            rows.iter().try_fold(0u128, |total, row| {
                let operand = u128::from(row_value(row.row, column)?.unsigned_abs())
                    .checked_mul(u128::from(row.weight.unsigned_abs()))
                    .ok_or("RS-1002: join operand budget overflow")?;
                total
                    .checked_add(operand)
                    .ok_or_else(|| "RS-1002: join operand budget overflow".to_string())
            })?
        } else {
            0
        };
        let budget = u128::from(before.side(value_side).abs_value_sum)
            .checked_add(delta_value)
            .and_then(|value| value.checked_mul(count(value_side.opposite())));
        return Ok(budget.is_some_and(|budget| budget <= i64::MAX as u128));
    }
    Ok(true)
}

fn group_request<'a>(
    request: &'a SourceDeltaRequest,
    column: usize,
) -> Result<BTreeMap<String, Vec<IndexedRow<'a>>>, String> {
    let mut grouped = BTreeMap::<String, BTreeMap<String, IndexedRow<'_>>>::new();
    for (index, row) in request.rows.iter().enumerate() {
        let key = key_for(row, column)?;
        let entry = grouped
            .entry(key.to_owned())
            .or_default()
            .entry(row.values_tsv.clone())
            .or_insert(IndexedRow {
                index,
                row,
                weight: 0,
            });
        entry.weight = entry
            .weight
            .checked_add(row.weight)
            .ok_or("join epoch multiplicity overflow")?;
    }
    Ok(grouped
        .into_iter()
        .map(|(key, rows)| {
            (
                key,
                rows.into_values().filter(|row| row.weight != 0).collect(),
            )
        })
        .collect())
}

/// Request bounded-memory scans only when a concrete arithmetic fallback
/// needs the exact opposite-side scalar extrema. No source rows leave the
/// anchor; the result is three scalar bounds per affected key.
pub fn join_fallback_scans(
    point_state: &BTreeMap<String, Value>,
    request: &SourceDeltaRequest,
    topology: &WorkloadTopologySnapshot,
    plan_json: &str,
) -> Result<Vec<JoinLedgerScan>, String> {
    let Some(spec) = join_spec(plan_json, Some(topology)) else {
        return Ok(vec![]);
    };
    if spec.aggregate.is_none() {
        return Ok(vec![]);
    }
    let side = spec
        .side(&request.source)
        .ok_or("source is not a join input")?;
    let mut scans = Vec::new();
    for (key, rows) in group_request(request, spec.key_column(side))? {
        if rows.is_empty() || key == "\\N" {
            continue;
        }
        let state: KeyState = point_state
            .get(&state_key(&spec, &key))
            .map(|value| serde_json::from_value(value.clone()).map_err(|error| error.to_string()))
            .transpose()?
            .unwrap_or_default();
        let opposite = state.side(side.opposite());
        if (state.canonical_owner.is_some() || !scalar_budget(&state, side, &rows, &spec)?)
            && opposite.live_weight > 0
            && opposite.snapshot_incomplete
        {
            scans.push(JoinLedgerScan {
                prefix: row_prefix(&spec, &key, side.opposite()),
                value_column: spec
                    .value
                    .filter(|(input, _)| *input == side.opposite())
                    .map(|(_, column)| column),
            });
        }
    }
    Ok(scans)
}

fn check_individual_join_products(
    before: &KeyState,
    rows: &[IndexedRow<'_>],
    side: JoinInputSide,
    spec: &JoinSpec,
    point_state: &BTreeMap<String, Value>,
) -> Result<(), String> {
    let opposite = before.side(side.opposite());
    if opposite.live_weight == 0 {
        return Ok(());
    }
    let opposite_value = spec
        .value
        .filter(|(input, _)| *input == side.opposite())
        .map(|(_, column)| column);
    let stats = if opposite.snapshot_incomplete {
        let result_key = format!("{}stats", row_prefix(spec, &before.key, side.opposite()));
        serde_json::from_value::<JoinLedgerStats>(
            point_state
                .get(&result_key)
                .ok_or("canonical join fallback needs an opposite-row scalar proof")?
                .clone(),
        )
        .map_err(|error| error.to_string())?
    } else {
        let mut stats = JoinLedgerStats::default();
        for (row, weight) in &opposite.rows {
            stats.observe(
                &serde_json::to_value(SaltedRow {
                    row: row.clone(),
                    weight: 0,
                    total_weight: *weight,
                })
                .map_err(|error| error.to_string())?,
                opposite_value,
            )?;
        }
        stats
    };
    if stats
        .total_weight
        .parse::<i128>()
        .map_err(|_| "invalid joined row proof total")?
        != i128::from(opposite.live_weight)
    {
        return Err(
            "RS-0001: join scalar proof does not cover the complete opposite arrangement".into(),
        );
    }
    for row in rows {
        let joined_weight = row
            .weight
            .checked_mul(stats.max_row_weight)
            .ok_or("RS-1002: joined row multiplicity overflow")?;
        if let Some((value_side, column)) = spec.value {
            if value_side == side {
                row_value(row.row, column)?
                    .checked_mul(joined_weight)
                    .ok_or("RS-1002: joined row contribution overflow")?;
            } else {
                for bound in [&stats.min_weighted_value, &stats.max_weighted_value] {
                    let bound: i128 = if bound.is_empty() {
                        0
                    } else {
                        bound.parse().map_err(|_| "invalid joined scalar bound")?
                    };
                    let contribution = bound
                        .checked_mul(i128::from(row.weight))
                        .ok_or("RS-1002: joined row contribution overflow")?;
                    i64::try_from(contribution)
                        .map_err(|_| "RS-1002: joined row contribution overflow")?;
                }
            }
        }
    }
    Ok(())
}

fn final_value(
    left: &SideState,
    right: &SideState,
    spec: &JoinSpec,
) -> Result<Option<i64>, String> {
    let count = i128::from(left.live_weight)
        .checked_mul(i128::from(right.live_weight))
        .ok_or("join final count overflow")?;
    if count == 0 {
        return Ok(None);
    }
    i64::try_from(count).map_err(|_| "RS-1002: canonical join count overflow")?;
    let value = match spec.aggregate {
        Some(AggregateFunc::Count) => count,
        Some(AggregateFunc::Sum) => {
            let (side, _) = spec.value.ok_or("SUM routing is missing its operand")?;
            let (values, other) = if side == JoinInputSide::Left {
                (left, right)
            } else {
                (right, left)
            };
            signed_sum(values)?
                .checked_mul(i128::from(other.live_weight))
                .ok_or("join final sum overflow")?
        }
        _ => return Err("RS-1013: canonical join finalization requires SUM or COUNT".into()),
    };
    Ok(Some(
        i64::try_from(value).map_err(|_| "RS-1002: canonical join sum overflow")?,
    ))
}

fn group_row(key: &str, value: i64, weight: i64, spec: &JoinSpec) -> RuntimeRow {
    RuntimeRow {
        values_tsv: if spec.merge_key_columns == [0] {
            format!("{key}\t{value}")
        } else {
            format!("{value}\t{key}")
        },
        weight,
    }
}

fn push_final_row(
    plan: &mut EpochPlan,
    shard: ShardId,
    side: JoinInputSide,
    row: RuntimeRow,
    spec: &JoinSpec,
) {
    let bootstrap = plan
        .shards
        .entry(shard)
        .or_default()
        .bootstrap
        .get_or_insert_with(|| JoinBootstrap {
            source: spec.source(side.opposite()).into(),
            rows: Vec::new(),
            final_rows: Vec::new(),
        });
    bootstrap.final_rows.push(row);
}

fn finalize_canonical_key(
    plan: &mut EpochPlan,
    before: &KeyState,
    after: &mut KeyState,
    side: JoinInputSide,
    original: ShardId,
    spec: &JoinSpec,
) -> Result<(), String> {
    let owner = before
        .canonical_owner
        .or_else(|| before.promoted.as_ref().map(|route| route.original_shard))
        .unwrap_or(original);
    if let Some(previous_owner) = before.canonical_owner {
        if let Some(value) = final_value(&before.left, &before.right, spec)? {
            push_final_row(
                plan,
                previous_owner,
                side,
                group_row(&before.key, value, -1, spec),
                spec,
            );
        }
    } else if let Some(promoted) = &before.promoted {
        for (shard, high) in &before.partial_high {
            let low = before.side(promoted.high_side.opposite());
            let (left, right) = if promoted.high_side == JoinInputSide::Left {
                (high, low)
            } else {
                (low, high)
            };
            if let Some(value) = final_value(left, right, spec)? {
                push_final_row(
                    plan,
                    *shard,
                    side,
                    group_row(&before.key, value, -1, spec),
                    spec,
                );
            }
        }
    } else if let Some(value) = final_value(&before.left, &before.right, spec)? {
        push_final_row(
            plan,
            owner,
            side,
            group_row(&before.key, value, -1, spec),
            spec,
        );
    }
    if let Some(value) = final_value(&after.left, &after.right, spec)? {
        push_final_row(
            plan,
            owner,
            side,
            group_row(&before.key, value, 1, spec),
            spec,
        );
    }
    after.canonical_owner = Some(owner);
    after.partial_high.clear();
    for input in [&mut after.left, &mut after.right] {
        input.rows.clear();
        input.snapshot_incomplete = input.live_weight != 0;
    }
    Ok(())
}

fn target_shards(topology: &WorkloadTopologySnapshot, original: ShardId) -> Vec<ShardId> {
    let mut placements: Vec<_> = topology.shards.iter().collect();
    placements.sort_by_key(|placement| placement.shard_id);
    let mut targets = vec![original];
    let mut workers = BTreeSet::<WorkerId>::new();
    if let Some(owner) = placements
        .iter()
        .find(|placement| placement.shard_id == original)
    {
        workers.insert(owner.worker_id);
    }
    // Prefer a different worker for each partial, rather than accidentally
    // spreading a hot key across many virtual shards on the same worker.
    for placement in placements {
        if workers.insert(placement.worker_id) && targets.len() < MAX_JOIN_BUCKETS {
            targets.push(placement.shard_id);
        }
    }
    targets.sort();
    targets
}

fn push_route(
    shards: &mut BTreeMap<ShardId, ShardPlan>,
    shard: ShardId,
    row: &IndexedRow<'_>,
    weight: i64,
) {
    if weight != 0 {
        shards.entry(shard).or_default().rows.push(RowRoute {
            index: row.index,
            weight,
        });
    }
}

fn update_partial(
    state: &mut KeyState,
    shard: ShardId,
    row: &IndexedRow<'_>,
    weight: i64,
    value_column: Option<usize>,
) -> Result<(), String> {
    if weight == 0 {
        return Ok(());
    }
    let partial = state.partial_high.entry(shard).or_default();
    partial.snapshot_incomplete = true;
    partial.apply(
        &[IndexedRow {
            index: row.index,
            row: row.row,
            weight,
        }],
        value_column,
    )?;
    partial.rows.clear();
    partial.snapshot_incomplete = partial.live_weight != 0;
    Ok(())
}

/// Propose one source epoch. Callers must initialize the routing journal before
/// the first input is committed, prepare this immutable record before fanout,
/// and atomically install `point_mutations` only after every worker durable ACK.
pub fn prepare_join_epoch(
    previous: Option<&Value>,
    point_state: &BTreeMap<String, Value>,
    request: &SourceDeltaRequest,
    topology: &WorkloadTopologySnapshot,
    plan_json: &str,
) -> Result<Option<JoinEpoch>, String> {
    let Some(spec) = join_spec(plan_json, Some(topology)) else {
        return Ok(None);
    };
    validate_row_identity(request, topology)?;
    if let Some(previous) = previous {
        if previous.get("kind").and_then(Value::as_str) != Some("join")
            || previous.get("plan_digest").and_then(Value::as_str) != Some(&spec.plan_digest)
        {
            return Err("RS-0001: join routing state does not match deployed plan".into());
        }
    }
    let side = spec
        .side(&request.source)
        .ok_or("source is not a join input")?;
    let mut shard_ids: Vec<_> = topology
        .shards
        .iter()
        .map(|placement| placement.shard_id)
        .collect();
    shard_ids.sort();
    shard_ids.dedup();
    if shard_ids.is_empty() {
        return Err("RS-1703: join routing has no active shards".into());
    }
    let grouped = group_request(request, spec.key_column(side))?;
    let mut plan = EpochPlan {
        kind: "heavy_light_join/v1".into(),
        request_digest: request_digest(request)?,
        shards: BTreeMap::new(),
        diagnostics: Vec::new(),
    };
    let mut mutations = BTreeMap::new();
    let mut epoch_seed_bytes = 0usize;
    let mut generation = previous
        .and_then(|state| state.get("routing_generation"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    for (key, rows) in grouped {
        if rows.is_empty() {
            continue;
        }
        let original = rendezvous_hash(fnv1a_64(key.as_bytes()) % VIRTUAL_BUCKETS, &shard_ids, 1)
            .ok_or("RS-1703: join routing has no active shards")?;
        if key == "\\N" {
            for row in &rows {
                push_route(&mut plan.shards, original, row, row.weight);
            }
            continue;
        }
        // Refuse alternate textual encodings of integer keys: the existing
        // hash router compares TSV bytes, while the operator compares integers.
        let canonical: i64 = key
            .parse()
            .map_err(|_| "join routing key is not an integer")?;
        if canonical.to_string() != key {
            return Err("RS-1002: join routing requires canonical integer keys".into());
        }
        let storage_key = state_key(&spec, &key);
        let mut state: KeyState = point_state
            .get(&storage_key)
            .map(|value| serde_json::from_value(value.clone()).map_err(|error| error.to_string()))
            .transpose()?
            .unwrap_or_else(|| KeyState {
                key: key.clone(),
                ..KeyState::default()
            });
        if state.key != key {
            return Err("RS-0001: join key identity does not match durable record".into());
        }
        if state
            .last_observed_epoch
            .is_some_and(|epoch| request.epoch <= epoch)
        {
            return Err("RS-3008: join routing epoch did not advance".into());
        }
        let admitted = scalar_budget(&state, side, &rows, &spec)?;
        let before = state.clone();
        let mut row_updates = BTreeMap::<String, SaltedRow>::new();
        for row in &rows {
            let ledger_key = row_key(&spec, &key, side, &row.row.values_tsv);
            let mut ledger: SaltedRow = point_state
                .get(&ledger_key)
                .map(|value| {
                    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
                })
                .transpose()?
                .unwrap_or_else(|| SaltedRow {
                    row: row.row.values_tsv.clone(),
                    weight: 0,
                    total_weight: 0,
                });
            if ledger.row != row.row.values_tsv || ledger.total_weight < 0 || ledger.weight < 0 {
                return Err("RS-0001: join row identity or multiplicity is invalid".into());
            }
            ledger.total_weight = ledger
                .total_weight
                .checked_add(row.weight)
                .ok_or("join row multiplicity overflow")?;
            if ledger.total_weight < 0 {
                return Err("RS-1002: join deletion exceeds the live row multiplicity".into());
            }
            row_updates.insert(ledger_key, ledger);
        }
        let value_column = spec
            .value
            .filter(|(input, _)| *input == side)
            .map(|(_, column)| column);
        state.side_mut(side).apply(&rows, value_column)?;
        if let Some(promoted) = &state.promoted {
            if promoted
                .targets
                .iter()
                .any(|shard| !shard_ids.contains(shard))
            {
                return Err(
                    "RS-3004: join lost a target shard; finish canonical shard migration first"
                        .into(),
                );
            }
        }
        if state.canonical_owner.is_some() || (!admitted && spec.aggregate.is_some()) {
            check_individual_join_products(&before, &rows, side, &spec, point_state)?;
            finalize_canonical_key(&mut plan, &before, &mut state, side, original, &spec)?;
            state.last_observed_epoch = Some(request.epoch);
            plan.diagnostics.push(JoinRoutingDiagnostic {
                key_digest: blake3::hash(key.as_bytes()).to_hex().to_string(),
                decision: "single_owner_arithmetic_fallback".into(),
                high_rows: state.left.live_weight.max(state.right.live_weight),
                low_rows: state.left.live_weight.min(state.right.live_weight),
                buckets: 1,
                generation,
            });
            for (ledger_key, mut ledger) in row_updates {
                ledger.weight = 0;
                mutations.insert(
                    ledger_key,
                    if ledger.total_weight == 0 {
                        None
                    } else {
                        Some(serde_json::to_value(ledger).map_err(|error| error.to_string())?)
                    },
                );
            }
            // Canonical mode is permanent, including through an empty key:
            // retained old operator arrangements must never become executable
            // again without an explicit state-cleanup generation.
            mutations.insert(
                storage_key,
                Some(serde_json::to_value(state).map_err(|error| error.to_string())?),
            );
            continue;
        }
        let mut just_promoted = false;
        let mut bootstrap_budget_deferred = false;
        if state.promoted.is_none() {
            let candidate = if admitted { state.candidate() } else { None };
            state.sustained_epochs = if candidate.is_some()
                && state.candidate == candidate
                && state
                    .last_observed_epoch
                    .and_then(|epoch| epoch.checked_add(1))
                    == Some(request.epoch)
            {
                state.sustained_epochs.saturating_add(1)
            } else {
                u32::from(candidate.is_some())
            };
            state.candidate = candidate;
            if let Some(high_side) =
                candidate.filter(|_| state.sustained_epochs >= JOIN_PROMOTION_EPOCHS)
            {
                let targets = target_shards(topology, original);
                if targets.len() > 1 {
                    let snapshot: Vec<_> = state
                        .side(high_side.opposite())
                        .rows
                        .iter()
                        .map(|(values_tsv, weight)| RuntimeRow {
                            values_tsv: values_tsv.clone(),
                            weight: *weight,
                        })
                        .collect();
                    let bytes = serde_json::to_vec(&snapshot)
                        .map_err(|error| error.to_string())?
                        .len()
                        .checked_add(256)
                        .and_then(|bytes| bytes.checked_mul(targets.len() - 1))
                        .and_then(|bytes| epoch_seed_bytes.checked_add(bytes));
                    if let Some(bytes) = bytes.filter(|bytes| *bytes <= MAX_JOIN_EPOCH_SEED_BYTES) {
                        epoch_seed_bytes = bytes;
                        generation = generation
                            .checked_add(1)
                            .ok_or("join routing generation overflow")?;
                        state.promoted = Some(PromotedKey {
                            high_side,
                            original_shard: original,
                            targets,
                            generation,
                        });
                        just_promoted = true;
                    } else {
                        // Keep executing the ordinary owner this epoch. The
                        // observation and complete low snapshot remain durable
                        // so a later epoch can admit this promotion.
                        bootstrap_budget_deferred = true;
                    }
                }
            }
        }
        state.last_observed_epoch = Some(request.epoch);
        if let Some(promoted) = state.promoted.clone() {
            let low_side = promoted.high_side.opposite();
            if just_promoted {
                let mut historic = before.side(promoted.high_side).clone();
                historic.rows.clear();
                historic.snapshot_incomplete = historic.live_weight != 0;
                state.partial_high.insert(promoted.original_shard, historic);
                let snapshot: Vec<_> = state
                    .side(low_side)
                    .rows
                    .iter()
                    .map(|(values_tsv, weight)| RuntimeRow {
                        values_tsv: values_tsv.clone(),
                        weight: *weight,
                    })
                    .collect();
                for target in &promoted.targets {
                    if *target == promoted.original_shard {
                        continue;
                    }
                    let batch = plan.shards.entry(*target).or_default();
                    if side == low_side {
                        batch.seed_rows.extend(snapshot.clone());
                    } else {
                        let bootstrap = batch.bootstrap.get_or_insert_with(|| JoinBootstrap {
                            source: spec.source(low_side).to_owned(),
                            rows: Vec::new(),
                            final_rows: Vec::new(),
                        });
                        if bootstrap.source != spec.source(low_side) {
                            return Err(
                                "RS-0001: one epoch requires incompatible join bootstraps".into()
                            );
                        }
                        bootstrap.rows.extend(snapshot.clone());
                    }
                }
            }
            if side == low_side {
                for target in &promoted.targets {
                    // On the promotion epoch the new target receives the full
                    // post-epoch snapshot, not that snapshot plus the delta.
                    if just_promoted && *target != promoted.original_shard {
                        continue;
                    }
                    for row in &rows {
                        push_route(&mut plan.shards, *target, row, row.weight);
                    }
                }
            } else {
                for row in &rows {
                    let ledger_key = row_key(&spec, &key, side, &row.row.values_tsv);
                    let salted = row_updates
                        .get_mut(&ledger_key)
                        .ok_or("missing join row ledger")?;
                    if salted.row != row.row.values_tsv || salted.weight < 0 {
                        return Err(
                            "RS-0001: salted join row identity or multiplicity is invalid".into(),
                        );
                    }
                    let target = promoted.targets[(fnv1a_64(row.row.values_tsv.as_bytes())
                        % promoted.targets.len() as u64)
                        as usize];
                    if row.weight > 0 {
                        salted.weight = salted
                            .weight
                            .checked_add(row.weight)
                            .ok_or("salted join multiplicity overflow")?;
                        push_route(&mut plan.shards, target, row, row.weight);
                        update_partial(&mut state, target, row, row.weight, value_column)?;
                    } else {
                        let removed = row
                            .weight
                            .checked_neg()
                            .ok_or("join inverse is not representable")?;
                        let from_salted = salted.weight.min(removed);
                        salted.weight -= from_salted;
                        push_route(&mut plan.shards, target, row, -from_salted);
                        update_partial(&mut state, target, row, -from_salted, value_column)?;
                        push_route(
                            &mut plan.shards,
                            promoted.original_shard,
                            row,
                            -(removed - from_salted),
                        );
                        update_partial(
                            &mut state,
                            promoted.original_shard,
                            row,
                            -(removed - from_salted),
                            value_column,
                        )?;
                    }
                }
            }
            plan.diagnostics.push(JoinRoutingDiagnostic {
                key_digest: blake3::hash(key.as_bytes()).to_hex().to_string(),
                decision: if just_promoted { "promote" } else { "salted" }.into(),
                high_rows: state.side(promoted.high_side).live_weight,
                low_rows: state.side(low_side).live_weight,
                buckets: promoted.targets.len(),
                generation: promoted.generation,
            });
            // High rows are never replicated. Their independently persisted
            // per-row ledger supports bounded lookup even for a huge hot key.
            let high = state.side_mut(promoted.high_side);
            high.rows.clear();
            high.snapshot_incomplete = high.live_weight != 0;
            // The low snapshot is needed only for the atomic promotion. Once
            // every replica has it, future low deltas stream under the normal
            // per-frame byte bound; total low-side state is not a dataset-size
            // limit and is never recopied into routing metadata.
            let low = state.side_mut(low_side);
            low.rows.clear();
            low.snapshot_incomplete = low.live_weight != 0;
        } else {
            for row in &rows {
                push_route(&mut plan.shards, original, row, row.weight);
            }
            if state.candidate.is_some() || !admitted {
                let high = if state.left.live_weight >= state.right.live_weight {
                    JoinInputSide::Left
                } else {
                    JoinInputSide::Right
                };
                plan.diagnostics.push(JoinRoutingDiagnostic {
                    key_digest: blake3::hash(key.as_bytes()).to_hex().to_string(),
                    decision: if bootstrap_budget_deferred {
                        "defer_bootstrap_budget"
                    } else if admitted {
                        "observe"
                    } else {
                        "single_owner_arithmetic_fallback"
                    }
                    .into(),
                    high_rows: state.side(high).live_weight,
                    low_rows: state.side(high.opposite()).live_weight,
                    buckets: 1,
                    generation,
                });
            }
        }
        for (ledger_key, ledger) in row_updates {
            mutations.insert(
                ledger_key,
                if ledger.total_weight == 0 {
                    None
                } else {
                    Some(serde_json::to_value(ledger).map_err(|error| error.to_string())?)
                },
            );
        }
        mutations.insert(
            storage_key,
            if state.left.live_weight == 0 && state.right.live_weight == 0 {
                None
            } else {
                Some(serde_json::to_value(state).map_err(|error| error.to_string())?)
            },
        );
    }
    // Every epoch reaches all targets, including empty markers, so a promoted
    // key cannot advance one worker's frontier past an unseeded replica.
    for shard in shard_ids {
        plan.shards.entry(shard).or_default();
    }
    let plan = serde_json::to_value(plan).map_err(|error| error.to_string())?;
    let batches = route_join_epoch(&plan, request)?;
    Ok(Some(JoinEpoch {
        batches,
        plan,
        point_mutations: mutations,
        next_state: serde_json::json!({"kind":"join", "plan_digest":spec.plan_digest, "routing_generation":generation}),
    }))
}

/// Reconstruct exactly the prepared routing after a caller/worker crash.
pub fn route_join_epoch(
    plan: &Value,
    request: &SourceDeltaRequest,
) -> Result<BTreeMap<ShardId, JoinShardBatch>, String> {
    let plan: EpochPlan =
        serde_json::from_value(plan.clone()).map_err(|error| error.to_string())?;
    if plan.kind != "heavy_light_join/v1" || plan.request_digest != request_digest(request)? {
        return Err("RS-3008: join routing replay input does not match prepared epoch".into());
    }
    let mut batches = BTreeMap::new();
    for (shard, plan) in plan.shards {
        let mut rows = plan.seed_rows;
        for route in plan.rows {
            let row = request
                .rows
                .get(route.index)
                .ok_or("join routing row index is out of bounds")?;
            rows.push(RuntimeRow {
                values_tsv: row.values_tsv.clone(),
                weight: route.weight,
            });
        }
        let bootstrap_bytes =
            plan.bootstrap
                .as_ref()
                .map(|bootstrap| {
                    bootstrap.rows.iter().chain(&bootstrap.final_rows).try_fold(
                        0usize,
                        |bytes, row| {
                            bytes
                                .checked_add(row.values_tsv.len())
                                .and_then(|bytes| bytes.checked_add(8))
                                .ok_or("join bootstrap size overflow")
                        },
                    )
                })
                .transpose()?
                .unwrap_or(0);
        if bootstrap_bytes > MAX_JOIN_BOOTSTRAP_BYTES {
            return Err("RS-3006: join bootstrap exceeds epoch replication budget".into());
        }
        batches.insert(
            shard,
            JoinShardBatch {
                rows,
                bootstrap: plan.bootstrap,
            },
        );
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use rockstream_ops::factorized::{FactorizedAggregateKind, FactorizedJoinAggregateOp};
    use rockstream_plan::{AggregateExpr, JoinSemantics};
    use rockstream_types::data_plane::{
        DeploymentColumn, DeploymentSchema, WorkloadShardPlacement, DEPLOYMENT_DESCRIPTOR_VERSION,
    };
    use rockstream_types::ids::{LeaseToken, OperatorId, WorkloadId};

    fn topology() -> WorkloadTopologySnapshot {
        WorkloadTopologySnapshot {
            topology_epoch: 1,
            workload_id: WorkloadId(17),
            plan_json: plan(AggregateFunc::Sum),
            routing_columns: BTreeMap::from([("left".into(), 0), ("right".into(), 0)]),
            schemas: ["left", "right"]
                .into_iter()
                .map(|relation| DeploymentSchema {
                    relation: relation.into(),
                    columns: vec![
                        DeploymentColumn {
                            name: "key".into(),
                            data_type: "i64".into(),
                        },
                        DeploymentColumn {
                            name: "value".into(),
                            data_type: "i64".into(),
                        },
                    ],
                })
                .collect(),
            shards: (1..=4)
                .map(|id| WorkloadShardPlacement {
                    shard_id: ShardId(id),
                    shard_index: (id - 1) as u32,
                    worker_id: WorkerId(id),
                    worker_address: format!("127.0.0.1:{}", 5000 + id),
                    lease_token: LeaseToken(id),
                    operator_id: OperatorId(100),
                    storage_identity: format!("shard-{id}"),
                    frontier: 0,
                })
                .collect(),
            workers: Vec::new(),
        }
    }

    fn plan(func: AggregateFunc) -> String {
        serde_json::to_string(&PlanNode::ViewSink {
            view_name: "joined".into(),
            pk: vec![0],
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::InnerJoin {
                    left: Box::new(PlanNode::Source {
                        name: "left".into(),
                    }),
                    right: Box::new(PlanNode::Source {
                        name: "right".into(),
                    }),
                    left_keys: vec![0],
                    right_keys: vec![0],
                    left_arr_id: OperatorId(1),
                    right_arr_id: OperatorId(2),
                    semantics: JoinSemantics::default(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            }),
        })
        .unwrap()
    }

    fn row(key: i64, value: i64, weight: i64) -> RuntimeRow {
        RuntimeRow {
            values_tsv: format!("{key}\t{value}"),
            weight,
        }
    }

    fn request(epoch: u64, source: &str, rows: Vec<RuntimeRow>) -> SourceDeltaRequest {
        SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: format!("join-epoch-{epoch}"),
            workload_id: WorkloadId(17),
            epoch,
            source: source.into(),
            rows,
        }
    }

    struct Harness {
        topology: WorkloadTopologySnapshot,
        plan: String,
        metadata: Option<Value>,
        points: BTreeMap<String, Value>,
        workers: BTreeMap<ShardId, FactorizedJoinAggregateOp>,
        output: BTreeMap<ShardId, BTreeMap<(i64, i64), i64>>,
        left: BTreeMap<(i64, i64), i64>,
        right: BTreeMap<(i64, i64), i64>,
    }

    impl Harness {
        fn new() -> Self {
            let topology = topology();
            Self {
                workers: topology
                    .shards
                    .iter()
                    .map(|placement| {
                        (
                            placement.shard_id,
                            FactorizedJoinAggregateOp::new(
                                OperatorId(1),
                                vec![0],
                                vec![0],
                                2,
                                2,
                                0,
                                1,
                                FactorizedAggregateKind::Sum,
                            ),
                        )
                    })
                    .collect(),
                topology,
                plan: plan(AggregateFunc::Sum),
                metadata: None,
                points: BTreeMap::new(),
                output: BTreeMap::new(),
                left: BTreeMap::new(),
                right: BTreeMap::new(),
            }
        }

        fn propose(&self, request: &SourceDeltaRequest) -> JoinEpoch {
            let keys = join_state_keys(request, &self.topology, &self.plan).unwrap();
            let mut points: BTreeMap<String, Value> = keys
                .into_iter()
                .filter_map(|key| self.points.get(&key).cloned().map(|value| (key, value)))
                .collect();
            for scan in join_fallback_scans(&points, request, &self.topology, &self.plan).unwrap() {
                let mut stats = JoinLedgerStats::default();
                for (_, value) in self
                    .points
                    .range(scan.prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(&scan.prefix))
                {
                    stats.observe(value, scan.value_column).unwrap();
                }
                points.insert(scan.result_key(), serde_json::to_value(stats).unwrap());
            }
            prepare_join_epoch(
                self.metadata.as_ref(),
                &points,
                request,
                &self.topology,
                &self.plan,
            )
            .unwrap()
            .unwrap()
        }

        fn commit(&mut self, request: &SourceDeltaRequest) -> JoinEpoch {
            let epoch = self.propose(request);
            // JSON round-trip is the journal's crash/replay representation.
            let persisted: Value =
                serde_json::from_slice(&serde_json::to_vec(&epoch.plan).unwrap()).unwrap();
            assert_eq!(
                route_join_epoch(&persisted, request).unwrap(),
                epoch.batches
            );
            let schemas =
                crate::client::deployment_schemas_from_slice(&self.topology.schemas).unwrap();
            for (shard, batch) in &epoch.batches {
                let empty = Vec::new();
                let bootstrap = batch
                    .bootstrap
                    .as_ref()
                    .map_or(&empty, |bootstrap| &bootstrap.rows);
                let (left, right) = if request.source == "left" {
                    (&batch.rows, bootstrap)
                } else {
                    (bootstrap, &batch.rows)
                };
                let output = self.workers[shard]
                    .process_epoch(
                        crate::client::rows_to_zset(left, schemas["left"].clone()).unwrap(),
                        crate::client::rows_to_zset(right, schemas["right"].clone()).unwrap(),
                    )
                    .unwrap();
                let keys = output
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = output
                    .data
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(keys.len(), values.len());
                assert_eq!(keys.len(), output.weights.len());
                for ((key, value), output_weight) in keys
                    .values()
                    .iter()
                    .zip(values.values())
                    .zip(&output.weights)
                {
                    let identity = (*key, *value);
                    let rows = self.output.entry(*shard).or_default();
                    let weight = rows.get(&identity).copied().unwrap_or(0) + *output_weight;
                    if weight == 0 {
                        rows.remove(&identity);
                    } else {
                        rows.insert(identity, weight);
                    }
                }
                if let Some(bootstrap) = &batch.bootstrap {
                    for row in &bootstrap.final_rows {
                        let mut values = row
                            .values_tsv
                            .split('\t')
                            .map(|value| value.parse::<i64>().unwrap());
                        let identity = (values.next().unwrap(), values.next().unwrap());
                        let rows = self.output.entry(*shard).or_default();
                        let weight = rows.get(&identity).copied().unwrap_or(0) + row.weight;
                        if weight == 0 {
                            rows.remove(&identity);
                        } else {
                            rows.insert(identity, weight);
                        }
                    }
                }
                assert_eq!(self.workers[shard].joined_intermediate_rows(), 0);
            }
            for (key, value) in &epoch.point_mutations {
                if let Some(value) = value {
                    self.points.insert(key.clone(), value.clone());
                } else {
                    self.points.remove(key);
                }
            }
            self.metadata = Some(epoch.next_state.clone());
            let source = if request.source == "left" {
                &mut self.left
            } else {
                &mut self.right
            };
            for row in &request.rows {
                let mut values = row
                    .values_tsv
                    .split('\t')
                    .map(|value| value.parse::<i64>().unwrap());
                let key = (values.next().unwrap(), values.next().unwrap());
                let next = source.get(&key).copied().unwrap_or(0) + row.weight;
                assert!(next >= 0);
                if next == 0 {
                    source.remove(&key);
                } else {
                    source.insert(key, next);
                }
            }
            let mut oracle = BTreeMap::<i64, i64>::new();
            for ((left_key, value), left_weight) in &self.left {
                for ((right_key, _), right_weight) in &self.right {
                    if left_key == right_key {
                        *oracle.entry(*left_key).or_default() += value * left_weight * right_weight;
                    }
                }
            }
            let mut actual = BTreeMap::<i64, i64>::new();
            for rows in self.output.values() {
                for ((key, sum), weight) in rows {
                    assert_eq!(*weight, 1);
                    *actual.entry(*key).or_default() += sum;
                }
            }
            assert_eq!(actual, oracle);
            epoch
        }
    }

    #[test]
    fn live_promotion_bootstraps_only_low_side_and_matches_factorized_oracle() {
        let mut h = Harness::new();
        h.commit(&request(1, "right", vec![row(1, 10, 1), row(2, 20, 1)]));
        h.commit(&request(
            2,
            "left",
            (0..64)
                .map(|value| row(1, value, 1))
                .chain([row(2, 9, 1)])
                .collect(),
        ));
        h.commit(&request(
            3,
            "left",
            (64..128).map(|value| row(1, value, 1)).collect(),
        ));
        let promotion = h.commit(&request(
            4,
            "left",
            (128..192).map(|value| row(1, value, 1)).collect(),
        ));
        let spec = join_spec(&h.plan, Some(&h.topology)).unwrap();
        let state: KeyState =
            serde_json::from_value(h.points[&state_key(&spec, "1")].clone()).unwrap();
        let promoted = state.promoted.unwrap();
        assert_eq!(
            (
                promoted.high_side,
                promoted.targets.clone(),
                promoted.generation
            ),
            (
                JoinInputSide::Left,
                vec![ShardId(1), ShardId(2), ShardId(3), ShardId(4)],
                1
            )
        );
        for (target, batch) in &promotion.batches {
            let expected = (*target != promoted.original_shard).then(|| JoinBootstrap {
                source: "right".into(),
                rows: vec![row(1, 10, 1)],
                final_rows: Vec::new(),
            });
            assert_eq!(batch.bootstrap, expected);
        }
        // Old and new copies of the same logical row can coexist. Deletion
        // consumes the salted copy first, then the historical original copy.
        h.commit(&request(5, "left", vec![row(1, 100, 1)]));
        let deletion = h.commit(&request(
            6,
            "left",
            vec![row(1, 100, -2), row(1, 150, -1), row(1, 3, -1)],
        ));
        let mut actual: Vec<_> = deletion
            .batches
            .iter()
            .flat_map(|(shard, batch)| {
                assert_eq!(batch.bootstrap, None);
                batch
                    .rows
                    .iter()
                    .map(move |row| (*shard, row.values_tsv.clone(), row.weight))
            })
            .collect();
        let salted_target =
            |value| promoted.targets[(fnv1a_64(format!("1\t{value}").as_bytes()) % 4) as usize];
        let mut expected = vec![
            (salted_target(100), "1\t100".into(), -1),
            (promoted.original_shard, "1\t100".into(), -1),
            (salted_target(150), "1\t150".into(), -1),
            (promoted.original_shard, "1\t3".into(), -1),
        ];
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        let low = h.commit(&request(7, "right", vec![row(1, 10, -1), row(1, 30, 2)]));
        assert_eq!(
            low.batches
                .into_iter()
                .map(|(shard, batch)| (shard, batch.rows))
                .collect::<BTreeMap<_, _>>(),
            promoted
                .targets
                .into_iter()
                .map(|shard| (shard, vec![row(1, 10, -1), row(1, 30, 2)]))
                .collect()
        );
    }

    #[test]
    fn right_high_side_promotes_with_exact_low_snapshot_and_no_side_flip() {
        let mut h = Harness::new();
        h.commit(&request(1, "left", vec![row(1, 7, 1)]));
        h.commit(&request(
            2,
            "right",
            (0..64).map(|value| row(1, value, 1)).collect(),
        ));
        h.commit(&request(
            3,
            "right",
            (64..128).map(|value| row(1, value, 1)).collect(),
        ));
        let epoch = h.commit(&request(
            4,
            "right",
            (128..192).map(|value| row(1, value, 1)).collect(),
        ));
        let spec = join_spec(&h.plan, Some(&h.topology)).unwrap();
        let before: KeyState =
            serde_json::from_value(h.points[&state_key(&spec, "1")].clone()).unwrap();
        let before = before.promoted.unwrap();
        assert_eq!(before.high_side, JoinInputSide::Right);
        for (shard, batch) in epoch.batches {
            assert_eq!(
                batch.bootstrap,
                (shard != before.original_shard).then(|| JoinBootstrap {
                    source: "left".into(),
                    rows: vec![row(1, 7, 1)],
                    final_rows: Vec::new(),
                })
            );
        }
        h.commit(&request(5, "left", vec![row(1, 7, 1024)]));
        let after: KeyState =
            serde_json::from_value(h.points[&state_key(&spec, "1")].clone()).unwrap();
        let after = after.promoted.unwrap();
        assert_eq!(
            (after.high_side, after.targets, after.generation),
            (before.high_side, before.targets, before.generation)
        );
    }

    #[test]
    fn prepared_plan_rejects_changed_replay_and_promotion_requires_sustained_epochs() {
        let mut h = Harness::new();
        h.commit(&request(1, "right", vec![row(1, 10, 1)]));
        h.commit(&request(
            2,
            "left",
            (0..64).map(|value| row(1, value, 1)).collect(),
        ));
        h.commit(&request(
            3,
            "left",
            (0..64).map(|value| row(1, value, -1)).collect(),
        ));
        let input = request(4, "left", (0..64).map(|value| row(1, value, 1)).collect());
        let epoch = h.commit(&input);
        assert_eq!(epoch.next_state["routing_generation"], 0);
        let mut wrong = input.clone();
        wrong.rows[0].weight = 2;
        assert_eq!(
            route_join_epoch(&epoch.plan, &wrong).unwrap_err(),
            "RS-3008: join routing replay input does not match prepared epoch"
        );
        assert_eq!(
            route_join_epoch(&epoch.plan, &input).unwrap(),
            epoch.batches
        );
    }

    #[test]
    fn arithmetic_fallback_retracts_partials_and_survives_cancellation_and_replay() {
        let mut h = Harness::new();
        h.commit(&request(1, "right", vec![row(1, 10, 1)]));
        for epoch in 2..=4 {
            h.commit(&request(
                epoch,
                "left",
                ((epoch - 2) * 64..(epoch - 1) * 64)
                    .map(|value| row(1, value as i64, 1))
                    .collect(),
            ));
        }
        let input = request(5, "left", vec![row(1, i64::MAX, 1), row(1, -i64::MAX, 1)]);
        let epoch = h.commit(&input);
        assert_eq!(
            epoch.plan["diagnostics"][0]["decision"],
            "single_owner_arithmetic_fallback"
        );
        let spec = join_spec(&h.plan, Some(&h.topology)).unwrap();
        let state: KeyState =
            serde_json::from_value(h.points[&state_key(&spec, "1")].clone()).unwrap();
        assert_eq!(
            state.canonical_owner,
            state.promoted.as_ref().map(|route| route.original_shard)
        );
        assert!(epoch.batches.values().all(|batch| batch.rows.is_empty()));
        // Reopening the journal and all row ledgers retains the canonical
        // decision; no unbounded historical high-row replay is needed.
        h.points = serde_json::from_slice(&serde_json::to_vec(&h.points).unwrap()).unwrap();
        h.metadata = serde_json::from_slice(&serde_json::to_vec(&h.metadata).unwrap()).unwrap();
        h.commit(&request(
            6,
            "left",
            vec![row(1, i64::MAX, -1), row(1, -i64::MAX, -1)],
        ));
        h.commit(&request(7, "right", vec![row(1, 10, 1)]));
        h.commit(&request(8, "left", vec![row(1, 5, -1)]));
        let after: KeyState =
            serde_json::from_value(h.points[&state_key(&spec, "1")].clone()).unwrap();
        assert_eq!(after.canonical_owner, state.canonical_owner);
    }

    #[test]
    fn canonical_fallback_never_hides_per_row_multiplication_overflow() {
        let mut h = Harness::new();
        h.commit(&request(1, "right", vec![row(1, 10, 1)]));
        h.commit(&request(
            2,
            "left",
            vec![row(1, i64::MAX, 1), row(1, -i64::MAX, 1)],
        ));
        let input = request(3, "left", vec![row(1, i64::MAX, 2), row(1, -i64::MAX, 2)]);
        let mut points = h.points.clone();
        for scan in join_fallback_scans(&points, &input, &h.topology, &h.plan).unwrap() {
            let mut stats = JoinLedgerStats::default();
            for (key, value) in &h.points {
                if key.starts_with(&scan.prefix) {
                    stats.observe(value, scan.value_column).unwrap();
                }
            }
            points.insert(scan.result_key(), serde_json::to_value(stats).unwrap());
        }
        assert_eq!(
            prepare_join_epoch(h.metadata.as_ref(), &points, &input, &h.topology, &h.plan)
                .unwrap_err(),
            "RS-1002: joined row contribution overflow"
        );
        // The same exact state still accepts a valid canonical retraction.
        h.commit(&request(
            3,
            "left",
            vec![row(1, i64::MAX, -1), row(1, -i64::MAX, -1)],
        ));
    }

    #[test]
    fn join_admission_composes_source_intermediate_and_output_projections() {
        use rockstream_plan::ExchangeKind;
        let join = PlanNode::InnerJoin {
            left: Box::new(PlanNode::Project {
                input: Box::new(PlanNode::Source {
                    name: "left".into(),
                }),
                columns: vec![Expr::Column(1), Expr::Column(0)],
            }),
            right: Box::new(PlanNode::Source {
                name: "right".into(),
            }),
            left_keys: vec![1],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        };
        let aggregate = PlanNode::Aggregate {
            input: Box::new(PlanNode::Exchange {
                kind: ExchangeKind::Hash,
                partition_key: vec![1],
                child: Box::new(PlanNode::Project {
                    input: Box::new(join),
                    columns: vec![Expr::Column(3), Expr::Column(2), Expr::Column(0)],
                }),
            }),
            group_by: vec![Expr::Column(1)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(2),
                distinct: false,
            }],
        };
        let plan = PlanNode::ViewSink {
            view_name: "projected".into(),
            pk: vec![1],
            child: Box::new(PlanNode::Project {
                input: Box::new(PlanNode::Exchange {
                    kind: ExchangeKind::Hash,
                    partition_key: vec![1],
                    child: Box::new(PlanNode::Project {
                        input: Box::new(aggregate),
                        columns: vec![Expr::Column(1), Expr::Column(0)],
                    }),
                }),
                columns: vec![Expr::Column(0), Expr::Column(1)],
            }),
        };
        let plan = serde_json::to_string(&plan).unwrap();
        let spec = join_spec(&plan, Some(&topology())).unwrap();
        assert_eq!(
            (
                spec.left_source,
                spec.left_key,
                spec.right_source,
                spec.right_key,
                spec.value,
                spec.aggregate,
                spec.merge_key_columns,
            ),
            (
                "left".into(),
                0,
                "right".into(),
                0,
                Some((JoinInputSide::Left, 1)),
                Some(AggregateFunc::Sum),
                vec![1],
            )
        );
    }

    #[test]
    fn simultaneous_promotions_defer_with_exact_results_and_resume_next_epoch() {
        let mut harness = Harness::new();
        for key in 1..=8 {
            harness.commit(&request(
                key as u64,
                "right",
                (0..3500).map(|value| row(key, value, 1)).collect(),
            ));
        }
        harness.commit(&request(
            9,
            "left",
            (1..=8).map(|key| row(key, 1, 1_000_000)).collect(),
        ));
        harness.commit(&request(
            10,
            "left",
            (1..=8).map(|key| row(key, 1, 1)).collect(),
        ));
        for (epoch, decisions, expected_sum) in [
            (
                11,
                [
                    "promote",
                    "promote",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                ],
                3_500_007_000i64,
            ),
            (
                12,
                [
                    "salted",
                    "salted",
                    "promote",
                    "promote",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                    "defer_bootstrap_budget",
                ],
                3_500_010_500i64,
            ),
        ] {
            let result = harness.commit(&request(
                epoch,
                "left",
                (1..=8).map(|key| row(key, 1, 1)).collect(),
            ));
            let actual_decisions = result.plan["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|event| (event["key_digest"].clone(), event["decision"].clone()))
                .collect::<Vec<_>>();
            let expected_decisions = (1..=8)
                .zip(decisions)
                .map(|(key, decision)| {
                    (
                        Value::String(
                            blake3::hash(key.to_string().as_bytes())
                                .to_hex()
                                .to_string(),
                        ),
                        Value::String(decision.into()),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(actual_decisions, expected_decisions);
            let mut actual = BTreeMap::<i64, i64>::new();
            for output in harness.output.values() {
                for ((key, sum), weight) in output {
                    assert_eq!(*weight, 1);
                    *actual.entry(*key).or_default() += sum;
                }
            }
            assert_eq!(actual, (1..=8).map(|key| (key, expected_sum)).collect());
            let journal = serde_json::to_vec(&serde_json::json!({
                "plan":result.plan, "mutations":result.point_mutations,
            }))
            .unwrap();
            assert!(journal.len() < MAX_JOIN_BOOTSTRAP_BYTES);
        }
    }

    #[test]
    fn unsupported_aggregates_do_not_admit_regrouping() {
        let topology = topology();
        let input = request(1, "left", vec![row(1, 4, 1)]);
        assert_eq!(
            join_state_keys(&input, &topology, &plan(AggregateFunc::Avg)).unwrap(),
            Vec::<String>::new()
        );
        assert!(prepare_join_epoch(
            None,
            &BTreeMap::new(),
            &input,
            &topology,
            &plan(AggregateFunc::Avg)
        )
        .unwrap()
        .is_none());
        assert_eq!(
            join_merge_key_columns(&plan(AggregateFunc::Sum), &topology),
            Some(vec![0])
        );
    }
}
