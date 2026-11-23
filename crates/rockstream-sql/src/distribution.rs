//! Distributed physical planning pass for RockStream `PlanNode` trees.
//!
//! Supports both single-shard loopback mode (`shard_count == 1`, preserving
//! v0.7 behavior) and multi-shard distributed execution (`shard_count > 1`):
//!
//! 1. Annotates each operator with its output partitioning (`DistributionAnnotation`)
//!    with accurate column-index remapping through `Project` and `Map`.
//! 2. Preserves co-partitioned joins with zero network exchanges when both
//!    inputs are already partitioned on their join keys.
//! 3. Selects size-bounded broadcast joins (`ExchangeKind::Broadcast`) when the
//!    build side is within `broadcast_threshold_bytes` (for `Inner`, `Left`,
//!    `Semi`, and `Anti` joins).
//! 4. Inserts hash repartition exchanges (`ExchangeKind::Hash` with exact
//!    `partition_key`) whenever join, grouping, window, or Top-K keys differ
//!    from upstream partitioning.
//! 5. Splits aggregations into partial + final stages when admitted by ADR 0004
//!    checked merge-law arithmetic (`Sum`/`Count` without `DISTINCT`).
//! 6. Fails closed with `RS-1013` on unsupported distributed shapes (e.g.,
//!    multi-shard recursion, non-equi/cross joins that exceed the broadcast
//!    budget, or non-column grouping keys that cannot be hash-partitioned).

use std::collections::HashMap;

use rockstream_plan::{
    validate_pre_aggregation_algebra, AggregateExpr, AggregateFunc, ExchangeKind, Expr,
    OuterJoinKind, PlanNode,
};

use crate::error::SqlError;

/// Default build-side size threshold for broadcast joins (16 MiB).
pub const DEFAULT_BROADCAST_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;

/// Default estimated bytes per source when statistics are not provided (64 MiB),
/// ensuring broadcast is only chosen when a source is explicitly known to be
/// size-bounded.
pub const DEFAULT_UNKNOWN_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

/// Worker placement metadata attached to a distributed physical plan for
/// `EXPLAIN INCREMENTAL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPlacementInfo {
    pub worker_id: u64,
    pub address: String,
    pub shard_ids: Vec<u64>,
}

/// Configuration for the distributed physical planning pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributionConfig {
    /// Number of target shards (`1` = single-shard loopback, `> 1` = distributed).
    pub shard_count: usize,
    /// Known initial partition key column indices per source relation.
    pub source_partition_keys: HashMap<String, Vec<usize>>,
    /// Estimated state/input size in bytes per source relation (used for
    /// size-bounded broadcast join selection and shuffle estimation).
    pub source_estimated_bytes: HashMap<String, u64>,
    /// Maximum build-side size in bytes eligible for a broadcast join.
    pub broadcast_threshold_bytes: u64,
    /// Enable ADR 0004 partial + final aggregation pushdown when admitted.
    pub enable_partial_aggregation: bool,
    /// Optional concrete worker placements for `EXPLAIN INCREMENTAL`.
    pub worker_placements: Vec<WorkerPlacementInfo>,
}

impl Default for DistributionConfig {
    fn default() -> Self {
        Self::single_shard()
    }
}

impl DistributionConfig {
    /// Single-shard loopback configuration (zero network shuffle).
    pub fn single_shard() -> Self {
        Self {
            shard_count: 1,
            source_partition_keys: HashMap::new(),
            source_estimated_bytes: HashMap::new(),
            broadcast_threshold_bytes: DEFAULT_BROADCAST_THRESHOLD_BYTES,
            enable_partial_aggregation: false,
            worker_placements: Vec::new(),
        }
    }

    /// Multi-shard distributed configuration.
    pub fn distributed(shard_count: usize) -> Self {
        Self {
            shard_count: shard_count.max(1),
            source_partition_keys: HashMap::new(),
            source_estimated_bytes: HashMap::new(),
            broadcast_threshold_bytes: DEFAULT_BROADCAST_THRESHOLD_BYTES,
            enable_partial_aggregation: true,
            worker_placements: Vec::new(),
        }
    }

    pub fn with_source_partition(
        mut self,
        source: impl Into<String>,
        partition_key: Vec<usize>,
    ) -> Self {
        self.source_partition_keys
            .insert(source.into(), partition_key);
        self
    }

    pub fn with_source_estimated_bytes(mut self, source: impl Into<String>, bytes: u64) -> Self {
        self.source_estimated_bytes.insert(source.into(), bytes);
        self
    }

    pub fn with_broadcast_threshold_bytes(mut self, bytes: u64) -> Self {
        self.broadcast_threshold_bytes = bytes;
        self
    }

    pub fn with_partial_aggregation(mut self, enabled: bool) -> Self {
        self.enable_partial_aggregation = enabled;
        self
    }

    pub fn with_worker_placements(mut self, placements: Vec<WorkerPlacementInfo>) -> Self {
        self.worker_placements = placements;
        self
    }

    pub fn is_distributed(&self) -> bool {
        self.shard_count > 1
    }
}

/// Annotation produced by the distribution pass for a single node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributionAnnotation {
    /// Column indices the output is partitioned by (empty = unpartitioned).
    pub partition_key: Vec<usize>,
    /// True when the stream is replicated (broadcast) to every shard.
    pub replicated: bool,
    /// Estimated byte volume flowing out of this operator.
    pub estimated_bytes: u64,
}

impl DistributionAnnotation {
    fn unpartitioned() -> Self {
        Self {
            partition_key: vec![],
            replicated: false,
            estimated_bytes: DEFAULT_UNKNOWN_SOURCE_BYTES,
        }
    }

    fn by_columns(cols: Vec<usize>) -> Self {
        Self {
            partition_key: cols,
            replicated: false,
            estimated_bytes: DEFAULT_UNKNOWN_SOURCE_BYTES,
        }
    }

    fn with_bytes(mut self, bytes: u64) -> Self {
        self.estimated_bytes = bytes;
        self
    }
}

/// Summary of one network exchange inserted into the distributed physical plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedExchange {
    pub kind: ExchangeKind,
    pub partition_key: Vec<usize>,
    pub expected_shuffle_bytes: u64,
    pub reason: String,
}

/// Full output of the distributed physical planning pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPlan {
    /// Transformed `PlanNode` tree with physical `Exchange` nodes inserted.
    pub plan: PlanNode,
    /// Output partitioning of the root operator.
    pub output_annotation: DistributionAnnotation,
    /// Number of target shards this plan was compiled for.
    pub shard_count: usize,
    /// All exchanges inserted into the plan (in post-order).
    pub exchanges: Vec<PlannedExchange>,
    /// Number of two-phase partial+final aggregations emitted.
    pub partial_aggregation_count: usize,
    /// Total estimated state bytes across stateful operators.
    pub estimated_state_bytes: u64,
    /// Total expected network shuffle bytes across all non-loopback exchanges.
    pub expected_shuffle_bytes: u64,
    /// Concrete worker placements, if supplied.
    pub worker_placements: Vec<WorkerPlacementInfo>,
}

/// Apply the single-shard distribution pass to a `PlanNode` tree.
///
/// Preserves the v0.7 API: returns the transformed tree with
/// `Exchange { kind: Loopback }` nodes inserted wherever partitioning
/// requirements change.
pub fn apply_distribution(plan: PlanNode) -> PlanNode {
    let config = DistributionConfig::single_shard();
    apply_distribution_with_config(plan, &config)
        .expect("single-shard loopback distribution cannot fail")
        .plan
}

/// Apply the distributed physical planning pass with explicit cluster and
/// statistics configuration.
pub fn apply_distribution_with_config(
    plan: PlanNode,
    config: &DistributionConfig,
) -> Result<DistributedPlan, SqlError> {
    let mut ctx = DistCtx {
        config,
        exchanges: Vec::new(),
        partial_aggregation_count: 0,
        estimated_state_bytes: 0,
        expected_shuffle_bytes: 0,
    };
    let (transformed, output_annotation) = distribute(plan, &mut ctx)?;
    Ok(DistributedPlan {
        plan: transformed,
        output_annotation,
        shard_count: config.shard_count,
        exchanges: ctx.exchanges,
        partial_aggregation_count: ctx.partial_aggregation_count,
        estimated_state_bytes: ctx.estimated_state_bytes,
        expected_shuffle_bytes: ctx.expected_shuffle_bytes,
        worker_placements: config.worker_placements.clone(),
    })
}

struct DistCtx<'a> {
    config: &'a DistributionConfig,
    exchanges: Vec<PlannedExchange>,
    partial_aggregation_count: usize,
    estimated_state_bytes: u64,
    expected_shuffle_bytes: u64,
}

impl<'a> DistCtx<'a> {
    fn record_exchange(
        &mut self,
        kind: ExchangeKind,
        partition_key: Vec<usize>,
        input_bytes: u64,
        reason: impl Into<String>,
    ) {
        let shuffle_bytes = match kind {
            ExchangeKind::Loopback => 0,
            ExchangeKind::Broadcast => input_bytes.saturating_mul(self.config.shard_count as u64),
            ExchangeKind::Hash | ExchangeKind::Range | ExchangeKind::Single => input_bytes,
        };
        self.expected_shuffle_bytes = self.expected_shuffle_bytes.saturating_add(shuffle_bytes);
        self.exchanges.push(PlannedExchange {
            kind,
            partition_key,
            expected_shuffle_bytes: shuffle_bytes,
            reason: reason.into(),
        });
    }

    fn make_exchange(
        &mut self,
        child: PlanNode,
        kind: ExchangeKind,
        partition_key: Vec<usize>,
        input_bytes: u64,
        reason: impl Into<String>,
    ) -> PlanNode {
        self.record_exchange(kind, partition_key.clone(), input_bytes, reason);
        PlanNode::Exchange {
            kind,
            partition_key,
            child: Box::new(child),
        }
    }
}

/// Remap upstream `partition_key` column indices through a `Project` expression
/// list. Every upstream partition column must appear verbatim as an
/// `Expr::Column(orig_idx)` in the projection; otherwise partitioning is lost
/// (`vec![]`).
fn remap_partition_key_through_project(
    input_key: &[usize],
    project_columns: &[Expr],
) -> Vec<usize> {
    if input_key.is_empty() {
        return Vec::new();
    }
    let mut remapped = Vec::with_capacity(input_key.len());
    for &orig_col in input_key {
        let Some(new_pos) = project_columns
            .iter()
            .position(|expr| matches!(expr, Expr::Column(idx) if *idx == orig_col))
        else {
            return Vec::new();
        };
        remapped.push(new_pos);
    }
    remapped
}

/// ADR 0004 merge-law admission check for two-phase partial + final aggregation.
///
/// Only `Sum` and `Count` without `DISTINCT` are admitted for checked `i64`
/// `WeightAdd/v1` / `SumCount/v1` partial pushdown. `Min`/`Max` require
/// `IndexedMultiset/v1` (non-retractable without full multiset state), `Avg`
/// requires separate sum/count decomposition, and `DISTINCT` / `Median` are
/// non-distributive.
fn partial_aggregation_admitted(plan: &PlanNode, aggregates: &[AggregateExpr]) -> bool {
    if aggregates.is_empty() {
        return false;
    }
    if !validate_pre_aggregation_algebra(plan).is_admitted() {
        return false;
    }
    aggregates
        .iter()
        .all(|agg| !agg.distinct && matches!(agg.func, AggregateFunc::Sum | AggregateFunc::Count))
}

/// Build the final-stage aggregate expressions corresponding to a partial
/// aggregation whose output schema is `[group_by_0..group_by_{g-1}, agg_0..agg_{a-1}]`.
///
/// For both `Sum` and `Count`, the partial stage emits integer partial sums /
/// counts, so the final stage sums those partial values (`AggregateFunc::Sum`).
fn final_aggregate_exprs(group_len: usize, partial_aggs: &[AggregateExpr]) -> Vec<AggregateExpr> {
    partial_aggs
        .iter()
        .enumerate()
        .map(|(i, _)| AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(group_len + i),
            distinct: false,
        })
        .collect()
}

/// Internal recursive pass. Returns `(transformed_plan, output_annotation)`.
fn distribute(
    plan: PlanNode,
    ctx: &mut DistCtx<'_>,
) -> Result<(PlanNode, DistributionAnnotation), SqlError> {
    let distributed = ctx.config.is_distributed();

    match plan {
        PlanNode::Source { name } => {
            let partition_key = ctx
                .config
                .source_partition_keys
                .get(&name)
                .cloned()
                .unwrap_or_default();
            let est_bytes = ctx
                .config
                .source_estimated_bytes
                .get(&name)
                .copied()
                .unwrap_or(DEFAULT_UNKNOWN_SOURCE_BYTES);
            let ann = DistributionAnnotation::by_columns(partition_key).with_bytes(est_bytes);
            Ok((PlanNode::Source { name }, ann))
        }

        PlanNode::Filter { input, predicate } => {
            let (new_input, ann) = distribute(*input, ctx)?;
            Ok((
                PlanNode::Filter {
                    input: Box::new(new_input),
                    predicate,
                },
                ann,
            ))
        }

        PlanNode::Project { input, columns } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            let remapped_key = if distributed {
                remap_partition_key_through_project(&input_ann.partition_key, &columns)
            } else {
                input_ann.partition_key.clone()
            };
            let out_ann = DistributionAnnotation {
                partition_key: remapped_key,
                replicated: input_ann.replicated,
                estimated_bytes: input_ann.estimated_bytes,
            };
            Ok((
                PlanNode::Project {
                    input: Box::new(new_input),
                    columns,
                },
                out_ann,
            ))
        }

        PlanNode::Map { input, func } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            // `Map` transforms/appends rows without guaranteed column-index
            // preservation in distributed mode unless the upstream key is empty.
            let out_ann = if distributed {
                DistributionAnnotation {
                    partition_key: Vec::new(),
                    replicated: input_ann.replicated,
                    estimated_bytes: input_ann.estimated_bytes,
                }
            } else {
                input_ann
            };
            Ok((
                PlanNode::Map {
                    input: Box::new(new_input),
                    func,
                },
                out_ann,
            ))
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;

            let mut group_cols = Vec::with_capacity(group_by.len());
            for expr in &group_by {
                if let Expr::Column(idx) = expr {
                    group_cols.push(*idx);
                } else if distributed {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: "Aggregate(non-column GROUP BY expression in distributed mode)"
                            .to_owned(),
                    });
                }
            }

            ctx.estimated_state_bytes = ctx
                .estimated_state_bytes
                .saturating_add(input_ann.estimated_bytes / 2);

            if !distributed {
                let needs_exchange = input_ann.partition_key != group_cols;
                let actual_input = if needs_exchange {
                    ctx.make_exchange(
                        new_input,
                        ExchangeKind::Loopback,
                        group_cols.clone(),
                        input_ann.estimated_bytes,
                        "aggregate_loopback",
                    )
                } else {
                    new_input
                };
                let out_ann = DistributionAnnotation::by_columns(group_cols)
                    .with_bytes(input_ann.estimated_bytes / 2);
                return Ok((
                    PlanNode::Aggregate {
                        input: Box::new(actual_input),
                        group_by,
                        aggregates,
                    },
                    out_ann,
                ));
            }

            // Distributed mode (`shard_count > 1`):
            // Output of Aggregate has group_by columns at 0..group_cols.len().
            let out_group_positions: Vec<usize> = (0..group_cols.len()).collect();

            if !group_cols.is_empty() && input_ann.partition_key == group_cols {
                // Already co-partitioned by the GROUP BY columns: zero shuffle!
                let out_ann = DistributionAnnotation::by_columns(out_group_positions)
                    .with_bytes(input_ann.estimated_bytes / 2);
                return Ok((
                    PlanNode::Aggregate {
                        input: Box::new(new_input),
                        group_by,
                        aggregates,
                    },
                    out_ann,
                ));
            }

            let probe_agg = PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "_probe".to_owned(),
                }),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            };

            let exchange_kind = if group_cols.is_empty() {
                ExchangeKind::Single
            } else {
                ExchangeKind::Hash
            };

            if ctx.config.enable_partial_aggregation
                && partial_aggregation_admitted(&probe_agg, &aggregates)
            {
                // Two-phase Partial -> Exchange -> Final aggregation (ADR 0004).
                let partial_bytes = (input_ann.estimated_bytes / 4).max(1);
                let partial_node = PlanNode::Aggregate {
                    input: Box::new(new_input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                };
                let exchanged = ctx.make_exchange(
                    partial_node,
                    exchange_kind,
                    out_group_positions.clone(),
                    partial_bytes,
                    if group_cols.is_empty() {
                        "partial_final_global_aggregate"
                    } else {
                        "partial_final_hash_aggregate"
                    },
                );
                let final_group_by: Vec<Expr> = out_group_positions
                    .iter()
                    .copied()
                    .map(Expr::Column)
                    .collect();
                let final_aggs = final_aggregate_exprs(group_cols.len(), &aggregates);
                ctx.partial_aggregation_count += 1;
                let out_ann = DistributionAnnotation::by_columns(out_group_positions)
                    .with_bytes(partial_bytes);
                return Ok((
                    PlanNode::Aggregate {
                        input: Box::new(exchanged),
                        group_by: final_group_by,
                        aggregates: final_aggs,
                    },
                    out_ann,
                ));
            }

            // Single-phase Exchange -> Aggregate (non-distributive or partial disabled).
            let exchanged = ctx.make_exchange(
                new_input,
                exchange_kind,
                group_cols.clone(),
                input_ann.estimated_bytes,
                if group_cols.is_empty() {
                    "global_aggregate_gather"
                } else {
                    "aggregate_hash_repartition"
                },
            );
            let out_ann = DistributionAnnotation::by_columns(out_group_positions)
                .with_bytes(input_ann.estimated_bytes / 2);
            Ok((
                PlanNode::Aggregate {
                    input: Box::new(exchanged),
                    group_by,
                    aggregates,
                },
                out_ann,
            ))
        }

        PlanNode::Exchange {
            kind,
            partition_key,
            child,
        } => {
            let (new_child, child_ann) = distribute(*child, ctx)?;
            let out_ann = match kind {
                ExchangeKind::Hash | ExchangeKind::Range => {
                    DistributionAnnotation::by_columns(partition_key.clone())
                        .with_bytes(child_ann.estimated_bytes)
                }
                ExchangeKind::Broadcast => DistributionAnnotation {
                    partition_key: vec![],
                    replicated: true,
                    estimated_bytes: child_ann.estimated_bytes,
                },
                ExchangeKind::Loopback | ExchangeKind::Single => {
                    DistributionAnnotation::unpartitioned().with_bytes(child_ann.estimated_bytes)
                }
            };
            Ok((
                PlanNode::Exchange {
                    kind,
                    partition_key,
                    child: Box::new(new_child),
                },
                out_ann,
            ))
        }

        PlanNode::ViewSink {
            view_name,
            pk,
            child,
        } => {
            let (new_child, ann) = distribute(*child, ctx)?;
            Ok((
                PlanNode::ViewSink {
                    view_name,
                    pk,
                    child: Box::new(new_child),
                },
                ann,
            ))
        }

        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            semantics,
        } => {
            let (new_left, left_ann) = distribute(*left, ctx)?;
            let (new_right, right_ann) = distribute(*right, ctx)?;
            ctx.estimated_state_bytes = ctx
                .estimated_state_bytes
                .saturating_add(left_ann.estimated_bytes)
                .saturating_add(right_ann.estimated_bytes);

            if !distributed {
                return Ok((
                    PlanNode::InnerJoin {
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        semantics,
                    },
                    DistributionAnnotation::unpartitioned(),
                ));
            }

            if left_keys.is_empty() || right_keys.is_empty() || left_keys.len() != right_keys.len()
            {
                // Cross join: only admissible if one side fits the broadcast bound.
                if right_ann.estimated_bytes <= ctx.config.broadcast_threshold_bytes {
                    let bcast_right = ctx.make_exchange(
                        new_right,
                        ExchangeKind::Broadcast,
                        vec![],
                        right_ann.estimated_bytes,
                        "broadcast_cross_join_right",
                    );
                    let out_ann = DistributionAnnotation {
                        partition_key: left_ann.partition_key.clone(),
                        replicated: false,
                        estimated_bytes: left_ann
                            .estimated_bytes
                            .saturating_add(right_ann.estimated_bytes),
                    };
                    return Ok((
                        PlanNode::InnerJoin {
                            left: Box::new(new_left),
                            right: Box::new(bcast_right),
                            left_keys,
                            right_keys,
                            left_arr_id,
                            right_arr_id,
                            semantics,
                        },
                        out_ann,
                    ));
                }
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "InnerJoin(cross/non-equi join exceeding broadcast threshold in distributed mode)".to_owned(),
                });
            }

            // 1. Co-partitioned join: both sides already partitioned by join keys.
            if left_ann.partition_key == left_keys && right_ann.partition_key == right_keys {
                let out_ann = DistributionAnnotation::by_columns(left_keys.clone()).with_bytes(
                    left_ann
                        .estimated_bytes
                        .saturating_add(right_ann.estimated_bytes),
                );
                return Ok((
                    PlanNode::InnerJoin {
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        semantics,
                    },
                    out_ann,
                ));
            }

            // 2. Size-bounded broadcast join: broadcast the smaller build side if
            //    it is within `broadcast_threshold_bytes`.
            if right_ann.estimated_bytes <= ctx.config.broadcast_threshold_bytes
                && right_ann.estimated_bytes <= left_ann.estimated_bytes
            {
                let bcast_right = if right_ann.replicated {
                    new_right
                } else {
                    ctx.make_exchange(
                        new_right,
                        ExchangeKind::Broadcast,
                        vec![],
                        right_ann.estimated_bytes,
                        "broadcast_inner_join_right",
                    )
                };
                let out_ann = DistributionAnnotation {
                    partition_key: left_ann.partition_key.clone(),
                    replicated: false,
                    estimated_bytes: left_ann
                        .estimated_bytes
                        .saturating_add(right_ann.estimated_bytes),
                };
                return Ok((
                    PlanNode::InnerJoin {
                        left: Box::new(new_left),
                        right: Box::new(bcast_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        semantics,
                    },
                    out_ann,
                ));
            }

            // 3. Hash repartition either/both sides whose partitioning differs
            //    from the join keys.
            let final_left = if left_ann.partition_key == left_keys {
                new_left
            } else {
                ctx.make_exchange(
                    new_left,
                    ExchangeKind::Hash,
                    left_keys.clone(),
                    left_ann.estimated_bytes,
                    "inner_join_hash_repartition_left",
                )
            };
            let final_right = if right_ann.partition_key == right_keys {
                new_right
            } else {
                ctx.make_exchange(
                    new_right,
                    ExchangeKind::Hash,
                    right_keys.clone(),
                    right_ann.estimated_bytes,
                    "inner_join_hash_repartition_right",
                )
            };
            let out_ann = DistributionAnnotation::by_columns(left_keys.clone()).with_bytes(
                left_ann
                    .estimated_bytes
                    .saturating_add(right_ann.estimated_bytes),
            );
            Ok((
                PlanNode::InnerJoin {
                    left: Box::new(final_left),
                    right: Box::new(final_right),
                    left_keys,
                    right_keys,
                    left_arr_id,
                    right_arr_id,
                    semantics,
                },
                out_ann,
            ))
        }

        PlanNode::OuterJoin {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            unmatched_arr_id,
        } => {
            let (new_left, left_ann) = distribute(*left, ctx)?;
            let (new_right, right_ann) = distribute(*right, ctx)?;
            ctx.estimated_state_bytes = ctx
                .estimated_state_bytes
                .saturating_add(left_ann.estimated_bytes)
                .saturating_add(right_ann.estimated_bytes);

            if !distributed {
                return Ok((
                    PlanNode::OuterJoin {
                        kind,
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        unmatched_arr_id,
                    },
                    DistributionAnnotation::unpartitioned(),
                ));
            }

            if left_keys.is_empty() || right_keys.is_empty() || left_keys.len() != right_keys.len()
            {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: format!(
                        "OuterJoin[{kind:?}](non-equi join is not supported in distributed mode)"
                    ),
                });
            }

            let out_partition = match kind {
                OuterJoinKind::Left
                | OuterJoinKind::Semi
                | OuterJoinKind::Anti
                | OuterJoinKind::NullAwareAnti => left_keys.clone(),
                OuterJoinKind::Right | OuterJoinKind::Full => Vec::new(),
            };

            // 1. Co-partitioned outer join.
            if left_ann.partition_key == left_keys && right_ann.partition_key == right_keys {
                let out_ann = DistributionAnnotation::by_columns(out_partition).with_bytes(
                    left_ann
                        .estimated_bytes
                        .saturating_add(right_ann.estimated_bytes),
                );
                return Ok((
                    PlanNode::OuterJoin {
                        kind,
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        unmatched_arr_id,
                    },
                    out_ann,
                ));
            }

            // 2. Broadcast right build side only for Left / Semi / Anti joins.
            let right_broadcast_safe = matches!(
                kind,
                OuterJoinKind::Left
                    | OuterJoinKind::Semi
                    | OuterJoinKind::Anti
                    | OuterJoinKind::NullAwareAnti
            );
            if right_broadcast_safe
                && right_ann.estimated_bytes <= ctx.config.broadcast_threshold_bytes
                && right_ann.estimated_bytes <= left_ann.estimated_bytes
            {
                let bcast_right = if right_ann.replicated {
                    new_right
                } else {
                    ctx.make_exchange(
                        new_right,
                        ExchangeKind::Broadcast,
                        vec![],
                        right_ann.estimated_bytes,
                        "broadcast_outer_join_right",
                    )
                };
                let out_ann = DistributionAnnotation {
                    partition_key: left_ann.partition_key.clone(),
                    replicated: false,
                    estimated_bytes: left_ann
                        .estimated_bytes
                        .saturating_add(right_ann.estimated_bytes),
                };
                return Ok((
                    PlanNode::OuterJoin {
                        kind,
                        left: Box::new(new_left),
                        right: Box::new(bcast_right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        unmatched_arr_id,
                    },
                    out_ann,
                ));
            }

            // 3. Hash repartition mismatched sides.
            let final_left = if left_ann.partition_key == left_keys {
                new_left
            } else {
                ctx.make_exchange(
                    new_left,
                    ExchangeKind::Hash,
                    left_keys.clone(),
                    left_ann.estimated_bytes,
                    "outer_join_hash_repartition_left",
                )
            };
            let final_right = if right_ann.partition_key == right_keys {
                new_right
            } else {
                ctx.make_exchange(
                    new_right,
                    ExchangeKind::Hash,
                    right_keys.clone(),
                    right_ann.estimated_bytes,
                    "outer_join_hash_repartition_right",
                )
            };
            let out_ann = DistributionAnnotation::by_columns(out_partition).with_bytes(
                left_ann
                    .estimated_bytes
                    .saturating_add(right_ann.estimated_bytes),
            );
            Ok((
                PlanNode::OuterJoin {
                    kind,
                    left: Box::new(final_left),
                    right: Box::new(final_right),
                    left_keys,
                    right_keys,
                    left_arr_id,
                    right_arr_id,
                    unmatched_arr_id,
                },
                out_ann,
            ))
        }

        PlanNode::Distinct { input, arr_id } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            ctx.estimated_state_bytes = ctx
                .estimated_state_bytes
                .saturating_add(input_ann.estimated_bytes);
            let kind = if distributed {
                ExchangeKind::Hash
            } else {
                ExchangeKind::Loopback
            };
            let actual_input = ctx.make_exchange(
                new_input,
                kind,
                vec![],
                input_ann.estimated_bytes,
                "distinct_full_row_hash",
            );
            Ok((
                PlanNode::Distinct {
                    input: Box::new(actual_input),
                    arr_id,
                },
                DistributionAnnotation::unpartitioned().with_bytes(input_ann.estimated_bytes),
            ))
        }

        PlanNode::Intersect {
            left,
            right,
            all,
            left_arr_id,
            right_arr_id,
        } => {
            let (new_left, left_ann) = distribute(*left, ctx)?;
            let (new_right, right_ann) = distribute(*right, ctx)?;
            let kind = if distributed {
                ExchangeKind::Hash
            } else {
                ExchangeKind::Loopback
            };
            let ex_left = ctx.make_exchange(
                new_left,
                kind,
                vec![],
                left_ann.estimated_bytes,
                "intersect_left_hash",
            );
            let ex_right = ctx.make_exchange(
                new_right,
                kind,
                vec![],
                right_ann.estimated_bytes,
                "intersect_right_hash",
            );
            Ok((
                PlanNode::Intersect {
                    left: Box::new(ex_left),
                    right: Box::new(ex_right),
                    all,
                    left_arr_id,
                    right_arr_id,
                },
                DistributionAnnotation::unpartitioned(),
            ))
        }

        PlanNode::Except {
            left,
            right,
            all,
            left_arr_id,
            right_arr_id,
        } => {
            let (new_left, left_ann) = distribute(*left, ctx)?;
            let (new_right, right_ann) = distribute(*right, ctx)?;
            let kind = if distributed {
                ExchangeKind::Hash
            } else {
                ExchangeKind::Loopback
            };
            let ex_left = ctx.make_exchange(
                new_left,
                kind,
                vec![],
                left_ann.estimated_bytes,
                "except_left_hash",
            );
            let ex_right = ctx.make_exchange(
                new_right,
                kind,
                vec![],
                right_ann.estimated_bytes,
                "except_right_hash",
            );
            Ok((
                PlanNode::Except {
                    left: Box::new(ex_left),
                    right: Box::new(ex_right),
                    all,
                    left_arr_id,
                    right_arr_id,
                },
                DistributionAnnotation::unpartitioned(),
            ))
        }

        PlanNode::Window {
            input,
            window_exprs,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            let partition_by = window_exprs
                .first()
                .map(|w| w.partition_by.clone())
                .unwrap_or_default();
            if distributed && window_exprs.iter().any(|w| w.partition_by != partition_by) {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "Window(mismatched PARTITION BY keys in distributed mode)"
                        .to_owned(),
                });
            }
            let actual_input = if !distributed {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Loopback,
                    partition_by.clone(),
                    input_ann.estimated_bytes,
                    "window_loopback",
                )
            } else if partition_by.is_empty() {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Single,
                    vec![],
                    input_ann.estimated_bytes,
                    "window_global_gather",
                )
            } else if input_ann.partition_key == partition_by {
                new_input
            } else {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Hash,
                    partition_by.clone(),
                    input_ann.estimated_bytes,
                    "window_hash_repartition",
                )
            };
            Ok((
                PlanNode::Window {
                    input: Box::new(actual_input),
                    window_exprs,
                },
                DistributionAnnotation::by_columns(partition_by)
                    .with_bytes(input_ann.estimated_bytes),
            ))
        }

        PlanNode::TumbleWindow {
            input,
            time_col,
            window_size_ms,
            late_data_policy,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            let kind = if distributed {
                ExchangeKind::Hash
            } else {
                ExchangeKind::Loopback
            };
            let actual_input = if distributed && !input_ann.partition_key.is_empty() {
                new_input
            } else {
                ctx.make_exchange(
                    new_input,
                    kind,
                    vec![time_col],
                    input_ann.estimated_bytes,
                    "tumble_window_partition",
                )
            };
            Ok((
                PlanNode::TumbleWindow {
                    input: Box::new(actual_input),
                    time_col,
                    window_size_ms,
                    late_data_policy,
                },
                DistributionAnnotation::by_columns(input_ann.partition_key)
                    .with_bytes(input_ann.estimated_bytes),
            ))
        }

        PlanNode::HopWindow {
            input,
            time_col,
            window_size_ms,
            slide_ms,
            late_data_policy,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            let kind = if distributed {
                ExchangeKind::Hash
            } else {
                ExchangeKind::Loopback
            };
            let actual_input = ctx.make_exchange(
                new_input,
                kind,
                vec![time_col],
                input_ann.estimated_bytes,
                "hop_window_partition",
            );
            Ok((
                PlanNode::HopWindow {
                    input: Box::new(actual_input),
                    time_col,
                    window_size_ms,
                    slide_ms,
                    late_data_policy,
                },
                DistributionAnnotation::unpartitioned(),
            ))
        }

        PlanNode::SessionWindow {
            input,
            time_col,
            gap_ms,
            late_data_policy,
            partition_by,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            if distributed && partition_by.as_ref().is_none_or(|p| p.is_empty()) {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "SessionWindow(missing explicit PARTITION BY in distributed mode)"
                        .to_owned(),
                });
            }
            let pkey = partition_by.clone().unwrap_or_default();
            let actual_input = if !distributed {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Loopback,
                    pkey.clone(),
                    input_ann.estimated_bytes,
                    "session_window_loopback",
                )
            } else if input_ann.partition_key == pkey {
                new_input
            } else {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Hash,
                    pkey.clone(),
                    input_ann.estimated_bytes,
                    "session_window_hash_repartition",
                )
            };
            Ok((
                PlanNode::SessionWindow {
                    input: Box::new(actual_input),
                    time_col,
                    gap_ms,
                    late_data_policy,
                    partition_by,
                },
                DistributionAnnotation::by_columns(pkey).with_bytes(input_ann.estimated_bytes),
            ))
        }

        PlanNode::TopK {
            input,
            k,
            rank_col,
            partition_by,
            order_by,
        } => {
            let (new_input, input_ann) = distribute(*input, ctx)?;
            let actual_input = if !distributed {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Loopback,
                    partition_by.clone(),
                    input_ann.estimated_bytes,
                    "topk_loopback",
                )
            } else if partition_by.is_empty() {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Single,
                    vec![],
                    input_ann.estimated_bytes,
                    "topk_global_gather",
                )
            } else if input_ann.partition_key == partition_by {
                new_input
            } else {
                ctx.make_exchange(
                    new_input,
                    ExchangeKind::Hash,
                    partition_by.clone(),
                    input_ann.estimated_bytes,
                    "topk_hash_repartition",
                )
            };
            Ok((
                PlanNode::TopK {
                    input: Box::new(actual_input),
                    k,
                    rank_col,
                    partition_by: partition_by.clone(),
                    order_by,
                },
                DistributionAnnotation::by_columns(partition_by)
                    .with_bytes(input_ann.estimated_bytes),
            ))
        }

        PlanNode::Recursion { .. } if distributed => Err(SqlError::UnsupportedPlanNode {
            node_type: "Recursion(multi-shard distributed recursion is not supported)".to_owned(),
        }),

        other => Ok((other, DistributionAnnotation::unpartitioned())),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{
        AggregateExpr, AggregateFunc, ExchangeKind, Expr, JoinSemantics, PlanNode, WindowExpr,
        WindowFunc,
    };
    use rockstream_types::ids::OperatorId;

    fn source(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
        }
    }

    fn filter_node(input: PlanNode) -> PlanNode {
        PlanNode::Filter {
            input: Box::new(input),
            predicate: Expr::Column(0),
        }
    }

    fn aggregate_node(input: PlanNode) -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(input),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    #[test]
    fn source_has_no_exchange() {
        let plan = source("t");
        let result = apply_distribution(plan);
        assert!(matches!(result, PlanNode::Source { .. }));
    }

    #[test]
    fn filter_preserves_no_exchange() {
        let plan = filter_node(source("t"));
        let result = apply_distribution(plan);
        assert!(matches!(result, PlanNode::Filter { .. }));
        if let PlanNode::Filter { input, .. } = &result {
            assert!(matches!(input.as_ref(), PlanNode::Source { .. }));
        }
    }

    #[test]
    fn aggregate_inserts_loopback_exchange() {
        let plan = aggregate_node(source("t"));
        let result = apply_distribution(plan);
        if let PlanNode::Aggregate { input, .. } = &result {
            assert!(
                matches!(
                    input.as_ref(),
                    PlanNode::Exchange {
                        kind: ExchangeKind::Loopback,
                        ..
                    }
                ),
                "expected Loopback exchange before Aggregate, got: {input:?}"
            );
        } else {
            panic!("expected Aggregate, got: {result:?}");
        }
    }

    #[test]
    fn distribution_annotation_by_columns() {
        let ann = DistributionAnnotation::by_columns(vec![0, 1]);
        assert_eq!(ann.partition_key, vec![0, 1]);
    }

    #[test]
    fn distinct_inserts_loopback_exchange() {
        let plan = PlanNode::Distinct {
            input: Box::new(source("t")),
            arr_id: OperatorId(0),
        };
        let result = apply_distribution(plan);
        if let PlanNode::Distinct { input, .. } = &result {
            assert!(matches!(
                input.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
        } else {
            panic!("expected Distinct, got: {result:?}");
        }
    }

    #[test]
    fn intersect_inserts_loopback_on_both_sides() {
        let plan = PlanNode::Intersect {
            left: Box::new(source("l")),
            right: Box::new(source("r")),
            all: false,
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
        };
        let result = apply_distribution(plan);
        if let PlanNode::Intersect { left, right, .. } = &result {
            assert!(matches!(
                left.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
            assert!(matches!(
                right.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
        } else {
            panic!("expected Intersect, got: {result:?}");
        }
    }

    #[test]
    fn window_inserts_loopback_exchange() {
        let plan = PlanNode::Window {
            input: Box::new(source("t")),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };
        let result = apply_distribution(plan);
        if let PlanNode::Window { input, .. } = &result {
            assert!(matches!(
                input.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
        } else {
            panic!("expected Window, got: {result:?}");
        }
    }

    #[test]
    fn except_inserts_loopback_on_both_sides() {
        let plan = PlanNode::Except {
            left: Box::new(source("l")),
            right: Box::new(source("r")),
            all: true,
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
        };
        let result = apply_distribution(plan);
        if let PlanNode::Except {
            left, right, all, ..
        } = &result
        {
            assert!(*all);
            assert!(matches!(
                left.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
            assert!(matches!(
                right.as_ref(),
                PlanNode::Exchange {
                    kind: ExchangeKind::Loopback,
                    ..
                }
            ));
        } else {
            panic!("expected Except, got: {result:?}");
        }
    }

    #[test]
    fn distribution_tumble_window_plan() {
        use rockstream_plan::LateDataPolicy;
        let plan = PlanNode::TumbleWindow {
            input: Box::new(source("t")),
            time_col: 0,
            window_size_ms: 1000,
            late_data_policy: LateDataPolicy::Drop,
        };
        let result = apply_distribution(plan);
        assert!(matches!(
            result,
            PlanNode::TumbleWindow { ref input, .. }
                if matches!(input.as_ref(), PlanNode::Exchange { kind: ExchangeKind::Loopback, .. })
        ));
    }

    #[test]
    fn distribution_topk_plan() {
        let plan = PlanNode::TopK {
            input: Box::new(source("t")),
            k: 3,
            rank_col: 0,
            partition_by: vec![],
            order_by: vec![],
        };
        let result = apply_distribution(plan);
        assert!(matches!(
            result,
            PlanNode::TopK { ref input, .. }
                if matches!(input.as_ref(), PlanNode::Exchange { kind: ExchangeKind::Loopback, .. })
        ));
    }

    // ── Distributed multi-shard planning tests (Issue #400 R3) ───────────────

    #[test]
    fn distributed_copartitioned_join_emits_zero_exchanges() {
        let plan = PlanNode::InnerJoin {
            left: Box::new(source("orders")),
            right: Box::new(source("customers")),
            left_keys: vec![1],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        };
        let cfg = DistributionConfig::distributed(4)
            .with_source_partition("orders", vec![1])
            .with_source_partition("customers", vec![0]);
        let dist = apply_distribution_with_config(plan, &cfg).unwrap();
        assert_eq!(dist.exchanges, vec![]);
        assert_eq!(dist.expected_shuffle_bytes, 0);
        assert_eq!(dist.output_annotation.partition_key, vec![1]);
    }

    #[test]
    fn distributed_project_remaps_partition_key_accurately() {
        // Source partitioned on column 0; Project swaps columns [col(2), col(0)]
        // so partition key shifts from [0] to [1]. Aggregate on col(1) needs NO exchange!
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Project {
                input: Box::new(source("orders")),
                columns: vec![Expr::Column(2), Expr::Column(0), Expr::Column(1)],
            }),
            group_by: vec![Expr::Column(1)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(2),
                distinct: false,
            }],
        };
        let cfg = DistributionConfig::distributed(4).with_source_partition("orders", vec![0]);
        let dist = apply_distribution_with_config(plan, &cfg).unwrap();
        assert_eq!(dist.exchanges, vec![]);
        assert_eq!(dist.expected_shuffle_bytes, 0);
    }

    #[test]
    fn distributed_hash_repartition_and_partial_final_aggregate() {
        // orders partitioned on col 0 (order_id), joined on col 1 (cust_id) with
        // customers partitioned on col 0 (cust_id), then GROUP BY col 2 (region)
        // with SUM(col 3) -> Hash exchange on left join input, then Partial->Hash->Final agg.
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::InnerJoin {
                left: Box::new(source("orders")),
                right: Box::new(source("customers")),
                left_keys: vec![1],
                right_keys: vec![0],
                left_arr_id: OperatorId(1),
                right_arr_id: OperatorId(2),
                semantics: JoinSemantics::default(),
            }),
            group_by: vec![Expr::Column(2)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(3),
                distinct: false,
            }],
        };
        let cfg = DistributionConfig::distributed(4)
            .with_source_partition("orders", vec![0])
            .with_source_partition("customers", vec![0])
            .with_source_estimated_bytes("orders", 100 * 1024 * 1024)
            .with_source_estimated_bytes("customers", 80 * 1024 * 1024);
        let dist = apply_distribution_with_config(plan, &cfg).unwrap();
        assert_eq!(dist.partial_aggregation_count, 1);
        assert_eq!(dist.exchanges.len(), 2);
        assert_eq!(dist.exchanges[0].kind, ExchangeKind::Hash);
        assert_eq!(dist.exchanges[0].partition_key, vec![1]);
        assert_eq!(dist.exchanges[1].kind, ExchangeKind::Hash);
        assert_eq!(dist.exchanges[1].partition_key, vec![0]);
        assert!(dist.expected_shuffle_bytes > 0);
    }

    #[test]
    fn distributed_size_bounded_broadcast_join() {
        let plan = PlanNode::InnerJoin {
            left: Box::new(source("fact_orders")),
            right: Box::new(source("dim_regions")),
            left_keys: vec![2],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        };
        let cfg = DistributionConfig::distributed(4)
            .with_source_partition("fact_orders", vec![0])
            .with_source_estimated_bytes("fact_orders", 200 * 1024 * 1024)
            .with_source_estimated_bytes("dim_regions", 1024 * 1024)
            .with_broadcast_threshold_bytes(16 * 1024 * 1024);
        let dist = apply_distribution_with_config(plan, &cfg).unwrap();
        assert_eq!(dist.exchanges.len(), 1);
        assert_eq!(dist.exchanges[0].kind, ExchangeKind::Broadcast);
        assert_eq!(dist.expected_shuffle_bytes, 4 * 1024 * 1024);
        assert_eq!(dist.output_annotation.partition_key, vec![0]);
    }

    #[test]
    fn distributed_rejects_unsupported_shapes_fail_closed() {
        let recursive = PlanNode::Recursion {
            base: Box::new(source("edges")),
            step: Box::new(source("edges")),
            max_iterations: 16,
            monotone: true,
        };
        let cfg = DistributionConfig::distributed(4);
        let err = apply_distribution_with_config(recursive, &cfg).unwrap_err();
        assert_eq!(err.error_code().to_string(), "RS-1013");

        let cross_join = PlanNode::InnerJoin {
            left: Box::new(source("big_a")),
            right: Box::new(source("big_b")),
            left_keys: vec![],
            right_keys: vec![],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        };
        let err = apply_distribution_with_config(cross_join, &cfg).unwrap_err();
        assert_eq!(err.error_code().to_string(), "RS-1013");
    }
}
