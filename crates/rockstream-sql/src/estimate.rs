//! `EXPLAIN INCREMENTAL ESTIMATE` — calibrated capacity model (v0.59.23).
//!
//! Produces a per-operator and end-to-end capacity estimate based on canonical
//! arrangements, physical execution strategy, source statistic provenance, and
//! reference sizing profiles.

use rockstream_plan::{AggregateFunc, PlanNode};
use rockstream_types::arrangement::CanonicalType;
use rockstream_types::capacity::{
    CapacityEstimate, CapacityProfile, PhysicalStrategy, SourceStatisticProvenance,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Public types ─────────────────────────────────────────────────────────────

/// A single row in the `EXPLAIN INCREMENTAL ESTIMATE` operator table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EstimateRow {
    /// Operator kind label (e.g. `"Source[t]"`, `"Filter"`, `"Aggregate"`, `"InnerJoin[classic]"`).
    pub operator_kind: String,
    /// Estimated arrangement state in bytes (0 for stateless operators).
    pub predicted_state_bytes: u64,
    /// Estimated time in milliseconds to process one epoch of `batch_rows` rows.
    pub epoch_ms: f64,
}

/// Canonical arrangement entry describing an existing or candidate arrangement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalArrangementEntry {
    pub arrangement_id: String,
    pub state_bytes: u64,
    pub consumers: Vec<String>,
}

/// Context supplied to the capacity estimator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityEstimateContext {
    pub cardinality_hint: u64,
    pub batch_rows: u64,
    pub profile: CapacityProfile,
    pub selected_strategy: PhysicalStrategy,
    pub key_type: Option<CanonicalType>,
    pub val_type: Option<CanonicalType>,
    pub fanout: Option<u64>,
    pub canonical_arrangements: Vec<CanonicalArrangementEntry>,
    pub provenance: Vec<SourceStatisticProvenance>,
}

impl Default for CapacityEstimateContext {
    fn default() -> Self {
        Self {
            cardinality_hint: 1_000,
            batch_rows: 10_000,
            profile: CapacityProfile::Medium,
            selected_strategy: PhysicalStrategy::Classic,
            key_type: Some(CanonicalType::Int64),
            val_type: Some(CanonicalType::Int64),
            fanout: Some(1),
            canonical_arrangements: Vec::new(),
            provenance: vec![SourceStatisticProvenance::Fallback {
                table_name: "default".to_string(),
                estimated_rows: 1_000,
            }],
        }
    }
}

/// Compute a static cost estimate for the `plan` tree (legacy convenience).
pub fn explain_incremental_estimate(
    plan: &PlanNode,
    cardinality_hint: u64,
    batch_rows: u64,
) -> Vec<EstimateRow> {
    let ctx = CapacityEstimateContext {
        cardinality_hint,
        batch_rows,
        ..Default::default()
    };
    let (_, rows) = explain_incremental_estimate_capacity(plan, &ctx);
    rows
}

/// Compute a calibrated capacity estimate and per-operator breakdown for the `plan` tree.
pub fn explain_incremental_estimate_capacity(
    plan: &PlanNode,
    ctx: &CapacityEstimateContext,
) -> (CapacityEstimate, Vec<EstimateRow>) {
    let mut rows = Vec::new();
    estimate_node(plan, ctx, &mut rows);

    let key_size = key_byte_size(ctx.key_type.as_ref());
    let val_size = val_byte_size(ctx.val_type.as_ref());
    let fanout = ctx.fanout.unwrap_or(1).max(1);

    // Sum operator state
    let total_op_state: u64 = rows.iter().map(|r| r.predicted_state_bytes).sum();

    // Canonical arrangement sharing resolution
    let (
        maintained_arrangements,
        consumer_count,
        shared_state_bytes,
        private_state_bytes,
        saved_bytes,
    ) = if !ctx.canonical_arrangements.is_empty() {
        let mut unique_arrs = BTreeMap::new();
        let mut total_consumers = 0;
        let mut unshared_total = 0u64;

        for entry in &ctx.canonical_arrangements {
            unique_arrs.insert(entry.arrangement_id.clone(), entry.state_bytes);
            let consumers_count = entry.consumers.len().max(1);
            total_consumers += consumers_count;
            unshared_total += entry.state_bytes * consumers_count as u64;
        }

        let shared_bytes: u64 = unique_arrs.values().sum();
        let saved = unshared_total.saturating_sub(shared_bytes);

        (
            unique_arrs.len(),
            total_consumers,
            shared_bytes,
            0u64,
            saved,
        )
    } else {
        (1, 1, 0u64, total_op_state, 0u64)
    };

    let effective_state = if shared_state_bytes > 0 {
        shared_state_bytes
    } else {
        private_state_bytes
    };

    let workers = ctx.profile.worker_count() as u64;
    let rss_bytes = effective_state * 3 + workers * 64 * 1024 * 1024;
    let spill_bytes = if ctx.profile == CapacityProfile::Large && effective_state > 50 * 1024 * 1024
    {
        effective_state / 4
    } else {
        0
    };
    let cache_hit_ratio = if spill_bytes > 0 { 0.85 } else { 0.98 };

    let total_epoch_ms: f64 = rows.iter().map(|r| r.epoch_ms).sum::<f64>().max(0.5);
    let commit_group_duration_ms = (total_epoch_ms * 0.35).max(1.0);
    let p99_freshness_ms = total_epoch_ms + commit_group_duration_ms + 10.0;

    let is_shuffle = rows.iter().any(|r| {
        r.operator_kind.starts_with("InnerJoin") || r.operator_kind.starts_with("Exchange")
    });
    let shuffle_bytes = if is_shuffle {
        ctx.batch_rows * (key_size + val_size) * workers
    } else {
        0
    };

    let logical_writes = ctx.batch_rows;
    let write_amp = match &ctx.selected_strategy {
        PhysicalStrategy::Factorized {
            delta_amplification,
            ..
        } => *delta_amplification,
        PhysicalStrategy::Classic => {
            if fanout > 1 {
                1.25
            } else {
                1.05
            }
        }
    };
    let physical_writes = (logical_writes as f64 * write_amp).ceil() as u64;
    let object_store_requests = (physical_writes / 500).max(1);
    let checkpoint_cost_ms = 10.0 + (effective_state as f64 / 1_000_000.0) * 0.5;
    let compaction_debt_bytes = 0;

    let estimate = CapacityEstimate {
        private_state_bytes,
        shared_state_bytes,
        saved_bytes,
        rss_bytes,
        spill_bytes,
        cache_hit_ratio,
        epoch_duration_ms: total_epoch_ms,
        commit_group_duration_ms,
        p99_freshness_ms,
        shuffle_bytes,
        logical_writes,
        physical_writes,
        object_store_requests,
        checkpoint_cost_ms,
        compaction_debt_bytes,
        consumer_count,
        maintained_arrangements,
        selected_strategy: ctx.selected_strategy.clone(),
        provenance: ctx.provenance.clone(),
    };

    (estimate, rows)
}

/// Format an estimate as a human-readable multi-line string (backward compatible table).
pub fn format_estimate(rows: &[EstimateRow]) -> String {
    let mut out = String::new();
    out.push_str("EXPLAIN INCREMENTAL ESTIMATE\n");
    out.push_str("───────────────────────────────────────────────────\n");
    out.push_str("Operator                        state_bytes   epoch_ms\n");
    out.push_str("───────────────────────────────────────────────────\n");
    for row in rows {
        out.push_str(&format!(
            "{:<32} {:>10}   {:>8.2}\n",
            row.operator_kind, row.predicted_state_bytes, row.epoch_ms
        ));
    }
    out.push_str("───────────────────────────────────────────────────\n");
    out.push_str("(estimates only; no operators deployed)\n");
    out
}

/// Format a comprehensive calibrated capacity estimate report for CLI and PGWire.
pub fn format_capacity_estimate_report(
    estimate: &CapacityEstimate,
    rows: &[EstimateRow],
) -> String {
    let mut out = String::new();
    out.push_str("EXPLAIN INCREMENTAL ESTIMATE (Calibrated Capacity Report)\n");
    out.push_str(
        "═══════════════════════════════════════════════════════════════════════════════\n",
    );
    let strat_str = match &estimate.selected_strategy {
        PhysicalStrategy::Classic => "classic (materialized intermediate)".to_string(),
        PhysicalStrategy::Factorized {
            payload_bound,
            factor_payload_bytes,
            delta_amplification,
        } => {
            format!("factorized (payload_bound={payload_bound}, payload_bytes={factor_payload_bytes}, delta_amp={delta_amplification:.2}x)")
        }
    };
    out.push_str(&format!("Selected Strategy : {}\n", strat_str));
    out.push_str(&format!(
        "Arrangements      : {} maintained canonical arrangements, {} active consumers\n",
        estimate.maintained_arrangements, estimate.consumer_count
    ));
    out.push_str(
        "───────────────────────────────────────────────────────────────────────────────\n",
    );
    out.push_str(&format!(
        "Private State     : {} bytes\n",
        estimate.private_state_bytes
    ));
    out.push_str(&format!(
        "Shared State      : {} bytes\n",
        estimate.shared_state_bytes
    ));
    out.push_str(&format!(
        "Saved Shared Bytes: {} bytes\n",
        estimate.saved_bytes
    ));
    out.push_str(&format!(
        "Estimated RSS     : {} bytes\n",
        estimate.rss_bytes
    ));
    out.push_str(&format!(
        "Spill State       : {} bytes\n",
        estimate.spill_bytes
    ));
    out.push_str(&format!(
        "Cache Hit Ratio   : {:.2}%\n",
        estimate.cache_hit_ratio * 100.0
    ));
    out.push_str(&format!(
        "Epoch Duration    : {:.2} ms\n",
        estimate.epoch_duration_ms
    ));
    out.push_str(&format!(
        "Commit Group Dur  : {:.2} ms\n",
        estimate.commit_group_duration_ms
    ));
    out.push_str(&format!(
        "p99 Freshness     : {:.2} ms\n",
        estimate.p99_freshness_ms
    ));
    out.push_str(&format!(
        "Shuffle Traffic   : {} bytes\n",
        estimate.shuffle_bytes
    ));
    out.push_str(&format!(
        "Logical Writes    : {} rows/epoch\n",
        estimate.logical_writes
    ));
    out.push_str(&format!(
        "Physical Writes   : {} rows/epoch\n",
        estimate.physical_writes
    ));
    out.push_str(&format!(
        "Object Store Reqs : {} reqs/epoch\n",
        estimate.object_store_requests
    ));
    out.push_str(&format!(
        "Checkpoint Cost   : {:.2} ms\n",
        estimate.checkpoint_cost_ms
    ));
    out.push_str(
        "───────────────────────────────────────────────────────────────────────────────\n",
    );
    out.push_str("Operator                        state_bytes   epoch_ms\n");
    out.push_str(
        "───────────────────────────────────────────────────────────────────────────────\n",
    );
    for row in rows {
        out.push_str(&format!(
            "{:<32} {:>10}   {:>8.2}\n",
            row.operator_kind, row.predicted_state_bytes, row.epoch_ms
        ));
    }
    out.push_str(
        "═══════════════════════════════════════════════════════════════════════════════\n",
    );
    out.push_str("(estimates based on canonical arrangements and sealed reference profiles; no operators deployed)\n");
    out
}

// ─── Internal ────────────────────────────────────────────────────────────────

fn key_byte_size(ty: Option<&CanonicalType>) -> u64 {
    match ty {
        Some(CanonicalType::Int64)
        | Some(CanonicalType::Timestamp)
        | Some(CanonicalType::TimestampTz) => 8,
        Some(CanonicalType::Int32) | Some(CanonicalType::Date) => 4,
        Some(CanonicalType::Utf8) => 32,
        Some(CanonicalType::Decimal(_, _)) => 16,
        Some(CanonicalType::Uuid) | Some(CanonicalType::Binary) => 16,
        _ => 8,
    }
}

fn val_byte_size(ty: Option<&CanonicalType>) -> u64 {
    match ty {
        Some(CanonicalType::Int64) => 8,
        Some(CanonicalType::Decimal(_, _)) => 16,
        Some(CanonicalType::Utf8) => 32,
        Some(CanonicalType::Uuid) | Some(CanonicalType::Binary) => 16,
        _ => 8,
    }
}

fn estimate_node(plan: &PlanNode, ctx: &CapacityEstimateContext, out: &mut Vec<EstimateRow>) {
    let cardinality_hint = ctx.cardinality_hint;
    let batch_rows = ctx.batch_rows;
    let key_size = key_byte_size(ctx.key_type.as_ref());
    let val_size = val_byte_size(ctx.val_type.as_ref());
    let fanout = ctx.fanout.unwrap_or(1).max(1);

    match plan {
        PlanNode::Source { name } => {
            out.push(EstimateRow {
                operator_kind: format!("Source[{name}]"),
                predicted_state_bytes: 0,
                epoch_ms: 0.0,
            });
        }

        PlanNode::Filter { input, .. } => {
            estimate_node(input, ctx, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Filter".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Project { input, .. } => {
            estimate_node(input, ctx, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Project".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Map { input, .. } => {
            estimate_node(input, ctx, out);
            let throughput_rows_per_s = 5_000_000.0_f64;
            out.push(EstimateRow {
                operator_kind: "Map".to_string(),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::Aggregate {
            input, aggregates, ..
        } => {
            estimate_node(input, ctx, out);

            let has_minmax = aggregates
                .iter()
                .any(|a| matches!(a.func, AggregateFunc::Min | AggregateFunc::Max));
            let has_avg = aggregates
                .iter()
                .any(|a| matches!(a.func, AggregateFunc::Avg));

            let (throughput_rows_per_s, state_per_group_bytes, kind) = if has_minmax {
                (100_000.0_f64, key_size + 32, "Aggregate[MinMax]")
            } else if has_avg {
                (800_000.0_f64, key_size + val_size + 16, "Aggregate[Avg]")
            } else {
                (1_000_000.0_f64, key_size + val_size + 8, "Aggregate")
            };

            out.push(EstimateRow {
                operator_kind: kind.to_string(),
                predicted_state_bytes: cardinality_hint * state_per_group_bytes,
                epoch_ms: epoch_ms(batch_rows, throughput_rows_per_s),
            });
        }

        PlanNode::InnerJoin { left, right, .. } | PlanNode::Join { left, right, .. } => {
            estimate_node(left, ctx, out);
            estimate_node(right, ctx, out);

            let left_row_size = key_size + 16;
            let right_row_size = key_size + 16;
            let left_arr_state = cardinality_hint * left_row_size;
            let right_arr_state = cardinality_hint * fanout * right_row_size;

            let (join_state, kind, tp) = match &ctx.selected_strategy {
                PhysicalStrategy::Classic => {
                    let materialized_intermediate =
                        cardinality_hint * fanout * (left_row_size + right_row_size);
                    (
                        left_arr_state + right_arr_state + materialized_intermediate,
                        "InnerJoin[classic]",
                        500_000.0_f64 / fanout as f64,
                    )
                }
                PhysicalStrategy::Factorized {
                    factor_payload_bytes,
                    ..
                } => {
                    let factor_state = cardinality_hint * (left_row_size + *factor_payload_bytes);
                    (
                        left_arr_state + right_arr_state + factor_state,
                        "InnerJoin[factorized]",
                        1_000_000.0_f64,
                    )
                }
            };

            out.push(EstimateRow {
                operator_kind: kind.to_string(),
                predicted_state_bytes: join_state,
                epoch_ms: epoch_ms(batch_rows, tp),
            });
        }

        PlanNode::TumbleWindow { input, .. } => {
            estimate_node(input, ctx, out);
            let state_per_window = cardinality_hint * (key_size + 16) * 2;
            out.push(EstimateRow {
                operator_kind: "TumbleWindow".to_string(),
                predicted_state_bytes: state_per_window,
                epoch_ms: epoch_ms(batch_rows, 1_000_000.0),
            });
        }

        PlanNode::HopWindow {
            input,
            window_size_ms,
            slide_ms,
            ..
        } => {
            estimate_node(input, ctx, out);
            let slices = (*window_size_ms as f64 / *slide_ms as f64).ceil() as u64;
            let state_per_window = cardinality_hint * (key_size + 16) * slices.max(1);
            out.push(EstimateRow {
                operator_kind: "HopWindow".to_string(),
                predicted_state_bytes: state_per_window,
                epoch_ms: epoch_ms(batch_rows, 800_000.0),
            });
        }

        PlanNode::SessionWindow { input, .. } => {
            estimate_node(input, ctx, out);
            let state_per_session = cardinality_hint * (key_size + 32);
            out.push(EstimateRow {
                operator_kind: "SessionWindow".to_string(),
                predicted_state_bytes: state_per_session,
                epoch_ms: epoch_ms(batch_rows, 600_000.0),
            });
        }

        PlanNode::Exchange { child, kind } => {
            estimate_node(child, ctx, out);
            out.push(EstimateRow {
                operator_kind: format!("Exchange[{kind:?}]"),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, 4_000_000.0),
            });
        }

        PlanNode::ViewSink {
            child, view_name, ..
        } => {
            estimate_node(child, ctx, out);
            out.push(EstimateRow {
                operator_kind: format!("ViewSink[{view_name}]"),
                predicted_state_bytes: 0,
                epoch_ms: epoch_ms(batch_rows, 2_000_000.0),
            });
        }

        PlanNode::IndexArrange {
            input,
            index_cols,
            pk_cols,
            ..
        } => {
            estimate_node(input, ctx, out);
            let state_per_row = 24u64;
            out.push(EstimateRow {
                operator_kind: format!(
                    "IndexArrange[idx={index_cols:?},pk={pk_cols:?}] estimated_index_state_bytes={}",
                    cardinality_hint * state_per_row
                ),
                predicted_state_bytes: cardinality_hint * state_per_row,
                epoch_ms: epoch_ms(batch_rows, 1_000_000.0),
            });
        }

        _ => {}
    }
}

fn epoch_ms(batch_rows: u64, throughput_rows_per_s: f64) -> f64 {
    (batch_rows as f64 / throughput_rows_per_s) * 1000.0
}
