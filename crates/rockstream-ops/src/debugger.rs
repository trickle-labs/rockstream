//! IVM Arrangement Debugger (v0.53.2, DESIGN.md §14.7.1).
//!
//! Provides deep inspection of intermediate arrangement state for operator-level
//! addressability, user-level key decoding/encoding, and non-perturbing snapshot reads.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use rockstream_plan::PlanNode;
use rockstream_storage::keys::{
    JoinSide, ShardKeyEncoder, MINMAX_DISCRIMINATOR, TK_DISCRIMINATOR, TW_DISCRIMINATOR,
};
use rockstream_storage::{reader::ShardReader, ShardDb, ShardPrefix};
use rockstream_types::ids::OperatorId;
use serde::{Deserialize, Serialize};

use crate::compile::find_source_name;
use crate::error::OpError;
use crate::live_exec::{with_view_id_scope, GroupKeyPacker, Stage, Utf8KeyPacker};

/// Information about a single operator node in a view pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorNodeInfo {
    pub op_id: String,
    pub kind: String,
    pub details: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// Decoded arrangement key representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedArrangementKey {
    pub family: String,
    pub user_key: String,
    pub group_key_i64: Option<i64>,
    pub composite_vals: Option<Vec<i64>>,
    pub utf8_val: Option<String>,
    pub window_id: Option<i64>,
    pub join_side: Option<String>,
    pub internal_key_bytes: Vec<u8>,
}

/// Arrangement inspection result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArrangementDebugResult {
    pub view_name: String,
    pub op_id: String,
    pub operator_kind: String,
    pub details: String,
    pub shard: String,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<String>,
    pub user_key: String,
    pub internal_key: String,
    pub state: serde_json::Value,
    pub weight: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delta: Option<String>,
    pub formatted_text: String,
}

impl ArrangementDebugResult {
    pub fn format_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "op_id:       {}  ({})\n",
            self.op_id, self.details
        ));
        out.push_str(&format!("shard:       {}\n", self.shard));
        if let Some(ref committed_at) = self.committed_at {
            out.push_str(&format!(
                "epoch:       {} (committed at {})\n",
                self.epoch, committed_at
            ));
        } else {
            out.push_str(&format!("epoch:       {}\n", self.epoch));
        }
        out.push_str(&format!("key:         {}\n", self.user_key));
        out.push_str(&format!("state:       {}\n", self.state));
        let weight_sign = if self.weight > 0 {
            format!("+{}", self.weight)
        } else {
            format!("{}", self.weight)
        };
        out.push_str(&format!("weight:      {}\n", weight_sign));
        if let Some(ref delta) = self.last_delta {
            out.push_str(&format!("last_delta:  {}\n", delta));
        }
        out
    }
}

/// Explain the operator IDs for a view's execution plan.
pub fn explain_view_op_ids(
    view_name: &str,
    plan: &PlanNode,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<Vec<OperatorNodeInfo>, OpError> {
    with_view_id_scope(view_name, || {
        explain_plan_op_ids_scoped(view_name, plan, table_schemas)
    })
}

fn explain_plan_op_ids_scoped(
    _view_name: &str,
    plan: &PlanNode,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<Vec<OperatorNodeInfo>, OpError> {
    let mut out = Vec::new();

    let PlanNode::ViewSink {
        view_name: root_view_name,
        pk: _,
        child,
    } = plan
    else {
        return Err(OpError::unsupported_plan_node("expected ViewSink root"));
    };

    let source_name = find_source_name(child).unwrap_or_default();
    let source_schema = table_schemas
        .get(&source_name)
        .cloned()
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

    if let Some(shape) = crate::compile::try_compile_join_shape(child, table_schemas)? {
        let sink_op_id = crate::live_exec::next_stateful_op_id();
        match shape.join {
            crate::live_exec::JoinKind::Inner(join_op) => {
                out.push(OperatorNodeInfo {
                    op_id: join_op.op_id().to_string(),
                    kind: "Join".to_string(),
                    details: "INNER JOIN".to_string(),
                    schema: Some(format!(
                        "left_source: {}, right_source: {}",
                        shape.left_source, shape.right_source
                    )),
                });
            }
            crate::live_exec::JoinKind::Outer(outer_op) => {
                out.push(OperatorNodeInfo {
                    op_id: outer_op.op_id().to_string(),
                    kind: "OuterJoin".to_string(),
                    details: "OUTER/SEMI/ANTI JOIN".to_string(),
                    schema: Some(format!(
                        "left_source: {}, right_source: {}",
                        shape.left_source, shape.right_source
                    )),
                });
            }
        }
        for (i, stage) in shape.post.iter().enumerate() {
            if let Some(op_id) = stage.op_id() {
                out.push(OperatorNodeInfo {
                    op_id: op_id.to_string(),
                    kind: stage.kind_name().to_string(),
                    details: format!("Post-join stage {}", i),
                    schema: None,
                });
            }
        }
        out.push(OperatorNodeInfo {
            op_id: sink_op_id.to_string(),
            kind: "ViewSink".to_string(),
            details: format!("Materialize output into {}", root_view_name),
            schema: None,
        });
        return Ok(out);
    }

    let (stages, out_schema) = crate::compile::compile_node(child, &source_schema)?;
    for (i, stage) in stages.iter().enumerate() {
        let op_id_str = stage
            .op_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| format!("stateless-{}", i));
        let details = match stage {
            Stage::Aggregate(op) => format!("SUM/COUNT/AVG aggregate (op_id: {})", op.op_id),
            Stage::MinMax(op, _) => format!("{:?} extremum multiset", op.kind),
            Stage::Distinct(_, _) => "Deduplicate rows by full content".to_string(),
            Stage::TumbleWindow(op, _) => format!("Tumble window (size: {} ms)", op.window_size_ms),
            Stage::HopWindow(op, _) => format!(
                "Hop window (size: {} ms, slide: {} ms)",
                op.window_size_ms, op.slide_ms
            ),
            Stage::SessionWindow(op, _) => format!("Session window (gap: {} ms)", op.gap_ms),
            Stage::TopK(_, _) => "TopK buffer arrangement".to_string(),
            Stage::KeyPack(_, _) => "GroupKeyPacker composite key pack".to_string(),
            Stage::KeyUnpack(_, _) => "GroupKeyPacker composite key unpack".to_string(),
            Stage::Utf8KeyPack(_, _) => "Utf8KeyPacker string key pack".to_string(),
            Stage::Utf8KeyUnpack(_, _) => "Utf8KeyUnpack string key unpack".to_string(),
            Stage::Utf8ColumnPack(_, idx, _) => format!("Utf8ColumnPacker col {}", idx),
            Stage::Utf8ColumnUnpack(_, idx, _) => format!("Utf8ColumnUnpack col {}", idx),
            Stage::MultiAggregate(_) => "MultiAggregatePipeline cascade joined lanes".to_string(),
            Stage::Stateless(_) => "Stateless linear transformation".to_string(),
            Stage::Window(_, _) => "Window arrangement".to_string(),
        };
        out.push(OperatorNodeInfo {
            op_id: op_id_str,
            kind: stage.kind_name().to_string(),
            details,
            schema: Some(format!("{:?}", out_schema.fields())),
        });
    }

    let sink_op_id = crate::live_exec::next_stateful_op_id();
    out.push(OperatorNodeInfo {
        op_id: sink_op_id.to_string(),
        kind: "ViewSink".to_string(),
        details: format!("Materialize output into {}", root_view_name),
        schema: Some(format!("{:?}", out_schema.fields())),
    });

    Ok(out)
}

/// Format `OperatorNodeInfo` into human-readable addressability text.
pub fn format_explain_op_ids(view_name: &str, query: &str, ops: &[OperatorNodeInfo]) -> String {
    let mut out = String::new();
    out.push_str(&format!("VIEW  {}\n", view_name));
    out.push_str(&format!("QUERY {}\n", query));
    out.push_str("OPERATORS:\n");
    for op in ops {
        out.push_str(&format!(
            "  ├─ {:<20} {:<16} ({})\n",
            op.op_id, op.kind, op.details
        ));
    }
    out
}

// ─── Key Decoding & Encoding Engine ──────────────────────────────────────────

/// Decode user-provided key string into structured key information.
pub fn decode_user_key(
    user_input: &str,
    operator_kind: &str,
    packer: Option<&GroupKeyPacker>,
    utf8_packer: Option<&Utf8KeyPacker>,
) -> Result<DecodedArrangementKey, OpError> {
    let trimmed = user_input.trim();

    match operator_kind.to_ascii_lowercase().as_str() {
        "aggregate" | "minmax" | "distinct" | "topk" => {
            // Check if composite key syntax: "k1=1, k2=2" or "1, 2"
            if trimmed.contains(',') {
                let parts: Vec<&str> = trimmed.split(',').map(|s| s.trim()).collect();
                let mut int_vals = Vec::new();
                for p in parts {
                    let val_str = if let Some((_, v)) = p.split_once('=') {
                        v.trim()
                    } else {
                        p
                    };
                    let val: i64 = val_str.parse().map_err(|_| {
                        OpError::arrangement_key_decode_failed(
                            operator_kind,
                            format!("failed to parse integer key component from '{}'", p),
                        )
                    })?;
                    int_vals.push(val);
                }

                let surrogate = if let Some(p) = packer {
                    p.surrogate_for_vals(&int_vals)
                } else {
                    int_vals[0]
                };

                let mut internal = Vec::new();
                internal.extend_from_slice(&surrogate.to_be_bytes());

                return Ok(DecodedArrangementKey {
                    family: operator_kind.to_string(),
                    user_key: trimmed.to_string(),
                    group_key_i64: Some(surrogate),
                    composite_vals: Some(int_vals),
                    utf8_val: None,
                    window_id: None,
                    join_side: None,
                    internal_key_bytes: internal,
                });
            }

            // Single value: "key=val" or "val"
            let val_str = if let Some((_, v)) = trimmed.split_once('=') {
                v.trim()
            } else {
                trimmed
            };

            // Check if numeric
            if let Ok(num) = val_str.parse::<i64>() {
                return Ok(DecodedArrangementKey {
                    family: operator_kind.to_string(),
                    user_key: trimmed.to_string(),
                    group_key_i64: Some(num),
                    composite_vals: Some(vec![num]),
                    utf8_val: None,
                    window_id: None,
                    join_side: None,
                    internal_key_bytes: num.to_be_bytes().to_vec(),
                });
            }

            // Utf8 string key
            let surrogate = if let Some(up) = utf8_packer {
                up.surrogate_for_key(val_str)
            } else {
                0
            };
            Ok(DecodedArrangementKey {
                family: operator_kind.to_string(),
                user_key: trimmed.to_string(),
                group_key_i64: Some(surrogate),
                composite_vals: None,
                utf8_val: Some(val_str.to_string()),
                window_id: None,
                join_side: None,
                internal_key_bytes: surrogate.to_be_bytes().to_vec(),
            })
        }
        "tumblewindow" | "hopwindow" => {
            // "window_start=1000, key=42" or "1000, 42"
            let (w_id, g_key) = parse_window_key_parts(trimmed, operator_kind)?;
            let mut internal = Vec::new();
            internal.extend_from_slice(&w_id.to_be_bytes());
            internal.extend_from_slice(&g_key.to_be_bytes());
            Ok(DecodedArrangementKey {
                family: operator_kind.to_string(),
                user_key: trimmed.to_string(),
                group_key_i64: Some(g_key),
                composite_vals: Some(vec![g_key]),
                utf8_val: None,
                window_id: Some(w_id),
                join_side: None,
                internal_key_bytes: internal,
            })
        }
        "sessionwindow" => {
            // "session_start=1000, user_id=42" or "1000, 42"
            let (s_id, g_key) = parse_window_key_parts(trimmed, operator_kind)?;
            let mut internal = Vec::new();
            internal.extend_from_slice(&s_id.to_be_bytes());
            internal.extend_from_slice(&g_key.to_be_bytes());
            Ok(DecodedArrangementKey {
                family: operator_kind.to_string(),
                user_key: trimmed.to_string(),
                group_key_i64: Some(g_key),
                composite_vals: Some(vec![g_key]),
                utf8_val: None,
                window_id: Some(s_id),
                join_side: None,
                internal_key_bytes: internal,
            })
        }
        "join" | "outerjoin" => {
            // "left: product_id=42" or "left: 42" or "right: 42"
            let (side, key_num) = parse_join_key_parts(trimmed, operator_kind)?;
            let mut internal = Vec::new();
            internal.extend_from_slice(&key_num.to_be_bytes());
            Ok(DecodedArrangementKey {
                family: operator_kind.to_string(),
                user_key: trimmed.to_string(),
                group_key_i64: Some(key_num),
                composite_vals: Some(vec![key_num]),
                utf8_val: None,
                window_id: None,
                join_side: Some(side),
                internal_key_bytes: internal,
            })
        }
        _ => Err(OpError::arrangement_key_decode_failed(
            operator_kind,
            format!(
                "unsupported operator family '{}' for key decoding",
                operator_kind
            ),
        )),
    }
}

fn parse_window_key_parts(input: &str, family: &str) -> Result<(i64, i64), OpError> {
    let parts: Vec<&str> = input.split(',').map(|s| s.trim()).collect();
    if parts.len() < 2 {
        // Single number fallback: treat as window_id=0, key=val or vice versa
        if let Ok(v) = input.parse::<i64>() {
            return Ok((0, v));
        }
        return Err(OpError::arrangement_key_decode_failed(
            family,
            format!("expected 'window_start=X, key=Y', got '{}'", input),
        ));
    }
    let p0 = if let Some((_, v)) = parts[0].split_once('=') {
        v.trim()
    } else {
        parts[0]
    };
    let p1 = if let Some((_, v)) = parts[1].split_once('=') {
        v.trim()
    } else {
        parts[1]
    };

    let w: i64 = p0.parse().map_err(|_| {
        OpError::arrangement_key_decode_failed(
            family,
            format!("invalid window/session identifier '{}'", p0),
        )
    })?;
    let k: i64 = p1.parse().map_err(|_| {
        OpError::arrangement_key_decode_failed(family, format!("invalid group key '{}'", p1))
    })?;
    Ok((w, k))
}

fn parse_join_key_parts(input: &str, family: &str) -> Result<(String, i64), OpError> {
    let (side_str, rest) = if let Some((s, r)) = input.split_once(':') {
        (s.trim().to_ascii_lowercase(), r.trim())
    } else if let Some((s, r)) = input.split_once(',') {
        (s.trim().to_ascii_lowercase(), r.trim())
    } else {
        ("left".to_string(), input)
    };

    let val_str = if let Some((_, v)) = rest.split_once('=') {
        v.trim()
    } else {
        rest
    };

    let key_num: i64 = val_str.parse().map_err(|_| {
        OpError::arrangement_key_decode_failed(
            family,
            format!("invalid join key integer '{}'", val_str),
        )
    })?;

    let side = if side_str.contains("right") || side_str == "r" {
        "right".to_string()
    } else {
        "left".to_string()
    };

    Ok((side, key_num))
}

// ─── Storage Arrangement Inspection ──────────────────────────────────────────

/// Inspect the arrangement state in `ShardDb` for a given operator and key.
pub async fn inspect_arrangement_db(
    db: &ShardDb,
    view_name: &str,
    op_id: OperatorId,
    operator_kind: &str,
    decoded_key: &DecodedArrangementKey,
    epoch: Option<u64>,
    shard_name: &str,
) -> Result<ArrangementDebugResult, OpError> {
    let oid = op_id.0;
    let kind_lower = operator_kind.to_ascii_lowercase();

    let (state_json, weight, last_delta) = match kind_lower.as_str() {
        "aggregate" => {
            let group_key = decoded_key.group_key_i64.unwrap_or(0);
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, oid, &group_key.to_be_bytes());
            let val = db.get(&key).await.map_err(OpError::storage)?;
            if let Some(bytes) = val {
                if bytes.len() >= 16 {
                    let sum = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
                    let count = i64::from_be_bytes(bytes[8..16].try_into().unwrap_or([0; 8]));
                    let w = if count > 0 { 1 } else { 0 };
                    (
                        serde_json::json!({"sum": sum, "row_count": count}),
                        w,
                        Some(format!(
                            "epoch {} (+{} sum, +{} rows)",
                            epoch.unwrap_or(0),
                            sum,
                            count
                        )),
                    )
                } else {
                    (serde_json::json!({}), 0, None)
                }
            } else {
                (serde_json::json!({}), 0, None)
            }
        }
        "minmax" => {
            let group_key = decoded_key.group_key_i64.unwrap_or(0);
            let ext_prefix = ShardKeyEncoder::minmax_group_prefix(oid, group_key);
            let multiset_prefix = {
                let mut p = Vec::with_capacity(1 + 1 + 8 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.push(MINMAX_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p.extend_from_slice(&group_key.to_be_bytes());
                p
            };
            let entries = db
                .scan_prefix(&multiset_prefix)
                .await
                .map_err(OpError::storage)?;
            let total_count: i64 = entries
                .iter()
                .filter_map(|(_, v)| {
                    if v.len() >= 8 {
                        Some(i64::from_be_bytes(v[0..8].try_into().unwrap_or([0; 8])))
                    } else {
                        None
                    }
                })
                .sum();
            let ext_val = db.get(&ext_prefix).await.map_err(OpError::storage)?;
            let extremum = ext_val.and_then(|b| {
                if b.len() >= 8 {
                    Some(i64::from_be_bytes(b[0..8].try_into().unwrap_or([0; 8])))
                } else {
                    None
                }
            });
            let w = if total_count > 0 { 1 } else { 0 };
            (
                serde_json::json!({"extremum": extremum, "multiset_entries": entries.len(), "total_weight": total_count}),
                w,
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "distinct" => {
            let prefix = ShardKeyEncoder::distinct_op_prefix(oid);
            let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
            let w = if !entries.is_empty() { 1 } else { 0 };
            (
                serde_json::json!({"distinct_rows": entries.len()}),
                w,
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "tumblewindow" | "hopwindow" | "sessionwindow" => {
            let prefix = {
                let mut p = Vec::with_capacity(1 + 2 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.extend_from_slice(&TW_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p
            };
            let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
            let total_weight: i64 = entries.len() as i64;
            (
                serde_json::json!({"window_entries": entries.len(), "window_id": decoded_key.window_id}),
                if total_weight > 0 { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "join" | "outerjoin" => {
            let side = if decoded_key.join_side.as_deref() == Some("right") {
                JoinSide::Right
            } else {
                JoinSide::Left
            };
            let prefix = ShardKeyEncoder::join_arr_op_prefix(side, oid);
            let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
            (
                serde_json::json!({"side": format!("{:?}", side), "matched_rows": entries.len()}),
                if !entries.is_empty() { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "topk" => {
            let prefix = {
                let mut p = Vec::with_capacity(1 + 2 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.extend_from_slice(&TK_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p
            };
            let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
            (
                serde_json::json!({"topk_buffer_entries": entries.len()}),
                if !entries.is_empty() { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        other => {
            return Err(OpError::arrangement_key_decode_failed(
                other,
                format!("unsupported operator family '{}' for inspection", other),
            ));
        }
    };

    let result = ArrangementDebugResult {
        view_name: view_name.to_string(),
        op_id: op_id.to_string(),
        operator_kind: operator_kind.to_string(),
        details: format!("Inspection for {}", operator_kind),
        shard: shard_name.to_string(),
        epoch: epoch.unwrap_or(0),
        committed_at: None,
        user_key: decoded_key.user_key.clone(),
        internal_key: encode_hex(&decoded_key.internal_key_bytes),
        state: state_json,
        weight,
        last_delta,
        formatted_text: String::new(),
    };

    let formatted = result.format_text();
    Ok(ArrangementDebugResult {
        formatted_text: formatted,
        ..result
    })
}

/// Inspect arrangement state using a read-only `ShardReader`.
pub async fn inspect_arrangement_reader(
    reader: &ShardReader,
    view_name: &str,
    op_id: OperatorId,
    operator_kind: &str,
    decoded_key: &DecodedArrangementKey,
    epoch: Option<u64>,
    shard_name: &str,
) -> Result<ArrangementDebugResult, OpError> {
    let oid = op_id.0;
    let kind_lower = operator_kind.to_ascii_lowercase();

    let (state_json, weight, last_delta) = match kind_lower.as_str() {
        "aggregate" => {
            let group_key = decoded_key.group_key_i64.unwrap_or(0);
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, oid, &group_key.to_be_bytes());
            let val = reader.get(&key).await.map_err(OpError::storage)?;
            if let Some(bytes) = val {
                if bytes.len() >= 16 {
                    let sum = i64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
                    let count = i64::from_be_bytes(bytes[8..16].try_into().unwrap_or([0; 8]));
                    let w = if count > 0 { 1 } else { 0 };
                    (
                        serde_json::json!({"sum": sum, "row_count": count}),
                        w,
                        Some(format!(
                            "epoch {} (+{} sum, +{} rows)",
                            epoch.unwrap_or(0),
                            sum,
                            count
                        )),
                    )
                } else {
                    (serde_json::json!({}), 0, None)
                }
            } else {
                (serde_json::json!({}), 0, None)
            }
        }
        "minmax" => {
            let group_key = decoded_key.group_key_i64.unwrap_or(0);
            let multiset_prefix = {
                let mut p = Vec::with_capacity(1 + 1 + 8 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.push(MINMAX_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p.extend_from_slice(&group_key.to_be_bytes());
                p
            };
            let entries = reader
                .scan_prefix(&multiset_prefix)
                .await
                .map_err(OpError::storage)?;
            let total_count: i64 = entries
                .iter()
                .filter_map(|(_, v)| {
                    if v.len() >= 8 {
                        Some(i64::from_be_bytes(v[0..8].try_into().unwrap_or([0; 8])))
                    } else {
                        None
                    }
                })
                .sum();
            let w = if total_count > 0 { 1 } else { 0 };
            (
                serde_json::json!({"multiset_entries": entries.len(), "total_weight": total_count}),
                w,
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "distinct" => {
            let prefix = ShardKeyEncoder::distinct_op_prefix(oid);
            let entries = reader
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::storage)?;
            let w = if !entries.is_empty() { 1 } else { 0 };
            (
                serde_json::json!({"distinct_rows": entries.len()}),
                w,
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "tumblewindow" | "hopwindow" | "sessionwindow" => {
            let prefix = {
                let mut p = Vec::with_capacity(1 + 2 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.extend_from_slice(&TW_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p
            };
            let entries = reader
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::storage)?;
            (
                serde_json::json!({"window_entries": entries.len(), "window_id": decoded_key.window_id}),
                if !entries.is_empty() { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "join" | "outerjoin" => {
            let side = if decoded_key.join_side.as_deref() == Some("right") {
                JoinSide::Right
            } else {
                JoinSide::Left
            };
            let prefix = ShardKeyEncoder::join_arr_op_prefix(side, oid);
            let entries = reader
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::storage)?;
            (
                serde_json::json!({"side": format!("{:?}", side), "matched_rows": entries.len()}),
                if !entries.is_empty() { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        "topk" => {
            let prefix = {
                let mut p = Vec::with_capacity(1 + 2 + 8);
                p.push(ShardPrefix::OpState.as_byte());
                p.extend_from_slice(&TK_DISCRIMINATOR);
                p.extend_from_slice(&oid.to_be_bytes());
                p
            };
            let entries = reader
                .scan_prefix(&prefix)
                .await
                .map_err(OpError::storage)?;
            (
                serde_json::json!({"topk_buffer_entries": entries.len()}),
                if !entries.is_empty() { 1 } else { 0 },
                Some(format!("epoch {}", epoch.unwrap_or(0))),
            )
        }
        other => {
            return Err(OpError::arrangement_key_decode_failed(
                other,
                format!("unsupported operator family '{}' for inspection", other),
            ));
        }
    };

    let result = ArrangementDebugResult {
        view_name: view_name.to_string(),
        op_id: op_id.to_string(),
        operator_kind: operator_kind.to_string(),
        details: format!("Inspection for {}", operator_kind),
        shard: shard_name.to_string(),
        epoch: epoch.unwrap_or(0),
        committed_at: None,
        user_key: decoded_key.user_key.clone(),
        internal_key: encode_hex(&decoded_key.internal_key_bytes),
        state: state_json,
        weight,
        last_delta,
        formatted_text: String::new(),
    };

    let formatted = result.format_text();
    Ok(ArrangementDebugResult {
        formatted_text: formatted,
        ..result
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrangement_key_packer_decoders_and_refusal() {
        // 1. Single numeric key
        let k1 = decode_user_key("product_id=42", "Aggregate", None, None).unwrap();
        assert_eq!(k1.group_key_i64, Some(42));
        assert_eq!(k1.user_key, "product_id=42");

        // 2. Composite surrogate key roundtrip with GroupKeyPacker
        let packer = GroupKeyPacker::new(2);
        let surrogate = packer.surrogate_for_vals(&[5, 10]);
        let k2 = decode_user_key(
            "category_id=5, region_id=10",
            "Aggregate",
            Some(&packer),
            None,
        )
        .unwrap();
        assert_eq!(k2.group_key_i64, Some(surrogate));
        assert_eq!(packer.reverse_lookup(surrogate), Some(vec![5, 10]));

        // 3. Utf8 key roundtrip with Utf8KeyPacker
        let utf8_packer = Utf8KeyPacker::new();
        let utf8_surrogate = utf8_packer.surrogate_for_key("electronics");
        let k3 =
            decode_user_key("name=electronics", "Aggregate", None, Some(&utf8_packer)).unwrap();
        assert_eq!(k3.group_key_i64, Some(utf8_surrogate));
        assert_eq!(
            utf8_packer.reverse_lookup(utf8_surrogate),
            Some("electronics".to_string())
        );

        // 4. Window key
        let k4 = decode_user_key("window_start=1000, key=42", "TumbleWindow", None, None).unwrap();
        assert_eq!(k4.window_id, Some(1000));
        assert_eq!(k4.group_key_i64, Some(42));

        // 5. Session key
        let k5 = decode_user_key(
            "session_start=2000, user_id=99",
            "SessionWindow",
            None,
            None,
        )
        .unwrap();
        assert_eq!(k5.window_id, Some(2000));
        assert_eq!(k5.group_key_i64, Some(99));

        // 6. Join key
        let k6 = decode_user_key("left: product_id=42", "Join", None, None).unwrap();
        assert_eq!(k6.join_side, Some("left".to_string()));
        assert_eq!(k6.group_key_i64, Some(42));

        // 7. Unsupported family refusal
        let err = decode_user_key("key=123", "CustomUnsupportedFamily", None, None).unwrap_err();
        match err {
            OpError::ArrangementKeyDecodeFailed { family, .. } => {
                assert_eq!(family, "CustomUnsupportedFamily");
            }
            _ => panic!("expected ArrangementKeyDecodeFailed error"),
        }
    }
}
